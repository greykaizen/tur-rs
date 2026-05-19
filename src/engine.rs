use std::collections::{HashMap, HashSet};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fs::File as StdFile;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use ::http::header::{
    ACCEPT, ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE,
    LOCATION, RANGE, USER_AGENT,
};
use ::http::{Method, Request, Uri, Version};
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use sysinfo::System;
use url::Url;
use uuid::Uuid;

use crate::storage::{self, StorageConfig};

const MB: u64 = 1024 * 1024;
const INDEX_STATE_MB: u64 = 8;
const INDEX_STATE_BYTES: u64 = INDEX_STATE_MB * MB;
const DEFAULT_BORROW_LIMIT_MB: u64 = 2;
const LIVE_SEED_FLOOR_MB: u64 = 13;
const LIVE_PREFETCH_MIN_MB: u64 = 2;
const LIVE_PREFETCH_HANDSHAKE_MS: u64 = 700;
const MIN_REUSED_RTT_SAMPLES_FOR_GROWTH_SHIELD: u64 = 2;
const MAX_RANGE_RETRIES: u32 = 8;
const RETRY_BASE_DELAY_MS: u64 = 250;
const RETRY_MAX_DELAY_MS: u64 = 2_000;
const DRY_RUN_STEP_BYTES: u64 = 256 * 1024;
const DRY_RUN_STEP_DELAY_MS: u64 = 4;
const WRITE_BUFFER_MIN_BYTES: usize = 128 * 1024;
const WRITE_BUFFER_MEDIUM_BYTES: usize = 256 * 1024;
const WRITE_BUFFER_LARGE_BYTES: usize = 512 * 1024;
const WRITE_BUFFER_MAX_BYTES: usize = MB as usize;
const WRITE_BUFFER_MEDIUM_SPEED_BPS: u64 = MB;
const WRITE_BUFFER_LARGE_SPEED_BPS: u64 = 3 * MB;
const WRITE_BUFFER_MAX_SPEED_BPS: u64 = 6 * MB;
const HTTP2_MAX_FRAME_BYTES: u32 = 256 * 1024;
const ORIGIN_PHI_RATIO_CAPACITY: usize = 256;
const ORIGIN_MEMORY_CAPACITY: usize = 1000;
const TCP_KEEPALIVE_INTERVAL_SECS: u64 = 20;
const GOLDEN_RATIO_NUM: u64 = 633;
const GOLDEN_RATIO_DEN: u64 = 1024;
const MAX_REDIRECTS: usize = 8;
const USER_AGENT_VALUE: &str = concat!("tur/", env!("CARGO_PKG_VERSION"));

type DownloadHttpClient =
    HyperClient<HttpsConnector<crate::connector::TunedConnector>, Empty<Bytes>>;


mod coordinator;
mod helpers;
pub(super) use helpers::*;
mod http;
mod metrics;
mod origin_memory;
mod persistence;
mod ranges;
mod runtime;
mod scaler;
#[cfg(test)]
mod tests;
mod types;
mod worker;

pub use runtime::DownloadEngine;
pub use types::{
    ActiveRange, DownloadStatus, DownloadTask, EngineCommand, EngineEvent, HaltMode, HttpMode,
    ScheduleMode, WorkRequest, WorkerSnapshot, WorkerState,
};

use coordinator::{
    Coordinator, IndexStateMap, TaskSnapshot, INITIAL_PHI_MAX_RATIO,
    RANGE_STATUS_FINISHED, RANGE_STATUS_PENDING, STORAGE_BLOCK_SIZE, UNASSIGNED_CONNECTION,
};
use http::{
    align_down, build_http_client, bytes_to_ceiling_mb, bytes_to_floor_mb,
    compute_protocol_aware_steal_floor_bytes,
    dominant_protocol_from_metrics, http_version_label, learn_http2_client_tuning,    protocol_family_for_version,
    protocol_growth_shield_min_samples, protocol_growth_shield_multiplier,
    record_max_active_connections, record_protocol_request_metric,
    record_protocol_scale_metric, send_request_follow_redirects,
};
use metrics::SchedulerMetrics;

// Used only by the tests module
#[cfg(test)]
use http::{
    protocol_effective_add_threshold,
    protocol_prefetch_handshake_ms,
};
use origin_memory::{
    origin_key, ClientTuning, H2TuningSource, OriginH2TuningStore, OriginMemoryStore,
    SessionRequirementField,
};

/// Build an `http::HeaderMap` from a `DownloadTask`'s request context.
/// Returns `None` if there are no custom headers to add.
fn build_request_headers(task: &DownloadTask) -> Option<::http::HeaderMap> {
    let ctx = task.request_context.as_ref()?;
    let mut headers = ::http::HeaderMap::new();

    // Custom headers
    for (name, value) in &ctx.headers {
        if let (Ok(n), Ok(v)) = (
            ::http::HeaderName::from_bytes(name.as_bytes()),
            ::http::HeaderValue::from_str(value),
        ) {
            headers.insert(n, v);
        }
    }

    // Authorization
    if let Some(ref auth) = ctx.auth {
        if let Ok(v) = ::http::HeaderValue::from_str(auth) {
            headers.insert(::http::header::AUTHORIZATION, v);
        }
    }

    // Referer
    if let Some(ref referer) = ctx.referer {
        if let Ok(v) = ::http::HeaderValue::from_str(referer) {
            headers.insert(::http::header::REFERER, v);
        }
    }

    // User-Agent override
    if let Some(ref ua) = ctx.user_agent {
        if let Ok(v) = ::http::HeaderValue::from_str(ua) {
            headers.insert(::http::header::USER_AGENT, v);
        }
    }

    // Cookies — convert Vec<CookieEntry> to Cookie header
    if let Some(ref cookies) = ctx.cookies {
        if !cookies.is_empty() {
            let value = cookies
                .iter()
                .map(|c| c.to_request_value())
                .collect::<Vec<_>>()
                .join("; ");
            if let Ok(v) = ::http::HeaderValue::from_str(&value) {
                headers.insert(::http::header::COOKIE, v);
            }
        }
    }

    if headers.is_empty() { None } else { Some(headers) }
}

/// Detect whether a response body indicates a challenge/interstitial page
/// rather than a real file response.
pub(super) fn classify_response_body(body_bytes: &[u8]) -> Option<ChallengeKind> {
    // Fast path: short circuit for empty or obviously binary
    if body_bytes.len() < 64 {
        return None;
    }

    // Check if the body looks like HTML
    let sample = body_bytes.get(..4096).unwrap_or(body_bytes);
    let as_str = std::str::from_utf8(sample).ok()?;
    let lower = as_str.to_ascii_lowercase();

    if !lower.contains("<html") && !lower.contains("<!doc") {
        return None; // Not HTML — likely a real binary response
    }

    // Cloudflare challenge detection
    if lower.contains("cloudflare")
        && (lower.contains("challenge")
            || lower.contains("attention required")
            || lower.contains("cf-browser-verification"))
    {
        return Some(ChallengeKind::CloudflareChallenge);
    }

    // Generic challenge/interstitial
    if lower.contains("challenge") && lower.contains("captcha") {
        return Some(ChallengeKind::CaptchaChallenge);
    }

    if lower.contains("just a moment...")
        || (lower.contains("checking your browser") && lower.contains("ddo"))
    {
        return Some(ChallengeKind::BrowserCheck);
    }

    // Generic interstitial (login wall, terms page, etc.)
    if lower.contains("sign in") || lower.contains("log in") || lower.contains("authenticate")
        && (lower.contains("continue") || lower.contains("proceed"))
    {
        return Some(ChallengeKind::AuthInterstitial);
    }

    // The response is HTML but doesn't match known patterns — flag as unexpected HTML
    Some(ChallengeKind::UnexpectedHtml)
}

/// Classification of non-file responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChallengeKind {
    CloudflareChallenge,
    CaptchaChallenge,
    BrowserCheck,
    AuthInterstitial,
    UnexpectedHtml,
}

impl ChallengeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CloudflareChallenge => "cloudflare-challenge",
            Self::CaptchaChallenge => "captcha-challenge",
            Self::BrowserCheck => "browser-check",
            Self::AuthInterstitial => "auth-interstitial",
            Self::UnexpectedHtml => "unexpected-html",
        }
    }

    /// Returns true if the challenge likely requires a browser-assisted handoff.
    pub fn requires_browser_session(self) -> bool {
        matches!(
            self,
            Self::CloudflareChallenge | Self::CaptchaChallenge | Self::BrowserCheck
        )
    }
}
use persistence::{ensure_parent_dir, load_snapshot, log_path, metadata_path, persist_snapshot, unix_time_ms};
use ranges::{is_tail_phase_bytes, snapshot_downloaded};
use runtime::{OriginPhiRatioStore, RuntimeControl, WorkerControl, WorkerSlot};
use scaler::{ProtocolFamily, Scaler, ScalerAction, ScalerConfig, TokenBucket};
use worker::ConnectionWorker;

async fn run_download_task(
    engine: Rc<DownloadEngine>,
    task: DownloadTask,
    snapshot: Option<TaskSnapshot>,
    control: Rc<RuntimeControl>,
    bucket: Rc<TokenBucket>,
    cmd_tx: mpsc::Sender<EngineCommand>,
    event_tx: mpsc::Sender<EngineEvent>,
    default_connections: usize,
) -> Result<()> {
    run_download_task_local(
        engine,
        task,
        snapshot,
        control,
        bucket,
        cmd_tx,
        event_tx,
        default_connections,
    ).await
}

async fn run_download_task_local(
    engine: Rc<DownloadEngine>,
    mut task: DownloadTask,
    snapshot: Option<TaskSnapshot>,
    control: Rc<RuntimeControl>,
    bucket: Rc<TokenBucket>,
    cmd_tx: mpsc::Sender<EngineCommand>,
    event_tx: mpsc::Sender<EngineEvent>,
    default_connections: usize,
) -> Result<()> {
    // Phase 6: Parallel startup — overlap HEAD request with setup work
    let origin = origin_key(&task.url);
    if task.connections == 0 {
        task.connections = default_connections.max(1);
    }
    if task.borrow_limit_mb == 0 {
        task.borrow_limit_mb = DEFAULT_BORROW_LIMIT_MB;
    }

    let log_path = log_path(&task);
    ensure_parent_dir(&log_path)?;
    let metrics = Rc::new(SchedulerMetrics::default());
    let protocol_hint = engine
        .origin_memory
        .borrow_mut()
        .protocol_hint_for_origin(&origin)
        .unwrap_or(ProtocolFamily::Other);
    let phi_max_ratio = engine.origin_phi_ratios.borrow_mut().ratio_for_origin(&origin);
    let origin_memory_hit = engine.origin_memory.borrow().memory_hit_for_origin(&origin);
    let shared_write_latency_ms = Rc::new(Cell::new(10.0));
    let write_buffer_cap_bytes = engine.write_buffer_cap_bytes.clone();
    let adaptive_minimum_steal_bytes = Rc::new(Cell::new(
        (task.borrow_limit_mb.max(1) * MB).max(2 * STORAGE_BLOCK_SIZE),
    ));

    // Set up connection acquisition state early so HEAD and connections can run concurrently
    let leased_connections = Rc::new(Cell::new(0usize));
    let desired_initial_connections = task.connections.max(1);

    let total_size = if let Some(snapshot) = &snapshot {
        snapshot.task.total_size
    } else if task.dry_run && task.dry_run_size_mb.is_some() {
        task.dry_run_size_mb.unwrap() * MB
    } else {
        // Phase 6: Run HEAD request concurrently with connection budget acquisition
        let (head_client, _) = build_http_client(task.http_mode, task.max_connections.max(1), None);
        let head_url = task.url.clone();

        let mut conn_done = false;
        let conn_fut = async {
            let mut cnt = 0usize;
            while cnt == 0 {
                if control.is_halted() {
                    break;
                }
                if engine.request_connection() {
                    cnt = 1;
                    leased_connections.set(leased_connections.get() + 1);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            while cnt < desired_initial_connections {
                if !engine.request_connection() {
                    break;
                }
                cnt += 1;
                leased_connections.set(leased_connections.get() + 1);
            }
            cnt
        };
        let head_extra_headers = build_request_headers(&task);
        let head_fut =
            send_request_follow_redirects(&head_client, Method::HEAD, &head_url, None, head_extra_headers.as_ref());
        tokio::pin!(head_fut);
        tokio::pin!(conn_fut);

        let res = loop {
            tokio::select! {
                result = &mut head_fut => {
                    break result?;
                }
                _ = &mut conn_fut, if !conn_done => {
                    conn_done = true;
                }
            }
        };

        // Only await conn_fut if it hasn't completed yet (head_fut won the race)
        if !conn_done {
            let _ = conn_fut.await;
        }
        let size = res
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        if size == 0 {
            return Err(anyhow!("Could not determine file size"));
        }

        if !task.dry_run {
            engine.origin_memory.borrow_mut().note_content_length_reliable(&origin, size > 0);
            // Phase 3c: Parse Alt-Svc from HEAD response for H3 upgrade hint
            if let Some(h3_port) = crate::quic::parse_alt_svc_h3_port(res.headers()) {
                engine.origin_memory.borrow_mut().note_protocol(&origin, ProtocolFamily::Http3);
                if let Ok(mut log_file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&log_path)
                {
                    let _ = writeln!(log_file, "alt_svc_h3_cached origin={} port={}", origin, h3_port);
                }
            }
        }

        size
    };

    task.total_size = total_size;
    let _ = event_tx.send(EngineEvent::TotalSize(task.id, total_size)).await;
    let _ = event_tx
        .send(EngineEvent::StatusChanged(task.id, DownloadStatus::Downloading))
        .await;
    let mut coordinator = if let Some(snapshot) = snapshot {
        Coordinator::from_snapshot(
            snapshot.coordinator,
            snapshot.task.total_size,
            &log_path,
            task.schedule_mode,
            metrics.clone(),
            adaptive_minimum_steal_bytes.clone(),
        )?
    } else {
        Coordinator::new(
            task.id,
            total_size,
            &log_path,
            task.borrow_limit_mb,
            task.connections,
            task.dry_run,
            task.schedule_mode,
            metrics.clone(),
            adaptive_minimum_steal_bytes.clone(),
            phi_max_ratio,
        )?
    };

    let downloaded = snapshot_downloaded(&coordinator, total_size);
    let global_downloaded = Rc::new(Cell::new(downloaded));
    let index_state = coordinator.index_state.clone();

    let file_path = task.dir.join(&task.filename);
    if !task.dry_run {
        if let Err(err) = storage::prepare_download_file(&file_path, total_size) {
            let _ = event_tx
                .send(EngineEvent::StatusChanged(
                    task.id,
                    DownloadStatus::Error(format!(
                        "failed to prepare download file {}: {}",
                        file_path.display(),
                        err
                    )),
                ))
                .await;
            return Ok(());
        }
    }

    let min_connections = task.min_connections.max(1).min(task.max_connections.max(1));
    let max_connections = task.max_connections.max(min_connections);
    task.connections = task.connections.clamp(min_connections, max_connections);
    let (work_tx, work_rx) = mpsc::channel(128);
    let learned_h2_tuning = engine.origin_h2_tunings.borrow_mut().tuning_for_origin(&origin);
    let (http_client, client_tuning) =
        build_http_client(task.http_mode, max_connections, learned_h2_tuning);

    // Initialize H3 client — only available with http3 feature
    let h3_client: Option<Rc<RefCell<crate::quic::H3Client>>> = {
        #[cfg(feature = "http3")]
        {
            let should_try_h3 = task.http_mode == HttpMode::Http3
                || (task.http_mode == HttpMode::Auto && protocol_hint == ProtocolFamily::Http3);
            if should_try_h3 {
                match crate::quic::H3Client::new() {
                    Ok(h3) => Some(Rc::new(RefCell::new(h3))),
                    Err(e) => {
                        eprintln!("WARNING: Failed to create H3 client: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        }
        #[cfg(not(feature = "http3"))]
        {
            None
        }
    };

    let scaler_config = ScalerConfig {
        min_connections,
        max_connections,
        heartbeat_ms: 2000,
    };
    {
        let config = control.scaler_config();
        *config.borrow_mut() = scaler_config;
    }
    let scaler = Scaler::from_config_handle(control.scaler_config());
    scaler.last_protocol.set(protocol_hint);
    let scaler_engine = engine.clone();

    let handles = Rc::new(RefCell::new(Vec::<WorkerSlot>::new()));
    // For snapshot/dry_run paths, connections were not pre-acquired; do it now
    let mut initial_connections = leased_connections.get();
    if initial_connections == 0 && !matches!(total_size, 0) {
        while initial_connections == 0 {
            if control.is_halted() {
                break;
            }
            if engine.request_connection() {
                initial_connections = 1;
                leased_connections.set(leased_connections.get() + 1);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        while initial_connections < desired_initial_connections {
            if !engine.request_connection() {
                break;
            }
            initial_connections += 1;
            leased_connections.set(leased_connections.get() + 1);
        }
    }
    if initial_connections == 0 {
        let _ = event_tx
            .send(EngineEvent::StatusChanged(
                task.id,
                DownloadStatus::Error("no connection budget available".to_string()),
            ))
            .await;
        return Ok(());
    }
    // Build request_headers once, used by all workers and the scaler spawn
    let request_headers = build_request_headers(&task);
    scaler.n_active.set(initial_connections);
    record_max_active_connections(&metrics, initial_connections);
        for connection_id in 0..initial_connections {
        let worker_control = WorkerControl::new(connection_id as u32);
        worker_control.pending_growth_probe.set(false);
        let worker = ConnectionWorker {
            connection_id: connection_id as u32,
            url: task.url.clone(),
            origin: origin.clone(),
            file_path: file_path.clone(),
            log_path: log_path.clone(),
            coordinator_tx: work_tx.clone(),
            global_downloaded: global_downloaded.clone(),
            control: control.clone(),
            worker_control: worker_control.clone(),
            dry_run: task.dry_run,
            borrow_limit_bytes: task.borrow_limit_mb * MB,
            adaptive_minimum_steal_bytes: adaptive_minimum_steal_bytes.clone(),
            write_buffer_cap_bytes: write_buffer_cap_bytes.clone(),
            total_size,
            origin_memory: engine.origin_memory.clone(),
            shared_write_latency_ms: shared_write_latency_ms.clone(),
            ewma_connection_rtt_ms: Cell::new(200.0),
            metrics: metrics.clone(),
            client: http_client.clone(),
            index_state: index_state.clone(),
            bucket: bucket.clone(),
            scaler: scaler.clone(),
            storage_config: engine.storage_config.clone(),
            h3_client: h3_client.clone(),
            request_headers: request_headers.clone(),
        };

        let handle = tokio::task::spawn_local(async move {
            let _ = worker.run().await;
        });
        handles.borrow_mut().push(WorkerSlot {
            control: worker_control,
            handle,
        });
    }

    let scaler_handles = handles.clone();
    let scaler_leased_connections = leased_connections.clone();
    let scaler_work_tx = work_tx.clone();
    let scaler_url = task.url.clone();
    let scaler_file_path = file_path.clone();
    let scaler_log_path = log_path.clone();
    let scaler_global_downloaded = global_downloaded.clone();
    let scaler_control = control.clone();
    let scaler_metrics = metrics.clone();
    let scaler_http_client = http_client.clone();
    let scaler_h3_client = h3_client.clone();
    let scaler_index_state = index_state.clone();
    let scaler_bucket = bucket.clone();
    let scaler_borrow_limit = task.borrow_limit_mb * MB;
    let scaler_adaptive_minimum_steal_bytes = adaptive_minimum_steal_bytes.clone();
    let scaler_dry_run = task.dry_run;
    let scaler_origin = origin.clone();
    let mut connection_id_counter = initial_connections as u32;
    let mut worker_history: HashMap<u32, (u64, u64)> = HashMap::new();
    let mut phi_ratio_recorded = false;

    let scaler_for_task = scaler.clone();
    let scaler_task = tokio::task::spawn_local(async move {
        let mut last_downloaded = scaler_global_downloaded.get();
        let mut heartbeat_ms_for_tick = scaler_for_task.config.borrow().heartbeat_ms.max(500);
        
        loop {
            tokio::time::sleep(Duration::from_millis(heartbeat_ms_for_tick)).await;
            if scaler_control.is_halted() || scaler_global_downloaded.get() >= total_size {
                break;
            }

            let live_worker_count = {
                let mut slots = scaler_handles.borrow_mut();
                slots.retain(|slot| !slot.handle.is_finished());
                slots.len()
            };
            let leased = scaler_leased_connections.get();
            if leased > live_worker_count {
                for _ in 0..(leased - live_worker_count) {
                    scaler_engine.release_connection();
                }
                scaler_leased_connections.set(live_worker_count);
            }
            scaler_for_task.n_active.set(live_worker_count);

            let current_downloaded = scaler_global_downloaded.get();
            let downloaded_in_tick = current_downloaded.saturating_sub(last_downloaded);
            last_downloaded = current_downloaded;

            let heartbeat_ms_prev = heartbeat_ms_for_tick;
            let interval_secs = heartbeat_ms_prev as f64 / 1000.0;
            let current_throughput = downloaded_in_tick as f64 / interval_secs;
            let (alpha, _cv) = update_scaler_signal_stats(&scaler_for_task, current_throughput);
            let skip_growth_sample = scaler_for_task.skip_growth_sample.replace(false);
            let new_ewma = if skip_growth_sample {
                SchedulerMetrics::add(&scaler_metrics.skipped_growth_samples, 1);
                scaler_for_task.ewma_throughput.get()
            } else {
                let ewma = scaler_for_task.ewma_throughput.get();
                let updated = if ewma == 0.0 {
                    current_throughput
                } else {
                    (ewma * (1.0 - alpha)) + (current_throughput * alpha)
                };
                scaler_for_task.ewma_throughput.set(updated);
                updated
            };
            let dominant_protocol =
                dominant_protocol_from_metrics(&scaler_metrics, scaler_for_task.last_protocol.get());
            let next_heartbeat_ms = compute_heartbeat_ms(&scaler_for_task);
            scaler_for_task.config.borrow_mut().heartbeat_ms = next_heartbeat_ms;
            heartbeat_ms_for_tick = next_heartbeat_ms;
            let bytes_per_heartbeat = new_ewma * interval_secs;
            let adaptive_min_steal_bytes = compute_protocol_aware_steal_floor_bytes(
                dominant_protocol,
                scaler_for_task.reuse_rate.get(),
                bytes_per_heartbeat,
            );
            scaler_adaptive_minimum_steal_bytes.set(adaptive_min_steal_bytes);
            update_reuse_health(
                &scaler_for_task,
                &scaler_url,
                &scaler_log_path,
                dominant_protocol,
            )
            .await;

            let n_active = scaler_for_task.n_active.get();
            let current_efficiency = if n_active > 0 {
                new_ewma / n_active as f64
            } else {
                0.0
            };
            let peak = scaler_for_task.peak_efficiency.get();
            if current_efficiency > peak {
                scaler_for_task.peak_efficiency.set(current_efficiency);
            }

            let slow_start = scaler_for_task.slow_start_remaining.get();
            if slow_start > 0 {
                scaler_for_task.slow_start_remaining.set(slow_start - 1);
                continue;
            }

            let config = scaler_for_task.config.borrow();
            let min_c = config.min_connections;
            let max_c = config.max_connections;
            let last_action = scaler_for_task.last_action.get();

            let weakest_connection = {
                let slots = scaler_handles.borrow();
                let mut weakest = None::<(u32, u64)>;
                let mut connection_speeds = Vec::<f64>::new();
                for slot in slots.iter() {
                    if slot.control.stop_requested.get() {
                        continue;
                    }
                    let current_total = slot.control.transferred_bytes.get();
                    let (prev_total, prev_delta) = worker_history
                        .get(&slot.control.connection_id)
                        .copied()
                        .unwrap_or((current_total, 0));
                    let current_delta = current_total.saturating_sub(prev_total);
                    worker_history.insert(slot.control.connection_id, (current_total, current_delta));
                    let score = prev_delta.saturating_add(current_delta);
                    if current_delta > 0 {
                        connection_speeds.push(current_delta as f64 / interval_secs);
                    }
                    if !matches!(slot.control.diagnostics.state(), WorkerState::Downloading) {
                        continue;
                    }
                    match weakest {
                        Some((_, best_score)) if best_score <= score => {}
                        _ => weakest = Some((slot.control.connection_id, score)),
                    }
                }
                if !phi_ratio_recorded {
                    if let Some(cv_connections) = compute_connection_cv(&connection_speeds) {
                        let next_phi_ratio = (1.0 + cv_connections).clamp(1.1, 2.5);
                        scaler_engine
                            .origin_phi_ratios
                            .borrow_mut()
                            .update_origin_ratio(scaler_origin.clone(), next_phi_ratio);
                        scaler_engine
                            .origin_memory
                            .borrow_mut()
                            .note_phi_ratio(&scaler_origin, next_phi_ratio);
                        phi_ratio_recorded = true;
                        log_phase_a_info(
                            &scaler_log_path,
                            &format!(
                                "phase_d: next_phi_ratio_updated cv_connections={:.3} max_ratio_for_next_download={:.3}",
                                cv_connections,
                                next_phi_ratio
                            ),
                        )
                        .await;
                    }
                }
                weakest.map(|(connection_id, _)| connection_id)
            };

            let mut did_add = false;
            let mut drop_connection_id = None::<u32>;

            log_phase_a_info(
                &scaler_log_path,
                &format!(
                    "heartbeat protocol={} throughput_bps={:.0} alpha={:.3} cv={:.3} n_active={} current_efficiency={:.0} peak_efficiency={:.0} reuse_rate={:.2} effective_add_threshold={:.2} slow_start_remaining={} adaptive_min_steal_bytes={} heartbeat_ms_prev={} heartbeat_ms_next={} http1_requests={} http2_requests={}",
                    dominant_protocol.as_str(),
                    new_ewma,
                    scaler_for_task.alpha.get(),
                    scaler_for_task.cv.get(),
                    n_active,
                    current_efficiency,
                    scaler_for_task.peak_efficiency.get(),
                    scaler_for_task.reuse_rate.get(),
                    scaler_for_task.effective_add_threshold.get(),
                    scaler_for_task.slow_start_remaining.get(),
                    adaptive_min_steal_bytes,
                    heartbeat_ms_prev,
                    next_heartbeat_ms,
                    scaler_metrics.http1_requests.get(),
                    scaler_metrics.http2_requests.get(),
                ),
            )
            .await;

            if n_active == 0 {
                let recover_target = min_c.max(1);
                let mut recovered = 0usize;
                while recovered < recover_target && scaler_engine.request_connection() {
                    let worker_control = WorkerControl::new(connection_id_counter);
                    worker_control.pending_growth_probe.set(true);
                    let worker = ConnectionWorker {
                        connection_id: connection_id_counter,
                        url: scaler_url.clone(),
                        origin: scaler_origin.clone(),
                        file_path: scaler_file_path.clone(),
                        log_path: scaler_log_path.clone(),
                        coordinator_tx: scaler_work_tx.clone(),
                        global_downloaded: scaler_global_downloaded.clone(),
                        control: scaler_control.clone(),
                        worker_control: worker_control.clone(),
                        dry_run: scaler_dry_run,
                        borrow_limit_bytes: scaler_borrow_limit,
                        adaptive_minimum_steal_bytes: scaler_adaptive_minimum_steal_bytes.clone(),
                        write_buffer_cap_bytes: write_buffer_cap_bytes.clone(),
                        total_size,
                        origin_memory: scaler_engine.origin_memory.clone(),
                        shared_write_latency_ms: shared_write_latency_ms.clone(),
                        ewma_connection_rtt_ms: Cell::new(200.0),
                        metrics: scaler_metrics.clone(),
                        client: scaler_http_client.clone(),
                        index_state: scaler_index_state.clone(),
                        bucket: scaler_bucket.clone(),
                        scaler: scaler_for_task.clone(),
                        storage_config: scaler_engine.storage_config.clone(),
                        h3_client: scaler_h3_client.clone(),
                        request_headers: request_headers.clone(),
                    };
                    connection_id_counter += 1;
                    let handle = tokio::task::spawn_local(async move {
                        let _ = worker.run().await;
                    });
                    scaler_handles.borrow_mut().push(WorkerSlot {
                        control: worker_control,
                        handle,
                    });
                    recovered += 1;
                    scaler_leased_connections.set(scaler_leased_connections.get() + 1);
                    record_protocol_scale_metric(
                        &scaler_metrics,
                        dominant_protocol,
                        ScalerAction::Grow,
                        false,
                    );
                }
                if recovered > 0 {
                    scaler_for_task.n_active.set(recovered);
                    scaler_for_task.last_action.set(ScalerAction::Grow);
                    scaler_for_task
                        .slow_start_remaining
                        .set(compute_slow_start_heartbeats(&scaler_for_task, dominant_protocol, false));
                    record_max_active_connections(&scaler_metrics, recovered);
                    log_phase_a_info(
                        &scaler_log_path,
                        &format!(
                            "scale_recover recovered={} target={} slow_start_remaining={}",
                            recovered,
                            recover_target,
                            scaler_for_task.slow_start_remaining.get(),
                        ),
                    )
                    .await;
                }
                continue;
            }

            if last_action == ScalerAction::Grow {
                let prev = scaler_for_task.throughput_before_add.get();
                if prev > 0.0 {
                    let marginal_gain = (new_ewma - prev) / prev;
                    if marginal_gain < -0.03 {
                        if n_active > min_c {
                            drop_connection_id = weakest_connection;
                        }
                        scaler_for_task.last_action.set(ScalerAction::Hold);
                    } else if marginal_gain >= scaler_for_task.effective_add_threshold.get() {
                        scaler_for_task.last_action.set(ScalerAction::Hold);
                    }
                }
            } else if current_efficiency < 0.85 * scaler_for_task.peak_efficiency.get() && n_active > min_c {
                drop_connection_id = weakest_connection;
            } else if n_active < max_c && !scaler_for_task.h2_stream_saturated.get() {
                if scaler_engine.request_connection() {
                    scaler_for_task.throughput_before_add.set(new_ewma);
                    did_add = true;
                let is_stream_add = dominant_protocol == ProtocolFamily::Http2
                    && scaler_for_task.reuse_rate.get() > 0.70
                    && scaler_for_task.reused_rtt_samples.get() >= 2;
                scaler_for_task.last_add_was_stream.set(is_stream_add);
                if is_stream_add {
                    scaler_for_task.h2_stream_count.set(scaler_for_task.h2_stream_count.get() + 1);
                }
                // H2 stream saturation detection: if we've added 3+ stream workers
                // recently without throughput improvement, flag as saturated
                if is_stream_add && scaler_for_task.h2_stream_count.get() >= 3 {
                    let prev_throughput = scaler_for_task.throughput_before_add.get();
                    if prev_throughput > 0.0 && new_ewma < prev_throughput * 1.05 {
                        if !scaler_for_task.h2_stream_saturated.get() {
                            log_phase_a_info(
                                &scaler_log_path,
                                &format!(
                                    "h2_stream_saturation_detected stream_count={} throughput_bps={:.0} prev_throughput_bps={:.0}",
                                    scaler_for_task.h2_stream_count.get(),
                                    new_ewma,
                                    prev_throughput,
                                ),
                            ).await;
                            scaler_for_task.h2_stream_saturated.set(true);
                            SchedulerMetrics::add(&scaler_metrics.h2_stream_saturated_count, 1);
                        }
                    } else {
                        // Throughput improved — not saturated
                        scaler_for_task.h2_stream_saturated.set(false);
                    }
                }
                scaler_for_task
                    .slow_start_remaining
                    .set(compute_slow_start_heartbeats(&scaler_for_task, dominant_protocol, is_stream_add));
                scaler_for_task.last_action.set(ScalerAction::Grow);
                scaler_leased_connections.set(scaler_leased_connections.get() + 1);
                record_protocol_scale_metric(
                    &scaler_metrics,
                    dominant_protocol,
                    ScalerAction::Grow,
                    is_stream_add,
                );
                    record_max_active_connections(&scaler_metrics, n_active + 1);
                }
            }

            if let Some(connection_id) = drop_connection_id {
                let mut dropped = false;
                for slot in scaler_handles.borrow().iter() {
                    if slot.control.connection_id != connection_id || slot.control.stop_requested.get() {
                        continue;
                    }
                    slot.control.stop_requested.set(true);
                    dropped = true;
                    break;
                }
                if dropped {
                    scaler_for_task.n_active.set(n_active - 1);
                    scaler_engine.release_connection();
                    scaler_leased_connections
                        .set(scaler_leased_connections.get().saturating_sub(1));
                    scaler_for_task.last_action.set(ScalerAction::Shrink);
                record_protocol_scale_metric(
                    &scaler_metrics,
                    dominant_protocol,
                    ScalerAction::Shrink,
                    false,
                );
                    log_phase_a_info(
                        &scaler_log_path,
                        &format!("scale_drop connection_id={} n_active={}", connection_id, scaler_for_task.n_active.get()),
                    )
                    .await;
                }
            }

            if did_add {
                let worker_control = WorkerControl::new(connection_id_counter);
                worker_control.pending_growth_probe.set(true);
                let worker = ConnectionWorker {
                    connection_id: connection_id_counter,
                    url: scaler_url.clone(),
                    origin: scaler_origin.clone(),
                    file_path: scaler_file_path.clone(),
                    log_path: scaler_log_path.clone(),
                    coordinator_tx: scaler_work_tx.clone(),
                    global_downloaded: scaler_global_downloaded.clone(),
                    control: scaler_control.clone(),
                    worker_control: worker_control.clone(),
                    dry_run: scaler_dry_run,
                    borrow_limit_bytes: scaler_borrow_limit,
                    adaptive_minimum_steal_bytes: scaler_adaptive_minimum_steal_bytes.clone(),
                    write_buffer_cap_bytes: write_buffer_cap_bytes.clone(),
                    total_size,
                    origin_memory: scaler_engine.origin_memory.clone(),
                    shared_write_latency_ms: shared_write_latency_ms.clone(),
                    ewma_connection_rtt_ms: Cell::new(200.0),
                    metrics: scaler_metrics.clone(),
                    client: scaler_http_client.clone(),
                    index_state: scaler_index_state.clone(),
                    bucket: scaler_bucket.clone(),
                    scaler: scaler_for_task.clone(),
                    storage_config: scaler_engine.storage_config.clone(),
                    h3_client: scaler_h3_client.clone(),
                    request_headers: request_headers.clone(),
                };
                connection_id_counter += 1;
                let handle = tokio::task::spawn_local(async move {
                    let _ = worker.run().await;
                });
                scaler_handles.borrow_mut().push(WorkerSlot {
                    control: worker_control,
                    handle,
                });
                scaler_for_task.n_active.set(n_active + 1);
                log_phase_a_info(
                    &scaler_log_path,
                    &format!(
                        "scale_add connection_id={} n_active={} slow_start_remaining={}",
                        connection_id_counter - 1,
                        scaler_for_task.n_active.get(),
                        scaler_for_task.slow_start_remaining.get(),
                    ),
                )
                .await;
            }
        }
    });

    drop(work_tx);

    let progress_task_id = task.id;
    let progress_tx = event_tx.clone();
    let progress_counter = global_downloaded.clone();
    let progress_control = control.clone();
    let progress_total = total_size;
    let progress_handles = handles.clone();
    let progress_handle = tokio::task::spawn_local(async move {
        let mut last_downloaded = progress_counter.get();
        let mut last_tick = Instant::now();
        loop {
            if progress_control.is_halted() && progress_counter.get() < progress_total {
                break;
            }

            tokio::time::sleep(Duration::from_millis(400)).await;
            let current_downloaded = progress_counter.get();
            let elapsed = last_tick.elapsed().as_secs_f64();
            let speed = if elapsed > 0.0 {
                (current_downloaded.saturating_sub(last_downloaded)) as f64 / elapsed
            } else {
                0.0
            };

            let _ = progress_tx
                .send(EngineEvent::Progress(progress_task_id, current_downloaded, speed))
                .await;
            let worker_snapshots = {
                progress_handles
                    .borrow_mut()
                    .retain(|slot| !slot.handle.is_finished());
                let slots = progress_handles.borrow();
                slots.iter()
                    .map(|slot| {
                        slot.control
                            .diagnostics
                            .snapshot(slot.control.transferred_bytes.get())
                    })
                    .collect::<Vec<_>>()
            };
            let _ = progress_tx
                .send(EngineEvent::Workers(progress_task_id, worker_snapshots))
                .await;

            last_downloaded = current_downloaded;
            last_tick = Instant::now();

            if current_downloaded >= progress_total {
                break;
            }
        }
    });

    coordinator.run(work_rx, control.clone()).await;

    scaler_task.abort();
    {
        let final_handles = handles.borrow();
        for slot in final_handles.iter() {
            slot.control.stop_requested.set(true);
        }
    }
    let drained_handles: Vec<JoinHandle<()>> = {
        let mut final_handles = handles.borrow_mut();
        final_handles.drain(..).map(|slot| slot.handle).collect()
    };
    for handle in drained_handles {
        let _ = handle.await;
    }

    let remaining_leases = leased_connections.replace(0);
    for _ in 0..remaining_leases {
        engine.release_connection();
    }

    let _ = progress_handle.await;

    metrics
        .adaptive_heartbeat_ms_final
        .set(control.scaler_config().borrow().heartbeat_ms.max(500));
    metrics
        .adaptive_refill_interval_ms_final
        .set(bucket.refill_interval_ms.get().max(10));
    let dominant_protocol = dominant_protocol_from_metrics(&metrics, scaler.last_protocol.get());
    if dominant_protocol == ProtocolFamily::Http2 && metrics.http2_requests.get() > 0 {
        let learned_h2_tuning = learn_http2_client_tuning(
            max_connections,
            metrics.max_active_connections_observed.get().max(1) as usize,
            scaler.ewma_rtt_ms.get(),
            scaler.ewma_throughput.get(),
        );
        engine
            .origin_h2_tunings
            .borrow_mut()
            .update_origin_tuning(origin.clone(), learned_h2_tuning);
        engine
            .origin_memory
            .borrow_mut()
            .note_h2_tuning(&origin, learned_h2_tuning);
        coordinator.log(&format!(
            "phase_h2_tuning_update origin={} observed_rtt_ms={:.1} ewma_throughput_bps={:.0} max_active_connections_observed={} next_initial_h2_stream_window_bytes={} next_initial_h2_connection_window_bytes={} next_initial_h2_max_send_buffer_bytes={}",
            origin,
            scaler.ewma_rtt_ms.get(),
            scaler.ewma_throughput.get(),
            metrics.max_active_connections_observed.get().max(1),
            learned_h2_tuning.http2_stream_window_bytes,
            learned_h2_tuning.http2_connection_window_bytes,
            learned_h2_tuning.http2_max_send_buffer_bytes,
        ));
    }
    // Phase 3c: Preserve AltSvc Http3 hint — don't overwrite with actual protocol
    {
        let mut om = engine.origin_memory.borrow_mut();
        let current_hint = om.protocol_hint_for_origin(&origin);
        if current_hint != Some(ProtocolFamily::Http3) {
            om.note_protocol(&origin, dominant_protocol);
        }
    }
    engine
        .origin_memory
        .borrow_mut()
        .note_reuse_metrics(&origin, scaler.reuse_rate.get(), scaler.ewma_handshake_ms.get());

    // Check if the download was aborted due to a challenge/interstitial page.
    if let Some(challenge_reason) = control.challenge_reason.borrow().clone() {
        let _ = event_tx
            .send(EngineEvent::StatusChanged(
                task.id,
                DownloadStatus::Error(challenge_reason),
            ))
            .await;
        return Ok(());
    }

    // Infer session requirements from what was actually used in this download.
    // This populates origin memory so future downloads can warn or suggest flags.
    if let Some(ref ctx) = task.request_context {
        if ctx.cookies.as_ref().map_or(false, |c| !c.is_empty()) {
            engine
                .origin_memory
                .borrow_mut()
                .note_session_requirement(&origin, SessionRequirementField::Cookies, true);
        }
        if ctx.auth.is_some() {
            engine
                .origin_memory
                .borrow_mut()
                .note_session_requirement(&origin, SessionRequirementField::Auth, true);
        }
        if ctx.referer.is_some() {
            engine
                .origin_memory
                .borrow_mut()
                .note_session_requirement(&origin, SessionRequirementField::Referer, true);
        }
    }

    // Log session memory info (Phase 3: session-aware diagnostics)
    if let Some(session_info) = engine.origin_memory.borrow().session_info_for_origin(&origin) {
        coordinator.log(&format!(
            "session_memory origin={} cookies_used={:?} auth_used={:?} referer_used={:?} challenge_detected={:?} challenge_kind={} saw_rate_limit={}",
            origin,
            session_info.cookies_used,
            session_info.auth_used,
            session_info.referer_used,
            session_info.challenge_detected,
            session_info.challenge_kind.as_deref().unwrap_or("none"),
            session_info.saw_rate_limit,
        ));
    }

    coordinator.log_summary(total_size);
    coordinator.log(&format!(
        "phase_protocol_summary http_mode={} dominant_protocol={} http1_requests={} http2_requests={} http_other_requests={} http1_reused_requests={} http1_fresh_requests={} http2_reused_requests={} http2_fresh_requests={} http1_scale_adds={} http1_scale_drops={} http2_scale_adds={} http2_scale_drops={} initial_h2_stream_window_bytes={} initial_h2_connection_window_bytes={} initial_h2_max_send_buffer_bytes={} configured_max_connections={} max_active_connections_observed={} h2_tuning_source={}",
        task.http_mode.as_str(),
        dominant_protocol.as_str(),
        metrics.http1_requests.get(),
        metrics.http2_requests.get(),
        metrics.http_other_requests.get(),
        metrics.http1_reused_requests.get(),
        metrics.http1_fresh_requests.get(),
        metrics.http2_reused_requests.get(),
        metrics.http2_fresh_requests.get(),
        metrics.http1_scale_adds.get(),
        metrics.http1_scale_drops.get(),
        metrics.http2_scale_adds.get(),
        metrics.http2_scale_drops.get(),
        client_tuning.http2_stream_window_bytes,
        client_tuning.http2_connection_window_bytes,
        client_tuning.http2_max_send_buffer_bytes,
        client_tuning.expected_concurrency,
        metrics.max_active_connections_observed.get(),
        client_tuning.source.as_str(),
    ));
    coordinator.log(&format!(
        "origin_memory_summary origin={} protocol_hint={} memory_hit={} reuse_rate={:.2} handshake_ms={:.1}",
        origin,
        protocol_hint.as_str(),
        origin_memory_hit,
        scaler.reuse_rate.get(),
        scaler.ewma_handshake_ms.get(),
    ));
    coordinator.log(&format!(
        "phase_a_summary alpha={:.3} cv={:.3} ewma_rtt_ms={:.1} ewma_handshake_ms={:.1} reuse_rate={:.2} effective_add_threshold={:.2} slow_start_remaining={} reused_rtt_samples={}",
        scaler.alpha.get(),
        scaler.cv.get(),
        scaler.ewma_rtt_ms.get(),
        scaler.ewma_handshake_ms.get(),
        scaler.reuse_rate.get(),
        scaler.effective_add_threshold.get(),
        scaler.slow_start_remaining.get(),
        scaler.reused_rtt_samples.get(),
    ));
    coordinator.log(&format!(
        "phase_c_summary heartbeat_ms={} refill_interval_ms={}",
        control.scaler_config().borrow().heartbeat_ms.max(500),
        bucket.refill_interval_ms.get().max(10),
    ));
    coordinator.log(&format!(
        "phase_d_summary effective_connection_budget={} configured_connection_budget={} next_phi_ratio={:.3}",
        engine.effective_connection_budget.get(),
        engine.configured_connection_budget.get(),
        engine
            .origin_phi_ratios
            .borrow()
            .current_ratio(&origin)
            .unwrap_or(INITIAL_PHI_MAX_RATIO),
    ));
    coordinator.log(&format!(
        "phase_e_summary write_buffer_cap_bytes={} max_write_buffer_target_bytes={} max_ewma_write_latency_ms={:.1}",
        engine.write_buffer_cap_bytes.get(),
        metrics.max_write_buffer_target_bytes.get(),
        metrics.max_ewma_write_latency_x10.get() as f64 / 10.0,
    ));

    if task.total_size > 0 && global_downloaded.get() >= task.total_size {
        let _ = std::fs::remove_file(metadata_path(&task));
        let _ = event_tx
            .send(EngineEvent::Progress(task.id, global_downloaded.get(), 0.0))
            .await;
        let _ = event_tx
            .send(EngineEvent::StatusChanged(task.id, DownloadStatus::Completed))
            .await;
    } else {
    match control.halt_mode() {
        HaltMode::Running => {
            let _ = std::fs::remove_file(metadata_path(&task));
            let _ = event_tx
                .send(EngineEvent::Progress(
                    task.id,
                    global_downloaded.get(),
                    0.0,
                ))
                .await;
            let _ = event_tx
                .send(EngineEvent::StatusChanged(task.id, DownloadStatus::Completed))
                .await;
        }
        halt_mode => {
            task.downloaded_size = global_downloaded.get();
            let snapshot = TaskSnapshot {
                task,
                coordinator: coordinator.snapshot(),
            };
            let _ = cmd_tx
                .send(EngineCommand::RuntimeStopped(snapshot, halt_mode))
                .await;
        }
    }
    }

    Ok(())
}
