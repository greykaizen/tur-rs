use std::collections::HashMap;
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
use http::header::{ACCEPT, ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, LOCATION, RANGE, USER_AGENT};
use http::{Method, Request, Uri, Version};
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

use crate::storage;

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

type DownloadHttpClient = HyperClient<HttpsConnector<crate::connector::TunedConnector>, Empty<Bytes>>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DownloadStatus {
    Queued,
    Downloading,
    Paused,
    Stopped,
    Completed,
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadTask {
    pub id: Uuid,
    pub url: String,
    pub filename: String,
    pub dir: PathBuf,
    pub total_size: u64,
    pub downloaded_size: u64,
    pub connections: usize,
    pub status: DownloadStatus,
    pub speed: f64,
    pub dry_run: bool,
    pub dry_run_size_mb: Option<u64>,
    pub borrow_limit_mb: u64,
    pub min_connections: usize,
    pub max_connections: usize,
    pub per_download_bandwidth_limit_bps: u64,
    pub schedule_mode: ScheduleMode,
    pub http_mode: HttpMode,
    pub log_root: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScheduleMode {
    Fib,
    FibAdaptive,
    Equal,
}

impl ScheduleMode {
    pub fn parse(input: &str) -> Result<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "fib" => Ok(Self::Fib),
            "fib-adaptive" | "fib_adaptive" | "adaptive-fib" | "adaptive_fib" => Ok(Self::FibAdaptive),
            "equal" => Ok(Self::Equal),
            other => Err(anyhow!("unsupported schedule mode: {}", other)),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Fib => "fib",
            Self::FibAdaptive => "fib-adaptive",
            Self::Equal => "equal",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpMode {
    Auto,
    Http1,
    Http2,
}

impl HttpMode {
    pub fn parse(input: &str) -> Result<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "http1" | "http/1.1" | "h1" => Ok(Self::Http1),
            "http2" | "http/2" | "h2" => Ok(Self::Http2),
            other => Err(anyhow!("unsupported http mode: {}", other)),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Http1 => "http1",
            Self::Http2 => "http2",
        }
    }
}

#[derive(Debug)]
pub struct ActiveRange {
    pub id: u64,
    pub label_start_mb: u64,
    pub label_end_mb: u64,
    pub byte_start: u64,
    pub assigned_to: Cell<u32>,
    pub cursor: Cell<u64>,
    pub end: Cell<u64>,
    pub parent_range_id: Option<u64>,
    pub status: Cell<u8>,
    pub last_sample_cursor: Cell<u64>,
    pub last_sample_at_ms: Cell<u64>,
    pub recent_speed_bps: Cell<u64>,
}

#[derive(Debug)]
pub struct WorkRequest {
    pub connection_id: u32,
    pub tx: oneshot::Sender<Option<Rc<ActiveRange>>>,
}

#[derive(Debug, Clone)]
pub enum EngineEvent {
    Progress(Uuid, u64, f64),
    StatusChanged(Uuid, DownloadStatus),
    TotalSize(Uuid, u64),
}

pub enum EngineCommand {
    Add(DownloadTask),
    Resume(Uuid),
    Stop(Uuid),
    Cancel(Uuid),
    UpdateScaling(Uuid, ScalerConfig),
    RuntimeStopped(TaskSnapshot, HaltMode),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HaltMode {
    Running,
    PauseMemory,
    PersistToDisk,
}

#[derive(Debug)]
struct RuntimeControl {
    halt_mode: Cell<HaltMode>,
    cancel_flag: Cell<bool>,
    scaler_config: Rc<RefCell<ScalerConfig>>,
}

impl RuntimeControl {
    fn new(scaler_config: ScalerConfig) -> Self {
        Self {
            halt_mode: Cell::new(HaltMode::Running),
            cancel_flag: Cell::new(false),
            scaler_config: Rc::new(RefCell::new(scaler_config)),
        }
    }

    fn halt_mode(&self) -> HaltMode {
        self.halt_mode.get()
    }

    fn request_pause(&self) {
        self.halt_mode.set(HaltMode::PauseMemory);
        self.cancel_flag.set(true);
    }

    fn request_persist(&self) {
        self.halt_mode.set(HaltMode::PersistToDisk);
        self.cancel_flag.set(true);
    }

    fn is_halted(&self) -> bool {
        self.halt_mode() != HaltMode::Running || self.cancel_flag.get()
    }

    fn scaler_config(&self) -> Rc<RefCell<ScalerConfig>> {
        self.scaler_config.clone()
    }
}

enum PendingLaunch {
    Fresh(DownloadTask),
    Resume(TaskSnapshot),
}

#[derive(Debug, Default)]
struct SchedulerMetrics {
    direct_assignments: Cell<u64>,
    borrow_assignments: Cell<u64>,
    bytes_borrowed: Cell<u64>,
    straggler_splits: Cell<u64>,
    tail_splits: Cell<u64>,
    work_requests: Cell<u64>,
    request_wait_ms: Cell<u64>,
    prefetch_requests: Cell<u64>,
    prefetch_ready: Cell<u64>,
    prefetch_hits: Cell<u64>,
    http_requests: Cell<u64>,
    http1_requests: Cell<u64>,
    http2_requests: Cell<u64>,
    http_other_requests: Cell<u64>,
    http_setup_ms: Cell<u64>,
    http_ttfb_ms: Cell<u64>,
    http_stream_ms: Cell<u64>,
    file_write_ms: Cell<u64>,
    completed_ranges: Cell<u64>,
    retry_attempts: Cell<u64>,
    retry_wait_ms: Cell<u64>,
    startup_workers: Cell<u64>,
    startup_open_file_ms: Cell<u64>,
    startup_first_assignment_wait_ms: Cell<u64>,
    startup_first_request_setup_ms: Cell<u64>,
    startup_first_byte_ms: Cell<u64>,
    startup_total_to_first_byte_ms: Cell<u64>,
    max_active_connections_observed: Cell<u64>,
    reused_requests: Cell<u64>,
    fresh_requests: Cell<u64>,
    http1_reused_requests: Cell<u64>,
    http1_fresh_requests: Cell<u64>,
    http2_reused_requests: Cell<u64>,
    http2_fresh_requests: Cell<u64>,
    fresh_handshake_ms: Cell<u64>,
    http1_fresh_handshake_ms: Cell<u64>,
    http2_fresh_handshake_ms: Cell<u64>,
    skipped_growth_samples: Cell<u64>,
    http1_scale_adds: Cell<u64>,
    http1_scale_drops: Cell<u64>,
    http2_scale_adds: Cell<u64>,
    http2_scale_drops: Cell<u64>,
    adaptive_min_steal_bytes_final: Cell<u64>,
    adaptive_heartbeat_ms_final: Cell<u64>,
    adaptive_refill_interval_ms_final: Cell<u64>,
    max_prefetch_trigger_bytes: Cell<u64>,
    max_write_buffer_target_bytes: Cell<u64>,
    max_ewma_write_latency_x10: Cell<u64>,
}

impl SchedulerMetrics {
    fn summary_line(&self) -> String {
        format!(
            "metrics direct_assignments={} borrow_assignments={} bytes_borrowed={} straggler_splits={} tail_splits={} work_requests={} request_wait_ms={} prefetch_requests={} prefetch_ready={} prefetch_hits={} http_requests={} http1_requests={} http2_requests={} http_other_requests={} http_setup_ms={} http_ttfb_ms={} http_stream_ms={} file_write_ms={} completed_ranges={} retry_attempts={} retry_wait_ms={} startup_workers={} startup_open_file_ms={} startup_first_assignment_wait_ms={} startup_first_request_setup_ms={} startup_first_byte_ms={} startup_total_to_first_byte_ms={} max_active_connections_observed={} reused_requests={} fresh_requests={} http1_reused_requests={} http1_fresh_requests={} http2_reused_requests={} http2_fresh_requests={} fresh_handshake_ms={} http1_fresh_handshake_ms={} http2_fresh_handshake_ms={} skipped_growth_samples={} http1_scale_adds={} http1_scale_drops={} http2_scale_adds={} http2_scale_drops={} adaptive_min_steal_bytes_final={} adaptive_heartbeat_ms_final={} adaptive_refill_interval_ms_final={} max_prefetch_trigger_bytes={} max_write_buffer_target_bytes={} max_ewma_write_latency_ms={:.1}",
            self.direct_assignments.get(),
            self.borrow_assignments.get(),
            self.bytes_borrowed.get(),
            self.straggler_splits.get(),
            self.tail_splits.get(),
            self.work_requests.get(),
            self.request_wait_ms.get(),
            self.prefetch_requests.get(),
            self.prefetch_ready.get(),
            self.prefetch_hits.get(),
            self.http_requests.get(),
            self.http1_requests.get(),
            self.http2_requests.get(),
            self.http_other_requests.get(),
            self.http_setup_ms.get(),
            self.http_ttfb_ms.get(),
            self.http_stream_ms.get(),
            self.file_write_ms.get(),
            self.completed_ranges.get(),
            self.retry_attempts.get(),
            self.retry_wait_ms.get(),
            self.startup_workers.get(),
            self.startup_open_file_ms.get(),
            self.startup_first_assignment_wait_ms.get(),
            self.startup_first_request_setup_ms.get(),
            self.startup_first_byte_ms.get(),
            self.startup_total_to_first_byte_ms.get(),
            self.max_active_connections_observed.get(),
            self.reused_requests.get(),
            self.fresh_requests.get(),
            self.http1_reused_requests.get(),
            self.http1_fresh_requests.get(),
            self.http2_reused_requests.get(),
            self.http2_fresh_requests.get(),
            self.fresh_handshake_ms.get(),
            self.http1_fresh_handshake_ms.get(),
            self.http2_fresh_handshake_ms.get(),
            self.skipped_growth_samples.get(),
            self.http1_scale_adds.get(),
            self.http1_scale_drops.get(),
            self.http2_scale_adds.get(),
            self.http2_scale_drops.get(),
            self.adaptive_min_steal_bytes_final.get(),
            self.adaptive_heartbeat_ms_final.get(),
            self.adaptive_refill_interval_ms_final.get(),
            self.max_prefetch_trigger_bytes.get(),
            self.max_write_buffer_target_bytes.get(),
            self.max_ewma_write_latency_x10.get() as f64 / 10.0,
        )
    }

    fn add(cell: &Cell<u64>, value: u64) {
        cell.set(cell.get().saturating_add(value));
    }

    fn update_max(cell: &Cell<u64>, value: u64) {
        if value > cell.get() {
            cell.set(value);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ScalerConfig {
    pub min_connections: usize,
    pub max_connections: usize,
    pub heartbeat_ms: u64,
}

impl Default for ScalerConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 16,
            heartbeat_ms: 2000,
        }
    }
}

pub struct TokenBucket {
    pub quota_bytes_per_sec: Cell<u64>,
    pub tokens: Cell<i64>,
    pub refill_interval_ms: Cell<u64>,
}

impl TokenBucket {
    pub fn new() -> Self {
        Self {
            quota_bytes_per_sec: Cell::new(0),
            tokens: Cell::new(0),
            refill_interval_ms: Cell::new(100),
        }
    }

    pub fn refill(&self, interval_ms: u64) {
        let quota = self.quota_bytes_per_sec.get();
        if quota == 0 {
            self.tokens.set(i64::MAX / 2);
            return;
        }
        let add = (quota * interval_ms / 1000) as i64;
        let cap = (quota * 2) as i64;
        self.tokens.set((self.tokens.get() + add).min(cap));
    }

    pub fn consume(&self, bytes: usize) -> bool {
        let quota = self.quota_bytes_per_sec.get();
        if quota == 0 {
            return true;
        }
        let remaining = self.tokens.get() - bytes as i64;
        self.tokens.set(remaining);
        remaining >= 0
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum ScalerAction {
    Grow,
    Shrink,
    Hold,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
enum ProtocolFamily {
    Http1,
    Http2,
    Other,
}

impl ProtocolFamily {
    fn as_str(self) -> &'static str {
        match self {
            Self::Http1 => "http1",
            Self::Http2 => "http2",
            Self::Other => "other",
        }
    }
}

pub struct Scaler {
    pub ewma_throughput: Cell<f64>,
    pub peak_efficiency: Cell<f64>,
    pub throughput_before_add: Cell<f64>,
    pub n_active: Cell<usize>,
    pub last_action: Cell<ScalerAction>,
    pub slow_start_remaining: Cell<u32>,
    pub config: Rc<RefCell<ScalerConfig>>,
    pub sample_ring: RefCell<[f64; 10]>,
    pub sample_head: Cell<usize>,
    pub sample_count: Cell<usize>,
    pub alpha: Cell<f64>,
    pub cv: Cell<f64>,
    pub ewma_rtt_ms: Cell<f64>,
    pub ewma_handshake_ms: Cell<f64>,
    pub reused_count: Cell<u64>,
    pub total_request_count: Cell<u64>,
    pub reuse_rate: Cell<f64>,
    pub last_reuse_reset: Cell<Instant>,
    pub effective_add_threshold: Cell<f64>,
    pub reused_rtt_samples: Cell<u64>,
    pub skip_growth_sample: Cell<bool>,
    pub reuse_health_low: Cell<bool>,
    last_protocol: Cell<ProtocolFamily>,
}

impl Scaler {
    pub fn new(config: ScalerConfig) -> Rc<Self> {
        Self::from_config_handle(Rc::new(RefCell::new(config)))
    }

    pub fn from_config_handle(config: Rc<RefCell<ScalerConfig>>) -> Rc<Self> {
        Rc::new(Self {
            ewma_throughput: Cell::new(0.0),
            peak_efficiency: Cell::new(0.0),
            throughput_before_add: Cell::new(0.0),
            n_active: Cell::new(1),
            last_action: Cell::new(ScalerAction::Hold),
            slow_start_remaining: Cell::new(3),
            config,
            sample_ring: RefCell::new([0.0; 10]),
            sample_head: Cell::new(0),
            sample_count: Cell::new(0),
            alpha: Cell::new(0.3),
            cv: Cell::new(0.0),
            ewma_rtt_ms: Cell::new(200.0),
            ewma_handshake_ms: Cell::new(50.0),
            reused_count: Cell::new(0),
            total_request_count: Cell::new(0),
            reuse_rate: Cell::new(1.0),
            last_reuse_reset: Cell::new(Instant::now()),
            effective_add_threshold: Cell::new(0.05),
            reused_rtt_samples: Cell::new(0),
            skip_growth_sample: Cell::new(false),
            reuse_health_low: Cell::new(false),
            last_protocol: Cell::new(ProtocolFamily::Other),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum H2TuningSource {
    Default,
    LearnedOrigin,
    OriginMemoryHint,
}

impl H2TuningSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::LearnedOrigin => "learned-origin",
            Self::OriginMemoryHint => "origin-memory-hint",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct ClientTuning {
    expected_concurrency: usize,
    http2_stream_window_bytes: u32,
    http2_connection_window_bytes: u32,
    http2_max_send_buffer_bytes: usize,
    source: H2TuningSource,
}

#[derive(Debug, Clone, Copy)]
struct OriginH2TuningEntry {
    tuning: ClientTuning,
    last_used_tick: u64,
}

#[derive(Debug, Default)]
struct OriginH2TuningStore {
    entries: HashMap<String, OriginH2TuningEntry>,
    usage_tick: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedOriginProfile {
    phi_ratio: Option<f64>,
    h2_tuning: Option<ClientTuning>,
    protocol_hint: Option<ProtocolFamily>,
    reuse_rate: Option<f64>,
    handshake_ms: Option<f64>,
    supports_ranges: Option<bool>,
    content_length_reliable: Option<bool>,
    saw_rate_limit: bool,
    last_used_tick: u64,
}

#[derive(Debug, Default)]
struct OriginMemoryStore {
    entries: HashMap<String, PersistedOriginProfile>,
    usage_tick: u64,
    enabled: bool,
}

impl OriginMemoryStore {
    fn load_enabled(enabled: bool) -> Self {
        if !enabled {
            return Self {
                entries: HashMap::new(),
                usage_tick: 0,
                enabled: false,
            };
        }
        let path = origin_memory_path();
        let Ok(bytes) = fs::read(path) else {
            return Self {
                entries: HashMap::new(),
                usage_tick: 0,
                enabled: true,
            };
        };
        let Ok(entries) = bincode::deserialize::<HashMap<String, PersistedOriginProfile>>(&bytes) else {
            return Self {
                entries: HashMap::new(),
                usage_tick: 0,
                enabled: true,
            };
        };
        let usage_tick = entries
            .values()
            .map(|profile| profile.last_used_tick)
            .max()
            .unwrap_or(0);
        Self {
            entries,
            usage_tick,
            enabled: true,
        }
    }

    fn persist(&self) {
        if !self.enabled {
            return;
        }
        let path = origin_memory_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        if let Ok(bytes) = bincode::serialize(&self.entries) {
            let _ = fs::write(path, bytes);
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.usage_tick = self.usage_tick.saturating_add(1);
        self.usage_tick
    }

    fn touch_profile(&mut self, origin: &str) -> &mut PersistedOriginProfile {
        let tick = self.next_tick();
        let profile = self.entries.entry(origin.to_string()).or_insert(PersistedOriginProfile {
            phi_ratio: None,
            h2_tuning: None,
            protocol_hint: None,
            reuse_rate: None,
            handshake_ms: None,
            supports_ranges: None,
            content_length_reliable: None,
            saw_rate_limit: false,
            last_used_tick: tick,
        });
        profile.last_used_tick = tick;
        profile
    }

    fn protocol_hint_for_origin(&mut self, origin: &str) -> Option<ProtocolFamily> {
        if !self.enabled {
            return None;
        }
        self.touch_profile(origin).protocol_hint
    }

    fn note_protocol(&mut self, origin: &str, protocol: ProtocolFamily) {
        if !self.enabled {
            return;
        }
        self.touch_profile(origin).protocol_hint = Some(protocol);
        self.prune_lru();
        self.persist();
    }

    fn note_phi_ratio(&mut self, origin: &str, ratio: f64) {
        if !self.enabled {
            return;
        }
        self.touch_profile(origin).phi_ratio = Some(ratio);
        self.prune_lru();
        self.persist();
    }

    fn note_h2_tuning(&mut self, origin: &str, tuning: ClientTuning) {
        if !self.enabled {
            return;
        }
        self.touch_profile(origin).h2_tuning = Some(tuning);
        self.prune_lru();
        self.persist();
    }

    fn note_reuse_metrics(&mut self, origin: &str, reuse_rate: f64, handshake_ms: f64) {
        if !self.enabled {
            return;
        }
        let profile = self.touch_profile(origin);
        profile.reuse_rate = Some(reuse_rate);
        profile.handshake_ms = Some(handshake_ms);
        self.prune_lru();
        self.persist();
    }

    fn note_range_support(&mut self, origin: &str, supports_ranges: bool) {
        if !self.enabled {
            return;
        }
        self.touch_profile(origin).supports_ranges = Some(supports_ranges);
        self.prune_lru();
        self.persist();
    }

    fn note_content_length_reliable(&mut self, origin: &str, reliable: bool) {
        if !self.enabled {
            return;
        }
        self.touch_profile(origin).content_length_reliable = Some(reliable);
        self.prune_lru();
        self.persist();
    }

    fn note_rate_limit(&mut self, origin: &str) {
        if !self.enabled {
            return;
        }
        self.touch_profile(origin).saw_rate_limit = true;
        self.prune_lru();
        self.persist();
    }

    fn prune_lru(&mut self) {
        while self.entries.len() > ORIGIN_MEMORY_CAPACITY {
            let Some(lru_key) = self
                .entries
                .iter()
                .min_by_key(|(_, profile)| profile.last_used_tick)
                .map(|(origin, _)| origin.clone())
            else {
                break;
            };
            self.entries.remove(&lru_key);
        }
    }

    fn hydrate_phi_ratios(&self) -> OriginPhiRatioStore {
        let mut store = OriginPhiRatioStore::default();
        for (origin, profile) in &self.entries {
            if let Some(phi_ratio) = profile.phi_ratio {
                store.entries.insert(
                    origin.clone(),
                    OriginPhiRatioEntry {
                        ratio: phi_ratio,
                        last_used_tick: profile.last_used_tick,
                    },
                );
            }
        }
        store.usage_tick = self.usage_tick;
        store
    }

    fn hydrate_h2_tunings(&self) -> OriginH2TuningStore {
        let mut store = OriginH2TuningStore::default();
        for (origin, profile) in &self.entries {
            if let Some(mut tuning) = profile.h2_tuning {
                tuning.source = H2TuningSource::OriginMemoryHint;
                store.entries.insert(
                    origin.clone(),
                    OriginH2TuningEntry {
                        tuning,
                        last_used_tick: profile.last_used_tick,
                    },
                );
            }
        }
        store.usage_tick = self.usage_tick;
        store
    }

    fn memory_hit_for_origin(&self, origin: &str) -> bool {
        self.enabled && self.entries.contains_key(origin)
    }
}

impl OriginH2TuningStore {
    fn next_tick(&mut self) -> u64 {
        self.usage_tick = self.usage_tick.saturating_add(1);
        self.usage_tick
    }

    fn tuning_for_origin(&mut self, origin: &str) -> Option<ClientTuning> {
        let tick = self.next_tick();
        self.entries.get_mut(origin).map(|entry| {
            entry.last_used_tick = tick;
            entry.tuning
        })
    }

    fn update_origin_tuning(&mut self, origin: String, tuning: ClientTuning) {
        let tick = self.next_tick();
        self.entries.insert(
            origin,
            OriginH2TuningEntry {
                tuning,
                last_used_tick: tick,
            },
        );
        self.prune_lru();
    }

    #[cfg(test)]
    fn current_tuning(&self, origin: &str) -> Option<ClientTuning> {
        self.entries.get(origin).map(|entry| entry.tuning)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn prune_lru(&mut self) {
        while self.entries.len() > ORIGIN_PHI_RATIO_CAPACITY {
            let Some(lru_key) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used_tick)
                .map(|(origin, _)| origin.clone())
            else {
                break;
            };
            self.entries.remove(&lru_key);
        }
    }
}

pub struct DownloadHandle {
    pub id: Uuid,
    pub bucket: Rc<TokenBucket>,
    pub per_download_limit_bps: u64,
}

pub struct WorkerControl {
    pub connection_id: u32,
    pub stop_requested: Cell<bool>,
    pub transferred_bytes: Cell<u64>,
    pub pending_growth_probe: Cell<bool>,
}

impl WorkerControl {
    fn new(connection_id: u32) -> Rc<Self> {
        Rc::new(Self {
            connection_id,
            stop_requested: Cell::new(false),
            transferred_bytes: Cell::new(0),
            pending_growth_probe: Cell::new(false),
        })
    }
}

struct WorkerSlot {
    control: Rc<WorkerControl>,
    handle: JoinHandle<()>,
}

#[derive(Debug, Clone, Copy)]
struct OriginPhiRatioEntry {
    ratio: f64,
    last_used_tick: u64,
}

#[derive(Debug, Default)]
struct OriginPhiRatioStore {
    entries: HashMap<String, OriginPhiRatioEntry>,
    usage_tick: u64,
}

impl OriginPhiRatioStore {
    fn next_tick(&mut self) -> u64 {
        self.usage_tick = self.usage_tick.saturating_add(1);
        self.usage_tick
    }

    fn ratio_for_origin(&mut self, origin: &str) -> f64 {
        let tick = self.next_tick();
        if let Some(entry) = self.entries.get_mut(origin) {
            entry.last_used_tick = tick;
            entry.ratio
        } else {
            INITIAL_PHI_MAX_RATIO
        }
    }

    fn update_origin_ratio(&mut self, origin: String, ratio: f64) {
        let tick = self.next_tick();
        self.entries.insert(
            origin,
            OriginPhiRatioEntry {
                ratio,
                last_used_tick: tick,
            },
        );
        self.prune_lru();
    }

    fn current_ratio(&self, origin: &str) -> Option<f64> {
        self.entries.get(origin).map(|entry| entry.ratio)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn prune_lru(&mut self) {
        while self.entries.len() > ORIGIN_PHI_RATIO_CAPACITY {
            let Some(lru_key) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used_tick)
                .map(|(origin, _)| origin.clone())
            else {
                break;
            };
            self.entries.remove(&lru_key);
        }
    }
}

pub struct DownloadEngine {
    pub connections_per_download: usize,
    pub max_concurrent_tasks: usize,
    pub configured_connection_budget: Cell<usize>,
    pub effective_connection_budget: Cell<usize>,
    pub connection_budget: Cell<usize>,
    pub global_bandwidth_limit: Cell<u64>,
    pub refill_interval_ms: Cell<u64>,
    pub last_memory_check: Cell<Instant>,
    origin_phi_ratios: RefCell<OriginPhiRatioStore>,
    origin_h2_tunings: RefCell<OriginH2TuningStore>,
    origin_memory: Rc<RefCell<OriginMemoryStore>>,
    pub write_buffer_cap_bytes: Rc<Cell<usize>>,
    pub downloads: RefCell<Vec<DownloadHandle>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RangeSpec {
    id: u64,
    label_start_mb: u64,
    label_end_mb: u64,
    byte_start: u64,
    byte_end: u64,
}

const INITIAL_PHI_MAX_RATIO: f64 = 1.5;
const STORAGE_BLOCK_SIZE: u64 = 4194304; // 4MB
const RANGE_STATUS_PENDING: u8 = 0;
const RANGE_STATUS_ACTIVE: u8 = 1;
const RANGE_STATUS_FINISHED: u8 = 2;
const UNASSIGNED_CONNECTION: u32 = u32::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BorrowKind {
    Standard,
    Straggler,
    Tail,
}

impl BorrowKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Straggler => "straggler",
            Self::Tail => "tail",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlRangeSnapshot {
    id: u64,
    label_start_mb: u64,
    label_end_mb: u64,
    byte_start: u64,
    assigned_to: u32,
    cursor: u64,
    end: u64,
    parent_range_id: Option<u64>,
    status: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorSnapshot {
    dl_ranges: Vec<DlRangeSnapshot>,
    next_unassigned_idx: usize,
    borrow_limit_bytes: u64,
    borrow_cursor: usize,
    next_range_id: u64,
    index_state_bits: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSnapshot {
    task: DownloadTask,
    coordinator: CoordinatorSnapshot,
}

#[derive(Debug)]
struct Coordinator {
    dl_ranges: Vec<Rc<ActiveRange>>,
    next_unassigned_idx: usize,
    borrow_limit_bytes: u64,
    adaptive_minimum_steal_bytes: Rc<Cell<u64>>,
    borrow_cursor: usize,
    next_range_id: u64,
    total_size: u64,
    index_state: Rc<IndexStateMap>,
    log_file: StdFile,
    metrics: Rc<SchedulerMetrics>,
}

#[derive(Debug)]
struct IndexStateMap {
    total_size: u64,
    buckets: Vec<Cell<u8>>,
}

impl IndexStateMap {
    fn new(total_size: u64) -> Self {
        let bucket_count = total_size.div_ceil(INDEX_STATE_BYTES) as usize;
        let mut buckets = Vec::with_capacity(bucket_count);
        buckets.resize_with(bucket_count, || Cell::new(0));
        Self { total_size, buckets }
    }

    fn from_snapshot(total_size: u64, bits: Vec<u8>) -> Self {
        let bucket_count = total_size.div_ceil(INDEX_STATE_BYTES) as usize;
        let mut buckets = Vec::with_capacity(bucket_count);
        for idx in 0..bucket_count {
            let value = bits.get(idx).copied().unwrap_or(0);
            buckets.push(Cell::new(value));
        }
        Self { total_size, buckets }
    }

    fn snapshot_bits(&self) -> Vec<u8> {
        self.buckets
            .iter()
            .map(|bucket| bucket.get())
            .collect()
    }

    fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    fn storage_bytes(&self) -> usize {
        self.buckets.len()
    }

    fn completed_slices(&self) -> u64 {
        self.buckets
            .iter()
            .enumerate()
            .map(|(idx, bucket)| {
                let raw = bucket.get();
                let mask = valid_slice_mask(self.total_size, idx);
                (raw & mask).count_ones() as u64
            })
            .sum()
    }

    fn mark_completed_span(&self, from_byte: u64, to_byte: u64) {
        if to_byte <= from_byte || self.total_size == 0 {
            return;
        }

        let clamped_from = from_byte.min(self.total_size);
        let clamped_to = to_byte.min(self.total_size);
        if clamped_to <= clamped_from {
            return;
        }

        let start_slice = (clamped_from / MB) as usize;
        let end_slice_exclusive = if clamped_to >= self.total_size {
            self.total_size.div_ceil(MB) as usize
        } else {
            (clamped_to / MB) as usize
        };

        for slice_idx in start_slice..end_slice_exclusive {
            let bucket_idx = slice_idx / 8;
            let bit_idx = slice_idx % 8;
            if let Some(bucket) = self.buckets.get(bucket_idx) {
                bucket.set(bucket.get() | (1_u8 << bit_idx));
            }
        }
    }
}

impl DownloadEngine {
    pub fn new(
        connections_per_download: usize,
        max_concurrent_tasks: usize,
        max_total_connections: usize,
        global_bandwidth_limit_bps: u64,
        enable_origin_memory: bool,
    ) -> Rc<Self> {
        let configured_budget = max_total_connections.max(1);
        let origin_memory = OriginMemoryStore::load_enabled(enable_origin_memory);
        let origin_phi_ratios = origin_memory.hydrate_phi_ratios();
        let origin_h2_tunings = origin_memory.hydrate_h2_tunings();
        Rc::new(Self {
            connections_per_download,
            max_concurrent_tasks,
            configured_connection_budget: Cell::new(configured_budget),
            effective_connection_budget: Cell::new(configured_budget),
            connection_budget: Cell::new(configured_budget),
            global_bandwidth_limit: Cell::new(global_bandwidth_limit_bps),
            refill_interval_ms: Cell::new(if global_bandwidth_limit_bps == 0 { 50 } else { 100 }),
            last_memory_check: Cell::new(Instant::now()),
            origin_phi_ratios: RefCell::new(origin_phi_ratios),
            origin_h2_tunings: RefCell::new(origin_h2_tunings),
            origin_memory: Rc::new(RefCell::new(origin_memory)),
            write_buffer_cap_bytes: Rc::new(Cell::new(4 * MB as usize)),
            downloads: RefCell::new(Vec::new()),
        })
    }

    pub fn request_connection(&self) -> bool {
        let current = self.connection_budget.get();
        if current > 0 {
            self.connection_budget.set(current - 1);
            true
        } else {
            false
        }
    }

    pub fn release_connection(&self) {
        let next = self.connection_budget.get().saturating_add(1);
        self.connection_budget
            .set(next.min(self.effective_connection_budget.get()));
    }

    pub async fn run(
        self: Rc<Self>,
        mut cmd_rx: mpsc::Receiver<EngineCommand>,
        cmd_tx: mpsc::Sender<EngineCommand>,
        event_tx: mpsc::Sender<EngineEvent>,
    ) -> Result<()> {
        let mut active_controls: HashMap<Uuid, Rc<RuntimeControl>> = HashMap::new();
        let mut paused_tasks: HashMap<Uuid, TaskSnapshot> = HashMap::new();
        let mut persisted_paths: HashMap<Uuid, PathBuf> = HashMap::new();
        let mut pending_launches = VecDeque::<PendingLaunch>::new();
        let mut last_refill_recompute = Instant::now();
        let mut memory_system = System::new();

        loop {
            let refill_sleep_ms = self.refill_interval_ms.get().max(10);
            let refill_sleep = tokio::time::sleep(Duration::from_millis(refill_sleep_ms));
            tokio::pin!(refill_sleep);
            tokio::select! {
                _ = &mut refill_sleep => {
                    if self.last_memory_check.get().elapsed() >= Duration::from_secs(30) {
                        memory_system.refresh_memory();
                        let available_mb = memory_system.available_memory() / (1024 * 1024);
                        let configured_budget = self.configured_connection_budget.get();
                        let previous_effective = self.effective_connection_budget.get();
                        let new_effective =
                            compute_effective_connection_budget(configured_budget, available_mb);
                        let write_buffer_cap_bytes = if available_mb < 512 {
                            WRITE_BUFFER_LARGE_BYTES
                        } else {
                            4 * MB as usize
                        };
                        self.write_buffer_cap_bytes.set(write_buffer_cap_bytes);
                        if new_effective != previous_effective {
                            let current_available = self.connection_budget.get().min(previous_effective);
                            let active_leases = previous_effective.saturating_sub(current_available);
                            let new_available = new_effective.saturating_sub(active_leases.min(new_effective));
                            self.effective_connection_budget.set(new_effective);
                            self.connection_budget.set(new_available);
                            if new_effective < previous_effective {
                                eprintln!(
                                    "INFO: memory pressure reduced connection budget available_mb={} effective_budget={}/{}",
                                    available_mb,
                                    new_effective,
                                    configured_budget
                                );
                            } else {
                                eprintln!(
                                    "INFO: memory pressure restored connection budget available_mb={} effective_budget={}/{}",
                                    available_mb,
                                    new_effective,
                                    configured_budget
                                );
                            }
                        }
                        self.last_memory_check.set(Instant::now());
                    }

                    while active_controls.len() < self.max_concurrent_tasks {
                        let Some(next_launch) = pending_launches.pop_front() else { break; };
                        match next_launch {
                            PendingLaunch::Fresh(task) => {
                                let control = Rc::new(RuntimeControl::new(ScalerConfig {
                                    min_connections: task.min_connections,
                                    max_connections: task.max_connections,
                                    heartbeat_ms: 2000,
                                }));
                                active_controls.insert(task.id, control.clone());

                                let bucket = Rc::new(TokenBucket::new());
                                self.downloads.borrow_mut().push(DownloadHandle {
                                    id: task.id,
                                    bucket: bucket.clone(),
                                    per_download_limit_bps: task.per_download_bandwidth_limit_bps,
                                });

                                self.spawn_download_task(task, None, control, bucket, cmd_tx.clone(), event_tx.clone());
                            }
                            PendingLaunch::Resume(snapshot) => {
                                let task = snapshot.task.clone();
                                let control = Rc::new(RuntimeControl::new(ScalerConfig {
                                    min_connections: task.min_connections,
                                    max_connections: task.max_connections,
                                    heartbeat_ms: 2000,
                                }));
                                active_controls.insert(task.id, control.clone());

                                let bucket = Rc::new(TokenBucket::new());
                                self.downloads.borrow_mut().push(DownloadHandle {
                                    id: task.id,
                                    bucket: bucket.clone(),
                                    per_download_limit_bps: task.per_download_bandwidth_limit_bps,
                                });

                                self.spawn_download_task(task, Some(snapshot), control, bucket, cmd_tx.clone(), event_tx.clone());
                            }
                        }
                    }

                    let downloads = self.downloads.borrow();
                    let n_active = downloads.len();
                    if n_active > 0 {
                        let global_limit = self.global_bandwidth_limit.get();
                        if last_refill_recompute.elapsed() >= Duration::from_secs(5) {
                            self.refill_interval_ms
                                .set(compute_refill_interval_ms(global_limit));
                            last_refill_recompute = Instant::now();
                        }
                        let refill_interval_ms = self.refill_interval_ms.get();
                        let per_download = if global_limit == 0 {
                            0
                        } else {
                            global_limit / n_active as u64
                        };
                        for handle in downloads.iter() {
                            let quota = if per_download == 0 {
                                handle.per_download_limit_bps
                            } else if handle.per_download_limit_bps == 0 {
                                per_download
                            } else {
                                per_download.min(handle.per_download_limit_bps)
                            };
                            handle.bucket.quota_bytes_per_sec.set(quota);
                            handle.bucket.refill_interval_ms.set(refill_interval_ms);
                            handle.bucket.refill(refill_interval_ms);
                        }
                    } else if last_refill_recompute.elapsed() >= Duration::from_secs(5) {
                        self.refill_interval_ms
                            .set(compute_refill_interval_ms(self.global_bandwidth_limit.get()));
                        last_refill_recompute = Instant::now();
                    }
                }
                cmd_opt = cmd_rx.recv() => {
                    let Some(cmd) = cmd_opt else { break; };
                    match cmd {
                        EngineCommand::Add(task) => {
                            pending_launches.push_back(PendingLaunch::Fresh(task));
                        }
                        EngineCommand::Stop(id) => {
                            if let Some(control) = active_controls.get(&id) {
                                control.request_pause();
                            }
                        }
                        EngineCommand::Cancel(id) => {
                            if let Some(control) = active_controls.get(&id) {
                                control.request_persist();
                            }
                        }
                        EngineCommand::Resume(id) => {
                            if active_controls.contains_key(&id) {
                                continue;
                            }

                            let snapshot = if let Some(snapshot) = paused_tasks.remove(&id) {
                                snapshot
                            } else {
                                let path = persisted_paths
                                    .get(&id)
                                    .cloned()
                                    .ok_or_else(|| anyhow!("No paused or persisted task found for {}", id))?;
                                load_snapshot(&path)?
                            };
                            pending_launches.push_back(PendingLaunch::Resume(snapshot));
                        }
                        EngineCommand::UpdateScaling(id, config) => {
                            if config.min_connections > config.max_connections {
                                let _ = event_tx
                                    .send(EngineEvent::StatusChanged(
                                        id,
                                        DownloadStatus::Error(format!(
                                            "invalid scaling update: min_connections {} exceeds max_connections {}",
                                            config.min_connections, config.max_connections
                                        )),
                                    ))
                                    .await;
                                continue;
                            }

                            if let Some(control) = active_controls.get(&id) {
                                *control.scaler_config().borrow_mut() = config;
                            } else if let Some(snapshot) = paused_tasks.get_mut(&id) {
                                snapshot.task.min_connections = config.min_connections;
                                snapshot.task.max_connections = config.max_connections;
                            } else if let Some(path) = persisted_paths.get(&id).cloned() {
                                let mut snapshot = load_snapshot(&path)?;
                                snapshot.task.min_connections = config.min_connections;
                                snapshot.task.max_connections = config.max_connections;
                                persist_snapshot(&path, &snapshot)?;
                            }
                        }
                        EngineCommand::RuntimeStopped(snapshot, halt_mode) => {
                            active_controls.remove(&snapshot.task.id);
                            self.downloads.borrow_mut().retain(|h| h.id != snapshot.task.id);
                            
                            match halt_mode {
                                HaltMode::PauseMemory => {
                                    paused_tasks.insert(snapshot.task.id, snapshot.clone());
                                    let _ = event_tx
                                        .send(EngineEvent::StatusChanged(
                                            snapshot.task.id,
                                            DownloadStatus::Paused,
                                        ))
                                        .await;
                                }
                                HaltMode::PersistToDisk => {
                                    let path = metadata_path(&snapshot.task);
                                    persist_snapshot(&path, &snapshot)?;
                                    persisted_paths.insert(snapshot.task.id, path);
                                    let _ = event_tx
                                        .send(EngineEvent::StatusChanged(
                                            snapshot.task.id,
                                            DownloadStatus::Stopped,
                                        ))
                                        .await;
                                }
                                HaltMode::Running => {}
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn spawn_download_task(
        self: &Rc<Self>,
        task: DownloadTask,
        snapshot: Option<TaskSnapshot>,
        control: Rc<RuntimeControl>,
        bucket: Rc<TokenBucket>,
        cmd_tx: mpsc::Sender<EngineCommand>,
        event_tx: mpsc::Sender<EngineEvent>,
    ) {
        let default_connections = self.connections_per_download;
        let engine = self.clone();
        tokio::task::spawn_local(async move {
            let task_id = task.id;
            let result = run_download_task(
                engine,
                task,
                snapshot,
                control,
                bucket,
                cmd_tx.clone(),
                event_tx.clone(),
                default_connections,
            ).await;
            if let Err(err) = result {
                let _ = event_tx.send(EngineEvent::StatusChanged(
                    task_id,
                    DownloadStatus::Error(err.to_string()),
                )).await;
            }
        });
    }
}

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
    let total_size = if let Some(snapshot) = &snapshot {
        snapshot.task.total_size
    } else {
        resolve_total_size(&task).await?
    };

    task.total_size = total_size;
    let origin = origin_key(&task.url);
    let origin_memory_hit = engine.origin_memory.borrow().memory_hit_for_origin(&origin);
    if !task.dry_run {
        engine
            .origin_memory
            .borrow_mut()
            .note_content_length_reliable(&origin, total_size > 0);
    }
    if task.connections == 0 {
        task.connections = default_connections.max(1);
    }
    if task.borrow_limit_mb == 0 {
        task.borrow_limit_mb = DEFAULT_BORROW_LIMIT_MB;
    }

    let _ = event_tx.send(EngineEvent::TotalSize(task.id, total_size)).await;
    let _ = event_tx
        .send(EngineEvent::StatusChanged(task.id, DownloadStatus::Downloading))
        .await;

    let log_path = log_path(&task);
    ensure_parent_dir(&log_path)?;
    let metrics = Rc::new(SchedulerMetrics::default());
    let protocol_hint = engine
        .origin_memory
        .borrow_mut()
        .protocol_hint_for_origin(&origin)
        .unwrap_or(ProtocolFamily::Other);
    let phi_max_ratio = engine.origin_phi_ratios.borrow_mut().ratio_for_origin(&origin);
    let shared_write_latency_ms = Rc::new(Cell::new(10.0));
    let write_buffer_cap_bytes = engine.write_buffer_cap_bytes.clone();
    let adaptive_minimum_steal_bytes = Rc::new(Cell::new(
        (task.borrow_limit_mb.max(1) * MB).max(2 * STORAGE_BLOCK_SIZE),
    ));
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
    let leased_connections = Rc::new(Cell::new(0usize));
    let desired_initial_connections = task.connections.max(1);
    let mut initial_connections = 0usize;
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
    if initial_connections == 0 {
        let _ = event_tx
            .send(EngineEvent::StatusChanged(
                task.id,
                DownloadStatus::Error("no connection budget available".to_string()),
            ))
            .await;
        return Ok(());
    }
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
            if n_active == 0 {
                continue;
            }

            let current_efficiency = new_ewma / n_active as f64;
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
            } else if n_active < max_c {
                if scaler_engine.request_connection() {
                    scaler_for_task.throughput_before_add.set(new_ewma);
                    did_add = true;
                    scaler_for_task
                        .slow_start_remaining
                        .set(compute_slow_start_heartbeats(&scaler_for_task, dominant_protocol));
                    scaler_for_task.last_action.set(ScalerAction::Grow);
                    scaler_leased_connections.set(scaler_leased_connections.get() + 1);
                    record_protocol_scale_metric(
                        &scaler_metrics,
                        dominant_protocol,
                        ScalerAction::Grow,
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
    engine
        .origin_memory
        .borrow_mut()
        .note_protocol(&origin, dominant_protocol);
    engine
        .origin_memory
        .borrow_mut()
        .note_reuse_metrics(&origin, scaler.reuse_rate.get(), scaler.ewma_handshake_ms.get());
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

    Ok(())
}

async fn resolve_total_size(task: &DownloadTask) -> Result<u64> {
    if task.dry_run {
        if let Some(size_mb) = task.dry_run_size_mb {
            return Ok(size_mb * MB);
        }
    }

    let (client, _) = build_http_client(task.http_mode, task.max_connections.max(1), None);
    let res = send_request_follow_redirects(&client, Method::HEAD, &task.url, None).await?;
    let total_size = res
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    if total_size == 0 {
        return Err(anyhow!("Could not determine file size"));
    }

    Ok(total_size)
}

impl Coordinator {
    fn new(
        task_id: Uuid,
        total_size: u64,
        log_path: &Path,
        borrow_limit_mb: u64,
        connections: usize,
        dry_run: bool,
        schedule_mode: ScheduleMode,
        metrics: Rc<SchedulerMetrics>,
        adaptive_minimum_steal_bytes: Rc<Cell<u64>>,
        phi_max_ratio: f64,
    ) -> Result<Self> {
        let fib_mb = build_fib_mb();
        let ceil_mb = total_size.div_ceil(MB);
        let support_idx = fib_mb
            .iter()
            .position(|value| *value >= ceil_mb.max(1))
            .ok_or_else(|| anyhow!("Download exceeds generated Fibonacci range table"))?;

        let seed_ranges = match schedule_mode {
            ScheduleMode::FibAdaptive => {
                build_phi_geometric_ranges(total_size, connections.max(1), phi_max_ratio, STORAGE_BLOCK_SIZE)
            }
            ScheduleMode::Fib => {
                let seed_start_idx = choose_seed_start_idx(&fib_mb, support_idx, connections.max(1), dry_run);
                build_seed_ranges(&fib_mb, seed_start_idx, support_idx, total_size)
            }
            ScheduleMode::Equal => build_equal_ranges(total_size, connections.max(1)),
        };

        let geometry_label = if seed_ranges.len() > 1 {
            let mut labels = Vec::new();
            for r in &seed_ranges {
                let pct = (r.byte_end - r.byte_start) as f64 / total_size as f64 * 100.0;
                labels.push(format!("{:.1}%", pct));
            }
            labels.join(" / ")
        } else {
            "100%".to_string()
        };
        let dl_ranges: Vec<Rc<ActiveRange>> = seed_ranges
            .iter()
            .map(|spec| {
                Rc::new(ActiveRange {
                    id: spec.id + 1,
                    label_start_mb: spec.label_start_mb,
                    label_end_mb: spec.label_end_mb,
                    byte_start: spec.byte_start,
                    assigned_to: Cell::new(UNASSIGNED_CONNECTION),
                    cursor: Cell::new(spec.byte_start),
                    end: Cell::new(spec.byte_end),
                    parent_range_id: None,
                    status: Cell::new(RANGE_STATUS_PENDING),
                    last_sample_cursor: Cell::new(spec.byte_start),
                    last_sample_at_ms: Cell::new(0),
                    recent_speed_bps: Cell::new(0),
                })
            })
            .collect();

        let mut coordinator = Self {
            dl_ranges,
            next_unassigned_idx: 0,
            borrow_limit_bytes: borrow_limit_mb.max(1) * MB,
            adaptive_minimum_steal_bytes,
            borrow_cursor: 0,
            next_range_id: seed_ranges.len() as u64 + 1,
            total_size,
            index_state: Rc::new(IndexStateMap::new(total_size)),
            log_file: StdFile::create(log_path)?,
            metrics,
        };

        coordinator.log(&format!(
            "Coordinator started for task={} total_size={}B ceil_mb={} schedule_mode={} seed_floor_mb={} seed_start={}MB support_end={}MB borrow_limit={}MB dry_run={} phi_max_ratio={:.3} index_state_bucket_mb={} index_state_buckets={} index_state_bytes={}",
            task_id,
            total_size,
            ceil_mb,
            schedule_mode.as_str(),
            if dry_run { 1 } else { LIVE_SEED_FLOOR_MB },
            seed_ranges.first().map(|r| r.label_start_mb).unwrap_or(0),
            fib_mb[support_idx],
            borrow_limit_mb.max(1),
            dry_run,
            phi_max_ratio,
            INDEX_STATE_MB,
            coordinator.index_state.bucket_count(),
            coordinator.index_state.storage_bytes(),
        ));
        coordinator.log(&format!("Initial distribution geometry: [{}]", geometry_label));
        let range_lines: Vec<String> = seed_ranges
            .iter()
            .map(|spec| {
                let fit_end_mb = bytes_to_ceiling_mb(spec.byte_end);
                format!(
                "vector range#{} support={}..{}MB fit_end={}MB bytes={}..{}",
                spec.id,
                spec.label_start_mb,
                spec.label_end_mb,
                fit_end_mb,
                spec.byte_start,
                spec.byte_end
            )
            })
            .collect();
        for line in range_lines {
            coordinator.log(&line);
        }

        Ok(coordinator)
    }

    fn from_snapshot(
        snapshot: CoordinatorSnapshot,
        total_size: u64,
        log_path: &Path,
        _schedule_mode: ScheduleMode,
        metrics: Rc<SchedulerMetrics>,
        adaptive_minimum_steal_bytes: Rc<Cell<u64>>,
    ) -> Result<Self> {
        let dl_ranges = snapshot
            .dl_ranges
            .into_iter()
            .map(|range| {
                Rc::new(ActiveRange {
                    id: range.id,
                    label_start_mb: range.label_start_mb,
                    label_end_mb: range.label_end_mb,
                    byte_start: range.byte_start,
                    assigned_to: Cell::new(range.assigned_to),
                    cursor: Cell::new(range.cursor),
                    end: Cell::new(range.end),
                    parent_range_id: range.parent_range_id,
                    status: Cell::new(range.status),
                    last_sample_cursor: Cell::new(range.cursor),
                    last_sample_at_ms: Cell::new(0),
                    recent_speed_bps: Cell::new(0),
                })
            })
            .collect();

        let mut coordinator = Self {
            dl_ranges,
            next_unassigned_idx: snapshot.next_unassigned_idx,
            borrow_limit_bytes: snapshot.borrow_limit_bytes,
            adaptive_minimum_steal_bytes,
            borrow_cursor: snapshot.borrow_cursor,
            next_range_id: snapshot.next_range_id,
            total_size,
            index_state: Rc::new(IndexStateMap::from_snapshot(total_size, snapshot.index_state_bits)),
            log_file: std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path)?,
            metrics,
        };

        coordinator.log(&format!(
            "Coordinator resumed from snapshot. index_state_buckets={} index_state_bytes={} completed_slices={}",
            coordinator.index_state.bucket_count(),
            coordinator.index_state.storage_bytes(),
            coordinator.index_state.completed_slices(),
        ));
        Ok(coordinator)
    }

    fn log(&mut self, msg: &str) {
        let _ = writeln!(self.log_file, "[{}] {}", chrono::Local::now(), msg);
    }

    fn log_summary(&mut self, total_size: u64) {
        self.metrics
            .adaptive_min_steal_bytes_final
            .set(self.adaptive_minimum_steal_bytes.get());
        self.log(&format!(
            "{} total_size={} final_ranges={} vector_consumed={} index_state_buckets={} index_state_bytes={} completed_slices={}",
            self.metrics.summary_line(),
            total_size,
            self.dl_ranges.len(),
            self.next_unassigned_idx,
            self.index_state.bucket_count(),
            self.index_state.storage_bytes(),
            self.index_state.completed_slices(),
        ));
    }

    async fn run(&mut self, mut work_rx: mpsc::Receiver<WorkRequest>, control: Rc<RuntimeControl>) {
        while let Some(req) = work_rx.recv().await {
            if control.is_halted() {
                let _ = req.tx.send(None);
                continue;
            }

            let work = self.get_work(req.connection_id);
            if work.is_none() {
                self.log(&format!("conn={} no more work", req.connection_id));
            }
            let _ = req.tx.send(work);
        }
        self.log("Coordinator finished.");
    }

    fn get_work(&mut self, connection_id: u32) -> Option<Rc<ActiveRange>> {
        while self.next_unassigned_idx < self.dl_ranges.len() {
            let range = self.dl_ranges[self.next_unassigned_idx].clone();
            self.next_unassigned_idx += 1;
            if range.assigned_to.get() != UNASSIGNED_CONNECTION {
                continue;
            }
            range.assigned_to.set(connection_id);
            range.status.set(RANGE_STATUS_ACTIVE);
            SchedulerMetrics::add(&self.metrics.direct_assignments, 1);
            self.log(&format!(
                "assign conn={} active_range#{} support={}..{}MB bytes={}..{}",
                connection_id,
                range.id,
                range.label_start_mb,
                range.label_end_mb,
                range.byte_start,
                range.end.get()
            ));
            return Some(range);
        }

        for idx in 0..self.dl_ranges.len() {
            let range = self.dl_ranges[idx].clone();
            if range.assigned_to.get() != UNASSIGNED_CONNECTION {
                continue;
            }
            if range.status.get() != RANGE_STATUS_PENDING {
                continue;
            }
            if range.cursor.get() >= range.end.get() {
                continue;
            }
            range.assigned_to.set(connection_id);
            range.status.set(RANGE_STATUS_ACTIVE);
            SchedulerMetrics::add(&self.metrics.direct_assignments, 1);
            self.log(&format!(
                "reassign conn={} active_range#{} support={}..{}MB bytes={}..{}",
                connection_id,
                range.id,
                range.label_start_mb,
                range.label_end_mb,
                range.cursor.get(),
                range.end.get()
            ));
            return Some(range.clone());
        }

        self.borrow_work(connection_id)
    }

    fn borrow_work(&mut self, connection_id: u32) -> Option<Rc<ActiveRange>> {
        if self.dl_ranges.is_empty() {
            return None;
        }

        if let Some((idx, kind)) = self.select_borrow_candidate(connection_id) {
            return self.split_active_range(idx, connection_id, kind);
        }

        None
    }

    fn select_borrow_candidate(&self, connection_id: u32) -> Option<(usize, BorrowKind)> {
        let effective_limit = self.effective_borrow_limit_bytes();
        let mut active_speeds = Vec::new();
        for range in &self.dl_ranges {
            let owner_connection = range.assigned_to.get();
            if owner_connection == connection_id || owner_connection == UNASSIGNED_CONNECTION {
                continue;
            }
            if range.status.get() != RANGE_STATUS_ACTIVE {
                continue;
            }
            let remaining = range.end.get().saturating_sub(range.cursor.get());
            if remaining < effective_limit {
                continue;
            }
            let speed = range.recent_speed_bps.get();
            if speed > 0 {
                active_speeds.push(speed);
            }
        }

        let median_speed = median_u64(&mut active_speeds);
        let mut best_straggler = None::<(usize, u64)>;
        let total = self.dl_ranges.len();
        for offset in 0..total {
            let idx = (self.borrow_cursor + offset) % total;
            let active = &self.dl_ranges[idx];
            let owner_connection = active.assigned_to.get();
            if owner_connection == connection_id || owner_connection == UNASSIGNED_CONNECTION {
                continue;
            }
            if active.status.get() != RANGE_STATUS_ACTIVE {
                continue;
            }
            let remaining = active.end.get().saturating_sub(active.cursor.get());
            if remaining <= effective_limit.saturating_mul(2) {
                continue;
            }

            let speed = active.recent_speed_bps.get();
            if median_speed > 0
                && speed > 0
                && speed.saturating_mul(100) <= median_speed.saturating_mul(60)
                && remaining >= effective_limit.saturating_mul(3)
            {
                match best_straggler {
                    Some((_, best_remaining)) if best_remaining >= remaining => {}
                    _ => best_straggler = Some((idx, remaining)),
                }
            }
        }

        if let Some((idx, _)) = best_straggler {
            return Some((idx, BorrowKind::Straggler));
        }

        let mut best_standard = None::<(usize, u64)>;
        for offset in 0..total {
            let idx = (self.borrow_cursor + offset) % total;
            let active = &self.dl_ranges[idx];
            let owner_connection = active.assigned_to.get();
            if owner_connection == connection_id || owner_connection == UNASSIGNED_CONNECTION {
                continue;
            }
            if active.status.get() != RANGE_STATUS_ACTIVE {
                continue;
            }
            let remaining = active.end.get().saturating_sub(active.cursor.get());
            if remaining <= effective_limit.saturating_mul(2) {
                continue;
            }
            
            match best_standard {
                Some((_, best_remaining)) if best_remaining >= remaining => {}
                _ => best_standard = Some((idx, remaining)),
            }
        }

        if let Some((idx, _)) = best_standard {
            let kind = if self.is_tail_phase() {
                BorrowKind::Tail
            } else {
                BorrowKind::Standard
            };
            return Some((idx, kind));
        }

        None
    }

    fn split_active_range(
        &mut self,
        idx: usize,
        connection_id: u32,
        kind: BorrowKind,
    ) -> Option<Rc<ActiveRange>> {
        let active = self.dl_ranges[idx].clone();
        let owner_connection = active.assigned_to.get();
        let start = active.cursor.get();
        let end = active.end.get();
        let effective_limit = self.effective_borrow_limit_bytes();
        let remaining = end.saturating_sub(start);
        if remaining <= effective_limit.saturating_mul(2) {
            return None;
        }

        let steal_size = match kind {
            BorrowKind::Straggler => remaining / 2,
            BorrowKind::Tail => remaining / 2,
            BorrowKind::Standard => (((remaining as u128) * (GOLDEN_RATIO_NUM as u128))
                / (GOLDEN_RATIO_DEN as u128)) as u64,
        };
        let aligned_split = align_down(end.saturating_sub(steal_size), MB);
        if aligned_split <= start + effective_limit {
            return None;
        }

        let stolen_size = end.saturating_sub(aligned_split);
        if stolen_size < effective_limit {
            return None;
        }

        active.end.set(aligned_split);

        let borrowed = Rc::new(ActiveRange {
            id: self.next_range_id,
            label_start_mb: active.label_start_mb,
            label_end_mb: active.label_end_mb,
            byte_start: aligned_split,
            assigned_to: Cell::new(connection_id),
            cursor: Cell::new(aligned_split),
            end: Cell::new(end),
            parent_range_id: Some(active.id),
            status: Cell::new(RANGE_STATUS_ACTIVE),
            last_sample_cursor: Cell::new(aligned_split),
            last_sample_at_ms: Cell::new(0),
            recent_speed_bps: Cell::new(0),
        });
        self.next_range_id += 1;
        SchedulerMetrics::add(&self.metrics.borrow_assignments, 1);
        SchedulerMetrics::add(&self.metrics.bytes_borrowed, stolen_size);
        if kind == BorrowKind::Straggler {
            SchedulerMetrics::add(&self.metrics.straggler_splits, 1);
        }
        if kind == BorrowKind::Tail {
            SchedulerMetrics::add(&self.metrics.tail_splits, 1);
        }
        let donor_id = active.id;
        let donor_label_start = active.label_start_mb;
        let donor_label_end = active.label_end_mb;
        let donor_speed = active.recent_speed_bps.get();
        self.dl_ranges.push(borrowed.clone());
        self.borrow_cursor = idx + 1;

        self.log(&format!(
            "borrow kind={} conn={} from_conn={} donor_range#{} new_range#{} support={}..{}MB bytes={}..{} donor_speed_Bps={}",
            kind.as_str(),
            connection_id,
            owner_connection,
            donor_id,
            borrowed.id,
            donor_label_start,
            donor_label_end,
            aligned_split,
            end,
            donor_speed,
        ));
        Some(borrowed)
    }

    fn effective_borrow_limit_bytes(&self) -> u64 {
        let base_limit = if self.is_tail_phase() {
            self.borrow_limit_bytes.min(MB).max(MB)
        } else {
            self.borrow_limit_bytes
        };
        if self.is_tail_phase() {
            base_limit
        } else {
            base_limit.max(self.adaptive_minimum_steal_bytes.get())
        }
    }

    fn is_tail_phase(&self) -> bool {
        is_tail_phase_bytes(snapshot_downloaded(self, self.total_size), self.total_size)
    }

    fn snapshot(&self) -> CoordinatorSnapshot {
        CoordinatorSnapshot {
            dl_ranges: self
                .dl_ranges
                .iter()
                .map(|range| DlRangeSnapshot {
                    id: range.id,
                    label_start_mb: range.label_start_mb,
                    label_end_mb: range.label_end_mb,
                    byte_start: range.byte_start,
                    assigned_to: range.assigned_to.get(),
                    cursor: range.cursor.get(),
                    end: range.end.get(),
                    parent_range_id: range.parent_range_id,
                    status: range.status.get(),
                })
                .collect(),
            next_unassigned_idx: self.next_unassigned_idx,
            borrow_limit_bytes: self.borrow_limit_bytes,
            borrow_cursor: self.borrow_cursor,
            next_range_id: self.next_range_id,
            index_state_bits: self.index_state.snapshot_bits(),
        }
    }
}

struct ConnectionWorker {
    connection_id: u32,
    url: String,
    origin: String,
    file_path: PathBuf,
    log_path: PathBuf,
    coordinator_tx: mpsc::Sender<WorkRequest>,
    global_downloaded: Rc<Cell<u64>>,
    control: Rc<RuntimeControl>,
    worker_control: Rc<WorkerControl>,
    dry_run: bool,
    borrow_limit_bytes: u64,
    adaptive_minimum_steal_bytes: Rc<Cell<u64>>,
    write_buffer_cap_bytes: Rc<Cell<usize>>,
    total_size: u64,
    origin_memory: Rc<RefCell<OriginMemoryStore>>,
    shared_write_latency_ms: Rc<Cell<f64>>,
    ewma_connection_rtt_ms: Cell<f64>,
    metrics: Rc<SchedulerMetrics>,
    client: DownloadHttpClient,
    index_state: Rc<IndexStateMap>,
    bucket: Rc<TokenBucket>,
    scaler: Rc<Scaler>,
}

#[derive(Debug, Default)]
struct AttemptTiming {
    request_setup_ms: u64,
    first_byte_ms: u64,
    stream_ms: u64,
    write_ms: u64,
    bytes_written: u64,
    chunks: u64,
    request_kind: Option<RequestKind>,
    protocol: Option<ProtocolFamily>,
    handshake_cost_ms: u64,
}

#[derive(Debug, Default)]
struct PendingWrite {
    start_offset: u64,
    data: Vec<u8>,
    target_bytes: usize,
}

#[derive(Debug)]
struct StartupProbe {
    worker_started_at: Instant,
    open_file_ms: u64,
    first_assignment_wait_ms: Option<u64>,
    first_request_setup_ms: Option<u64>,
    first_byte_ms: Option<u64>,
    total_to_first_byte_ms: Option<u64>,
    logged: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestKind {
    Reused,
    Fresh,
}

#[derive(Debug, Clone, Copy)]
enum RetryHint {
    Immediate,
    Backoff(u64),
    ReduceWorkers(u64),
    ShrinkRange(u64),
    Abort,
}

impl ConnectionWorker {
    fn effective_prefetch_limit_bytes(&self) -> u64 {
        let base_limit = if is_tail_phase_bytes(self.global_downloaded.get(), self.total_size) {
            self.borrow_limit_bytes.min(MB).max(MB)
        } else {
            self.borrow_limit_bytes
        };

        if is_tail_phase_bytes(self.global_downloaded.get(), self.total_size) {
            base_limit
        } else {
            base_limit.max(self.adaptive_minimum_steal_bytes.get())
        }
    }

    fn should_exit_for_scale_down(&self) -> bool {
        self.worker_control.stop_requested.get()
    }

    async fn relinquish_range(&self, range: &Rc<ActiveRange>, local_cursor: u64) {
        range.cursor.set(local_cursor);
        range.assigned_to.set(UNASSIGNED_CONNECTION);
        range.status.set(RANGE_STATUS_PENDING);
        self.log_msg(&format!(
            "range#{} relinquished at byte={} for scale-down",
            range.id,
            local_cursor
        ))
        .await;
    }

    fn note_first_assignment(&self, startup: &mut StartupProbe, wait_started: Instant) {
        if startup.first_assignment_wait_ms.is_none() {
            let waited_ms = wait_started.elapsed().as_millis() as u64;
            startup.first_assignment_wait_ms = Some(waited_ms);
            SchedulerMetrics::add(&self.metrics.startup_first_assignment_wait_ms, waited_ms);
        }
    }

    fn note_first_request_setup(&self, startup: &mut StartupProbe, setup_ms: u64) {
        if startup.first_request_setup_ms.is_none() {
            startup.first_request_setup_ms = Some(setup_ms);
            SchedulerMetrics::add(&self.metrics.startup_first_request_setup_ms, setup_ms);
        }
    }

    async fn note_first_byte(
        &self,
        startup: &mut StartupProbe,
        file_backend: storage::StorageBackendKind,
    ) {
        if startup.logged {
            return;
        }

        let first_byte_ms = startup.first_byte_ms.unwrap_or_default();
        let total_to_first_byte_ms = startup.total_to_first_byte_ms.unwrap_or_default();
        self.log_msg(&format!(
            "startup backend={:?} open_file_ms={} first_assignment_wait_ms={} first_request_setup_ms={} first_byte_ms={} total_to_first_byte_ms={}",
            file_backend,
            startup.open_file_ms,
            startup.first_assignment_wait_ms.unwrap_or_default(),
            startup.first_request_setup_ms.unwrap_or_default(),
            first_byte_ms,
            total_to_first_byte_ms,
        ))
        .await;
        startup.logged = true;
    }

    async fn record_request_classification(
        &self,
        attempt_timing: &mut AttemptTiming,
        total_ttfb_ms: u64,
        protocol: ProtocolFamily,
    ) {
        let was_growth_probe = self.worker_control.pending_growth_probe.replace(false);
        let ttfb_ms = total_ttfb_ms as f64;
        let prev_conn_rtt = self.ewma_connection_rtt_ms.get();
        let updated_conn_rtt = if prev_conn_rtt <= 0.0 {
            ttfb_ms
        } else {
            0.25 * ttfb_ms + 0.75 * prev_conn_rtt
        };
        self.ewma_connection_rtt_ms.set(updated_conn_rtt);
        let ewma_rtt_ms = self.scaler.ewma_rtt_ms.get();
        let threshold_ms = ewma_rtt_ms * 1.5;
        let kind = if ttfb_ms < threshold_ms {
            RequestKind::Reused
        } else {
            RequestKind::Fresh
        };
        attempt_timing.request_kind = Some(kind);
        attempt_timing.protocol = Some(protocol);
        self.scaler.last_protocol.set(protocol);
        self.scaler
            .total_request_count
            .set(self.scaler.total_request_count.get().saturating_add(1));

        match kind {
            RequestKind::Reused => {
                self.scaler
                    .reused_count
                    .set(self.scaler.reused_count.get().saturating_add(1));
                let prev = self.scaler.ewma_rtt_ms.get();
                let updated = if self.scaler.reused_rtt_samples.get() == 0 {
                    ttfb_ms
                } else {
                    0.25 * ttfb_ms + 0.75 * prev
                };
                self.scaler.ewma_rtt_ms.set(updated);
                self.scaler
                    .reused_rtt_samples
                    .set(self.scaler.reused_rtt_samples.get().saturating_add(1));
                SchedulerMetrics::add(&self.metrics.reused_requests, 1);
                match protocol {
                    ProtocolFamily::Http1 => {
                        SchedulerMetrics::add(&self.metrics.http1_reused_requests, 1);
                    }
                    ProtocolFamily::Http2 => {
                        SchedulerMetrics::add(&self.metrics.http2_reused_requests, 1);
                    }
                    ProtocolFamily::Other => {}
                }
            }
            RequestKind::Fresh => {
                let handshake_cost_ms = (ttfb_ms - ewma_rtt_ms.max(1.0)).max(0.0);
                attempt_timing.handshake_cost_ms = handshake_cost_ms.round() as u64;
                let prev = self.scaler.ewma_handshake_ms.get();
                let updated = 0.25 * handshake_cost_ms + 0.75 * prev;
                self.scaler.ewma_handshake_ms.set(updated);
                SchedulerMetrics::add(&self.metrics.fresh_requests, 1);
                SchedulerMetrics::add(
                    &self.metrics.fresh_handshake_ms,
                    attempt_timing.handshake_cost_ms,
                );
                match protocol {
                    ProtocolFamily::Http1 => {
                        SchedulerMetrics::add(&self.metrics.http1_fresh_requests, 1);
                        SchedulerMetrics::add(
                            &self.metrics.http1_fresh_handshake_ms,
                            attempt_timing.handshake_cost_ms,
                        );
                    }
                    ProtocolFamily::Http2 => {
                        SchedulerMetrics::add(&self.metrics.http2_fresh_requests, 1);
                        SchedulerMetrics::add(
                            &self.metrics.http2_fresh_handshake_ms,
                            attempt_timing.handshake_cost_ms,
                        );
                    }
                    ProtocolFamily::Other => {}
                }

                if was_growth_probe
                    && self.scaler.last_action.get() == ScalerAction::Grow
                    && self.scaler.reused_rtt_samples.get()
                        >= protocol_growth_shield_min_samples(protocol)
                {
                    self.scaler.skip_growth_sample.set(true);
                    let heartbeat_ms = self.scaler.config.borrow().heartbeat_ms.max(1);
                    let adjusted_handshake_ms = ((attempt_timing.handshake_cost_ms as f64)
                        * protocol_growth_shield_multiplier(protocol))
                        .round() as u64;
                    let extra_heartbeats =
                        ((adjusted_handshake_ms + heartbeat_ms - 1) / heartbeat_ms) as u32;
                    self.scaler.slow_start_remaining.set(
                        self.scaler
                            .slow_start_remaining
                            .get()
                            .saturating_add(extra_heartbeats.max(1)),
                    );
                    self.log_msg(&format!(
                        "growth_probe_shield protocol={} handshake_ms={} extra_heartbeats={} reused_rtt_samples={}",
                        protocol.as_str(),
                        attempt_timing.handshake_cost_ms,
                        extra_heartbeats.max(1),
                        self.scaler.reused_rtt_samples.get(),
                    ))
                    .await;
                }
            }
        }

        let total_requests = self.scaler.total_request_count.get();
        if total_requests > 0 {
            self.scaler.reuse_rate.set(
                self.scaler.reused_count.get() as f64 / total_requests as f64,
            );
        }

        self.log_msg(&format!(
            "request_classified protocol={} kind={:?} total_ttfb_ms={} ewma_rtt_ms={:.1} reuse_rate={:.2} effective_add_threshold={:.2}",
            protocol.as_str(),
            kind,
            total_ttfb_ms,
            self.scaler.ewma_rtt_ms.get(),
            self.scaler.reuse_rate.get(),
            self.scaler.effective_add_threshold.get(),
        ))
        .await;
    }

    async fn flush_pending_write(
        &self,
        write_tx: &tokio::sync::mpsc::Sender<(u64, Vec<u8>)>,
        recycle_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        pending: &mut PendingWrite,
        attempt_timing: &mut AttemptTiming,
    ) -> Result<()> {
        if pending.data.is_empty() {
            return Ok(());
        }

        let write_started = Instant::now();
        let data_to_write = std::mem::take(&mut pending.data);
        write_tx.send((pending.start_offset, data_to_write)).await
            .map_err(|_| anyhow::anyhow!("writer task died"))?;
            
        if let Ok(mut recycled) = recycle_rx.try_recv() {
            recycled.clear();
            pending.data = recycled;
        } else {
            pending.data = Vec::with_capacity(pending.target_bytes.max(WRITE_BUFFER_MIN_BYTES));
        }

        let write_ms = write_started.elapsed().as_millis() as u64;
        attempt_timing.write_ms = attempt_timing.write_ms.saturating_add(write_ms);
        if write_ms > 0 && write_ms <= 500 {
            let sample = write_ms as f64;
            let prev = self.shared_write_latency_ms.get();
            let updated = 0.2 * sample + 0.8 * prev;
            self.shared_write_latency_ms.set(updated);
            let max_x10 = self.metrics.max_ewma_write_latency_x10.get();
            let updated_x10 = (updated * 10.0).round() as u64;
            if updated_x10 > max_x10 {
                self.metrics.max_ewma_write_latency_x10.set(updated_x10);
            }
        }
        
        self.trim_pending_write(pending);
        Ok(())
    }

    fn append_pending_write(&self, pending: &mut PendingWrite, offset: u64, data: &[u8]) {
        if pending.data.is_empty() {
            pending.start_offset = offset;
        }
        pending.data.extend_from_slice(data);
    }

    fn target_write_buffer_bytes(&self, recent_speed_bps: f64) -> usize {
        let speed_bps = recent_speed_bps.max(0.0) as u64;
        let speed_target = if speed_bps >= WRITE_BUFFER_MAX_SPEED_BPS {
            WRITE_BUFFER_MAX_BYTES
        } else if speed_bps >= WRITE_BUFFER_LARGE_SPEED_BPS {
            WRITE_BUFFER_LARGE_BYTES
        } else if speed_bps >= WRITE_BUFFER_MEDIUM_SPEED_BPS {
            WRITE_BUFFER_MEDIUM_BYTES
        } else {
            WRITE_BUFFER_MIN_BYTES
        };
        let latency_ratio = (self.shared_write_latency_ms.get() / 10.0).max(1.0);
        let latency_target = ((WRITE_BUFFER_LARGE_BYTES as f64) * latency_ratio).round() as usize;
        let cap = self.write_buffer_cap_bytes.get().max(WRITE_BUFFER_LARGE_BYTES);
        speed_target
            .max(latency_target)
            .clamp(WRITE_BUFFER_MIN_BYTES, cap)
    }

    fn record_write_buffer_target_metric(&self, target: usize) {
        let target_u64 = target as u64;
        if target_u64 > self.metrics.max_write_buffer_target_bytes.get() {
            self.metrics.max_write_buffer_target_bytes.set(target_u64);
        }
    }

    fn update_pending_write_target(&self, pending: &mut PendingWrite, recent_speed_bps: f64) {
        let target = self.target_write_buffer_bytes(recent_speed_bps);
        self.record_write_buffer_target_metric(target);
        if pending.target_bytes == 0 {
            pending.target_bytes = target;
        } else {
            pending.target_bytes = target;
        }

        if pending.data.capacity() < pending.target_bytes {
            pending
                .data
                .reserve(pending.target_bytes.saturating_sub(pending.data.capacity()));
        } else if pending.data.is_empty() {
            self.trim_pending_write(pending);
        }
    }

    fn trim_pending_write(&self, pending: &mut PendingWrite) {
        let target = pending.target_bytes.max(WRITE_BUFFER_MIN_BYTES);
        if pending.data.is_empty() && pending.data.capacity() > target.saturating_mul(2) {
            pending.data.shrink_to(target);
        }
    }

    fn reset_pending_write_target(&self, pending: &mut PendingWrite) {
        pending.target_bytes = WRITE_BUFFER_MIN_BYTES;
        self.trim_pending_write(pending);
    }

    fn update_range_speed_sample(&self, range: &Rc<ActiveRange>, current_cursor: u64) {
        let now_ms = unix_time_ms();
        let last_at = range.last_sample_at_ms.get();
        let last_cursor = range.last_sample_cursor.get();
        if last_at == 0 {
            range.last_sample_at_ms.set(now_ms);
            range.last_sample_cursor.set(current_cursor);
            return;
        }

        let elapsed_ms = now_ms.saturating_sub(last_at);
        let advanced = current_cursor.saturating_sub(last_cursor);
        if elapsed_ms < 250 || advanced == 0 {
            return;
        }

        let speed_bps = ((advanced as u128) * 1000 / (elapsed_ms as u128)) as u64;
        range.recent_speed_bps.set(speed_bps);
        range.last_sample_at_ms.set(now_ms);
        range.last_sample_cursor.set(current_cursor);
    }

    async fn log_msg(&self, msg: &str) {
        if let Ok(mut f) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .await
        {
            let _ = f
                .write_all(format!("[{}] conn={}: {}\n", chrono::Local::now(), self.connection_id, msg).as_bytes())
                .await;
        }
    }

    async fn run(self) -> Result<()> {
        if self.dry_run {
            self.run_dry().await
        } else {
            self.run_live().await
        }
    }

    async fn run_dry(self) -> Result<()> {
        let mut current_range: Option<Rc<ActiveRange>> = None;
        let mut prefetched_range: Option<Rc<ActiveRange>> = None;
        let mut prefetch_handle: Option<JoinHandle<Result<Option<Rc<ActiveRange>>>>> = None;
        let mut no_more_work_hint = false;
        let mut range_started_at = Instant::now();
        let mut range_start_cursor = 0_u64;
        let mut local_cursor = 0_u64;

        loop {
            if self.control.is_halted() || self.should_exit_for_scale_down() {
                break;
            }

            if self
                .collect_prefetch_result(&mut prefetch_handle, &mut prefetched_range, false)
                .await?
            {
                no_more_work_hint = true;
            }

            if current_range.is_none() {
                if let Some(range) = prefetched_range.take() {
                    SchedulerMetrics::add(&self.metrics.prefetch_hits, 1);
                    current_range = Some(range.clone());
                    range_started_at = Instant::now();
                    local_cursor = range.cursor.get();
                    range_start_cursor = local_cursor;
                } else {
                    current_range = self.request_work(false).await?;
                    if let Some(range) = &current_range {
                        range_started_at = Instant::now();
                        local_cursor = range.cursor.get();
                        range_start_cursor = local_cursor;
                    } else if no_more_work_hint {
                        break;
                    } else {
                        break;
                    }
                }
            }

            let range = current_range.as_ref().unwrap().clone();
            let end = range.end.get();
            if local_cursor >= end {
                range.status.set(RANGE_STATUS_FINISHED);
                current_range = None;
                continue;
            }

            let step = DRY_RUN_STEP_BYTES.min(end - local_cursor);
            let new_pos = local_cursor + step;
            range.cursor.set(new_pos);
            self.update_range_speed_sample(&range, new_pos);
            self.global_downloaded
                .set(self.global_downloaded.get().saturating_add(step));
            self.index_state.mark_completed_span(local_cursor, new_pos);
            local_cursor = new_pos;
            self.worker_control
                .transferred_bytes
                .set(self.worker_control.transferred_bytes.get().saturating_add(step));
            let recent_speed_bps = estimate_speed_bps(range_started_at, range_start_cursor, new_pos);

            let remaining = end.saturating_sub(new_pos);
            let prefetch_trigger_bytes = compute_prefetch_trigger_bytes(
                remaining,
                recent_speed_bps,
                self.effective_prefetch_limit_bytes(),
                self.scaler.last_protocol.get(),
                self.ewma_connection_rtt_ms.get(),
            );
            SchedulerMetrics::update_max(&self.metrics.max_prefetch_trigger_bytes, prefetch_trigger_bytes);
            if should_prefetch(
                remaining,
                recent_speed_bps,
                self.effective_prefetch_limit_bytes(),
                self.scaler.last_protocol.get(),
                self.ewma_connection_rtt_ms.get(),
            )
                && prefetch_handle.is_none()
                && prefetched_range.is_none()
                && !no_more_work_hint
            {
                SchedulerMetrics::add(&self.metrics.prefetch_requests, 1);
                prefetch_handle = Some(self.spawn_prefetch_request());
            }

            tokio::time::sleep(Duration::from_millis(DRY_RUN_STEP_DELAY_MS)).await;
        }

        if let Some(handle) = prefetch_handle {
            handle.abort();
        }
        Ok(())
    }

    async fn run_live(self) -> Result<()> {
        let worker_started_at = Instant::now();
        let file_open_started = Instant::now();
        let mut file = storage::open_download_file_for_write(&self.file_path).await?;
        let file_backend = file.backend();
        let open_file_ms = file_open_started.elapsed().as_millis() as u64;

        SchedulerMetrics::add(&self.metrics.startup_open_file_ms, open_file_ms);

        let (write_tx, mut write_rx) = tokio::sync::mpsc::channel::<(u64, Vec<u8>)>(2);
        let (recycle_tx, mut recycle_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let metrics_clone = std::rc::Rc::clone(&self.metrics);
        let writer_handle = tokio::task::spawn_local(async move {
            while let Some((offset, mut data)) = write_rx.recv().await {
                let write_started = Instant::now();
                if let Err(e) = file.write_all_at(offset, &data).await {
                    return Err(e);
                }
                let write_ms = write_started.elapsed().as_millis() as u64;
                SchedulerMetrics::add(&metrics_clone.file_write_ms, write_ms);
                
                data.clear();
                let _ = recycle_tx.send(data);
            }
            Ok::<(), anyhow::Error>(())
        });
        let mut startup = StartupProbe {
            worker_started_at,
            open_file_ms,
            first_assignment_wait_ms: None,
            first_request_setup_ms: None,
            first_byte_ms: None,
            total_to_first_byte_ms: None,
            logged: false,
        };
        let mut current_range: Option<Rc<ActiveRange>> = None;
        let mut prefetched_range: Option<Rc<ActiveRange>> = None;
        let mut prefetch_handle: Option<JoinHandle<Result<Option<Rc<ActiveRange>>>>> = None;
        let mut no_more_work_hint = false;
        let mut range_started_at = Instant::now();
        let mut range_start_cursor = 0_u64;
        let mut local_cursor = 0_u64;
        let mut current_range_id: Option<u64> = None;
        let mut consecutive_failures = 0_u32;
        let mut range_wait_started = Instant::now();
        let mut pending_write = PendingWrite {
            start_offset: 0,
            data: Vec::with_capacity(WRITE_BUFFER_MIN_BYTES),
            target_bytes: WRITE_BUFFER_MIN_BYTES,
        };

        loop {
            if self.control.is_halted() {
                break;
            }

            if self
                .collect_prefetch_result(&mut prefetch_handle, &mut prefetched_range, false)
                .await?
            {
                no_more_work_hint = true;
            }

            if current_range.is_none() {
                if let Some(range) = prefetched_range.take() {
                    SchedulerMetrics::add(&self.metrics.prefetch_hits, 1);
                    current_range = Some(range.clone());
                    self.note_first_assignment(&mut startup, range_wait_started);
                    range_started_at = Instant::now();
                    local_cursor = range.cursor.get();
                    range_start_cursor = local_cursor;
                    current_range_id = Some(range.id);
                    consecutive_failures = 0;
                    self.log_msg(&format!(
                        "range#{} assigned via prefetch wait_ms={} bytes={}..{} support={}..{}MB",
                        range.id,
                        range_wait_started.elapsed().as_millis(),
                        range.byte_start,
                        range.end.get(),
                        range.label_start_mb,
                        range.label_end_mb
                    ))
                    .await;
                } else {
                    current_range = self.request_work(false).await?;
                    if let Some(range) = &current_range {
                        self.note_first_assignment(&mut startup, range_wait_started);
                        range_started_at = Instant::now();
                        local_cursor = range.cursor.get();
                        range_start_cursor = local_cursor;
                        current_range_id = Some(range.id);
                        consecutive_failures = 0;
                        self.log_msg(&format!(
                            "range#{} assigned wait_ms={} bytes={}..{} support={}..{}MB",
                            range.id,
                            range_wait_started.elapsed().as_millis(),
                            range.byte_start,
                            range.end.get(),
                            range.label_start_mb,
                            range.label_end_mb
                        ))
                        .await;
                    } else if no_more_work_hint {
                        break;
                    } else {
                        break;
                    }
                }
            }

            let range = current_range.as_ref().unwrap().clone();
            if current_range_id != Some(range.id) {
                current_range_id = Some(range.id);
                consecutive_failures = 0;
            }
            let start = local_cursor;
            let end = range.end.get();

            if start >= end {
                range.status.set(RANGE_STATUS_FINISHED);
                current_range = None;
                current_range_id = None;
                consecutive_failures = 0;
                continue;
            }

            SchedulerMetrics::add(&self.metrics.http_requests, 1);
            let request_started = Instant::now();
            let mut made_progress_this_attempt = false;
            let mut attempt_timing = AttemptTiming::default();
            let response = match send_request_follow_redirects(
                &self.client,
                Method::GET,
                &self.url,
                Some((start, end - 1)),
            )
            .await
            {
                Ok(res) => res,
                Err(e) => {
                    let reason = format!("request error: {}", e);
                    let retry_hint = classify_error_retry(&reason, false);
                    consecutive_failures = self
                        .handle_range_retry(
                            &range,
                            start,
                            end,
                            consecutive_failures,
                            &reason,
                            retry_hint,
                        )
                        .await?;
                    continue;
                }
            };
            attempt_timing.request_setup_ms = request_started.elapsed().as_millis() as u64;
            SchedulerMetrics::add(&self.metrics.http_setup_ms, attempt_timing.request_setup_ms);
            self.note_first_request_setup(&mut startup, attempt_timing.request_setup_ms);

            let protocol_family = protocol_family_for_version(response.version());
            let http_version = http_version_label(response.version());
            record_protocol_request_metric(&self.metrics, protocol_family);

            if !response.status().is_success() {
                if response.status().as_u16() == 429 {
                    self.origin_memory
                        .borrow_mut()
                        .note_rate_limit(&self.origin);
                    self.log_msg("origin signalled rate limit (429)").await;
                }
                consecutive_failures = self
                    .handle_range_retry(
                        &range,
                        start,
                        end,
                        consecutive_failures,
                        &format!("HTTP error: {}", response.status()),
                        classify_http_status_retry(response.status()),
                    )
                    .await?;
                continue;
            }

            let supports_ranges = response.status() == http::StatusCode::PARTIAL_CONTENT
                || response.headers().contains_key(CONTENT_RANGE)
                || response
                    .headers()
                    .get(ACCEPT_RANGES)
                    .and_then(|value| value.to_str().ok())
                    .map(|value| value.eq_ignore_ascii_case("bytes"))
                    .unwrap_or(false);
            self.origin_memory
                .borrow_mut()
                .note_range_support(&self.origin, supports_ranges);

            let mut stream = response.into_body();
            let mut stream_failed = None::<String>;
            let stream_started = Instant::now();
            let mut first_chunk_at: Option<Instant> = None;
            while let Some(frame_result) = stream.frame().await {
                if self.control.is_halted() {
                    self
                        .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                        .await?;
                    return Ok(());
                }

                let frame = match frame_result {
                    Ok(frame) => frame,
                    Err(e) => {
                        stream_failed = Some(format!("stream error: {}", e));
                        break;
                    }
                };

                let chunk = match frame.into_data() {
                    Ok(chunk) => chunk,
                    Err(_) => continue,
                };

                while !self.bucket.consume(chunk.len()) {
                    tokio::task::yield_now().await;
                }

                if first_chunk_at.is_none() {
                    first_chunk_at = Some(Instant::now());
                    attempt_timing.first_byte_ms = stream_started.elapsed().as_millis() as u64;
                    SchedulerMetrics::add(&self.metrics.http_ttfb_ms, attempt_timing.first_byte_ms);
                    let total_ttfb_ms = attempt_timing
                        .request_setup_ms
                        .saturating_add(attempt_timing.first_byte_ms);
                    self
                        .record_request_classification(
                            &mut attempt_timing,
                            total_ttfb_ms,
                            protocol_family,
                        )
                        .await;
                    if startup.first_byte_ms.is_none() {
                        startup.first_byte_ms = Some(attempt_timing.first_byte_ms);
                        startup.total_to_first_byte_ms =
                            Some(startup.worker_started_at.elapsed().as_millis() as u64);
                        SchedulerMetrics::add(
                            &self.metrics.startup_first_byte_ms,
                            attempt_timing.first_byte_ms,
                        );
                        SchedulerMetrics::add(
                            &self.metrics.startup_total_to_first_byte_ms,
                            startup.total_to_first_byte_ms.unwrap_or_default(),
                        );
                        self.note_first_byte(&mut startup, file_backend).await;
                    }
                }

                let max_end = range.end.get();
                if local_cursor >= max_end {
                    self
                        .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                        .await?;
                    current_range = None;
                    break;
                }

                let to_write = (max_end - local_cursor).min(chunk.len() as u64) as usize;
                self.append_pending_write(&mut pending_write, local_cursor, &chunk[..to_write]);

                let new_pos = local_cursor + to_write as u64;
                range.cursor.set(new_pos);
                self.update_range_speed_sample(&range, new_pos);
                self.global_downloaded.set(
                    self.global_downloaded
                        .get()
                        .saturating_add(to_write as u64),
                );
                self.index_state.mark_completed_span(local_cursor, new_pos);
                made_progress_this_attempt = true;
                attempt_timing.bytes_written = attempt_timing.bytes_written.saturating_add(to_write as u64);
                attempt_timing.chunks = attempt_timing.chunks.saturating_add(1);
                self.worker_control.transferred_bytes.set(
                    self.worker_control
                        .transferred_bytes
                        .get()
                        .saturating_add(to_write as u64),
                );
                local_cursor = new_pos;
                let recent_speed_bps = estimate_speed_bps(range_started_at, range_start_cursor, new_pos);
                self.update_pending_write_target(&mut pending_write, recent_speed_bps);
                if pending_write.data.len() >= pending_write.target_bytes {
                    self
                        .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                        .await?;
                }

                let remaining = max_end.saturating_sub(new_pos);
                let prefetch_trigger_bytes = compute_prefetch_trigger_bytes(
                    remaining,
                    recent_speed_bps,
                    self.effective_prefetch_limit_bytes(),
                    self.scaler.last_protocol.get(),
                    self.ewma_connection_rtt_ms.get(),
                );
                SchedulerMetrics::update_max(&self.metrics.max_prefetch_trigger_bytes, prefetch_trigger_bytes);
                if should_prefetch(
                    remaining,
                    recent_speed_bps,
                    self.effective_prefetch_limit_bytes(),
                    self.scaler.last_protocol.get(),
                    self.ewma_connection_rtt_ms.get(),
                )
                    && prefetch_handle.is_none()
                    && prefetched_range.is_none()
                    && !no_more_work_hint
                {
                    SchedulerMetrics::add(&self.metrics.prefetch_requests, 1);
                    prefetch_handle = Some(self.spawn_prefetch_request());
                    self.log_msg(&format!(
                        "prefetch trigger remaining={} trigger_bytes={} recent_speed_bps={:.0}",
                        remaining, prefetch_trigger_bytes, recent_speed_bps
                    ))
                    .await;
                }

                if self
                    .collect_prefetch_result(&mut prefetch_handle, &mut prefetched_range, true)
                    .await?
                {
                    no_more_work_hint = true;
                }

                if self.should_exit_for_scale_down() {
                    self
                        .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                        .await?;
                    self.relinquish_range(&range, local_cursor).await;
                    if let Some(handle) = prefetch_handle {
                        handle.abort();
                    }
                    drop(write_tx);
                    let _ = writer_handle.await;
                    return Ok(());
                }

                if to_write < chunk.len() {
                    self
                        .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                        .await?;
                    current_range = None;
                    current_range_id = None;
                    consecutive_failures = 0;
                    range_wait_started = Instant::now();
                    self.reset_pending_write_target(&mut pending_write);
                    break;
                }
            }

            attempt_timing.stream_ms = stream_started.elapsed().as_millis() as u64;
            SchedulerMetrics::add(&self.metrics.http_stream_ms, attempt_timing.stream_ms);

            if let Some(reason) = stream_failed {
                self
                    .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                    .await?;
                if made_progress_this_attempt {
                    consecutive_failures = 0;
                    self.log_attempt_summary(&range, start, end, &attempt_timing, "reopen", http_version)
                        .await;
                    self.log_msg(&format!(
                        "{}; reopening range#{} from byte={}",
                        reason,
                        range.id,
                        range.cursor.get()
                    ))
                    .await;
                    self.reset_pending_write_target(&mut pending_write);
                } else {
                    let retry_hint = classify_error_retry(&reason, made_progress_this_attempt);
                    consecutive_failures = self
                        .handle_range_retry(
                            &range,
                            range.cursor.get(),
                            end,
                            consecutive_failures,
                            &reason,
                            retry_hint,
                        )
                        .await?;
                }
                continue;
            }

            if made_progress_this_attempt {
                consecutive_failures = 0;
            }
            if local_cursor >= range.end.get() {
                self
                    .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                    .await?;
                range.status.set(RANGE_STATUS_FINISHED);
                SchedulerMetrics::add(&self.metrics.completed_ranges, 1);
                self.log_attempt_summary(&range, start, end, &attempt_timing, "complete", http_version)
                    .await;
                current_range = None;
                current_range_id = None;
                consecutive_failures = 0;
                range_wait_started = Instant::now();
                self.reset_pending_write_target(&mut pending_write);
            } else if made_progress_this_attempt {
                self
                    .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut attempt_timing)
                    .await?;
                self.log_attempt_summary(&range, start, end, &attempt_timing, "partial", http_version)
                    .await;
                self.reset_pending_write_target(&mut pending_write);
            }
        }

        if !pending_write.data.is_empty() {
            let mut final_timing = AttemptTiming::default();
            self
                .flush_pending_write(&write_tx, &mut recycle_rx, &mut pending_write, &mut final_timing)
                .await?;
        }
        if let Some(handle) = prefetch_handle {
            handle.abort();
        }
        drop(write_tx);
        if let Err(e) = writer_handle.await.unwrap_or(Ok(())) {
            self.log_msg(&format!("Writer task failed: {}", e)).await;
            return Err(e);
        }
        Ok(())
    }

    async fn handle_range_retry(
        &self,
        range: &Rc<ActiveRange>,
        current_start: u64,
        current_end: u64,
        consecutive_failures: u32,
        reason: &str,
        retry_hint: RetryHint,
    ) -> Result<u32> {
        let next_failures = consecutive_failures.saturating_add(1);
        SchedulerMetrics::add(&self.metrics.retry_attempts, 1);

        if matches!(retry_hint, RetryHint::Abort) || next_failures > MAX_RANGE_RETRIES {
            return Err(anyhow!(
                "range#{} failed after {} retries at bytes {}..{}: {}",
                range.id,
                consecutive_failures,
                current_start,
                current_end,
                reason
            ));
        }

        let delay_ms = match retry_hint {
            RetryHint::Immediate => 0,
            RetryHint::Backoff(ms) => ms,
            RetryHint::ReduceWorkers(ms) => {
                self.scaler.skip_growth_sample.set(true);
                self.scaler
                    .slow_start_remaining
                    .set(self.scaler.slow_start_remaining.get().saturating_add(1));
                ms
            }
            RetryHint::ShrinkRange(ms) => ms,
            RetryHint::Abort => 0,
        };
        SchedulerMetrics::add(&self.metrics.retry_wait_ms, delay_ms);
        self.log_msg(&format!(
            "{}; retry_hint={:?} retry {}/{} after {}ms on range#{} bytes={}..{}",
            reason,
            retry_hint,
            next_failures,
            MAX_RANGE_RETRIES,
            delay_ms,
            range.id,
            current_start,
            current_end
        ))
        .await;
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        Ok(next_failures)
    }

    async fn log_attempt_summary(
        &self,
        range: &Rc<ActiveRange>,
        requested_start: u64,
        requested_end: u64,
        attempt_timing: &AttemptTiming,
        outcome: &str,
        http_version: &str,
    ) {
        self.log_msg(&format!(
            "range#{} {} version={} kind={:?} requested={}..{} advanced_to={} setup_ms={} first_byte_ms={} stream_ms={} write_ms={} handshake_ms={} bytes={} chunks={}",
            range.id,
            outcome,
            http_version,
            attempt_timing.request_kind,
            requested_start,
            requested_end,
            range.cursor.get(),
            attempt_timing.request_setup_ms,
            attempt_timing.first_byte_ms,
            attempt_timing.stream_ms,
            attempt_timing.write_ms,
            attempt_timing.handshake_cost_ms,
            attempt_timing.bytes_written,
            attempt_timing.chunks,
        ))
        .await;
    }

    fn spawn_prefetch_request(&self) -> JoinHandle<Result<Option<Rc<ActiveRange>>>> {
        let coordinator_tx = self.coordinator_tx.clone();
        let connection_id = self.connection_id;
        let metrics = self.metrics.clone();
        tokio::task::spawn_local(async move { request_work_inner(coordinator_tx, connection_id, metrics).await })
    }

    async fn collect_prefetch_result(
        &self,
        handle: &mut Option<JoinHandle<Result<Option<Rc<ActiveRange>>>>>,
        prefetched: &mut Option<Rc<ActiveRange>>,
        wait_even_if_not_finished: bool,
    ) -> Result<bool> {
        let should_take = handle
            .as_ref()
            .map(|task| wait_even_if_not_finished || task.is_finished())
            .unwrap_or(false);

        if should_take {
            let result = handle.take().unwrap().await??;
            if result.is_some() {
                SchedulerMetrics::add(&self.metrics.prefetch_ready, 1);
            }
            let exhausted = result.is_none();
            *prefetched = result;
            return Ok(exhausted);
        }

        Ok(false)
    }

    async fn request_work(&self, prefetched: bool) -> Result<Option<Rc<ActiveRange>>> {
        if prefetched {
            SchedulerMetrics::add(&self.metrics.prefetch_requests, 1);
        }
        self.log_msg(&format!("requesting_work prefetched={}", prefetched))
            .await;
        request_work_inner(
            self.coordinator_tx.clone(),
            self.connection_id,
            self.metrics.clone(),
        )
        .await
    }
}

async fn request_work_inner(
    coordinator_tx: mpsc::Sender<WorkRequest>,
    connection_id: u32,
    metrics: Rc<SchedulerMetrics>,
) -> Result<Option<Rc<ActiveRange>>> {
    let waited = Instant::now();
    SchedulerMetrics::add(&metrics.work_requests, 1);
    let (tx, rx) = oneshot::channel();
    if coordinator_tx
        .send(WorkRequest { connection_id, tx })
        .await
        .is_err()
    {
        return Ok(None);
    }

    let result = match rx.await {
        Ok(range) => range,
        Err(_) => None,
    };
    SchedulerMetrics::add(&metrics.request_wait_ms, waited.elapsed().as_millis() as u64);
    Ok(result)
}

fn snapshot_downloaded(coordinator: &Coordinator, total_size: u64) -> u64 {
    let downloaded = coordinator
        .dl_ranges
        .iter()
        .map(|range| {
            let cursor = range.cursor.get();
            let end = range.end.get();
            cursor.min(end).saturating_sub(range.byte_start)
        })
        .sum::<u64>();

    downloaded.min(total_size)
}

fn is_tail_phase_bytes(downloaded: u64, total_size: u64) -> bool {
    if total_size == 0 {
        return false;
    }

    downloaded.saturating_mul(100) >= total_size.saturating_mul(95)
}

fn build_fib_mb() -> Vec<u64> {
    let mut fib = vec![1_u64, 2_u64];
    let max_mb = u64::MAX / MB;
    loop {
        let len = fib.len();
        let next = match fib[len - 1].checked_add(fib[len - 2]) {
            Some(v) if v <= max_mb => v,
            _ => break,
        };
        fib.push(next);
    }
    fib
}

fn choose_seed_start_idx(fib_mb: &[u64], support_idx: usize, connections: usize, dry_run: bool) -> usize {
    if dry_run || support_idx == 0 {
        return 0;
    }

    let desired_start = fib_mb
        .iter()
        .position(|value| *value >= LIVE_SEED_FLOOR_MB)
        .unwrap_or(0);
    let max_start_with_enough_lanes = support_idx.saturating_sub(connections.max(1));
    desired_start.min(max_start_with_enough_lanes)
}

fn build_seed_ranges(
    fib_mb: &[u64],
    seed_start_idx: usize,
    support_idx: usize,
    total_size: u64,
) -> Vec<RangeSpec> {
    let mut ranges = Vec::with_capacity(support_idx.saturating_sub(seed_start_idx));
    let mut byte_start = 0_u64;

    for idx in seed_start_idx..support_idx {
        let label_start_mb = fib_mb[idx];
        let label_end_mb = fib_mb[idx + 1];
        let byte_end = ((label_end_mb as u128) * (MB as u128))
            .min(total_size as u128) as u64;

        if byte_end <= byte_start {
            continue;
        }

        ranges.push(RangeSpec {
            id: ranges.len() as u64,
            label_start_mb,
            label_end_mb,
            byte_start,
            byte_end,
        });

        byte_start = byte_end;
        if byte_start >= total_size {
            break;
        }
    }

    if let Some(last) = ranges.last_mut() {
        last.byte_end = total_size;
    }

    ranges
}

fn build_equal_ranges(total_size: u64, connections: usize) -> Vec<RangeSpec> {
    if total_size == 0 || connections == 0 {
        return Vec::new();
    }

    let lanes = connections.min(total_size.div_ceil(MB) as usize).max(1);
    let mut ranges = Vec::with_capacity(lanes);
    let mut start = 0_u64;

    for idx in 0..lanes {
        let remaining_bytes = total_size.saturating_sub(start);
        let remaining_lanes = (lanes - idx) as u64;
        let chunk_size = remaining_bytes.div_ceil(remaining_lanes);
        let end = if idx + 1 == lanes {
            total_size
        } else {
            (start + chunk_size).min(total_size)
        };

        if end <= start {
            continue;
        }

        ranges.push(RangeSpec {
            id: ranges.len() as u64,
            label_start_mb: bytes_to_floor_mb(start),
            label_end_mb: bytes_to_ceiling_mb(end),
            byte_start: start,
            byte_end: end,
        });
        start = end;
    }

    ranges
}

fn build_phi_geometric_ranges(
    file_size: u64,
    n: usize,
    max_ratio: f64,
    block_size: u64,
) -> Vec<RangeSpec> {
    if file_size == 0 || n == 0 {
        return Vec::new();
    }
    if n == 1 {
        return vec![RangeSpec {
            id: 0,
            label_start_mb: bytes_to_floor_mb(0),
            label_end_mb: bytes_to_ceiling_mb(file_size),
            byte_start: 0,
            byte_end: file_size,
        }];
    }

    let phi: f64 = 1.6180339887498948482;
    let alpha = max_ratio.ln() / ((n - 1) as f64 * phi.ln());

    let mut weights = Vec::with_capacity(n);
    let mut w_sum = 0.0;
    for i in 0..n {
        let w = phi.powf(alpha * (i as f64));
        weights.push(w);
        w_sum += w;
    }

    let mut boundaries = Vec::with_capacity(n + 1);
    boundaries.push(0_u64);

    let mut cumsum = 0.0;
    for i in 0..n - 1 {
        cumsum += file_size as f64 * weights[i] / w_sum;
        let mut boundary = (cumsum / block_size as f64).round() as u64 * block_size;
        boundary = boundary.clamp(boundaries[i], file_size);
        boundaries.push(boundary);
    }
    boundaries.push(file_size);

    if boundaries[1] - boundaries[0] < block_size {
        return build_equal_ranges(file_size, n);
    }

    let mut ranges = Vec::with_capacity(n);
    for i in 0..n {
        let byte_start = boundaries[i];
        let byte_end = boundaries[i + 1];
        if byte_end > byte_start {
            ranges.push(RangeSpec {
                id: i as u64,
                label_start_mb: bytes_to_floor_mb(byte_start),
                label_end_mb: bytes_to_ceiling_mb(byte_end),
                byte_start,
                byte_end,
            });
        }
    }

    ranges
}

fn estimate_speed_bps(started_at: Instant, start_offset: u64, current_offset: u64) -> f64 {
    let elapsed = started_at.elapsed().as_secs_f64();
    if elapsed <= 0.0 {
        return 0.0;
    }
    current_offset.saturating_sub(start_offset) as f64 / elapsed
}

fn compute_prefetch_trigger_bytes(
    remaining_bytes: u64,
    recent_speed_bps: f64,
    borrow_limit_bytes: u64,
    protocol: ProtocolFamily,
    worker_rtt_ms: f64,
) -> u64 {
    let handshake_ms = protocol_prefetch_handshake_ms(protocol).max(worker_rtt_ms.round() as u64);
    let handshake_bytes = ((recent_speed_bps * (handshake_ms as f64 / 1000.0)).ceil())
        .max((LIVE_PREFETCH_MIN_MB * MB) as f64) as u64;
    let _ = remaining_bytes;
    handshake_bytes.max(borrow_limit_bytes)
}

fn should_prefetch(
    remaining_bytes: u64,
    recent_speed_bps: f64,
    borrow_limit_bytes: u64,
    protocol: ProtocolFamily,
    worker_rtt_ms: f64,
) -> bool {
    remaining_bytes
        <= compute_prefetch_trigger_bytes(
            remaining_bytes,
            recent_speed_bps,
            borrow_limit_bytes,
            protocol,
            worker_rtt_ms,
        )
}

fn update_scaler_signal_stats(scaler: &Rc<Scaler>, sample_bps: f64) -> (f64, f64) {
    {
        let mut ring = scaler.sample_ring.borrow_mut();
        ring[scaler.sample_head.get()] = sample_bps;
    }
    scaler
        .sample_head
        .set((scaler.sample_head.get() + 1) % 10);
    scaler
        .sample_count
        .set((scaler.sample_count.get() + 1).min(10));

    let count = scaler.sample_count.get();
    if count < 5 {
        scaler.alpha.set(0.3);
        scaler.cv.set(0.0);
        return (0.3, 0.0);
    }

    let ring = scaler.sample_ring.borrow();
    let values = &ring[..count];
    let mean = values.iter().sum::<f64>() / count as f64;
    if mean < 1.0 {
        scaler.alpha.set(0.3);
        scaler.cv.set(0.0);
        return (0.3, 0.0);
    }

    let variance = values
        .iter()
        .map(|sample| {
            let delta = *sample - mean;
            delta * delta
        })
        .sum::<f64>()
        / count as f64;
    let cv = variance.sqrt() / mean;
    let alpha = (0.5 - 0.4 * cv).clamp(0.10, 0.50);
    scaler.cv.set(cv);
    scaler.alpha.set(alpha);
    (alpha, cv)
}

fn compute_slow_start_heartbeats(scaler: &Rc<Scaler>, protocol: ProtocolFamily) -> u32 {
    let heartbeat_ms = scaler.config.borrow().heartbeat_ms.max(1) as f64;
    let estimated_ms = scaler.ewma_rtt_ms.get() * 12.0;
    let mut heartbeats = ((estimated_ms / heartbeat_ms).ceil() as u32).clamp(1, 4);
    match protocol {
        ProtocolFamily::Http1 => {
            if scaler.reuse_rate.get() < 0.60 {
                heartbeats = heartbeats.saturating_add(1).clamp(1, 4);
            }
        }
        ProtocolFamily::Http2 => {
            if scaler.reuse_rate.get() > 0.70 && scaler.reused_rtt_samples.get() >= 2 {
                heartbeats = heartbeats.saturating_sub(1).clamp(1, 4);
            }
        }
        ProtocolFamily::Other => {}
    }
    heartbeats
}

fn compute_heartbeat_ms(scaler: &Rc<Scaler>) -> u64 {
    let cv = scaler.cv.get().max(0.0);
    ((500.0 + (cv * 2500.0)).round() as u64).clamp(500, 3000)
}

fn compute_refill_interval_ms(global_bandwidth_limit_bps: u64) -> u64 {
    if global_bandwidth_limit_bps == 0 {
        return 50;
    }

    ((256_u64 * 1024).saturating_mul(1000) / global_bandwidth_limit_bps.max(1)).clamp(10, 100)
}

fn compute_effective_connection_budget(configured_budget: usize, available_mb: u64) -> usize {
    let configured_min = configured_budget.min(2).max(1);
    if available_mb < 256 {
        configured_min.max(configured_budget / 4)
    } else if available_mb < 512 {
        configured_min.max(configured_budget / 2)
    } else if available_mb < 1024 {
        configured_min.max(configured_budget.saturating_mul(3) / 4)
    } else {
        configured_budget
    }
}

fn compute_connection_cv(samples: &[f64]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    if mean < 1.0 {
        return None;
    }
    let variance = samples
        .iter()
        .map(|sample| {
            let delta = *sample - mean;
            delta * delta
        })
        .sum::<f64>()
        / samples.len() as f64;
    Some((variance.sqrt() / mean).max(0.0))
}

fn origin_key(url: &str) -> String {
    if let Ok(parsed) = Url::parse(url) {
        let scheme = parsed.scheme();
        let host = parsed.host_str().unwrap_or(url);
        let port = parsed.port_or_known_default().unwrap_or_default();
        return format!("{scheme}://{host}:{port}");
    }
    url.to_string()
}

fn origin_memory_path() -> PathBuf {
    if let Ok(xdg_cache_home) = std::env::var("XDG_CACHE_HOME") {
        return PathBuf::from(xdg_cache_home).join("tur").join("origin-memory.bin");
    }
    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home)
            .join(".cache")
            .join("tur")
            .join("origin-memory.bin");
    }
    PathBuf::from(".tur-origin-memory.bin")
}

async fn update_reuse_health(
    scaler: &Rc<Scaler>,
    url: &str,
    log_path: &Path,
    protocol: ProtocolFamily,
) {
    let now = Instant::now();
    if now.duration_since(scaler.last_reuse_reset.get()) >= Duration::from_secs(60) {
        scaler.reused_count.set(0);
        scaler.total_request_count.set(0);
        scaler.reuse_rate.set(1.0);
        scaler.last_reuse_reset.set(now);
    }

    let rate = if scaler.total_request_count.get() == 0 {
        scaler.reuse_rate.get()
    } else {
        scaler.reused_count.get() as f64 / scaler.total_request_count.get() as f64
    };
    scaler.reuse_rate.set(rate);
    let threshold = protocol_effective_add_threshold(protocol, rate).clamp(0.04, 0.15);
    scaler.effective_add_threshold.set(threshold);
    let (degraded_threshold, recovered_threshold) = protocol_reuse_thresholds(protocol);

    let was_low = scaler.reuse_health_low.get();
    if !was_low && rate < degraded_threshold {
        scaler.reuse_health_low.set(true);
        log_phase_a_info(
            log_path,
            &format!(
                "reuse health degraded url={} protocol={} reuse_rate={:.2} effective_add_threshold={:.2}",
                url,
                protocol.as_str(),
                rate,
                threshold
            ),
        )
        .await;
    } else if was_low && rate > recovered_threshold {
        scaler.reuse_health_low.set(false);
        log_phase_a_info(
            log_path,
            &format!(
                "reuse health recovered url={} protocol={} reuse_rate={:.2} effective_add_threshold={:.2}",
                url,
                protocol.as_str(),
                rate,
                threshold
            ),
        )
        .await;
    }
}

async fn log_phase_a_info(log_path: &Path, msg: &str) {
    if let Ok(mut f) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .await
    {
        let _ = f
            .write_all(format!("[{}] phase_a: {}\n", chrono::Local::now(), msg).as_bytes())
            .await;
    }
}

fn median_u64(values: &mut [u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[values.len() / 2]
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn classify_http_status_retry(status: http::StatusCode) -> RetryHint {
    match status.as_u16() {
        429 => RetryHint::ReduceWorkers(RETRY_MAX_DELAY_MS),
        503 => RetryHint::Immediate,
        500..=599 => RetryHint::Backoff(RETRY_BASE_DELAY_MS.saturating_mul(2)),
        416 => RetryHint::Abort,
        _ => RetryHint::Backoff(RETRY_BASE_DELAY_MS),
    }
}

fn classify_error_retry(reason: &str, made_progress: bool) -> RetryHint {
    let reason = reason.to_ascii_lowercase();
    if reason.contains("tls") || reason.contains("ssl") || reason.contains("handshake") {
        return RetryHint::ReduceWorkers(RETRY_MAX_DELAY_MS);
    }
    if reason.contains("connection reset") || reason.contains("broken pipe") {
        return if made_progress {
            RetryHint::ShrinkRange(RETRY_BASE_DELAY_MS)
        } else {
            RetryHint::ReduceWorkers(RETRY_BASE_DELAY_MS.saturating_mul(4))
        };
    }
    if reason.contains("timed out") || reason.contains("timeout") {
        return if made_progress {
            RetryHint::ShrinkRange(RETRY_BASE_DELAY_MS)
        } else {
            RetryHint::Backoff(RETRY_BASE_DELAY_MS.saturating_mul(3))
        };
    }
    RetryHint::Backoff(RETRY_BASE_DELAY_MS)
}

fn valid_slice_mask(total_size: u64, bucket_idx: usize) -> u8 {
    if total_size == 0 {
        return 0;
    }

    let total_slices = total_size.div_ceil(MB) as usize;
    let bucket_start_slice = bucket_idx * 8;
    if bucket_start_slice >= total_slices {
        return 0;
    }

    let remaining = total_slices - bucket_start_slice;
    if remaining >= 8 {
        0xFF
    } else {
        ((1_u16 << remaining) - 1) as u8
    }
}

fn http_version_label(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 => "HTTP/0.9",
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        Version::HTTP_2 => "HTTP/2",
        Version::HTTP_3 => "HTTP/3",
        _ => "HTTP/?",
    }
}

fn protocol_family_for_version(version: Version) -> ProtocolFamily {
    match version {
        Version::HTTP_09 | Version::HTTP_10 | Version::HTTP_11 => ProtocolFamily::Http1,
        Version::HTTP_2 => ProtocolFamily::Http2,
        _ => ProtocolFamily::Other,
    }
}

fn dominant_protocol_from_metrics(
    metrics: &Rc<SchedulerMetrics>,
    fallback: ProtocolFamily,
) -> ProtocolFamily {
    let http1 = metrics.http1_requests.get();
    let http2 = metrics.http2_requests.get();
    if http1 == 0 && http2 == 0 {
        fallback
    } else if http2 > http1 {
        ProtocolFamily::Http2
    } else {
        ProtocolFamily::Http1
    }
}

fn record_protocol_request_metric(metrics: &Rc<SchedulerMetrics>, protocol: ProtocolFamily) {
    match protocol {
        ProtocolFamily::Http1 => SchedulerMetrics::add(&metrics.http1_requests, 1),
        ProtocolFamily::Http2 => SchedulerMetrics::add(&metrics.http2_requests, 1),
        ProtocolFamily::Other => SchedulerMetrics::add(&metrics.http_other_requests, 1),
    }
}

fn record_protocol_scale_metric(
    metrics: &Rc<SchedulerMetrics>,
    protocol: ProtocolFamily,
    action: ScalerAction,
) {
    match (protocol, action) {
        (ProtocolFamily::Http1, ScalerAction::Grow) => {
            SchedulerMetrics::add(&metrics.http1_scale_adds, 1)
        }
        (ProtocolFamily::Http1, ScalerAction::Shrink) => {
            SchedulerMetrics::add(&metrics.http1_scale_drops, 1)
        }
        (ProtocolFamily::Http2, ScalerAction::Grow) => {
            SchedulerMetrics::add(&metrics.http2_scale_adds, 1)
        }
        (ProtocolFamily::Http2, ScalerAction::Shrink) => {
            SchedulerMetrics::add(&metrics.http2_scale_drops, 1)
        }
        _ => {}
    }
}

fn record_max_active_connections(metrics: &Rc<SchedulerMetrics>, n_active: usize) {
    let observed = n_active as u64;
    if observed > metrics.max_active_connections_observed.get() {
        metrics.max_active_connections_observed.set(observed);
    }
}

fn protocol_prefetch_handshake_ms(protocol: ProtocolFamily) -> u64 {
    match protocol {
        ProtocolFamily::Http2 => 250,
        ProtocolFamily::Http1 | ProtocolFamily::Other => LIVE_PREFETCH_HANDSHAKE_MS,
    }
}

fn protocol_growth_shield_min_samples(protocol: ProtocolFamily) -> u64 {
    match protocol {
        ProtocolFamily::Http1 => 1,
        ProtocolFamily::Http2 => MIN_REUSED_RTT_SAMPLES_FOR_GROWTH_SHIELD,
        ProtocolFamily::Other => MIN_REUSED_RTT_SAMPLES_FOR_GROWTH_SHIELD,
    }
}

fn protocol_growth_shield_multiplier(protocol: ProtocolFamily) -> f64 {
    match protocol {
        ProtocolFamily::Http1 => 1.5,
        ProtocolFamily::Http2 => 1.0,
        ProtocolFamily::Other => 1.0,
    }
}

fn protocol_reuse_thresholds(protocol: ProtocolFamily) -> (f64, f64) {
    match protocol {
        ProtocolFamily::Http1 => (0.60, 0.80),
        ProtocolFamily::Http2 => (0.35, 0.60),
        ProtocolFamily::Other => (0.50, 0.70),
    }
}

fn protocol_effective_add_threshold(protocol: ProtocolFamily, reuse_rate: f64) -> f64 {
    match protocol {
        ProtocolFamily::Http1 => {
            if reuse_rate < 0.60 {
                0.12
            } else if reuse_rate < 0.80 {
                0.08
            } else {
                0.06
            }
        }
        ProtocolFamily::Http2 => {
            if reuse_rate < 0.35 {
                0.08
            } else if reuse_rate < 0.60 {
                0.06
            } else {
                0.04
            }
        }
        ProtocolFamily::Other => {
            if reuse_rate < 0.50 {
                0.10
            } else {
                0.05
            }
        }
    }
}

fn compute_protocol_aware_steal_floor_bytes(
    protocol: ProtocolFamily,
    reuse_rate: f64,
    bytes_per_heartbeat: f64,
) -> u64 {
    let modifier = match protocol {
        ProtocolFamily::Http1 => {
            if reuse_rate < 0.60 { 1.15 } else { 1.0 }
        }
        ProtocolFamily::Http2 => {
            if reuse_rate > 0.60 { 0.75 } else { 0.90 }
        }
        ProtocolFamily::Other => 1.0,
    };
    let floor = match protocol {
        ProtocolFamily::Http2 => STORAGE_BLOCK_SIZE,
        ProtocolFamily::Http1 | ProtocolFamily::Other => 2 * STORAGE_BLOCK_SIZE,
    };
    (((bytes_per_heartbeat / 4.0) * modifier).round() as u64).max(floor)
}

fn compute_http2_client_tuning(expected_concurrency: usize) -> ClientTuning {
    let concurrency = expected_concurrency.max(1);
    let stream_window_bytes =
        ((concurrency as u32).saturating_mul(MB as u32)).clamp(4 * MB as u32, 16 * MB as u32);
    let connection_window_bytes = stream_window_bytes
        .saturating_mul(concurrency as u32)
        .clamp(16 * MB as u32, 64 * MB as u32);
    let max_send_buffer_bytes =
        (concurrency.saturating_mul(512 * 1024)).clamp(2 * MB as usize, 8 * MB as usize);
    ClientTuning {
        expected_concurrency: concurrency,
        http2_stream_window_bytes: stream_window_bytes,
        http2_connection_window_bytes: connection_window_bytes,
        http2_max_send_buffer_bytes: max_send_buffer_bytes,
        source: H2TuningSource::Default,
    }
}

fn learn_http2_client_tuning(
    expected_concurrency: usize,
    max_active_connections_observed: usize,
    ewma_rtt_ms: f64,
    ewma_throughput_bps: f64,
) -> ClientTuning {
    let observed_concurrency = max_active_connections_observed.max(1);
    let per_stream_throughput = (ewma_throughput_bps / observed_concurrency as f64).max(1.0);
    let bdp_bytes = (per_stream_throughput * (ewma_rtt_ms.max(1.0) / 1000.0)).round();
    let stream_window_bytes =
        ((bdp_bytes as u32).saturating_mul(4)).clamp(MB as u32, 16 * MB as u32);
    let connection_window_bytes = stream_window_bytes
        .saturating_mul(expected_concurrency.max(observed_concurrency) as u32)
        .clamp(16 * MB as u32, 64 * MB as u32);
    let max_send_buffer_bytes = ((stream_window_bytes as usize)
        .saturating_mul(expected_concurrency.max(observed_concurrency))
        / 2)
        .clamp(2 * MB as usize, 8 * MB as usize);
    ClientTuning {
        expected_concurrency: expected_concurrency.max(observed_concurrency),
        http2_stream_window_bytes: stream_window_bytes,
        http2_connection_window_bytes: connection_window_bytes,
        http2_max_send_buffer_bytes: max_send_buffer_bytes,
        source: H2TuningSource::LearnedOrigin,
    }
}

fn build_http_client(
    http_mode: HttpMode,
    expected_concurrency: usize,
    learned_tuning: Option<ClientTuning>,
) -> (DownloadHttpClient, ClientTuning) {
    let http = crate::connector::TunedConnector::new();
    let tuning = learned_tuning.unwrap_or_else(|| compute_http2_client_tuning(expected_concurrency));
    let https = match http_mode {
        HttpMode::Auto => HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .wrap_connector(http),
        HttpMode::Http1 => HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .wrap_connector(http),
        HttpMode::Http2 => HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http2()
            .wrap_connector(http),
    };

    let mut builder = HyperClient::builder(TokioExecutor::new());
    builder.pool_timer(TokioTimer::new());
    builder.pool_idle_timeout(Duration::from_secs(30));
    builder.pool_max_idle_per_host(32);
    builder.retry_canceled_requests(true);
    builder.http1_writev(true);
    builder.http2_adaptive_window(true);
    builder.http2_initial_stream_window_size(Some(tuning.http2_stream_window_bytes));
    builder.http2_initial_connection_window_size(Some(tuning.http2_connection_window_bytes));
    builder.http2_max_frame_size(Some(HTTP2_MAX_FRAME_BYTES));
    builder.http2_max_send_buf_size(tuning.http2_max_send_buffer_bytes);
    builder.http2_keep_alive_interval(Some(Duration::from_secs(TCP_KEEPALIVE_INTERVAL_SECS)));
    builder.http2_keep_alive_timeout(Duration::from_secs(TCP_KEEPALIVE_INTERVAL_SECS * 2));
    builder.http2_keep_alive_while_idle(true);
    builder.timer(TokioTimer::new());
    (builder.build(https), tuning)
}

async fn send_request_follow_redirects(
    client: &DownloadHttpClient,
    method: Method,
    url: &str,
    range: Option<(u64, u64)>,
) -> Result<hyper::Response<Incoming>> {
    let mut current_url = url.to_owned();

    for _ in 0..=MAX_REDIRECTS {
        let uri: Uri = current_url.parse()?;
        let mut builder = Request::builder()
            .method(method.clone())
            .uri(uri)
            .header(USER_AGENT, USER_AGENT_VALUE)
            .header(ACCEPT, "*/*");
        if let Some((start, end)) = range {
            builder = builder.header(RANGE, format!("bytes={}-{}", start, end));
        }
        let request = builder.body(Empty::<Bytes>::new())?;
        let response: hyper::Response<Incoming> = client.request(request).await?;

        if response.status().is_redirection() {
            let location = response
                .headers()
                .get(LOCATION)
                .and_then(|value: &http::HeaderValue| value.to_str().ok())
                .ok_or_else(|| anyhow!("redirect missing location header"))?;
            current_url = resolve_redirect_url(&current_url, location)?;
            continue;
        }

        return Ok(response);
    }

    Err(anyhow!("too many redirects for {}", url))
}

fn resolve_redirect_url(base: &str, location: &str) -> Result<String> {
    let base = Url::parse(base)?;
    Ok(base.join(location)?.to_string())
}

fn align_down(value: u64, alignment: u64) -> u64 {
    if alignment == 0 {
        return value;
    }
    (value / alignment) * alignment
}

fn log_path(task: &DownloadTask) -> PathBuf {
    log_root(task).join(format!("{}.log", task.filename))
}

fn metadata_path(task: &DownloadTask) -> PathBuf {
    meta_root(task).join(format!("{}.tur.meta", task.filename))
}

fn log_root(task: &DownloadTask) -> PathBuf {
    if let Some(root) = &task.log_root {
        return root.join("tur");
    }
    task.dir.join(".tur").join("logs")
}

fn meta_root(task: &DownloadTask) -> PathBuf {
    if let Some(root) = &task.log_root {
        return root.join("tur-meta");
    }
    task.dir.join(".tur").join("meta")
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn bytes_to_ceiling_mb(bytes: u64) -> u64 {
    bytes.div_ceil(MB)
}

fn bytes_to_floor_mb(bytes: u64) -> u64 {
    bytes / MB
}

fn persist_snapshot(path: &Path, snapshot: &TaskSnapshot) -> Result<()> {
    ensure_parent_dir(path)?;
    let bytes = bincode::serialize(snapshot)?;
    std::fs::write(path, bytes)?;
    Ok(())
}

fn load_snapshot(path: &Path) -> Result<TaskSnapshot> {
    let bytes = std::fs::read(path)?;
    Ok(bincode::deserialize(&bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_budget_scales_by_available_memory() {
        assert_eq!(compute_effective_connection_budget(32, 2048), 32);
        assert_eq!(compute_effective_connection_budget(32, 900), 24);
        assert_eq!(compute_effective_connection_budget(32, 400), 16);
        assert_eq!(compute_effective_connection_budget(32, 200), 8);
        assert_eq!(compute_effective_connection_budget(1, 200), 1);
    }

    #[test]
    fn connection_cv_requires_meaningful_samples() {
        assert!(compute_connection_cv(&[]).is_none());
        assert!(compute_connection_cv(&[0.0, 0.0]).is_none());
        let cv = compute_connection_cv(&[100.0, 100.0, 200.0, 200.0]).unwrap();
        assert!(cv > 0.0);
    }

    #[test]
    fn origin_key_normalizes_scheme_host_and_port() {
        assert_eq!(origin_key("https://example.com/path"), "https://example.com:443");
        assert_eq!(origin_key("http://example.com/test"), "http://example.com:80");
        assert_eq!(origin_key("not a url"), "not a url");
    }

    #[test]
    fn origin_phi_ratio_store_prunes_least_recently_used_entries() {
        let mut store = OriginPhiRatioStore::default();
        for idx in 0..ORIGIN_PHI_RATIO_CAPACITY {
            store.update_origin_ratio(format!("https://host{idx}.example:443"), 1.1);
        }
        assert_eq!(store.len(), ORIGIN_PHI_RATIO_CAPACITY);

        let keep_key = "https://host0.example:443";
        assert_eq!(store.ratio_for_origin(keep_key), 1.1);

        store.update_origin_ratio("https://new.example:443".to_string(), 1.9);
        assert_eq!(store.len(), ORIGIN_PHI_RATIO_CAPACITY);
        assert_eq!(store.current_ratio(keep_key), Some(1.1));
        assert_eq!(store.current_ratio("https://new.example:443"), Some(1.9));
    }

    #[test]
    fn protocol_thresholds_are_directionally_sensible() {
        assert!(protocol_effective_add_threshold(ProtocolFamily::Http1, 0.30)
            > protocol_effective_add_threshold(ProtocolFamily::Http2, 0.30));
        assert!(compute_protocol_aware_steal_floor_bytes(ProtocolFamily::Http2, 0.80, 0.0)
            < compute_protocol_aware_steal_floor_bytes(ProtocolFamily::Http1, 0.80, 0.0));
        assert_eq!(protocol_prefetch_handshake_ms(ProtocolFamily::Http2), 250);
    }

    #[test]
    fn http2_client_tuning_scales_with_expected_concurrency() {
        let small = compute_http2_client_tuning(2);
        let large = compute_http2_client_tuning(16);
        assert!(large.http2_stream_window_bytes >= small.http2_stream_window_bytes);
        assert!(large.http2_connection_window_bytes >= small.http2_connection_window_bytes);
        assert!(large.http2_max_send_buffer_bytes >= small.http2_max_send_buffer_bytes);
    }

    #[test]
    fn learned_http2_tuning_is_origin_scoped_and_pruned() {
        let mut store = OriginH2TuningStore::default();
        let tuning = learn_http2_client_tuning(4, 3, 120.0, 12.0 * MB as f64);
        store.update_origin_tuning("https://example.com:443".to_string(), tuning);
        assert_eq!(
            store.current_tuning("https://example.com:443").unwrap().source,
            H2TuningSource::LearnedOrigin
        );

        for idx in 0..ORIGIN_PHI_RATIO_CAPACITY {
            store.update_origin_tuning(
                format!("https://host{idx}.example:443"),
                compute_http2_client_tuning(2),
            );
        }
        assert!(store.len() <= ORIGIN_PHI_RATIO_CAPACITY);
    }
}
