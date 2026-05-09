use std::collections::HashMap;
use std::cell::Cell;
use std::fs::File as StdFile;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use http::header::{ACCEPT, CONTENT_LENGTH, LOCATION, RANGE, USER_AGENT};
use http::{Method, Request, Uri, Version};
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, LocalSet};
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
const MAX_RANGE_RETRIES: u32 = 8;
const RETRY_BASE_DELAY_MS: u64 = 250;
const RETRY_MAX_DELAY_MS: u64 = 2_000;
const DRY_RUN_STEP_BYTES: u64 = 256 * 1024;
const DRY_RUN_STEP_DELAY_MS: u64 = 4;
const WRITE_BUFFER_MIN_BYTES: usize = 64 * 1024;
const WRITE_BUFFER_MEDIUM_BYTES: usize = 256 * 1024;
const WRITE_BUFFER_LARGE_BYTES: usize = 512 * 1024;
const WRITE_BUFFER_MAX_BYTES: usize = MB as usize;
const WRITE_BUFFER_MEDIUM_SPEED_BPS: u64 = MB;
const WRITE_BUFFER_LARGE_SPEED_BPS: u64 = 3 * MB;
const WRITE_BUFFER_MAX_SPEED_BPS: u64 = 6 * MB;
const HTTP2_STREAM_WINDOW_BYTES: u32 = 8 * 1024 * 1024;
const HTTP2_CONNECTION_WINDOW_BYTES: u32 = 16 * 1024 * 1024;
const HTTP2_MAX_FRAME_BYTES: u32 = 256 * 1024;
const HTTP2_MAX_SEND_BUFFER_BYTES: usize = 2 * MB as usize;
const TCP_KEEPALIVE_SECS: u64 = 60;
const TCP_KEEPALIVE_INTERVAL_SECS: u64 = 20;
const TCP_KEEPALIVE_RETRIES: u32 = 3;
const GOLDEN_RATIO_NUM: u64 = 633;
const GOLDEN_RATIO_DEN: u64 = 1024;
const MAX_REDIRECTS: usize = 8;
const USER_AGENT_VALUE: &str = concat!("tur/", env!("CARGO_PKG_VERSION"));

type DownloadHttpClient = HyperClient<HttpsConnector<HttpConnector>, Empty<Bytes>>;

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
    halt_mode: AtomicU8,
    cancel_flag: AtomicBool,
}

impl RuntimeControl {
    fn new() -> Self {
        Self {
            halt_mode: AtomicU8::new(HaltMode::Running as u8),
            cancel_flag: AtomicBool::new(false),
        }
    }

    fn halt_mode(&self) -> HaltMode {
        match self.halt_mode.load(Ordering::Acquire) {
            1 => HaltMode::PauseMemory,
            2 => HaltMode::PersistToDisk,
            _ => HaltMode::Running,
        }
    }

    fn request_pause(&self) {
        self.halt_mode
            .store(HaltMode::PauseMemory as u8, Ordering::Release);
        self.cancel_flag.store(true, Ordering::Release);
    }

    fn request_persist(&self) {
        self.halt_mode
            .store(HaltMode::PersistToDisk as u8, Ordering::Release);
        self.cancel_flag.store(true, Ordering::Release);
    }

    fn is_halted(&self) -> bool {
        self.halt_mode() != HaltMode::Running || self.cancel_flag.load(Ordering::Acquire)
    }
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
}

impl SchedulerMetrics {
    fn summary_line(&self) -> String {
        format!(
            "metrics direct_assignments={} borrow_assignments={} bytes_borrowed={} straggler_splits={} tail_splits={} work_requests={} request_wait_ms={} prefetch_requests={} prefetch_ready={} prefetch_hits={} http_requests={} http_setup_ms={} http_ttfb_ms={} http_stream_ms={} file_write_ms={} completed_ranges={} retry_attempts={} retry_wait_ms={} startup_workers={} startup_open_file_ms={} startup_first_assignment_wait_ms={} startup_first_request_setup_ms={} startup_first_byte_ms={} startup_total_to_first_byte_ms={}",
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
        )
    }

    fn add(cell: &Cell<u64>, value: u64) {
        cell.set(cell.get().saturating_add(value));
    }
}

#[derive(Debug, Clone)]
pub struct DownloadEngine {
    pub connections_per_download: usize,
    pub max_concurrent_tasks: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RangeSpec {
    id: u64,
    label_start_mb: u64,
    label_end_mb: u64,
    byte_start: u64,
    byte_end: u64,
}

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
    pub fn new(connections_per_download: usize, max_concurrent_tasks: usize) -> Self {
        Self {
            connections_per_download,
            max_concurrent_tasks,
        }
    }

    pub async fn run(
        &self,
        mut cmd_rx: mpsc::Receiver<EngineCommand>,
        cmd_tx: mpsc::Sender<EngineCommand>,
        event_tx: mpsc::Sender<EngineEvent>,
    ) -> Result<()> {
        let mut active_controls: HashMap<Uuid, Arc<RuntimeControl>> = HashMap::new();
        let mut paused_tasks: HashMap<Uuid, TaskSnapshot> = HashMap::new();
        let mut persisted_paths: HashMap<Uuid, PathBuf> = HashMap::new();

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                EngineCommand::Add(task) => {
                    let control = Arc::new(RuntimeControl::new());
                    active_controls.insert(task.id, control.clone());
                    self.spawn_download_task(task, None, control, cmd_tx.clone(), event_tx.clone());
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

                    let control = Arc::new(RuntimeControl::new());
                    active_controls.insert(id, control.clone());
                    self.spawn_download_task(
                        snapshot.task.clone(),
                        Some(snapshot),
                        control,
                        cmd_tx.clone(),
                        event_tx.clone(),
                    );
                }
                EngineCommand::RuntimeStopped(snapshot, halt_mode) => {
                    active_controls.remove(&snapshot.task.id);
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

        Ok(())
    }

    fn spawn_download_task(
        &self,
        task: DownloadTask,
        snapshot: Option<TaskSnapshot>,
        control: Arc<RuntimeControl>,
        cmd_tx: mpsc::Sender<EngineCommand>,
        event_tx: mpsc::Sender<EngineEvent>,
    ) {
        let default_connections = self.connections_per_download;
        std::thread::spawn(move || {
            let task_id = task.id;
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(err) => {
                    let _ = event_tx.blocking_send(EngineEvent::StatusChanged(
                        task_id,
                        DownloadStatus::Error(format!("failed to create download runtime: {}", err)),
                    ));
                    return;
                }
            };
            let result = runtime.block_on(run_download_task(
                task,
                snapshot,
                control,
                cmd_tx.clone(),
                event_tx.clone(),
                default_connections,
            ));
            if let Err(err) = result {
                let _ = event_tx.blocking_send(EngineEvent::StatusChanged(
                    task_id,
                    DownloadStatus::Error(err.to_string()),
                ));
            }
        });
    }
}

async fn run_download_task(
    task: DownloadTask,
    snapshot: Option<TaskSnapshot>,
    control: Arc<RuntimeControl>,
    cmd_tx: mpsc::Sender<EngineCommand>,
    event_tx: mpsc::Sender<EngineEvent>,
    default_connections: usize,
) -> Result<()> {
    let local = LocalSet::new();
    local
        .run_until(run_download_task_local(
            task,
            snapshot,
            control,
            cmd_tx,
            event_tx,
            default_connections,
        ))
        .await
}

async fn run_download_task_local(
    mut task: DownloadTask,
    snapshot: Option<TaskSnapshot>,
    control: Arc<RuntimeControl>,
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
    let mut coordinator = if let Some(snapshot) = snapshot {
        Coordinator::from_snapshot(
            snapshot.coordinator,
            snapshot.task.total_size,
            &log_path,
            task.schedule_mode,
            metrics.clone(),
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

    let (work_tx, work_rx) = mpsc::channel(128);
    let mut handles = Vec::with_capacity(task.connections);
    let http_client = build_http_client(task.http_mode);

    for connection_id in 0..task.connections {
        let worker = ConnectionWorker {
            connection_id: connection_id as u32,
            url: task.url.clone(),
            file_path: file_path.clone(),
            log_path: log_path.clone(),
            coordinator_tx: work_tx.clone(),
            global_downloaded: global_downloaded.clone(),
            control: control.clone(),
            dry_run: task.dry_run,
            borrow_limit_bytes: task.borrow_limit_mb * MB,
            metrics: metrics.clone(),
            client: http_client.clone(),
            index_state: index_state.clone(),
        };

        handles.push(tokio::task::spawn_local(async move { worker.run().await }));
    }
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

    for handle in handles {
        let _ = handle.await;
    }
    let _ = progress_handle.await;

    coordinator.log_summary(total_size);

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

    let client = build_http_client(task.http_mode);
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
    ) -> Result<Self> {
        let fib_mb = build_fib_mb();
        let ceil_mb = total_size.div_ceil(MB);
        let support_idx = fib_mb
            .iter()
            .position(|value| *value >= ceil_mb.max(1))
            .ok_or_else(|| anyhow!("Download exceeds generated Fibonacci range table"))?;

        let seed_start_idx = match schedule_mode {
            ScheduleMode::FibAdaptive => choose_adaptive_seed_start_idx(
                &fib_mb,
                support_idx,
                total_size,
                connections.max(1),
                dry_run,
            ),
            _ => choose_seed_start_idx(&fib_mb, support_idx, connections.max(1), dry_run),
        };
        let seed_ranges = build_initial_ranges(
            &fib_mb,
            seed_start_idx,
            support_idx,
            total_size,
            connections.max(1),
            schedule_mode,
        );
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
            borrow_cursor: 0,
            next_range_id: seed_ranges.len() as u64 + 1,
            total_size,
            index_state: Rc::new(IndexStateMap::new(total_size)),
            log_file: StdFile::create(log_path)?,
            metrics,
        };

        coordinator.log(&format!(
            "Coordinator started for task={} total_size={}B ceil_mb={} schedule_mode={} seed_floor_mb={} seed_start={}MB support_end={}MB borrow_limit={}MB dry_run={} index_state_bucket_mb={} index_state_buckets={} index_state_bytes={}",
            task_id,
            total_size,
            ceil_mb,
            schedule_mode.as_str(),
            if dry_run { 1 } else { LIVE_SEED_FLOOR_MB },
            fib_mb[seed_start_idx],
            fib_mb[support_idx],
            borrow_limit_mb.max(1),
            dry_run,
            INDEX_STATE_MB,
            coordinator.index_state.bucket_count(),
            coordinator.index_state.storage_bytes(),
        ));
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

    async fn run(&mut self, mut work_rx: mpsc::Receiver<WorkRequest>, control: Arc<RuntimeControl>) {
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
        if self.is_tail_phase() {
            self.borrow_limit_bytes.min(MB).max(MB)
        } else {
            self.borrow_limit_bytes
        }
    }

    fn is_tail_phase(&self) -> bool {
        if self.total_size == 0 {
            return false;
        }
        let completed = snapshot_downloaded(self, self.total_size);
        completed.saturating_mul(100) >= self.total_size.saturating_mul(95)
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
    file_path: PathBuf,
    log_path: PathBuf,
    coordinator_tx: mpsc::Sender<WorkRequest>,
    global_downloaded: Rc<Cell<u64>>,
    control: Arc<RuntimeControl>,
    dry_run: bool,
    borrow_limit_bytes: u64,
    metrics: Rc<SchedulerMetrics>,
    client: DownloadHttpClient,
    index_state: Rc<IndexStateMap>,
}

#[derive(Debug, Default)]
struct AttemptTiming {
    request_setup_ms: u64,
    first_byte_ms: u64,
    stream_ms: u64,
    write_ms: u64,
    bytes_written: u64,
    chunks: u64,
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

impl ConnectionWorker {
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

    async fn flush_pending_write(
        &self,
        file: &mut storage::DownloadFile,
        pending: &mut PendingWrite,
        attempt_timing: &mut AttemptTiming,
    ) -> Result<()> {
        if pending.data.is_empty() {
            return Ok(());
        }

        let write_started = Instant::now();
        file.write_all_at(pending.start_offset, &pending.data).await?;
        let write_ms = write_started.elapsed().as_millis() as u64;
        attempt_timing.write_ms = attempt_timing.write_ms.saturating_add(write_ms);
        SchedulerMetrics::add(&self.metrics.file_write_ms, write_ms);
        pending.data.clear();
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
        if speed_bps >= WRITE_BUFFER_MAX_SPEED_BPS {
            WRITE_BUFFER_MAX_BYTES
        } else if speed_bps >= WRITE_BUFFER_LARGE_SPEED_BPS {
            WRITE_BUFFER_LARGE_BYTES
        } else if speed_bps >= WRITE_BUFFER_MEDIUM_SPEED_BPS {
            WRITE_BUFFER_MEDIUM_BYTES
        } else {
            WRITE_BUFFER_MIN_BYTES
        }
    }

    fn update_pending_write_target(&self, pending: &mut PendingWrite, recent_speed_bps: f64) {
        let target = self.target_write_buffer_bytes(recent_speed_bps);
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
            let recent_speed_bps = estimate_speed_bps(range_started_at, range_start_cursor, new_pos);

            let remaining = end.saturating_sub(new_pos);
            if should_prefetch(remaining, recent_speed_bps, self.borrow_limit_bytes)
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
        SchedulerMetrics::add(&self.metrics.startup_workers, 1);
        SchedulerMetrics::add(&self.metrics.startup_open_file_ms, open_file_ms);
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
                    consecutive_failures = self
                        .handle_range_retry(
                            &range,
                            start,
                            end,
                            consecutive_failures,
                            &format!("request error: {}", e),
                        )
                        .await?;
                    continue;
                }
            };
            attempt_timing.request_setup_ms = request_started.elapsed().as_millis() as u64;
            SchedulerMetrics::add(&self.metrics.http_setup_ms, attempt_timing.request_setup_ms);
            self.note_first_request_setup(&mut startup, attempt_timing.request_setup_ms);

            let http_version = http_version_label(response.version());

            if !response.status().is_success() {
                consecutive_failures = self
                    .handle_range_retry(
                        &range,
                        start,
                        end,
                        consecutive_failures,
                        &format!("HTTP error: {}", response.status()),
                    )
                    .await?;
                continue;
            }

            let mut stream = response.into_body();
            let mut stream_failed = None::<String>;
            let stream_started = Instant::now();
            let mut first_chunk_at: Option<Instant> = None;
            while let Some(frame_result) = stream.frame().await {
                if self.control.is_halted() {
                    self
                        .flush_pending_write(&mut file, &mut pending_write, &mut attempt_timing)
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

                if first_chunk_at.is_none() {
                    first_chunk_at = Some(Instant::now());
                    attempt_timing.first_byte_ms = stream_started.elapsed().as_millis() as u64;
                    SchedulerMetrics::add(&self.metrics.http_ttfb_ms, attempt_timing.first_byte_ms);
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
                        .flush_pending_write(&mut file, &mut pending_write, &mut attempt_timing)
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
                local_cursor = new_pos;
                let recent_speed_bps = estimate_speed_bps(range_started_at, range_start_cursor, new_pos);
                self.update_pending_write_target(&mut pending_write, recent_speed_bps);
                if pending_write.data.len() >= pending_write.target_bytes {
                    self
                        .flush_pending_write(&mut file, &mut pending_write, &mut attempt_timing)
                        .await?;
                }

                let remaining = max_end.saturating_sub(new_pos);
                if should_prefetch(remaining, recent_speed_bps, self.borrow_limit_bytes)
                    && prefetch_handle.is_none()
                    && prefetched_range.is_none()
                    && !no_more_work_hint
                {
                    SchedulerMetrics::add(&self.metrics.prefetch_requests, 1);
                    prefetch_handle = Some(self.spawn_prefetch_request());
                    self.log_msg(&format!(
                        "prefetch trigger remaining={} recent_speed_bps={:.0}",
                        remaining, recent_speed_bps
                    ))
                    .await;
                }

                if self
                    .collect_prefetch_result(&mut prefetch_handle, &mut prefetched_range, true)
                    .await?
                {
                    no_more_work_hint = true;
                }

                if to_write < chunk.len() {
                    self
                        .flush_pending_write(&mut file, &mut pending_write, &mut attempt_timing)
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
                    .flush_pending_write(&mut file, &mut pending_write, &mut attempt_timing)
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
                    consecutive_failures = self
                        .handle_range_retry(
                            &range,
                            range.cursor.get(),
                            end,
                            consecutive_failures,
                            &reason,
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
                    .flush_pending_write(&mut file, &mut pending_write, &mut attempt_timing)
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
                    .flush_pending_write(&mut file, &mut pending_write, &mut attempt_timing)
                    .await?;
                self.log_attempt_summary(&range, start, end, &attempt_timing, "partial", http_version)
                    .await;
                self.reset_pending_write_target(&mut pending_write);
            }
        }

        if !pending_write.data.is_empty() {
            let mut final_timing = AttemptTiming::default();
            self
                .flush_pending_write(&mut file, &mut pending_write, &mut final_timing)
                .await?;
        }
        if let Some(handle) = prefetch_handle {
            handle.abort();
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
    ) -> Result<u32> {
        let next_failures = consecutive_failures.saturating_add(1);
        SchedulerMetrics::add(&self.metrics.retry_attempts, 1);

        if next_failures > MAX_RANGE_RETRIES {
            return Err(anyhow!(
                "range#{} failed after {} retries at bytes {}..{}: {}",
                range.id,
                consecutive_failures,
                current_start,
                current_end,
                reason
            ));
        }

        let delay_ms = retry_delay_ms(next_failures);
        SchedulerMetrics::add(&self.metrics.retry_wait_ms, delay_ms);
        self.log_msg(&format!(
            "{}; retry {}/{} after {}ms on range#{} bytes={}..{}",
            reason,
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
            "range#{} {} version={} requested={}..{} advanced_to={} setup_ms={} first_byte_ms={} stream_ms={} write_ms={} bytes={} chunks={}",
            range.id,
            outcome,
            http_version,
            requested_start,
            requested_end,
            range.cursor.get(),
            attempt_timing.request_setup_ms,
            attempt_timing.first_byte_ms,
            attempt_timing.stream_ms,
            attempt_timing.write_ms,
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

fn choose_adaptive_seed_start_idx(
    fib_mb: &[u64],
    support_idx: usize,
    total_size: u64,
    connections: usize,
    dry_run: bool,
) -> usize {
    if dry_run || support_idx == 0 {
        return 0;
    }

    let target_mb = total_size
        .div_ceil(connections.max(1) as u64)
        .div_ceil(MB)
        .max(1);
    let upper_idx = fib_mb
        .iter()
        .position(|value| *value >= target_mb)
        .unwrap_or(support_idx)
        .min(support_idx);
    let desired_start = upper_idx.saturating_sub(2);
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

fn build_initial_ranges(
    fib_mb: &[u64],
    seed_start_idx: usize,
    support_idx: usize,
    total_size: u64,
    connections: usize,
    schedule_mode: ScheduleMode,
) -> Vec<RangeSpec> {
    match schedule_mode {
        ScheduleMode::Fib | ScheduleMode::FibAdaptive => {
            build_seed_ranges(fib_mb, seed_start_idx, support_idx, total_size)
        }
        ScheduleMode::Equal => build_equal_ranges(total_size, connections),
    }
}

fn estimate_speed_bps(started_at: Instant, start_offset: u64, current_offset: u64) -> f64 {
    let elapsed = started_at.elapsed().as_secs_f64();
    if elapsed <= 0.0 {
        return 0.0;
    }
    current_offset.saturating_sub(start_offset) as f64 / elapsed
}

fn should_prefetch(remaining_bytes: u64, recent_speed_bps: f64, borrow_limit_bytes: u64) -> bool {
    let handshake_bytes = ((recent_speed_bps * (LIVE_PREFETCH_HANDSHAKE_MS as f64 / 1000.0)).ceil())
        .max((LIVE_PREFETCH_MIN_MB * MB) as f64) as u64;
    remaining_bytes <= handshake_bytes.max(borrow_limit_bytes)
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

fn retry_delay_ms(attempt: u32) -> u64 {
    let shift = attempt.saturating_sub(1).min(4);
    let scaled = RETRY_BASE_DELAY_MS.saturating_mul(1_u64 << shift);
    scaled.min(RETRY_MAX_DELAY_MS)
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

fn build_http_client(http_mode: HttpMode) -> DownloadHttpClient {
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    http.set_nodelay(true);
    http.set_keepalive(Some(Duration::from_secs(TCP_KEEPALIVE_SECS)));
    http.set_keepalive_interval(Some(Duration::from_secs(TCP_KEEPALIVE_INTERVAL_SECS)));
    http.set_keepalive_retries(Some(TCP_KEEPALIVE_RETRIES));
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
    builder.http2_initial_stream_window_size(Some(HTTP2_STREAM_WINDOW_BYTES));
    builder.http2_initial_connection_window_size(Some(HTTP2_CONNECTION_WINDOW_BYTES));
    builder.http2_max_frame_size(Some(HTTP2_MAX_FRAME_BYTES));
    builder.http2_max_send_buf_size(HTTP2_MAX_SEND_BUFFER_BYTES);
    builder.http2_keep_alive_interval(Some(Duration::from_secs(TCP_KEEPALIVE_INTERVAL_SECS)));
    builder.http2_keep_alive_timeout(Duration::from_secs(TCP_KEEPALIVE_INTERVAL_SECS * 2));
    builder.http2_keep_alive_while_idle(true);
    builder.timer(TokioTimer::new());
    builder.build(https)
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
