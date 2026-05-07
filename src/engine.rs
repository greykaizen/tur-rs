use std::collections::HashMap;
use std::cell::Cell;
use std::fs::File as StdFile;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, LocalSet};
use uuid::Uuid;

const MB: u64 = 1024 * 1024;
const INDEX_STATE_MB: u64 = 8;
const INDEX_STATE_BYTES: u64 = INDEX_STATE_MB * MB;
const DEFAULT_BORROW_LIMIT_MB: u64 = 2;
const LIVE_SEED_FLOOR_MB: u64 = 8;
const LIVE_PREFETCH_MIN_MB: u64 = 2;
const LIVE_PREFETCH_HANDSHAKE_MS: u64 = 700;
const MAX_RANGE_RETRIES: u32 = 8;
const RETRY_BASE_DELAY_MS: u64 = 250;
const RETRY_MAX_DELAY_MS: u64 = 2_000;
const DRY_RUN_STEP_BYTES: u64 = 256 * 1024;
const DRY_RUN_STEP_DELAY_MS: u64 = 4;
const GOLDEN_RATIO_NUM: u64 = 633;
const GOLDEN_RATIO_DEN: u64 = 1024;

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
    pub log_root: Option<PathBuf>,
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
}

impl SchedulerMetrics {
    fn summary_line(&self) -> String {
        format!(
            "metrics direct_assignments={} borrow_assignments={} bytes_borrowed={} work_requests={} request_wait_ms={} prefetch_requests={} prefetch_ready={} prefetch_hits={} http_requests={} http_setup_ms={} http_ttfb_ms={} http_stream_ms={} file_write_ms={} completed_ranges={} retry_attempts={} retry_wait_ms={}",
            self.direct_assignments.get(),
            self.borrow_assignments.get(),
            self.bytes_borrowed.get(),
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
            metrics.clone(),
        )?
    };

    let downloaded = snapshot_downloaded(&coordinator, total_size);
    let global_downloaded = Rc::new(Cell::new(downloaded));
    let index_state = coordinator.index_state.clone();

    let file_path = task.dir.join(&task.filename);
    if !task.dry_run {
        if let Ok(file) = std::fs::File::create(&file_path) {
            let _ = file.set_len(total_size);
        }
    }

    let (work_tx, work_rx) = mpsc::channel(128);
    let mut handles = Vec::with_capacity(task.connections);
    let http_client = reqwest::Client::builder()
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .pool_idle_timeout(Duration::from_secs(30))
        .build()?;

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

    let client = reqwest::Client::new();
    let res = client.head(&task.url).send().await?;
    let total_size = res
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
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
        metrics: Rc<SchedulerMetrics>,
    ) -> Result<Self> {
        let fib_mb = build_fib_mb();
        let ceil_mb = total_size.div_ceil(MB);
        let support_idx = fib_mb
            .iter()
            .position(|value| *value >= ceil_mb.max(1))
            .ok_or_else(|| anyhow!("Download exceeds generated Fibonacci range table"))?;

    let seed_start_idx = choose_seed_start_idx(&fib_mb, support_idx, connections.max(1), dry_run);
        let seed_ranges = build_seed_ranges(&fib_mb, seed_start_idx, support_idx, total_size);
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
                })
            })
            .collect();

        let mut coordinator = Self {
            dl_ranges,
            next_unassigned_idx: 0,
            borrow_limit_bytes: borrow_limit_mb.max(1) * MB,
            borrow_cursor: 0,
            next_range_id: seed_ranges.len() as u64 + 1,
            index_state: Rc::new(IndexStateMap::new(total_size)),
            log_file: StdFile::create(log_path)?,
            metrics,
        };

        coordinator.log(&format!(
            "Coordinator started for task={} total_size={}B ceil_mb={} seed_floor_mb={} seed_start={}MB support_end={}MB borrow_limit={}MB dry_run={} index_state_bucket_mb={} index_state_buckets={} index_state_bytes={}",
            task_id,
            total_size,
            ceil_mb,
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
                })
            })
            .collect();

        let mut coordinator = Self {
            dl_ranges,
            next_unassigned_idx: snapshot.next_unassigned_idx,
            borrow_limit_bytes: snapshot.borrow_limit_bytes,
            borrow_cursor: snapshot.borrow_cursor,
            next_range_id: snapshot.next_range_id,
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

        let total = self.dl_ranges.len();
        for offset in 0..total {
            let idx = (self.borrow_cursor + offset) % total;
            let active = self.dl_ranges[idx].clone();
            let owner_connection = active.assigned_to.get();
            if owner_connection == connection_id || owner_connection == UNASSIGNED_CONNECTION {
                continue;
            }

            let start = active.cursor.get();
            let end = active.end.get();
            let remaining = end.saturating_sub(start);
            if remaining <= self.borrow_limit_bytes.saturating_mul(2) {
                continue;
            }

            let steal_size = (((remaining as u128) * (GOLDEN_RATIO_NUM as u128))
                / (GOLDEN_RATIO_DEN as u128)) as u64;
            let aligned_split = align_down(end.saturating_sub(steal_size), MB);
            if aligned_split <= start + self.borrow_limit_bytes {
                continue;
            }

            let stolen_size = end.saturating_sub(aligned_split);
            if stolen_size < self.borrow_limit_bytes {
                continue;
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
            });
            self.next_range_id += 1;
            SchedulerMetrics::add(&self.metrics.borrow_assignments, 1);
            SchedulerMetrics::add(&self.metrics.bytes_borrowed, stolen_size);
            let donor_id = active.id;
            let donor_label_start = active.label_start_mb;
            let donor_label_end = active.label_end_mb;
            self.dl_ranges.push(borrowed.clone());
            self.borrow_cursor = idx + 1;

            self.log(&format!(
                "borrow conn={} from_conn={} donor_range#{} new_range#{} support={}..{}MB bytes={}..{}",
                connection_id,
                owner_connection,
                donor_id,
                borrowed.id,
                donor_label_start,
                donor_label_end,
                aligned_split,
                end
            ));
            return Some(borrowed);
        }

        None
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
    client: reqwest::Client,
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

impl ConnectionWorker {
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
        let mut file = OpenOptions::new().write(true).open(&self.file_path).await?;
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
            let response = match self.client
                .get(&self.url)
                .header("Range", format!("bytes={}-{}", start, end - 1))
                .send()
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

            let mut stream = response.bytes_stream();
            let mut stream_failed = None::<String>;
            let stream_started = Instant::now();
            let mut first_chunk_at: Option<Instant> = None;
            while let Some(chunk_result) = stream.next().await {
                if self.control.is_halted() {
                    return Ok(());
                }

                let chunk = match chunk_result {
                    Ok(c) => c,
                    Err(e) => {
                        stream_failed = Some(format!("stream error: {}", e));
                        break;
                    }
                };

                if first_chunk_at.is_none() {
                    first_chunk_at = Some(Instant::now());
                    attempt_timing.first_byte_ms = stream_started.elapsed().as_millis() as u64;
                    SchedulerMetrics::add(&self.metrics.http_ttfb_ms, attempt_timing.first_byte_ms);
                }

                let max_end = range.end.get();
                if local_cursor >= max_end {
                    current_range = None;
                    break;
                }

                let to_write = (max_end - local_cursor).min(chunk.len() as u64) as usize;
                let write_started = Instant::now();
                file.seek(SeekFrom::Start(local_cursor)).await?;
                file.write_all(&chunk[..to_write]).await?;
                let write_ms = write_started.elapsed().as_millis() as u64;
                attempt_timing.write_ms = attempt_timing.write_ms.saturating_add(write_ms);
                SchedulerMetrics::add(&self.metrics.file_write_ms, write_ms);

                let new_pos = local_cursor + to_write as u64;
                range.cursor.set(new_pos);
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
                    current_range = None;
                    current_range_id = None;
                    consecutive_failures = 0;
                    range_wait_started = Instant::now();
                    break;
                }
            }

            attempt_timing.stream_ms = stream_started.elapsed().as_millis() as u64;
            SchedulerMetrics::add(&self.metrics.http_stream_ms, attempt_timing.stream_ms);

            if let Some(reason) = stream_failed {
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
                range.status.set(RANGE_STATUS_FINISHED);
                SchedulerMetrics::add(&self.metrics.completed_ranges, 1);
                self.log_attempt_summary(&range, start, end, &attempt_timing, "complete", http_version)
                    .await;
                current_range = None;
                current_range_id = None;
                consecutive_failures = 0;
                range_wait_started = Instant::now();
            } else if made_progress_this_attempt {
                self.log_attempt_summary(&range, start, end, &attempt_timing, "partial", http_version)
                    .await;
            }
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

    let target_initial_ranges = connections.saturating_add(3);
    let desired_floor_idx = fib_mb
        .iter()
        .position(|value| *value >= LIVE_SEED_FLOOR_MB)
        .unwrap_or(0);
    let max_start_for_balance = support_idx.saturating_sub(target_initial_ranges);
    desired_floor_idx.min(max_start_for_balance)
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

fn http_version_label(version: reqwest::Version) -> &'static str {
    match version {
        reqwest::Version::HTTP_09 => "HTTP/0.9",
        reqwest::Version::HTTP_10 => "HTTP/1.0",
        reqwest::Version::HTTP_11 => "HTTP/1.1",
        reqwest::Version::HTTP_2 => "HTTP/2",
        reqwest::Version::HTTP_3 => "HTTP/3",
        _ => "HTTP/?",
    }
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
