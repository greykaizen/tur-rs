//! High-level download service for frontend integration.
//!
//! [`TurService`] wraps the engine and provides a stable API for:
//!
//! - Starting and stopping the download runtime
//! - Adding download tasks
//! - Receiving progress/status events per task
//! - Pausing, resuming, and cancelling individual tasks
//!
//! # Lifecycle
//!
//! 1. Create [`TurService::new`] with a [`ServiceConfig`]
//! 2. Call [`add_download`](TurService::add_download) one or more times
//! 3. Read events from the returned [`DownloadHandle`]
//! 4. Call [`shutdown`](TurService::shutdown) to stop the runtime cleanly
//!
//! # Thread safety
//!
//! `TurService` uses `tokio::task::spawn_local` internally and must run
//! inside a [`tokio::task::LocalSet`] context. The default `#[tokio::main]`
//! does **not** provide one — you must create an explicit `LocalSet`:
//!
//! ```rust,no_run
//! use tokio::task::LocalSet;
//! # async fn example() -> anyhow::Result<()> {
//! let local = LocalSet::new();
//! local.run_until(async {
//!     // create & use TurService here
//!     # Ok::<_, anyhow::Error>(())
//! }).await
//! # }
//! ```
//!
//! When using `tokio::runtime::Builder::new_current_thread()`, the
//! [`LocalSet::block_on`] or `run_until` pattern is also required.

use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;

use anyhow::Result;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::engine::{
    DownloadEngine,
    DownloadStatus, DownloadTask, EngineCommand, EngineEvent, HttpMode, ScheduleMode,
};
use crate::storage::StorageConfig;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Configuration for a [`TurService`] instance.
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    /// Per-download initial connection count (default: 8).
    pub connections_per_download: usize,

    /// Maximum concurrent downloads (default: 3).
    pub max_concurrent_tasks: usize,

    /// System-wide ceiling for active download connections (default: 32).
    pub max_total_connections: usize,

    /// Global bandwidth cap in bps; 0 disables (default: 0).
    pub global_bandwidth_limit_bps: u64,

    /// Enable persisted origin behaviour memory (default: true).
    pub enable_origin_memory: bool,

    /// Platform storage tuning options.
    pub storage_config: StorageConfig,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            connections_per_download: 8,
            max_concurrent_tasks: 3,
            max_total_connections: 32,
            global_bandwidth_limit_bps: 0,
            enable_origin_memory: true,
            storage_config: StorageConfig::default(),
        }
    }
}

/// A download request to be submitted to [`TurService::add_download`].
///
/// At minimum you must provide a URL. All other fields have sensible defaults.
///
/// # Example
///
/// ```rust,no_run
/// use tur_rs::DownloadRequest;
///
/// let req = DownloadRequest::new("https://example.com/file.zip")
///     .dir("./downloads")
///     .connections(16);
/// ```
#[derive(Debug, Clone)]
pub struct DownloadRequest {
    pub url: String,
    pub dir: PathBuf,
    pub filename: Option<String>,
    pub connections: Option<usize>,
    pub min_connections: Option<usize>,
    pub max_connections: Option<usize>,
    pub borrow_limit_mb: Option<u64>,
    pub per_download_bandwidth_limit_bps: Option<u64>,
    pub http_mode: Option<HttpMode>,
    pub schedule_mode: Option<ScheduleMode>,
    pub dry_run: bool,
    pub dry_run_size_mb: Option<u64>,
}

impl DownloadRequest {
    /// Create a new download request with the given URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            dir: PathBuf::from("."),
            filename: None,
            connections: None,
            min_connections: None,
            max_connections: None,
            borrow_limit_mb: None,
            per_download_bandwidth_limit_bps: None,
            http_mode: None,
            schedule_mode: None,
            dry_run: false,
            dry_run_size_mb: None,
        }
    }

    /// Set the output directory (builder style).
    pub fn dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.dir = dir.into();
        self
    }

    /// Set per-download connection count (builder style).
    pub fn connections(mut self, n: usize) -> Self {
        self.connections = Some(n);
        self
    }

    /// Set the filename override (builder style).
    pub fn filename(mut self, name: impl Into<String>) -> Self {
        self.filename = Some(name.into());
        self
    }

    /// Set minimum connections for autonomous scaling (builder style).
    pub fn min_connections(mut self, n: usize) -> Self {
        self.min_connections = Some(n);
        self
    }

    /// Set maximum connections for autonomous scaling (builder style).
    pub fn max_connections(mut self, n: usize) -> Self {
        self.max_connections = Some(n);
        self
    }

    /// Set borrow stop threshold in MiB (builder style).
    pub fn borrow_limit_mb(mut self, mb: u64) -> Self {
        self.borrow_limit_mb = Some(mb);
        self
    }

    /// Set per-download bandwidth cap in bps (builder style).
    pub fn per_download_bandwidth_limit_bps(mut self, bps: u64) -> Self {
        self.per_download_bandwidth_limit_bps = Some(bps);
        self
    }

    /// Set HTTP transport mode (builder style).
    pub fn http_mode(mut self, mode: HttpMode) -> Self {
        self.http_mode = Some(mode);
        self
    }

    /// Set schedule/range-chunking mode (builder style).
    pub fn schedule_mode(mut self, mode: ScheduleMode) -> Self {
        self.schedule_mode = Some(mode);
        self
    }

    /// Enable dry-run mode (builder style).
    pub fn dry_run(mut self, dry: bool) -> Self {
        self.dry_run = dry;
        self
    }

    /// Set synthetic file size for dry runs, in MiB (builder style).
    pub fn dry_run_size_mb(mut self, mb: u64) -> Self {
        self.dry_run_size_mb = Some(mb);
        self
    }
}



/// Events emitted during a download's lifecycle.
///
/// These are the stable, serialization-friendly counterpart to the
/// internal [`EngineEvent`] type.
#[derive(Debug, Clone)]
pub enum DownloadUpdate {
    /// Periodic progress notification.
    Progress {
        /// Total bytes downloaded so far.
        downloaded_bytes: u64,
        /// Instantaneous transfer rate (bytes/sec).
        speed_bps: f64,
    },
    /// Final file size discovered (may arrive well after the first progress event).
    TotalSize(u64),
    /// Task status transition.
    StatusChanged(DownloadStatus),
}

/// A handle to control a single download and receive its events.
///
/// Dropping the handle does **not** cancel the download — use
/// [`cancel`](DownloadHandle::cancel) explicitly.
pub struct DownloadHandle {
    /// The unique task identifier.
    pub id: Uuid,
    engine_tx: mpsc::Sender<EngineCommand>,
    event_rx: mpsc::UnboundedReceiver<DownloadUpdate>,
}

impl std::fmt::Debug for DownloadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadHandle")
            .field("id", &self.id)
            .finish()
    }
}

impl DownloadHandle {
    /// Wait for the next update from this download (async).
    ///
    /// Returns `None` when the sender has been dropped (service shut down).
    pub async fn recv(&mut self) -> Option<DownloadUpdate> {
        self.event_rx.recv().await
    }

    /// Try to receive an update without blocking.
    pub fn try_recv(&mut self) -> Result<DownloadUpdate, mpsc::error::TryRecvError> {
        self.event_rx.try_recv()
    }

    /// Pause the download (connections are released, state kept in memory).
    pub async fn pause(&self) {
        let _ = self
            .engine_tx
            .send(EngineCommand::Stop(self.id))
            .await;
    }

    /// Resume a paused download.
    pub async fn resume(&self) {
        let _ = self
            .engine_tx
            .send(EngineCommand::Resume(self.id))
            .await;
    }

    /// Cancel and persist the download state to disk.
    pub async fn cancel(&self) {
        let _ = self
            .engine_tx
            .send(EngineCommand::Cancel(self.id))
            .await;
    }
}

// ---------------------------------------------------------------------------
// The service
// ---------------------------------------------------------------------------

/// High-level download service.
///
/// Spawns the engine runtime on construction and provides a stable API
/// for frontend integration.
pub struct TurService {
    engine: Rc<DownloadEngine>,
    engine_tx: mpsc::Sender<EngineCommand>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handles: Rc<std::cell::RefCell<HashMap<Uuid, mpsc::UnboundedSender<DownloadUpdate>>>>,
}

impl std::fmt::Debug for TurService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurService").finish()
    }
}

impl TurService {
    /// Create a new download service.
    ///
    /// This spawns the engine in a background `spawn_local` task. You must
    /// be running inside a [`tokio::task::LocalSet`]. See the
    /// [module-level docs](index.html#thread-safety) for the correct setup
    /// pattern.
    pub async fn new(config: ServiceConfig) -> Result<Self> {
        let (engine_tx, engine_rx) = mpsc::channel::<EngineCommand>(100);
        let (event_tx, event_rx) = mpsc::channel::<EngineEvent>(100);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let handles: Rc<std::cell::RefCell<HashMap<Uuid, mpsc::UnboundedSender<DownloadUpdate>>>> =
            Rc::new(std::cell::RefCell::new(HashMap::new()));

        let engine = DownloadEngine::new(
            config.connections_per_download,
            config.max_concurrent_tasks,
            config.max_total_connections,
            config.global_bandwidth_limit_bps,
            config.enable_origin_memory,
            config.storage_config,
        );

        let engine_tx_clone = engine_tx.clone();
        let handles_clone = handles.clone();

        // Event router: fans out engine events to per-handle channels
        tokio::task::spawn_local({
            let engine_tx = engine_tx.clone();
            async move {
                Self::route_events(event_rx, handles_clone, engine_tx, shutdown_rx).await;
            }
        });

        // Engine run loop — uses engine_tx for RuntimeStopped events
        tokio::task::spawn_local({
            let engine = engine.clone();
            let event_tx = event_tx.clone();
            async move {
                if let Err(e) = engine.run(engine_rx, engine_tx_clone, event_tx).await {
                    eprintln!("Engine error: {}", e);
                }
            }
        });

        Ok(Self {
            engine,
            engine_tx,
            shutdown_tx: Some(shutdown_tx),
            handles,
        })
    }

    /// Submit a download request and get back a [`DownloadHandle`].
    ///
    /// The download is queued immediately and will start as soon as the
    /// engine has capacity.
    pub async fn add_download(&self, request: DownloadRequest) -> Result<DownloadHandle> {
        let (event_tx, event_rx) = mpsc::unbounded_channel::<DownloadUpdate>();

        let filename = request
            .filename
            .clone()
            .unwrap_or_else(|| {
                request
                    .url
                    .split('/')
                    .last()
                    .unwrap_or("unknown")
                    .to_string()
            });

        let task = DownloadTask {
            id: Uuid::new_v4(),
            url: request.url,
            filename,
            dir: request.dir,
            total_size: 0,
            downloaded_size: 0,
            status: DownloadStatus::Queued,
            speed: 0.0,
            connections: request.connections.unwrap_or(self.engine.connections_per_download),
            dry_run: request.dry_run,
            dry_run_size_mb: request.dry_run_size_mb,
            borrow_limit_mb: request.borrow_limit_mb.unwrap_or(2),
            min_connections: request.min_connections.unwrap_or(1),
            max_connections: request.max_connections.unwrap_or(16),
            per_download_bandwidth_limit_bps: request
                .per_download_bandwidth_limit_bps
                .unwrap_or(0),
            schedule_mode: request.schedule_mode.unwrap_or(ScheduleMode::Equal),
            http_mode: request.http_mode.unwrap_or(HttpMode::Auto),
            log_root: None,
        };

        let id = task.id;
        self.handles.borrow_mut().insert(id, event_tx);
        self.engine_tx
            .send(EngineCommand::Add(task))
            .await
            .map_err(|_| anyhow::anyhow!("engine channel closed"))?;

        Ok(DownloadHandle {
            id,
            engine_tx: self.engine_tx.clone(),
            event_rx,
        })
    }

    /// Shut down the service gracefully.
    ///
    /// Signals the engine to stop. In-flight tasks are stopped and their
    /// in-memory state is preserved for later resume. The shutdown signal
    /// is sent immediately; this method does **not** await the engine's
    /// completion.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Drop engine_tx to close the engine's command channel
        drop(self.engine_tx);
    }

    /// Query the effective connection budget (system-aware).
    pub fn effective_connection_budget(&self) -> usize {
        self.engine.effective_connection_budget.get()
    }

    /// Query the configured connection budget.
    pub fn configured_connection_budget(&self) -> usize {
        self.engine.configured_connection_budget.get()
    }

    /// Internal: route engine events to per-handle channels.
    async fn route_events(
        mut event_rx: mpsc::Receiver<EngineEvent>,
        handles: Rc<std::cell::RefCell<HashMap<Uuid, mpsc::UnboundedSender<DownloadUpdate>>>>,
        engine_tx: mpsc::Sender<EngineCommand>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    // Signal all active downloads to stop gracefully
                    let ids: Vec<Uuid> = handles.borrow().keys().copied().collect();
                    for id in ids {
                        let _ = engine_tx.send(EngineCommand::Stop(id)).await;
                    }
                    break;
                }
                event_opt = event_rx.recv() => {
                    let Some(event) = event_opt else { break };
                    let update = match event {
                        EngineEvent::Progress(id, downloaded, speed) => {
                            Some((id, DownloadUpdate::Progress {
                                downloaded_bytes: downloaded,
                                speed_bps: speed,
                            }))
                        }
                        EngineEvent::TotalSize(id, size) => {
                            Some((id, DownloadUpdate::TotalSize(size)))
                        }
                        EngineEvent::StatusChanged(id, DownloadStatus::Completed) => {
                            let _ = handles.borrow_mut().remove(&id);
                            Some((id, DownloadUpdate::StatusChanged(DownloadStatus::Completed)))
                        }
                        EngineEvent::StatusChanged(id, status) => {
                            let is_terminal = matches!(status,
                                DownloadStatus::Stopped
                                | DownloadStatus::Paused
                                | DownloadStatus::Error(_)
                            );
                            if is_terminal {
                                let _ = handles.borrow_mut().remove(&id);
                            }
                            Some((id, DownloadUpdate::StatusChanged(status)))
                        }
                    };

                    if let Some((id, update)) = update {
                        if let Some(tx) = handles.borrow().get(&id) {
                            let _ = tx.send(update);
                        }
                    }
                }
            }
        }
    }
}
