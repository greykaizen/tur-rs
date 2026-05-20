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
use url::Url;
use uuid::Uuid;

use crate::engine::{
    DownloadEngine, DownloadStatus, DownloadTask, EngineCommand, EngineEvent, HttpMode,
    ScheduleMode,
};
use crate::storage::StorageConfig;

// ---------------------------------------------------------------------------
// Session / context types for session-aware downloading (0.8.0)
// ---------------------------------------------------------------------------

/// A single cookie entry for use with session-aware downloads.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CookieEntry {
    pub name: String,
    pub value: String,
    pub domain: String,
    pub path: String,
    pub secure: bool,
    pub expires: Option<String>,
}

impl CookieEntry {
    pub fn new(
        name: impl Into<String>,
        value: impl Into<String>,
        domain: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            domain: domain.into(),
            path: "/".into(),
            secure: false,
            expires: None,
        }
    }

    /// Parse a `Set-Cookie` header value into a `CookieEntry`,
    /// using the request URL's host as the default domain.
    pub fn from_set_cookie(header: &str, request_url: &Url) -> Option<Self> {
        let mut name = String::new();
        let mut value = String::new();
        let mut domain = request_url.host_str()?.to_string();
        let mut path = "/".to_string();
        let mut secure = false;
        let mut expires = None;

        let mut parts = header.split(';');
        if let Some(first) = parts.next() {
            let eq_pos = first.find('=')?;
            name = first[..eq_pos].trim().to_string();
            value = first[eq_pos + 1..].trim().to_string();
        }

        for part in parts {
            let part = part.trim();
            if let Some(eq_pos) = part.find('=') {
                let key = part[..eq_pos].trim().to_ascii_lowercase();
                let val = part[eq_pos + 1..].trim().to_string();
                match key.as_str() {
                    "domain" => domain = val.trim_start_matches('.').to_string(),
                    "path" => path = val,
                    "expires" => expires = Some(val),
                    _ => {}
                }
            } else if part.eq_ignore_ascii_case("secure") {
                secure = true;
            }
        }

        Some(Self {
            name,
            value,
            domain,
            path,
            secure,
            expires,
        })
    }

    /// Format as a `Cookie` request header value.
    pub fn to_request_value(&self) -> String {
        format!("{}={}", self.name, self.value)
    }
}

/// An in-memory cookie store.
#[derive(Debug, Clone, Default)]
pub struct CookieJar {
    cookies: Vec<CookieEntry>,
}

impl CookieJar {
    pub fn new() -> Self {
        Self {
            cookies: Vec::new(),
        }
    }

    /// Add (or replace) a cookie.
    pub fn insert(&mut self, cookie: CookieEntry) {
        self.cookies.retain(|c| {
            !(c.name == cookie.name && c.domain == cookie.domain && c.path == cookie.path)
        });
        self.cookies.push(cookie);
    }

    /// Return cookies that match the given URL.
    pub fn match_url(&self, url: &Url) -> Vec<&CookieEntry> {
        let host = url.host_str().unwrap_or("");
        let path = url.path();
        self.cookies
            .iter()
            .filter(|c| {
                let domain_match = host == c.domain || host.ends_with(&format!(".{}", c.domain));
                let path_match = path.starts_with(&c.path);
                let secure_ok = !c.secure || url.scheme() == "https";
                domain_match && path_match && secure_ok
            })
            .collect()
    }

    /// Format all matching cookies as a single `Cookie` header value.
    pub fn header_value_for_url(&self, url: &Url) -> Option<String> {
        let matched = self.match_url(url);
        if matched.is_empty() {
            return None;
        }
        Some(
            matched
                .iter()
                .map(|c| c.to_request_value())
                .collect::<Vec<_>>()
                .join("; "),
        )
    }

    /// Import cookies from a string in Netscape cookie-file format or simple "name=value" lines.
    pub fn import_lines(&mut self, lines: &str, default_domain: &str) {
        for line in lines.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with("//") {
                continue;
            }
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 7 {
                let domain = parts[0].trim_start_matches('.');
                let path = parts[2];
                let secure = parts[3] == "TRUE";
                let name = parts[5];
                let value = parts[6];
                self.insert(CookieEntry {
                    name: name.to_string(),
                    value: value.to_string(),
                    domain: domain.to_string(),
                    path: path.to_string(),
                    secure,
                    expires: None,
                });
            } else if let Some(eq_pos) = line.find('=') {
                let name = line[..eq_pos].trim();
                let value = line[eq_pos + 1..].trim();
                if !name.is_empty() {
                    self.insert(CookieEntry::new(name, value, default_domain));
                }
            }
        }
    }

    /// Export cookies as a string (Netscape format for reuse).
    pub fn export_netscape(&self) -> String {
        let mut out = String::new();
        out.push_str("# Netscape HTTP Cookie File\n");
        out.push_str("# https://curl.se/rfc/cookie_spec.html\n");
        out.push_str("# This file was generated by tur-rs\n");
        for c in &self.cookies {
            let secure = if c.secure { "TRUE" } else { "FALSE" };
            let expires = c.expires.as_deref().unwrap_or("0");
            out.push_str(&format!(
                "{}\tTRUE\t{}\t{}\t{}\t{}\t{}\n",
                c.domain, c.path, secure, expires, c.name, c.value
            ));
        }
        out
    }

    pub fn len(&self) -> usize {
        self.cookies.len()
    }
    pub fn is_empty(&self) -> bool {
        self.cookies.is_empty()
    }
}

/// Request-level context for authenticated / session-aware downloading.
///
/// This is the primary input for browser-assisted handoff: a GUI frontend
/// can acquire cookies, tokens, and headers from a browser context and pass
/// them here for `tur-rs` to use during the actual download.
#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    /// Custom HTTP headers to attach to every request for this download.
    pub headers: HashMap<String, String>,
    /// Authorization header value (e.g. `"Bearer eyJ..."`).
    pub auth: Option<String>,
    /// Referer URL.
    pub referer: Option<String>,
    /// User-Agent override.
    pub user_agent: Option<String>,
    /// Cookies scoped to this download's origin.
    pub cookies: Option<Vec<CookieEntry>>,
}

impl RequestContext {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a custom header.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Set the Authorization header (e.g. `"Bearer eyJ..."`).
    pub fn auth(mut self, value: impl Into<String>) -> Self {
        self.auth = Some(value.into());
        self
    }

    /// Set the Referer header.
    pub fn referer(mut self, url: impl Into<String>) -> Self {
        self.referer = Some(url.into());
        self
    }

    /// Override the User-Agent.
    pub fn user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = Some(ua.into());
        self
    }

    /// Attach cookies for this download's origin.
    pub fn cookies(mut self, cookies: Vec<CookieEntry>) -> Self {
        self.cookies = Some(cookies);
        self
    }
}

/// A bundle of session context for browser-assisted handoff.
///
/// This is the primary handoff type for GUI/frontend integration:
/// a web view or Tauri app can acquire cookies, headers, and tokens
/// from a browser session and pass them here for `tur-rs` to use.
///
/// Convert to a [`RequestContext`] via [`SessionContext::to_request_context`].
#[derive(Debug, Clone, Default)]
pub struct SessionContext {
    pub cookies: Vec<CookieEntry>,
    pub headers: HashMap<String, String>,
    pub auth: Option<String>,
    pub referer: Option<String>,
    pub user_agent: Option<String>,
}

impl SessionContext {
    pub fn new() -> Self {
        Self::default()
    }

    /// Convert this session context into a [`RequestContext`] for use with a download.
    pub fn to_request_context(&self) -> RequestContext {
        RequestContext {
            headers: self.headers.clone(),
            auth: self.auth.clone(),
            referer: self.referer.clone(),
            user_agent: self.user_agent.clone(),
            cookies: if self.cookies.is_empty() {
                None
            } else {
                Some(self.cookies.clone())
            },
        }
    }

    /// Builder: add a cookie.
    pub fn cookie(mut self, entry: CookieEntry) -> Self {
        self.cookies.push(entry);
        self
    }

    /// Builder: add a custom header.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Builder: set Authorization (e.g. `"Bearer eyJ..."`).
    pub fn auth(mut self, value: impl Into<String>) -> Self {
        self.auth = Some(value.into());
        self
    }

    /// Builder: set Referer.
    pub fn referer(mut self, url: impl Into<String>) -> Self {
        self.referer = Some(url.into());
        self
    }

    /// Builder: override User-Agent.
    pub fn user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = Some(ua.into());
        self
    }
}

// ---------------------------------------------------------------------------
// Public config & request types
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
    /// Optional request context for session-aware downloading.
    pub request_context: Option<RequestContext>,
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
            request_context: None,
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

    /// Attach request context (auth, headers, cookies, referer) for
    /// session-aware downloading (builder style).
    pub fn context(mut self, ctx: RequestContext) -> Self {
        self.request_context = Some(ctx);
        self
    }

    /// Convenience: set a Bearer token for Authorization.
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.request_context
            .get_or_insert_with(RequestContext::new)
            .auth = Some(format!("Bearer {}", token.into()));
        self
    }

    /// Convenience: set the Referer header.
    pub fn referer(mut self, url: impl Into<String>) -> Self {
        self.request_context
            .get_or_insert_with(RequestContext::new)
            .referer = Some(url.into());
        self
    }

    /// Convenience: add a custom header.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.request_context
            .get_or_insert_with(RequestContext::new)
            .headers
            .insert(name.into(), value.into());
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
    /// Periodic worker/connection diagnostics.
    Workers(Vec<crate::engine::WorkerSnapshot>),
    /// Current protocol information: requested mode vs negotiated family.
    Protocol(crate::engine::ProtocolInfo),
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
        let _ = self.engine_tx.send(EngineCommand::Stop(self.id)).await;
    }

    /// Resume a paused download.
    pub async fn resume(&self) {
        let _ = self.engine_tx.send(EngineCommand::Resume(self.id)).await;
    }

    /// Cancel and persist the download state to disk.
    pub async fn cancel(&self) {
        let _ = self.engine_tx.send(EngineCommand::Cancel(self.id)).await;
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
    cookie_jar: std::cell::RefCell<CookieJar>,
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
            cookie_jar: std::cell::RefCell::new(CookieJar::new()),
        })
    }

    /// Return a reference to the internal cookie jar for inspection or import.
    pub fn cookie_jar(&self) -> std::cell::Ref<'_, CookieJar> {
        self.cookie_jar.borrow()
    }

    /// Return a mutable reference to the cookie jar for importing cookies.
    pub fn cookie_jar_mut(&self) -> std::cell::RefMut<'_, CookieJar> {
        self.cookie_jar.borrow_mut()
    }

    /// Submit a download request and get back a [`DownloadHandle`].
    ///
    /// The download is queued immediately and will start as soon as the
    /// engine has capacity.
    ///
    /// If the request includes a [`RequestContext`], its cookies are merged
    /// into the service-wide cookie jar, and the context is threaded through
    /// to every HTTP request made for this download.
    pub async fn add_download(&self, request: DownloadRequest) -> Result<DownloadHandle> {
        let (event_tx, event_rx) = mpsc::unbounded_channel::<DownloadUpdate>();

        let filename = request.filename.clone().unwrap_or_else(|| {
            request
                .url
                .split('/')
                .last()
                .unwrap_or("unknown")
                .to_string()
        });

        // Merge per-request cookies into the service cookie jar
        if let Some(ref ctx) = request.request_context {
            if let Some(ref cookies) = ctx.cookies {
                let mut jar = self.cookie_jar.borrow_mut();
                for c in cookies {
                    jar.insert(c.clone());
                }
            }
        }

        let mut task = DownloadTask {
            id: Uuid::new_v4(),
            url: request.url,
            filename,
            dir: request.dir,
            total_size: 0,
            downloaded_size: 0,
            status: DownloadStatus::Queued,
            speed: 0.0,
            connections: request
                .connections
                .unwrap_or(self.engine.connections_per_download),
            dry_run: request.dry_run,
            dry_run_size_mb: request.dry_run_size_mb,
            borrow_limit_mb: request.borrow_limit_mb.unwrap_or(2),
            min_connections: request.min_connections.unwrap_or(1),
            max_connections: request.max_connections.unwrap_or(16),
            per_download_bandwidth_limit_bps: request.per_download_bandwidth_limit_bps.unwrap_or(0),
            schedule_mode: request.schedule_mode.unwrap_or(ScheduleMode::Equal),
            http_mode: request.http_mode.unwrap_or(HttpMode::Auto),
            log_root: None,
            request_context: request.request_context,
        };

        // Merge service cookie jar cookies into the task's request context.
        // Service-level cookies are applied by URL match.
        if let Ok(url) = url::Url::parse(&task.url) {
            let jar = self.cookie_jar.borrow();
            let jar_cookies: Vec<CookieEntry> = jar.match_url(&url).into_iter().cloned().collect();
            if !jar_cookies.is_empty() {
                let ctx = task.request_context.get_or_insert_with(RequestContext::new);
                let mut existing = ctx.cookies.take().unwrap_or_default();
                // Only add service cookies that don't shadow existing per-request cookies
                for c in jar_cookies {
                    if !existing
                        .iter()
                        .any(|ec| ec.name == c.name && ec.domain == c.domain && ec.path == c.path)
                    {
                        existing.push(c);
                    }
                }
                ctx.cookies = Some(existing);
            }
        }

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

    /// Import cookies from a Netscape-format cookie file into the service cookie jar.
    ///
    /// The domain is extracted from each line; a fallback domain can be used
    /// for cookies with no explicit domain.
    pub async fn import_cookie_file(&self, path: &PathBuf) -> Result<()> {
        let contents = tokio::fs::read_to_string(path).await?;
        self.cookie_jar.borrow_mut().import_lines(&contents, "");
        Ok(())
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
                        EngineEvent::Workers(id, workers) => {
                            Some((id, DownloadUpdate::Workers(workers)))
                        }
                        EngineEvent::Protocol(id, protocol) => {
                            Some((id, DownloadUpdate::Protocol(protocol)))
                        }
                        EngineEvent::StatusChanged(id, DownloadStatus::Completed) => {
                            let _ = handles.borrow_mut().remove(&id);
                            Some((id, DownloadUpdate::StatusChanged(DownloadStatus::Completed)))
                        }
                        EngineEvent::StatusChanged(id, status) => {
                            let is_terminal = matches!(status, DownloadStatus::Error(_));
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
