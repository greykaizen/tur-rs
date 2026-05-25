use super::*;
use crate::engine::scaler::ProtocolFamily;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DownloadStatus {
    Queued,
    Downloading,
    Paused,
    Stopped,
    Canceled,
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
    /// Session context for authenticated/session-aware downloading.
    #[serde(skip)]
    pub request_context: Option<crate::service::RequestContext>,
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
            "fib-adaptive" | "fib_adaptive" | "adaptive-fib" | "adaptive_fib" => {
                Ok(Self::FibAdaptive)
            }
            "equal" => Ok(Self::Equal),
            other => Err(anyhow!("unsupported schedule mode: {}", other)),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
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
    Http3,
}

impl HttpMode {
    pub fn parse(input: &str) -> Result<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "http1" | "http/1.1" | "h1" => Ok(Self::Http1),
            "http2" | "http/2" | "h2" => Ok(Self::Http2),
            "http3" | "http/3" | "h3" | "quic" => Ok(Self::Http3),
            other => Err(anyhow!("unsupported http mode: {}", other)),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Http1 => "http1",
            Self::Http2 => "http2",
            Self::Http3 => "http3",
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
    Workers(Uuid, Vec<WorkerSnapshot>),
    /// Protocol information — carries both requested HTTP mode and the
    /// dominant negotiated protocol family observed during the download.
    Protocol(Uuid, ProtocolInfo),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerState {
    Connecting,
    WaitingForWork,
    Downloading,
    Retrying,
    Paused,
    Stopped,
    Finished,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerSnapshot {
    pub connection_id: u32,
    pub state: WorkerState,
    pub transferred_bytes: u64,
    pub speed_bps: f64,
    pub range_start: Option<u64>,
    pub range_end: Option<u64>,
    pub range_cursor: Option<u64>,
    pub detail: Option<String>,
}

pub enum EngineCommand {
    Add(DownloadTask),
    Resume(Uuid),
    Stop(Uuid),
    Cancel(Uuid),
    UpdateScaling(Uuid, ScalerConfig),
    RuntimeStopped(TaskSnapshot, HaltMode),
}

/// Rich protocol information: what the user requested vs what was negotiated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolInfo {
    /// The HTTP mode that was requested (Auto, Http1, Http2, Http3).
    pub requested: HttpMode,
    /// The dominant protocol family actually observed/negotiated.
    pub negotiated: ProtocolFamily,
}

impl ProtocolInfo {
    /// Short display label (e.g. "auto→h2", "h1", "h3").
    pub fn display_label(&self) -> String {
        let req = match self.requested {
            HttpMode::Auto => "auto",
            HttpMode::Http1 => "h1",
            HttpMode::Http2 => "h2",
            HttpMode::Http3 => "h3",
        };
        let neg = self.negotiated.as_str();
        if req == neg || self.negotiated == ProtocolFamily::Other {
            req.to_string()
        } else {
            format!("{}→{}", req, neg)
        }
    }
}

impl Default for ProtocolInfo {
    fn default() -> Self {
        Self {
            requested: HttpMode::Auto,
            negotiated: ProtocolFamily::Other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HaltMode {
    /// Task is actively running / scheduling work.
    Running,
    /// Stop issuing new work; allow workers to reach a safe relinquish boundary.
    Draining,
    /// Runtime remains alive, client state preserved, workers owned but idle.
    Hibernating,
    /// Cancel/stop path that writes resumable state to disk.
    PersistToDisk,
}
