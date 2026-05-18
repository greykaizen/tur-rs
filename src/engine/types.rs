use super::*;

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
