use std::path::PathBuf;
use tokio::sync::mpsc;
use anyhow::Result;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum DownloadStatus {
    Queued,
    Downloading,
    Paused,
    Stopped,
    Completed,
    Error(String),
}

#[derive(Debug, Clone)]
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
}

#[derive(Debug, Clone)]
pub enum EngineEvent {
    Progress(Uuid, u64, f64), // id, downloaded, speed
    StatusChanged(Uuid, DownloadStatus),
    TotalSize(Uuid, u64),
}

pub struct DownloadEngine {
    _connections_per_download: usize,
    _max_concurrent_tasks: usize,
}

impl DownloadEngine {
    pub fn new(connections_per_download: usize, max_concurrent_tasks: usize) -> Self {
        Self {
            _connections_per_download: connections_per_download,
            _max_concurrent_tasks: max_concurrent_tasks,
        }
    }

    pub async fn run(&self, mut _event_rx: mpsc::Receiver<EngineCommand>, _tx: mpsc::Sender<EngineEvent>) -> Result<()> {
        // Core engine loop will handle commands and spawn downloads
        Ok(())
    }
}

pub enum EngineCommand {
    Add(DownloadTask),
    Resume(Uuid),
    Stop(Uuid),
    Cancel(Uuid),
}
