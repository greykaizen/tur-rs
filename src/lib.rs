//! tur-rs: A relentless, high-concurrency download manager.
//!
//! `tur-rs` is a reusable download core with adaptive work-stealing,
//! protocol-aware scheduling, and platform-optimized storage backends.
//!
//! # Quick start (library embedding)
//!
//! ```rust,no_run
//! use tokio::task::LocalSet;
//! use tur_rs::{TurService, ServiceConfig, DownloadRequest, DownloadUpdate};
//!
//! # async fn example() -> anyhow::Result<()> {
//! let local = LocalSet::new();
//! local.run_until(async {
//!     let mut service = TurService::new(ServiceConfig::default()).await?;
//!
//!     let mut handle = service
//!         .add_download(DownloadRequest::new("https://example.com/file.zip"))
//!         .await?;
//!
//!     while let Some(update) = handle.recv().await {
//!         match update {
//!             DownloadUpdate::Progress { downloaded_bytes, speed_bps } => {
//!                 println!("{downloaded_bytes} bytes at {speed_bps:.0} bps");
//!             }
//!             DownloadUpdate::TotalSize(size) => {
//!                 println!("Total: {size} bytes");
//!             }
//!             DownloadUpdate::Workers(workers) => {
//!                 println!("{} worker snapshots", workers.len());
//!             }
//!             DownloadUpdate::Protocol(protocol) => {
//!                 println!("Protocol: {protocol:?}");
//!             }
//!             DownloadUpdate::StatusChanged(status) => {
//!                 println!("Status: {status:?}");
//!                 if matches!(status, tur_rs::DownloadStatus::Completed) {
//!                     break;
//!                 }
//!             }
//!         }
//!     }
//!
//!     service.shutdown().await;
//!     Ok::<_, anyhow::Error>(())
//! }).await
//! # }
//! ```
//!
//! See `examples/embed.rs` for a complete runnable example.
//!
//! # Features
//!
//! | Feature                       | Description                                                  |
//! |-------------------------------|--------------------------------------------------------------|
//! | `tui` (default)               | Terminal UI via ratatui/crossterm                            |
//! | `http3`                       | Experimental HTTP/3 via QUIC (quinn, h3, rustls)             |
//! | `linux-io-uring-experimental` | Experimental Linux io_uring storage backend                  |
//!
//! # Stability
//!
//! The types re-exported from this crate root (`TurService`, `ServiceConfig`,
//! `DownloadRequest`, `DownloadHandle`, `DownloadUpdate`, `DownloadStatus`,
//! `HttpMode`, `ScheduleMode`, `StorageConfig`) are the **stable public API**
//! and follow semantic versioning.
//!
//! Internal modules (`engine`, `connector`, `quic`, `storage`, `cli`) are
//! exposed for advanced use but may change between minor releases. Prefer
//! the re-exported types where possible.

// ---------------------------------------------------------------------------
// Public modules (exposed for the binary frontends and advanced embedding)
// ---------------------------------------------------------------------------

pub mod cli;
pub mod connector;
pub mod engine;
pub mod quic;
pub mod storage;

/// Stable service facade for frontend integration.
pub mod service;

#[cfg(feature = "tui")]
pub mod tui;

// ---------------------------------------------------------------------------
// Re-exports — stable public API surface
//
// These are the types that downstream consumers should rely on.
// Everything behind `engine::`, `connector::`, `quic::`, `storage::` is
// internal and subject to change.
// ---------------------------------------------------------------------------

pub use service::{
    CookieEntry, CookieJar, DownloadHandle, DownloadRequest, DownloadUpdate, RequestContext,
    ServiceConfig, SessionContext, TurService,
};
pub use storage::StorageConfig;

pub use engine::{
    DownloadStatus, HttpMode, ProtocolFamily, ProtocolInfo, ScheduleMode, WorkerSnapshot,
    WorkerState,
};
