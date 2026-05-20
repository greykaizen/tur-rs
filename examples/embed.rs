//! Embedding example for tur-rs.
//!
//! This shows how to use `tur-rs` as a library — no CLI invocation needed.
//!
//! Usage:
//!   cargo run --example embed -- https://example.com/file.zip
//!
//! Run with the `http3` feature to enable QUIC:
//!   cargo run --example embed --features http3 -- https://example.com/file.zip

use std::env;
use std::path::PathBuf;

use anyhow::Result;
use tokio::task::LocalSet;

use tur_rs::{DownloadRequest, DownloadUpdate, ServiceConfig, TurService};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: embed <url> [dir]");
        std::process::exit(1);
    }

    let url = &args[1];
    let dir = args
        .get(2)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    // Create the service inside a LocalSet (required for !Send engine types).
    let local = LocalSet::new();
    local
        .run_until(async { run_service(url, &dir).await })
        .await
}

async fn run_service(url: &str, dir: &PathBuf) -> Result<()> {
    // 1. Configure and start the service
    let config = ServiceConfig {
        connections_per_download: 4,
        max_concurrent_tasks: 2,
        ..ServiceConfig::default()
    };

    let service = TurService::new(config).await?;
    println!("tur-rs service started");

    // 2. Submit a download request
    let request = DownloadRequest::new(url).dir(dir.clone()).connections(4);

    let mut handle = service.add_download(request).await?;
    println!("Download {} queued", handle.id);

    // 3. Stream events until completion
    while let Some(update) = handle.recv().await {
        match update {
            DownloadUpdate::TotalSize(size) => {
                println!("Total size: {} bytes", size);
            }
            DownloadUpdate::Workers(workers) => {
                println!("Workers: {}", workers.len());
            }
            DownloadUpdate::Protocol(protocol) => {
                println!("Protocol: {:?}", protocol);
            }
            DownloadUpdate::Progress {
                downloaded_bytes,
                speed_bps,
            } => {
                println!(
                    "Progress: {} bytes @ {:.1} MiB/s",
                    downloaded_bytes,
                    speed_bps / 1_048_576.0
                );
            }
            DownloadUpdate::StatusChanged(status) => {
                println!("Status: {:?}", status);
                if matches!(status, tur_rs::DownloadStatus::Completed) {
                    println!("Download complete!");
                    break;
                }
                if let tur_rs::DownloadStatus::Error(ref e) = status {
                    eprintln!("Download failed: {}", e);
                    break;
                }
            }
        }
    }

    // 4. Shut down
    service.shutdown().await;
    println!("Service shut down cleanly");
    Ok(())
}
