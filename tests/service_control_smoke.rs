use std::time::Duration;

use tokio::runtime::Builder;
use tokio::task::LocalSet;
use tur_rs::{DownloadRequest, DownloadStatus, DownloadUpdate, ServiceConfig, TurService};

fn run_local_test<F>(fut: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    #[cfg(feature = "http3")]
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    let local = LocalSet::new();
    runtime.block_on(local.run_until(fut));
}

#[test]
fn service_pause_resume_dry_run_continues_after_resume() {
    run_local_test(async {
        let service = TurService::new(ServiceConfig::default())
            .await
            .expect("service starts");

        let mut handle = service
            .add_download(
                DownloadRequest::new("https://example.com/archive.bin")
                    .dry_run(true)
                    .dry_run_size_mb(128)
                    .per_download_bandwidth_limit_bps(256 * 1024),
            )
            .await
            .expect("download starts");

        let mut first_progress = 0_u64;
        let started = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match handle.recv().await {
                    Some(DownloadUpdate::Progress {
                        downloaded_bytes, ..
                    }) if downloaded_bytes > 0 => {
                        first_progress = downloaded_bytes;
                        break;
                    }
                    Some(_) => {}
                    None => panic!("event stream closed before first progress"),
                }
            }
        })
        .await;
        assert!(started.is_ok(), "timed out waiting for first progress");

        handle.pause().await;

        let paused = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match handle.recv().await {
                    Some(DownloadUpdate::StatusChanged(DownloadStatus::Paused)) => break,
                    Some(_) => {}
                    None => panic!("event stream closed before paused status"),
                }
            }
        })
        .await;
        assert!(paused.is_ok(), "timed out waiting for paused status");

        handle.resume().await;

        let resumed = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                match handle.recv().await {
                    Some(DownloadUpdate::Progress {
                        downloaded_bytes, ..
                    }) if downloaded_bytes > first_progress => break,
                    Some(DownloadUpdate::StatusChanged(DownloadStatus::Completed)) => break,
                    Some(_) => {}
                    None => panic!("event stream closed before resumed progress"),
                }
            }
        })
        .await;
        assert!(resumed.is_ok(), "timed out waiting for resumed progress");

        service.shutdown().await;
    });
}

#[test]
fn service_cancel_dry_run_emits_stopped_status() {
    run_local_test(async {
        let service = TurService::new(ServiceConfig::default())
            .await
            .expect("service starts");

        let mut handle = service
            .add_download(
                DownloadRequest::new("https://example.com/archive.bin")
                    .dry_run(true)
                    .dry_run_size_mb(128)
                    .per_download_bandwidth_limit_bps(256 * 1024),
            )
            .await
            .expect("download starts");

        let started = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match handle.recv().await {
                    Some(DownloadUpdate::Progress {
                        downloaded_bytes, ..
                    }) if downloaded_bytes > 0 => break,
                    Some(_) => {}
                    None => panic!("event stream closed before first progress"),
                }
            }
        })
        .await;
        assert!(started.is_ok(), "timed out waiting for first progress");

        handle.cancel().await;

        let canceled = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match handle.recv().await {
                    Some(DownloadUpdate::StatusChanged(DownloadStatus::Stopped)) => break,
                    Some(_) => {}
                    None => panic!("event stream closed before stopped status"),
                }
            }
        })
        .await;
        assert!(canceled.is_ok(), "timed out waiting for stopped status");

        service.shutdown().await;
    });
}
