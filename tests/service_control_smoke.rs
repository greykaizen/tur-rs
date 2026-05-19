use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::runtime::Builder;
use tokio::task::LocalSet;
use tur_rs::{DownloadRequest, DownloadStatus, DownloadUpdate, ServiceConfig, TurService};

fn run_local_test<F>(fut: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = TEST_MUTEX
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("serialize service control smoke tests");

    #[cfg(feature = "http3")]
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    let local = LocalSet::new();
    runtime.block_on(local.run_until(fut));
}

struct TestServer {
    url: String,
    shutdown: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl TestServer {
    fn spawn(total_size: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        listener
            .set_nonblocking(true)
            .expect("configure nonblocking listener");
        let addr = listener.local_addr().expect("listener addr");
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_thread = shutdown.clone();
        let thread = thread::spawn(move || {
            while !shutdown_thread.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                        let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
                        handle_connection(&mut stream, total_size);
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            url: format!("http://{addr}/archive.bin"),
            shutdown,
            thread: Some(thread),
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(
            self.url
                .strip_prefix("http://")
                .and_then(|rest| rest.split('/').next())
                .unwrap_or("127.0.0.1:0"),
        );
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn handle_connection(stream: &mut TcpStream, total_size: usize) {
    let mut request = Vec::new();
    let mut buf = [0_u8; 1024];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => {
                request.extend_from_slice(&buf[..n]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }

    let request_text = String::from_utf8_lossy(&request);
    let mut lines = request_text.lines();
    let request_line = lines.next().unwrap_or_default();
    let method = request_line.split_whitespace().next().unwrap_or_default();
    let range_header = request_text
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("range:"))
        .and_then(|line| line.split(':').nth(1))
        .map(str::trim)
        .map(str::to_string);

    let (start, end, status_line) = if let Some(range) = range_header {
        let range = range.strip_prefix("bytes=").unwrap_or(&range);
        let mut parts = range.splitn(2, '-');
        let start = parts
            .next()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        let end = parts
            .next()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(total_size.saturating_sub(1))
            .min(total_size.saturating_sub(1));
        (start.min(total_size.saturating_sub(1)), end, "HTTP/1.1 206 Partial Content")
    } else {
        (0, total_size.saturating_sub(1), "HTTP/1.1 200 OK")
    };
    let body_len = end.saturating_sub(start).saturating_add(1);

    let headers = format!(
        "{status_line}\r\nContent-Length: {body_len}\r\nAccept-Ranges: bytes\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n{}\r\n",
        if status_line.contains("206") {
            format!("Content-Range: bytes {start}-{end}/{total_size}\r\n")
        } else {
            String::new()
        }
    );

    if stream.write_all(headers.as_bytes()).is_err() {
        return;
    }

    if method.eq_ignore_ascii_case("HEAD") {
        let _ = stream.flush();
        return;
    }

    let mut offset = start;
    let mut chunk = vec![0_u8; 64 * 1024];
    while offset <= end {
        let remaining = end + 1 - offset;
        let write_len = remaining.min(chunk.len());
        for (idx, byte) in chunk[..write_len].iter_mut().enumerate() {
            *byte = ((offset + idx) % 251) as u8;
        }
        if stream.write_all(&chunk[..write_len]).is_err() {
            return;
        }
        let _ = stream.flush();
        offset += write_len;
        thread::sleep(Duration::from_millis(20));
    }
}

fn temp_download_dir() -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("unix time")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("tur-service-control-{unique}"));
    std::fs::create_dir_all(&dir).expect("create temp download dir");
    dir
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

#[test]
fn service_pause_resume_live_download_continues_after_resume() {
    run_local_test(async {
        let server = TestServer::spawn(8 * 1024 * 1024);
        let service = TurService::new(ServiceConfig {
            connections_per_download: 1,
            max_concurrent_tasks: 1,
            max_total_connections: 2,
            ..ServiceConfig::default()
        })
        .await
        .expect("service starts");

        let mut handle = service
            .add_download(
                DownloadRequest::new(server.url.clone())
                    .dir(temp_download_dir())
                    .connections(1)
                    .min_connections(1)
                    .max_connections(1),
            )
            .await
            .expect("download starts");

        let mut first_progress = 0_u64;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match handle.recv().await {
                    Some(DownloadUpdate::Progress {
                        downloaded_bytes, ..
                    }) if downloaded_bytes > 128 * 1024 => {
                        first_progress = downloaded_bytes;
                        break;
                    }
                    Some(_) => {}
                    None => panic!("event stream closed before first live progress"),
                }
            }
        })
        .await
        .expect("timed out waiting for first live progress");

        handle.pause().await;

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match handle.recv().await {
                    Some(DownloadUpdate::StatusChanged(DownloadStatus::Paused)) => break,
                    Some(_) => {}
                    None => panic!("event stream closed before paused status"),
                }
            }
        })
        .await
        .expect("timed out waiting for paused status");

        tokio::time::sleep(Duration::from_secs(1)).await;
        handle.resume().await;

        tokio::time::timeout(Duration::from_secs(8), async {
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
        .await
        .expect("timed out waiting for resumed live progress");

        service.shutdown().await;
        drop(server);
    });
}

#[test]
fn service_multi_connection_pause_resume_twice_keeps_progressing() {
    run_local_test(async {
        let server = TestServer::spawn(32 * 1024 * 1024);
        let service = TurService::new(ServiceConfig {
            connections_per_download: 4,
            max_concurrent_tasks: 1,
            max_total_connections: 8,
            ..ServiceConfig::default()
        })
        .await
        .expect("service starts");

        let mut handle = service
            .add_download(
                DownloadRequest::new(server.url.clone())
                    .dir(temp_download_dir())
                    .connections(4)
                    .min_connections(2)
                    .max_connections(4),
            )
            .await
            .expect("download starts");

        let mut progress_mark = 0_u64;
        for cycle in 0..2 {
            tokio::time::timeout(Duration::from_secs(6), async {
                loop {
                    match handle.recv().await {
                        Some(DownloadUpdate::Progress {
                            downloaded_bytes, ..
                        }) if downloaded_bytes > progress_mark + (256 * 1024) => {
                            progress_mark = downloaded_bytes;
                            break;
                        }
                        Some(_) => {}
                        None => panic!("event stream closed before progress in cycle {cycle}"),
                    }
                }
            })
            .await
            .expect("timed out waiting for progress before pause");

            handle.pause().await;
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match handle.recv().await {
                        Some(DownloadUpdate::StatusChanged(DownloadStatus::Paused)) => break,
                        Some(_) => {}
                        None => panic!("event stream closed before paused status in cycle {cycle}"),
                    }
                }
            })
            .await
            .expect("timed out waiting for paused status");

            tokio::time::sleep(Duration::from_secs(1)).await;
            handle.resume().await;

            tokio::time::timeout(Duration::from_secs(8), async {
                loop {
                    match handle.recv().await {
                        Some(DownloadUpdate::Progress {
                            downloaded_bytes, ..
                        }) if downloaded_bytes > progress_mark => {
                            progress_mark = downloaded_bytes;
                            break;
                        }
                        Some(DownloadUpdate::StatusChanged(DownloadStatus::Completed)) => break,
                        Some(_) => {}
                        None => panic!("event stream closed before resumed progress in cycle {cycle}"),
                    }
                }
            })
            .await
            .expect("timed out waiting for resumed progress");
        }

        service.shutdown().await;
        drop(server);
    });
}
