use hyper::Uri;
use hyper_util::client::legacy::connect::HttpConnector;
use socket2::{SockRef, TcpKeepalive};
use std::future::Future;
#[cfg(target_os = "linux")]
use std::io::ErrorKind;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tower_service::Service;

#[derive(Clone)]
pub struct TunedConnector {
    inner: HttpConnector,
}

impl TunedConnector {
    pub fn new() -> Self {
        let mut inner = HttpConnector::new();
        inner.enforce_http(false);
        inner.set_nodelay(true);
        Self { inner }
    }
}

impl Service<Uri> for TunedConnector {
    type Response = <HttpConnector as Service<Uri>>::Response;
    type Error = <HttpConnector as Service<Uri>>::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let fut = self.inner.call(uri);
        Box::pin(async move {
            let stream = fut.await?;

            // Try to tune the socket. Errors are ignored per the spec.
            let sock = SockRef::from(stream.inner());
            let keepalive = TcpKeepalive::new()
                .with_time(Duration::from_secs(10))
                .with_interval(Duration::from_secs(5))
                .with_retries(3);
            let _ = sock.set_tcp_keepalive(&keepalive);

            #[cfg(target_os = "linux")]
            {
                if let Err(_err) = sock.set_tcp_quickack(true) {
                    #[cfg(debug_assertions)]
                    eprintln!("DEBUG: TCP_QUICKACK failed: {}", _err);
                }

                if let Err(_err) = sock.set_tcp_congestion(b"bbr") {
                    #[cfg(debug_assertions)]
                    if _err.kind() != ErrorKind::NotFound {
                        eprintln!("DEBUG: TCP_CONGESTION(bbr) failed: {}", _err);
                    }
                }
            }

            #[cfg(target_os = "macos")]
            {
                // macOS does not have TCP_QUICKACK or BBR, but it does have:
                //
                // 1. TCP_NOPUSH — equivalent of Linux TCP_CORK.
                //    socket2 dropped set_tcp_nopush(); call setsockopt directly.
                #[allow(unsafe_code)]
                unsafe {
                    use std::os::unix::io::AsRawFd;
                    let fd = sock.as_raw_fd();
                    let val: libc::c_int = 1;
                    libc::setsockopt(
                        fd,
                        libc::IPPROTO_TCP,
                        libc::TCP_NOPUSH,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                    );
                }

                // 2. SO_RCVBUF — macOS autotuning is conservative.
                //    Bump to 4 MB to match Linux auto-tuning on fast links.
                if let Err(_err) = sock.set_recv_buffer_size(4 * 1024 * 1024) {
                    #[cfg(debug_assertions)]
                    eprintln!("DEBUG: SO_RCVBUF (macOS) failed: {}", _err);
                }
            }

            #[cfg(target_os = "windows")]
            {
                // Windows does not have TCP_QUICKACK or BBR. It defaults
                // to 64 KB receive buffers, far too small for BDP on fast links.
                //
                // 1. SO_RCVBUF — Match Linux auto-tuning territory (~4 MB).
                //    Windows caps this via the system-wide MaxBufferSize
                //    (typically 8 MB on modern Windows). 4 MB is safe.
                if let Err(_err) = sock.set_recv_buffer_size(4 * 1024 * 1024) {
                    #[cfg(debug_assertions)]
                    eprintln!("DEBUG: SO_RCVBUF (Windows) failed: {}", _err);
                }

                // 2. SIO_LOOPBACK_FAST_PATH — Enables a faster loopback
                //    path on Windows for localhost connections.
                //    No-op for remote connections, safe to set always.
                //    socket2 does not expose this directly; we fall back
                //    to the raw ioctl via std::os::windows::io::AsRawSocket.
                #[cfg(windows)]
                {
                    use std::os::windows::io::AsRawSocket;
                    const SIO_LOOPBACK_FAST_PATH: u32 = 0x98000010u32;
                    // INVALID_SOCKET was removed from std::os::windows::raw in
                    // Rust 1.x — use the WinSock constant value directly (usize::MAX).
                    const INVALID_SOCKET: usize = usize::MAX;
                    let raw = sock.as_raw_socket() as usize;
                    if raw != INVALID_SOCKET {
                        let mut enabled: u32 = 1;
                        unsafe {
                            let _ = windows_sys::Win32::Networking::WinSock::WSAIoctl(
                                raw as windows_sys::Win32::Networking::WinSock::SOCKET,
                                SIO_LOOPBACK_FAST_PATH,
                                &mut enabled as *mut _ as *mut std::ffi::c_void,
                                std::mem::size_of::<u32>() as u32,
                                std::ptr::null_mut(),
                                0,
                                std::ptr::null_mut(),
                                std::ptr::null_mut(),
                                None,
                            );
                        }
                    }
                }
            }

            Ok(stream)
        })
    }
}
