use hyper::Uri;
use hyper_util::client::legacy::connect::HttpConnector;
use socket2::{SockRef, TcpKeepalive};
use std::future::Future;
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
                    eprintln!("DEBUG: TCP_CONGESTION(bbr) failed: {}", _err);
                }
            }

            #[cfg(target_os = "macos")]
            {
                // macOS does not have TCP_QUICKACK or BBR, but it does have:
                //
                // 1. TCP_NOPUSH — equivalent of Linux TCP_CORK.
                //    Batches small sends into full MTU-sized segments.
                //    The peer TunedConnector calls set_nodelay(true) above,
                //    so NOPUSH + NODELAY interact as: NOPUSH delays until
                //    the buffer is full or an explicit push; NODELAY is
                //    irrelevant when NOPUSH is active. We set NOPUSH so that
                //    HTTP request headers + body are coalesced into one segment.
                if let Err(_err) = sock.set_tcp_nopush(true) {
                    #[cfg(debug_assertions)]
                    eprintln!("DEBUG: TCP_NOPUSH failed: {}", _err);
                }

                // 2. SO_RCVBUF — macOS autotuning is conservative.
                //    Bump to 4 MB to match Linux auto-tuning on fast links.
                //    This is a hint; macOS may cap it to the system max
                //    (kern.ipc.maxsockbuf, typically 4 MB on modern macOS).
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
                    let raw: std::os::windows::raw::SOCKET = sock.as_raw_socket();
                    if raw != std::os::windows::raw::INVALID_SOCKET {
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
