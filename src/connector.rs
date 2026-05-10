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
                if let Err(err) = sock.set_tcp_quickack(true) {
                    #[cfg(debug_assertions)]
                    eprintln!("DEBUG: TCP_QUICKACK failed: {}", err);
                }

                if let Err(err) = sock.set_tcp_congestion(b"bbr") {
                    #[cfg(debug_assertions)]
                    eprintln!("DEBUG: TCP_CONGESTION(bbr) failed: {}", err);
                }
            }

            Ok(stream)
        })
    }
}
