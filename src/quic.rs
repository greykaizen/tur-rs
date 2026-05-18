use http::HeaderMap;

/// Parse the Alt-Svc header for an h3 port hint.
pub fn parse_alt_svc_h3_port(headers: &HeaderMap) -> Option<u16> {
    let alt_svc = headers.get("alt-svc")?.to_str().ok()?;
    for token in alt_svc.split(',') {
        let token = token.trim();
        if token.starts_with("h3") && token.contains("\":") {
            if let Some(start) = token.find("\":") {
                let rest = &token[start + 2..];
                if let Some(end) = rest.find('"') {
                    return rest[..end].parse::<u16>().ok();
                }
            }
        }
    }
    None
}

#[cfg(feature = "http3")]
mod inner {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::{Result, anyhow};
    use bytes::Buf as _;
    use bytes::Bytes;
    use http::Method;
    use quinn::crypto::rustls::QuicClientConfig;
    use quinn::{ClientConfig, IdleTimeout};
    use rustls::{ClientConfig as TlsConfig, RootCertStore};

    pub struct H3Client {
        endpoint: quinn::Endpoint,
    }

    pub struct H3Response {
        pub status: http::StatusCode,
        pub body: Vec<u8>,
    }

    fn build_tls_config() -> Result<TlsConfig> {
        let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut crypto = TlsConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        crypto.enable_early_data = true;
        crypto.alpn_protocols = vec![b"h3".to_vec()];
        Ok(crypto)
    }

    fn build_quic_endpoint() -> Result<quinn::Endpoint> {
        let crypto = build_tls_config()?;
        let quic_crypto = QuicClientConfig::try_from(Arc::new(crypto))?;
        let mut client_config = ClientConfig::new(Arc::new(quic_crypto));
        let mut transport_config = quinn::TransportConfig::default();
        transport_config.max_idle_timeout(Some(
            IdleTimeout::try_from(Duration::from_secs(30))
                .map_err(|_| anyhow!("invalid idle timeout"))?,
        ));
        client_config.transport_config(Arc::new(transport_config));

        let mut endpoint = quinn::Endpoint::client("[::]:0".parse().unwrap())?;
        endpoint.set_default_client_config(client_config);
        Ok(endpoint)
    }

    impl H3Client {
        pub fn new() -> Result<Self> {
            let endpoint = build_quic_endpoint()?;
            Ok(Self { endpoint })
        }

        pub async fn get(
            &self,
            origin: &str,
            server_name: &str,
            url: &str,
            range: Option<&str>,
        ) -> Result<H3Response> {
            let origin_str = origin
                .trim_start_matches("https://")
                .trim_start_matches("http://");
            let (host, port) = if let Some((h, p)) = origin_str.rsplit_once(':') {
                (h, p.parse::<u16>().unwrap_or(443))
            } else {
                (origin_str, 443)
            };
            let addr: SocketAddr = format!("{}:{}", host, port)
                .parse()
                .map_err(|e| anyhow!("invalid H3 address {host}:{port}: {e}"))?;

            let connect = self
                .endpoint
                .connect(addr, server_name)?
                .await
                .map_err(|e| anyhow!("H3 connect to {addr} failed: {e}"))?;

            let (mut connection, mut send_request) = h3::client::builder()
                .build::<h3_quinn::Connection, h3_quinn::OpenStreams, Bytes>(
                    h3_quinn::Connection::new(connect),
                )
                .await
                .map_err(|e| anyhow!("H3 handshake failed: {e}"))?;

            tokio::task::spawn(async move {
                let _ = futures_util::future::poll_fn(|cx| connection.poll_close(cx)).await;
            });

            let mut request = http::Request::new(());
            *request.method_mut() = Method::GET;
            *request.uri_mut() = url
                .parse::<http::Uri>()
                .map_err(|e| anyhow!("invalid H3 request URI: {e}"))?;
            request.headers_mut().insert(
                http::header::HOST,
                http::header::HeaderValue::from_str(server_name)
                    .map_err(|_| anyhow!("invalid host header"))?,
            );
            if let Some(range_val) = range {
                request.headers_mut().insert(
                    http::header::RANGE,
                    http::header::HeaderValue::from_str(range_val)
                        .map_err(|_| anyhow!("invalid range header"))?,
                );
            }

            let mut stream = send_request
                .send_request(request)
                .await
                .map_err(|e| anyhow!("H3 send_request failed: {e}"))?;

            let resp = stream
                .recv_response()
                .await
                .map_err(|e| anyhow!("H3 recv_response failed: {e}"))?;

            let status = resp.status();
            let mut body = Vec::new();
            while let Some(mut chunk) = stream
                .recv_data()
                .await
                .map_err(|e| anyhow!("H3 recv_data failed: {e}"))?
            {
                let remaining = chunk.remaining();
                if remaining > 0 {
                    body.extend_from_slice(chunk.chunk());
                    chunk.advance(remaining);
                }
            }

            Ok(H3Response { status, body })
        }
    }
}

#[cfg(feature = "http3")]
pub use inner::*;

#[cfg(not(feature = "http3"))]
mod stub {
    use anyhow::{Result, anyhow};

    pub struct H3Client;

    pub struct H3Response {
        pub status: http::StatusCode,
        pub body: Vec<u8>,
    }

    impl H3Client {
        pub fn new() -> Result<Self> {
            Err(anyhow!(
                "http3 feature not enabled; rebuild with --features http3"
            ))
        }

        pub async fn get(
            &self,
            _origin: &str,
            _server_name: &str,
            _url: &str,
            _range: Option<&str>,
        ) -> Result<H3Response> {
            Err(anyhow!(
                "http3 feature not enabled; rebuild with --features http3"
            ))
        }
    }
}

#[cfg(not(feature = "http3"))]
pub use stub::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_alt_svc_h3_port() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::ALT_SVC,
            "h3=\":443\"; ma=86400, h3-29=\":443\"; ma=86400"
                .parse()
                .unwrap(),
        );
        assert_eq!(parse_alt_svc_h3_port(&headers), Some(443));

        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::ALT_SVC,
            "h3=\":8443\"; ma=3600".parse().unwrap(),
        );
        assert_eq!(parse_alt_svc_h3_port(&headers), Some(8443));

        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::ALT_SVC,
            "h2=\":443\"; ma=86400".parse().unwrap(),
        );
        assert_eq!(parse_alt_svc_h3_port(&headers), None);
    }
}
