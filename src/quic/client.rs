use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use bytes::{Buf as _, Bytes};
use http::Method;

use super::cache::build_quic_endpoint;
use super::stream::{H3ChunkFuture, H3Response};

pub struct H3Client {
    endpoint: quinn::Endpoint,
}

impl H3Client {
    pub fn new() -> Result<Self> {
        Ok(Self {
            endpoint: build_quic_endpoint()?,
        })
    }

    pub async fn get(
        &self,
        origin: &str,
        server_name: &str,
        url: &str,
        range: Option<&str>,
    ) -> Result<H3Response> {
        let mut body = Vec::new();
        let status = self
            .get_streaming(origin, server_name, url, range, |chunk| {
                body.extend_from_slice(&chunk);
                Box::pin(async { Ok(()) })
            })
            .await?;
        Ok(H3Response { status, body })
    }

    pub async fn get_streaming<F>(
        &self,
        origin: &str,
        server_name: &str,
        url: &str,
        range: Option<&str>,
        mut on_chunk: F,
    ) -> Result<http::StatusCode>
    where
        F: FnMut(Bytes) -> std::pin::Pin<Box<H3ChunkFuture>>,
    {
        let origin_str = origin
            .trim_start_matches("https://")
            .trim_start_matches("http://");
        let (host, port) = if let Some((h, p)) = origin_str.rsplit_once(':') {
            (h, p.parse::<u16>().unwrap_or(443))
        } else {
            (origin_str, 443)
        };
        let addr: SocketAddr = format!("{host}:{port}")
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

        let response = stream
            .recv_response()
            .await
            .map_err(|e| anyhow!("H3 recv_response failed: {e}"))?;
        let status = response.status();

        while let Some(mut chunk) = stream
            .recv_data()
            .await
            .map_err(|e| anyhow!("H3 recv_data failed: {e}"))?
        {
            let remaining = chunk.remaining();
            if remaining == 0 {
                continue;
            }
            let bytes = chunk.copy_to_bytes(remaining);
            on_chunk(bytes).await?;
        }

        Ok(status)
    }
}
