use std::pin::Pin;

use anyhow::{Result, anyhow};
use bytes::Bytes;

pub struct H3Client;
use super::stream::{H3ChunkFuture, H3Response};

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

    pub async fn get_streaming<F>(
        &self,
        _origin: &str,
        _server_name: &str,
        _url: &str,
        _range: Option<&str>,
        _on_chunk: F,
    ) -> Result<http::StatusCode>
    where
        F: FnMut(Bytes) -> Pin<Box<H3ChunkFuture>>,
    {
        Err(anyhow!(
            "http3 feature not enabled; rebuild with --features http3"
        ))
    }
}
