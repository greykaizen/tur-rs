use std::future::Future;

use anyhow::Result;
pub type H3ChunkFuture = dyn Future<Output = Result<()>> + 'static;

pub struct H3Response {
    pub status: http::StatusCode,
    pub body: Vec<u8>,
}
