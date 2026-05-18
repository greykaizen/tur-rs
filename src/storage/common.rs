use super::*;
use tokio::fs::OpenOptions;

pub const DIRECT_IO_ALIGNMENT: usize = 4096;

pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
    let _ = (file, total_size);
    Ok(())
}

pub async fn open_download_file_for_write(
    path: &Path,
    _config: &StorageConfig,
) -> Result<DownloadFile> {
    let file = OpenOptions::new().write(true).open(path).await?;
    Ok(DownloadFile {
        inner: DownloadFileInner::Tokio(file),
        backend: StorageBackendKind::Standard,
    })
}

pub async fn write_all_at_tokio(file: &mut File, offset: u64, data: &[u8]) -> Result<()> {
    file.seek(SeekFrom::Start(offset)).await?;
    file.write_all(data).await?;
    Ok(())
}
