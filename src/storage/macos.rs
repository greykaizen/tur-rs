use super::*;

use anyhow::anyhow;

pub const DIRECT_IO_ALIGNMENT: usize = 4096;

pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
    let _ = (file, total_size);
    Ok(())
}

pub async fn open_download_file_for_write(
    path: &Path,
    _config: &StorageConfig,
) -> Result<DownloadFile> {
    use std::os::unix::fs::OpenOptionsExt;

    let path = path.to_path_buf();
    let file = tokio::task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(0)
            .open(&path)
            .map_err(|e| anyhow!("failed to open file for macos pwrite: {e}"))?;

        rustix::fs::fcntl_nocache(&file, true)?;
        Ok::<_, anyhow::Error>(file)
    })
    .await
    .map_err(|e| anyhow!("spawn_blocking failed: {e}"))??;

    Ok(DownloadFile {
        inner: DownloadFileInner::MacosPwrite(file),
        backend: StorageBackendKind::MacosPwrite,
    })
}

pub async fn write_all_at_tokio(file: &mut File, offset: u64, data: &[u8]) -> Result<()> {
    file.seek(SeekFrom::Start(offset)).await?;
    file.write_all(data).await?;
    Ok(())
}

pub async fn write_all_at_pwrite(
    file: &mut std::fs::File,
    offset: u64,
    data: &[u8],
) -> Result<()> {
    use std::os::unix::fs::FileExt;

    let data = data.to_vec();
    let cloned = file.try_clone()?;

    tokio::task::spawn_blocking(move || {
        cloned.write_all_at(&data, offset)?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("spawn_blocking failed: {e}"))??;

    Ok(())
}
