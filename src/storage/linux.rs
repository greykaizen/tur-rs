use super::*;

use anyhow::anyhow;
use tokio::sync::mpsc;

use aligned_buffer::LinuxIoUringCommand;

pub const DIRECT_IO_ALIGNMENT: usize = 4096;
#[allow(dead_code)]
pub const DIRECT_IO_BLOCK_BYTES: usize = 4096;
const SPLICE_CHUNK_BYTES: usize = 65536;

pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
    let _ = (file, total_size);
    Ok(())
}

pub async fn open_download_file_for_write(
    path: &Path,
    config: &StorageConfig,
) -> Result<DownloadFile> {
    if !config.no_io_uring && let Ok(file) = open_download_file_for_write_linux_uring(path).await {
        return Ok(file);
    }

    if config.use_splice && let Ok(file) = open_download_file_for_write_linux_splice(path).await {
        return Ok(file);
    }

    if config.use_pwrite {
        return open_download_file_for_write_linux_pwrite(path).await;
    }

    open_download_file_for_write_linux_tokio(path).await
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

pub async fn write_all_at_splice(
    file: &mut std::fs::File,
    pipe_read: &mut std::fs::File,
    pipe_write: &mut std::fs::File,
    offset: u64,
    data: &[u8],
) -> Result<()> {
    use rustix::pipe::{splice, SpliceFlags};
    use std::io::Write;
    use std::os::unix::fs::FileExt;

    let len = data.len();
    let file_clone = file.try_clone()?;
    let pipe_read_clone = pipe_read.try_clone()?;
    let mut pipe_write_clone = pipe_write.try_clone()?;
    let data_vec = data.to_vec();

    tokio::task::spawn_blocking(move || {
        let mut written: u64 = 0;
        let mut file_offset = offset;
        while written < len as u64 {
            let remaining = (len as u64).saturating_sub(written);
            let chunk_size = (remaining as usize).min(SPLICE_CHUNK_BYTES);
            let start = written as usize;
            let end = start + chunk_size;

            pipe_write_clone.write_all(&data_vec[start..end])?;

            match splice(
                &pipe_read_clone,
                None,
                &file_clone,
                Some(&mut file_offset),
                chunk_size,
                SpliceFlags::MOVE,
            ) {
                Ok(n) if n > 0 => written += n as u64,
                Ok(_) => break,
                Err(rustix::io::Errno::NOSYS) | Err(rustix::io::Errno::INVAL) => {
                    let _ = pipe_read_clone.set_len(0);
                    let written_sofar = written;
                    file_clone.write_all_at(&data_vec[written_sofar as usize..], offset + written_sofar)?;
                    return Ok::<_, anyhow::Error>(());
                }
                Err(e) => {
                    let _ = pipe_read_clone.set_len(0);
                    return Err(anyhow::anyhow!("splice failed: {e}"));
                }
            }
        }

        let _ = pipe_read_clone.set_len(0);
        Ok(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("spawn_blocking failed: {e}"))??;

    Ok(())
}

async fn open_download_file_for_write_linux_pwrite(path: &Path) -> Result<DownloadFile> {
    use std::os::unix::fs::OpenOptionsExt;

    let path = path.to_path_buf();
    let file = tokio::task::spawn_blocking(move || {
        std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(0)
            .open(&path)
            .map_err(|e| anyhow!("failed to open file for pwrite: {e}"))
    })
    .await
    .map_err(|e| anyhow!("spawn_blocking failed: {e}"))??;

    Ok(DownloadFile {
        inner: DownloadFileInner::LinuxPwrite(file),
        backend: StorageBackendKind::LinuxPwrite,
    })
}

async fn open_download_file_for_write_linux_splice(path: &Path) -> Result<DownloadFile> {
    use rustix::pipe::pipe;
    use std::os::unix::fs::OpenOptionsExt;

    let path = path.to_path_buf();
    let result = tokio::task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(0)
            .open(&path)
            .map_err(|e| anyhow!("failed to open file for splice: {e}"))?;
        let (pipe_read_fd, pipe_write_fd) =
            pipe().map_err(|e| anyhow!("pipe() failed: {e}"))?;
        Ok::<_, anyhow::Error>((file, pipe_read_fd, pipe_write_fd))
    })
    .await
    .map_err(|e| anyhow!("spawn_blocking failed: {e}"))??;

    let (file, pipe_read_fd, pipe_write_fd) = result;
    let pipe_read: std::fs::File = pipe_read_fd.into();
    let pipe_write: std::fs::File = pipe_write_fd.into();

    Ok(DownloadFile {
        inner: DownloadFileInner::LinuxSplice {
            file,
            pipe_read,
            pipe_write,
        },
        backend: StorageBackendKind::LinuxSplice,
    })
}

async fn open_download_file_for_write_linux_tokio(path: &Path) -> Result<DownloadFile> {
    let file = tokio::fs::OpenOptions::new().write(true).open(path).await?;
    Ok(DownloadFile {
        inner: DownloadFileInner::Tokio(file),
        backend: StorageBackendKind::LinuxTokio,
    })
}

async fn open_download_file_for_write_linux_uring(path: &Path) -> Result<DownloadFile> {
    use rustix::fs::OFlags;
    use std::os::unix::fs::OpenOptionsExt;

    let (tx, mut rx) = mpsc::channel::<LinuxIoUringCommand>(32);
    let path = path.to_path_buf();
    let fallback = File::from_std(std::fs::OpenOptions::new().write(true).open(&path)?);
    std::thread::Builder::new()
        .name("tur-io-uring".to_string())
        .spawn(move || {
            tokio_uring::start(async move {
                let mut options = tokio_uring::fs::OpenOptions::new();
                options.write(true);
                options.custom_flags(OFlags::DIRECT.bits() as i32);
                let file = match options.open(&path).await {
                    Ok(file) => file,
                    Err(_) => return,
                };

                while let Some(cmd) = rx.recv().await {
                    match cmd {
                        LinuxIoUringCommand::WriteAllAt { offset, data, resp } => {
                            let (result, _) = file.write_all_at(data, offset).await;
                            let _ = resp.send(result.map_err(anyhow::Error::from));
                        }
                        LinuxIoUringCommand::Shutdown => {
                            let _ = file.close().await;
                            break;
                        }
                    }
                }
            });
        })
        .map_err(|err| anyhow!("failed to spawn io_uring backend thread: {}", err))?;

    Ok(DownloadFile {
        inner: DownloadFileInner::LinuxIoUring { tx, fallback },
        backend: StorageBackendKind::LinuxIoUring,
    })
}
