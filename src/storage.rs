use std::path::Path;

use anyhow::Result;
#[cfg(target_os = "linux")]
use anyhow::anyhow;
use tokio::fs::File;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
#[cfg(target_os = "linux")]
use tokio::sync::{mpsc, oneshot};

mod aligned_buffer;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod common;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
use common as platform;
#[cfg(target_os = "windows")]
use windows as platform;

use aligned_buffer::AlignedBuffer;
#[cfg(target_os = "linux")]
use aligned_buffer::LinuxIoUringCommand;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageConfig {
    pub use_pwrite: bool,
    pub use_splice: bool,
    pub no_io_uring: bool,
    pub no_direct_io: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            use_pwrite: true,
            use_splice: true,
            no_io_uring: false,
            no_direct_io: false,
        }
    }
}

pub fn prepare_download_file(path: &Path, total_size: u64) -> Result<()> {
    let file = std::fs::File::create(path)?;
    file.set_len(total_size)?;
    platform::prepare_download_file(&file, total_size)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBackendKind {
    Standard,
    LinuxTokio,
    LinuxPwrite,
    LinuxSplice,
    LinuxIoUring,
    MacosPwrite,
    MacosNoCache,
    WindowsPwrite,
    WindowsDirectIo,
    WindowsSequential,
}

enum DownloadFileInner {
    #[cfg_attr(target_os = "linux", allow(dead_code))]
    Tokio(File),
    #[cfg(target_os = "linux")]
    LinuxPwrite(std::fs::File),
    #[cfg(target_os = "linux")]
    LinuxSplice {
        file: std::fs::File,
        pipe_read: std::fs::File,
        pipe_write: std::fs::File,
    },
    #[cfg(target_os = "linux")]
    LinuxIoUring {
        tx: mpsc::Sender<LinuxIoUringCommand>,
        fallback: File,
    },
    #[cfg(target_os = "macos")]
    MacosPwrite(std::fs::File),
    #[cfg(target_os = "windows")]
    WindowsPwrite(std::fs::File),
    #[cfg(target_os = "windows")]
    WindowsDirectIo(std::fs::File),
}

pub struct DownloadFile {
    inner: DownloadFileInner,
    backend: StorageBackendKind,
}

impl DownloadFile {
    pub fn backend(&self) -> StorageBackendKind {
        self.backend
    }

    pub fn direct_io_alignment(&self) -> Option<usize> {
        match self.backend {
            StorageBackendKind::LinuxIoUring => Some(platform::DIRECT_IO_ALIGNMENT),
            StorageBackendKind::WindowsDirectIo => Some(platform::DIRECT_IO_ALIGNMENT),
            _ => None,
        }
    }

    pub async fn write_all_at(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        match &mut self.inner {
            DownloadFileInner::Tokio(file) => platform::write_all_at_tokio(file, offset, data).await,
            #[cfg(target_os = "linux")]
            DownloadFileInner::LinuxPwrite(file) => platform::write_all_at_pwrite(file, offset, data).await,
            #[cfg(target_os = "linux")]
            DownloadFileInner::LinuxSplice {
                pipe_write,
                pipe_read,
                file,
            } => platform::write_all_at_splice(file, pipe_read, pipe_write, offset, data).await,
            #[cfg(target_os = "linux")]
            DownloadFileInner::LinuxIoUring { tx, fallback } => {
                let alignment = platform::DIRECT_IO_ALIGNMENT as u64;
                let start = offset;
                let end = offset + data.len() as u64;

                let aligned_start = if start % alignment == 0 {
                    start
                } else {
                    start + (alignment - (start % alignment))
                };
                let aligned_end = end - (end % alignment);

                if aligned_start >= aligned_end {
                    return platform::write_all_at_tokio(fallback, offset, data).await;
                }

                let prefix_len = aligned_start.saturating_sub(start) as usize;
                if prefix_len > 0 {
                    platform::write_all_at_tokio(fallback, offset, &data[..prefix_len]).await?;
                }

                let middle_start = prefix_len;
                let middle_len = (aligned_end - aligned_start) as usize;
                let middle_end = middle_start + middle_len;
                if middle_len > 0 {
                    let mut aligned = AlignedBuffer::new(middle_len, platform::DIRECT_IO_ALIGNMENT);
                    aligned.as_mut_slice()[..middle_len]
                        .copy_from_slice(&data[middle_start..middle_end]);

                    let (resp_tx, resp_rx) = oneshot::channel();
                    tx.send(LinuxIoUringCommand::WriteAllAt {
                        offset: aligned_start,
                        data: aligned,
                        resp: resp_tx,
                    })
                    .await
                    .map_err(|_| anyhow!("io_uring backend thread is not available"))?;
                    resp_rx
                        .await
                        .map_err(|_| anyhow!("io_uring backend response channel closed"))??;
                }

                if middle_end < data.len() {
                    platform::write_all_at_tokio(fallback, aligned_end, &data[middle_end..]).await?;
                }

                Ok(())
            }
            #[cfg(target_os = "macos")]
            DownloadFileInner::MacosPwrite(file) => platform::write_all_at_pwrite(file, offset, data).await,
            #[cfg(target_os = "windows")]
            DownloadFileInner::WindowsPwrite(file) => {
                platform::write_all_at_windows_pwrite(file, offset, data).await
            }
            #[cfg(target_os = "windows")]
            DownloadFileInner::WindowsDirectIo(file) => {
                platform::write_all_at_windows_direct_io(file, offset, data).await
            }
        }
    }
}

impl Drop for DownloadFile {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        if let DownloadFileInner::LinuxIoUring { tx, .. } = &self.inner {
            let _ = tx.try_send(LinuxIoUringCommand::Shutdown);
        }
    }
}

pub async fn open_download_file_for_write(path: &Path) -> Result<DownloadFile> {
    open_download_file_for_write_with_config(path, &StorageConfig::default()).await
}

pub async fn open_download_file_for_write_with_config(
    path: &Path,
    config: &StorageConfig,
) -> Result<DownloadFile> {
    platform::open_download_file_for_write(path, config).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn selects_a_supported_backend_for_current_platform() {
        let dir = std::env::temp_dir().join(format!("tur-storage-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("file.bin");
        prepare_download_file(&path, 8192).unwrap();

        let file = open_download_file_for_write_with_config(&path, &StorageConfig::default())
            .await
            .unwrap();
        let backend = file.backend();

        #[cfg(target_os = "linux")]
        assert!(matches!(
            backend,
            StorageBackendKind::LinuxIoUring
                | StorageBackendKind::LinuxSplice
                | StorageBackendKind::LinuxPwrite
                | StorageBackendKind::LinuxTokio
        ));
        #[cfg(target_os = "macos")]
        assert!(matches!(backend, StorageBackendKind::MacosPwrite));
        #[cfg(target_os = "windows")]
        assert!(matches!(
            backend,
            StorageBackendKind::WindowsDirectIo | StorageBackendKind::WindowsPwrite
        ));
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        assert!(matches!(backend, StorageBackendKind::Standard));

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_dir_all(dir);
    }
}
