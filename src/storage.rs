use std::path::Path;
use std::ptr::NonNull;

use anyhow::Result;
#[cfg(target_os = "linux")]
use anyhow::anyhow;
use tokio::fs::File;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
#[cfg(target_os = "linux")]
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageConfig {
    pub use_splice: bool,
    pub no_io_uring: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            use_splice: true,
            no_io_uring: false,
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
    LinuxSplice,
    LinuxIoUring,
    MacosNoCache,
    WindowsSequential,
}

enum DownloadFileInner {
    #[cfg_attr(target_os = "linux", allow(dead_code))]
    Tokio(File),
    #[cfg(target_os = "linux")]
    LinuxSplice(std::fs::File),
    #[cfg(target_os = "linux")]
    LinuxIoUring {
        tx: mpsc::Sender<LinuxIoUringCommand>,
        fallback: File,
    },
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
            _ => None,
        }
    }

    pub async fn write_all_at(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        match &mut self.inner {
            DownloadFileInner::Tokio(file) => platform::write_all_at_tokio(file, offset, data).await,
            #[cfg(target_os = "linux")]
            DownloadFileInner::LinuxSplice(file) => {
                platform::write_all_at_splice(file, offset, data).await
            }
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

#[derive(Debug)]
pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    len: usize,
    align: usize,
}

impl AlignedBuffer {
    pub fn new(len: usize, align: usize) -> Self {
        assert!(align.is_power_of_two(), "alignment must be a power of two");
        assert!(len > 0, "aligned buffer length must be non-zero");
        let layout = std::alloc::Layout::from_size_align(len, align)
            .expect("valid aligned buffer layout");
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr = match NonNull::new(ptr) {
            Some(ptr) => ptr,
            None => std::alloc::handle_alloc_error(layout),
        };
        Self { ptr, len, align }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn align(&self) -> usize {
        self.align
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::from_size_align(self.len, self.align)
            .expect("valid aligned buffer layout");
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), layout) };
    }
}

#[cfg(target_os = "linux")]
unsafe impl tokio_uring::buf::IoBuf for AlignedBuffer {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn bytes_total(&self) -> usize {
        self.len()
    }
}

#[cfg(target_os = "linux")]
unsafe impl Send for AlignedBuffer {}

#[cfg(target_os = "linux")]
enum LinuxIoUringCommand {
    WriteAllAt {
        offset: u64,
        data: AlignedBuffer,
        resp: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

mod platform {
    use super::*;

    pub const DIRECT_IO_ALIGNMENT: usize = 4096;
    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    pub const DIRECT_IO_BLOCK_BYTES: usize = 4096;

    pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
        let _ = (file, total_size);
        #[cfg(target_os = "linux")]
        {
            // Linux already benefits from explicit sizing before the write path opens.
        }
        #[cfg(target_os = "macos")]
        {
            // Darwin caching policy is applied on the long-lived write handle when it opens.
        }
        #[cfg(target_os = "windows")]
        {
            // Windows gets the current stable path by pre-sizing here and using sequential
            // write hints on the long-lived write handle when it opens.
        }
        Ok(())
    }

    pub async fn open_download_file_for_write(
        path: &Path,
        config: &StorageConfig,
    ) -> Result<DownloadFile> {
        #[cfg(target_os = "linux")]
        if !config.no_io_uring {
            return open_download_file_for_write_linux_uring(path).await;
        }

        #[cfg(target_os = "linux")]
        if config.use_splice {
            return open_download_file_for_write_linux_splice(path).await;
        }

        #[cfg(target_os = "linux")]
        {
            return open_download_file_for_write_linux_tokio(path).await;
        }

        #[cfg(target_os = "macos")]
        {
            return open_download_file_for_write_macos(path).await;
        }

        #[cfg(target_os = "windows")]
        {
            return open_download_file_for_write_windows(path).await;
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            let file = OpenOptions::new().write(true).open(path).await?;
            Ok(DownloadFile {
                inner: DownloadFileInner::Tokio(file),
                backend: StorageBackendKind::Standard,
            })
        }
    }

    pub async fn write_all_at_tokio(file: &mut File, offset: u64, data: &[u8]) -> Result<()> {
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub async fn write_all_at_splice(file: &mut std::fs::File, offset: u64, data: &[u8]) -> Result<()> {
        use std::os::unix::fs::FileExt;

        let data = data.to_vec();
        // Clone the fd so the spawned closure owns its own handle
        let cloned = file.try_clone()?;

        tokio::task::spawn_blocking(move || {
            cloned.write_all_at(&data, offset)?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking failed: {e}"))??;

        Ok(())
    }

    #[cfg(target_os = "linux")]
    async fn open_download_file_for_write_linux_splice(path: &Path) -> Result<DownloadFile> {
        use std::os::unix::fs::OpenOptionsExt;

        let path = path.to_path_buf();
        let file = tokio::task::spawn_blocking(move || {
            // Open with O_APPEND disabled and O_WRONLY for posix write path
            std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(0) // no special flags
                .open(&path)
                .map_err(|e| anyhow!("failed to open file for splice write: {e}"))
        })
        .await
        .map_err(|e| anyhow!("spawn_blocking failed: {e}"))??;

        Ok(DownloadFile {
            inner: DownloadFileInner::LinuxSplice(file),
            backend: StorageBackendKind::LinuxSplice,
        })
    }

    #[cfg(target_os = "linux")]
    async fn open_download_file_for_write_linux_tokio(path: &Path) -> Result<DownloadFile> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .await?;
        Ok(DownloadFile {
            inner: DownloadFileInner::Tokio(file),
            backend: StorageBackendKind::LinuxTokio,
        })
    }

    #[cfg(target_os = "linux")]
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

                    let file = file;
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

    #[cfg(target_os = "macos")]
    async fn open_download_file_for_write_macos(path: &Path) -> Result<DownloadFile> {
        let std_file = std::fs::OpenOptions::new().write(true).open(path)?;
        rustix::fs::fcntl_nocache(&std_file, true)?;
        Ok(DownloadFile {
            inner: DownloadFileInner::Tokio(File::from_std(std_file)),
            backend: StorageBackendKind::MacosNoCache,
        })
    }

    #[cfg(target_os = "windows")]
    async fn open_download_file_for_write_windows(path: &Path) -> Result<DownloadFile> {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_SEQUENTIAL_SCAN;

        let file = OpenOptions::new()
            .write(true)
            .custom_flags(FILE_FLAG_SEQUENTIAL_SCAN)
            .open(path)
            .await?;
        Ok(DownloadFile {
            inner: DownloadFileInner::Tokio(file),
            backend: StorageBackendKind::WindowsSequential,
        })
    }
}
