use std::path::Path;
use std::ptr::NonNull;

use anyhow::Result;
#[cfg(all(target_os = "linux", feature = "linux-io-uring-experimental"))]
use anyhow::anyhow;
use tokio::fs::File;
#[cfg(not(all(target_os = "linux", feature = "linux-io-uring-experimental")))]
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
#[cfg(all(target_os = "linux", feature = "linux-io-uring-experimental"))]
use tokio::sync::{mpsc, oneshot};

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
    LinuxIoUringExperimental,
    MacosPlanned,
    WindowsPlanned,
}

enum DownloadFileInner {
    #[cfg_attr(all(target_os = "linux", feature = "linux-io-uring-experimental"), allow(dead_code))]
    Tokio(File),
    #[cfg(all(target_os = "linux", feature = "linux-io-uring-experimental"))]
    LinuxIoUring {
        tx: mpsc::UnboundedSender<LinuxIoUringCommand>,
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
            StorageBackendKind::LinuxIoUringExperimental => Some(platform::DIRECT_IO_ALIGNMENT),
            _ => None,
        }
    }

    pub async fn write_all_at(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        match &mut self.inner {
            DownloadFileInner::Tokio(file) => platform::write_all_at_tokio(file, offset, data).await,
            #[cfg(all(target_os = "linux", feature = "linux-io-uring-experimental"))]
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
        #[cfg(all(target_os = "linux", feature = "linux-io-uring-experimental"))]
        if let DownloadFileInner::LinuxIoUring { tx, .. } = &self.inner {
            let _ = tx.send(LinuxIoUringCommand::Shutdown);
        }
    }
}

pub async fn open_download_file_for_write(path: &Path) -> Result<DownloadFile> {
    platform::open_download_file_for_write(path).await
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

#[cfg(all(target_os = "linux", feature = "linux-io-uring-experimental"))]
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

#[cfg(all(target_os = "linux", feature = "linux-io-uring-experimental"))]
unsafe impl Send for AlignedBuffer {}

#[cfg(all(target_os = "linux", feature = "linux-io-uring-experimental"))]
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
    #[cfg(all(target_os = "linux", feature = "linux-io-uring-experimental"))]
    #[allow(dead_code)]
    pub const DIRECT_IO_BLOCK_BYTES: usize = 4096;

    pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
        let _ = (file, total_size);
        #[cfg(target_os = "linux")]
        {
            // TODO(io/linux): Add O_DIRECT-aware preallocation once the Linux storage path
            // moves behind a dedicated io_uring/direct-I/O implementation.
        }
        #[cfg(target_os = "macos")]
        {
            // TODO(io/macos): Apply Darwin-specific preallocation and document whether the
            // target volume benefits from sparse vs eager allocation for large files.
        }
        #[cfg(target_os = "windows")]
        {
            // TODO(io/windows): Investigate SetFileValidData and the required privileges before
            // using it. We should not enable it by default without a safe capability check.
        }
        Ok(())
    }

    pub async fn open_download_file_for_write(path: &Path) -> Result<DownloadFile> {
        #[cfg(all(target_os = "linux", feature = "linux-io-uring-experimental"))]
        {
            return open_download_file_for_write_linux_uring(path).await;
        }

        #[cfg(not(all(target_os = "linux", feature = "linux-io-uring-experimental")))]
        {
            let file = OpenOptions::new().write(true).open(path).await?;
            let backend = detect_backend_kind();
            Ok(DownloadFile {
                inner: DownloadFileInner::Tokio(file),
                backend,
            })
        }
    }

    #[cfg_attr(all(target_os = "linux", feature = "linux-io-uring-experimental"), allow(dead_code))]
    fn detect_backend_kind() -> StorageBackendKind {
        #[cfg(target_os = "linux")]
        {
            return StorageBackendKind::LinuxTokio;
        }
        #[cfg(target_os = "macos")]
        {
            return StorageBackendKind::MacosPlanned;
        }
        #[cfg(target_os = "windows")]
        {
            return StorageBackendKind::WindowsPlanned;
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            return StorageBackendKind::Standard;
        }
    }

    pub async fn write_all_at_tokio(file: &mut File, offset: u64, data: &[u8]) -> Result<()> {
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        Ok(())
    }

    #[cfg(all(target_os = "linux", feature = "linux-io-uring-experimental"))]
    async fn open_download_file_for_write_linux_uring(path: &Path) -> Result<DownloadFile> {
        use std::os::unix::fs::OpenOptionsExt;

        let (tx, mut rx) = mpsc::unbounded_channel::<LinuxIoUringCommand>();
        let path = path.to_path_buf();
        let fallback = File::from_std(std::fs::OpenOptions::new().write(true).open(&path)?);
        std::thread::Builder::new()
            .name("tur-io-uring".to_string())
            .spawn(move || {
                tokio_uring::start(async move {
                    let mut options = tokio_uring::fs::OpenOptions::new();
                    options.write(true);
                    options.custom_flags(libc::O_DIRECT);
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
            backend: StorageBackendKind::LinuxIoUringExperimental,
        })
    }
}
