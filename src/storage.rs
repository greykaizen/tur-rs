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
            DownloadFileInner::LinuxPwrite(file) => {
                platform::write_all_at_pwrite(file, offset, data).await
            }
            #[cfg(target_os = "linux")]
            DownloadFileInner::LinuxSplice { pipe_write, pipe_read, file } => {
                platform::write_all_at_splice(file, pipe_read, pipe_write, offset, data).await
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
            #[cfg(target_os = "macos")]
            DownloadFileInner::MacosPwrite(file) => {
                platform::write_all_at_pwrite(file, offset, data).await
            }
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
    #[cfg(target_os = "linux")]
    const SPLICE_CHUNK_BYTES: usize = 65536;

    pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
        let _ = (file, total_size);
        Ok(())
    }

    pub async fn open_download_file_for_write(
        path: &Path,
        config: &StorageConfig,
    ) -> Result<DownloadFile> {
        #[cfg(target_os = "linux")]
        if !config.no_io_uring {
            if let Ok(file) = open_download_file_for_write_linux_uring(path).await {
                return Ok(file);
            }
        }

        #[cfg(target_os = "linux")]
        if config.use_splice {
            if let Ok(file) = open_download_file_for_write_linux_splice(path).await {
                return Ok(file);
            }
        }

        #[cfg(target_os = "linux")]
        if config.use_pwrite {
            return open_download_file_for_write_linux_pwrite(path).await;
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
            return open_download_file_for_write_windows(path, config).await;
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

    // ── Linux backends ──

    #[cfg(target_os = "linux")]
    pub async fn write_all_at_pwrite(file: &mut std::fs::File, offset: u64, data: &[u8]) -> Result<()> {
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

    #[cfg(target_os = "linux")]
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
            // Interleave writes to pipe with splices to file in ~64KB chunks.
            // This avoids deadlocking when data exceeds the default pipe capacity (64KB).
            let mut written: u64 = 0;
            let mut file_offset = offset;
            while written < len as u64 {
                let remaining = (len as u64).saturating_sub(written);
                let chunk_size = (remaining as usize).min(SPLICE_CHUNK_BYTES);
                let start = written as usize;
                let end = start + chunk_size;

                // 1. Write a chunk to the pipe
                pipe_write_clone.write_all(&data_vec[start..end])?;

                // 2. Splice it from pipe read end to file
                match splice(
                    &pipe_read_clone,
                    None,
                    &file_clone,
                    Some(&mut file_offset),
                    chunk_size,
                    SpliceFlags::MOVE,
                ) {
                    Ok(n) if n > 0 => {
                        written += n as u64;
                    }
                    Ok(_) => break,
                    Err(rustix::io::Errno::NOSYS)
                    | Err(rustix::io::Errno::INVAL) => {
                        // splice not available on this kernel/FS — drain any remaining
                        // data from pipe and fall back to pwrite
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

            // Clear any residual data in the pipe
            let _ = pipe_read_clone.set_len(0);

            Ok(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking failed: {e}"))??;

        Ok(())
    }

    #[cfg(target_os = "linux")]
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

    #[cfg(target_os = "linux")]
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
            let (pipe_read_fd, pipe_write_fd) = pipe()
                .map_err(|e| anyhow!("pipe() failed: {e}"))?;
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

    // ── macOS backend ──

    #[cfg(target_os = "macos")]
    async fn open_download_file_for_write_macos(path: &Path) -> Result<DownloadFile> {
        use std::os::unix::fs::OpenOptionsExt;

        let path = path.to_path_buf();
        let file = tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(0)
                .open(&path)
                .map_err(|e| anyhow!("failed to open file for macos pwrite: {e}"))?;

            // Disable page cache (fcntl_nocache equivalent on macOS)
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

    #[cfg(target_os = "macos")]
    pub async fn write_all_at_pwrite(file: &mut std::fs::File, offset: u64, data: &[u8]) -> Result<()> {
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

    // ── Windows backends ──

    #[cfg(target_os = "windows")]
    async fn open_download_file_for_write_windows(
        path: &Path,
        config: &StorageConfig,
    ) -> Result<DownloadFile> {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_NO_BUFFERING, FILE_FLAG_OVERLAPPED, FILE_FLAG_SEQUENTIAL_SCAN,
        };

        let path = path.to_path_buf();
        let no_direct_io = config.no_direct_io;

        let (file, use_direct_io) = tokio::task::spawn_blocking(move || {
            if no_direct_io {
                // Buffered path with sequential scan hint
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .custom_flags(FILE_FLAG_SEQUENTIAL_SCAN)
                    .open(&path)
                    .map_err(|e| anyhow!("failed to open file for windows write: {e}"))?;
                return Ok::<_, anyhow::Error>((file, false));
            }

            // Try Direct I/O (FILE_FLAG_NO_BUFFERING)
            match std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED)
                .open(&path)
            {
                Ok(file) => Ok((file, true)),
                Err(e) => {
                    // Direct I/O may fail on network drives — fall back to buffered
                    let file = std::fs::OpenOptions::new()
                        .write(true)
                        .custom_flags(FILE_FLAG_SEQUENTIAL_SCAN)
                        .open(&path)
                        .map_err(|e2| anyhow!("failed to open file for windows write (direct io failed: {e}, fallback: {e2})"))?;
                    Ok((file, false))
                }
            }
        })
        .await
        .map_err(|e| anyhow!("spawn_blocking failed: {e}"))??;

        if use_direct_io {
            Ok(DownloadFile {
                inner: DownloadFileInner::WindowsDirectIo(file),
                backend: StorageBackendKind::WindowsDirectIo,
            })
        } else {
            Ok(DownloadFile {
                inner: DownloadFileInner::WindowsPwrite(file),
                backend: StorageBackendKind::WindowsPwrite,
            })
        }
    }

    #[cfg(target_os = "windows")]
    pub async fn write_all_at_windows_pwrite(
        file: &mut std::fs::File,
        offset: u64,
        data: &[u8],
    ) -> Result<()> {
        use windows_sys::Win32::Storage::FileSystem::{
            SetFilePointerEx, WriteFile,
        };
        use std::os::windows::io::AsRawHandle;

        let data = data.to_vec();
        let handle = file.as_raw_handle();
        let offset = offset;

        tokio::task::spawn_blocking(move || {
            unsafe {
                let mut bytes_written: u32 = 0;
                let li_offset: i64 = offset as i64;
                // FILE_BEGIN = 0 means the offset is absolute from the start of the file
                if SetFilePointerEx(handle, li_offset, std::ptr::null_mut(), 0u32) == 0 {
                    return Err(anyhow::anyhow!("SetFilePointerEx failed"));
                }
                if WriteFile(
                    handle,
                    data.as_ptr() as *const std::ffi::c_void,
                    data.len() as u32,
                    &mut bytes_written,
                    std::ptr::null(),
                ) == 0
                {
                    return Err(anyhow::anyhow!("WriteFile failed"));
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking failed: {e}"))??;

        Ok(())
    }

    #[cfg(target_os = "windows")]
    pub async fn write_all_at_windows_direct_io(
        file: &mut std::fs::File,
        offset: u64,
        data: &[u8],
    ) -> Result<()> {
        use windows_sys::Win32::Storage::FileSystem::WriteFile;
        use std::os::windows::io::AsRawHandle;

        // For Direct I/O, we need aligned buffers and aligned offsets/byte counts.
        let buf_ptr = data.as_ptr() as usize;
        let offset_aligned = offset % DIRECT_IO_ALIGNMENT as u64 == 0;
        let ptr_aligned = buf_ptr % DIRECT_IO_ALIGNMENT == 0;
        let len_aligned = data.len() % DIRECT_IO_ALIGNMENT == 0;

        if offset_aligned && ptr_aligned && len_aligned {
            let handle = file.as_raw_handle();
            let data = data.to_vec();

            tokio::task::spawn_blocking(move || {
                unsafe {
                    let mut bytes_written: u32 = 0;
                    let mut overlapped: std::mem::MaybeUninit<windows_sys::Win32::System::IO::OVERLAPPED> =
                        std::mem::MaybeUninit::zeroed();
                    let ov = overlapped.assume_init_mut();
                    ov.Anonymous.Anonymous.Offset = offset as u32;
                    ov.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;

                    if WriteFile(
                        handle,
                        data.as_ptr() as *const std::ffi::c_void,
                        data.len() as u32,
                        &mut bytes_written,
                        ov as *mut _,
                    ) == 0
                    {
                        let err = std::io::Error::last_os_error();
                        if err.raw_os_error() == Some(997) {
                            // ERROR_IO_PENDING = 997
                            let mut bytes = 0u32;
                            if windows_sys::Win32::System::IO::GetOverlappedResult(
                                handle,
                                ov as *mut _,
                                &mut bytes,
                                1, // bWait = TRUE
                            ) == 0
                            {
                                return Err(anyhow::anyhow!("GetOverlappedResult failed: {}", std::io::Error::last_os_error()));
                            }
                        } else {
                            return Err(anyhow::anyhow!("WriteFile failed: {}", err));
                        }
                    }
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
            .map_err(|e| anyhow::anyhow!("spawn_blocking failed: {e}"))??;
        } else {
            // Fall back to pwrite (buffered) for unaligned data
            write_all_at_windows_pwrite(file, offset, data).await?;
        }

        Ok(())
    }
}
