use std::path::Path;
use std::ptr::NonNull;

use anyhow::Result;
use tokio::fs::{File, OpenOptions};

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

pub struct DownloadFile {
    inner: File,
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
        platform::write_all_at(&mut self.inner, offset, data).await
    }
}

pub async fn open_download_file_for_write(path: &Path) -> Result<DownloadFile> {
    let file = OpenOptions::new().write(true).open(path).await?;
    let backend = platform::configure_write_file(&file)?;
    Ok(DownloadFile { inner: file, backend })
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
mod platform {
    use anyhow::Result;
    use tokio::fs::File;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};

    use crate::storage::StorageBackendKind;

    pub const DIRECT_IO_ALIGNMENT: usize = 4096;
    #[cfg(feature = "linux-io-uring-experimental")]
    pub const DIRECT_IO_BLOCK_BYTES: usize = 4096;

    pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
        let _ = (file, total_size);
        // TODO(io/linux): Add O_DIRECT-aware preallocation once the Linux storage path
        // moves behind a dedicated io_uring/direct-I/O implementation.
        Ok(())
    }

    pub fn configure_write_file(file: &File) -> Result<StorageBackendKind> {
        let _ = file;
        #[cfg(feature = "linux-io-uring-experimental")]
        {
            let _ = (DIRECT_IO_ALIGNMENT, DIRECT_IO_BLOCK_BYTES);
            // TODO(io/linux): Replace this placeholder with a real io_uring-backed writer.
            // The backend contract is now explicit:
            // - aligned buffers at `DIRECT_IO_ALIGNMENT`
            // - direct-I/O-sized writes in `DIRECT_IO_BLOCK_BYTES` chunks
            // - fallback to buffered Tokio writes when ranges end with a short tail
            return Ok(StorageBackendKind::LinuxIoUringExperimental);
        }

        #[cfg(not(feature = "linux-io-uring-experimental"))]
        {
            // Current stable Linux path: Tokio file writes behind the storage seam.
            Ok(StorageBackendKind::LinuxTokio)
        }
    }

    pub async fn write_all_at(file: &mut File, offset: u64, data: &[u8]) -> Result<()> {
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        Ok(())
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use anyhow::Result;
    use tokio::fs::File;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};

    use crate::storage::StorageBackendKind;

    pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
        let _ = (file, total_size);
        // TODO(io/macos): Apply Darwin-specific preallocation and document whether the
        // target volume benefits from sparse vs eager allocation for large files.
        Ok(())
    }

    pub fn configure_write_file(file: &File) -> Result<StorageBackendKind> {
        let _ = file;
        // TODO(io/macos): Evaluate F_NOCACHE on the raw descriptor for large downloads.
        // This should stay opt-in until we verify it helps without harming small files.
        Ok(StorageBackendKind::MacosPlanned)
    }

    pub async fn write_all_at(file: &mut File, offset: u64, data: &[u8]) -> Result<()> {
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        Ok(())
    }
}

#[cfg(target_os = "windows")]
mod platform {
    use anyhow::Result;
    use tokio::fs::File;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};

    use crate::storage::StorageBackendKind;

    pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
        let _ = (file, total_size);
        // TODO(io/windows): Investigate SetFileValidData and the required privileges before
        // using it. We should not enable it by default without a safe capability check.
        Ok(())
    }

    pub fn configure_write_file(file: &File) -> Result<StorageBackendKind> {
        let _ = file;
        // TODO(io/windows): Evaluate FILE_FLAG_NO_BUFFERING / FILE_FLAG_OVERLAPPED via a
        // Windows-specific storage backend. This will also need aligned buffers.
        Ok(StorageBackendKind::WindowsPlanned)
    }

    pub async fn write_all_at(file: &mut File, offset: u64, data: &[u8]) -> Result<()> {
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        Ok(())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod platform {
    use anyhow::Result;
    use tokio::fs::File;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};

    use crate::storage::StorageBackendKind;

    pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
        let _ = (file, total_size);
        Ok(())
    }

    pub fn configure_write_file(file: &File) -> Result<StorageBackendKind> {
        let _ = file;
        Ok(StorageBackendKind::Standard)
    }

    pub async fn write_all_at(file: &mut File, offset: u64, data: &[u8]) -> Result<()> {
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        Ok(())
    }
}
