use std::path::Path;

use anyhow::Result;
use tokio::fs::{File, OpenOptions};

pub fn prepare_download_file(path: &Path, total_size: u64) -> Result<()> {
    let file = std::fs::File::create(path)?;
    file.set_len(total_size)?;
    platform::prepare_download_file(&file, total_size)?;
    Ok(())
}

pub async fn open_download_file_for_write(path: &Path) -> Result<File> {
    let file = OpenOptions::new().write(true).open(path).await?;
    platform::configure_write_file(&file)?;
    Ok(file)
}

#[cfg(target_os = "linux")]
mod platform {
    use anyhow::Result;
    use tokio::fs::File;

    pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
        let _ = (file, total_size);
        // TODO(io/linux): Add O_DIRECT-aware preallocation once the Linux storage path
        // moves behind a dedicated io_uring/direct-I/O implementation.
        Ok(())
    }

    pub fn configure_write_file(file: &File) -> Result<()> {
        let _ = file;
        // TODO(io/linux): Replace tokio::fs writes with a Linux-specific io_uring storage
        // backend. Direct I/O will also require aligned buffers and stricter write sizing.
        Ok(())
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use anyhow::Result;
    use tokio::fs::File;

    pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
        let _ = (file, total_size);
        // TODO(io/macos): Apply Darwin-specific preallocation and document whether the
        // target volume benefits from sparse vs eager allocation for large files.
        Ok(())
    }

    pub fn configure_write_file(file: &File) -> Result<()> {
        let _ = file;
        // TODO(io/macos): Evaluate F_NOCACHE on the raw descriptor for large downloads.
        // This should stay opt-in until we verify it helps without harming small files.
        Ok(())
    }
}

#[cfg(target_os = "windows")]
mod platform {
    use anyhow::Result;
    use tokio::fs::File;

    pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
        let _ = (file, total_size);
        // TODO(io/windows): Investigate SetFileValidData and the required privileges before
        // using it. We should not enable it by default without a safe capability check.
        Ok(())
    }

    pub fn configure_write_file(file: &File) -> Result<()> {
        let _ = file;
        // TODO(io/windows): Evaluate FILE_FLAG_NO_BUFFERING / FILE_FLAG_OVERLAPPED via a
        // Windows-specific storage backend. This will also need aligned buffers.
        Ok(())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod platform {
    use anyhow::Result;
    use tokio::fs::File;

    pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
        let _ = (file, total_size);
        Ok(())
    }

    pub fn configure_write_file(file: &File) -> Result<()> {
        let _ = file;
        Ok(())
    }
}
