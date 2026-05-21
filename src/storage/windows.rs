use super::*;

use anyhow::anyhow;

pub const DIRECT_IO_ALIGNMENT: usize = 4096;

pub fn prepare_download_file(file: &std::fs::File, total_size: u64) -> Result<()> {
    let _ = (file, total_size);
    Ok(())
}

pub async fn open_download_file_for_write(
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
            let file = std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(FILE_FLAG_SEQUENTIAL_SCAN)
                .open(&path)
                .map_err(|e| anyhow!("failed to open file for windows write: {e}"))?;
            return Ok::<_, anyhow::Error>((file, false));
        }

        match std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED)
            .open(&path)
        {
            Ok(file) => Ok((file, true)),
            Err(e) => {
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

pub async fn write_all_at_tokio(file: &mut File, offset: u64, data: &[u8]) -> Result<()> {
    file.seek(SeekFrom::Start(offset)).await?;
    file.write_all(data).await?;
    Ok(())
}

pub async fn write_all_at_windows_pwrite(
    file: &mut std::fs::File,
    offset: u64,
    data: &[u8],
) -> Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{SetFilePointerEx, WriteFile};

    let data = data.to_vec();
    let handle = file.as_raw_handle();

    tokio::task::spawn_blocking(move || {
        unsafe {
            let mut bytes_written: u32 = 0;
            let li_offset: i64 = offset as i64;
            if SetFilePointerEx(handle, li_offset, std::ptr::null_mut(), 0u32) == 0 {
                return Err(anyhow::anyhow!("SetFilePointerEx failed"));
            }
            if WriteFile(
                handle,
                data.as_ptr(),
                data.len() as u32,
                &mut bytes_written,
                std::ptr::null_mut(),
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

pub async fn write_all_at_windows_direct_io(
    file: &mut std::fs::File,
    offset: u64,
    data: &[u8],
) -> Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::WriteFile;

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
                let mut overlapped: std::mem::MaybeUninit<
                    windows_sys::Win32::System::IO::OVERLAPPED,
                > = std::mem::MaybeUninit::zeroed();
                let ov = overlapped.assume_init_mut();
                ov.Anonymous.Anonymous.Offset = offset as u32;
                ov.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;

                if WriteFile(
                    handle,
                    data.as_ptr(),
                    data.len() as u32,
                    &mut bytes_written,
                    ov as *mut _,
                ) == 0
                {
                    let err = std::io::Error::last_os_error();
                    if err.raw_os_error() == Some(997) {
                        let mut bytes = 0u32;
                        if windows_sys::Win32::System::IO::GetOverlappedResult(
                            handle,
                            ov as *mut _,
                            &mut bytes,
                            1,
                        ) == 0
                        {
                            return Err(anyhow::anyhow!(
                                "GetOverlappedResult failed: {}",
                                std::io::Error::last_os_error()
                            ));
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
        write_all_at_windows_pwrite(file, offset, data).await?;
    }

    Ok(())
}
