use std::ptr::NonNull;

use anyhow::Result;
#[cfg(target_os = "linux")]
use tokio::sync::oneshot;

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
        let layout =
            std::alloc::Layout::from_size_align(len, align).expect("valid aligned buffer layout");
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

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
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
pub enum LinuxIoUringCommand {
    WriteAllAt {
        offset: u64,
        data: AlignedBuffer,
        resp: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}
