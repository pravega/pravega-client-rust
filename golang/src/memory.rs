use std::mem;
use std::slice;

#[derive(Copy, Clone)]
#[repr(C)]
pub struct Buffer {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}

impl Buffer {
    pub fn from_vec(mut v: Vec<u8>) -> Self {
        let buf = Buffer {
            ptr: v.as_mut_ptr(),
            len: v.len(),
            cap: v.capacity(),
        };
        mem::forget(v);
        buf
    }

    pub fn to_vec(self) -> Vec<u8> {
        if self.is_empty() {
            return Vec::new();
        }
        let mut v = unsafe { 
            Vec::from_raw_parts(self.ptr, self.len, self.cap) 
        };
        v.shrink_to_fit();
        v
    }

    pub fn read(&self) -> Option<&[u8]> {
        if self.is_empty() {
            None
        } else {
            unsafe { Some(slice::from_raw_parts(self.ptr, self.len)) }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.ptr.is_null() || self.len == 0 || self.cap == 0
    }
}

pub fn set_buffer(v: Vec<u8>, buf: Option<&mut Buffer>) {
    if let Some(mb) = buf {
        *mb = Buffer::from_vec(v);
    }
}

#[no_mangle]
pub extern "C" fn free_buffer(buf: Buffer) {
    buf.to_vec();
}
