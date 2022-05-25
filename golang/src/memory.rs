use std::mem;

#[derive(Copy, Clone)]
#[repr(C)]
pub struct Buffer {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}

// this frees memory we released earlier
#[no_mangle]
pub extern "C" fn free_buffer(buf: Buffer) {
    unsafe {
        let _ = buf.consume();
    }
}

impl Buffer {
    // consume must only be used on memory previously released by from_vec
    // when the Vec is out of scope, it will deallocate the memory previously referenced by Buffer
    pub unsafe fn consume(self) -> Vec<u8> {
        if self.is_empty() {
            return Vec::new();
        }
        let mut v = Vec::from_raw_parts(self.ptr, self.len, self.cap);
        v.shrink_to_fit();
        v
    }

    // this releases our memory to the caller
    pub fn from_vec(mut v: Vec<u8>) -> Self {
        let buf = Buffer {
            ptr: v.as_mut_ptr(),
            len: v.len(),
            cap: v.capacity(),
        };
        mem::forget(v);
        buf
    }

    pub fn is_empty(&self) -> bool {
        self.ptr.is_null() || self.len == 0 || self.cap == 0
    }
}
