use libc::c_char;
use pravega_client::event::reader_group::ReaderGroup;
use std::ffi::CStr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use tokio::runtime::Handle;
use crate::error::{clear_error, set_error};
use crate::memory::Buffer;
use crate::stream_reader::StreamReader;

pub struct StreamReaderGroup {
    reader_group: ReaderGroup,
    runtime_handle: Handle,
}

impl StreamReaderGroup {
    pub fn new(reader_group: ReaderGroup, runtime_handle: Handle) -> Self {
        StreamReaderGroup {
            reader_group,
            runtime_handle,
        }
    }

    pub fn create_reader(&self, reader_name: &str) -> StreamReader {
        let reader = self
            .runtime_handle
            .block_on(self.reader_group.create_reader(reader_name.to_string()));
        StreamReader::new(
            reader,
            self.runtime_handle.clone(),
            self.reader_group.get_managed_streams(),
        )
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_reader_group_create_reader(reader_group: *const StreamReaderGroup, reader: *const c_char, err: Option<&mut Buffer>) -> *mut StreamReader {
    let raw = CStr::from_ptr(reader);
    let reader_name = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("failed to parse reader".to_string(), err);
            return ptr::null_mut();
        }
    };

    let rg = &*reader_group;
    match catch_unwind(AssertUnwindSafe(move || { rg.create_reader(reader_name)})) {
        Ok(reader) => {
            clear_error();
            Box::into_raw(Box::new(reader))
        },
        Err(_) => {
            set_error("caught panic".to_string(), err);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn stream_reader_destroy(reader: *mut StreamReader) {
    if !reader.is_null() {
        unsafe {
            Box::from_raw(reader);
        }
    }
}
