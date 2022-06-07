use pravega_client::event::reader::{Event, EventReader, EventReaderError, SegmentSlice};
use pravega_client_shared::ScopedStream;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use tokio::runtime::Handle;
use crate::error::set_error;
use crate::memory::Buffer;

pub struct StreamReader {
    reader: EventReader,
    runtime_handle: Handle,
    streams: Vec<ScopedStream>,
}

impl StreamReader {
    pub fn new(reader: EventReader, runtime_handle: Handle, streams: Vec<ScopedStream>) -> Self {
        StreamReader {
            reader,
            runtime_handle,
            streams,
        }
    }

    pub fn get_segment_slice(&mut self) -> Result<Option<SegmentSlice>, EventReaderError> {
        self.runtime_handle.block_on(self.reader.acquire_segment())
    }

    pub fn release_segment(&mut self, slice: Option<SegmentSlice>) -> Result<(), EventReaderError> {
        if let Some(s) = slice {
            self.runtime_handle.block_on(self.reader.release_segment(s))?;
        }
        Ok(())
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_reader_get_segment_slice(reader: *mut StreamReader, err: Option<&mut Buffer>) -> *mut Slice {
    let stream_reader = &mut *reader;
    match catch_unwind(AssertUnwindSafe(move || { stream_reader.get_segment_slice()})) {
        Ok(result) => {
            match result {
                Ok(seg_slice) => Box::into_raw(Box::new(Slice{ seg_slice })),
                Err(e) => {
                    set_error(format!("Error while attempting to acquire segment {:?}", e), err);
                    ptr::null_mut()
                }
            }
        }
        Err(_) => {
            set_error("caught panic".to_string(), err);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_reader_release_segment_slice(reader: *mut StreamReader, slice: *mut Slice, err: Option<&mut Buffer>) {
    let stream_reader = &mut *reader;
    let slice = &mut *slice;
    match catch_unwind(AssertUnwindSafe(move || { stream_reader.release_segment(slice.get_set_to_none())})) {
        Ok(result) => {
            if let Err(e) = result {
                set_error(format!("Error while releasing segment {:?}", e), err);
            }
        
        }
        Err(_) => {
            set_error("caught panic".to_string(), err);
        }
    }
}

pub struct Slice {
    seg_slice: Option<SegmentSlice>,
}

impl Slice {
    fn get_set_to_none(&mut self) -> Option<SegmentSlice> {
        self.seg_slice.take()
    }
}

impl Slice {
    fn next(mut self) -> Option<EventData> {
        if let Some(mut slice) = self.seg_slice.take() {
            let next_event: Option<Event> = slice.next();
            self.seg_slice = Some(slice);
            next_event.map(|e| EventData {
                offset_in_segment: e.offset_in_segment,
                value: e.value,
            })
        } else {
            None
        }
    }
}

pub struct EventData {
    offset_in_segment: i64,
    value: Vec<u8>,
}

impl EventData {
    ///Return the data
    fn data(&self) -> &[u8] {
        self.value.as_slice()
    }
    /// Returns the string representation.
    fn to_str(&self) -> String {
        format!("offset {:?} data :{:?}", self.offset_in_segment, self.value)
    }
}
