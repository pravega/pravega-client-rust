use pravega_client::event::reader::{Event, EventReader, EventReaderError, SegmentSlice};
use pravega_client_shared::ScopedStream;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use tokio::runtime::Handle;
use crate::error::{clear_error, set_error};
use crate::memory::{Buffer, set_buffer};
use tokio::sync::mpsc::UnboundedSender;
use crate::reactor::*;
pub struct StreamReader {
    reader: EventReader,
    runtime_handle: Handle,
    streams: Vec<ScopedStream>,
    sender: UnboundedSender<Incoming>
}

impl StreamReader {
    pub fn new(reader: EventReader, runtime_handle: Handle, streams: Vec<ScopedStream>, sender: UnboundedSender<Incoming>) -> Self {
        StreamReader {
            reader,
            runtime_handle,
            streams,
            sender
        }
    }

    pub async fn get_segment_slice(&mut self) -> Result<Option<SegmentSlice>, EventReaderError> {
        self.reader.acquire_segment().await
    }

    pub fn release_segment(&mut self, slice: Option<SegmentSlice>) -> Result<(), EventReaderError> {
        if let Some(s) = slice {
            self.runtime_handle.block_on(self.reader.release_segment(s))?;
        }
        Ok(())
    }
}

pub struct SafeStreamReader {
    v: *mut StreamReader
}

impl SafeStreamReader {
    pub fn new(v: *mut StreamReader) -> Self {
        Self { v }
    }
    pub fn get(&self) -> &mut StreamReader {
        unsafe { &mut *(self.v) }
    }
}

unsafe impl Send for SafeStreamReader {}

impl Clone for SafeStreamReader {
    fn clone(&self) -> Self {
        Self { v: self.v.clone() }
    }
}

impl Copy for SafeStreamReader {}

#[no_mangle]
pub unsafe extern "C" fn stream_reader_get_segment_slice(reader: *mut StreamReader, chan_id: i64, err: Option<&mut Buffer>) {
    let stream_reader = &mut *reader;

    if let Err(_) = stream_reader.sender.send(Incoming::new(
        chan_id,
        Operation::GetSegmentSlice(SafeStreamReader::new(reader))
    )) {
        set_error("Error while getting segment slice".to_string(), err);
    }
}


#[no_mangle]
pub extern "C" fn segment_slice_destroy(slice: *mut Slice) {
    if !slice.is_null() {
        unsafe {
            Box::from_raw(slice);
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
    pub fn new(seg_slice: Option<SegmentSlice>) -> Self {
        Self {
            seg_slice
        }
    }

    fn get_set_to_none(&mut self) -> Option<SegmentSlice> {
        self.seg_slice.take()
    }
}

impl Slice {
    fn next(&mut self) -> Option<EventData> {
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

#[no_mangle]
pub unsafe extern "C" fn segment_slice_next(slice: *mut Slice, event: Option<&mut Buffer>, err: Option<&mut Buffer>) {
    let slice = &mut *slice;
    match catch_unwind(AssertUnwindSafe(move || { slice.next()})) {
        Ok(result) => {
            match result {
                Some(data) => {
                    set_buffer(data.value, event);
                },
                None => {
                    set_buffer(Vec::new(), event);
                }
            }
        }
        Err(_) => {
            set_error("caught panic".to_string(), err);
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
