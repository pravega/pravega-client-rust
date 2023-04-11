use crate::error::set_error;
use crate::memory::{ackOperationDone, set_buffer, Buffer};
use pravega_client::event::reader::{Event, EventReader, EventReaderError, SegmentSlice};
use std::panic::{catch_unwind, AssertUnwindSafe};
use tokio::runtime::Handle;

pub struct StreamReader {
    reader: EventReader,
    runtime_handle: Handle,
}

impl StreamReader {
    pub fn new(reader: EventReader, runtime_handle: Handle) -> Self {
        StreamReader {
            reader,
            runtime_handle,
        }
    }

    pub async fn get_segment_slice(&mut self) -> Result<Option<SegmentSlice>, EventReaderError> {
        self.reader.acquire_segment().await
    }
    
    pub fn release_segment(
        &mut self,
        slice: Option<SegmentSlice>,
    ) -> Result<(), Box<pravega_client::event::reader::EventReaderError>> {
        if let Some(s) = slice {
            self.runtime_handle.block_on(self.reader.release_segment(s))?;
        }
        Ok(())
    }
}

#[no_mangle]
pub extern "C" fn stream_reader_get_segment_slice(reader: *mut StreamReader, id: i64) {
    let stream_reader = unsafe { &mut *reader };

    let handle = stream_reader.runtime_handle.clone();
    handle.spawn(async move {
        match stream_reader.get_segment_slice().await {
            Ok(seg_slice) => {
                let ptr = Box::into_raw(Box::new(Slice::new(seg_slice)));
                unsafe {
                    ackOperationDone(id, ptr as usize);
                };
            }
            Err(err) => {
                // TODO: send error msg through the channel
                println!("Error while getting segment slice {:?}", err);
            }
        }
    });
}

#[no_mangle]
pub extern "C" fn segment_slice_destroy(slice: *mut Slice) {
    if !slice.is_null() {
        unsafe {
            drop(Box::from_raw(slice));
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_reader_release_segment_slice(
    reader: *mut StreamReader,
    slice: *mut Slice,
    err: Option<&mut Buffer>,
) {
    let stream_reader = &mut *reader;
    let slice = &mut *slice;
    match catch_unwind(AssertUnwindSafe(move || {
        stream_reader.release_segment(slice.get_set_to_none())
    })) {
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
        Self { seg_slice }
    }

    fn get_set_to_none(&mut self) -> Option<SegmentSlice> {
        self.seg_slice.take()
    }
}

impl Slice {
    fn next(&mut self) -> Option<Event> {
        if let Some(mut slice) = self.seg_slice.take() {
            let next_event: Option<Event> = slice.next();
            self.seg_slice = Some(slice);
            next_event
        } else {
            None
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn segment_slice_next(
    slice: *mut Slice,
    event: Option<&mut Buffer>,
    err: Option<&mut Buffer>,
) {
    let slice = &mut *slice;
    match catch_unwind(AssertUnwindSafe(move || slice.next())) {
        Ok(result) => match result {
            Some(data) => {
                set_buffer(data.value, event);
            }
            None => {
                set_buffer(Vec::new(), event);
            }
        },
        Err(_) => {
            set_error("caught panic".to_string(), err);
        }
    }
}
