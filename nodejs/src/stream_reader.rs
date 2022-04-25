// Copyright Pravega Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use neon::prelude::*;
use pravega_client::event::reader::{Event, SegmentSlice};
use pravega_client::event::reader::{EventReader, EventReaderError};
use pravega_client_shared::ScopedStream;
use std::cell::RefCell;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tracing::info;

impl Finalize for EventData {}

///
/// This represents an event that was read from a Pravega Segment and the offset at which the event
/// was read from.
///
#[derive(new)]
pub(crate) struct EventData {
    offset_in_segment: i64,
    value: Vec<u8>,
}

impl EventData {
    ///
    /// Return the event data as `ArrayBuffer`.
    ///
    pub fn js_data(mut cx: FunctionContext) -> JsResult<JsArrayBuffer> {
        let event_data = cx.this().downcast_or_throw::<JsBox<EventData>, _>(&mut cx)?;
        let data = event_data.value.as_slice();

        Ok(JsArrayBuffer::external(&mut cx, data.to_owned()))
    }

    ///
    /// Return the event offset in the segment.
    ///
    pub fn js_offset(mut cx: FunctionContext) -> JsResult<JsNumber> {
        let event_data = cx.this().downcast_or_throw::<JsBox<EventData>, _>(&mut cx)?;
        // this may result to a f64::MAX, but I'm not able to find a f64: From<i64> impl
        let offset = event_data.offset_in_segment as f64;

        Ok(cx.number(offset))
    }

    ///
    /// Return the string representation.
    ///
    pub fn js_to_str(mut cx: FunctionContext) -> JsResult<JsString> {
        let event_data = cx.this().downcast_or_throw::<JsBox<EventData>, _>(&mut cx)?;
        Ok(cx.string(format!(
            "offset {:?} data :{:?}",
            event_data.offset_in_segment, event_data.value
        )))
    }
}

impl Finalize for Slice {}

///
/// This represents a segment slice which can be used to read events from a Pravega segment.
///
#[derive(new)]
pub(crate) struct Slice {
    seg_slice: Option<SegmentSlice>,
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
            info!("Empty Slice");
            None
        }
    }

    pub fn js_next(mut cx: FunctionContext) -> JsResult<JsBox<EventData>> {
        // Use RefCell as self.seg_slice needs to be changed.
        // Example at https://github.com/neon-bindings/neon/blob/0.10.0/test/napi/src/js/boxed.rs
        let slice = cx.argument::<JsBox<RefCell<Slice>>>(0)?;
        let event_data = slice.borrow_mut().next();

        match event_data {
            Some(event_data) => Ok(cx.boxed(event_data)),
            None => cx.throw_error("No more data in the stream!"),
        }
    }
}

impl Finalize for StreamReader {}

///
/// This represents a Stream reader for a given Stream.
/// Note: A StreamReader cannot be created directly without using the StreamReaderGroup.
///
#[derive(new)]
pub(crate) struct StreamReader {
    reader: Arc<Mutex<EventReader>>,
    runtime_handle: Handle,
    streams: Vec<ScopedStream>,
}

impl StreamReader {
    // Helper method for to set the reader offline.
    async fn reader_offline_async(&self) -> Result<(), EventReaderError> {
        self.reader.lock().await.reader_offline().await
    }

    // Helper method for to release the segment.
    async fn release_segment_async(&self, slice: SegmentSlice) -> Result<(), EventReaderError> {
        self.reader.lock().await.release_segment(slice).await
    }
}

impl StreamReader {
    ///
    /// Return a Slice in an asynchronous call.
    /// The actual returned type from await will be `Promise<Slice>` aka `JsResult<JsBoxed<RefCell<Slice>>>`.
    ///
    /// See the tokio-fetch example for more details on how to return a Promise and await.
    /// https://github.com/neon-bindings/examples/tree/2dbbef55f483635d0118c20c9902bf4c6faa1ecc/examples/tokio-fetch
    ///
    pub fn js_get_segment_slice(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let stream_reader = cx.this().downcast_or_throw::<JsBox<StreamReader>, _>(&mut cx)?;
        let channel = cx.channel();

        let (deferred, promise) = cx.promise();

        // Spawn an `async` task on the tokio runtime.
        let reader = Arc::clone(&stream_reader.reader);
        stream_reader.runtime_handle.spawn(async move {
            // expensive async procedure executed in the tokio thread
            let slice_result = reader.lock().await.acquire_segment().await;

            // notify and execute in the javascript main thread
            deferred.settle_with(&channel, move |mut cx| match slice_result {
                Ok(slice_) => {
                    let slice = RefCell::new(Slice { seg_slice: slice_ });
                    Ok(cx.boxed(slice))
                }
                Err(e) => cx.throw_error(e.to_string()),
            })
        });

        Ok(promise)
    }

    ///
    /// Set the reader offline.
    ///
    pub fn js_reader_offline(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let stream_reader = cx.this().downcast_or_throw::<JsBox<StreamReader>, _>(&mut cx)?;

        match stream_reader
            .runtime_handle
            .block_on(stream_reader.reader_offline_async())
        {
            Ok(_) => Ok(cx.undefined()),
            Err(e) => cx.throw_error(format!("Error while attempting to acquire segment {:?}", e)),
        }
    }

    ///
    /// Release the segment back.
    ///
    pub fn js_release_segment(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let stream_reader = cx.this().downcast_or_throw::<JsBox<StreamReader>, _>(&mut cx)?;
        let slice = cx.argument::<JsBox<RefCell<Slice>>>(0)?;

        if let Some(s) = slice.borrow_mut().seg_slice.take() {
            return match stream_reader
                .runtime_handle
                .block_on(stream_reader.release_segment_async(s))
            {
                Ok(_) => Ok(cx.undefined()),
                Err(e) => cx.throw_error(format!("Error while attempting to acquire segment {:?}", e)),
            };
        }
        Ok(cx.undefined())
    }

    ///
    /// Return the string representation.
    ///
    pub fn js_to_str(mut cx: FunctionContext) -> JsResult<JsString> {
        let stream_reader = cx.this().downcast_or_throw::<JsBox<StreamReader>, _>(&mut cx)?;

        Ok(cx.string(format!("Streams: {:?} ", stream_reader.streams)))
    }
}
