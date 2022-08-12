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
use neon::types::buffer::TypedArray;
use pravega_client::event::writer::EventWriter;
use pravega_client_shared::ScopedStream;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Mutex;

impl Finalize for StreamWriter {
    fn finalize<'a, C: Context<'a>>(self, _cx: &mut C) {
        self.runtime_handle.block_on(async {
            let _ = self.writer.lock().await.flush().await;
        });
    }
}

///
/// This represents a Stream writer for a given Stream.
/// Note: A StreamWriter cannot be created directly without using the StreamManager.
///
pub(crate) struct StreamWriter {
    writer: Arc<Mutex<EventWriter>>,
    runtime_handle: Handle,
    stream: ScopedStream,
}

impl StreamWriter {
    pub fn new(writer: EventWriter, runtime_handle: Handle, stream: ScopedStream) -> Self {
        StreamWriter {
            writer: Arc::new(Mutex::new(writer)),
            runtime_handle,
            stream,
        }
    }

    ///
    /// Return the write result in an asynchronous call by
    /// sending the payload to a background task called reactor to process.
    ///
    /// See the tokio-fetch example for more details on how to return a Promise and await.
    /// https://github.com/neon-bindings/examples/tree/2dbbef55f483635d0118c20c9902bf4c6faa1ecc/examples/tokio-fetch
    ///
    pub fn js_write_event_bytes(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let stream_writer = cx.this().downcast_or_throw::<JsBox<StreamWriter>, _>(&mut cx)?;
        let buf = cx.argument::<JsTypedArray<u8>>(0)?;
        let event = buf.as_slice(&cx).to_owned();
        let routing_key = match cx.argument_opt(1) {
            Some(v) => match v.downcast::<JsUndefined, FunctionContext>(&mut cx) {
                Ok(_) => None,
                Err(_) => match v.downcast::<JsString, FunctionContext>(&mut cx) {
                    Ok(s) => Some(s.value(&mut cx)),
                    Err(_) => return cx.throw_error("Routing key is not either undefined or string!"),
                },
            },
            None => return cx.throw_error("Not enough arguments!"),
        };

        let channel = cx.channel();
        let (deferred, promise) = cx.promise();

        // spawn an `async` task on the tokio runtime.
        let writer = Arc::clone(&stream_writer.writer);
        stream_writer.runtime_handle.spawn(async move {
            // run an async function in the tokio thread
            let result = match routing_key {
                Some(routing_key) => {
                    writer
                        .lock()
                        .await
                        .write_event_by_routing_key(routing_key, event.to_vec())
                        .await
                }
                None => writer.lock().await.write_event(event.to_vec()).await,
            }
            .await;

            // notify and execute in the javascript main thread
            deferred.settle_with(&channel, move |mut cx| match result {
                Ok(result) => match result {
                    Ok(_) => Ok(cx.undefined()),
                    Err(e) => cx.throw_error(e.to_string()),
                },
                Err(e) => cx.throw_error(e.to_string()),
            })
        });

        Ok(promise)
    }

    ///
    /// Flush data.
    /// It will notify the js thread when all the pending appends are acknowledged by Pravega.
    ///
    /// See the tokio-fetch example for more details on how to return a Promise and await.
    /// https://github.com/neon-bindings/examples/tree/2dbbef55f483635d0118c20c9902bf4c6faa1ecc/examples/tokio-fetch
    ///
    pub fn js_flush(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let stream_writer = cx.this().downcast_or_throw::<JsBox<StreamWriter>, _>(&mut cx)?;

        let channel = cx.channel();
        let (deferred, promise) = cx.promise();

        // spawn an `async` task on the tokio runtime.
        let writer = Arc::clone(&stream_writer.writer);
        stream_writer.runtime_handle.spawn(async move {
            // run an async function in the tokio thread
            let result = writer.lock().await.flush().await;

            // notify and execute in the javascript main thread
            deferred.settle_with(&channel, move |mut cx| match result {
                Ok(_) => Ok(cx.undefined()),
                Err(e) => cx.throw_error(e.to_string()),
            })
        });

        Ok(promise)
    }

    ///
    /// Return the string representation.
    ///
    pub fn js_to_str(mut cx: FunctionContext) -> JsResult<JsString> {
        let stream_writer = cx.this().downcast_or_throw::<JsBox<StreamWriter>, _>(&mut cx)?;

        Ok(cx.string(format!("Stream: {:?} ", stream_writer.stream)))
    }
}
