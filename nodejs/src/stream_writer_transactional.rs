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
use pravega_client::event::transactional_writer::TransactionalEventWriter;
use pravega_client_shared::ScopedStream;
use pravega_client_shared::TxId;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Mutex;

use crate::transaction::StreamTransaction;

impl Finalize for StreamTxnWriter {}

///
/// This represents a Stream writer for a given Stream.
/// Note: A StreamTxnWriter cannot be created directly without using the StreamManager.
///
pub(crate) struct StreamTxnWriter {
    writer: Arc<Mutex<TransactionalEventWriter>>,
    runtime_handle: Handle,
    stream: ScopedStream,
}

impl StreamTxnWriter {
    pub fn new(writer: TransactionalEventWriter, runtime_handle: Handle, stream: ScopedStream) -> Self {
        StreamTxnWriter {
            writer: Arc::new(Mutex::new(writer)),
            runtime_handle,
            stream,
        }
    }

    ///
    /// Create a new transaction.
    /// This returns a StreamTransaction which can be perform writes on the created transaction. It
    /// can also be used to perform commit() and abort() operations on the created transaction.
    ///
    pub fn js_begin_txn(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let stream_txn_writer = cx
            .this()
            .downcast_or_throw::<JsBox<StreamTxnWriter>, _>(&mut cx)?;

        let channel = cx.channel();
        let (deferred, promise) = cx.promise();

        // spawn an `async` task on the tokio runtime.
        let writer = Arc::clone(&stream_txn_writer.writer);
        let runtime_handle = stream_txn_writer.runtime_handle.clone();
        stream_txn_writer.runtime_handle.spawn(async move {
            // run an async function in the tokio thread
            let result = writer.lock().await.begin().await;

            // notify and execute in the javascript main thread
            deferred.settle_with(&channel, move |mut cx| match result {
                Ok(txn) => Ok(cx.boxed(StreamTransaction::new(txn, runtime_handle))),
                Err(e) => cx.throw_error(e.to_string()),
            })
        });

        Ok(promise)
    }

    ///
    /// Get a StreamTransaction for a given transaction id.
    ///
    pub fn js_get_txn(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let stream_txn_writer = cx
            .this()
            .downcast_or_throw::<JsBox<StreamTxnWriter>, _>(&mut cx)?;
        // convert a string back to u128, js should verify the txn_id
        // to be string containing only number
        let txn_id = cx
            .argument::<JsString>(0)?
            .value(&mut cx)
            .parse::<u128>()
            .unwrap();

        let channel = cx.channel();
        let (deferred, promise) = cx.promise();

        // spawn an `async` task on the tokio runtime.
        let writer = Arc::clone(&stream_txn_writer.writer);
        let runtime_handle = stream_txn_writer.runtime_handle.clone();
        stream_txn_writer.runtime_handle.spawn(async move {
            // run an async function in the tokio thread
            let result = writer.lock().await.get_txn(TxId(txn_id)).await;

            // notify and execute in the javascript main thread
            deferred.settle_with(&channel, move |mut cx| match result {
                Ok(txn) => Ok(cx.boxed(StreamTransaction::new(txn, runtime_handle))),
                Err(e) => cx.throw_error(e.to_string()),
            })
        });

        Ok(promise)
    }

    ///
    /// Return the string representation.
    ///
    pub fn js_to_str(mut cx: FunctionContext) -> JsResult<JsString> {
        let stream_txn_writer = cx
            .this()
            .downcast_or_throw::<JsBox<StreamTxnWriter>, _>(&mut cx)?;

        Ok(cx.string(format!("Stream: {:?} ", stream_txn_writer.stream)))
    }
}
