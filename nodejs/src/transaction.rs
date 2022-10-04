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
use pravega_client::event::transactional_writer::{Transaction, TransactionError};
use pravega_client_shared::ScopedStream;
use pravega_client_shared::{Timestamp, TransactionStatus};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Mutex;

pub(crate) struct StreamTransaction {
    txn: Arc<Mutex<Transaction>>,
    runtime_handle: Handle,
}

impl Finalize for StreamTransaction {}

impl StreamTransaction {
    // Helper method to get txn id from Arc<Mutex<Transaction>>
    async fn get_txn_id(&self) -> String {
        self.txn.lock().await.txn_id().0.to_string()
    }

    // Helper method to get txn id from Arc<Mutex<Transaction>>
    async fn get_stream(&self) -> ScopedStream {
        self.txn.lock().await.stream()
    }
}

impl StreamTransaction {
    pub fn new(txn: Transaction, runtime_handle: Handle) -> Self {
        StreamTransaction {
            txn: Arc::new(Mutex::new(txn)),
            runtime_handle,
        }
    }

    ///
    /// Get the transaction id.
    ///
    pub fn js_get_txn_id(mut cx: FunctionContext) -> JsResult<JsString> {
        let transaction = cx
            .this()
            .downcast_or_throw::<JsBox<StreamTransaction>, _>(&mut cx)?;

        // we need to get obj from mutex before use, this is an async operation.
        Ok(cx.string(transaction.runtime_handle.block_on(transaction.get_txn_id())))
    }

    ///
    /// Check if the transaction is an OPEN state.
    ///
    pub fn js_is_open(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let transaction = cx
            .this()
            .downcast_or_throw::<JsBox<StreamTransaction>, _>(&mut cx)?;

        let channel = cx.channel();
        let (deferred, promise) = cx.promise();

        // spawn an `async` task on the tokio runtime.
        let txn = Arc::clone(&transaction.txn);
        transaction.runtime_handle.spawn(async move {
            let result: Result<TransactionStatus, TransactionError> = txn.lock().await.check_status().await;

            deferred.settle_with(&channel, move |mut cx| match result {
                Ok(TransactionStatus::Open) => Ok(cx.boolean(true)),
                Ok(_e) => Ok(cx.boolean(false)),
                Err(e) => cx.throw_error(e.to_string()),
            })
        });

        Ok(promise)
    }

    ///
    /// Write an event in bytes to a Pravega Transaction that is created by StreamTxnWriter#begin_txn.
    /// The operation blocks until the write operations is completed.
    ///
    pub fn js_write_event_bytes(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let transaction = cx
            .this()
            .downcast_or_throw::<JsBox<StreamTransaction>, _>(&mut cx)?;
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
        let txn = Arc::clone(&transaction.txn);
        transaction.runtime_handle.spawn(async move {
            // run an async function in the tokio thread
            let result = txn.lock().await.write_event(routing_key, event.to_vec()).await;

            // notify and execute in the javascript main thread
            deferred.settle_with(&channel, move |mut cx| match result {
                Ok(_) => Ok(cx.undefined()),
                Err(e) => cx.throw_error(e.to_string()),
            })
        });

        Ok(promise)
    }

    ///
    /// Commit the Transaction and the associated timestamp.
    /// This Causes all messages previously written to the transaction to go into the stream contiguously.
    //  This operation will either fully succeed making all events consumable or fully fail such that none of them are.
    //  There may be some time delay before readers see the events after this call has returned.
    ///
    pub fn js_commit_timestamp(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let transaction = cx
            .this()
            .downcast_or_throw::<JsBox<StreamTransaction>, _>(&mut cx)?;
        let timestamp = cx.argument::<JsNumber>(0)?.value(&mut cx) as u64;

        let channel = cx.channel();
        let (deferred, promise) = cx.promise();

        // spawn an `async` task on the tokio runtime.
        let txn = Arc::clone(&transaction.txn);
        transaction.runtime_handle.spawn(async move {
            // run an async function in the tokio thread
            let result = txn.lock().await.commit(Timestamp::from(timestamp)).await;

            // notify and execute in the javascript main thread
            deferred.settle_with(&channel, move |mut cx| match result {
                Ok(_) => Ok(cx.undefined()),
                Err(TransactionError::TxnClosed { id }) => {
                    cx.throw_error(format!("Transaction {:?} already closed", id))
                }
                Err(e) => cx.throw_error(format!("Commit of transaction failed with {:?}", e)),
            })
        });

        Ok(promise)
    }

    ///
    /// Abort the Transaction.
    /// Drops the transaction, causing all events written to it to be deleted.
    ///
    pub fn js_abort(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let transaction = cx
            .this()
            .downcast_or_throw::<JsBox<StreamTransaction>, _>(&mut cx)?;

        let channel = cx.channel();
        let (deferred, promise) = cx.promise();

        // spawn an `async` task on the tokio runtime.
        let txn = Arc::clone(&transaction.txn);
        transaction.runtime_handle.spawn(async move {
            // run an async function in the tokio thread
            let result = txn.lock().await.abort().await;

            // notify and execute in the javascript main thread
            deferred.settle_with(&channel, move |mut cx| match result {
                Ok(_) => Ok(cx.undefined()),
                Err(TransactionError::TxnClosed { id }) => {
                    cx.throw_error(format!("Transaction {:?} already closed", id))
                }
                Err(e) => cx.throw_error(format!("Abort of transaction failed with {:?}", e)),
            })
        });

        Ok(promise)
    }

    ///
    /// Return the string representation.
    ///
    pub fn js_to_str(mut cx: FunctionContext) -> JsResult<JsString> {
        let transaction = cx
            .this()
            .downcast_or_throw::<JsBox<StreamTransaction>, _>(&mut cx)?;

        Ok(cx.string(format!(
            "Txn id: {:?} , {:?}",
            transaction.runtime_handle.block_on(transaction.get_txn_id()),
            transaction.runtime_handle.block_on(transaction.get_stream())
        )))
    }
}
