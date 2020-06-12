//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::transaction::StreamTransaction;
use pravega_rust_client_shared::TxId;
cfg_if! {
    if #[cfg(feature = "python_binding")] {
        use pravega_client_rust::transaction::transactional_event_stream_writer::TransactionalEventStreamWriter;
        use pyo3::exceptions;
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use tokio::runtime::Handle;
    }
}

#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(new)] // this ensures the python object cannot be created without the using StreamManager.
pub(crate) struct StreamTxnWriter {
    writer: TransactionalEventStreamWriter,
    handle: Handle,
}

#[cfg(feature = "python_binding")]
#[pymethods]
impl StreamTxnWriter {
    ///
    /// Create a new transaction.
    /// This returns a StreamTransaction which can be perform writes on the created transaction. It
    /// can also be used to perform commit() and abort() operations.
    /// TODO: expand
    ///
    #[cfg(feature = "python_binding")]
    #[text_signature = "($self)"]
    pub fn begin_txn(&mut self) -> PyResult<StreamTransaction> {
        let result = self.handle.block_on(self.writer.begin());
        match result {
            Ok(txn) => Ok(StreamTransaction::new(txn, self.handle.clone())),
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }

    ///
    /// Get a StreamTransaction for a given transaction id.
    ///
    #[cfg(feature = "python_binding")]
    #[text_signature = "($self, routing_key, event)"]
    pub fn get_txn(&mut self, txn_id: u128) -> PyResult<StreamTransaction> {
        println!("Writing a single event for a given routing key");
        let result = self.handle.block_on(self.writer.get_txn(TxId(txn_id)));

        match result {
            Ok(txn) => Ok(StreamTransaction::new(txn, self.handle.clone())),
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }
}
