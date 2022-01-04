//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

cfg_if! {
    if #[cfg(feature = "python_binding")] {
        use pravega_client::event::transactional_writer::TransactionalEventWriter;
        use pyo3::exceptions;
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use pyo3::PyObjectProtocol;
        use crate::transaction::StreamTransaction;
        use pravega_client_shared::TxId;
        use pravega_client_shared::ScopedStream;
        use tracing::debug;
        use tokio::runtime::Handle;
    }
}

///
/// This represents a Transaction writer for a given Stream.
/// Note: A python object of StreamTxnWriter cannot be created directly without using the StreamManager.
///
#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(new)]
pub(crate) struct StreamTxnWriter {
    writer: TransactionalEventWriter,
    runtime_handle: Handle,
    stream: ScopedStream,
}

#[cfg(feature = "python_binding")]
#[pymethods]
impl StreamTxnWriter {
    ///
    /// Create a new transaction.
    /// This returns a StreamTransaction which can be perform writes on the created transaction. It
    /// can also be used to perform commit() and abort() operations on the created transaction.
    ///
    #[pyo3(text_signature = "($self)")]
    pub fn begin_txn(&mut self) -> PyResult<StreamTransaction> {
        let result = self.runtime_handle.block_on(self.writer.begin());
        match result {
            Ok(txn) => Ok(StreamTransaction::new(txn, self.runtime_handle.clone())),
            Err(e) => Err(exceptions::PyValueError::new_err(format!("{:?}", e))),
        }
    }

    ///
    /// Get a StreamTransaction for a given transaction id.
    ///
    #[pyo3(text_signature = "($self, txn_id)")]
    pub fn get_txn(&mut self, txn_id: u128) -> PyResult<StreamTransaction> {
        debug!("Writing a single event for a given routing key");
        let result = self.runtime_handle.block_on(self.writer.get_txn(TxId(txn_id)));

        match result {
            Ok(txn) => Ok(StreamTransaction::new(txn, self.runtime_handle.clone())),
            Err(e) => Err(exceptions::PyValueError::new_err(format!("{:?}", e))),
        }
    }

    /// Returns the string representation.
    fn to_str(&self) -> String {
        format!("Stream: {:?} ", self.stream)
    }
}

///
/// Refer https://docs.python.org/3/reference/datamodel.html#basic-customization
/// This function will be called by the repr() built-in function to compute the “official” string
/// representation of an Python object.
///
#[cfg(feature = "python_binding")]
#[pyproto]
impl PyObjectProtocol for StreamTxnWriter {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("StreamTxnWriter({})", self.to_str()))
    }
}
