//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_client_rust::error::TransactionError;
use pravega_rust_client_shared::{TransactionStatus, TxId};
cfg_if! {
    if #[cfg(feature = "python_binding")] {
        use pravega_client_rust::transaction::Transaction;
        use pyo3::exceptions;
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use tokio::runtime::Handle;
        use crate::TxnFailedException;
    }
}

#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(new)] // this ensures the python object cannot be created without the using StreamTxnWriter.
pub(crate) struct StreamTransaction {
    txn: Transaction,
    handle: Handle,
}

#[cfg(feature = "python_binding")]
#[pymethods]
impl StreamTransaction {
    ///
    /// Get the transaction id.
    ///
    #[cfg(feature = "python_binding")]
    #[text_signature = "($self)"]
    pub fn get_txn_id(&mut self) -> PyResult<u128> {
        let result: TxId = self.txn.get_txn_id();
        Ok(result.0)
        //TODO: make it a result.
    }

    ///
    /// Check if the transaction is an OPEN state.
    ///
    #[cfg(feature = "python_binding")]
    #[text_signature = "($self)"]
    pub fn is_open(&self) -> PyResult<bool> {
        let result: Result<TransactionStatus, TransactionError> =
            self.handle.block_on(self.txn.check_status());

        match result {
            Ok(TransactionStatus::Open) => Ok(true),
            Ok(_t) => Ok(false),
            Err(TransactionError::TxnClosed { id }) => Err(TxnFailedException::py_err(id.0)),
            Err(t) => Err(exceptions::ValueError::py_err(format!("{:?}", t))),
        }
    }

    ///
    /// Write an event to Pravega Transaction Stream. The operation blocks until the write operations is completed.
    /// Python can also be used to convert a given object into bytes.
    ///
    /// E.g:
    /// >>> e="test"
    /// >>> b=e.encode("utf-8") // Python api to convert an object to byte array.
    /// >>> w1.write_event_bytes(b)
    ///
    #[cfg(feature = "python_binding")]
    #[text_signature = "($self, event)"]
    pub fn write_event_bytes(&mut self, event: &[u8]) -> PyResult<()> {
        println!("Writing a single event to a transaction");
        // to_vec creates an owned copy of the python byte array object.
        let result: Result<(), TransactionError> = self
            .handle
            .block_on(self.txn.write_event(Option::None, event.to_vec()));

        match result {
            Ok(_t) => Ok(()),
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }
}
