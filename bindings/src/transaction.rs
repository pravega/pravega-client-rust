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
        use pravega_client_rust::transaction::Transaction;
        use pyo3::exceptions;
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use tokio::runtime::Handle;
        use crate::TxnFailedException;
        use pravega_client_rust::error::TransactionError;
        use pravega_rust_client_shared::{Timestamp, TransactionStatus, TxId};
        use pyo3::PyObjectProtocol;
        use log::{debug, info, warn};
    }
}

#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(new)]
///
/// This represents a transaction on a given Stream.
/// Note: A python object of StreamTransaction cannot be created directly without using the StreamTxnWriter.
///
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
    }

    ///
    /// Check if the transaction is an OPEN state.
    ///
    #[text_signature = "($self)"]
    pub fn is_open(&self) -> PyResult<bool> {
        let result: Result<TransactionStatus, TransactionError> =
            self.handle.block_on(self.txn.check_status());

        match result {
            Ok(TransactionStatus::Open) => Ok(true),
            Ok(_t) => Ok(false),
            Err(t) => Err(exceptions::ValueError::py_err(format!("{:?}", t))),
        }
    }

    ///
    /// Write an event of type String into to the Transaction. The operation blocks until the write operations is completed.
    ///
    #[text_signature = "($self, event)"]
    pub fn write_event(&mut self, event: &str) -> PyResult<()> {
        self.write_event_bytes(event.as_bytes()) //
    }

    ///
    /// Write an event in bytes to a Pravega Transaction that is created by StreamTxnWriter#begin_txn.
    /// The operation blocks until the write operations is completed.
    ///
    /// Note: Python can also be used to convert a given object into bytes.
    ///
    /// E.g:
    /// >>> e="test"
    /// >>> b=e.encode("utf-8") // Python api to convert an object to byte array.
    /// >>> w1.write_event_bytes(b)
    ///
    #[text_signature = "($self, event_as_byte_array)"]
    pub fn write_event_bytes(&mut self, event: &[u8]) -> PyResult<()> {
        debug!("Writing a single event to a transaction");
        // to_vec creates an owned copy of the python byte array object.
        let result: Result<(), TransactionError> = self
            .handle
            .block_on(self.txn.write_event(Option::None, event.to_vec()));

        match result {
            Ok(_t) => Ok(()),
            Err(TransactionError::TxnClosed { id }) => {
                warn!("Transaction is already closed");
                Err(TxnFailedException::py_err(id.0))
            }
            Err(e) => Err(exceptions::ValueError::py_err(format!("Error {:?}", e))),
        }
    }

    ///
    /// Commit the Transaction.
    /// This Causes all messages previously written to the transaction to go into the stream contiguously.
    //  This operation will either fully succeed making all events consumable or fully fail such that none of them are.
    //  There may be some time delay before readers see the events after this call has returned.
    ///
    #[text_signature = "($self)"]
    pub fn commit(&mut self) -> PyResult<()> {
        self.commit_timestamp(i64::MIN as u64)
    }

    ///
    /// Commit the Transaction and the associated timestamp.
    /// This Causes all messages previously written to the transaction to go into the stream contiguously.
    //  This operation will either fully succeed making all events consumable or fully fail such that none of them are.
    //  There may be some time delay before readers see the events after this call has returned.
    ///
    #[text_signature = "($self, timestamp_as_u64)"]
    pub fn commit_timestamp(&mut self, timestamp: u64) -> PyResult<()> {
        debug!("Committing the transaction");
        let result: Result<(), TransactionError> =
            self.handle.block_on(self.txn.commit(Timestamp::new(timestamp)));

        match result {
            Ok(_t) => Ok(()),
            Err(TransactionError::TxnClosed { id }) => {
                warn!("Transaction is already closed");
                Err(TxnFailedException::py_err(id.0))
            }
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }

    ///
    /// Abort the Transaction.
    /// Drops the transaction, causing all events written to it to be deleted.
    ///
    #[text_signature = "($self)"]
    pub fn abort(&mut self) -> PyResult<()> {
        info!("Aborting the transaction {}", self.txn.get_txn_id());
        let result: Result<(), TransactionError> = self.handle.block_on(self.txn.abort());

        match result {
            Ok(_t) => Ok(()),
            Err(TransactionError::TxnClosed { id }) => {
                warn!("Transaction is already closed");
                Err(TxnFailedException::py_err(id.0))
            }
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }

    /// Returns the facet string representation.
    fn to_str(&self) -> String {
        format!(
            "Txn id: {:?} , {:?}",
            self.txn.get_txn_id(),
            self.txn.get_stream()
        )
    }
}

///
/// Refer https://docs.python.org/3/reference/datamodel.html#basic-customization
/// This function will be called by the repr() built-in function to compute the “official” string
/// representation of an Python object.
///
#[cfg(feature = "python_binding")]
#[pyproto]
impl PyObjectProtocol for StreamTransaction {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("StreamTransaction({})", self.to_str()))
    }
}
