//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

#![allow(clippy::borrow_deref_ref)] // https://github.com/rust-lang/rust-clippy/issues/8971
#[macro_use]
extern crate cfg_if;

use crate::stream_reader_group::StreamReaderGroupConfig;

mod byte_stream;
mod stream_manager;
mod stream_reader;
mod stream_reader_group;
mod stream_writer;
mod stream_writer_transactional;
mod transaction;

cfg_if! {
    if #[cfg(feature = "python_binding")] {
        use pyo3::prelude::*;
        use stream_manager::StreamManager;
        use stream_manager::{StreamScalingPolicy, StreamRetentionPolicy};
        #[macro_use]
        extern crate derive_new;
        use byte_stream::ByteStream;
        use stream_writer::StreamWriter;
        use stream_reader::StreamReader;
        use stream_reader_group::StreamReaderGroup;
        use crate::stream_writer_transactional::StreamTxnWriter;
        use crate::transaction::StreamTransaction;
        use pyo3::create_exception;
        use pyo3::exceptions::PyException;

        const TXNFAILED_EXCEPTION_DOCSTRING: &str = "This exception indicates a transaction has failed.\
        Usually because the transaction timed out or someone called transaction.abort()";
        create_exception!(pravega_client, TxnFailedException, PyException);
    }
}

#[cfg(feature = "python_binding")]
#[pymodule]
/// A Python module for Pravega implemented in Rust.
fn pravega_client(py: Python, m: &PyModule) -> PyResult<()> {
    let _ = tracing_subscriber::fmt::try_init();
    m.add_class::<StreamManager>()?;
    m.add_class::<StreamWriter>()?;
    m.add_class::<StreamTxnWriter>()?;
    m.add_class::<StreamTransaction>()?;
    m.add_class::<StreamReader>()?;
    m.add_class::<StreamReaderGroupConfig>()?;
    m.add_class::<StreamReaderGroup>()?;
    m.add_class::<StreamScalingPolicy>()?;
    m.add_class::<StreamRetentionPolicy>()?;
    m.add_class::<ByteStream>()?;
    let txn_exception = py.get_type::<TxnFailedException>();
    txn_exception.setattr("__doc__", TXNFAILED_EXCEPTION_DOCSTRING)?;
    m.add("TxnFailedException", txn_exception)?;
    Ok(())
}
