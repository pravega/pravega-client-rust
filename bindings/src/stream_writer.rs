//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_client_rust::error::EventStreamWriterError;
use pravega_client_rust::event_stream_writer::EventStreamWriter;
use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::PyResult;
use tokio::runtime::Handle;

#[pyclass]
#[derive(new)] // this ensures the python object cannot be created without the using StreamManager.
pub(crate) struct StreamWriter {
    writer: EventStreamWriter,
    handle: Handle,
}

#[pymethods]
impl StreamWriter {
    ///
    /// Write an event as a String into to the Pravega Stream. The operation blocks until the write operations is completed.
    ///
    pub fn write_event(&mut self, event: String) -> PyResult<()> {
        self.write_event_bytes(event.into_bytes()) //
    }

    ///
    /// Write an event into the Pravega Stream for the given routing key.
    ///
    pub fn write_event_by_routing_key(&mut self, event: String, routing_key: String) -> PyResult<()> {
        self.write_event_by_routing_key_bytes(event.into_bytes(), routing_key)
    }

    ///
    /// Write an event to Pravega Stream. The operation blocks until the write operations is completed.
    /// Python can also be used to convert a given object into bytes.
    ///
    /// E.g:
    /// >>> e="test"
    /// >>> b=e.encode("utf-8") // Python api to convert an object to byte array.
    /// >>> w1.write_event_bytes(b)
    ///
    pub fn write_event_bytes(&mut self, event: Vec<u8>) -> PyResult<()> {
        println!("Writing a single event");
        let result = self.handle.block_on(self.writer.write_event(event));
        let result_oneshot: Result<(), EventStreamWriterError> =
            self.handle.block_on(result).expect("Write failed");

        match result_oneshot {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }

    ///
    /// Write an event to the Pravega Stream given a routing key.
    ///
    pub fn write_event_by_routing_key_bytes(&mut self, event: Vec<u8>, routing_key: String) -> PyResult<()> {
        println!("Writing a single event for a given routing key");
        let result = self
            .handle
            .block_on(self.writer.write_event_by_routing_key(routing_key, event));
        let result_oneshot: Result<(), EventStreamWriterError> = self
            .handle
            .block_on(result)
            .expect("Write for specified routing key failed");

        match result_oneshot {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }
}
