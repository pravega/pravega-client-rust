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
        use pravega_client_rust::error::EventStreamWriterError;
        use pravega_client_rust::event_stream_writer::EventStreamWriter;
        use pravega_rust_client_shared::ScopedStream;
        use pyo3::exceptions;
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use pyo3::PyObjectProtocol;
        use tokio::runtime::Handle;
    }
}

#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(new)] // this ensures the python object cannot be created without the using StreamManager.
pub(crate) struct StreamWriter {
    writer: EventStreamWriter,
    handle: Handle,
    stream: ScopedStream,
}

#[cfg(feature = "python_binding")]
#[pymethods]
impl StreamWriter {
    ///
    /// Write an event into the Pravega Stream. The events that are written will appear
    /// in the Stream exactly once. The event of type String is converted into bytes with `UTF-8` encoding.
    /// The user can optionally specify the routing key.
    ///
    /// Note that the implementation provides retry logic to handle connection failures and service host
    /// failures. Internal retries will not violate the exactly once semantic so it is better to rely on them
    /// than to wrap this with custom retry logic.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("127.0.0.1:9090")
    /// // lets assume the Pravega scope and stream are already created.
    /// writer=manager.create_writer("scope", "stream")
    /// // write into Pravega stream without specifying the routing key.
    /// writer.write_event("e1")
    /// // write into Pravega stream by specifying the routing key.
    /// writer.write_event("e2", "key1")
    /// ```
    ///
    #[text_signature = "($self, event, routing_key=None)"]
    #[args(event, routing_key = "None", "*")]
    pub fn write_event(&mut self, event: &str, routing_key: Option<&str>) -> PyResult<()> {
        match routing_key {
            Option::None => self.write_event_bytes(event.as_bytes(), Option::None),
            Option::Some(_key) => self.write_event_bytes(event.as_bytes(), routing_key),
        }
    }

    ///
    /// Write a byte array into the Pravega Stream. This is similar to `write_event(...)` api except
    /// that the the event to be written is a byte array. The user can optionally specify the
    //  routing key.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("127.0.0.1:9090")
    /// // lets assume the Pravega scope and stream are already created.
    /// writer=manager.create_writer("scope", "stream")
    ///
    /// e="eventData"
    /// // Convert the event object to a byte array.
    /// e_bytes=e.encode("utf-8")
    /// // write into Pravega stream without specifying the routing key.
    /// writer.write_event_bytes("e1")
    /// // write into Pravega stream by specifying the routing key.
    /// writer.write_event_bytes("e2", "key1")
    /// ```
    ///
    #[text_signature = "($self, event, routing_key=None)"]
    #[args(event, routing_key = "None", "*")]
    pub fn write_event_bytes(&mut self, event: &[u8], routing_key: Option<&str>) -> PyResult<()> {
        // to_vec creates an owned copy of the python byte array object.
        let result = match routing_key {
            Option::None => {
                println!("Writing a single event with no routing key");
                self.handle.block_on(self.writer.write_event(event.to_vec()))
            }
            Option::Some(key) => {
                println!("Writing a single event for a given routing key {:?}", key);
                self.handle
                    .block_on(self.writer.write_event_by_routing_key(key.into(), event.to_vec()))
            }
        };
        let result_oneshot: Result<(), EventStreamWriterError> =
            self.handle.block_on(result).expect("Event Write failed");

        match result_oneshot {
            Ok(_t) => Ok(()),
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }

    /// Returns the facet string representation.
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
impl PyObjectProtocol for StreamWriter {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("StreamWriter({})", self.to_str()))
    }
}
