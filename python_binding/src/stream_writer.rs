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
        use pravega_client::error::Error as WriterError;
        use pravega_client::event::writer::EventWriter;
        use pravega_client_shared::ScopedStream;
        use pyo3::exceptions;
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use pyo3::PyObjectProtocol;
        use tracing::trace;
        use tracing::info;
        use std::time::Duration;
        use tokio::runtime::Handle;
        use tokio::time::timeout;
        use tokio::sync::oneshot::error::RecvError;
        use pravega_client::util::oneshot_holder::OneShotHolder;
    }
}

///
/// This represents a Stream writer for a given Stream.
/// Note: A python object of StreamWriter cannot be created directly without using the StreamManager.
///
#[cfg(feature = "python_binding")]
#[pyclass]
pub(crate) struct StreamWriter {
    writer: EventWriter,
    runtime_handle: Handle,
    stream: ScopedStream,
    inflight: OneShotHolder<WriterError>,
}

// The amount of time the python api will wait for the underlying write to be completed.
const TIMEOUT_IN_SECONDS: u64 = 120;

impl StreamWriter {
    pub fn new(
        writer: EventWriter,
        runtime_handle: Handle,
        stream: ScopedStream,
        max_inflight_count: usize,
    ) -> Self {
        StreamWriter {
            writer,
            runtime_handle,
            stream,
            inflight: OneShotHolder::new(max_inflight_count),
        }
    }
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
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
    /// // lets assume the Pravega scope and stream are already created.
    /// writer=manager.create_writer("scope", "stream")
    /// // write into Pravega stream without specifying the routing key.
    /// writer.write_event("e1")
    /// // write into Pravega stream by specifying the routing key.
    /// writer.write_event("e2", "key1")
    /// ```
    ///
    #[pyo3(text_signature = "($self, event, routing_key=None)")]
    #[args(event, routing_key = "None", "*")]
    pub fn write_event(&mut self, event: &str, routing_key: Option<String>) -> PyResult<()> {
        self.write_event_bytes(event.as_bytes(), routing_key)
    }

    ///
    /// Write a byte array into the Pravega Stream. This is similar to `write_event(...)` api except
    /// that the the event to be written is a byte array. The user can optionally specify the
    ///  routing key.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
    /// // lets assume the Pravega scope and stream are already created.
    /// writer=manager.create_writer("scope", "stream")
    ///
    /// e="eventData"
    /// // Convert the event object to a byte array.
    /// e_bytes=e.encode("utf-8")
    /// // write into Pravega stream without specifying the routing key.
    /// writer.write_event_bytes(e_bytes)
    /// // write into Pravega stream by specifying the routing key.
    /// writer.write_event_bytes(e_bytes, "key1")
    /// ```
    ///
    #[pyo3(text_signature = "($self, event, routing_key=None)")]
    #[args(event, routing_key = "None", "*")]
    pub fn write_event_bytes(&mut self, event: &[u8], routing_key: Option<String>) -> PyResult<()> {
        // to_vec creates an owned copy of the python byte array object.
        let write_future: tokio::sync::oneshot::Receiver<Result<(), WriterError>> = match routing_key {
            Option::None => {
                trace!("Writing a single event with no routing key");
                self.runtime_handle
                    .block_on(self.writer.write_event(event.to_vec()))
            }
            Option::Some(key) => {
                trace!("Writing a single event for a given routing key {:?}", key);
                self.runtime_handle
                    .block_on(self.writer.write_event_by_routing_key(key, event.to_vec()))
            }
        };
        let _guard = self.runtime_handle.enter();
        let timeout_fut = timeout(
            Duration::from_secs(TIMEOUT_IN_SECONDS),
            self.inflight.add(write_future),
        );

        let result: Result<Result<Result<(), WriterError>, RecvError>, _> =
            self.runtime_handle.block_on(timeout_fut);
        match result {
            Ok(t) => match t {
                Ok(t1) => match t1 {
                    Ok(_) => Ok(()),
                    Err(e) => Err(exceptions::PyValueError::new_err(format!(
                        "Error observed while writing an event {:?}",
                        e
                    ))),
                },
                Err(e) => Err(exceptions::PyValueError::new_err(format!(
                    "Error observed while writing an event {:?}",
                    e
                ))),
            },
            Err(_) => Err(exceptions::PyValueError::new_err(
                "Write timed out, please check connectivity with Pravega.",
            )),
        }
    }

    ///
    /// Flush all the inflight events into Pravega Stream.
    /// This will ensure all the inflight events are completely persisted on the Pravega Stream.
    ///
    #[pyo3(text_signature = "($self)")]
    #[args("*")]
    pub fn flush(&mut self) -> PyResult<()> {
        info!("Invoking flush() on writer {:?}", self.to_str());
        let mut flush_result: PyResult<()> = Ok(());
        for x in self.inflight.drain() {
            let res = self.runtime_handle.block_on(x);
            // fail fast on error.
            if let Err(e) = res {
                info!(
                    "RecvError observed while flushing events on stream {:?}",
                    self.stream
                );
                flush_result = Err(exceptions::PyValueError::new_err(format!(
                    "RecvError observed while writing an event {:?}",
                    e
                )));
                break;
            } else if let Err(e) = res.unwrap() {
                info!("Error observed while flushing events on {:?}", self.stream);
                flush_result = Err(exceptions::PyValueError::new_err(format!(
                    "Error observed while writing an event {:?}",
                    e
                )));
                break;
            }
            flush_result = Ok(());
        }
        flush_result
    }

    /// Returns the string representation.
    fn to_str(&self) -> String {
        format!("Stream: {:?} ", self.stream)
    }
}

impl Drop for StreamWriter {
    fn drop(&mut self) {
        let _ = self.flush();
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
