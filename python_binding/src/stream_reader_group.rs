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
        use pravega_client_shared::ScopedStream;
        use pravega_client::event::reader_group::ReaderGroup;
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use pyo3::PyObjectProtocol;
        use tracing::{info, error};
        use std::sync::Arc;
        use tokio::sync::Mutex;
        use tokio::runtime::Handle;
        use crate::stream_reader::StreamReader;
        use pravega_client::event::reader_group::{ReaderGroupConfig, ReaderGroupConfigBuilder};
        use pravega_client::event::reader_group_state::ReaderGroupStateError;
        use pravega_client_shared::{Scope, Stream};
        use pyo3::types::PyTuple;
        use pyo3::exceptions;
    }
}

#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(Clone)]
pub(crate) struct StreamReaderGroupConfig {
    pub(crate) reader_group_config: ReaderGroupConfig,
}

///
/// Create a StreamManager by providing a controller uri.
/// ```
/// import pravega_client;
/// // Create a ReaderGroupConfig to read from the Tail/end of the Stream(s)
/// rg_config_read_from_tail = pravega_client.StreamReaderGroupConfig(True, "scope", "stream1", "stream2")
/// // Create a ReaderGroupConfig to read from the Head/start of the Stream(s)
/// rg_config_read_from_head = pravega_client.StreamReaderGroupConfig(False, "scope", "stream1", "stream2")
/// ```
///
#[cfg(feature = "python_binding")]
#[pymethods]
impl StreamReaderGroupConfig {
    #[new]
    #[args(stream_names = "*")]
    fn new(read_from_tail: bool, scope_name: &str, stream_names: &PyTuple) -> PyResult<Self> {
        let mut rg_config_builder = ReaderGroupConfigBuilder::default();
        if !stream_names.is_empty() {
            let streams: Vec<String> = stream_names
                .extract()
                .expect("Error while reading the stream names");
            for stream in streams {
                let scoped_stream = ScopedStream {
                    scope: Scope::from(scope_name.to_string()),
                    stream: Stream::from(stream),
                };
                if read_from_tail {
                    rg_config_builder.read_from_tail_of_stream(scoped_stream);
                } else {
                    rg_config_builder.read_from_head_of_stream(scoped_stream);
                }
            }
            let rg_config = rg_config_builder.build();
            info!("RGConfig {:?}", rg_config);
            Ok(StreamReaderGroupConfig {
                reader_group_config: rg_config,
            })
        } else {
            error!("No Streams provided while creating a ReaderGroup config");
            Err(pyo3::exceptions::PyValueError::new_err(
                "No Pravega Streams present in the ReaderGroupConfiguration",
            ))
        }
    }

    /// Returns the string representation.
    fn to_str(&self) -> String {
        format!("ReaderGroupConfig: {:?}", self.reader_group_config)
    }
}

///
/// Refer https://docs.python.org/3/reference/datamodel.html#basic-customization
/// This function will be called by the repr() built-in function to compute the “official” string
/// representation of an Python object.
///
#[cfg(feature = "python_binding")]
#[pyproto]
impl PyObjectProtocol for StreamReaderGroupConfig {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("StreamReaderGroupConfig({:?})", self.to_str()))
    }
}

///
/// This represents a Stream reader for a given Stream.
/// Note: A python object of StreamReader cannot be created directly without using the StreamManager.
///
#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(new)]
pub(crate) struct StreamReaderGroup {
    reader_group: ReaderGroup,
    runtime_handle: Handle,
}

#[cfg(feature = "python_binding")]
#[pymethods]
impl StreamReaderGroup {
    ///
    /// This method is used to create a reader under a ReaderGroup.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
    /// // lets assume the Pravega scope and stream are already created.
    /// event.reader_group=manager.create_reader_group("rg1", "scope", "stream")
    /// reader=event.reader_group.create_reader("reader_id");
    /// slice=await reader.get_segment_slice_async()
    /// for event in slice:
    ///     print(event.data())
    ///```
    ///
    pub fn create_reader(&self, reader_name: &str) -> PyResult<StreamReader> {
        info!(
            "Creating reader {:?} under reader group {:?}",
            reader_name, self.reader_group.name
        );
        let reader = self
            .runtime_handle
            .block_on(self.reader_group.create_reader(reader_name.to_string()));
        let stream_reader = StreamReader::new(
            Arc::new(Mutex::new(reader)),
            self.runtime_handle.clone(),
            self.reader_group.get_managed_streams(),
        );
        Ok(stream_reader)
    }

    ///
    /// This method is used to manually mark a reader as offline under a ReaderGroup.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
    /// // lets assume the Pravega scope and stream are already created.
    /// event.reader_group=manager.create_reader_group("rg1", "scope", "stream")
    /// event.reader_group.reader_offline("reader_id");
    ///```
    ///
    pub fn reader_offline(&self, reader_name: &str) -> PyResult<()> {
        info!(
            "Marking reader {:?} under reader group {:?} as offline",
            reader_name, self.reader_group.name
        );
        let res = self
            .runtime_handle
            .block_on(self.reader_group.reader_offline(reader_name.to_string(), None));
        match res {
            Ok(_) => Ok(()),
            Err(e) => match e {
                ReaderGroupStateError::SyncError { .. } => {
                    error!("Failed to mark the reader {:?} offline {:?} ", reader_name, e);
                    Err(exceptions::PyValueError::new_err(format!(
                        " Failed to mark reader offline {:?}",
                        e
                    )))
                }
                ReaderGroupStateError::ReaderAlreadyOfflineError { .. } => {
                    info!("Reader {:?} is already offline", reader_name);
                    Ok(())
                }
            },
        }
    }

    /// Returns the string representation.
    fn to_str(&self) -> String {
        format!(
            "ReaderGroup: {:?}, ReaderGroup config : {:?}",
            self.reader_group.name, self.reader_group.config
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
impl PyObjectProtocol for StreamReaderGroup {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("StreamReaderGroup({})", self.to_str()))
    }
}
