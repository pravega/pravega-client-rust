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
        use crate::stream_writer_transactional::StreamTxnWriter;
        use crate::stream_writer::StreamWriter;
        use pravega_client_rust::client_factory::ClientFactory;
        use pravega_rust_client_shared::*;
        use pravega_rust_client_config::{ClientConfig, ClientConfigBuilder};
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use pyo3::{exceptions, PyObjectProtocol};
        use log::info;
    }
}

///
/// Create a StreamManager by providing a controller uri.
/// ```
/// import pravega_client;
/// manager=pravega_client.StreamManager("127.0.0.1:9090")
/// // this manager can be used to create scopes, streams, writers and readers against Pravega.
/// manager.create_scope("scope")
/// ```
///
#[cfg(feature = "python_binding")]
#[pyclass]
#[text_signature = "(controller_uri)"]
pub(crate) struct StreamManager {
    controller_ip: String,
    cf: ClientFactory,
    config: ClientConfig,
}

///
/// Create a StreamManager by providing a controller uri.
/// ```
/// import pravega_client;
/// manager=pravega_client.StreamManager("127.0.0.1:9090")
/// // this manager can be used to create scopes, streams, writers and readers against Pravega.
/// manager.create_scope("scope")
/// ```
///
#[cfg(feature = "python_binding")]
#[pymethods]
impl StreamManager {
    #[new]
    fn new(controller_uri: &str) -> Self {
        let config = ClientConfigBuilder::default()
            .controller_uri(controller_uri)
            .build()
            .expect("creating config");
        let client_factory = ClientFactory::new(config.clone());

        StreamManager {
            controller_ip: controller_uri.into(),
            cf: client_factory,
            config,
        }
    }

    ///
    /// Create a Scope in Pravega.
    ///
    #[text_signature = "($self, scope_name)"]
    pub fn create_scope(&self, scope_name: &str) -> PyResult<bool> {
        let handle = self.cf.get_runtime_handle();

        info!("creating scope {:?}", scope_name);

        let controller = self.cf.get_controller_client();
        let scope_name = Scope::from(scope_name.to_string());

        let scope_result = handle.block_on(controller.create_scope(&scope_name));
        info!("Scope creation status {:?}", scope_result);
        match scope_result {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }

    ///
    /// Delete a Scope in Pravega.
    ///
    #[text_signature = "($self, scope_name)"]
    pub fn delete_scope(&self, scope_name: &str) -> PyResult<bool> {
        let handle = self.cf.get_runtime_handle();
        info!("Delete scope {:?}", scope_name);

        let controller = self.cf.get_controller_client();
        let scope_name = Scope::from(scope_name.to_string());

        let scope_result = handle.block_on(controller.delete_scope(&scope_name));
        info!("Scope deletion status {:?}", scope_result);
        match scope_result {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }

    ///
    /// Create a Stream in Pravega.
    ///
    #[text_signature = "($self, scope_name, stream_name, initial_segments)"]
    pub fn create_stream(
        &self,
        scope_name: &str,
        stream_name: &str,
        initial_segments: i32,
    ) -> PyResult<bool> {
        let handle = self.cf.get_runtime_handle();
        info!(
            "creating stream {:?} under scope {:?} with segment count {:?}",
            stream_name, scope_name, initial_segments
        );
        let stream_cfg = StreamConfiguration {
            scoped_stream: ScopedStream {
                scope: Scope::from(scope_name.to_string()),
                stream: Stream::from(stream_name.to_string()),
            },
            scaling: Scaling {
                scale_type: ScaleType::FixedNumSegments,
                target_rate: 0,
                scale_factor: 0,
                min_num_segments: initial_segments,
            },
            retention: Retention {
                retention_type: RetentionType::None,
                retention_param: 0,
            },
        };
        let controller = self.cf.get_controller_client();

        let stream_result = handle.block_on(controller.create_stream(&stream_cfg));
        info!("Stream creation status {:?}", stream_result);
        match stream_result {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }

    ///
    /// Create a Stream in Pravega.
    ///
    #[text_signature = "($self, scope_name, stream_name)"]
    pub fn seal_stream(&self, scope_name: &str, stream_name: &str) -> PyResult<bool> {
        let handle = self.cf.get_runtime_handle();
        info!("Sealing stream {:?} under scope {:?} ", stream_name, scope_name);
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };

        let controller = self.cf.get_controller_client();

        let stream_result = handle.block_on(controller.seal_stream(&scoped_stream));
        info!("Sealing stream status {:?}", stream_result);
        match stream_result {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }

    ///
    /// Delete a Stream in Pravega.
    ///
    #[text_signature = "($self, scope_name, stream_name)"]
    pub fn delete_stream(&self, scope_name: &str, stream_name: &str) -> PyResult<bool> {
        let handle = self.cf.get_runtime_handle();
        info!("Deleting stream {:?} under scope {:?} ", stream_name, scope_name);
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };

        let controller = self.cf.get_controller_client();
        let stream_result = handle.block_on(controller.delete_stream(&scoped_stream));
        info!("Deleting stream status {:?}", stream_result);
        match stream_result {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::ValueError::py_err(format!("{:?}", e))),
        }
    }

    ///
    /// Create a Writer for a given Stream.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("127.0.0.1:9090")
    /// // Create a writer against an already created Pravega scope and Stream.
    /// writer=manager.create_writer("scope", "stream")
    /// ```
    ///
    #[text_signature = "($self, scope_name, stream_name)"]
    pub fn create_writer(&self, scope_name: &str, stream_name: &str) -> PyResult<StreamWriter> {
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };
        let stream_writer = StreamWriter::new(
            self.cf.create_event_stream_writer(scoped_stream.clone()),
            self.cf.get_runtime_handle(),
            scoped_stream,
        );
        Ok(stream_writer)
    }

    ///
    /// Create a Transactional Writer for a given Stream.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("127.0.0.1:9090")
    /// // Create a transactional writer against an already created Pravega scope and Stream.
    /// writer=manager.create_transaction_writer("scope", "stream", "123")
    /// ```
    ///
    #[text_signature = "($self, scope_name, stream_name, writer_id)"]
    pub fn create_transaction_writer(
        &self,
        scope_name: &str,
        stream_name: &str,
        writer_id: u128,
    ) -> PyResult<StreamTxnWriter> {
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_owned()),
            stream: Stream::from(stream_name.to_owned()),
        };
        let handle = self.cf.get_runtime_handle();
        let txn_writer = handle.block_on(
            self.cf
                .create_transactional_event_stream_writer(scoped_stream.clone(), WriterId(writer_id)),
        );
        let txn_stream_writer = StreamTxnWriter::new(txn_writer, self.cf.get_runtime_handle(), scoped_stream);
        Ok(txn_stream_writer)
    }

    ///
    /// Create a Reader for a given Stream.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("127.0.0.1:9090")
    /// // Create a reader against an already created Pravega scope and Stream.
    /// reader=manager.create_reader("scope", "stream")
    /// ```
    ///
    // #[text_signature = "($self, scope_name, stream_name)"]
    // pub fn create_reader_group(&self, scope_name: &str, stream_name: &str) -> PyResult<StreamReader> {
    //     let scoped_stream = ScopedStream {
    //         scope: Scope::from(scope_name.to_string()),
    //         stream: Stream::from(stream_name.to_string()),
    //     };
    //     let handle = self.cf.get_runtime_handle();
    //     let reader = handle.block_on(self.cf.create_event_stream_reader(scoped_stream.clone()));
    //     let stream_reader = StreamReader::new(
    //         Arc::new(Mutex::new(reader)),
    //         self.cf.get_runtime_handle(),
    //         scoped_stream,
    //     );
    //     Ok(stream_reader)
    // }

    /// Returns the facet string representation.
    fn to_str(&self) -> String {
        format!(
            "Controller ip: {:?} ClientConfig: {:?}",
            self.controller_ip, self.config
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
impl PyObjectProtocol for StreamManager {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("StreamManager({})", self.to_str()))
    }
}
