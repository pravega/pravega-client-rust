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
        use crate::stream_reader_group::StreamReaderGroup;
        use pravega_client::client_factory::ClientFactory;
        use pravega_client_shared::*;
        use pravega_client_config::{ClientConfig, ClientConfigBuilder};
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use pyo3::{exceptions, PyObjectProtocol};
        use tracing::info;
    }
}

///
/// Create a StreamManager by providing a controller uri.
/// ```
/// import pravega_client;
/// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
/// // this manager can be used to create scopes, streams, writers and readers against Pravega.
/// manager.create_scope("scope")
///
/// // optionally enable tls support using tls:// scheme
/// manager=pravega_client.StreamManager("tls://127.0.0.1:9090")
/// ```
///
#[cfg(feature = "python_binding")]
#[pyclass]
#[text_signature = "(controller_uri, auth_enabled, tls_enabled)"]
pub(crate) struct StreamManager {
    controller_ip: String,
    cf: ClientFactory,
    config: ClientConfig,
}

#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(Clone)]
pub(crate) struct StreamRetentionPolicy {
    retention: Retention,
}

impl Default for StreamRetentionPolicy {
    fn default() -> Self {
        StreamRetentionPolicy {
            retention: Default::default(),
        }
    }
}

#[cfg(feature = "python_binding")]
#[pymethods]
impl StreamRetentionPolicy {
    #[staticmethod]
    pub fn none() -> StreamRetentionPolicy {
        StreamRetentionPolicy {
            retention: Default::default(),
        }
    }

    #[staticmethod]
    pub fn by_size(size_in_bytes: i64) -> StreamRetentionPolicy {
        StreamRetentionPolicy {
            retention: Retention {
                retention_type: RetentionType::Size,
                retention_param: size_in_bytes,
            },
        }
    }

    #[staticmethod]
    pub fn by_time(time_in_millis: i64) -> StreamRetentionPolicy {
        StreamRetentionPolicy {
            retention: Retention {
                retention_type: RetentionType::Size,
                retention_param: time_in_millis,
            },
        }
    }
}

#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(Clone)]
pub(crate) struct StreamScalingPolicy {
    scaling: Scaling,
}

impl Default for StreamScalingPolicy {
    fn default() -> Self {
        StreamScalingPolicy {
            scaling: Default::default(),
        }
    }
}

#[cfg(feature = "python_binding")]
#[pymethods]
impl StreamScalingPolicy {
    #[staticmethod]
    pub fn fixed_scaling_policy(initial_segments: i32) -> StreamScalingPolicy {
        StreamScalingPolicy {
            scaling: Scaling {
                scale_type: ScaleType::FixedNumSegments,
                target_rate: 0,
                scale_factor: 0,
                min_num_segments: initial_segments,
            },
        }
    }

    #[staticmethod]
    pub fn auto_scaling_policy_by_data_rate(
        target_rate_kbytes_per_sec: i32,
        scale_factor: i32,
        initial_segments: i32,
    ) -> StreamScalingPolicy {
        StreamScalingPolicy {
            scaling: Scaling {
                scale_type: ScaleType::ByRateInKbytesPerSec,
                target_rate: target_rate_kbytes_per_sec,
                scale_factor,
                min_num_segments: initial_segments,
            },
        }
    }

    #[staticmethod]
    pub fn auto_scaling_policy_by_event_rate(
        target_events_per_sec: i32,
        scale_factor: i32,
        initial_segments: i32,
    ) -> StreamScalingPolicy {
        StreamScalingPolicy {
            scaling: Scaling {
                scale_type: ScaleType::ByRateInEventsPerSec,
                target_rate: target_events_per_sec,
                scale_factor,
                min_num_segments: initial_segments,
            },
        }
    }
}

///
/// Create a StreamManager by providing a controller uri.
/// ```
/// import pravega_client;
/// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
/// // this manager can be used to create scopes, streams, writers and readers against Pravega.
/// manager.create_scope("scope")
///
/// // optionally enable tls support using tls:// scheme
/// manager=pravega_client.StreamManager("tls://127.0.0.1:9090")
/// ```
///
#[cfg(feature = "python_binding")]
#[pymethods]
impl StreamManager {
    #[new]
    #[args(
        auth_enabled = "false",
        tls_enabled = "false",
        disable_cert_verification = "false"
    )]
    fn new(
        controller_uri: &str,
        auth_enabled: bool,
        tls_enabled: bool,
        disable_cert_verification: bool,
    ) -> Self {
        let mut builder = ClientConfigBuilder::default();

        builder
            .controller_uri(controller_uri)
            .is_auth_enabled(auth_enabled);
        if tls_enabled {
            // would be better to have tls_enabled be &PyAny
            // and args tls_enabled = None or sentinel e.g. missing=object()
            builder.is_tls_enabled(tls_enabled);
            builder.disable_cert_verification(disable_cert_verification);
        }
        let config = builder.build().expect("creating config");
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
        let handle = self.cf.runtime();

        info!("creating scope {:?}", scope_name);

        let controller = self.cf.controller_client();
        let scope_name = Scope::from(scope_name.to_string());

        let scope_result = handle.block_on(controller.create_scope(&scope_name));
        info!("Scope creation status {:?}", scope_result);
        match scope_result {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::PyValueError::new_err(format!("{:?}", e))),
        }
    }

    ///
    /// Delete a Scope in Pravega.
    ///
    #[text_signature = "($self, scope_name)"]
    pub fn delete_scope(&self, scope_name: &str) -> PyResult<bool> {
        let handle = self.cf.runtime();
        info!("Delete scope {:?}", scope_name);

        let controller = self.cf.controller_client();
        let scope_name = Scope::from(scope_name.to_string());

        let scope_result = handle.block_on(controller.delete_scope(&scope_name));
        info!("Scope deletion status {:?}", scope_result);
        match scope_result {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::PyValueError::new_err(format!("{:?}", e))),
        }
    }

    ///
    /// Create a Stream in Pravega
    ///
    #[text_signature = "($self, scope_name, stream_name, scaling_policy, retention_policy, tags)"]
    #[args(
        scaling_policy = "Default::default()",
        retention_policy = "Default::default()",
        tags = "None"
    )]
    pub fn create_stream_with_policy(
        &self,
        scope_name: &str,
        stream_name: &str,
        scaling_policy: StreamScalingPolicy,
        retention_policy: StreamRetentionPolicy,
        tags: Option<Vec<String>>,
    ) -> PyResult<bool> {
        let handle = self.cf.runtime();
        info!(
            "creating stream {:?} under scope {:?} with scaling policy {:?}, retention policy {:?} and tags {:?}",
            stream_name, scope_name, scaling_policy.scaling, retention_policy.retention, tags
        );
        let stream_cfg = StreamConfiguration {
            scoped_stream: ScopedStream {
                scope: Scope::from(scope_name.to_string()),
                stream: Stream::from(stream_name.to_string()),
            },
            scaling: scaling_policy.scaling,
            retention: retention_policy.retention,
            tags,
        };
        let controller = self.cf.controller_client();

        let stream_result = handle.block_on(controller.create_stream(&stream_cfg));
        info!("Stream creation status {:?}", stream_result);
        match stream_result {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::PyValueError::new_err(format!("{:?}", e))),
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
        self.create_stream_with_policy(
            scope_name,
            stream_name,
            StreamScalingPolicy::fixed_scaling_policy(initial_segments),
            Default::default(),
            None,
        )
    }

    ///
    /// Update Stream Configuration in Pravega
    ///
    #[text_signature = "($self, scope_name, stream_name, scaling_policy, retention_policy, tags)"]
    #[args(
        scaling_policy = "Default::default()",
        retention_policy = "Default::default()",
        tags = "None"
    )]
    pub fn update_stream_with_policy(
        &self,
        scope_name: &str,
        stream_name: &str,
        scaling_policy: StreamScalingPolicy,
        retention_policy: StreamRetentionPolicy,
        tags: Option<Vec<String>>,
    ) -> PyResult<bool> {
        let handle = self.cf.runtime();
        info!(
            "updating stream {:?} under scope {:?} with scaling policy {:?}, retention policy {:?} and tags {:?}",
            stream_name, scope_name, scaling_policy.scaling, retention_policy.retention, tags
        );
        let stream_cfg = StreamConfiguration {
            scoped_stream: ScopedStream {
                scope: Scope::from(scope_name.to_string()),
                stream: Stream::from(stream_name.to_string()),
            },
            scaling: scaling_policy.scaling,
            retention: retention_policy.retention,
            tags,
        };
        let controller = self.cf.controller_client();

        let stream_result = handle.block_on(controller.update_stream(&stream_cfg));
        info!("Stream updation status {:?}", stream_result);
        match stream_result {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::PyValueError::new_err(format!("{:?}", e))),
        }
    }

    ///
    /// Get Stream tags from Pravega
    ///
    #[text_signature = "($self, scope_name, stream_name, scaling_policy, retention_policy, tags)"]
    pub fn get_stream_tags(&self, scope_name: &str, stream_name: &str) -> PyResult<Option<Vec<String>>> {
        let handle = self.cf.runtime();
        info!(
            "fetch tags for stream {:?} under scope {:?}",
            stream_name, scope_name,
        );
        let stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };
        let controller = self.cf.controller_client();

        let stream_configuration = handle.block_on(controller.get_stream_configuration(&stream));
        info!("Stream configuration is {:?}", stream_configuration);
        match stream_configuration {
            Ok(t) => Ok(t.tags),
            Err(e) => Err(exceptions::PyValueError::new_err(format!("{:?}", e))),
        }
    }

    ///
    /// Create a Stream in Pravega.
    ///
    #[text_signature = "($self, scope_name, stream_name)"]
    pub fn seal_stream(&self, scope_name: &str, stream_name: &str) -> PyResult<bool> {
        let handle = self.cf.runtime();
        info!("Sealing stream {:?} under scope {:?} ", stream_name, scope_name);
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };

        let controller = self.cf.controller_client();

        let stream_result = handle.block_on(controller.seal_stream(&scoped_stream));
        info!("Sealing stream status {:?}", stream_result);
        match stream_result {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::PyValueError::new_err(format!("{:?}", e))),
        }
    }

    ///
    /// Delete a Stream in Pravega.
    ///
    #[text_signature = "($self, scope_name, stream_name)"]
    pub fn delete_stream(&self, scope_name: &str, stream_name: &str) -> PyResult<bool> {
        let handle = self.cf.runtime();
        info!("Deleting stream {:?} under scope {:?} ", stream_name, scope_name);
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };

        let controller = self.cf.controller_client();
        let stream_result = handle.block_on(controller.delete_stream(&scoped_stream));
        info!("Deleting stream status {:?}", stream_result);
        match stream_result {
            Ok(t) => Ok(t),
            Err(e) => Err(exceptions::PyValueError::new_err(format!("{:?}", e))),
        }
    }

    ///
    /// Create a Writer for a given Stream.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
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
            self.cf.create_event_writer(scoped_stream.clone()),
            self.cf.clone(),
            scoped_stream,
        );
        Ok(stream_writer)
    }

    ///
    /// Create a Transactional Writer for a given Stream.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
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
        let handle = self.cf.runtime();
        let txn_writer = handle.block_on(
            self.cf
                .create_transactional_event_writer(scoped_stream.clone(), WriterId(writer_id)),
        );
        let txn_stream_writer = StreamTxnWriter::new(txn_writer, self.cf.clone(), scoped_stream);
        Ok(txn_stream_writer)
    }

    ///
    /// Create a ReaderGroup for a given Stream.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
    /// // Create a ReaderGroup against an already created Pravega scope and Stream.
    /// event.reader_group=manager.create_reader_group("rg1", "scope", "stream")
    /// ```
    ///
    #[text_signature = "($self, reader_group_name, scope_name, stream_name)"]
    pub fn create_reader_group(
        &self,
        reader_group_name: &str,
        scope_name: &str,
        stream_name: &str,
    ) -> PyResult<StreamReaderGroup> {
        let scope = Scope::from(scope_name.to_string());
        let scoped_stream = ScopedStream {
            scope: scope.clone(),
            stream: Stream::from(stream_name.to_string()),
        };
        let handle = self.cf.runtime();
        let rg = handle.block_on(self.cf.create_reader_group(
            scope,
            reader_group_name.to_string(),
            scoped_stream.clone(),
        ));
        let reader_group = StreamReaderGroup::new(rg, self.cf.clone(), scoped_stream);
        Ok(reader_group)
    }

    /// Returns the string representation.
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
