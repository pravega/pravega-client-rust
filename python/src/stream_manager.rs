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
        use crate::byte_stream::ByteStream;
        use pravega_client::client_factory::ClientFactory;
        use pravega_client_shared::*;
        use pravega_client_config::{ClientConfig, ClientConfigBuilder};
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use pyo3::{exceptions, PyObjectProtocol};
        use tracing::info;
        use pravega_client::event::reader_group::ReaderGroupConfigBuilder;
        use crate::stream_reader_group::StreamReaderGroupConfig;
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
#[pyo3(text_signature = "(controller_uri, auth_enabled, tls_enabled)")]
pub(crate) struct StreamManager {
    controller_ip: String,
    cf: ClientFactory,
    config: ClientConfig,
}

#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(Clone, Default)]
pub(crate) struct StreamRetentionPolicy {
    retention: Retention,
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
                retention_type: RetentionType::Time,
                retention_param: time_in_millis,
            },
        }
    }
}

#[cfg(feature = "python_binding")]
#[pyclass]
#[derive(Clone, Default)]
pub(crate) struct StreamScalingPolicy {
    scaling: Scaling,
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
    #[pyo3(text_signature = "($self, scope_name)")]
    pub fn create_scope(&self, scope_name: &str) -> PyResult<bool> {
        let handle = self.cf.runtime_handle();

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
    #[pyo3(text_signature = "($self, scope_name)")]
    pub fn delete_scope(&self, scope_name: &str) -> PyResult<bool> {
        let handle = self.cf.runtime_handle();
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
    /// Create a Stream in Pravega.
    ///
    #[pyo3(text_signature = "($self, scope_name, stream_name, scaling_policy, retention_policy, tags)")]
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
        let handle = self.cf.runtime_handle();
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
    #[pyo3(text_signature = "($self, scope_name, stream_name, initial_segments)")]
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
    /// Update Stream Configuration in Pravega.
    ///
    #[pyo3(text_signature = "($self, scope_name, stream_name, scaling_policy, retention_policy, tags)")]
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
        let handle = self.cf.runtime_handle();
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
    /// Get Stream tags from Pravega.
    ///
    #[pyo3(text_signature = "($self, scope_name, stream_name, scaling_policy, retention_policy, tags)")]
    pub fn get_stream_tags(&self, scope_name: &str, stream_name: &str) -> PyResult<Option<Vec<String>>> {
        let handle = self.cf.runtime_handle();
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
    /// Seal a Stream in Pravega.
    ///
    #[pyo3(text_signature = "($self, scope_name, stream_name)")]
    pub fn seal_stream(&self, scope_name: &str, stream_name: &str) -> PyResult<bool> {
        let handle = self.cf.runtime_handle();
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
    #[pyo3(text_signature = "($self, scope_name, stream_name)")]
    pub fn delete_stream(&self, scope_name: &str, stream_name: &str) -> PyResult<bool> {
        let handle = self.cf.runtime_handle();
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
    ///
    /// By default the max inflight events is configured for 0. The users can change this value
    /// to ensure there are multiple inflight events at any given point in time and can use the
    /// flush() API on the writer to wait until all the events are persisted.
    ///
    ///
    /// ```
    ///
    #[pyo3(text_signature = "($self, scope_name, stream_name, max_inflight_events)")]
    #[args(scope_name, stream_name, max_inflight_events = 0)]
    pub fn create_writer(
        &self,
        scope_name: &str,
        stream_name: &str,
        max_inflight_events: usize,
    ) -> PyResult<StreamWriter> {
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };
        let stream_writer = StreamWriter::new(
            self.cf.create_event_writer(scoped_stream.clone()),
            self.cf.runtime_handle(),
            scoped_stream,
            max_inflight_events,
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
    #[pyo3(text_signature = "($self, scope_name, stream_name, writer_id)")]
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
        let handle = self.cf.runtime_handle();
        let txn_writer = handle.block_on(
            self.cf
                .create_transactional_event_writer(scoped_stream.clone(), WriterId(writer_id)),
        );
        let txn_stream_writer = StreamTxnWriter::new(txn_writer, self.cf.runtime_handle(), scoped_stream);
        Ok(txn_stream_writer)
    }

    ///
    /// Create a ReaderGroup for a given Stream.
    /// By default all the readers created from this ReaderGroup will start reading from the
    /// current HEAD/start of the Stream.
    /// The user can also choose to read from the tail of the Stream.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
    /// // Create a ReaderGroup against an already created Pravega scope and Stream.
    /// event.reader_group=manager.create_reader_group("rg1", "scope", "stream")
    ///
    /// // Create a ReaderGroup to read from the current TAIL/end of an already created Pravega scope and Stream
    /// event.reader_group=manager.create_reader_group("rg1", "scope", "stream", true)
    /// ```
    ///
    #[pyo3(text_signature = "($self, reader_group_name, scope_name, stream_name, read_from_tail)")]
    #[args(read_from_tail = "false")]
    pub fn create_reader_group(
        &self,
        reader_group_name: &str,
        scope_name: &str,
        stream_name: &str,
        read_from_tail: bool,
    ) -> PyResult<StreamReaderGroup> {
        let scope = Scope::from(scope_name.to_string());
        let scoped_stream = ScopedStream {
            scope: scope.clone(),
            stream: Stream::from(stream_name.to_string()),
        };
        let handle = self.cf.runtime_handle();
        let rg_config = if read_from_tail {
            // Create a reader group to read from the current TAIL/end of the Stream.
            ReaderGroupConfigBuilder::default()
                .read_from_tail_of_stream(scoped_stream)
                .build()
        } else {
            // Create a reader group to read from current HEAD/start of the Stream.
            ReaderGroupConfigBuilder::default()
                .read_from_head_of_stream(scoped_stream)
                .build()
        };
        let rg = handle.block_on(self.cf.create_reader_group_with_config(
            reader_group_name.to_string(),
            rg_config,
            scope,
        ));
        let reader_group = StreamReaderGroup::new(rg, self.cf.runtime_handle());
        Ok(reader_group)
    }

    ///
    /// Create a ReaderGroup for a given Stream given the Reader Group Config.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
    /// // Create a ReaderGroup against 1 or more Pravega streams.
    /// rg_config = pravega_client.StreamReaderGroupConfig(False, "Sc", "s1", "s2", "s3")
    /// // Create a ReaderGroup against an already created Pravega scope and Stream.
    /// event.reader_group=manager.create_reader_group_with_config("rg1", "scope", rg_config)
    ///
    /// // Create a ReaderGroup to read from the current TAIL/end of an already created Pravega scope and Stream
    /// event.reader_group=manager.create_reader_group("rg1", "scope", "stream", true)
    /// ```
    ///
    #[pyo3(text_signature = "($self, reader_group_name, scope_name, stream_name, read_from_tail)")]
    #[args(read_from_tail = "false")]
    pub fn create_reader_group_with_config(
        &self,
        reader_group_name: &str,
        scope_name: &str,
        rg_config: StreamReaderGroupConfig,
    ) -> PyResult<StreamReaderGroup> {
        let scope = Scope::from(scope_name.to_string());

        let handle = self.cf.runtime_handle();

        let rg = handle.block_on(self.cf.create_reader_group_with_config(
            reader_group_name.to_string(),
            rg_config.reader_group_config,
            scope,
        ));
        let reader_group = StreamReaderGroup::new(rg, self.cf.runtime_handle());
        Ok(reader_group)
    }

    ///
    /// Delete a ReaderGroup.
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
    /// // Delete a ReaderGroup against an already created Pravega scope..
    /// manager.delete_reader_group_with_config("rg1", "scope", rg_config)
    ///
    /// ```
    ///
    #[pyo3(text_signature = "($self, reader_group_name, scope_name)")]
    pub fn delete_reader_group(&self, reader_group_name: &str, scope_name: &str) -> PyResult<()> {
        let scope = Scope::from(scope_name.to_string());

        let handle = self.cf.runtime_handle();

        let delete_result =
            handle.block_on(self.cf.delete_reader_group(scope, reader_group_name.to_string()));
        info!("Delete ReaderGroup {:?}", delete_result);
        match delete_result {
            Ok(_) => Ok(()),
            Err(e) => Err(exceptions::PyValueError::new_err(format!("{:?}", e))),
        }
    }

    ///
    /// Create a Binary I/O representation of a Pravega Stream. This ByteStream implements the
    /// APIs provided by [io.IOBase](https://docs.python.org/3/library/io.html#io.IOBase)
    ///
    /// ```
    /// import pravega_client;
    /// manager=pravega_client.StreamManager("tcp://127.0.0.1:9090")
    /// Create a Byte_stream against an already created Pravega scope and Stream.
    /// byte_stream=manager.create_byte_stream("scope", "stream");
    /// # write bytes into the Pravega Stream.
    /// byte_stream.write(b"bytes")
    /// 5
    /// # Seek to a specified read offset.
    /// byte_stream.seek(2, 0)
    /// # Display the current read offset.
    /// byte_stream.tell()
    /// 2
    ///
    /// buf=bytearray(5)
    /// byte_stream.readinto(buf)
    /// 3
    /// buf
    /// bytearray(b'tes\x00\x00')
    /// ```
    ///
    #[pyo3(text_signature = "($self, scope_name, stream_name)")]
    pub fn create_byte_stream(&self, scope_name: &str, stream_name: &str) -> PyResult<ByteStream> {
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };

        let byte_stream = ByteStream::new(
            scoped_stream.clone(),
            self.cf.runtime_handle(),
            self.cf
                .runtime_handle()
                .block_on(self.cf.create_byte_writer(scoped_stream.clone())),
            self.cf
                .runtime_handle()
                .block_on(self.cf.create_byte_reader(scoped_stream)),
        );
        Ok(byte_stream)
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
