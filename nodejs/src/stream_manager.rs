// Copyright Pravega Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::stream_reader_group::{StreamCut, StreamReaderGroup};
use crate::stream_writer::StreamWriter;
use crate::stream_writer_transactional::StreamTxnWriter;
use neon::prelude::*;
use pravega_client::client_factory::ClientFactory;
use pravega_client::event::reader_group::{ReaderGroupConfigBuilder, StreamCutVersioned};
use pravega_client::sync::table::TableError;
use pravega_client_config::{ClientConfig, ClientConfigBuilder};
use pravega_client_retry::retry_result::RetryError;
use pravega_client_shared::*;
use pravega_controller_client::ControllerError;
use std::time::Duration;
use tokio::time;
use tracing::info;

const TIMEOUT_IN_MILLISECONDS: u64 = 120000; // timeout for list scopes and streams

///
/// An internal rust struct that holds the necessary info to perform actions on StreamManager.
/// The `js_new` method will return a boxed(wrapped) StreamManager as an external object.
///
pub(crate) struct StreamManager {
    controller_ip: String,
    pub cf: ClientFactory,
    config: ClientConfig,
}

pub(crate) struct StreamRetentionPolicy {
    retention: Retention,
}

impl Finalize for StreamRetentionPolicy {}

impl StreamRetentionPolicy {
    pub fn js_none(mut cx: FunctionContext) -> JsResult<JsBox<StreamRetentionPolicy>> {
        let stream_retention_policy = StreamRetentionPolicy {
            retention: Default::default(),
        };

        Ok(cx.boxed(stream_retention_policy))
    }

    pub fn js_by_size(mut cx: FunctionContext) -> JsResult<JsBox<StreamRetentionPolicy>> {
        let size_in_bytes = cx.argument::<JsNumber>(0)?.value(&mut cx) as i64;

        let stream_retention_policy = StreamRetentionPolicy {
            retention: Retention {
                retention_type: RetentionType::Size,
                retention_param: size_in_bytes,
            },
        };

        Ok(cx.boxed(stream_retention_policy))
    }

    pub fn js_by_time(mut cx: FunctionContext) -> JsResult<JsBox<StreamRetentionPolicy>> {
        let time_in_millis = cx.argument::<JsNumber>(0)?.value(&mut cx) as i64;

        let stream_retention_policy = StreamRetentionPolicy {
            retention: Retention {
                retention_type: RetentionType::Time,
                retention_param: time_in_millis,
            },
        };

        Ok(cx.boxed(stream_retention_policy))
    }
}

pub(crate) struct StreamScalingPolicy {
    scaling: Scaling,
}

impl Finalize for StreamScalingPolicy {}

impl StreamScalingPolicy {
    pub fn js_fixed_scaling_policy(mut cx: FunctionContext) -> JsResult<JsBox<StreamScalingPolicy>> {
        let initial_segments = cx.argument::<JsNumber>(0)?.value(&mut cx) as i32;

        let stream_scaling_policy = StreamScalingPolicy {
            scaling: Scaling {
                scale_type: ScaleType::FixedNumSegments,
                target_rate: 0,
                scale_factor: 0,
                min_num_segments: initial_segments,
            },
        };

        Ok(cx.boxed(stream_scaling_policy))
    }

    pub fn js_auto_scaling_policy_by_data_rate(
        mut cx: FunctionContext,
    ) -> JsResult<JsBox<StreamScalingPolicy>> {
        let target_rate_kbytes_per_sec = cx.argument::<JsNumber>(0)?.value(&mut cx) as i32;
        let scale_factor = cx.argument::<JsNumber>(1)?.value(&mut cx) as i32;
        let initial_segments = cx.argument::<JsNumber>(2)?.value(&mut cx) as i32;

        let stream_scaling_policy = StreamScalingPolicy {
            scaling: Scaling {
                scale_type: ScaleType::ByRateInKbytesPerSec,
                target_rate: target_rate_kbytes_per_sec,
                scale_factor,
                min_num_segments: initial_segments,
            },
        };

        Ok(cx.boxed(stream_scaling_policy))
    }

    pub fn js_auto_scaling_policy_by_event_rate(
        mut cx: FunctionContext,
    ) -> JsResult<JsBox<StreamScalingPolicy>> {
        let target_events_per_sec = cx.argument::<JsNumber>(0)?.value(&mut cx) as i32;
        let scale_factor = cx.argument::<JsNumber>(1)?.value(&mut cx) as i32;
        let initial_segments = cx.argument::<JsNumber>(2)?.value(&mut cx) as i32;

        let stream_scaling_policy = StreamScalingPolicy {
            scaling: Scaling {
                scale_type: ScaleType::ByRateInEventsPerSec,
                target_rate: target_events_per_sec,
                scale_factor,
                min_num_segments: initial_segments,
            },
        };

        Ok(cx.boxed(stream_scaling_policy))
    }
}

impl Finalize for StreamManager {}

///
/// The implementation of the pure Rust client call.
///
impl StreamManager {
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
            builder.is_tls_enabled(tls_enabled);
            builder.disable_cert_verification(disable_cert_verification);
        }
        let config = builder.build().expect("creating config");
        let client_factory = ClientFactory::new(config.clone());

        StreamManager {
            controller_ip: controller_uri.to_string(),
            cf: client_factory,
            config,
        }
    }

    ///
    /// Create a Scope in Pravega.
    ///
    fn create_scope(&self, scope_name: &str) -> Result<bool, RetryError<ControllerError>> {
        let handle = self.cf.runtime_handle();
        info!("creating scope {:?}", scope_name);

        let controller = self.cf.controller_client();
        let scope_name = Scope::from(scope_name.to_string());

        handle.block_on(controller.create_scope(&scope_name))
    }

    ///
    /// Delete a Scope in Pravega.
    ///
    fn delete_scope(&self, scope_name: &str) -> Result<bool, RetryError<ControllerError>> {
        let handle = self.cf.runtime_handle();
        info!("Delete scope {:?}", scope_name);

        let controller = self.cf.controller_client();
        let scope_name = Scope::from(scope_name.to_string());

        handle.block_on(controller.delete_scope(&scope_name))
    }

    ///
    /// Create a Stream in Pravega.
    ///
    pub fn create_stream_with_policy(
        &self,
        scope_name: &str,
        stream_name: &str,
        scaling_policy: Scaling,
        retention_policy: Retention,
        tags: Option<Vec<String>>,
    ) -> Result<bool, RetryError<ControllerError>> {
        let handle = self.cf.runtime_handle();
        info!(
            "creating stream {:?} under scope {:?} with scaling policy {:?}, retention policy {:?} and tags {:?}",
            stream_name, scope_name, scaling_policy, retention_policy, tags
        );
        let stream_cfg = StreamConfiguration {
            scoped_stream: ScopedStream {
                scope: Scope::from(scope_name.to_string()),
                stream: Stream::from(stream_name.to_string()),
            },
            scaling: scaling_policy,
            retention: retention_policy,
            tags,
        };
        let controller = self.cf.controller_client();

        handle.block_on(controller.create_stream(&stream_cfg))
    }

    ///
    /// Update Stream Configuration in Pravega
    ///
    pub fn update_stream_with_policy(
        &self,
        scope_name: &str,
        stream_name: &str,
        scaling_policy: Scaling,
        retention_policy: Retention,
        tags: Option<Vec<String>>,
    ) -> Result<bool, RetryError<ControllerError>> {
        let handle = self.cf.runtime_handle();
        info!(
            "updating stream {:?} under scope {:?} with scaling policy {:?}, retention policy {:?} and tags {:?}",
            stream_name, scope_name, scaling_policy, retention_policy, tags
        );
        let stream_cfg = StreamConfiguration {
            scoped_stream: ScopedStream {
                scope: Scope::from(scope_name.to_string()),
                stream: Stream::from(stream_name.to_string()),
            },
            scaling: scaling_policy,
            retention: retention_policy,
            tags,
        };
        let controller = self.cf.controller_client();

        handle.block_on(controller.update_stream(&stream_cfg))
    }

    ///
    /// Get Stream tags from Pravega.
    ///
    pub fn get_stream_tags(
        &self,
        scope_name: &str,
        stream_name: &str,
    ) -> Result<Option<Vec<String>>, RetryError<ControllerError>> {
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
        match stream_configuration {
            Ok(val) => Ok(val.tags),
            Err(err) => Err(err),
        }
    }

    ///
    /// Seal a Stream in Pravega.
    ///
    pub fn seal_stream(
        &self,
        scope_name: &str,
        stream_name: &str,
    ) -> Result<bool, RetryError<ControllerError>> {
        let handle = self.cf.runtime_handle();
        info!("Sealing stream {:?} under scope {:?} ", stream_name, scope_name);
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };

        let controller = self.cf.controller_client();

        handle.block_on(controller.seal_stream(&scoped_stream))
    }

    ///
    /// Delete a Stream in Pravega.
    ///
    pub fn delete_stream(
        &self,
        scope_name: &str,
        stream_name: &str,
    ) -> Result<bool, RetryError<ControllerError>> {
        let handle = self.cf.runtime_handle();
        info!("Deleting stream {:?} under scope {:?} ", stream_name, scope_name);
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };

        let controller = self.cf.controller_client();
        handle.block_on(controller.delete_stream(&scoped_stream))
    }

    ///
    /// Create a ReaderGroup for a given Stream.
    ///
    fn create_reader_group(
        &self,
        reader_group_name: &str,
        scope_name: &str,
        streams: Vec<String>,
        stream_cut: &StreamCutVersioned,
    ) -> StreamReaderGroup {
        let mut rg_config_builder = ReaderGroupConfigBuilder::default();

        let scope = Scope::from(scope_name.to_string());
        for stream in streams {
            let scoped_stream = ScopedStream {
                scope: scope.clone(),
                stream: Stream::from(stream.to_string()),
            };
            rg_config_builder.read_from_stream(scoped_stream, stream_cut.clone());
        }
        let rg_config = rg_config_builder.build();

        let handle = self.cf.runtime_handle();
        let rg = handle.block_on(self.cf.create_reader_group_with_config(
            reader_group_name.to_string(),
            rg_config,
            scope,
        ));
        StreamReaderGroup::new(rg, self.cf.runtime_handle())
    }

    ///
    /// Delete a ReaderGroup for a given Stream.
    ///
    fn delete_reader_group(&self, scope_name: &str, reader_group_name: &str) -> Result<(), TableError> {
        let scope = Scope::from(scope_name.to_string());

        let handle = self.cf.runtime_handle();
        handle.block_on(self.cf.delete_reader_group(scope, reader_group_name.to_string()))
    }

    ///
    /// Create a Writer for a given Stream.
    ///
    pub fn create_writer(&self, scope_name: &str, stream_name: &str) -> StreamWriter {
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };
        StreamWriter::new(
            self.cf.create_event_writer(scoped_stream.clone()),
            self.cf.runtime_handle(),
            scoped_stream,
        )
    }

    ///
    /// Create a Transactional Writer for a given Stream.
    ///
    pub fn create_transaction_writer(
        &self,
        scope_name: &str,
        stream_name: &str,
        writer_id: u128,
    ) -> StreamTxnWriter {
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };
        let txn_writer = self.cf.runtime_handle().block_on(
            self.cf
                .create_transactional_event_writer(scoped_stream.clone(), WriterId(writer_id)),
        );
        StreamTxnWriter::new(txn_writer, self.cf.runtime_handle(), scoped_stream)
    }
}

///
/// The implementation of the JavaScript proxy call and the parameters cast.
///
impl StreamManager {
    pub fn js_new(mut cx: FunctionContext) -> JsResult<JsBox<StreamManager>> {
        let controller_uri = cx.argument::<JsString>(0)?.value(&mut cx);
        let auth_enabled = cx.argument::<JsBoolean>(1)?.value(&mut cx);
        let tls_enabled = cx.argument::<JsBoolean>(2)?.value(&mut cx);
        let disable_cert_verification = cx.argument::<JsBoolean>(3)?.value(&mut cx);

        let stream_manager = StreamManager::new(
            &controller_uri,
            auth_enabled,
            tls_enabled,
            disable_cert_verification,
        );

        Ok(cx.boxed(stream_manager))
    }

    pub fn js_create_scope(mut cx: FunctionContext) -> JsResult<JsBoolean> {
        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;
        let scope_name = cx.argument::<JsString>(0)?.value(&mut cx);

        let scope_result = stream_manager.create_scope(&scope_name);
        match scope_result {
            Ok(t) => Ok(cx.boolean(t)),
            Err(e) => cx.throw_error(e.to_string()),
        }
    }

    pub fn js_delete_scope(mut cx: FunctionContext) -> JsResult<JsBoolean> {
        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;
        let scope_name = cx.argument::<JsString>(0)?.value(&mut cx);

        let scope_result = stream_manager.delete_scope(&scope_name);
        match scope_result {
            Ok(t) => Ok(cx.boolean(t)),
            Err(e) => cx.throw_error(e.to_string()),
        }
    }

    pub fn js_list_scopes(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;

        let channel = cx.channel();
        let (deferred, promise) = cx.promise();

        // spawn an `async` task on the tokio runtime.
        use std::sync::Arc;
        let cf = Arc::new(stream_manager.cf.to_async());
        let handle = stream_manager.cf.runtime_handle();
        // enter the runtime context and call spawn with timeout
        let _rt = handle.enter();
        tokio::spawn(time::timeout(
            Duration::from_millis(TIMEOUT_IN_MILLISECONDS),
            async move {
                use futures::stream::StreamExt;
                use pravega_controller_client::paginator::list_scopes;

                // expensive block procedure executed in the tokio thread
                let cf = cf.clone();
                let controller = cf.controller_client();
                let scope = list_scopes(controller);
                let scopes = scope.map(|str| str.unwrap()).collect::<Vec<Scope>>().await;

                // notify and execute in the javascript main thread
                deferred.settle_with(&channel, move |mut cx| {
                    use std::convert::TryInto;
                    let scopes_length: u32 = match scopes.len().try_into() {
                        Ok(val) => val,
                        Err(_err) => return cx.throw_error("Too many scopes"),
                    };
                    let array: Handle<JsArray> = JsArray::new(&mut cx, scopes_length);
                    for (pos, e) in scopes.iter().enumerate() {
                        let scope_name = cx.string(e.name.clone());
                        array.set(&mut cx, pos as u32, scope_name)?;
                    }
                    Ok(array)
                });
            },
        ));
        Ok(promise)
    }

    pub fn js_create_stream_with_policy(mut cx: FunctionContext) -> JsResult<JsBoolean> {
        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;
        let scope_name = cx.argument::<JsString>(0)?.value(&mut cx);
        let stream_name = cx.argument::<JsString>(1)?.value(&mut cx);
        let retention_policy = cx.argument::<JsBox<StreamRetentionPolicy>>(2)?;
        let scaling_policy = cx.argument::<JsBox<StreamScalingPolicy>>(3)?;
        // Wonderful example for how to read and cast string[] to Vec<String>.
        // https://github.com/neon-bindings/neon/issues/613
        let tags = cx
            .argument::<JsArray>(4)?
            .to_vec(&mut cx)?
            .into_iter()
            .map(|v| {
                v.downcast_or_throw::<JsString, _>(&mut cx)
                    .map(|v| v.value(&mut cx))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let stream_result = stream_manager.create_stream_with_policy(
            &scope_name,
            &stream_name,
            // TODO: Handle<JsBox<T>> auto dereferencing won't work for this (no idea why),
            //       so manually getting the internal scaling and retention is required.
            // https://docs.rs/neon/latest/neon/types/struct.JsBox.html#deref-behavior
            scaling_policy.scaling.clone(),
            retention_policy.retention.clone(),
            match tags.len() {
                l if l == 0 => None,
                _ => Some(tags),
            },
        );
        match stream_result {
            Ok(t) => Ok(cx.boolean(t)),
            Err(e) => cx.throw_error(e.to_string()),
        }
    }

    pub fn js_update_stream_with_policy(mut cx: FunctionContext) -> JsResult<JsBoolean> {
        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;
        let scope_name = cx.argument::<JsString>(0)?.value(&mut cx);
        let stream_name = cx.argument::<JsString>(1)?.value(&mut cx);
        let retention_policy = cx.argument::<JsBox<StreamRetentionPolicy>>(2)?;
        let scaling_policy = cx.argument::<JsBox<StreamScalingPolicy>>(3)?;
        let tags = cx
            .argument::<JsArray>(4)?
            .to_vec(&mut cx)?
            .into_iter()
            .map(|v| {
                v.downcast_or_throw::<JsString, _>(&mut cx)
                    .map(|v| v.value(&mut cx))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let stream_result = stream_manager.update_stream_with_policy(
            &scope_name,
            &stream_name,
            scaling_policy.scaling.clone(),
            retention_policy.retention.clone(),
            match tags.len() {
                l if l == 0 => None,
                _ => Some(tags),
            },
        );
        match stream_result {
            Ok(t) => Ok(cx.boolean(t)),
            Err(e) => cx.throw_error(e.to_string()),
        }
    }

    pub fn js_get_stream_tags(mut cx: FunctionContext) -> JsResult<JsArray> {
        use std::convert::TryInto;

        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;
        let scope_name = cx.argument::<JsString>(0)?.value(&mut cx);
        let stream_name = cx.argument::<JsString>(1)?.value(&mut cx);

        let tags = match stream_manager.get_stream_tags(&scope_name, &stream_name) {
            Ok(val) => match val {
                Some(val) => val,
                None => return Ok(cx.empty_array()),
            },
            Err(err) => {
                return cx.throw_error(format!(
                    "Internal error in getting StreamConfiguration: {:?}",
                    err
                ));
            }
        };
        let tags_length: u32 = match tags.len().try_into() {
            Ok(val) => val,
            Err(_err) => return cx.throw_error("Too many tags"),
        };

        let array: Handle<JsArray> = JsArray::new(&mut cx, tags_length);
        for (pos, e) in tags.iter().enumerate() {
            let tag_name = cx.string(e.clone());
            array.set(&mut cx, pos as u32, tag_name)?;
        }
        Ok(array)
    }

    pub fn js_seal_stream(mut cx: FunctionContext) -> JsResult<JsBoolean> {
        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;
        let scope_name = cx.argument::<JsString>(0)?.value(&mut cx);
        let stream_name = cx.argument::<JsString>(1)?.value(&mut cx);

        let scope_result = stream_manager.seal_stream(&scope_name, &stream_name);
        match scope_result {
            Ok(t) => Ok(cx.boolean(t)),
            Err(e) => cx.throw_error(e.to_string()),
        }
    }

    pub fn js_delete_stream(mut cx: FunctionContext) -> JsResult<JsBoolean> {
        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;
        let scope_name = cx.argument::<JsString>(0)?.value(&mut cx);
        let stream_name = cx.argument::<JsString>(1)?.value(&mut cx);

        let scope_result = stream_manager.delete_stream(&scope_name, &stream_name);
        match scope_result {
            Ok(t) => Ok(cx.boolean(t)),
            Err(e) => cx.throw_error(e.to_string()),
        }
    }

    pub fn js_list_streams(mut cx: FunctionContext) -> JsResult<JsPromise> {
        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;
        let scope_name = cx.argument::<JsString>(0)?.value(&mut cx);

        let channel = cx.channel();
        let (deferred, promise) = cx.promise();

        // spawn an `async` task on the tokio runtime.
        use std::sync::Arc;
        let cf = Arc::new(stream_manager.cf.to_async());
        let handle = stream_manager.cf.runtime_handle();
        // enter the runtime context and call spawn with timeout
        let _rt = handle.enter();
        tokio::spawn(time::timeout(
            Duration::from_millis(TIMEOUT_IN_MILLISECONDS),
            async move {
                use futures::stream::StreamExt;
                use pravega_controller_client::paginator::list_streams;

                // expensive block procedure executed in the tokio thread
                let cf = cf.clone();
                let controller = cf.controller_client();
                let scope = list_streams(
                    Scope {
                        name: scope_name.to_string(),
                    },
                    controller,
                );
                let streams = scope.map(|str| str.unwrap()).collect::<Vec<ScopedStream>>().await;

                // notify and execute in the javascript main thread
                deferred.settle_with(&channel, move |mut cx| {
                    use std::convert::TryInto;
                    let streams_length: u32 = match streams.len().try_into() {
                        Ok(val) => val,
                        Err(_err) => return cx.throw_error("Too many streams"),
                    };

                    let array: Handle<JsArray> = JsArray::new(&mut cx, streams_length);
                    for (pos, e) in streams.iter().enumerate() {
                        let stream_name = cx.string(e.stream.name.clone());
                        array.set(&mut cx, pos as u32, stream_name)?;
                    }
                    Ok(array)
                });
            },
        ));
        Ok(promise)
    }

    pub fn js_delete_reader_group(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;
        let scope_name = cx.argument::<JsString>(0)?.value(&mut cx);
        let reader_group_name = cx.argument::<JsString>(1)?.value(&mut cx);

        let delete_result = stream_manager.delete_reader_group(&scope_name, &reader_group_name);

        match delete_result {
            Ok(_) => Ok(cx.undefined()),
            Err(e) => cx.throw_error(e.to_string()),
        }
    }

    pub fn js_create_reader_group(mut cx: FunctionContext) -> JsResult<JsBox<StreamReaderGroup>> {
        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;
        let reader_group_name = cx.argument::<JsString>(0)?.value(&mut cx);
        let scope_name = cx.argument::<JsString>(1)?.value(&mut cx);
        let streams = cx
            .argument::<JsArray>(2)?
            .to_vec(&mut cx)?
            .into_iter()
            .map(|v| {
                v.downcast_or_throw::<JsString, _>(&mut cx)
                    .map(|v| v.value(&mut cx))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let stream_cut = cx.argument::<JsBox<StreamCut>>(3)?;

        let stream_reader_group = stream_manager.create_reader_group(
            &reader_group_name,
            &scope_name,
            streams,
            &stream_cut.stream_cut,
        );

        Ok(cx.boxed(stream_reader_group))
    }

    pub fn js_create_writer(mut cx: FunctionContext) -> JsResult<JsBox<StreamWriter>> {
        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;
        let scope_name = cx.argument::<JsString>(0)?.value(&mut cx);
        let stream_name = cx.argument::<JsString>(1)?.value(&mut cx);

        let stream_writer = stream_manager.create_writer(&scope_name, &stream_name);

        Ok(cx.boxed(stream_writer))
    }

    pub fn js_create_transaction_writer(mut cx: FunctionContext) -> JsResult<JsBox<StreamTxnWriter>> {
        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;
        let scope_name = cx.argument::<JsString>(0)?.value(&mut cx);
        let stream_name = cx.argument::<JsString>(1)?.value(&mut cx);
        let writer_id = cx
            .argument::<JsString>(2)?
            .value(&mut cx)
            .parse::<u128>()
            .unwrap();

        let stream_txn_writer =
            stream_manager.create_transaction_writer(&scope_name, &stream_name, writer_id);

        Ok(cx.boxed(stream_txn_writer))
    }

    pub fn js_to_str(mut cx: FunctionContext) -> JsResult<JsString> {
        let stream_manager = cx.this().downcast_or_throw::<JsBox<StreamManager>, _>(&mut cx)?;

        Ok(cx.string(format!(
            "Controller ip: {:?} ClientConfig: {:?}",
            stream_manager.controller_ip, stream_manager.config
        )))
    }
}
