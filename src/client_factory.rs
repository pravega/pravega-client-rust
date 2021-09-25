//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
#![allow(bare_trait_objects)]

//! Factory to create components in Pravega Rust client.
//!
//! Applications should always use this ClientFactory to initialize components.
//!
use crate::byte::reader::ByteReader;
use crate::byte::writer::ByteWriter;
use crate::event::reader_group::{ReaderGroup, ReaderGroupConfigBuilder};
use crate::event::transactional_writer::TransactionalEventWriter;
use crate::event::writer::EventWriter;
use crate::segment::metadata::SegmentMetadataClient;
use crate::segment::raw_client::RawClientImpl;
use crate::segment::reader::AsyncSegmentReaderImpl;
use crate::sync::synchronizer::Synchronizer;
use crate::sync::table::Table;
cfg_if::cfg_if! {
    if #[cfg(feature = "integration-test")] {
        use crate::test_utils::{RawClientWrapper, SegmentReaderWrapper};
    }
}

use crate::index::Fields;
use pravega_client_auth::DelegationTokenProvider;
use pravega_client_config::ClientConfig;
use pravega_client_shared::{DelegationToken, PravegaNodeUri, Scope, ScopedSegment, ScopedStream, WriterId};
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::mock_controller::MockController;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_wire_protocol::connection_factory::{
    ConnectionFactory, ConnectionFactoryConfig, SegmentConnectionManager,
};

use crate::index::{IndexReader, IndexWriter};
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};
use tracing::info;

/// Applications should use ClientFactory to create resources they need.
///
/// ClientFactory contains a connection pool that is shared by all the readers and writers it creates.
/// It also contains a tokio runtime that is used to drive async tasks. Spawned tasks in readers and
/// writers are tied to this runtime.
///
/// Note that dropping Runtime in async context is not a good practice and it will have warning messages.
/// ClientFactory is the only place that's holding the Runtime, so it should not be used in any async contexts.
/// You can use ['ClientFactoryAsync'] in async contexts instead.
///
/// # Examples
/// ```no_run
/// use pravega_client_config::ClientConfigBuilder;
/// use pravega_client::client_factory::ClientFactory;
///
/// fn main() {
///    let config = ClientConfigBuilder::default()
///         .controller_uri("localhost:8000")
///         .build()
///         .expect("create config");
///     let client_factory = ClientFactory::new(config);
/// }
/// ```
/// ```no_run
/// use pravega_client_config::ClientConfigBuilder;
/// use pravega_client::client_factory::ClientFactoryAsync;
/// use tokio::runtime::Handle;
///
/// #[tokio::main]
/// async fn main() {
///    let config = ClientConfigBuilder::default()
///         .controller_uri("localhost:8000")
///         .build()
///         .expect("create config");
///     let handle = Handle::try_current().expect("get current runtime handle");
///     let client_factory = ClientFactoryAsync::new(config, handle);
/// }
/// ```
/// [`ClientFactoryAsync`]: ClientFactoryAsync
pub struct ClientFactory {
    runtime: Runtime,
    client_factory_async: ClientFactoryAsync,
}

impl ClientFactory {
    pub fn new(config: ClientConfig) -> ClientFactory {
        let rt = tokio::runtime::Runtime::new().expect("create runtime");
        ClientFactory::new_with_runtime(config, rt)
    }

    pub fn new_with_runtime(config: ClientConfig, rt: Runtime) -> ClientFactory {
        let async_factory = ClientFactoryAsync::new(config, rt.handle().clone());
        ClientFactory {
            runtime: rt,
            client_factory_async: async_factory,
        }
    }

    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    pub fn runtime_handle(&self) -> Handle {
        self.runtime.handle().clone()
    }

    pub fn config(&self) -> &ClientConfig {
        self.client_factory_async.config()
    }

    pub fn controller_client(&self) -> &dyn ControllerClient {
        self.client_factory_async.controller_client()
    }

    pub fn create_event_writer(&self, stream: ScopedStream) -> EventWriter {
        self.client_factory_async.create_event_writer(stream)
    }

    pub async fn create_reader_group(&self, reader_group_name: String, stream: ScopedStream) -> ReaderGroup {
        info!(
            "Creating reader group {:?} to read data from stream {:?}",
            reader_group_name, stream
        );
        self.client_factory_async
            .create_reader_group(reader_group_name, stream)
            .await
    }

    pub async fn create_transactional_event_writer(
        &self,
        stream: ScopedStream,
        writer_id: WriterId,
    ) -> TransactionalEventWriter {
        self.client_factory_async
            .create_transactional_event_writer(stream, writer_id)
            .await
    }

    pub async fn create_byte_writer(&self, stream: ScopedStream) -> ByteWriter {
        self.client_factory_async.create_byte_writer(stream).await
    }

    pub async fn create_byte_reader(&self, stream: ScopedStream) -> ByteReader {
        self.client_factory_async.create_byte_reader(stream).await
    }

    pub async fn create_index_writer<T: Fields + PartialOrd + PartialEq + Debug>(
        &self,
        stream: ScopedStream,
    ) -> IndexWriter<T> {
        self.client_factory_async.create_index_writer(stream).await
    }

    pub async fn create_index_reader(&self, stream: ScopedStream) -> IndexReader {
        self.client_factory_async.create_index_reader(stream).await
    }

    pub async fn create_table(&self, scope: Scope, name: String) -> Table {
        self.client_factory_async.create_table(scope, name).await
    }

    pub async fn create_synchronizer(&self, scope: Scope, name: String) -> Synchronizer {
        self.client_factory_async.create_synchronizer(scope, name).await
    }

    pub fn to_async(&self) -> ClientFactoryAsync {
        self.client_factory_async.clone()
    }

    pub(crate) async fn create_async_segment_reader(&self, segment: ScopedSegment) -> AsyncSegmentReaderImpl {
        self.client_factory_async
            .create_async_segment_reader(segment)
            .await
    }

    pub(crate) async fn create_raw_client(&self, segment: &ScopedSegment) -> RawClientImpl<'_> {
        self.client_factory_async.create_raw_client(segment).await
    }

    pub(crate) fn create_raw_client_for_endpoint(&self, endpoint: PravegaNodeUri) -> RawClientImpl<'_> {
        self.client_factory_async.create_raw_client_for_endpoint(endpoint)
    }

    pub(crate) async fn create_delegation_token_provider(
        &self,
        stream: ScopedStream,
    ) -> DelegationTokenProvider {
        self.client_factory_async
            .create_delegation_token_provider(stream)
            .await
    }

    pub(crate) async fn create_segment_metadata_client(
        &self,
        segment: ScopedSegment,
    ) -> SegmentMetadataClient {
        self.client_factory_async
            .create_segment_metadata_client(segment)
            .await
    }

    #[doc(hidden)]
    #[cfg(feature = "integration-test")]
    pub async fn create_raw_client_wrapper(&self, segment: &ScopedSegment) -> RawClientWrapper<'_> {
        let endpoint = self
            .client_factory_async
            .controller_client
            .get_endpoint_for_segment(segment)
            .await
            .expect("get endpoint for segment");
        RawClientWrapper::new(
            &self.client_factory_async.connection_pool,
            endpoint,
            self.client_factory_async.config.request_timeout,
        )
    }

    #[doc(hidden)]
    #[cfg(feature = "integration-test")]
    pub async fn create_segment_reader_wrapper(&self, segment: ScopedSegment) -> SegmentReaderWrapper {
        SegmentReaderWrapper::new(
            segment.clone(),
            self.to_async(),
            self.client_factory_async
                .create_delegation_token_provider(ScopedStream::from(&segment))
                .await,
        )
        .await
    }
}

#[derive(Clone)]
pub struct ClientFactoryAsync {
    connection_pool: Arc<ConnectionPool<SegmentConnectionManager>>,
    controller_client: Arc<Box<dyn ControllerClient>>,
    config: Arc<ClientConfig>,
    runtime_handle: Handle,
}

impl ClientFactoryAsync {
    pub fn new(config: ClientConfig, handle: Handle) -> Self {
        let cf = ConnectionFactory::create(ConnectionFactoryConfig::from(&config));
        let pool = ConnectionPool::new(SegmentConnectionManager::new(cf, config.max_connections_in_pool));
        let controller = if config.mock {
            Box::new(MockController::new(config.controller_uri.clone())) as Box<dyn ControllerClient>
        } else {
            Box::new(ControllerClientImpl::new(config.clone(), &handle)) as Box<dyn ControllerClient>
        };
        ClientFactoryAsync {
            connection_pool: Arc::new(pool),
            controller_client: Arc::new(controller),
            config: Arc::new(config),
            runtime_handle: handle,
        }
    }
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub fn create_event_writer(&self, stream: ScopedStream) -> EventWriter {
        EventWriter::new(stream, self.clone())
    }

    pub async fn create_reader_group(&self, reader_group_name: String, stream: ScopedStream) -> ReaderGroup {
        let scope = stream.scope.clone();
        let rg_config = ReaderGroupConfigBuilder::default().add_stream(stream).build();
        ReaderGroup::create(scope, reader_group_name, rg_config, self.clone()).await
    }

    pub async fn create_transactional_event_writer(
        &self,
        stream: ScopedStream,
        writer_id: WriterId,
    ) -> TransactionalEventWriter {
        TransactionalEventWriter::new(stream, writer_id, self.clone()).await
    }

    pub async fn create_byte_writer(&self, stream: ScopedStream) -> ByteWriter {
        ByteWriter::new(stream, self.clone()).await
    }

    pub async fn create_byte_reader(&self, stream: ScopedStream) -> ByteReader {
        ByteReader::new(stream, self.clone(), self.config().reader_wrapper_buffer_size()).await
    }

    pub async fn create_index_writer<T: Fields + PartialOrd + PartialEq + Debug>(
        &self,
        stream: ScopedStream,
    ) -> IndexWriter<T> {
        IndexWriter::new(self.clone(), stream).await
    }

    pub async fn create_index_reader(&self, stream: ScopedStream) -> IndexReader {
        IndexReader::new(self.clone(), stream).await
    }

    pub async fn create_table(&self, scope: Scope, name: String) -> Table {
        Table::new(scope, name, self.clone())
            .await
            .expect("Failed to create Table map")
    }

    pub async fn create_synchronizer(&self, scope: Scope, name: String) -> Synchronizer {
        Synchronizer::new(scope, name, self.clone()).await
    }

    pub fn controller_client(&self) -> &dyn ControllerClient {
        &**self.controller_client
    }

    pub fn runtime_handle(&self) -> Handle {
        self.runtime_handle.clone()
    }

    pub(crate) async fn create_async_segment_reader(&self, segment: ScopedSegment) -> AsyncSegmentReaderImpl {
        AsyncSegmentReaderImpl::new(
            segment.clone(),
            self.clone(),
            self.create_delegation_token_provider(ScopedStream::from(&segment))
                .await,
        )
        .await
    }

    pub(crate) async fn create_raw_client(&self, segment: &ScopedSegment) -> RawClientImpl<'_> {
        let endpoint = self
            .controller_client
            .get_endpoint_for_segment(segment)
            .await
            .expect("get endpoint for segment");
        RawClientImpl::new(&self.connection_pool, endpoint, self.config.request_timeout)
    }

    pub(crate) fn create_raw_client_for_endpoint(&self, endpoint: PravegaNodeUri) -> RawClientImpl<'_> {
        RawClientImpl::new(&self.connection_pool, endpoint, self.config.request_timeout)
    }

    pub(crate) async fn create_segment_metadata_client(
        &self,
        segment: ScopedSegment,
    ) -> SegmentMetadataClient {
        SegmentMetadataClient::new(segment, self.clone()).await
    }

    pub(crate) async fn create_delegation_token_provider(
        &self,
        stream: ScopedStream,
    ) -> DelegationTokenProvider {
        let token_provider = DelegationTokenProvider::new(stream);
        if !self.config.is_auth_enabled {
            let empty_token = DelegationToken::new("".to_string(), None);
            token_provider.populate(empty_token).await;
        }
        token_provider
    }

    pub(crate) fn get_connection_pool(&self) -> &ConnectionPool<SegmentConnectionManager> {
        &self.connection_pool
    }
}

impl fmt::Debug for ClientFactoryAsync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientFactoryInternal")
            .field("connection pool", &self.connection_pool)
            .field("client config,", &self.config)
            .finish()
    }
}
