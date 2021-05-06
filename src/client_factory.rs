//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

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

use pravega_client_auth::DelegationTokenProvider;
use pravega_client_config::ClientConfig;
use pravega_client_shared::{DelegationToken, PravegaNodeUri, Scope, ScopedSegment, ScopedStream, WriterId};
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::mock_controller::MockController;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_wire_protocol::connection_factory::{
    ConnectionFactory, ConnectionFactoryConfig, SegmentConnectionManager,
};

use std::fmt;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tracing::info;

/// Applications should use ClientFactory to create resources they need.
///
/// ClientFactory contains a connection pool that is shared by all the readers and writers it creates.
/// It also contains a tokio runtime that is used to drive for async tasks. Spawned tasks in readers and
/// writers are tied to this runtime.
///
/// Applications can call clone on ClientFactory in case ownership is needed. It holds an Arc to the
/// internal object so cloned objects are still sharing the same connection pool and runtime.
///
#[derive(Clone)]
pub struct ClientFactory(Arc<ClientFactoryInternal>);

struct ClientFactoryInternal {
    connection_pool: ConnectionPool<SegmentConnectionManager>,
    controller_client: Box<dyn ControllerClient>,
    config: ClientConfig,
    runtime: Runtime,
}

impl ClientFactory {
    /// Create a new ClientFactory.
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
    pub fn new(config: ClientConfig) -> ClientFactory {
        let rt = tokio::runtime::Runtime::new().expect("create runtime");
        let cf = ConnectionFactory::create(ConnectionFactoryConfig::from(&config));
        let pool = ConnectionPool::new(SegmentConnectionManager::new(cf, config.max_connections_in_pool));
        let controller = if config.mock {
            Box::new(MockController::new(config.controller_uri.clone())) as Box<dyn ControllerClient>
        } else {
            Box::new(ControllerClientImpl::new(config.clone(), &rt)) as Box<dyn ControllerClient>
        };
        ClientFactory(Arc::new(ClientFactoryInternal {
            connection_pool: pool,
            controller_client: controller,
            config,
            runtime: rt,
        }))
    }

    pub fn runtime(&self) -> &Runtime {
        self.0.runtime()
    }

    pub fn config(&self) -> &ClientConfig {
        &self.0.config
    }

    pub fn controller_client(&self) -> &dyn ControllerClient {
        self.0.controller_client()
    }

    pub fn create_event_writer(&self, stream: ScopedStream) -> EventWriter {
        EventWriter::new(stream, self.clone())
    }

    pub async fn create_reader_group(
        &self,
        scope: Scope,
        reader_group_name: String,
        stream: ScopedStream,
    ) -> ReaderGroup {
        info!(
            "Creating reader group {:?} to read data from stream {:?}",
            reader_group_name, stream
        );
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

    pub fn create_byte_writer(&self, segment: ScopedSegment) -> ByteWriter {
        ByteWriter::new(segment, self.clone())
    }

    pub fn create_byte_reader(&self, segment: ScopedSegment) -> ByteReader {
        ByteReader::new(segment, self.clone(), self.config().reader_wrapper_buffer_size())
    }

    pub async fn create_table(&self, scope: Scope, name: String) -> Table {
        Table::new(scope, name, self.clone())
            .await
            .expect("Failed to create Table map")
    }

    pub async fn create_synchronizer(&self, scope: Scope, name: String) -> Synchronizer {
        Synchronizer::new(scope, name, self.clone()).await
    }

    pub(crate) async fn create_async_event_reader(&self, segment: ScopedSegment) -> AsyncSegmentReaderImpl {
        AsyncSegmentReaderImpl::new(
            segment.clone(),
            self.clone(),
            self.0
                .create_delegation_token_provider(ScopedStream::from(&segment))
                .await,
        )
        .await
    }

    pub(crate) async fn create_raw_client(&self, segment: &ScopedSegment) -> RawClientImpl<'_> {
        let endpoint = self
            .0
            .controller_client
            .get_endpoint_for_segment(segment)
            .await
            .expect("get endpoint for segment");
        self.0.create_raw_client(endpoint)
    }

    pub(crate) fn create_raw_client_for_endpoint(&self, endpoint: PravegaNodeUri) -> RawClientImpl<'_> {
        self.0.create_raw_client(endpoint)
    }

    pub(crate) async fn create_delegation_token_provider(
        &self,
        stream: ScopedStream,
    ) -> DelegationTokenProvider {
        self.0.create_delegation_token_provider(stream).await
    }

    pub(crate) async fn create_segment_metadata_client(
        &self,
        segment: ScopedSegment,
    ) -> SegmentMetadataClient {
        SegmentMetadataClient::new(segment.clone(), self.clone()).await
    }

    #[doc(hidden)]
    #[cfg(feature = "integration-test")]
    pub async fn create_raw_client_wrapper(&self, segment: &ScopedSegment) -> RawClientWrapper<'_> {
        let endpoint = self
            .0
            .controller_client
            .get_endpoint_for_segment(segment)
            .await
            .expect("get endpoint for segment");
        RawClientWrapper::new(&self.0.connection_pool, endpoint, self.0.config.request_timeout)
    }

    #[doc(hidden)]
    #[cfg(feature = "integration-test")]
    pub async fn create_segment_reader_wrapper(&self, segment: ScopedSegment) -> SegmentReaderWrapper {
        SegmentReaderWrapper::new(
            segment.clone(),
            self.clone(),
            self.0
                .create_delegation_token_provider(ScopedStream::from(&segment))
                .await,
        )
        .await
    }
}

impl ClientFactoryInternal {
    pub(crate) fn create_raw_client(&self, endpoint: PravegaNodeUri) -> RawClientImpl {
        RawClientImpl::new(&self.connection_pool, endpoint, self.config.request_timeout)
    }

    pub(crate) async fn create_delegation_token_provider(
        &self,
        stream: ScopedStream,
    ) -> DelegationTokenProvider {
        let token_provider = DelegationTokenProvider::new(stream);
        if self.config.is_auth_enabled {
            token_provider
        } else {
            let empty_token = DelegationToken::new("".to_string(), None);
            token_provider.populate(empty_token).await;
            token_provider
        }
    }

    pub(crate) fn get_connection_pool(&self) -> &ConnectionPool<SegmentConnectionManager> {
        &self.connection_pool
    }

    pub(crate) fn controller_client(&self) -> &dyn ControllerClient {
        &*self.controller_client
    }

    // borrow the Runtime.
    pub(crate) fn runtime(&self) -> &Runtime {
        &self.runtime
    }
}

impl fmt::Debug for ClientFactoryInternal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientFactoryInternal")
            .field("connection pool", &self.connection_pool)
            .field("client config,", &self.config)
            .finish()
    }
}
