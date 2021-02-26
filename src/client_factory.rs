//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_client_config::ClientConfig;
use pravega_client_shared::{DelegationToken, PravegaNodeUri, Scope, ScopedSegment, ScopedStream, WriterId};
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::mock_controller::MockController;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_wire_protocol::connection_factory::{
    ConnectionFactory, ConnectionFactoryConfig, SegmentConnectionManager,
};

use crate::byte_stream::{ByteStreamReader, ByteStreamWriter};
use crate::event_reader_group::ReaderGroup;
use crate::event_stream_writer::EventStreamWriter;
use crate::raw_client::RawClientImpl;
use crate::reader_group_config::{ReaderGroupConfig, ReaderGroupConfigBuilder};
use crate::segment_metadata::SegmentMetadataClient;
use crate::segment_reader::AsyncSegmentReaderImpl;
use crate::table_synchronizer::TableSynchronizer;
use crate::tablemap::TableMap;
use crate::transaction::transactional_event_stream_writer::TransactionalEventStreamWriter;
use pravega_client_auth::DelegationTokenProvider;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tracing::info;

#[derive(Clone)]
pub struct ClientFactory(Arc<ClientFactoryInternal>);

struct ClientFactoryInternal {
    connection_pool: ConnectionPool<SegmentConnectionManager>,
    controller_client: Box<dyn ControllerClient>,
    config: ClientConfig,
    runtime: Runtime,
}

impl ClientFactory {
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

    ///
    /// Get the Runtime handle.
    ///
    pub fn get_runtime(&self) -> &Runtime {
        self.0.get_runtime()
    }

    pub async fn create_async_event_reader(&self, segment: ScopedSegment) -> AsyncSegmentReaderImpl {
        AsyncSegmentReaderImpl::init(
            segment.clone(),
            self.clone(),
            self.0
                .create_delegation_token_provider(ScopedStream::from(&segment))
                .await,
            true,
        )
        .await
    }

    pub async fn create_table_map(&self, scope: Scope, name: String) -> TableMap {
        TableMap::new(scope, name, self.clone())
            .await
            .expect("Failed to create Table map")
    }

    pub async fn create_table_synchronizer(&self, scope: Scope, name: String) -> TableSynchronizer {
        TableSynchronizer::new(scope, name, self.clone()).await
    }

    pub async fn create_raw_client(&self, segment: &ScopedSegment) -> RawClientImpl<'_> {
        let endpoint = self
            .0
            .controller_client
            .get_endpoint_for_segment(segment)
            .await
            .expect("get endpoint for segment");
        self.0.create_raw_client(endpoint, true)
    }

    pub fn create_raw_client_for_endpoint(&self, endpoint: PravegaNodeUri) -> RawClientImpl<'_> {
        self.0.create_raw_client(endpoint, true)
    }

    pub fn create_raw_client_for_endpoint_without_recycling_connection(
        &self,
        endpoint: PravegaNodeUri,
    ) -> RawClientImpl<'_> {
        self.0.create_raw_client(endpoint, false)
    }

    pub fn create_event_stream_writer(&self, stream: ScopedStream) -> EventStreamWriter {
        EventStreamWriter::new(stream, self.clone())
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

    pub async fn create_reader_group_with_config(
        &self,
        scope: Scope,
        reader_group_name: String,
        rg_config: ReaderGroupConfig,
    ) -> ReaderGroup {
        info!("Creating reader group {:?} ", reader_group_name);
        ReaderGroup::create(scope, reader_group_name, rg_config, self.clone()).await
    }

    pub async fn create_transactional_event_stream_writer(
        &self,
        stream: ScopedStream,
        writer_id: WriterId,
    ) -> TransactionalEventStreamWriter {
        TransactionalEventStreamWriter::new(stream, writer_id, self.clone()).await
    }

    pub fn create_byte_stream_writer(&self, segment: ScopedSegment) -> ByteStreamWriter {
        ByteStreamWriter::new(segment, self.clone())
    }

    pub fn create_byte_stream_reader(&self, segment: ScopedSegment) -> ByteStreamReader {
        ByteStreamReader::new(
            segment,
            self.clone(),
            self.get_config().reader_wrapper_buffer_size(),
        )
    }

    pub async fn create_delegation_token_provider(&self, stream: ScopedStream) -> DelegationTokenProvider {
        self.0.create_delegation_token_provider(stream).await
    }

    pub async fn create_segment_metadata_client(&self, segment: ScopedSegment) -> SegmentMetadataClient {
        SegmentMetadataClient::new(segment.clone(), self.clone()).await
    }

    pub fn get_controller_client(&self) -> &dyn ControllerClient {
        self.0.get_controller_client()
    }

    pub fn get_config(&self) -> &ClientConfig {
        &self.0.config
    }
}

impl ClientFactoryInternal {
    pub(crate) fn create_raw_client(
        &self,
        endpoint: PravegaNodeUri,
        recycle_connection: bool,
    ) -> RawClientImpl {
        RawClientImpl::new(
            &self.connection_pool,
            endpoint,
            self.config.request_timeout,
            recycle_connection,
        )
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

    pub(crate) fn get_controller_client(&self) -> &dyn ControllerClient {
        &*self.controller_client
    }

    ///
    /// borrow the Runtime.
    ///
    pub(crate) fn get_runtime(&self) -> &Runtime {
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
