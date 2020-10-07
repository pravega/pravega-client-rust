//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::mock_controller::MockController;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_rust_client_config::ClientConfig;
use pravega_rust_client_shared::{DelegationToken, PravegaNodeUri, ScopedSegment, ScopedStream, WriterId};
use pravega_wire_protocol::connection_factory::{
    ConnectionFactory, ConnectionFactoryConfig, SegmentConnectionManager,
};

use crate::byte_stream::{ByteStreamReader, ByteStreamWriter};
use crate::event_reader::EventReader;
use crate::event_stream_writer::EventStreamWriter;
use crate::raw_client::RawClientImpl;
use crate::segment_metadata::SegmentMetadataClient;
use crate::segment_reader::AsyncSegmentReaderImpl;
use crate::table_synchronizer::TableSynchronizer;
use crate::tablemap::TableMap;
use crate::transaction::transactional_event_stream_writer::TransactionalEventStreamWriter;
use pravega_rust_client_auth::DelegationTokenProvider;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};

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
            Box::new(ControllerClientImpl::new(config.clone(), rt.handle().clone()))
                as Box<dyn ControllerClient>
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
    pub fn get_runtime_handle(&self) -> Handle {
        self.0.get_runtime_handle()
    }

    pub async fn create_async_event_reader(&self, segment: ScopedSegment) -> AsyncSegmentReaderImpl {
        AsyncSegmentReaderImpl::init(
            segment.clone(),
            self.clone(),
            self.0
                .create_delegation_token_provider(ScopedStream::from(&segment))
                .await,
        )
        .await
    }

    pub async fn create_table_map(&self, name: String) -> TableMap {
        TableMap::new(name, self.clone())
            .await
            .expect("Failed to create Table map")
    }

    pub async fn create_table_synchronizer(&self, name: String) -> TableSynchronizer {
        TableSynchronizer::new(name, self.clone()).await
    }

    pub async fn create_raw_client(&self, segment: &ScopedSegment) -> RawClientImpl<'_> {
        let endpoint = self
            .0
            .controller_client
            .get_endpoint_for_segment(segment)
            .await
            .expect("get endpoint for segment");
        self.0.create_raw_client(endpoint)
    }

    pub fn create_raw_client_for_endpoint(&self, endpoint: PravegaNodeUri) -> RawClientImpl<'_> {
        self.0.create_raw_client(endpoint)
    }

    pub fn create_event_stream_writer(&self, stream: ScopedStream) -> EventStreamWriter {
        EventStreamWriter::new(stream, self.0.config.clone(), self.clone())
    }

    pub async fn create_event_stream_reader(&self, stream: ScopedStream) -> EventReader {
        EventReader::init(stream, self.clone()).await
    }

    pub async fn create_transactional_event_stream_writer(
        &self,
        stream: ScopedStream,
        writer_id: WriterId,
    ) -> TransactionalEventStreamWriter {
        TransactionalEventStreamWriter::new(stream, writer_id, self.clone(), self.0.config.clone()).await
    }

    pub fn create_byte_stream_writer(&self, segment: ScopedSegment) -> ByteStreamWriter {
        ByteStreamWriter::new(segment, self.0.config.clone(), self.clone())
    }

    pub fn create_byte_stream_reader(&self, segment: ScopedSegment) -> ByteStreamReader {
        ByteStreamReader::new(segment, self)
    }

    pub async fn create_delegation_token_provider(&self, stream: ScopedStream) -> DelegationTokenProvider {
        self.0.create_delegation_token_provider(stream).await
    }

    pub async fn create_segment_metadata_client(&self, segment: ScopedSegment) -> SegmentMetadataClient {
        SegmentMetadataClient::new(
            segment.clone(),
            self.clone(),
            self.0
                .create_delegation_token_provider(ScopedStream::from(&segment))
                .await,
        )
    }

    pub fn get_controller_client(&self) -> &dyn ControllerClient {
        self.0.get_controller_client()
    }

    pub fn get_config(&self) -> &ClientConfig {
        &self.0.config
    }
}

impl ClientFactoryInternal {
    pub(crate) fn create_raw_client(&self, endpoint: PravegaNodeUri) -> RawClientImpl {
        RawClientImpl::new(&self.connection_pool, endpoint)
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
    /// Get the Runtime handle. The Handle is internally reference counted and can be cloned.
    ///
    pub(crate) fn get_runtime_handle(&self) -> Handle {
        self.runtime.handle().clone()
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
