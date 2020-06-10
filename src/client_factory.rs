//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use std::net::SocketAddr;

use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::mock_controller::MockController;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_rust_client_shared::{ScopedSegment, ScopedStream, WriterId};
use pravega_wire_protocol::client_config::ClientConfig;
use pravega_wire_protocol::connection_factory::{ConnectionFactory, SegmentConnectionManager};

use crate::event_stream_writer::EventStreamWriter;
use crate::raw_client::RawClientImpl;
use crate::segment_reader::AsyncSegmentReaderImpl;
use crate::setup_logger;
use crate::table_synchronizer::TableSynchronizer;
use crate::tablemap::TableMap;
use crate::transaction::transactional_event_stream_writer::TransactionalEventStreamWriter;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};

pub struct ClientFactory(Arc<ClientFactoryInternal>);

pub struct ClientFactoryInternal {
    connection_pool: ConnectionPool<SegmentConnectionManager>,
    controller_client: Box<dyn ControllerClient>,
    config: ClientConfig,
    runtime: Runtime,
}

impl ClientFactory {
    pub fn new(config: ClientConfig) -> ClientFactory {
        let _ = setup_logger(); //Ignore failure
        let rt = tokio::runtime::Runtime::new().expect("create runtime");
        let cf = ConnectionFactory::create(config.connection_type);
        let pool = ConnectionPool::new(SegmentConnectionManager::new(cf, config.max_connections_in_pool));
        let controller = if config.mock {
            Box::new(MockController::new(config.controller_uri)) as Box<dyn ControllerClient>
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

    #[allow(clippy::needless_lifetimes)] //Normally the compiler could infer lifetimes but async is throwing it for a loop.
    pub async fn create_async_event_reader<'a>(
        &'a self,
        segment: ScopedSegment,
    ) -> AsyncSegmentReaderImpl<'a> {
        AsyncSegmentReaderImpl::init(segment, &self.0).await
    }

    #[allow(clippy::needless_lifetimes)] //Normally the compiler could infer lifetimes but async is throwing it for a loop.
    pub async fn create_table_map<'a>(&'a self, name: String) -> TableMap<'a> {
        TableMap::new(name, &self.0)
            .await
            .expect("Failed to create Table map")
    }
    #[allow(clippy::needless_lifetimes)]
    pub async fn create_table_synchronizer<'a, K>(&'a self, name: String) -> TableSynchronizer<'a, K>
    where
        K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
    {
        TableSynchronizer::new(name, &self.0).await
    }

    pub async fn create_raw_client(&self, segment: &ScopedSegment) -> RawClientImpl<'_> {
        let endpoint = self
            .0
            .controller_client
            .get_endpoint_for_segment(segment)
            .await
            .expect("get endpoint for segment")
            .parse::<SocketAddr>()
            .expect("convert to socketaddr");
        self.0.create_raw_client(endpoint)
    }

    pub fn create_event_stream_writer(&self, stream: ScopedStream) -> EventStreamWriter {
        EventStreamWriter::new(stream, self.0.config.clone(), self.0.clone())
    }

    pub async fn create_transactional_event_stream_writer(
        &self,
        stream: ScopedStream,
        writer_id: WriterId,
    ) -> TransactionalEventStreamWriter {
        TransactionalEventStreamWriter::new(stream, writer_id, self.0.clone(), self.0.config.clone()).await
    }

    pub fn get_controller_client(&self) -> &dyn ControllerClient {
        &*self.0.controller_client
    }
}

impl ClientFactoryInternal {
    pub(crate) fn create_raw_client(&self, endpoint: SocketAddr) -> RawClientImpl {
        RawClientImpl::new(&self.connection_pool, endpoint)
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
