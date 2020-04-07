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

use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_rust_client_shared::{ScopedSegment, ScopedStream};
use pravega_wire_protocol::client_config::ClientConfig;
use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryImpl};
use pravega_wire_protocol::connection_pool::{ConnectionPool, SegmentConnectionManager};

use crate::raw_client::RawClientImpl;
use crate::segment_reader::AsyncSegmentReaderImpl;
use crate::event_stream_writer::EventStreamWriter;
use crate::setup_logger;

pub struct ClientFactory {
    connection_pool: ConnectionPool<SegmentConnectionManager>,
    controller_client: ControllerClientImpl,
}

impl ClientFactory {

    pub fn new(config: ClientConfig) -> ClientFactory {
        setup_logger().expect("setting up logger");
        let cf = Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>;
        let controller_uri = config.controller_uri.clone();
        let pool = ConnectionPool::new(SegmentConnectionManager::new(cf, config));
        let controller = ControllerClientImpl::new(controller_uri);
        ClientFactory {
            connection_pool: pool,
            controller_client: controller,
        }
    }

    pub(crate) fn create_raw_client(&self, endpoint: SocketAddr) -> RawClientImpl {
        RawClientImpl::new(&self.connection_pool, endpoint)
    }

    pub(crate) async fn create_async_event_reader<'a>(&'a self, segment: ScopedSegment) -> AsyncSegmentReaderImpl<'a> {
        AsyncSegmentReaderImpl::init(segment, &self.connection_pool, &self.controller_client).await
    }

    pub(crate) async fn create_event_stream_writer(stream: ScopedStream, config: ClientConfig) -> EventStreamWriter {
        EventStreamWriter::new(stream, config).await
    }

    pub(crate) fn get_connection_pool(&self) -> &ConnectionPool<SegmentConnectionManager> {
        &self.connection_pool
    }

    pub(crate) fn get_controller_client(&self) -> &dyn ControllerClient {
        &self.controller_client
    }
}
