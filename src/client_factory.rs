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
use pravega_wire_protocol::client_config::ClientConfig;
use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryImpl};
use pravega_wire_protocol::connection_pool::{ConnectionPool, SegmentConnectionManager};

use crate::raw_client::{RawClient, RawClientImpl};

pub struct ClientFactory {
    connection_pool: ConnectionPool<SegmentConnectionManager>,
    controller_client: ControllerClientImpl,
}

impl ClientFactory {

    pub fn new(config: ClientConfig) -> ClientFactory {
        let cf = Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>;
        let controller_uri = config.controller_uri.clone();
        let pool = ConnectionPool::new(SegmentConnectionManager::new(cf, config));
        let controller = ControllerClientImpl::new(controller_uri);
        ClientFactory {
            connection_pool: pool,
            controller_client: controller,
        }
    }

    pub(crate) fn get_raw_client<'a>(&'a self, endpoint: SocketAddr) -> Box<dyn RawClient<'a> + 'a> {
        RawClientImpl::new(&self.connection_pool, endpoint)
    }

    pub(crate) fn get_connection_pool(&self) -> &ConnectionPool<SegmentConnectionManager> {
        &self.connection_pool
    }

    pub(crate) fn get_controller_client(&self) -> &dyn ControllerClient {
        &self.controller_client
    }
}
