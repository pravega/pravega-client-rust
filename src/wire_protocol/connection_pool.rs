//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

extern crate r2d2;
extern crate chashmap;
use snafu::{ResultExt, Snafu};
use async_trait::async_trait;
use crate::wire_protocol::connection_factory::{ConnectionType, ConnectionFactory, ConnectionFactoryImpl, Connection, ConnectionFactoryError};
use crate::wire_protocol::client_config;
use std::net::SocketAddr;
use self::r2d2::PooledConnection;
use tokio::runtime::Runtime;

#[derive(Debug, Snafu)]
pub enum ConnectionPoolError {
    #[snafu(display("Could not connect to endpoint"))]
    Connect {
        source: ConnectionFactoryError,
    },
}

type Result<T, E = ConnectionPoolError> = std::result::Result<T, E>;

#[async_trait]
pub trait ConnectionPool {
    async fn get_connection(&self, endpoint: SocketAddr, connection_type: Option<ConnectionType>) -> Result<PooledConnection<PravegaConnectionManager>>;
}

pub struct ConnectionPoolImpl {
    map: chashmap::CHashMap<SocketAddr, r2d2::Pool<PravegaConnectionManager>>,
    config: client_config::ClientConfig,
}

#[async_trait]
impl ConnectionPool for ConnectionPoolImpl {
    async fn get_connection(&self, endpoint: SocketAddr, connection_type: Option<ConnectionType>) -> Result<PooledConnection<PravegaConnectionManager>> {
        if !self.map.contains_key(&endpoint)  {
            let manager = PravegaConnectionManager::new(endpoint, connection_type);
            let pool = r2d2::Pool::builder().max_size(12).build(*manager).unwrap();
            self.map.insert(endpoint, pool);
        }

        Ok(self.map.get(&endpoint).unwrap().get().unwrap())
    }
}

pub struct PravegaConnectionManager {
    connection_factory: Box<dyn ConnectionFactory + Send + Sync>,
    connection_type: Option<ConnectionType>,
    endpoint: SocketAddr,
}

impl PravegaConnectionManager {
    pub fn new(endpoint: SocketAddr, connection_type: Option<ConnectionType>) -> Box<PravegaConnectionManager> {
        let connection_factory = Box::new(ConnectionFactoryImpl{});
        Box::new(PravegaConnectionManager {connection_factory, endpoint, connection_type })
    }
}

impl r2d2::ManageConnection for PravegaConnectionManager {
    type Connection = Box<dyn Connection>;
    type Error = ConnectionPoolError;

    fn connect(&self) -> Result<Box<dyn Connection>, ConnectionPoolError> {
        let mut connection_future = self.connection_factory.establish_connection(self.endpoint, &self.connection_type.as_ref());
        let mut rt = Runtime::new().unwrap();
        rt.block_on(connection_future).context(Connect{})
    }

    fn is_valid(&self, conn: &mut Box<dyn Connection>) -> Result<(), ConnectionPoolError> {
        Ok(())
    }

    fn has_broken(&self, _: &mut Box<dyn Connection>) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
}