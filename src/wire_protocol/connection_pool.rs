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
use crate::wire_protocol::connection_factory::{ConnectionType, ConnectionFactory, ConnectionFactoryImpl,Connection, Error};
use std::net::SocketAddr;


pub enum ConnectionPoolError {

}

type Result<T, E = ConnectionPoolError> = std::result::Result<T, E>;

#[async_trait]
pub trait ConnectionPool {
    async fn get_connection(&self, endpoint: SocketAddr, connection_type: Option<ConnectionType>) -> Result<dyn Connection>;
}

#[derive(Debug)]
pub struct ConnectionPoolImpl {
    pub map: chashmap<SocketAddr, r2d2::Pool<PravegaConnectionManager>>,
}

impl ConnectionPool for ConnectionPoolImpl {
    async fn get_connection(&self, endpoint: SocketAddr, connection_type: Option<ConnectionType>) -> Result<dyn Connection> {
        if !self.map.contains_key(&endpoint)  {
            let manager = PravegaConnectionManager::new(endpoint, connection_type);
            let pool = r2d2::Pool::builder().max_size(15).build(manager).unwrap();
            self.map.insert(endpoint, pool);
        }

        self.map.get(&endpoint).unwrap().get().unwrap()
    }
}

#[derive(Debug)]
pub struct PravegaConnectionManager {
    pub connection_factory: dyn ConnectionFactory,
    pub connection_type: Option<ConnectionType>,
    pub endpoint: SocketAddr,
}

impl PravegaConnectionManager {
    pub fn new(endpoint: SocketAddr, connection_type: Option<ConnectionType>) -> Box<PravegaConnectionManager> {
        Box::new(PravegaConnectionManager {connection_factory: ConnectionFactoryImpl{}, endpoint, connection_type })
    }
}

impl r2d2::ManageConnection for PravegaConnectionManager {
    type Connection = dyn Connection;
    type Error = Error;

    fn connect(&self) -> Result<dyn Connection, Error> {
        &self.connection_factory.establish_connection(self.connection_type, self.endpoint).await?
    }

    fn is_valid(&self, conn: &mut dyn Connection) -> Result<(), GraphError> {
        Ok(())
    }

    fn has_broken(&self, _: &mut dyn Connection) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
}