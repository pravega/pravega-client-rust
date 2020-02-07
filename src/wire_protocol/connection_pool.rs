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
//extern crate parking_lot;
//extern crate chashmap;
use self::r2d2::PooledConnection;
use crate::wire_protocol::client_config::ClientConfig;
use crate::wire_protocol::connection_factory::{
    Connection, ConnectionFactory, ConnectionFactoryError, ConnectionFactoryImpl, ConnectionType,
};
use async_trait::async_trait;
use snafu::{ResultExt, Snafu};
use std::net::SocketAddr;
use tokio::runtime;
//use self::chashmap::CHashMap;
//use parking_lot::RwLock;
use std::collections::HashMap;
//use futures::lock::Mutex;
use std::borrow::Borrow;
use std::sync::Arc;
use std::sync::RwLock;
use futures::Future;

#[derive(Debug, Snafu)]
pub enum ConnectionPoolError {
    #[snafu(display("Could not connect to endpoint"))]
    Connect { source: ConnectionFactoryError },
}

type Result<T, E = ConnectionPoolError> = std::result::Result<T, E>;

//#[async_trait]
pub trait ConnectionPool {
    fn get_connection(
        &mut self,
        endpoint: SocketAddr,
    ) -> Result<PooledConnection<PravegaConnectionManager>>;
}

pub struct ConnectionPoolImpl {
    //    map: chashmap::CHashMap<SocketAddr, r2d2::Pool<PravegaConnectionManager>>,
    map: Arc<RwLock<HashMap<SocketAddr, r2d2::Pool<PravegaConnectionManager>>>>,
    config: ClientConfig,
}

impl ConnectionPoolImpl {
    pub fn new(config: ClientConfig) -> Self {
        //        let map: CHashMap<SocketAddr, r2d2::Pool<PravegaConnectionManager>> = CHashMap::new();
        let map = Arc::new(RwLock::new(HashMap::new()));
        ConnectionPoolImpl { map, config }
    }
}

impl ConnectionPool for ConnectionPoolImpl {
    fn get_connection(
        &mut self,
        endpoint: SocketAddr,
    ) -> Result<PooledConnection<PravegaConnectionManager>> {
        //        let borrowed_map = self.map.into_inner();
        //        println!("{}", borrowed_map.len());
        let mut read_guard = self.map.write().unwrap();
        //        let borrowed_map = &self.map.into_inner().unwrap();
        if read_guard.contains_key(&endpoint) {
            println!("endpoint exists in the map");
            Ok(read_guard.get(&endpoint).unwrap().get().unwrap())
        } else {
            //            drop(read_guard);
            println!("endpoint does not exist in the map, inserting now");
            //            let mut mut_map = self.map.write().unwrap();
            if read_guard.contains_key(&endpoint) {
                Ok(read_guard.get(&endpoint).unwrap().get().unwrap())
            } else {
                let manager = PravegaConnectionManager::new(endpoint, self.config.connection_type);
                let pool = r2d2::Pool::builder().max_size(2).build(manager).unwrap();

                read_guard.insert(endpoint, pool);
                Ok(read_guard.get(&endpoint).unwrap().get().unwrap())
            }
        }
    }
}

pub struct PravegaConnectionManager {
    connection_factory: Box<dyn ConnectionFactory + Send + Sync>,
    connection_type: ConnectionType,
    endpoint: SocketAddr,
}

impl PravegaConnectionManager {
    pub fn new(endpoint: SocketAddr, connection_type: ConnectionType) -> PravegaConnectionManager {
        let connection_factory = Box::new(ConnectionFactoryImpl {});
        PravegaConnectionManager {
            connection_factory,
            endpoint,
            connection_type,
        }
    }
}
//struct ConnFuture (Box<dyn Future<Output=Result<Box<dyn Connection>, ConnectionFactoryError>> + Send + Unpin>);

impl r2d2::ManageConnection for PravegaConnectionManager {
    type Connection = Box<dyn Future<Output=Result<Box<dyn Connection>, ConnectionFactoryError>> + Send + Unpin>;
    type Error = ConnectionPoolError;

    fn connect(&self) -> Result<Box<dyn Future<Output=Result<Box<dyn Connection>, ConnectionFactoryError>> + Send + Unpin>, ConnectionPoolError> {
        Ok(Box::new(self.connection_factory.establish_connection(self.endpoint, self.connection_type)))
    }

    fn is_valid(&self, _conn: &mut Box<dyn Future<Output=Result<Box<dyn Connection>, ConnectionFactoryError>> + Send + Unpin>) -> Result<(), ConnectionPoolError> {
        Ok(())
    }

    fn has_broken(&self, _: &mut Box<dyn Future<Output=Result<Box<dyn Connection>, ConnectionFactoryError>> + Send + Unpin>) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire_protocol::client_config::ClientConfigBuilder;
    use log::info;
    use std::any::Any;
    use std::borrow::BorrowMut;
    use std::io::Read;
    use std::net::{SocketAddr, TcpListener};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread;
    use std::time;
    use tokio::runtime::Runtime;
    use std::ops::DerefMut;

    struct Server {
        address: SocketAddr,
        listener: TcpListener,
    }

    impl Server {
        pub fn new() -> Server {
            let listener = TcpListener::bind("127.0.0.1:0").expect("local server");
            let address = listener.local_addr().unwrap();
            info!("server created");
            Server { address, listener }
        }

        pub fn receive(&mut self, mut num: i32) {
            for stream in self.listener.incoming() {
                println!("server listening");
                num -= 1;
                if num <= 0 {
                    break;
                }
                let mut buf = vec![];
                let mut stream = stream.unwrap();
                match stream.read(&mut buf) {
                    Ok(_) => println!("received data"),
                    Err(e) => panic!("encountered IO error: {}", e),
                }
            }
        }
    }

    #[test]
    fn test_connection_pool() {
        let mut server = Server::new();
        let shared_address = Arc::new(server.address);

        let config = ClientConfigBuilder::default()
            .max_connections_per_segmentstore(15 as u32)
            .build()
            .unwrap();
        let connection_pool =
            crate::wire_protocol::connection_pool::ConnectionPoolImpl::new(config);
        let shared_pool = Arc::new(connection_pool);

        let mut v = vec![];
        for i in 1..3 {

            let mut shared_pool = shared_pool.clone();
            let shared_address = shared_address.clone();
            let h = thread::spawn(move || {
                let mut rt = Runtime::new().unwrap();

                println!("number {} from the spawned thread!", i);
                let mut conn = shared_pool.get_connection(*shared_address).unwrap();

                let mut payload: Vec<u8> = Vec::new();
                payload.push(42);
//                println!("{:?}", conn.get_uuid());
                let c = rt.block_on(conn.deref_mut()).unwrap();
                let sent = c.send_async(&payload);
                let res = rt.block_on(sent);
                match res {
                    Ok(o) => println!("fine"),
                    Err(e) => println!("{:?}", e),
                }
            });
            v.push(h);
        }
        println!("waiting threads to finish");
        for i in v {
            i.join().unwrap();
        }
        println!("all threads joined");
        server.receive(2);
    }
}
