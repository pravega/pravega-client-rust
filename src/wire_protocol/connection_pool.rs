//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::wire_protocol::client_config::ClientConfig;
use crate::wire_protocol::connection_factory::{
    Connection, ConnectionFactory, ConnectionFactoryError, ConnectionFactoryImpl,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::RwLock;
use tokio::runtime::Runtime;

#[derive(Debug, Snafu)]
pub enum ConnectionPoolError {
    #[snafu(display("Could not connect to endpoint"))]
    Connect { source: ConnectionFactoryError },
}

type Result<T, E = ConnectionPoolError> = std::result::Result<T, E>;

pub struct PooledConnection {
    inner: Option<Box<dyn Connection>>,
    pool: Arc<Box<dyn ConnectionPool>>,
}

//impl fmt::Debug for PooledConnection {
//    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
//        fmt::Debug::fmt(&self.inner.as_ref().unwrap(), fmt)
//    }
//}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        self.pool.put_back(self.inner.take().unwrap());
    }
}

impl Deref for PooledConnection {
    type Target = Box<dyn Connection>;

    fn deref(&self) -> &Box<dyn Connection> {
        &self.inner.as_ref().unwrap()
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Box<dyn Connection> {
        self.inner.as_mut().unwrap()
    }
}

struct InternalPool {
    conns: Vec<Box<dyn Connection>>,
    num_conns: u32,
    min_conns: u32,
}

impl InternalPool {
    fn add_connection_to_pool(&mut self, connection: Box<dyn Connection>) {
        self.conns.push(connection);
        self.num_conns += 1;
    }

    fn get_connection_from_pool(&mut self) -> Result<Box<dyn Connection>> {
        if let Some(mut conn) = self.conns.pop() {
            self.num_conns -= 1;
            Ok(conn)
        } else {
            //            TODO
            // return an error
            panic!("to do");
        }
    }
}

pub trait ConnectionPool: Send {
    fn get_connection(&self, endpoint: SocketAddr) -> Result<PooledConnection>;

    fn put_back(&self, connection: Box<dyn Connection>);
}

pub struct ConnectionPoolImpl {
    rt: Arc<Mutex<Runtime>>,
    map: Arc<RwLock<HashMap<SocketAddr, Mutex<InternalPool>>>>,
    config: ClientConfig,
    connection_factory: Box<dyn ConnectionFactory>,
}

impl Clone for ConnectionPoolImpl {
    fn clone(&self) -> ConnectionPoolImpl {
        ConnectionPoolImpl {
            rt: self.rt.clone(),
            map: self.map.clone(),
            config: self.config.clone(),
            connection_factory: Box::new(ConnectionFactoryImpl {}),
        }
    }
}

impl ConnectionPoolImpl {
    pub fn new(config: ClientConfig) -> Self {
        let rt = Arc::new(Mutex::new(Runtime::new().unwrap()));
        let map = Arc::new(RwLock::new(HashMap::new()));
        let connection_factory = Box::new(ConnectionFactoryImpl {});
        ConnectionPoolImpl {
            rt,
            map,
            config,
            connection_factory,
        }
    }
}

impl ConnectionPool for ConnectionPoolImpl {
    fn get_connection(&self, endpoint: SocketAddr) -> Result<PooledConnection> {
        //
        let mut read_guard = self.map.read().unwrap();
        if read_guard.contains_key(&endpoint) {
            let mut internal = read_guard.get(&endpoint).unwrap().lock();
            let connection;
            if internal.num_conns == 0 {
                let fut = self
                    .connection_factory
                    .establish_connection(endpoint, self.config.connection_type);
                let mut rt_mutex = self.rt.lock();
                connection = rt_mutex.block_on(fut).unwrap();
            } else {
                connection = internal.get_connection_from_pool().unwrap();
            }
            Ok(PooledConnection {
                pool: Arc::new(Box::new(self.clone())),
                inner: Some(connection),
            })
        } else {
            drop(read_guard);
            let mut write_guard = self.map.write().unwrap();
            if !write_guard.contains_key(&endpoint) {
                let internal = Mutex::new(InternalPool {
                    conns: vec![],
                    num_conns: 0,
                    min_conns: 1,
                });
                write_guard.insert(endpoint, internal);
            }
            let mut internal = write_guard.get(&endpoint).unwrap().lock();
            let connection;
            if internal.num_conns == 0 {
                let fut = self
                    .connection_factory
                    .establish_connection(endpoint, self.config.connection_type);
                let mut rt_mutex = self.rt.lock();
                connection = rt_mutex.block_on(fut).unwrap();
            } else {
                connection = internal.get_connection_from_pool().unwrap();
            }
            Ok(PooledConnection {
                pool: Arc::new(Box::new(self.clone())),
                inner: Some(connection),
            })
        }
    }

    fn put_back(&self, connection: Box<dyn Connection>) {
        let write_guard = self.map.write().unwrap();
        let endpoint = connection.get_endpoint();
        let mut internal = write_guard.get(&endpoint).unwrap().lock();
        internal.add_connection_to_pool(connection);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire_protocol::client_config::ClientConfigBuilder;
    use log::info;
    use std::io::Read;
    use std::net::{SocketAddr, TcpListener};
    use std::ops::DerefMut;
    use std::sync::Arc;
    use std::thread;
    use std::time;
    use tokio::runtime::Runtime;

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
        for i in 1..51 {
            let mut shared_pool = shared_pool.clone();
            let shared_address = shared_address.clone();
            let h = thread::spawn(move || {
                let mut rt = Runtime::new().unwrap();

                let ten_millis = time::Duration::from_millis(100);

                thread::sleep(ten_millis);
                let mut conn = shared_pool.get_connection(*shared_address).unwrap();

                let mut payload: Vec<u8> = Vec::new();
                payload.push(42);
                println!("{:?}", conn.get_uuid());
                let res = rt.block_on(conn.deref_mut().send_async(&payload)).unwrap();
            });
            v.push(h);
        }

        println!("waiting threads to finish");
        for i in v {
            i.join().unwrap();
        }
        println!("all threads joined");
        server.receive(50);
    }
}
