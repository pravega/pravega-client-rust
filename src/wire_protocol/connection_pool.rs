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

/// ConnectionPool can create a pool of threads and let caller to reuse the existing connections from the pool.
/// It is safe to use across threads
pub trait ConnectionPool: Send + Sync + 'static {
    /// get_connection takes an endpoint and returns a PooledConnection.
    /// # Example
    ///
    /// ```no_run
    /// use std::net::SocketAddr;
    /// use pravega_client_rust::wire_protocol::connection_pool::ConnectionPoolImpl;
    /// use pravega_client_rust::wire_protocol::client_config::ClientConfig;
    /// use tokio::runtime::Runtime;
    ///
    /// fn main() {
    ///   use pravega_client_rust::wire_protocol::connection_pool::ConnectionPool;
    ///   let mut rt = Runtime::new().unwrap();
    ///   let endpoint: SocketAddr = "127.0.0.1:0".parse().expect("Unable to parse socket address");
    ///   let config = ClientConfigBuilder::default().build();
    ///   let pool = ConnectionPoolImpl::new(config);
    ///   let connection = pool.get_connection(endpoint);
    /// }
    /// ```
    fn get_connection(&self, endpoint: SocketAddr) -> Result<PooledConnection>;
}

// put_back takes a ConnectionPoolImpl instance and a Connection instance.
// This method puts that Connection back into the connection pool, which is used by the Drop method
// of the PooledConnection.
fn put_back(conntion_pool: Arc<ConnectionPoolImpl>, connection: Box<dyn Connection>) {
    let write_guard = conntion_pool.map.write().unwrap();
    let endpoint = connection.get_endpoint();
    let mut internal = write_guard.get(&endpoint).unwrap().lock();
    internal.add_connection_to_pool(connection);
}
/// An implementation of the ConnectionPool.
#[derive(Clone)]
pub struct ConnectionPoolImpl {
    rt: Arc<Mutex<Runtime>>,
    map: Arc<RwLock<HashMap<SocketAddr, Mutex<InternalPool>>>>,
    config: ClientConfig,
    connection_factory: Arc<Box<dyn ConnectionFactory>>,
}

impl ConnectionPoolImpl {
    /// Create a new ConnectionPoolImpl instances by passing into a ClientConfig. It will create
    /// a Runtime, a map and a ConnectionFactory.
    pub fn new(config: ClientConfig) -> Self {
        let rt = Arc::new(Mutex::new(Runtime::new().unwrap()));
        let map = Arc::new(RwLock::new(HashMap::new()));
        let connection_factory =
            Arc::new(Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>);
        ConnectionPoolImpl {
            rt,
            map,
            config,
            connection_factory,
        }
    }
}

impl ConnectionPool for ConnectionPoolImpl {
    /// get_connection takes an endpoint and returns a PooledConnection. The PooledConnection is a
    /// wrapper that contains a Connection that can be used to send and read.
    ///
    /// This method is thread safe and can be called concurrently
    fn get_connection(&self, endpoint: SocketAddr) -> Result<PooledConnection> {
        // Get a read lock instead of locking the whole map to allow other threads to get internal pool of
        // different endpoints at the same time.
        let mut read_guard = self.map.read().unwrap();

        // If map contains the endpoint, we can get connection from that internal pool
        if read_guard.contains_key(&endpoint) {
            let mut internal = read_guard.get(&endpoint).unwrap().lock();
            let connection;

            // If there are no connections in the pool, create one using ConnectionFactory.
            // Otherwise, get the connection from the pool.
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
                pool: Arc::new(self.clone()),
                inner: Some(connection),
            })
        } else {
            // This is a new endpoint, we will need to create a new internal pool for it.
            // Drop the read lock and acquire the write lock since we are going to modify the hash map.
            drop(read_guard);
            let mut write_guard = self.map.write().unwrap();

            // Check again to see if the map contains that endpoint. This could happen if other threads
            // acquire the write lock before this thread does.
            if !write_guard.contains_key(&endpoint) {
                let internal = Mutex::new(InternalPool {
                    conns: vec![],
                    num_conns: 0,
                    min_conns: 1,
                });
                write_guard.insert(endpoint, internal);
            }

            // Get the Connection from the internal pool.
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
                pool: Arc::new(self.clone()),
                inner: Some(connection),
            })
        }
    }
}

/// A smart pointer wrapping a Connection so that the inner Connection can return to the ConnectionPool once
/// this pointer is dropped.
pub struct PooledConnection {
    inner: Option<Box<dyn Connection>>,
    pool: Arc<ConnectionPoolImpl>,
}

impl fmt::Debug for PooledConnection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.inner.as_ref().unwrap().get_uuid(), fmt)
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        put_back(self.pool.clone(), self.inner.take().unwrap());
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

// An InternalPool that maintains a vector that stores all the connections.
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
            // TODO
            // return an error
            panic!("to do");
        }
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
            let mut count = 0;
            for stream in self.listener.incoming() {
                let mut buf = [0; 1024];
                let mut stream = stream.unwrap();
                match stream.read(&mut buf) {
                    Ok(_) => {
                        for ptr in 0..buf.len() {
                            if buf[ptr] != 0 {
                                num -= 1;
                            } else {
                                break;
                            }
                        }
                        if num <= 0 {
                            break;
                        }
                        info!("received data");
                    }
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
        let mut rt = Arc::new(Mutex::new(Runtime::new().unwrap()));
        let mut v = vec![];
        for i in 1..51 {
            let mut shared_pool = shared_pool.clone();
            let shared_address = shared_address.clone();
            let mut rt = rt.clone();
            let h = thread::spawn(move || {
                let mut conn = shared_pool.get_connection(*shared_address).unwrap();
                let mut payload: Vec<u8> = Vec::new();
                payload.push(42);

                let mut rt_mutex = rt.lock();
                let res = rt_mutex
                    .block_on(conn.deref_mut().send_async(&payload))
                    .unwrap();
            });
            v.push(h);
        }

        info!("waiting threads to finish");
        for i in v {
            i.join().unwrap();
        }
        info!("all threads joined");
        server.receive(50);
    }
}
