//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_config::ClientConfig;
use crate::connection_factory::{Connection, ConnectionFactory, ConnectionFactoryImpl};
use crate::error::*;

use async_trait::async_trait;
use dashmap::DashMap;
use log::warn;
use snafu::ResultExt;
use std::fmt;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};

/// ConnectionPool creates a pool of threads for reuse.
/// It is thread safe
#[async_trait]
pub trait ConnectionPool: Send + Sync + 'static {
    /// get_connection takes an endpoint and returns a PooledConnection.
    /// # Example
    ///
    /// ```no_run
    /// use std::net::SocketAddr;
    /// use pravega_wire_protocol::connection_pool::ConnectionPool;
    /// use pravega_wire_protocol::connection_pool::ConnectionPoolImpl;
    /// use pravega_wire_protocol::client_config::{ClientConfig, ClientConfigBuilder};
    /// use tokio::runtime::Runtime;
    ///
    /// let mut rt = Runtime::new().unwrap();
    /// let endpoint: SocketAddr = "127.0.0.1:0".parse().expect("Unable to parse socket address");
    /// let config = ClientConfigBuilder::default().build().unwrap();
    /// let pool = ConnectionPoolImpl::new(config);
    /// let connection = rt.block_on(pool.get_connection(endpoint));
    /// ```
    async fn get_connection(&self, endpoint: SocketAddr) -> Result<PooledConnection, ConnectionPoolError>;
}

/// An implementation of the ConnectionPool.
pub struct ConnectionPoolImpl {
    /// managed_pool holds a map that maps endpoint to the internal pool.
    /// each endpoint has its own internal pool.
    managed_pool: ManagedPool,

    /// The client configuration.
    config: ClientConfig,

    /// connection_factory is used to establish connection to the remote server
    /// when there is no connection available in the internal pool.
    connection_factory: Box<dyn ConnectionFactory>,
}

impl ConnectionPoolImpl {
    /// Create a new ConnectionPoolImpl instances by passing into a ClientConfig. It will create
    /// a Runtime, a map and a ConnectionFactory.
    pub fn new(config: ClientConfig) -> Self {
        let managed_pool = ManagedPool::new(config);
        let connection_factory = Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>;
        ConnectionPoolImpl {
            managed_pool,
            config,
            connection_factory,
        }
    }

    /// Returns the pool length of a specific internal pool
    pub fn pool_len(&self, endpoint: &SocketAddr) -> usize {
        self.managed_pool.pool_len(endpoint)
    }
}

#[async_trait]
impl ConnectionPool for ConnectionPoolImpl {
    /// get_connection takes an endpoint and returns a PooledConnection. The PooledConnection is a
    /// wrapper that contains a Connection that can be used to send and read.
    ///
    /// This method is thread safe and can be called concurrently. It will return an error if it fails
    /// to establish connection to the remote server.
    async fn get_connection(
        &self,
        endpoint: SocketAddr,
    ) -> Result<PooledConnection<'_>, ConnectionPoolError> {
        match self.managed_pool.get_connection(endpoint) {
            Ok(conn) => Ok(PooledConnection {
                inner: Some(conn),
                pool: &self.managed_pool,
            }),

            Err(_e) => self
                .connection_factory
                .establish_connection(endpoint, self.config.connection_type)
                .await
                .context(EstablishConnection {})
                .map_or_else(
                    // track clippy issue https://github.com/rust-lang/rust-clippy/issues/3071
                    |e| {
                        warn!("connection failed to establish");
                        Err(e)
                    },
                    |conn| {
                        Ok(PooledConnection {
                            inner: Some(conn),
                            pool: &self.managed_pool,
                        })
                    },
                ),
        }
    }
}

// ManagedPool maintains a map that maps endpoint to InternalPool.
// The map has a RwLock that ensures thread safety.
struct ManagedPool {
    map: DashMap<SocketAddr, InternalPool>,
    config: ClientConfig,
}

impl ManagedPool {
    pub fn new(config: ClientConfig) -> Self {
        let map = DashMap::new();
        ManagedPool { map, config }
    }

    // add a connection to the internal pool
    fn add_connection(&self, connection: Box<dyn Connection>) {
        let endpoint = connection.get_endpoint();
        let mut internal = self.map.entry(endpoint).or_insert(InternalPool::new());
        if self.config.max_connections_per_segmentstore > internal.conns.len() as u32 {
            internal.conns.push(connection);
        }
    }

    // get a connection from the internal pool. If there is no available connections, returns an error
    fn get_connection(&self, endpoint: SocketAddr) -> Result<Box<dyn Connection>, ConnectionPoolError> {
        let mut internal = self.map.entry(endpoint).or_insert(InternalPool::new());
        if internal.conns.is_empty() {
            Err(ConnectionPoolError::NoAvailableConnection {})
        } else {
            let conn = internal.conns.pop().expect("pop connection from vec");
            Ok(conn)
        }
    }

    // return the pool length of the internal pool
    fn pool_len(&self, endpoint: &SocketAddr) -> usize {
        let pool = self.map.get(endpoint).expect("internal pool");
        pool.conns.len()
    }
}

// An InternalPool that maintains a vector that stores all the connections.
struct InternalPool {
    conns: Vec<Box<dyn Connection>>,
}

impl InternalPool {
    fn new() -> Self {
        InternalPool { conns: vec![] }
    }
}

/// A smart pointer wrapping a Connection so that the inner Connection can return to the ConnectionPool once
/// this pointer is dropped.
pub struct PooledConnection<'a> {
    inner: Option<Box<dyn Connection>>,
    pool: &'a ManagedPool,
}

impl fmt::Debug for PooledConnection<'_> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(
            &self.inner.as_ref().expect("borrow inner connection").get_uuid(),
            fmt,
        )
    }
}

impl Drop for PooledConnection<'_> {
    fn drop(&mut self) {
        self.pool
            .add_connection(self.inner.take().expect("drop connection back to pool"))
    }
}

impl Deref for PooledConnection<'_> {
    type Target = Box<dyn Connection>;

    fn deref(&self) -> &Box<dyn Connection> {
        self.inner.as_ref().expect("borrow inner connection")
    }
}

impl DerefMut for PooledConnection<'_> {
    fn deref_mut(&mut self) -> &mut Box<dyn Connection> {
        self.inner.as_mut().expect("mutably borrow inner connection")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_config::ClientConfigBuilder;
    use log::info;
    use parking_lot::Mutex;
    use std::io::Read;
    use std::net::{SocketAddr, TcpListener};
    use std::ops::DerefMut;
    use std::sync::Arc;
    use std::{io, thread};
    use tokio::runtime::Runtime;

    struct Server {
        address: SocketAddr,
        listener: TcpListener,
    }

    impl Server {
        pub fn new() -> Server {
            let listener = TcpListener::bind("127.0.0.1:0").expect("local server");
            listener.set_nonblocking(true).expect("Cannot set non-blocking");
            let address = listener.local_addr().unwrap();
            info!("server created");
            Server { address, listener }
        }

        pub fn receive(&mut self) -> u32 {
            let mut connections: u32 = 0;

            for stream in self.listener.incoming() {
                match stream {
                    Ok(mut stream) => {
                        let mut buf = [0; 1024];
                        match stream.read(&mut buf) {
                            Ok(_) => {
                                info!("received data");
                            }
                            Err(e) => panic!("encountered IO error: {}", e),
                        }
                        connections += 1;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        break;
                    }
                    Err(e) => panic!("encountered IO error: {}", e),
                }
            }
            connections
        }
    }

    #[test]
    fn test_connection_pool() {
        info!("test connection pool");

        // Create server
        let mut server = Server::new();
        let shared_address = Arc::new(server.address);

        // Create a connection pool and a Runtime
        let config = ClientConfigBuilder::default().build().unwrap();
        let shared_pool = Arc::new(ConnectionPoolImpl::new(config));
        let rt = Arc::new(Mutex::new(Runtime::new().unwrap()));

        // Create a number of threads, each thread will use the connection pool to get a connection
        let mut v = vec![];
        for _i in 1..51 {
            let shared_pool = shared_pool.clone();
            let shared_address = shared_address.clone();
            let rt = rt.clone();
            let h = thread::spawn(move || {
                let mut rt_mutex = rt.lock();
                let mut conn = rt_mutex
                    .block_on(shared_pool.get_connection(*shared_address))
                    .unwrap();
                let mut payload: Vec<u8> = Vec::new();
                payload.push(42);
                rt_mutex.block_on(conn.deref_mut().send_async(&payload)).unwrap();
            });
            v.push(h);
        }

        info!("waiting connection threads to finish");
        for _i in v {
            _i.join().unwrap();
        }
        info!("connection threads joined");

        let received = server.receive();
        let connections = shared_pool.pool_len(shared_address.deref()) as u32;
        assert_eq!(received, connections);
    }
}
