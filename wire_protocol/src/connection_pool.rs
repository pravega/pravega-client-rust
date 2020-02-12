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

use parking_lot::Mutex;
use snafu::ResultExt;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::RwLock;
use tokio::runtime::Runtime;

/// ConnectionPool can create a pool of threads and let caller to reuse the existing connections from the pool.
/// It is safe to use across threads
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
    /// let connection = pool.get_connection(endpoint);
    /// ```
    fn get_connection(&self, endpoint: SocketAddr) -> Result<PooledConnection, ConnectionPoolError>;
}

// put_back takes a ConnectionPoolImpl instance and a Connection instance.
// This method puts that Connection back into the connection pool, which is used by the Drop method
// of the PooledConnection.
fn put_back(conntion_pool: Arc<ConnectionPoolImpl>, connection: Box<dyn Connection>) {
    let write_guard = conntion_pool.map.write().expect("get map write lock");
    let endpoint = connection.get_endpoint();
    let mut internal = write_guard
        .get(&endpoint)
        .expect("get internal pool mutex")
        .lock();
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
        let rt = Arc::new(Mutex::new(Runtime::new().expect("create runtime")));
        let map = Arc::new(RwLock::new(HashMap::new()));
        let connection_factory = Arc::new(Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>);
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
    fn get_connection(&self, endpoint: SocketAddr) -> Result<PooledConnection, ConnectionPoolError> {
        // Get a read lock instead of locking the whole map to allow other threads to get internal pool of
        // different endpoints at the same time.
        let read_guard = self.map.read().expect("get map read lock");

        // If map contains the endpoint, we can get connection from that internal pool
        if read_guard.contains_key(&endpoint) {
            let mut internal = read_guard.get(&endpoint).unwrap().lock();

            // If there are no connections in the pool, create one using ConnectionFactory.
            // Otherwise, get the connection from the pool.
            let connection = if internal.num_conns == 0 {
                let fut = self
                    .connection_factory
                    .establish_connection(endpoint, self.config.connection_type);
                let mut rt_mutex = self.rt.lock();
                rt_mutex.block_on(fut).context(EstablishConnection {})?
            } else {
                internal.get_connection_from_pool()?
            };

            Ok(PooledConnection {
                pool: Arc::new(self.clone()),
                inner: Some(connection),
            })
        } else {
            // This is a new endpoint, we will need to create a new internal pool for it.
            // Drop the read lock and acquire the write lock since we are going to modify the hash map.
            drop(read_guard);
            let mut write_guard = self.map.write().expect("get map write lock");

            // Check again to see if the map contains that endpoint. This is needed if other threads
            // acquire the write lock before this thread does.
            if !write_guard.contains_key(&endpoint) {
                let internal = Mutex::new(InternalPool {
                    conns: vec![],
                    num_conns: 0,
                });
                write_guard.insert(endpoint, internal);
            }

            // Get the Connection from the internal pool.
            let mut internal = write_guard
                .get(&endpoint)
                .expect("get internal pool mutex")
                .lock();
            let connection = if internal.num_conns == 0 {
                let fut = self
                    .connection_factory
                    .establish_connection(endpoint, self.config.connection_type);
                let mut rt_mutex = self.rt.lock();
                rt_mutex.block_on(fut).context(EstablishConnection {})?
            } else {
                internal.get_connection_from_pool()?
            };

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
        fmt::Debug::fmt(
            &self.inner.as_ref().expect("borrow inner connection").get_uuid(),
            fmt,
        )
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        put_back(
            self.pool.clone(),
            self.inner.take().expect("take ownership if inner connection"),
        )
    }
}

impl Deref for PooledConnection {
    type Target = Box<dyn Connection>;

    fn deref(&self) -> &Box<dyn Connection> {
        self.inner.as_ref().expect("borrow inner connection")
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Box<dyn Connection> {
        self.inner.as_mut().expect("mutably borrow inner connection")
    }
}

// An InternalPool that maintains a vector that stores all the connections.
struct InternalPool {
    conns: Vec<Box<dyn Connection>>,
    num_conns: u32,
}

impl InternalPool {
    fn add_connection_to_pool(&mut self, connection: Box<dyn Connection>) {
        self.conns.push(connection);
        self.num_conns += 1;
    }

    fn get_connection_from_pool(&mut self) -> Result<Box<dyn Connection>, ConnectionPoolError> {
        if let Some(conn) = self.conns.pop() {
            self.num_conns -= 1;
            Ok(conn)
        } else {
            let message = String::from("no available connection in the internal pool");
            Err(ConnectionPoolError::GetConnection { message })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_config::ClientConfigBuilder;
    use log::info;
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
                let mut conn = shared_pool.get_connection(*shared_address).unwrap();
                let mut payload: Vec<u8> = Vec::new();
                payload.push(42);
                let mut rt_mutex = rt.lock();
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
        let connections = shared_pool
            .map
            .write()
            .unwrap()
            .get(&server.address)
            .unwrap()
            .lock()
            .num_conns;
        assert_eq!(received, connections);
    }
}
