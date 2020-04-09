//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::connection::Connection;
use crate::connection_factory::ConnectionFactory;
use crate::error::*;

use async_trait::async_trait;
use dashmap::DashMap;
use snafu::ResultExt;
use std::fmt;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use uuid::Uuid;

/// Manager is a trait for defining custom connections. User can implement their own
/// type of connection and their own way of establishing the connection in this trait.
/// ConnectionPool will accept an implementation of this trait and manage the customized
/// connection using the method that user provides
/// # Example
///
/// ```no_run
/// use std::net::SocketAddr;
/// use pravega_wire_protocol::connection_pool::ConnectionPool;
/// use pravega_wire_protocol::connection_pool::SegmentConnectionManager;
/// use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionType};
/// use pravega_wire_protocol::client_config::{ClientConfig, ClientConfigBuilder};
/// use tokio::runtime::Runtime;
///
/// let mut rt = Runtime::new().unwrap();
/// let endpoint: SocketAddr = "127.0.0.1:0".parse().expect("Unable to parse socket address");
/// let factory = ConnectionFactory::create(ConnectionType::Tokio);
/// let manager = SegmentConnectionManager::new(factory, 2);
/// let pool = ConnectionPool::new(manager);
/// let connection = rt.block_on(pool.get_connection(endpoint));
/// ```
#[async_trait]
pub trait Manager {
    /// The customized connection must implement Send and Sized marker trait
    type Conn: Send + Sized;

    /// Define how to establish the customized connection
    async fn establish_connection(&self, endpoint: SocketAddr) -> Result<Self::Conn, ConnectionPoolError>;

    /// Check whether this connection is still valid. This method will be used to filter out
    /// invalid connections when putting connection back to the pool
    fn is_valid(&self, conn: &PooledConnection<'_, Self::Conn>) -> bool;

    /// Get the maximum connections in the pool
    fn get_max_connections(&self) -> u32;
}

/// An implementation of the Manager trait. This is for creating connections between
/// Rust client and Segmentstore server.
pub struct SegmentConnectionManager {
    /// connection_factory is used to establish connection to the remote server
    /// when there is no connection available in the internal pool.
    connection_factory: Box<dyn ConnectionFactory>,

    /// The client configuration.
    max_connections_in_pool: u32,
}

impl SegmentConnectionManager {
    pub fn new(connection_factory: Box<dyn ConnectionFactory>, max_connections_in_pool: u32) -> Self {
        SegmentConnectionManager {
            connection_factory,
            max_connections_in_pool,
        }
    }
}

#[async_trait]
impl Manager for SegmentConnectionManager {
    type Conn = Box<dyn Connection>;

    async fn establish_connection(&self, endpoint: SocketAddr) -> Result<Self::Conn, ConnectionPoolError> {
        self.connection_factory
            .establish_connection(endpoint)
            .await
            .context(EstablishConnection {})
    }

    fn is_valid(&self, conn: &PooledConnection<'_, Self::Conn>) -> bool {
        conn.inner.as_ref().expect("get inner connection").is_valid()
    }

    fn get_max_connections(&self) -> u32 {
        self.max_connections_in_pool
    }
}

/// ConnectionPool creates a pool of connections for reuse.
/// It is thread safe.
/// # Example
///
/// ```no_run
/// use std::net::SocketAddr;
/// use pravega_wire_protocol::connection_pool::ConnectionPool;
/// use pravega_wire_protocol::connection_pool::SegmentConnectionManager;
/// use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionType};
/// use pravega_wire_protocol::client_config::{ClientConfig, ClientConfigBuilder};
/// use tokio::runtime::Runtime;
///
/// let mut rt = Runtime::new().unwrap();
/// let endpoint: SocketAddr = "127.0.0.1:0".parse().expect("Unable to parse socket address");
/// let factory = ConnectionFactory::create(ConnectionType::Tokio);
/// let manager = SegmentConnectionManager::new(factory, 1);
/// let pool = ConnectionPool::new(manager);
/// let connection = rt.block_on(pool.get_connection(endpoint));
/// ```
pub struct ConnectionPool<M>
where
    M: Manager,
{
    manager: M,

    /// managed_pool holds a map that maps endpoint to the internal pool.
    /// each endpoint has its own internal pool.
    managed_pool: ManagedPool<M::Conn>,
}

impl<M> ConnectionPool<M>
where
    M: Manager,
{
    /// Create a new ConnectionPoolImpl instances by passing into a ClientConfig. It will create
    /// a Runtime, a map and a ConnectionFactory.
    pub fn new(manager: M) -> Self {
        let managed_pool = ManagedPool::new(manager.get_max_connections());
        ConnectionPool {
            manager,
            managed_pool,
        }
    }

    /// get_connection takes an endpoint and returns a PooledConnection. The PooledConnection is a
    /// wrapper that contains a Connection that can be used to send and read.
    ///
    /// This method is thread safe and can be called concurrently. It will return an error if it fails
    /// to establish connection to the remote server.
    pub async fn get_connection(
        &self,
        endpoint: SocketAddr,
    ) -> Result<PooledConnection<'_, M::Conn>, ConnectionPoolError> {
        match self.managed_pool.get_connection(endpoint) {
            Ok(internal_conn) => Ok(PooledConnection {
                uuid: internal_conn.uuid,
                inner: Some(internal_conn.conn),
                endpoint,
                pool: &self.managed_pool,
                valid: true,
            }),
            Err(_e) => {
                let conn = self.manager.establish_connection(endpoint).await?;
                Ok(PooledConnection {
                    uuid: Uuid::new_v4(),
                    inner: Some(conn),
                    endpoint,
                    pool: &self.managed_pool,
                    valid: true,
                })
            }
        }
    }

    /// Returns the pool length of a specific internal pool
    pub fn pool_len(&self, endpoint: &SocketAddr) -> usize {
        self.managed_pool.pool_len(endpoint)
    }
}

// ManagedPool maintains a map that maps endpoint to InternalPool.
// The map is a concurrent map named Dashmap, which supports multi-threading with high performance.
struct ManagedPool<T: Sized + Send> {
    map: DashMap<SocketAddr, InternalPool<T>>,
    max_connections: u32,
}

impl<T: Sized + Send> ManagedPool<T> {
    pub fn new(max_connections: u32) -> Self {
        let map = DashMap::new();
        ManagedPool { map, max_connections }
    }

    // add a connection to the internal pool
    fn add_connection(&self, endpoint: SocketAddr, connection: InternalConn<T>) {
        let mut internal = self.map.entry(endpoint).or_insert_with(InternalPool::new);
        if self.max_connections > internal.conns.len() as u32 {
            internal.conns.push(connection);
        }
    }

    // get a connection from the internal pool. If there is no available connections, returns an error
    fn get_connection(&self, endpoint: SocketAddr) -> Result<InternalConn<T>, ConnectionPoolError> {
        let mut internal = self.map.entry(endpoint).or_insert_with(InternalPool::new);
        if internal.conns.is_empty() {
            Err(ConnectionPoolError::NoAvailableConnection {})
        } else {
            let conn = internal.conns.pop().expect("pop connection from vec");
            Ok(conn)
        }
    }

    // return the pool length of the internal pool
    fn pool_len(&self, endpoint: &SocketAddr) -> usize {
        self.map.get(endpoint).map_or(0, |pool| pool.conns.len())
    }
}

// An internal connection struct that stores the uuid of the connection
struct InternalConn<T> {
    uuid: Uuid,
    conn: T,
}

// An InternalPool that maintains a vector that stores all the connections.
struct InternalPool<T> {
    conns: Vec<InternalConn<T>>,
}

impl<T: Send + Sized> InternalPool<T> {
    fn new() -> Self {
        InternalPool { conns: vec![] }
    }
}

/// A smart pointer wrapping a Connection so that the inner Connection can return to the ConnectionPool once
/// this pointer is dropped.
pub struct PooledConnection<'a, T: Send + Sized> {
    uuid: Uuid,
    endpoint: SocketAddr,
    inner: Option<T>,
    pool: &'a ManagedPool<T>,
    valid: bool,
}

impl<T: Send + Sized> PooledConnection<'_, T> {
    pub fn invalidate(&mut self) {
        self.valid = false;
    }
}

impl<T: Send + Sized> fmt::Debug for PooledConnection<'_, T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.uuid, fmt)
    }
}

impl<T: Send + Sized> Drop for PooledConnection<'_, T> {
    fn drop(&mut self) {
        let conn = self.inner.take().expect("get inner connection");
        if self.valid {
            self.pool.add_connection(
                self.endpoint,
                InternalConn {
                    uuid: self.uuid,
                    conn,
                },
            )
        }
    }
}

impl<T: Send + Sized> Deref for PooledConnection<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.inner.as_ref().expect("borrow inner connection")
    }
}

impl<T: Send + Sized> DerefMut for PooledConnection<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.inner.as_mut().expect("mutably borrow inner connection")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct FooConnection {}

    struct FooManager {
        max_connections_in_pool: u32,
    }

    #[async_trait]
    impl Manager for FooManager {
        type Conn = FooConnection;

        async fn establish_connection(
            &self,
            _endpoint: SocketAddr,
        ) -> Result<Self::Conn, ConnectionPoolError> {
            Ok(FooConnection {})
        }

        fn is_valid(&self, _conn: &PooledConnection<'_, Self::Conn>) -> bool {
            true
        }

        fn get_max_connections(&self) -> u32 {
            self.max_connections_in_pool
        }
    }

    #[tokio::test(core_threads = 4)]
    async fn test_connection_pool_basic() {
        let manager = FooManager {
            max_connections_in_pool: 2,
        };
        let pool = ConnectionPool::new(manager);
        let endpoint = "127.0.0.1:9090"
            .parse::<SocketAddr>()
            .expect("parse to socketaddr");

        assert_eq!(pool.pool_len(&endpoint), 0);
        let connection = pool.get_connection(endpoint).await.expect("get connection");
        assert_eq!(pool.pool_len(&endpoint), 0);
        drop(connection);
        assert_eq!(pool.pool_len(&endpoint), 1);
    }

    #[tokio::test(core_threads = 4)]
    async fn test_connection_pool_size() {
        const MAX_CONNECTION: u32 = 2;
        let manager = FooManager {
            max_connections_in_pool: MAX_CONNECTION,
        };
        let pool = Arc::new(ConnectionPool::new(manager));
        let endpoint = "127.0.0.1:9090"
            .parse::<SocketAddr>()
            .expect("parse to socketaddr");

        let mut handles = vec![];
        for _ in 0..100 {
            let cloned_pool = pool.clone();
            let handle = tokio::spawn(async move {
                let _ = cloned_pool
                    .get_connection(endpoint)
                    .await
                    .expect("get connection");
            });
            handles.push(handle);
        }

        while !handles.is_empty() {
            let handle = handles.pop().expect("get handle");
            handle.await.expect("handle should work");
        }
        assert_eq!(pool.pool_len(&endpoint) as u32, MAX_CONNECTION);
    }
}
