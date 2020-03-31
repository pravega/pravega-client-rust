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
use crate::connection::Connection;
use crate::connection_factory::ConnectionFactory;
use crate::error::*;

use async_trait::async_trait;
use dashmap::DashMap;
use snafu::ResultExt;
use std::fmt;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use tracing::{span, Level};
use uuid::Uuid;

#[async_trait]
pub trait Manager {
    type Conn: Send + Sized;

    async fn establish_connection(&self, endpoint: SocketAddr) -> Result<Self::Conn, ConnectionPoolError>;

    fn is_valid(&self, conn: &PooledConnection<'_, Self::Conn>) -> bool;

    fn get_config(&self) -> ClientConfig;
}

pub struct SegmentConnectionManager {
    /// connection_factory is used to establish connection to the remote server
    /// when there is no connection available in the internal pool.
    connection_factory: Box<dyn ConnectionFactory>,

    /// The client configuration.
    config: ClientConfig,
}

impl SegmentConnectionManager {
    pub fn new(connection_factory: Box<dyn ConnectionFactory>, config: ClientConfig) -> Self {
        SegmentConnectionManager {
            connection_factory,
            config,
        }
    }
}

#[async_trait]
impl Manager for SegmentConnectionManager {
    type Conn = Box<dyn Connection>;

    async fn establish_connection(&self, endpoint: SocketAddr) -> Result<Self::Conn, ConnectionPoolError> {
        self.connection_factory
            .establish_connection(endpoint, self.config.connection_type)
            .await
            .context(EstablishConnection {})
    }

    fn is_valid(&self, conn: &PooledConnection<'_, Self::Conn>) -> bool {
        conn.inner.as_ref().expect("get inner connection").is_valid()
    }

    fn get_config(&self) -> ClientConfig {
        self.config
    }
}

/// ConnectionPool creates a pool of threads for reuse.
/// It is thread safe.
/// # Example
///
/// ```no_run
/// use std::net::SocketAddr;
/// use pravega_wire_protocol::connection_pool::ConnectionPool;
/// use pravega_wire_protocol::connection_pool::SegmentConnectionManager;
/// use pravega_wire_protocol::connection_factory::ConnectionFactoryImpl;
/// use pravega_wire_protocol::client_config::{ClientConfig, ClientConfigBuilder};
/// use tokio::runtime::Runtime;
///
/// let mut rt = Runtime::new().unwrap();
/// let endpoint: SocketAddr = "127.0.0.1:0".parse().expect("Unable to parse socket address");
/// let config = ClientConfigBuilder::default().build().unwrap();
/// let factory = Box::new(ConnectionFactoryImpl{});
/// let manager = SegmentConnectionManager::new(factory, config);
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
        let managed_pool = ManagedPool::new(manager.get_config());
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
        let span = span!(Level::DEBUG, "send_setup_request");
        let _guard = span.enter();
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
    config: ClientConfig,
}

impl<T: Sized + Send> ManagedPool<T> {
    pub fn new(config: ClientConfig) -> Self {
        let map = DashMap::new();
        ManagedPool { map, config }
    }

    // add a connection to the internal pool
    fn add_connection(&self, endpoint: SocketAddr, connection: InternalConn<T>) {
        let mut internal = self.map.entry(endpoint).or_insert_with(InternalPool::new);
        if self.config.max_connections_per_segmentstore > internal.conns.len() as u32 {
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
        let pool = self.map.get(endpoint).expect("internal pool");
        pool.conns.len()
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
    use crate::client_config::ClientConfigBuilder;

    struct FooConnection {}

    struct FooManager {
        config: ClientConfig,
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

        fn get_config(&self) -> ClientConfig {
            self.config
        }
    }

    #[tokio::test(core_threads = 4)]
    async fn test_connection_pool() {
        let config = ClientConfigBuilder::default()
            .build()
            .expect("build client config");
        let manager = FooManager { config };
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
}
