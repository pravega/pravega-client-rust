//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_connection::{read_wirecommand, write_wirecommand};
use crate::commands::{HelloCommand, OLDEST_COMPATIBLE_VERSION, WIRE_VERSION};
use crate::connection::{Connection, TokioConnection};
use crate::error::*;
use crate::mock_connection::MockConnection;
use crate::wire_commands::{Replies, Requests};
use async_trait::async_trait;
use pravega_connection_pool::connection_pool::{ConnectionPoolError, Manager};
use snafu::ResultExt;
use std::fmt;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use uuid::Uuid;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ConnectionType {
    Mock,
    Tokio,
}

impl Default for ConnectionType {
    fn default() -> Self {
        ConnectionType::Tokio
    }
}

impl fmt::Display for ConnectionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// ConnectionFactory trait is the factory used to establish the TCP connection with remote servers.
#[async_trait]
pub trait ConnectionFactory: Send + Sync {
    /// establish_connection will return a Connection future that used to send and read data.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::net::SocketAddr;
    /// use pravega_wire_protocol::connection_factory;
    /// use pravega_wire_protocol::connection_factory::ConnectionFactory;
    /// use tokio::runtime::Runtime;
    ///
    /// fn main() {
    ///   let mut rt = Runtime::new().unwrap();
    ///   let endpoint: SocketAddr = "127.0.0.1:0".parse().expect("Unable to parse socket address");
    ///   let cf = connection_factory::ConnectionFactory::create(connection_factory::ConnectionType::Tokio);
    ///   let connection_future = cf.establish_connection(endpoint);
    ///   let mut connection = rt.block_on(connection_future).unwrap();
    /// }
    /// ```
    async fn establish_connection(
        &self,
        endpoint: SocketAddr,
    ) -> Result<Box<dyn Connection>, ConnectionFactoryError>;
}

impl dyn ConnectionFactory {
    pub fn create(connection_type: ConnectionType) -> Box<dyn ConnectionFactory> {
        match connection_type {
            ConnectionType::Tokio => Box::new(TokioConnectionFactory {}),
            ConnectionType::Mock => Box::new(MockConnectionFactory {}),
        }
    }
}

struct TokioConnectionFactory {}
struct MockConnectionFactory {}

#[async_trait]
impl ConnectionFactory for TokioConnectionFactory {
    async fn establish_connection(
        &self,
        endpoint: SocketAddr,
    ) -> Result<Box<dyn Connection>, ConnectionFactoryError> {
        let connection_type = ConnectionType::Tokio;
        let uuid = Uuid::new_v4();
        let stream = TcpStream::connect(endpoint).await.context(Connect {
            connection_type,
            endpoint,
        })?;
        let mut tokio_connection: Box<dyn Connection> = Box::new(TokioConnection {
            uuid,
            endpoint,
            stream: Some(stream),
        }) as Box<dyn Connection>;
        verify_connection(&mut *tokio_connection)
            .await
            .context(Verify {})?;
        Ok(tokio_connection)
    }
}

#[async_trait]
impl ConnectionFactory for MockConnectionFactory {
    async fn establish_connection(
        &self,
        endpoint: SocketAddr,
    ) -> Result<Box<dyn Connection>, ConnectionFactoryError> {
        let mock = MockConnection::new(endpoint);
        Ok(Box::new(mock) as Box<dyn Connection>)
    }
}

async fn verify_connection(conn: &mut dyn Connection) -> Result<(), ClientConnectionError> {
    let request = Requests::Hello(HelloCommand {
        high_version: WIRE_VERSION,
        low_version: OLDEST_COMPATIBLE_VERSION,
    });
    write_wirecommand(conn, &request).await?;
    let reply = read_wirecommand(conn).await?;

    match reply {
        Replies::Hello(cmd) => {
            if cmd.low_version <= WIRE_VERSION && cmd.high_version >= WIRE_VERSION {
                Ok(())
            } else {
                Err(ClientConnectionError::WrongHelloVersion {
                    wire_version: WIRE_VERSION,
                    oldest_compatible: OLDEST_COMPATIBLE_VERSION,
                    wire_version_received: cmd.high_version,
                    oldest_compatible_received: cmd.low_version,
                })
            }
        }
        _ => Err(ClientConnectionError::WrongReply { reply }),
    }
}

/// An implementation of the Manager trait to integrate with ConnectionPool.
/// This is for creating connections between Rust client and Segmentstore server.
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
        let result = self.connection_factory.establish_connection(endpoint).await;

        match result {
            Ok(conn) => Ok(conn),
            Err(_e) => Err(ConnectionPoolError::EstablishConnection {
                endpoint: endpoint.to_string(),
                error_msg: String::from("Could not establish connection"),
            }),
        }
    }

    fn is_valid(&self, conn: &Self::Conn) -> bool {
        conn.is_valid()
    }

    fn get_max_connections(&self) -> u32 {
        self.max_connections_in_pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire_commands::{Decode, Encode};
    use log::info;
    use std::net::SocketAddr;
    use tokio::runtime::Runtime;

    #[test]
    fn test_mock_connection() {
        info!("test mock connection factory");
        let mut rt = Runtime::new().unwrap();

        let connection_factory = ConnectionFactory::create(ConnectionType::Mock);
        let connection_future =
            connection_factory.establish_connection("127.1.1.1:9090".parse::<SocketAddr>().unwrap());
        let mut mock_connection = rt.block_on(connection_future).unwrap();

        let request = Requests::Hello(HelloCommand {
            high_version: 9,
            low_version: 5,
        })
        .write_fields()
        .unwrap();
        let len = request.len();
        rt.block_on(mock_connection.send_async(&request))
            .expect("write to mock connection");
        let mut buf = vec![0; len];
        rt.block_on(mock_connection.read_async(&mut buf))
            .expect("read from mock connection");
        let reply = Replies::read_from(&buf).unwrap();
        let expected = Replies::Hello(HelloCommand {
            high_version: 9,
            low_version: 5,
        });
        assert_eq!(reply, expected);
        info!("mock connection factory test passed");
    }

    #[test]
    #[should_panic]
    fn test_tokio_connection() {
        info!("test tokio connection factory");
        let mut rt = Runtime::new().unwrap();

        let connection_factory = ConnectionFactory::create(ConnectionType::Tokio);
        let connection_future =
            connection_factory.establish_connection("127.1.1.1:9090".parse::<SocketAddr>().unwrap());
        let mut _connection = rt.block_on(connection_future).expect("create tokio connection");

        info!("tokio connection factory test passed");
    }
}
