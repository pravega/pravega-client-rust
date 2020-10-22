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
use crate::commands::{HelloCommand, TableKey, TableValue, OLDEST_COMPATIBLE_VERSION, WIRE_VERSION};
use crate::connection::{Connection, TlsConnection, TokioConnection};
use crate::error::*;
use crate::mock_connection::MockConnection;
use crate::wire_commands::{Replies, Requests};
use async_trait::async_trait;
use pravega_connection_pool::connection_pool::{ConnectionPoolError, Manager};
use pravega_rust_client_config::connection_type::MockType;
use pravega_rust_client_config::{connection_type::ConnectionType, ClientConfig};
use pravega_rust_client_shared::{PravegaNodeUri, SegmentInfo};
use snafu::ResultExt;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::{rustls, webpki::DNSNameRef, TlsConnector};
use tracing::info;
use uuid::Uuid;

/// ConnectionFactory trait is the factory used to establish the TCP connection with remote servers.
#[async_trait]
pub trait ConnectionFactory: Send + Sync {
    /// establish_connection will return a Connection future that used to send and read data.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryConfig};
    /// use pravega_rust_client_shared::PravegaNodeUri;
    /// use pravega_rust_client_config::connection_type::ConnectionType;
    /// use tokio::runtime::Runtime;
    ///
    /// fn main() {
    ///   let mut rt = Runtime::new().unwrap();
    ///   let endpoint = PravegaNodeUri::from("localhost:9090".to_string());
    ///   let config = ConnectionFactoryConfig::new(ConnectionType::Tokio);
    ///   let cf = ConnectionFactory::create(config);
    ///   let connection_future = cf.establish_connection(endpoint);
    ///   let mut connection = rt.block_on(connection_future).unwrap();
    /// }
    /// ```
    async fn establish_connection(
        &self,
        endpoint: PravegaNodeUri,
    ) -> Result<Box<dyn Connection>, ConnectionFactoryError>;
}

impl dyn ConnectionFactory {
    pub fn create(config: ConnectionFactoryConfig) -> Box<dyn ConnectionFactory> {
        match config.connection_type {
            ConnectionType::Tokio => Box::new(TokioConnectionFactory::new(
                config.is_tls_enabled,
                &config.cert_path,
            )),
            ConnectionType::Mock(mock_type) => Box::new(MockConnectionFactory::new(mock_type)),
        }
    }
}

struct TokioConnectionFactory {
    tls_enabled: bool,
    path: String,
}

impl TokioConnectionFactory {
    fn new(tls_enabled: bool, path: &str) -> Self {
        TokioConnectionFactory {
            tls_enabled,
            path: path.to_owned(),
        }
    }
}

#[async_trait]
impl ConnectionFactory for TokioConnectionFactory {
    async fn establish_connection(
        &self,
        endpoint: PravegaNodeUri,
    ) -> Result<Box<dyn Connection>, ConnectionFactoryError> {
        let connection_type = ConnectionType::Tokio;
        let uuid = Uuid::new_v4();
        let mut tokio_connection = if self.tls_enabled {
            info!(
                "establish connection to segmentstore {:?} using TLS channel",
                endpoint
            );
            let mut config = rustls::ClientConfig::new();
            let mut pem = BufReader::new(File::open(&self.path).expect("open pem file"));
            config.root_store.add_pem_file(&mut pem).expect("add pem file");
            let connector = TlsConnector::from(Arc::new(config));
            let stream = TcpStream::connect(endpoint.to_socket_addr())
                .await
                .context(Connect {
                    connection_type,
                    endpoint: endpoint.clone(),
                })?;
            // Endpoint returned by controller by default is an IP address, it is necessary to configure
            // Pravega to return a hostname. Check pravegaservice.service.published.host.nameOrIp property.
            let domain_name = endpoint.domain_name();
            let domain = DNSNameRef::try_from_ascii_str(&domain_name).expect("get domain name");
            let stream = connector
                .connect(domain, stream)
                .await
                .expect("connect to tls stream");
            Box::new(TlsConnection {
                uuid,
                endpoint: endpoint.clone(),
                stream: Some(stream),
            }) as Box<dyn Connection>
        } else {
            let stream = TcpStream::connect(endpoint.to_socket_addr())
                .await
                .context(Connect {
                    connection_type,
                    endpoint: endpoint.clone(),
                })?;
            Box::new(TokioConnection {
                uuid,
                endpoint: endpoint.clone(),
                stream: Some(stream),
            }) as Box<dyn Connection>
        };
        verify_connection(&mut *tokio_connection)
            .await
            .context(Verify {})?;
        Ok(tokio_connection)
    }
}
struct MockConnectionFactory {
    segments: Arc<Mutex<HashMap<String, SegmentInfo>>>,
    writers: Arc<Mutex<HashMap<u128, String>>>,
    table_segments: Arc<Mutex<HashMap<String, HashMap<TableKey, TableValue>>>>,
    mock_type: MockType,
}

impl MockConnectionFactory {
    pub fn new(mock_type: MockType) -> Self {
        MockConnectionFactory {
            segments: Arc::new(Mutex::new(HashMap::new())),
            writers: Arc::new(Mutex::new(HashMap::new())),
            table_segments: Arc::new(Mutex::new(HashMap::new())),
            mock_type,
        }
    }
}

#[async_trait]
impl ConnectionFactory for MockConnectionFactory {
    async fn establish_connection(
        &self,
        endpoint: PravegaNodeUri,
    ) -> Result<Box<dyn Connection>, ConnectionFactoryError> {
        let mock = MockConnection::new(
            endpoint,
            self.segments.clone(),
            self.writers.clone(),
            self.table_segments.clone(),
            self.mock_type,
        );
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

    async fn establish_connection(
        &self,
        endpoint: PravegaNodeUri,
    ) -> Result<Self::Conn, ConnectionPoolError> {
        let result = self
            .connection_factory
            .establish_connection(endpoint.clone())
            .await;

        match result {
            Ok(conn) => Ok(conn),
            Err(e) => Err(ConnectionPoolError::EstablishConnection {
                endpoint: endpoint.to_string(),
                error_msg: format!("Could not establish connection due to {:?}", e),
            }),
        }
    }

    fn is_valid(&self, conn: &Self::Conn) -> bool {
        conn.is_valid()
    }

    fn get_max_connections(&self) -> u32 {
        self.max_connections_in_pool
    }

    fn name(&self) -> String {
        "SegmentConnectionManager".to_owned()
    }
}

impl fmt::Debug for SegmentConnectionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentConnectionManager")
            .field("max connections in pool", &self.max_connections_in_pool)
            .finish()
    }
}

/// The configuration for ConnectionFactory.
#[derive(new)]
pub struct ConnectionFactoryConfig {
    connection_type: ConnectionType,
    #[new(value = "false")]
    is_tls_enabled: bool,
    #[new(default)]
    cert_path: String,
}

/// ConnectionFactoryConfig can be built from ClientConfig.
impl From<&ClientConfig> for ConnectionFactoryConfig {
    fn from(client_config: &ClientConfig) -> Self {
        ConnectionFactoryConfig {
            connection_type: client_config.connection_type,
            is_tls_enabled: client_config.is_tls_enabled,
            cert_path: client_config.trustcert.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire_commands::{Decode, Encode};
    use log::info;
    use pravega_rust_client_config::connection_type::{ConnectionType, MockType};
    use tokio::runtime::Runtime;

    #[test]
    fn test_mock_connection() {
        info!("test mock connection factory");
        let mut rt = Runtime::new().unwrap();
        let config = ConnectionFactoryConfig::new(ConnectionType::Mock(MockType::Happy));
        let connection_factory = ConnectionFactory::create(config);
        let connection_future =
            connection_factory.establish_connection(PravegaNodeUri::from("127.1.1.1:9090"));
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
        let config = ConnectionFactoryConfig::new(ConnectionType::Tokio);
        let connection_factory = ConnectionFactory::create(config);
        let connection_future =
            connection_factory.establish_connection(PravegaNodeUri::from("127.1.1.1:9090".to_string()));
        let mut _connection = rt.block_on(connection_future).expect("create tokio connection");

        info!("tokio connection factory test passed");
    }
}
