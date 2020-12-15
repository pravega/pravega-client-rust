//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//

use crate::error::*;
use async_trait::async_trait;
use pravega_client_shared::PravegaNodeUri;
use snafu::ResultExt;
use std::fmt;
use std::fmt::{Debug, Formatter};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use uuid::Uuid;

/// Connection can send and read data using a TCP connection
#[async_trait]
pub trait Connection: Send + Sync + Debug {
    /// send_async will send a byte array payload to the remote server asynchronously.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryConfig};
    /// use pravega_client_shared::PravegaNodeUri;
    /// use pravega_client_config::connection_type::ConnectionType;
    /// use tokio::runtime::Runtime;
    ///
    /// fn main() {
    ///   let mut rt = Runtime::new().unwrap();
    ///   let endpoint = PravegaNodeUri::from("localhost:8080".to_string());
    ///   let config = ConnectionFactoryConfig::new(ConnectionType::Tokio);
    ///   let cf = ConnectionFactory::create(config);
    ///   let connection_future = cf.establish_connection(endpoint);
    ///   let mut connection = rt.block_on(connection_future).unwrap();
    ///   let mut payload: Vec<u8> = Vec::new();
    ///   let fut = connection.send_async(&payload);
    /// }
    /// ```
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError>;

    /// read_async will read exactly the amount of data needed to fill the provided buffer asynchronously.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryConfig};
    /// use pravega_client_shared::PravegaNodeUri;
    /// use pravega_client_config::connection_type::ConnectionType;
    /// use tokio::runtime::Runtime;
    ///
    /// fn main() {
    ///   let mut rt = Runtime::new().unwrap();
    ///   let endpoint = PravegaNodeUri::from("localhost:8080".to_string());
    ///   let config = ConnectionFactoryConfig::new(ConnectionType::Tokio);
    ///   let cf = ConnectionFactory::create(config);
    ///   let connection_future = cf.establish_connection(endpoint);
    ///   let mut connection = rt.block_on(connection_future).unwrap();
    ///   let mut buf = [0; 10];
    ///   let fut = connection.read_async(&mut buf);
    /// }
    /// ```
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError>;

    /// Splits the Connection into a read half and a writer half so they can be owned
    /// by different threads.
    fn split(&mut self) -> (Box<dyn ConnectionReadHalf>, Box<dyn ConnectionWriteHalf>);

    /// Returns the endpoint of this Connection.
    fn get_endpoint(&self) -> PravegaNodeUri;

    /// Returns the id of this Connection.
    fn get_uuid(&self) -> Uuid;

    /// Checks if this connection is valid. A Connection is considered to be invalid after
    /// split so it can be discarded when returning to the connection pol.
    fn is_valid(&self) -> bool;
}

/// The underlying connection is using Tokio TcpStream.
pub struct TokioConnection {
    pub uuid: Uuid,
    pub endpoint: PravegaNodeUri,
    pub stream: Option<TcpStream>,
}

#[async_trait]
impl Connection for TokioConnection {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        assert!(!self.stream.is_none());

        let endpoint = self.endpoint.clone();
        self.stream
            .as_mut()
            .expect("get connection")
            .write_all(payload)
            .await
            .context(SendData { endpoint })?;
        Ok(())
    }

    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError> {
        assert!(!self.stream.is_none());

        let endpoint = self.endpoint.clone();
        self.stream
            .as_mut()
            .expect("get connection")
            .read_exact(buf)
            .await
            .context(ReadData { endpoint })?;
        Ok(())
    }

    fn split(&mut self) -> (Box<dyn ConnectionReadHalf>, Box<dyn ConnectionWriteHalf>) {
        assert!(!self.stream.is_none());

        let (read_half, write_half) = tokio::io::split(self.stream.take().expect("take connection"));
        let read = Box::new(ConnectionReadHalfTokio {
            uuid: self.uuid,
            endpoint: self.endpoint.clone(),
            read_half: Some(read_half),
        }) as Box<dyn ConnectionReadHalf>;
        let write = Box::new(ConnectionWriteHalfTokio {
            uuid: self.uuid,
            endpoint: self.endpoint.clone(),
            write_half: Some(write_half),
        }) as Box<dyn ConnectionWriteHalf>;
        (read, write)
    }

    fn get_endpoint(&self) -> PravegaNodeUri {
        self.endpoint.clone()
    }

    fn get_uuid(&self) -> Uuid {
        self.uuid
    }

    fn is_valid(&self) -> bool {
        self.stream.as_ref().expect("get connection").is_valid()
    }
}

impl Debug for TokioConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConnection")
            .field("connection id", &self.uuid)
            .field("pravega endpoint", &self.endpoint)
            .finish()
    }
}

pub struct TlsConnection {
    pub uuid: Uuid,
    pub endpoint: PravegaNodeUri,
    pub stream: Option<TlsStream<TcpStream>>,
}

#[async_trait]
impl Connection for TlsConnection {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        assert!(!self.stream.is_none());

        let endpoint = self.endpoint.clone();
        self.stream
            .as_mut()
            .expect("get connection")
            .write_all(payload)
            .await
            .context(SendData { endpoint })?;
        Ok(())
    }

    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError> {
        assert!(!self.stream.is_none());

        let endpoint = self.endpoint.clone();
        self.stream
            .as_mut()
            .expect("get connection")
            .read_exact(buf)
            .await
            .context(ReadData { endpoint })?;
        Ok(())
    }

    fn split(&mut self) -> (Box<dyn ConnectionReadHalf>, Box<dyn ConnectionWriteHalf>) {
        assert!(!self.stream.is_none());

        let (read_half, write_half) = tokio::io::split(self.stream.take().expect("take connection"));
        let read = Box::new(ConnectionReadHalfTls {
            uuid: self.uuid,
            endpoint: self.endpoint.clone(),
            read_half: Some(read_half),
        }) as Box<dyn ConnectionReadHalf>;
        let write = Box::new(ConnectionWriteHalfTls {
            uuid: self.uuid,
            endpoint: self.endpoint.clone(),
            write_half: Some(write_half),
        }) as Box<dyn ConnectionWriteHalf>;
        (read, write)
    }

    fn get_endpoint(&self) -> PravegaNodeUri {
        self.endpoint.clone()
    }

    fn get_uuid(&self) -> Uuid {
        self.uuid
    }

    fn is_valid(&self) -> bool {
        self.stream.as_ref().expect("get connection").is_valid()
    }
}

impl Debug for TlsConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConnection")
            .field("connection id", &self.uuid)
            .field("pravega endpoint", &self.endpoint)
            .finish()
    }
}

#[async_trait]
pub trait ConnectionReadHalf: Send + Sync {
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError>;

    fn get_id(&self) -> Uuid;
}

pub struct ConnectionReadHalfTokio {
    uuid: Uuid,
    endpoint: PravegaNodeUri,
    read_half: Option<ReadHalf<TcpStream>>,
}

#[async_trait]
impl ConnectionReadHalf for ConnectionReadHalfTokio {
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError> {
        let endpoint = self.endpoint.clone();
        if let Some(ref mut reader) = self.read_half {
            reader.read_exact(buf).await.context(ReadData { endpoint })?;
        } else {
            panic!("should not try to read when read half is gone");
        }
        Ok(())
    }

    fn get_id(&self) -> Uuid {
        self.uuid
    }
}

pub struct ConnectionReadHalfTls {
    uuid: Uuid,
    endpoint: PravegaNodeUri,
    read_half: Option<ReadHalf<TlsStream<TcpStream>>>,
}

#[async_trait]
impl ConnectionReadHalf for ConnectionReadHalfTls {
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError> {
        let endpoint = self.endpoint.clone();
        if let Some(ref mut reader) = self.read_half {
            reader.read_exact(buf).await.context(ReadData { endpoint })?;
        } else {
            panic!("should not try to read when read half is gone");
        }
        Ok(())
    }

    fn get_id(&self) -> Uuid {
        self.uuid
    }
}

#[async_trait]
pub trait ConnectionWriteHalf: Send + Sync + Debug {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError>;

    fn get_id(&self) -> Uuid;
}

#[derive(Debug)]
pub struct ConnectionWriteHalfTokio {
    uuid: Uuid,
    endpoint: PravegaNodeUri,
    write_half: Option<WriteHalf<TcpStream>>,
}

#[async_trait]
impl ConnectionWriteHalf for ConnectionWriteHalfTokio {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        let endpoint = self.endpoint.clone();
        if let Some(ref mut writer) = self.write_half {
            writer.write_all(payload).await.context(SendData { endpoint })?;
        } else {
            panic!("should not try to write when write half is gone");
        }
        Ok(())
    }

    fn get_id(&self) -> Uuid {
        self.uuid
    }
}

#[derive(Debug)]
pub struct ConnectionWriteHalfTls {
    uuid: Uuid,
    endpoint: PravegaNodeUri,
    write_half: Option<WriteHalf<TlsStream<TcpStream>>>,
}

#[async_trait]
impl ConnectionWriteHalf for ConnectionWriteHalfTls {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        let endpoint = self.endpoint.clone();
        if let Some(ref mut writer) = self.write_half {
            writer.write_all(payload).await.context(SendData { endpoint })?;
        } else {
            panic!("should not try to write when write half is gone");
        }
        Ok(())
    }

    fn get_id(&self) -> Uuid {
        self.uuid
    }
}

pub trait Validate {
    fn is_valid(&self) -> bool;
}

impl Validate for TcpStream {
    fn is_valid(&self) -> bool {
        self.peer_addr().map_or_else(|_e| false, |_addr| true)
    }
}

impl Validate for TlsStream<TcpStream> {
    fn is_valid(&self) -> bool {
        let (io, _session) = self.get_ref();
        io.peer_addr().map_or_else(|_e| false, |_addr| true)
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {}
}
