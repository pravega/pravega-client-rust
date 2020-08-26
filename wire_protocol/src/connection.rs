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
use pravega_rust_client_shared::PravegaNodeUri;
use snafu::ResultExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use uuid::Uuid;

/// Connection can send and read data using a TCP connection
#[async_trait]
pub trait Connection: Send + Sync {
    /// send_async will send a byte array payload to the remote server asynchronously.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use pravega_wire_protocol::connection_factory;
    /// use pravega_wire_protocol::connection_factory::ConnectionFactory;
    /// use pravega_rust_client_shared::PravegaNodeUri;
    /// use pravega_rust_client_config::ConnectionType;
    /// use tokio::runtime::Runtime;
    ///
    /// fn main() {
    ///   let mut rt = Runtime::new().unwrap();
    ///   let endpoint = PravegaNodeUri::from("localhost:8080".to_string());
    ///   let cf = connection_factory::ConnectionFactory::create(connection_factory::ConnectionType::Tokio);
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
    /// use pravega_wire_protocol::connection_factory;
    /// use pravega_wire_protocol::connection_factory::ConnectionFactory;
    /// use pravega_rust_client_shared::PravegaNodeUri;
    /// use pravega_rust_client_config::ConnectionType;
    /// use tokio::runtime::Runtime;
    ///
    /// fn main() {
    ///   let mut rt = Runtime::new().unwrap();
    ///   let endpoint = PravegaNodeUri::from("localhost:8080".to_string());
    ///   let cf = connection_factory::ConnectionFactory::create(connection_factory::ConnectionType::Tokio);
    ///   let connection_future = cf.establish_connection(endpoint);
    ///   let mut connection = rt.block_on(connection_future).unwrap();
    ///   let mut buf = [0; 10];
    ///   let fut = connection.read_async(&mut buf);
    /// }
    /// ```
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError>;

    fn split(&mut self) -> (Box<dyn ReadingConnection>, Box<dyn WritingConnection>);

    fn get_endpoint(&self) -> PravegaNodeUri;

    fn get_uuid(&self) -> Uuid;

    fn is_valid(&self) -> bool;
}

pub struct TokioConnection<Stream: AsyncReadExt + AsyncWriteExt + Send + Sync + Unpin + 'static> {
    pub uuid: Uuid,
    pub endpoint: PravegaNodeUri,
    pub stream: Option<Stream>,
}

#[async_trait]
impl<Stream: AsyncReadExt + AsyncWriteExt + Send + Sync + Unpin + 'static + Validate> Connection
    for TokioConnection<Stream>
{
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

    fn split(&mut self) -> (Box<dyn ReadingConnection>, Box<dyn WritingConnection>) {
        assert!(!self.stream.is_none());

        let (read_half, write_half) = tokio::io::split(self.stream.take().expect("take connection"));
        let read = Box::new(ReadingConnectionImpl {
            uuid: self.uuid,
            endpoint: self.endpoint.clone(),
            read_half,
        }) as Box<dyn ReadingConnection>;
        let write = Box::new(WritingConnectionImpl {
            uuid: self.uuid,
            endpoint: self.endpoint.clone(),
            write_half,
        }) as Box<dyn WritingConnection>;
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

#[async_trait]
pub trait ReadingConnection: Send + Sync {
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError>;
    fn get_id(&self) -> Uuid;
}

pub struct ReadingConnectionImpl<Stream: AsyncReadExt + Send + Sync + 'static> {
    uuid: Uuid,
    endpoint: PravegaNodeUri,
    read_half: ReadHalf<Stream>,
}

#[async_trait]
impl<Stream: AsyncReadExt + Send + Sync + 'static> ReadingConnection for ReadingConnectionImpl<Stream> {
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError> {
        let endpoint = self.endpoint.clone();
        self.read_half
            .read_exact(buf)
            .await
            .context(ReadData { endpoint })?;
        Ok(())
    }

    fn get_id(&self) -> Uuid {
        self.uuid
    }
}

#[async_trait]
pub trait WritingConnection: Send + Sync {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError>;
    fn get_id(&self) -> Uuid;
}

pub struct WritingConnectionImpl<Stream: AsyncWriteExt + Send + Sync + 'static> {
    uuid: Uuid,
    endpoint: PravegaNodeUri,
    write_half: WriteHalf<Stream>,
}

#[async_trait]
impl<Stream: AsyncWriteExt + Send + Sync + 'static> WritingConnection for WritingConnectionImpl<Stream> {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        let endpoint = self.endpoint.clone();
        self.write_half
            .write_all(payload)
            .await
            .context(SendData { endpoint })?;
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

impl<Stream> Validate for TlsStream<Stream> {
    fn is_valid(&self) -> bool {
        false
    }
}
