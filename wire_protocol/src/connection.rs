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
use snafu::ResultExt;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use uuid::Uuid;

/// Connection can send and read data using a TCP connection
#[async_trait]
pub trait Connection: Send + Sync {
    /// send_async will send a byte array payload to the remote server asynchronously.
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
    ///   let mut buf = [0; 10];
    ///   let fut = connection.read_async(&mut buf);
    /// }
    /// ```
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError>;

    fn split(&mut self) -> (Box<dyn ReadingConnection>, Box<dyn WritingConnection>);

    fn get_endpoint(&self) -> SocketAddr;

    fn get_uuid(&self) -> Uuid;

    fn is_valid(&self) -> bool;
}

pub struct TokioConnection {
    pub uuid: Uuid,
    pub endpoint: SocketAddr,
    pub stream: Option<TcpStream>,
}

#[async_trait]
impl Connection for TokioConnection {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        assert!(!self.stream.is_none());

        let endpoint = self.endpoint;
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

        let endpoint = self.endpoint;
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
            endpoint: self.endpoint,
            read_half,
        }) as Box<dyn ReadingConnection>;
        let write = Box::new(WritingConnectionImpl {
            uuid: self.uuid,
            endpoint: self.endpoint,
            write_half,
        }) as Box<dyn WritingConnection>;
        (read, write)
    }

    fn get_endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    fn get_uuid(&self) -> Uuid {
        self.uuid
    }

    fn is_valid(&self) -> bool {
        let result = self.stream.as_ref().expect("get connection").peer_addr();
        match result {
            Err(_e) => false,
            Ok(_addr) => true,
        }
    }
}

#[async_trait]
pub trait ReadingConnection: Send + Sync {
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError>;
    fn get_id(&self) -> Uuid;
}

pub struct ReadingConnectionImpl {
    uuid: Uuid,
    endpoint: SocketAddr,
    read_half: ReadHalf<TcpStream>,
}

#[async_trait]
impl ReadingConnection for ReadingConnectionImpl {
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError> {
        let endpoint = self.endpoint;
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

pub struct WritingConnectionImpl {
    uuid: Uuid,
    endpoint: SocketAddr,
    write_half: WriteHalf<TcpStream>,
}

#[async_trait]
impl WritingConnection for WritingConnectionImpl {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        let endpoint = self.endpoint;
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
