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
use std::fmt;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{ReadHalf, WriteHalf};
use uuid::Uuid;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ConnectionType {
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
    ///   let cf = connection_factory::ConnectionFactoryImpl {};
    ///   let connection_future = cf.establish_connection(endpoint, connection_factory::ConnectionType::Tokio);
    ///   let mut connection = rt.block_on(connection_future).unwrap();
    /// }
    /// ```
    async fn establish_connection(
        &self,
        endpoint: SocketAddr,
        connection_type: ConnectionType,
    ) -> Result<Box<dyn Connection>, ConnectionError>;
}

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
    ///   let cf = connection_factory::ConnectionFactoryImpl {};
    ///   let connection_future = cf.establish_connection(endpoint, connection_factory::ConnectionType::Tokio);
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
    ///   let cf = connection_factory::ConnectionFactoryImpl {};
    ///   let connection_future = cf.establish_connection(endpoint, connection_factory::ConnectionType::Tokio);
    ///   let mut connection = rt.block_on(connection_future).unwrap();
    ///   let mut buf = [0; 10];
    ///   let fut = connection.read_async(&mut buf);
    /// }
    /// ```
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError>;

    fn split<'a>(&'a mut self) -> (Box<dyn ReadingConnection + 'a>, Box<dyn WritingConnection + 'a>);

    fn get_uuid(&self) -> Uuid;

    fn get_endpoint(&self) -> SocketAddr;
}

#[async_trait]
pub trait ReadingConnection: Send + Sync {
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError>;
}

struct ReadingConnectionImpl<'a> {
    endpoint: SocketAddr,
    read_half: ReadHalf<'a>,
}

#[async_trait]
impl ReadingConnection for ReadingConnectionImpl<'_> {
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError> {
        let endpoint = self.endpoint;
        self.read_half.read_exact(buf).await.context(ReadData { endpoint })?;
        Ok(())
    }
}
#[async_trait]
pub trait WritingConnection: Send + Sync {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError>;
}

struct WritingConnectionImpl<'a> {
    endpoint: SocketAddr,
    write_half: WriteHalf<'a>,
}

#[async_trait]
impl WritingConnection for WritingConnectionImpl<'_> {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        let endpoint = self.endpoint;
        self.write_half
            .write_all(payload)
            .await
            .context(SendData { endpoint })?;
        Ok(())
    }
}


#[derive(Debug)]
pub struct ConnectionFactoryImpl {}

impl ConnectionFactoryImpl {
    async fn establish_tokio_connection(
        &self,
        endpoint: SocketAddr,
    ) -> Result<Box<dyn Connection>, ConnectionError> {
        let connection_type = ConnectionType::Tokio;
        let uuid = Uuid::new_v4();
        let stream = TcpStream::connect(endpoint).await.context(Connect {
            connection_type,
            endpoint,
        })?;
        let tokio_connection: Box<dyn Connection> = Box::new(TokioConnection {
            uuid,
            endpoint,
            stream,
        }) as Box<dyn Connection>;
        Ok(tokio_connection)
    }
}

#[async_trait]
impl ConnectionFactory for ConnectionFactoryImpl {
    async fn establish_connection(
        &self,
        endpoint: SocketAddr,
        connection_type: ConnectionType,
    ) -> Result<Box<dyn Connection>, ConnectionError> {
        match connection_type {
            ConnectionType::Tokio => self.establish_tokio_connection(endpoint).await,
        }
    }
}

pub struct TokioConnection {
    pub uuid: Uuid,
    pub endpoint: SocketAddr,
    pub stream: TcpStream,
}

#[async_trait]
impl Connection for TokioConnection {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        let endpoint = self.endpoint;
        self.stream
            .write_all(payload)
            .await
            .context(SendData { endpoint })?;
        Ok(())
    }

    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError> {
        let endpoint = self.endpoint;
        self.stream.read_exact(buf).await.context(ReadData { endpoint })?;
        Ok(())
    }

    fn split<'a>(&'a mut self) -> (Box<dyn ReadingConnection + 'a>, Box<dyn WritingConnection + 'a>) {
        let (read_half, write_half): (ReadHalf<'a>, WriteHalf<'a>) = self.stream.split();
        let read = Box::new(ReadingConnectionImpl{endpoint: self.endpoint.clone(), read_half}) as Box<dyn ReadingConnection + 'a>;
        let write = Box::new(WritingConnectionImpl{endpoint: self.endpoint.clone(), write_half}) as Box<dyn WritingConnection + 'a>;
        (read, write)
    }

    fn get_uuid(&self) -> Uuid {
        self.uuid
    }

    fn get_endpoint(&self) -> SocketAddr {
        self.endpoint
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::{SocketAddr, TcpListener};
    use tokio::runtime::Runtime;

    struct Server {
        address: SocketAddr,
        listener: TcpListener,
    }

    impl Server {
        pub fn new() -> Server {
            let listener = TcpListener::bind("127.0.0.1:0").expect("local server");
            let address = listener.local_addr().unwrap();
            Server { address, listener }
        }

        pub fn echo(&mut self) {
            for stream in self.listener.incoming() {
                let mut stream = stream.unwrap();
                stream.write(b"Hello World\r\n").unwrap();
                break;
            }
        }
    }

    #[test]
    fn test_connection() {
        let mut rt = Runtime::new().unwrap();

        let mut server = Server::new();

        let connection_factory = self::ConnectionFactoryImpl {};
        let connection_future =
            connection_factory.establish_connection(server.address, ConnectionType::Tokio);
        let mut connection = rt.block_on(connection_future).unwrap();

        let mut payload: Vec<u8> = Vec::new();
        payload.push(12);
        let fut = connection.send_async(&payload);

        let _res = rt.block_on(fut).unwrap();

        server.echo();
        let mut buf = [0; 13];

        let fut = connection.read_async(&mut buf);
        let _res = rt.block_on(fut).unwrap();

        let echo = "Hello World\r\n".as_bytes();
        assert_eq!(buf, &echo[..]);
    }
}
