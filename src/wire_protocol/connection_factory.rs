//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
use crate::wire_protocol::connection_factory::ConnectionType::Tokio;
use async_trait::async_trait;
use log::error;
use log::info;
use snafu::{ResultExt, Snafu};
use std::fmt;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

#[allow(dead_code)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ConnectionType {
    Tokio,
}

impl Default for ConnectionType {
    fn default() -> Self {
        Tokio
    }
}

impl fmt::Display for ConnectionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Debug, Snafu)]
pub enum ConnectionFactoryError {
    #[snafu(display(
        "Could not connect to endpoint {} using connection type {}",
        endpoint,
        connection_type
    ))]
    Connect {
        connection_type: ConnectionType,
        endpoint: SocketAddr,
        source: std::io::Error,
    },
    #[snafu(display("Could not send data to {} asynchronously", endpoint))]
    SendData {
        endpoint: SocketAddr,
        source: std::io::Error,
    },
    #[snafu(display("Could not read data from {} asynchronously", endpoint))]
    ReadData {
        endpoint: SocketAddr,
        source: std::io::Error,
    },
}

type Result<T, E = ConnectionFactoryError> = std::result::Result<T, E>;

/// ConnectionFactory trait is the factory used to establish the TCP connection with remote servers.
#[async_trait]
pub trait ConnectionFactory {
    /// establish_connection will return a Connection future that used to send and read data.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::net::SocketAddr;
    /// use pravega_client_rust::wire_protocol::connection_factory;
    /// use pravega_client_rust::wire_protocol::connection_factory::ConnectionFactory;
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
    ) -> Result<Box<dyn Connection>>;
}

/// Connection can send and read data using a TCP connection
#[async_trait]
pub trait Connection: Send {
    /// send_async will send a byte array payload to the remote server asynchronously.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::net::SocketAddr;
    /// use pravega_client_rust::wire_protocol::connection_factory;
    /// use pravega_client_rust::wire_protocol::connection_factory::ConnectionFactory;
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
    async fn send_async(&mut self, payload: &[u8]) -> Result<()>;

    /// read_async will read exactly the amount of data needed to fill the provided buffer asynchronously.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::net::SocketAddr;
    /// use pravega_client_rust::wire_protocol::connection_factory;
    /// use pravega_client_rust::wire_protocol::connection_factory::ConnectionFactory;
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
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<()>;

    fn get_uuid(&self) -> Uuid;
}

pub struct ConnectionFactoryImpl {}

impl ConnectionFactoryImpl {
    async fn establish_tokio_connection(
        &self,
        endpoint: SocketAddr,
    ) -> Result<Box<dyn Connection>> {
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
    ) -> Result<Box<dyn Connection>> {
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
    async fn send_async(&mut self, payload: &[u8]) -> Result<()> {
        println!("sending message");
        let endpoint = self.endpoint;
        self.stream
            .write_all(payload)
            .await
            .context(SendData { endpoint })?;
        println!("message sent");
        Ok(())
    }

    async fn read_async(&mut self, buf: &mut [u8]) -> Result<()> {
        let endpoint = self.endpoint;
        self.stream
            .read_exact(buf)
            .await
            .context(ReadData { endpoint })?;
        Ok(())
    }

    fn get_uuid(&self) -> Uuid {
        self.uuid
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::info;
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
            info!("server created");
            Server { address, listener }
        }

        pub fn echo(&mut self) {
            for stream in self.listener.incoming() {
                let mut stream = stream.unwrap();
                stream.write(b"Hello World\r\n").unwrap();
                break;
            }
            info!("echo back");
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
        info!("connection established");

        let mut payload: Vec<u8> = Vec::new();
        payload.push(12);
        let fut = connection.send_async(&payload);

        let _res = rt.block_on(fut).unwrap();
        info!("payload sent");

        server.echo();
        let mut buf = [0; 13];

        let fut = connection.read_async(&mut buf);
        let _res = rt.block_on(fut).unwrap();

        let echo = "Hello World\r\n".as_bytes();
        assert_eq!(buf, &echo[..]);
        info!("Testing connection passed");
    }
}
