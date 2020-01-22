//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//

use super::error::Connect;
use super::error::ConnectionError;
use super::error::ReadData;
use super::error::SendData;
use async_trait::async_trait;
use snafu::ResultExt;
use std::fmt;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug)]
pub enum ConnectionType {
    Tokio,
}

impl fmt::Display for ConnectionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

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
    ///   let connection_future = cf.establish_connection(connection_factory::ConnectionType::Tokio, endpoint);
    ///   let mut connection = rt.block_on(connection_future).unwrap();
    /// }
    /// ```
    async fn establish_connection(
        &self,
        connection_type: ConnectionType,
        endpoint: SocketAddr,
    ) -> Result<Box<dyn Connection>, ConnectionError>;
}

/// Connection can send and read data using  a TCP connection
#[async_trait]
pub trait Connection {
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
    ///   let connection_future = cf.establish_connection(connection_factory::ConnectionType::Tokio, endpoint);
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
    /// use pravega_client_rust::wire_protocol::connection_factory;
    /// use pravega_client_rust::wire_protocol::connection_factory::ConnectionFactory;
    /// use tokio::runtime::Runtime;
    ///
    /// fn main() {
    ///   let mut rt = Runtime::new().unwrap();
    ///   let endpoint: SocketAddr = "127.0.0.1:0".parse().expect("Unable to parse socket address");
    ///   let cf = connection_factory::ConnectionFactoryImpl {};
    ///   let connection_future = cf.establish_connection(connection_factory::ConnectionType::Tokio, endpoint);
    ///   let mut connection = rt.block_on(connection_future).unwrap();
    ///   let mut buf = [0; 10];
    ///   let fut = connection.read_async(&mut buf);
    /// }
    /// ```
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError>;
}

pub struct ConnectionFactoryImpl {}

#[async_trait]
impl ConnectionFactory for ConnectionFactoryImpl {
    async fn establish_connection(
        &self,
        connection_type: ConnectionType,
        endpoint: SocketAddr,
    ) -> Result<Box<dyn Connection>, ConnectionError> {
        match connection_type {
            ConnectionType::Tokio => {
                let stream = TcpStream::connect(endpoint).await.context(Connect {
                    connection_type,
                    endpoint,
                })?;
                let tokio_connection: Box<dyn Connection> =
                    Box::new(TokioConnection { endpoint, stream }) as Box<dyn Connection>;
                Ok(tokio_connection)
            }
        }
    }
}

pub struct TokioConnection {
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
            connection_factory.establish_connection(self::ConnectionType::Tokio, server.address);
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
