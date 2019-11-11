/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
extern crate tokio;

use std::{error::Error, net::SocketAddr};
use tokio::net::TcpStream;
use async_trait::async_trait;
use self::tokio::io::{AsyncWriteExt, AsyncReadExt};

pub enum ConnectionType {
    Tokio,
}

/**
 * ConnectionFactory trait is the factory used to establish the TCP connection with remote servers.
 *
 */
#[async_trait]
pub trait ConnectionFactory {
    /**
     * establish_connection will return a Connection future that used to send and read data.
     *
     * # Example
     *
     * ```
     * use std::net::SocketAddr;
     *
     * fn main() {
     *   let endpoint: SocketAddr = endpoint.parse("127.0.0.1:0").expect("Unable to parse socket address");
     *   let connection_factory = connection_factory::ConnectionFactoryImpl {};
     *   let connection_future = connection_factory.establish_connection(connection_factory::ConnectionType::Tokio, endpoint);
     *   let mut connection = rt.block_on(connection_future).unwrap();
     * }
     * ```
     */
    async fn establish_connection(&self, connection_type: ConnectionType, endpoint: SocketAddr) -> Result<Box<dyn Connection>, Box<dyn Error>>;
}

/**
 * Connection can send and read data using  a TCP connection
 *
 */
#[async_trait]
pub trait Connection {
    /**
    * send_async will send a byte array payload to the remote server asynchronously.
    *
    * # Example
    *
    * ```
    * let mut payload: Vec<u8> = Vec::new();
    * let fut = connection.send_async(&payload);
    * ```
    */
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), Box<dyn Error>>;

    /**
     * read_async will read data into a byte buffer from the remote server asynchronously.
     *
     * # Example
     *
     * ```
     * let mut buf = [0; 10];
     * let fut = connection.read_async(&payload);
     * ```
     */
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<usize, Box<dyn Error>>;
}

pub struct ConnectionFactoryImpl {}

#[async_trait]
impl ConnectionFactory for ConnectionFactoryImpl {
    async fn establish_connection(&self, connection_type: ConnectionType, endpoint: SocketAddr) -> Result<Box<dyn Connection>, Box<dyn Error>> {
        match connection_type {
            ConnectionType::Tokio => {
                let stream = TcpStream::connect(endpoint).await?;
                let tokio_connection: Box<dyn Connection> = Box::new(TokioConnection { endpoint, stream }) as Box<dyn Connection>;
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
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), Box<dyn Error>> {
        self.stream.write_all(payload).await?;
        Ok(())
    }

    async fn read_async(&mut self, buf: &mut [u8]) -> Result<usize, Box<dyn Error>> {
        let read = self.stream.read(buf).await?;
        Ok(read)
    }
}