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
use tokio::prelude::*;
use async_trait::async_trait;
use self::tokio::io::{AsyncWriteExt, AsyncReadExt};

#[async_trait]
pub trait ConnectionFactory {
    async fn establish_connection(&self, endpoint: SocketAddr) -> Result<Box<dyn Connection>, Box<dyn Error>>;
}

#[async_trait]
pub trait Connection {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), Box<dyn Error>>;

    async fn read_async(&mut self, buf: &mut Vec<u8>) -> Result<(), Box<dyn Error>>;
}

pub struct ConnectionFactoryImpl {}

#[async_trait]
impl ConnectionFactory for ConnectionFactoryImpl {
    async fn establish_connection(&self, endpoint: SocketAddr) -> Result<Box<dyn Connection>, Box<dyn Error>> {
        let mut stream = TcpStream::connect(endpoint).await?;
        Ok(Box::new(ConnectionImpl { endpoint, stream }))
    }
}

pub struct ConnectionImpl {
    pub endpoint: SocketAddr,
    pub stream: TcpStream,
}

#[async_trait]
impl Connection for ConnectionImpl {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), Box<dyn Error>> {
        self.stream.write_all(payload).await?;
        Ok(())
    }

    async fn read_async(&mut self, buf: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.stream.read_to_end(buf).await?;
        Ok(())
    }
}