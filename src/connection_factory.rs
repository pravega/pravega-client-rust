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

pub trait ConnectionFactory {
    fn establish_connection(&self, endpoint: &str) -> Result<Connection, Box<dyn Error>> {
        let address = endpoint.parse()?;
        Ok(Connection { address })
    }
}

pub struct ConnectionFactoryImpl {}

impl ConnectionFactory for ConnectionFactoryImpl {}

pub struct Connection {
    pub address: SocketAddr,
}

impl Connection {
    pub async fn send_async(&self, data: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        let mut stream = TcpStream::connect(&self.address).await?;
        stream.write_all(data).await?;
        Ok(())
    }

    pub fn send(&self, data: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        use std::io::prelude::*;
        use std::net::TcpStream;

        let mut stream = TcpStream::connect(&self.address)?;

        stream.write(data)?;
        Ok(())
    }
}