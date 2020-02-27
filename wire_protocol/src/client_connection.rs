//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

extern crate byteorder;
use super::error::*;
use crate::commands::MAX_WIRECOMMAND_SIZE;
use crate::connection_pool::PooledConnection;
use crate::wire_commands::{Decode, Encode, Replies, Requests};
use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt};
use snafu::{ensure, ResultExt};
use std::io::Cursor;

pub const LENGTH_FIELD_OFFSET: u32 = 4;
pub const LENGTH_FIELD_LENGTH: u32 = 4;

/// ClientConnection is on top of the Connection. It can read or write wirecommand instead of raw bytes.
#[async_trait]
pub trait ClientConnection: Send + Sync {
    async fn read(&mut self) -> Result<Replies, ClientConnectionError>;
    async fn write(&mut self, command: &Requests) -> Result<(), ClientConnectionError>;
}

pub struct ClientConnectionImpl<'a> {
    pub connection: PooledConnection<'a>,
}

impl<'a> ClientConnectionImpl<'a> {
    pub fn new(connection: PooledConnection<'a>) -> Self {
        ClientConnectionImpl { connection }
    }
}

#[async_trait]
impl ClientConnection for ClientConnectionImpl<'_> {
    async fn read(&mut self) -> Result<Replies, ClientConnectionError> {
        let mut header: Vec<u8> = vec![0; LENGTH_FIELD_OFFSET as usize + LENGTH_FIELD_LENGTH as usize];
        self.connection.read_async(&mut header[..]).await.context(Read {
            part: "header".to_string(),
        })?;
        let mut rdr = Cursor::new(&header[4..8]);
        let payload_length = rdr.read_u32::<BigEndian>().expect("exact size");
        ensure!(
            payload_length <= MAX_WIRECOMMAND_SIZE,
            PayloadLengthTooLong {
                payload_size: payload_length,
                max_wirecommand_size: MAX_WIRECOMMAND_SIZE
            }
        );
        let mut payload: Vec<u8> = vec![0; payload_length as usize];
        self.connection.read_async(&mut payload[..]).await.context(Read {
            part: "payload".to_string(),
        })?;
        let concatenated = [&header[..], &payload[..]].concat();
        let reply: Replies = Replies::read_from(&concatenated).expect("decode reply");
        Ok(reply)
    }

    async fn write(&mut self, request: &Requests) -> Result<(), ClientConnectionError> {
        let payload = request.write_fields().expect("encode request");
        self.connection.send_async(&payload).await.context(Write {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_config::ClientConfigBuilder;
    use crate::commands::HelloCommand;
    use crate::connection_factory::ConnectionFactoryImpl;
    use crate::connection_pool::{ConnectionPool, ConnectionPoolImpl};
    use crate::wire_commands::{Encode, Replies};
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
            let address = listener.local_addr().expect("get listener address");
            info!("server created");
            Server { address, listener }
        }

        pub fn send_hello_wirecommand(&mut self) {
            let hello = Replies::Hello(HelloCommand {
                high_version: 9,
                low_version: 5,
            })
            .write_fields()
            .expect("serialize wirecommand");
            for stream in self.listener.incoming() {
                let mut stream = stream.expect("get tcp stream");
                stream.write(&hello).expect("reply with hello wirecommand");
                break;
            }
            info!("sent wirecommand");
        }
    }
    #[test]
    fn client_connection_read() {
        let mut rt = Runtime::new().expect("create tokio Runtime");

        let mut server = Server::new();

        let connection_factory = ConnectionFactoryImpl {};
        let pool = ConnectionPoolImpl::new(
            Box::new(connection_factory),
            ClientConfigBuilder::default()
                .build()
                .expect("build client config"),
        );
        let connection = rt
            .block_on(pool.get_connection(server.address))
            .expect("get connection from pool");
        info!("connection established");

        // server send wirecommand
        server.send_hello_wirecommand();

        // read wirecommand
        let mut reader = ClientConnectionImpl { connection };

        let fut = reader.read();
        let reply = rt.block_on(fut).expect("get reply from server");

        assert_eq!(
            reply,
            Replies::Hello(HelloCommand {
                high_version: 9,
                low_version: 5,
            })
        );
        info!("Testing wirecommand reader passed");
    }
}
