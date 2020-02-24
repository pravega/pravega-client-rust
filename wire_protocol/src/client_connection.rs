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
use crate::connection_factory::Connection;
use byteorder::{BigEndian, ReadBytesExt};
use snafu::{ensure, ResultExt};
use std::io::Cursor;
use crate::wire_commands::{Replies, Requests, Encode, Decode};
use async_trait::async_trait;
use std::fmt;
use crate::reply_processor::FailingReplyProcessor;

pub const MAX_WIRECOMMAND_SIZE: u32 = 0x007F_FFFF;
pub const LENGTH_FIELD_OFFSET: u32 = 4;
pub const LENGTH_FIELD_LENGTH: u32 = 4;

#[async_trait]
pub trait ClientConnection {
    fn new(connection: Box<dyn Connection>, rp: Box<dyn FailingReplyProcessor>) -> Self;
    async fn read(&mut self) -> Result<Replies, ClientConnectionError>;
    async fn write(&mut self, command: Requests) -> Result<(), ClientConnectionError>;
}

pub struct ClientConnectionImpl {
    pub connection: Box<dyn Connection>,
    pub rp: Box<dyn FailingReplyProcessor>,
}

#[async_trait]
impl ClientConnection for ClientConnectionImpl {

    fn new(connection: Box<dyn Connection>, rp: Box<dyn FailingReplyProcessor>) -> Self {
        ClientConnectionImpl{connection, rp}
    }

    async fn read(&mut self) -> Result<Replies, ClientConnectionError> {
        let mut header: Vec<u8> = vec![0; LENGTH_FIELD_OFFSET as usize + LENGTH_FIELD_LENGTH as usize];
        self.connection
            .read_async(&mut header[..])
            .await
            .context(Read {
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
        self.connection
            .read_async(&mut payload[..])
            .await
            .context(Read {
                part: "payload".to_string(),
            })?;

        let concatenated = [&header[..], &payload[..]].concat();

        let reply: Replies = Replies::read_from(&concatenated).expect("decode reply");
        Ok(reply)
    }

    async fn write(&mut self, request: Requests) -> Result<(), ClientConnectionError> {
        let payload = request.write_fields().expect("encode request");
        self.connection.send_async(&payload).await.context(Write{})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_factory::{ConnectionFactory, ConnectionFactoryImpl, ConnectionType};
    use byteorder::{BigEndian, WriteBytesExt};
    use log::info;
    use std::io::Write;
    use std::net::{SocketAddr, TcpListener};
    use tokio::runtime::Runtime;
    use crate::commands::HelloCommand;
    use crate::wire_commands::{Replies, Requests, Encode, Decode};

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

        pub fn send_hello_wirecommand(&mut self) {
            let hello = Replies::Hello(HelloCommand {
                high_version: 9,
                low_version: 5,
            }).write_fields().unwrap();
            for stream in self.listener.incoming() {
                let mut stream = stream.unwrap();
                stream.write(&hello).unwrap();
                break;
            }
            info!("sent wirecommand");
        }
    }
    #[test]
    fn client_connection_read() {
        let mut rt = Runtime::new().unwrap();

        let mut server = Server::new();

        let connection_factory = ConnectionFactoryImpl {};
        let connection_future =
            connection_factory.establish_connection(server.address, ConnectionType::Tokio);
        let connection = rt.block_on(connection_future).unwrap();
        info!("connection established");

        // server send wirecommand
        server.send_hello_wirecommand();

        // read wirecommand
        let mut reader = ClientConnectionImpl { connection };

        let fut = reader.read();
        let reply = rt.block_on(fut).unwrap();

        assert_eq!(reply, Replies::Hello(HelloCommand {
            high_version: 9,
            low_version: 5,
        }));
        info!("Testing wirecommand reader passed");
    }
}
