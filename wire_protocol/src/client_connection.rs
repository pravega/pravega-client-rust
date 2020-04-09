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
use crate::commands::MAX_WIRECOMMAND_SIZE;
use crate::connection::{Connection, ReadingConnection, WritingConnection};
use crate::connection_pool::PooledConnection;
use crate::error::*;
use crate::wire_commands::{Decode, Encode, Replies, Requests};
use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt};
use snafu::{ensure, ResultExt};
use std::io::Cursor;
use std::ops::DerefMut;
use uuid::Uuid;
pub const LENGTH_FIELD_OFFSET: u32 = 4;
pub const LENGTH_FIELD_LENGTH: u32 = 4;

/// ClientConnection is on top of the Connection. It can read or write wirecommand instead of raw bytes.
#[async_trait]
pub trait ClientConnection: Send + Sync {
    async fn read(&mut self) -> Result<Replies, ClientConnectionError>;
    async fn write(&mut self, request: &Requests) -> Result<(), ClientConnectionError>;
    fn split(&mut self) -> (ReadingClientConnection, WritingClientConnection);
    fn get_uuid(&self) -> Uuid;
}

pub struct ClientConnectionImpl<'a> {
    pub connection: PooledConnection<'a, Box<dyn Connection>>,
}

pub struct ReadingClientConnection {
    read_half: ReadingConnection,
}

pub struct WritingClientConnection {
    write_half: WritingConnection,
}

impl<'a> ClientConnectionImpl<'a> {
    pub fn new(connection: PooledConnection<'a, Box<dyn Connection>>) -> Self {
        ClientConnectionImpl { connection }
    }
}

impl ReadingClientConnection {
    pub async fn read(&mut self) -> Result<Replies, ClientConnectionError> {
        let mut header: Vec<u8> = vec![0; LENGTH_FIELD_OFFSET as usize + LENGTH_FIELD_LENGTH as usize];
        self.read_half.read_async(&mut header[..]).await.context(Read {
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
        self.read_half.read_async(&mut payload[..]).await.context(Read {
            part: "payload".to_string(),
        })?;
        let concatenated = [&header[..], &payload[..]].concat();
        let reply: Replies = Replies::read_from(&concatenated).context(DecodeCommand {})?;
        Ok(reply)
    }

    pub fn get_id(&self) -> Uuid {
        self.read_half.get_id()
    }
}

impl WritingClientConnection {
    pub async fn write(&mut self, request: &Requests) -> Result<(), ClientConnectionError> {
        let payload = request.write_fields().context(EncodeCommand {})?;
        self.write_half.send_async(&payload).await.context(Write {})
    }

    pub fn get_id(&self) -> Uuid {
        self.write_half.get_id()
    }
}

#[async_trait]
impl ClientConnection for ClientConnectionImpl<'_> {
    async fn read(&mut self) -> Result<Replies, ClientConnectionError> {
        read_wirecommand(&mut **self.connection.deref_mut()).await
    }

    async fn write(&mut self, request: &Requests) -> Result<(), ClientConnectionError> {
        write_wirecommand(&mut **self.connection.deref_mut(), request).await
    }

    fn split(&mut self) -> (ReadingClientConnection, WritingClientConnection) {
        let (r, w) = self.connection.split();
        self.connection.invalidate();
        let reader = ReadingClientConnection { read_half: r };
        let writer = WritingClientConnection { write_half: w };
        (reader, writer)
    }

    fn get_uuid(&self) -> Uuid {
        self.connection.get_uuid()
    }
}

pub async fn read_wirecommand(connection: &mut dyn Connection) -> Result<Replies, ClientConnectionError> {
    let mut header: Vec<u8> = vec![0; LENGTH_FIELD_OFFSET as usize + LENGTH_FIELD_LENGTH as usize];
    connection.read_async(&mut header[..]).await.context(Read {
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
    connection.read_async(&mut payload[..]).await.context(Read {
        part: "payload".to_string(),
    })?;
    let concatenated = [&header[..], &payload[..]].concat();
    let reply: Replies = Replies::read_from(&concatenated).context(DecodeCommand {})?;
    Ok(reply)
}

pub async fn write_wirecommand(
    connection: &mut dyn Connection,
    request: &Requests,
) -> Result<(), ClientConnectionError> {
    let payload = request.write_fields().context(EncodeCommand {})?;
    connection.send_async(&payload).await.context(Write {})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::HelloCommand;
    use crate::connection_factory::{ConnectionFactory, ConnectionType};
    use crate::connection_pool::{ConnectionPool, SegmentConnectionManager};
    use crate::wire_commands::{Encode, Replies};
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
        }
    }
    #[test]
    fn client_connection_read() {
        let mut rt = Runtime::new().expect("create tokio Runtime");

        let mut server = Server::new();

        let connection_factory = ConnectionFactory::create(ConnectionType::Mock);
        let manager = SegmentConnectionManager::new(connection_factory, 1);
        let pool = ConnectionPool::new(manager);
        let connection = rt
            .block_on(pool.get_connection(server.address))
            .expect("get connection from pool");

        // server send wirecommand
        server.send_hello_wirecommand();

        // read wirecommand
        let mut reader = ClientConnectionImpl::new(connection);

        let fut = reader.read();
        let reply = rt.block_on(fut).expect("get reply from server");

        assert_eq!(
            reply,
            Replies::Hello(HelloCommand {
                high_version: 9,
                low_version: 5,
            })
        );
    }
}
