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
use super::error::PayloadLengthTooLong;
use super::error::ReadWirecommand;
use super::error::ReaderError;
use crate::connection_factory::Connection;
use byteorder::{BigEndian, ReadBytesExt};
use snafu::{ensure, ResultExt};
use std::io::Cursor;

pub const MAX_WIRECOMMAND_SIZE: u32 = 0x007F_FFFF;
pub const LENGTH_FIELD_OFFSET: u32 = 4;
pub const LENGTH_FIELD_LENGTH: u32 = 4;

pub struct WireCommandReader {
    pub connection: Box<dyn Connection>,
}

impl WireCommandReader {
    pub async fn read(&mut self) -> Result<Vec<u8>, ReaderError> {
        let mut header: Vec<u8> = vec![0; LENGTH_FIELD_OFFSET as usize + LENGTH_FIELD_LENGTH as usize];
        self.connection
            .read_async(&mut header[..])
            .await
            .context(ReadWirecommand {
                part: "header".to_string(),
            })?;

        let mut rdr = Cursor::new(&header[4..8]);
        let payload_length = rdr.read_u32::<BigEndian>().expect("Exact size");

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
            .context(ReadWirecommand {
                part: "payload".to_string(),
            })?;

        let concatenated = [&header[..], &payload[..]].concat();

        Ok(concatenated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_factory::{
        ConnectionFactory, ConnectionFactoryImpl, ConnectionType,
    };
    use byteorder::{BigEndian, WriteBytesExt};
    use log::info;
    use std::io::Write;
    use std::mem;
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

        pub fn send_wirecommand(&mut self) {
            for stream in self.listener.incoming() {
                let mut stream = stream.unwrap();
                // offset is 4 bytes, payload length is 8 bytes and payload is 66.
                let mut bs = [0u8; 4 * mem::size_of::<i32>()];
                bs.as_mut().write_i32::<BigEndian>(4).expect("Unable to write");
                bs.as_mut().write_i32::<BigEndian>(8).expect("Unable to write");
                bs.as_mut().write_i32::<BigEndian>(6).expect("Unable to write");
                bs.as_mut().write_i32::<BigEndian>(6).expect("Unable to write");
                stream.write(bs.as_ref()).unwrap();
                break;
            }
            info!("sent wirecommand");
        }
    }
    #[test]
    fn test_wirecommand_reader() {
        let mut rt = Runtime::new().unwrap();

        let mut server = Server::new();

        let connection_factory = ConnectionFactoryImpl {};
        let connection_future =
            connection_factory.establish_connection(ConnectionType::Tokio, server.address);
        let connection = rt.block_on(connection_future).unwrap();
        info!("connection established");

        // server send wirecommand
        server.send_wirecommand();

        // read wirecommand
        let mut reader = WireCommandReader { connection };

        let fut = reader.read();
        let byte_buf = rt.block_on(fut).unwrap();

        let mut expected = [0u8; 2 * mem::size_of::<i32>()];
        expected
            .as_mut()
            .write_i32::<BigEndian>(6)
            .expect("Unable to write");
        expected
            .as_mut()
            .write_i32::<BigEndian>(6)
            .expect("Unable to write");

        assert_eq!(byte_buf, expected);
        info!("Testing wirecommand reader passed");
    }
}
