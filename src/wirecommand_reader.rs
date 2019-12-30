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
use crate::connection_factory::Connection;
use byteorder::{BigEndian, ReadBytesExt};
use std::error::Error;
use std::io::Cursor;

pub const MAX_WIRECOMMAND_SIZE: i32 = 0x007FFFFF;
pub const LENGTH_FIELD_OFFSET: i32 = 4;
pub const LENGTH_FIELD_LENGTH: i32 = 4;

pub struct WireCommandReader {
    pub connection: dyn Connection,
}

impl WireCommandReader {
    async fn read(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut header: Vec<u8> =
            vec![0; LENGTH_FIELD_OFFSET as usize + LENGTH_FIELD_LENGTH as usize];
        self.connection.read_async(&mut header[..]).await?;

        let mut rdr = Cursor::new(&header[4..8]);
        let payload_length = rdr.read_i32::<BigEndian>().unwrap();

        let mut payload: Vec<u8> = vec![0; payload_length as usize];
        self.connection.read_async(&mut payload[..]).await?;

        let concatenated = [&header[..], &payload[..]].concat();

        Ok(concatenated)
    }
}
