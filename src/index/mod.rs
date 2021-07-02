//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

//! The Index API provides a way to efficiently search data in the stream.
//!
//! The index API writes a fixed sized [`Record`] to the stream. Each [`Record`] contains the user data
//! and a number of user defined key-value pairs, a key-value pair is called `Entry`.
//! A set of `Entry` is called `Label` and an example of the `Label` is showed as below:
//! ```no_run
//! use pravega_client_macros::Label;
//!
//! #[derive(Label, Debug, PartialOrd, PartialEq)]
//! struct MyLabel {
//!     time: u64,
//!     id: u64,
//! }
//!
//! ```
//! The `Label` procedural marco will convert the `MyLabel` struct to a list of key-value pairs.
//! Notice that we only accept u64 as the `Entry` value type.
//!
//! To ensure the searching efficiency, we impose some constraints to the `Label`:
//! * The `Entry` value in the `Label` must be monotonically increasing.
//! * Deletion of the `Entry` in the `Label` is not permitted.
//! * Adding a new `Entry` is possible, but the `Entry` has to be appended at the tail.
//! * The Index Writer is generic over the `Label` struct, meaning if a new `Entry` is appended, it needs
//! to create a new Index Writer for the new `Label`.
//!
//! [`Record`]: crate::index::Record

pub mod writer;
#[doc(inline)]
pub use writer::IndexWriter;

pub mod reader;
#[doc(inline)]
pub use reader::IndexReader;

use bincode2::Config;
use bincode2::Error as BincodeError;
use bincode2::LengthOption;
use lazy_static::*;
use pravega_wire_protocol::commands::{Command, EventCommand};
use serde::{Deserialize, Serialize};
use tiny_keccak::{Hasher, Shake};

const RECORD_SIZE: u64 = 4 * 1024;
const DATA_SIZE: usize = 1024;

lazy_static! {
    static ref CONFIG: Config = {
        let mut config = bincode2::config();
        config.big_endian();
        config.limit(RECORD_SIZE);
        config.array_length(LengthOption::U32);
        config.string_length(LengthOption::U16);
        config
    };
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Record {
    type_code: i32,
    entries_len: u32,
    data_len: u32,
    entries: Vec<(u128, u64)>,
    data: Vec<u8>,
}

impl Record {
    const TYPE_CODE: i32 = 0;

    pub(crate) fn new(entries: Vec<(u128, u64)>, data: Vec<u8>) -> Record {
        Record {
            type_code: Record::TYPE_CODE,
            // u128 is 16 bytes and u64 is 8 bytes
            entries_len: entries.len() as u32 * 24,
            data_len: data.len() as u32,
            entries,
            data,
        }
    }

    fn write_fields(&self) -> Result<Vec<u8>, BincodeError> {
        let mut res = vec![];
        res.extend_from_slice(&EventCommand::TYPE_CODE.to_be_bytes());
        res.extend_from_slice(&((RECORD_SIZE - 8) as i32).to_be_bytes());
        let encoded = CONFIG.serialize(&self)?;
        let length = encoded.len();
        res.extend(encoded);
        let padding = vec![0u8; RECORD_SIZE as usize - length - 8];
        res.extend(padding);
        Ok(res)
    }

    fn read_from(input: &[u8]) -> Result<Self, BincodeError> {
        let decoded: Record = CONFIG.deserialize(&input[8..])?;
        Ok(decoded)
    }
}

pub(crate) fn hash_key_to_u128(key: &'static str) -> u128 {
    let mut shake = Shake::v128();
    shake.update(key.as_ref());
    let mut buf = [0u8; 16];
    shake.finalize(&mut buf);
    u128::from_be_bytes(buf)
}

pub trait Label {
    fn to_key_value_pairs(&self) -> Vec<(&'static str, u64)>;
}

pub trait Value {
    fn value(&self) -> u64;
}

impl Value for u64 {
    fn value(&self) -> u64 {
        self.to_owned()
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate as pravega_client;

    use pravega_client_macros::Label;

    #[derive(Label, Debug, PartialOrd, PartialEq)]
    struct LabelTest {
        time: u64,
        id: u64,
    }

    #[test]
    fn test_label_macro() {
        let label = LabelTest { time: 0, id: 0 };
        assert_eq!(label.to_key_value_pairs(), vec! {("time", 0), ("id", 0)});
    }

    #[test]
    fn test_record_serde() {
        let data = vec![1, 2, 3, 4];
        let entries = vec![(0, 0), (1, 1), (2, 2)];
        let record = Record::new(entries, data.clone());
        let encoded = record.write_fields().expect("serialize record");
        assert_eq!(encoded.len(), RECORD_SIZE as usize);
        let decoded = Record::read_from(&encoded).expect("deserialize record");
        assert_eq!(decoded.data, data);
    }
}
