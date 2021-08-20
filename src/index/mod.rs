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
//! The index API writes a fixed sized [`IndexRecord`] to the stream. Each [`IndexRecord`] contains the user data
//! and a number of user defined `Fields`, the value of each `Field` has to be u64 type.
//! An example of `Fields` is showed as below:
//! ```no_run
//! use pravega_client_macros::Fields;
//!
//! // Use Fields procedural marco to auto implement the necessary trait for MyFields.
//! #[derive(Fields, Debug, PartialOrd, PartialEq)]
//! struct MyFields {
//!     time: u64, // A field
//!     id: u64,
//! }
//!
//! ```
//!
//! To ensure the searching efficiency in an index stream, we impose some constraints to the `Fields`:
//! * The value of the `Field` must be monotonically increasing.
//! * Deletion of the `Field` is not permitted.
//! * Adding a new `Field` is possible, but the `Field` has to be appended at the tail.
//! * The Index Writer is generic over the `Fields` struct, meaning if a new `Field` is appended, it needs
//! to create a new Index Writer for the new `Fields`.
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
use bincode2::ErrorKind;
use bincode2::LengthOption;
use lazy_static::*;
use pravega_wire_protocol::commands::{Command, EventCommand};
use serde::{Deserialize, Serialize};
use tiny_keccak::{Hasher, Shake};

pub const RECORD_SIZE: u64 = 4 * 1024;

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
    version: i32,
    fields_len: u32,
    data_len: u32,
    fields: Vec<(u128, u64)>,
    data: Vec<u8>,
}

impl Record {
    const TYPE_CODE: i32 = 0;
    // increase the version if record structure changes
    const VERSION: i32 = 0;

    pub(crate) fn new(fields: Vec<(&'static str, u64)>, data: Vec<u8>) -> Record {
        let fields_len = fields.len();
        let fields_hash = Record::hash_keys(fields);
        Record {
            type_code: Record::TYPE_CODE,
            version: Record::VERSION,
            // u128 is 16 bytes and u64 is 8 bytes
            fields_len: fields_len as u32 * 24,
            data_len: data.len() as u32,
            fields: fields_hash,
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
        if res.len() > RECORD_SIZE as usize {
            return Err(BincodeError::from(ErrorKind::Custom(format!(
                "Record size {} exceeds the max size allowed {}",
                res.len(),
                RECORD_SIZE,
            ))));
        }
        let padding = vec![0u8; RECORD_SIZE as usize - length - 8];
        res.extend(padding);
        Ok(res)
    }

    fn read_from(input: &[u8]) -> Result<Self, BincodeError> {
        let decoded: Record = CONFIG.deserialize(&input[8..])?;
        Ok(decoded)
    }

    pub(crate) fn hash_keys(entries: Vec<(&'static str, u64)>) -> Vec<(u128, u64)> {
        let mut entries_hash = vec![];
        for (key, val) in entries {
            entries_hash.push((Record::hash_key_to_u128(key), val))
        }
        entries_hash
    }

    pub(crate) fn hash_key_to_u128(key: &'static str) -> u128 {
        let mut shake = Shake::v128();
        shake.update(key.as_ref());
        let mut buf = [0u8; 16];
        shake.finalize(&mut buf);
        u128::from_be_bytes(buf)
    }
}

pub trait Fields {
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

    use pravega_client_macros::Fields;

    #[derive(Fields, Debug, PartialOrd, PartialEq)]
    struct FieldsTest {
        time: u64,
        id: u64,
    }

    #[test]
    fn test_label_macro() {
        let fields = FieldsTest { time: 0, id: 0 };
        assert_eq!(fields.to_key_value_pairs(), vec! {("time", 0), ("id", 0)});
    }

    #[test]
    fn test_record_serde() {
        let data = vec![1, 2, 3, 4];
        let fields = vec![("hello", 0), ("index", 1), ("stream", 2)];
        let record = Record::new(fields, data.clone());
        let encoded = record.write_fields().expect("serialize record");
        assert_eq!(encoded.len(), RECORD_SIZE as usize);
        let decoded = Record::read_from(&encoded).expect("deserialize record");
        assert_eq!(decoded.data, data);
    }
}