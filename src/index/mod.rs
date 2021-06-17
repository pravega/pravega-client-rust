//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

//! The Index API for writing and reading data to a segment in fixed sized frame.
//!
//!

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

#[doc(hidden)]
#[macro_export]
macro_rules! label {
    ($($x:expr),*) => {
        {
            let mut _temp = Label{entries: vec!{}};
            $(_temp.entries.push($x);)*
            _temp
        }
    };
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Record {
    type_code: i32,
    entries_len: usize,
    data_len: usize,
    entries: Vec<(u128, u64)>,
    data: Vec<u8>,
}

impl Record {
    const TYPE_CODE: i32 = 0;

    pub(crate) fn new(entries: Vec<(u128, u64)>, data: Vec<u8>) -> Record {
        Record {
            type_code: Record::TYPE_CODE,
            // u128 is 16 bytes and u64 is 8 bytes
            entries_len: entries.len() * 24,
            data_len: data.len(),
            entries,
            data,
        }
    }

    fn write_fields(&self) -> Result<Vec<u8>, BincodeError> {
        let mut encoded = CONFIG.serialize(&self)?;
        let padding = vec![0u8; RECORD_SIZE as usize - encoded.len()];
        encoded.extend(padding);
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, BincodeError> {
        let decoded: Record = CONFIG.deserialize(input)?;
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

#[derive(Clone)]
pub struct Label {
    pub entries: Vec<(&'static str, u64)>,
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    #[test]
    fn test_label_macro() {
        let label = label! {("timestamp", 0), ("id", 0)};
        assert_eq!(label.entries.len(), 2);
        let label = label! {};
        assert_eq!(label.entries.len(), 0);
    }

    #[test]
    fn test_record_serde() {
        let label = label! {("timestamp", 0), ("id", 0)};
        let data = vec![1, 2, 3, 4];
        let entries = vec![(0, 0), (1, 1), (2, 2)];
        let record = Record::new(entries, data.clone());
        let encoded = record.write_fields().expect("serialize record");
        assert_eq!(encoded.len(), RECORD_SIZE as usize);
        let decoded = Record::read_from(&encoded).expect("deserialize record");
        assert_eq!(decoded.data, data);
    }
}
