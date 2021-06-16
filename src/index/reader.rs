//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::byte::ByteReader;
use crate::client_factory::ClientFactory;
use crate::index::{hash_key_to_u128, Record, RECORD_SIZE};
use crate::segment::reader::{AsyncSegmentReader, AsyncSegmentReaderImpl};

use pravega_client_shared::ScopedSegment;

use crate::index::reader::IndexReaderError::EntryNotFound;
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use std::io::{Error, ErrorKind};

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum IndexReaderError {
    #[snafu(display("Could not serialize/deserialize record because of: {}", source))]
    InvalidData {
        source: BincodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Entry {} does not exist}", msg))]
    EntryNotFound { msg: String },

    #[snafu(display("Internal error : {}", msg))]
    Internal { msg: String },
}

pub struct IndexReader {
    byte_reader: ByteReader,
    current_offset: u64,
    segment_reader: AsyncSegmentReaderImpl,
}

impl IndexReader {
    pub(crate) async fn new(factory: ClientFactory, segment: ScopedSegment) -> Self {
        let byte_reader = factory.create_byte_reader_async(segment).await;
        let current_offset = byte_reader
            .current_head()
            .await
            .expect("get current readable head");
        let segment_reader = factory.create_async_segment_reader(segment.clone()).await;
        IndexReader {
            byte_reader,
            current_offset,
            segment_reader,
        }
    }

    pub async fn search_offset(&mut self, entry: (&'static str, u64)) -> Result<u64, IndexReaderError> {
        let target_key = hash_key_to_u128(entry.0);
        let target_value = entry.1;

        let head = self.head_offset().await?;
        let tail = self.tail_offset().await?;
        let mut start = 0;
        let mut end = (tail - head) / RECORD_SIZE - 1;

        let mut offset: Option<u64> = None;
        while start <= end {
            let mid = start + (end - start) / 2;
            let record = self.read_record(head + mid * RECORD_SIZE).await?;

            if record.entries.iter().any(|&e| e.0 == target_key) {
                // record contains the entry, compare value with the target value.
                if e.1 >= target_value {
                    // value is large than or equal to the target value, check the first half.
                    end = mid - 1;
                    offset = Some(head + mid * RECORD_SIZE);
                } else {
                    // value is smaller than the target value, check the second half.
                    start = mid + 1;
                }
                // entry does not exist in the current record.
                // it might exist in the second half.
            } else {
                start = mid + 1;
            }
        }

        if let Some(res) = offset {
            Ok(result)
        } else {
            Err(IndexReaderError::EntryNotFound {
                msg: format!("key/value: {}/{}", entry.0, entry.1),
            })
        }
    }

    pub async fn read_from(&mut self, entry: (&'static str, u64)) -> Result<Vec<u8>, IndexReaderError> {
        let offset = self.search_offset(entry)?;
        let record = self.read_record(offset).await?;
        Ok(record.data)
    }

    /// First readable record.
    pub async fn first_record(&mut self) -> Result<Vec<u8>, Error> {
        let head_offset = self
            .byte_reader
            .current_head()
            .await
            .expect("get current readable head");
        let first_record = self.read_record(head_offset).await?;
        Ok(first_record.data)
    }

    /// Last record.
    pub async fn last_record(&mut self) -> Result<Vec<u8>, Error> {
        let last_offset = self.byte_reader.current_tail().await.expect("get current tail");
        let last_record_offset = last_offset - RECORD_SIZE;
        let last_record = self.read_record(last_record_offset).await?;
        Ok(last_record.data)
    }

    /// Read a record from a given offset.
    pub async fn read_record(&self, offset: u64) -> Result<Record, Error> {
        let segment_read_cmd = self
            .segment_reader
            .read(offset as i64, RECORD_SIZE as i32)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("segment reader error: {:?}", e)))?;
        let record = Record::read_from(&segment_read_cmd.data)
            .map_err(|e| Error::new(ErrorKind::Other, format!("record deserialization error: {:?}", e)))?;
        Ok(record)
    }

    pub async fn head_offset(&self) -> Result<u64, Error> {
        self.byte_reader.current_head().await
    }

    pub async fn tail_offset(&self) -> Result<u64, Error> {
        self.byte_reader.current_tail().await
    }

    // sort the entry by key alphabetically and hash the entry key
    fn preprocess_entry(&self, mut entries: Vec<&'static str>) -> Vec<u128> {
        entries.sort_by_key(|k| *k);

        let mut entries_hash: Vec<u128> = vec![];
        for e in entries {
            let num = hash_key_to_u128(e);
            entries_hash.push(num);
        }
        entries_hash
    }
}
