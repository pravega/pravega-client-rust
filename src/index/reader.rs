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

use snafu::Snafu;
use std::io::{Error, SeekFrom};

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum IndexReaderError {
    #[snafu(display("Entry {} does not exist", msg))]
    EntryNotFound { msg: String },

    #[snafu(display("Internal error : {}", msg))]
    Internal { msg: String },
}

/// Index Reader reads the Record from stream.
///
/// Write takes a byte array as data and a Label. It hashes the Label entry key and construct a Record. Then
/// it serializes the Record and writes to the stream.
///
/// # Examples
/// ```no_run
/// use pravega_client_config::ClientConfigBuilder;
/// use pravega_client::client_factory::ClientFactory;
/// use pravega_client_shared::ScopedSegment;
/// use std::io::Write;
/// use tokio;
///
/// // Suppose the existing Label in the stream is like below.
/// // #[derive(Label, Debug, PartialOrd, PartialEq)]
/// // struct MyLabel {
/// //    id: u64,
/// //    timestamp: u64,
/// // }
///
/// #[tokio::main]
/// async fn main() {
///     // assuming Pravega controller is running at endpoint `localhost:9090`
///     let config = ClientConfigBuilder::default()
///         .controller_uri("localhost:9090")
///         .build()
///         .expect("creating config");
///
///     let client_factory = ClientFactory::new(config);
///
///     // assuming scope:myscope, stream:mystream and segment 0 do exist.
///     let segment = ScopedSegment::from("myscope/mystream/0");
///
///     let mut index_reader = client_factory.create_index_reader(segment).await;
///
///     // search data
///     let offset = index_reader.search_offset(("id", 10)).await.expect("get offset");
///     index_reader.seek()
///     index_writer.flush().await.expect("flush");
/// }
/// ```
pub struct IndexReader {
    byte_reader: ByteReader,
    current_offset: u64,
    segment_reader: AsyncSegmentReaderImpl,
}

impl IndexReader {
    pub(crate) async fn new(factory: ClientFactory, segment: ScopedSegment) -> Self {
        let byte_reader = factory.create_byte_reader_async(segment.clone()).await;
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

    /// Given an entry (key, x), find the offset of the first record that contains the given entry key
    /// and value >= x.
    ///
    /// Note that if there are multiple entries that have the same entry key and value, this method will find and return
    /// the first one.
    pub async fn search_offset(&mut self, entry: (&'static str, u64)) -> Result<SeekFrom, IndexReaderError> {
        const RECORD_SIZE_SIGNED: i64 = RECORD_SIZE as i64;

        let target_key = hash_key_to_u128(entry.0);
        let target_value = entry.1;

        let head = self.head_offset().await.map_err(|e| IndexReaderError::Internal {
            msg: format!("error when fetching head offset: {:?}", e),
        })? as i64;
        let tail = self.tail_offset().await.map_err(|e| IndexReaderError::Internal {
            msg: format!("error when fetching tail offset: {:?}", e),
        })? as i64;
        let mut start = 0;
        let num_of_record = (tail - head) as i64 / RECORD_SIZE_SIGNED;
        let mut end = num_of_record - 1;

        while start <= end {
            let mid = start + (end - start) / 2;
            let record = self
                .read_record_from_random_offset((head + mid * RECORD_SIZE_SIGNED) as u64)
                .await?;

            if let Some(e) = record.entries.iter().find(|&e| e.0 == target_key) {
                // record contains the entry, compare value with the target value.
                if e.1 >= target_value {
                    // value is large than or equal to the target value, check the first half.
                    end = mid - 1;
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

        if start == num_of_record {
            Err(IndexReaderError::EntryNotFound {
                msg: format!("key/value: {}/{}", entry.0, entry.1),
            })
        } else {
            Ok(SeekFrom::Start((start * RECORD_SIZE_SIGNED) as u64))
        }
    }

    /// Read an record from the current offset.
    pub async fn read(&mut self) -> Result<Vec<u8>, IndexReaderError> {
        let mut buf = vec![0; RECORD_SIZE as usize];
        self.byte_reader
            .read_async(&mut buf)
            .await
            .map_err(|e| IndexReaderError::Internal {
                msg: format!("byte reader read error {:?}", e),
            })?;
        let record = Record::read_from(&buf).map_err(|e| IndexReaderError::Internal {
            msg: format!("deserialize record {:?}", e),
        })?;
        self.current_offset += RECORD_SIZE;
        Ok(record.data)
    }

    /// Seek to a given offset.
    ///
    /// After calling this method, read method will start to read from the seek offset.
    pub async fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.byte_reader.seek_async(pos).await
    }

    /// First readable record.
    pub async fn first_record(&mut self) -> Result<Vec<u8>, IndexReaderError> {
        let head_offset = self
            .byte_reader
            .current_head()
            .await
            .expect("get current readable head");
        let first_record = self.read_record_from_random_offset(head_offset).await?;
        Ok(first_record.data)
    }

    /// Last record.
    pub async fn last_record(&mut self) -> Result<Vec<u8>, IndexReaderError> {
        let last_offset = self.byte_reader.current_tail().await.expect("get current tail");
        let last_record_offset = last_offset - RECORD_SIZE;
        let last_record = self.read_record_from_random_offset(last_record_offset).await?;
        Ok(last_record.data)
    }

    /// Get the readable head offset.
    pub async fn head_offset(&self) -> Result<u64, Error> {
        self.byte_reader.current_head().await
    }

    /// Get the tail offset.
    pub async fn tail_offset(&self) -> Result<u64, Error> {
        self.byte_reader.current_tail().await
    }

    // Read a record from a given offset.
    pub(crate) async fn read_record_from_random_offset(
        &self,
        offset: u64,
    ) -> Result<Record, IndexReaderError> {
        let segment_read_cmd = self
            .segment_reader
            .read(offset as i64, RECORD_SIZE as i32)
            .await
            .map_err(|e| IndexReaderError::Internal {
                msg: format!("segment reader error: {:?}", e),
            })?;
        let record = Record::read_from(&segment_read_cmd.data).map_err(|e| IndexReaderError::Internal {
            msg: format!("record deserialization error: {:?}", e),
        })?;
        Ok(record)
    }
}
