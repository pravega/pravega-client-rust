//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactory;
use crate::index::{IndexRecord, RECORD_SIZE};
use crate::segment::reader::{AsyncSegmentReader, AsyncSegmentReaderImpl};

use pravega_client_shared::{ScopedSegment, ScopedStream};

use crate::segment::metadata::SegmentMetadataClient;
use async_stream::try_stream;
use futures::stream::Stream;
use snafu::{ensure, Snafu};
use std::io::SeekFrom;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum IndexReaderError {
    #[snafu(display("Field {} does not exist", msg))]
    FieldNotFound { msg: String },

    #[snafu(display("Invalid offset : {}", msg))]
    InvalidOffset { msg: String },

    #[snafu(display("Internal error : {}", msg))]
    Internal { msg: String },
}

/// Index Reader reads the Index Record from Stream.
///
/// The Stream has to be fixed size single segment stream like byte stream.
///
/// # Examples
/// ```no_run
/// use pravega_client_config::ClientConfigBuilder;
/// use pravega_client::client_factory::ClientFactory;
/// use pravega_client_shared::ScopedStream;
/// use futures_util::pin_mut;
/// use futures_util::StreamExt;
/// use std::io::Write;
/// use tokio;
///
/// // Suppose the existing Fields in the stream is like below.
/// // #[derive(Fields, Debug, PartialOrd, PartialEq)]
/// // struct MyFields {
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
///     // assuming scope:myscope, stream:mystream exist.
///     let stream = ScopedStream::from("myscope/mystream");
///
///     let mut index_reader = client_factory.create_index_reader(stream).await;
///
///     // search data
///     let offset = index_reader.search_offset(("id", 10)).await.expect("get offset");
///
///     // read data
///     let s = index_reader.read(offset, u64::MAX).expect("get read slice");
///     pin_mut!(s);
///     while let Some(res) = s.next().await {
///         // do something with the read result
///         res.expect("read next event");
///     }
/// }
/// ```
pub struct IndexReader {
    stream: ScopedStream,
    factory: ClientFactory,
    meta: SegmentMetadataClient,
    segment_reader: AsyncSegmentReaderImpl,
}

impl IndexReader {
    pub(crate) async fn new(factory: ClientFactory, stream: ScopedStream) -> Self {
        let segments = factory
            .controller_client()
            .get_head_segments(&stream)
            .await
            .expect("get head segments");
        assert_eq!(
            segments.len(),
            1,
            "Index stream is configured with more than one segment"
        );
        let segment = segments.iter().next().unwrap().0.clone();
        let scoped_segment = ScopedSegment {
            scope: stream.scope.clone(),
            stream: stream.stream.clone(),
            segment,
        };
        let segment_reader = factory.create_async_segment_reader(scoped_segment.clone()).await;
        let meta = factory
            .create_segment_metadata_client(scoped_segment.clone())
            .await;
        IndexReader {
            stream,
            factory,
            meta,
            segment_reader,
        }
    }

    /// Given an Field (name, v), find the offset of the first record that contains the given Field
    /// that has value >= v.
    ///
    /// Note that if there are multiple entries that have the same Field name and value, this method will find and return
    /// the first one.
    pub async fn search_offset(&self, field: (&'static str, u64)) -> Result<u64, IndexReaderError> {
        const RECORD_SIZE_SIGNED: i64 = RECORD_SIZE as i64;

        let target_key = IndexRecord::hash_key_to_u128(field.0);
        let target_value = field.1;

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

            if let Some(e) = record.fields.iter().find(|&e| e.0 == target_key) {
                // record contains the field, compare value with the target value.
                if e.1 >= target_value {
                    // value is large than or equal to the target value, check the first half.
                    end = mid - 1;
                } else {
                    // value is smaller than the target value, check the second half.
                    start = mid + 1;
                }
                // field does not exist in the current record.
                // it might exist in the second half.
            } else {
                start = mid + 1;
            }
        }

        if start == num_of_record {
            Err(IndexReaderError::FieldNotFound {
                msg: format!("key/value: {}/{}", field.0, field.1),
            })
        } else {
            Ok((head + start * RECORD_SIZE_SIGNED) as u64)
        }
    }

    /// Reads records starting from the given offset.
    ///
    /// This method returns a slice of stream that implements an iterator. Application can iterate on
    /// this slice to get the data. When `next()` is invoked on the iterator, a read request
    /// will be issued by the underlying reader.
    ///
    /// If we want to do tail read instead of reading just a slice of the data, we can set end_offset
    /// to be u64::MAX.
    pub fn read<'stream, 'reader: 'stream>(
        &'reader self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<impl Stream<Item = Result<Vec<u8>, IndexReaderError>> + 'stream, IndexReaderError> {
        ensure!(
            start_offset % (RECORD_SIZE as u64) == 0,
            InvalidOffset {
                msg: format!(
                    "Start offset {} is invalid as it cannot be divided by the record size {}",
                    start_offset, RECORD_SIZE
                )
            }
        );
        if end_offset != u64::MAX {
            ensure!(
                end_offset % (RECORD_SIZE as u64) == 0,
                InvalidOffset {
                    msg: format!(
                        "End offset {} is invalid as it cannot be divided by the record size {}",
                        end_offset, RECORD_SIZE
                    )
                }
            );
        }
        Ok(try_stream! {
            let stream = self.stream.clone();
            let mut byte_reader = self.factory.create_byte_reader_async(stream).await;
            let mut num_of_records_to_read = if end_offset == u64::MAX {
                u64::MAX
            } else {
                (end_offset - start_offset) / (RECORD_SIZE as u64)
            };
            byte_reader.seek_async(SeekFrom::Start(start_offset))
                .await
                .map_err(|e| IndexReaderError::InvalidOffset {
                    msg: format!("invalid seeking offset {:?}", e)
            })?;
            loop {
                let mut buf = vec!{};
                let mut size_to_read = RECORD_SIZE as usize;
                while size_to_read != 0 {
                    let mut tmp_buf = vec![0; size_to_read];
                    let size = byte_reader
                        .read_async(&mut tmp_buf)
                        .await
                        .map_err(|e| IndexReaderError::Internal {
                            msg: format!("byte reader read error {:?}", e),
                        })?;
                    buf.extend_from_slice(&tmp_buf[..size]);
                    size_to_read -= size;
                }
                let record = IndexRecord::read_from(&buf).map_err(|e| IndexReaderError::Internal {
                    msg: format!("deserialize record {:?}", e),
                })?;
                yield record.data;
                if num_of_records_to_read != u64::MAX {
                    num_of_records_to_read -= 1;
                }
                if num_of_records_to_read == 0 {
                    break;
                }
            }
        })
    }

    /// Data in the first readable record.
    pub async fn first_record_data(&self) -> Result<Vec<u8>, IndexReaderError> {
        let head_offset = self.head_offset().await?;
        let first_record = self.read_record_from_random_offset(head_offset).await?;
        Ok(first_record.data)
    }

    /// Data in the last record.
    pub async fn last_record_data(&self) -> Result<Vec<u8>, IndexReaderError> {
        let last_offset = self.tail_offset().await?;
        let last_record_offset = last_offset - RECORD_SIZE;
        let last_record = self.read_record_from_random_offset(last_record_offset).await?;
        Ok(last_record.data)
    }

    /// Get the readable head offset.
    pub async fn head_offset(&self) -> Result<u64, IndexReaderError> {
        self.meta
            .fetch_current_starting_head()
            .await
            .map(|i| i as u64)
            .map_err(|e| IndexReaderError::Internal {
                msg: format!("failed to get head offset: {:?}", e),
            })
    }

    /// Get the tail offset.
    pub async fn tail_offset(&self) -> Result<u64, IndexReaderError> {
        self.meta
            .fetch_current_segment_length()
            .await
            .map(|i| i as u64)
            .map_err(|e| IndexReaderError::Internal {
                msg: format!("failed to get tail offset: {:?}", e),
            })
    }

    // Read a record from a given offset.
    pub(crate) async fn read_record_from_random_offset(
        &self,
        offset: u64,
    ) -> Result<IndexRecord, IndexReaderError> {
        let segment_read_cmd = self
            .segment_reader
            .read(offset as i64, RECORD_SIZE as i32)
            .await
            .map_err(|e| IndexReaderError::Internal {
                msg: format!("segment reader error: {:?}", e),
            })?;
        let record =
            IndexRecord::read_from(&segment_read_cmd.data).map_err(|e| IndexReaderError::Internal {
                msg: format!("record deserialization error: {:?}", e),
            })?;
        Ok(record)
    }
}
