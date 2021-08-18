//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::byte::ByteWriter;
use crate::client_factory::ClientFactory;
use crate::index::{Label, Record, RECORD_SIZE};

use pravega_client_shared::ScopedStream;

use bincode2::Error as BincodeError;
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use std::fmt::Debug;
use std::marker::PhantomData;

const MAX_ENTRY_SIZE: usize = 100;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum IndexWriterError {
    #[snafu(display("Could not serialize/deserialize record because of: {}", source))]
    InvalidData {
        source: BincodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Label is not valid due to: {}", msg))]
    InvalidLabel { msg: String },

    #[snafu(display("Condition label is not valid due to: {}", msg))]
    InvalidCondition { msg: String },

    #[snafu(display("Internal error : {}", msg))]
    Internal { msg: String },
}

/// Index Writer writes a fixed size Record to the stream.
///
/// Write takes a byte array as data and a Label. It hashes the Label entry key and construct a Record. Then
/// it serializes the Record and writes to the stream.
///
/// # Examples
/// ```no_run
/// use pravega_client_config::ClientConfigBuilder;
/// use pravega_client::client_factory::ClientFactory;
/// use pravega_client_shared::ScopedStream;
/// use pravega_client_macros::Label;
/// use std::io::Write;
/// use tokio;
///
/// #[derive(Label, Debug, PartialOrd, PartialEq)]
/// struct MyLabel {
///     id: u64,
///     timestamp: u64,
/// }
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
///     // notice that this stream should be a fixed sized single segment stream
///     let stream = ScopedStream::from("myscope/mystream");
///
///     let mut index_writer = client_factory.create_index_writer(stream).await;
///
///     let label = MyLabel{id: 1, timestamp: 1000};
///     let data = vec!{1; 10};
///
///     index_writer.append(label, data).await.expect("append data with label");
///     index_writer.flush().await.expect("flush");
/// }
/// ```
pub struct IndexWriter<T: Label + PartialOrd + PartialEq + Debug> {
    byte_writer: ByteWriter,
    entries: Option<Vec<(u128, u64)>>,
    label: Option<T>,
    _label_type: PhantomData<T>,
}

impl<T: Label + PartialOrd + PartialEq + Debug> IndexWriter<T> {
    pub(crate) async fn new(factory: ClientFactory, stream: ScopedStream) -> Self {
        let mut byte_writer = factory.create_byte_writer_async(stream.clone()).await;
        byte_writer.seek_to_tail_async().await;

        let index_reader = factory.create_index_reader(stream.clone()).await;
        let tail_offset = index_reader.tail_offset().await.expect("get tail offset");
        let head_offset = index_reader
            .head_offset()
            .await
            .expect("get readable head offset");
        let entries = if head_offset != tail_offset {
            let prev_record_offset = tail_offset - RECORD_SIZE;
            let record = index_reader
                .read_record_from_random_offset(prev_record_offset)
                .await
                .expect("read last record");
            Some(record.entries)
        } else {
            None
        };
        IndexWriter {
            byte_writer,
            entries,
            label: None,
            _label_type: PhantomData,
        }
    }

    /// Append data with a given label.
    pub async fn append(&mut self, label: T, data: Vec<u8>) -> Result<(), IndexWriterError> {
        self.validate_label(&label)?;
        self.label = Some(label);
        self.entries = None;
        self.append_internal(data).await
    }

    /// Append data with a given label and conditioned on a label.
    /// The conditional label should match the latest label in the stream,
    /// if not, then this method will fail with error.
    pub async fn append_conditionally(
        &mut self,
        label: T,
        condition_on: T,
        data: Vec<u8>,
    ) -> Result<(), IndexWriterError> {
        self.check_condition(condition_on)?;
        self.validate_label(&label)?;
        self.label = Some(label);
        self.entries = None;
        self.append_internal(data).await
    }

    /// Flush data.
    pub async fn flush(&mut self) -> Result<(), IndexWriterError> {
        self.byte_writer
            .flush_async()
            .await
            .map_err(|e| IndexWriterError::Internal {
                msg: format!("failed to flush data {:?}", e),
            })
    }

    /// Truncate data to a given offset.
    pub async fn truncate(&mut self, offset: u64) -> Result<(), IndexWriterError> {
        self.byte_writer
            .truncate_data_before(offset as i64)
            .await
            .map_err(|e| IndexWriterError::Internal {
                msg: format!("failed to truncate data {:?}", e),
            })
    }

    async fn append_internal(&mut self, data: Vec<u8>) -> Result<(), IndexWriterError> {
        let entries = self.label.as_ref().unwrap().to_key_value_pairs();
        let record = Record::new(entries, data);
        let encoded = record.write_fields().context(InvalidData {})?;
        let _size = self.byte_writer.write_async(&encoded).await;
        Ok(())
    }

    // check if the provided label matches the previous label.
    fn check_condition(&self, condition_label: T) -> Result<(), IndexWriterError> {
        ensure!(
            *self.label.as_ref().unwrap() == condition_label,
            InvalidCondition {
                msg: format!(
                    "Previous label {:?} doesn't match condition label {:?}",
                    self.label, condition_label
                ),
            }
        );
        Ok(())
    }

    // check if the provided entry value is monotonically increasing.
    fn validate_label(&self, label: &T) -> Result<(), IndexWriterError> {
        let kv_pairs = label.to_key_value_pairs();
        ensure!(
            kv_pairs.len() <= MAX_ENTRY_SIZE,
            InvalidLabel {
                msg: format!(
                    "Label entry size {} exceeds max size allowed {}",
                    kv_pairs.len(),
                    MAX_ENTRY_SIZE,
                ),
            }
        );

        if self.label.is_none() && self.entries.is_none() {
            return Ok(());
        }
        if let Some(ref prev_label) = self.label {
            ensure!(
                *prev_label <= *label,
                InvalidLabel {
                    msg: format!(
                        "Label entry value should monotonically increasing: prev {:?}, current {:?}",
                        prev_label, label,
                    ),
                }
            );
            return Ok(());
        }

        if let Some(ref prev_entries_hash) = self.entries {
            let entries_hash = Record::hash_keys(kv_pairs);
            let matching = prev_entries_hash
                .iter()
                .zip(entries_hash.iter())
                .filter(|&(a, b)| a.0 == b.0 && a.1 <= b.1)
                .count();
            ensure!(
                matching == prev_entries_hash.len(),
                InvalidLabel {
                    msg: format!(
                        "Label entry value should monotonically increasing: prev {:?}, current {:?}",
                        prev_entries_hash, entries_hash,
                    ),
                }
            );
        }
        Ok(())
    }
}
