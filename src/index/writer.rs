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
use crate::client_factory::ClientFactoryAsync;
use crate::index::{Fields, IndexRecord, RECORD_SIZE};

use pravega_client_shared::ScopedStream;

use bincode2::Error as BincodeError;
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use std::fmt::Debug;
use std::marker::PhantomData;

const MAX_FIELDS_SIZE: usize = 100;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum IndexWriterError {
    #[snafu(display("Could not serialize/deserialize record because of: {}", source))]
    InvalidData {
        source: BincodeError,
        backtrace: Backtrace,
    },

    #[snafu(display("Field is not valid due to: {}", msg))]
    InvalidFields { msg: String },

    #[snafu(display("Condition field is not valid due to: {}", msg))]
    InvalidCondition { msg: String },

    #[snafu(display("Internal error : {}", msg))]
    Internal { msg: String },
}

/// Index Writer writes a fixed size Record to the stream.
///
/// Write takes a byte array as data and Fields. It hashes each Field name and construct a Record. Then
/// it serializes the Record and writes to the stream.
///
/// # Examples
/// ```no_run
/// use pravega_client_config::ClientConfigBuilder;
/// use pravega_client::client_factory::ClientFactory;
/// use pravega_client_shared::ScopedStream;
/// use pravega_client_macros::Fields;
/// use std::io::Write;
/// use tokio;
///
/// #[derive(Fields, Debug, PartialOrd, PartialEq)]
/// struct MyFields {
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
///     let fields = MyFields{id: 1, timestamp: 1000};
///     let data = vec!{1; 10};
///
///     index_writer.append(fields, data).await.expect("append data with fields");
///     index_writer.flush().await.expect("flush");
/// }
/// ```
pub struct IndexWriter<T: Fields + PartialOrd + PartialEq + Debug> {
    byte_writer: ByteWriter,
    hashed_fields: Option<Vec<(u128, u64)>>,
    fields: Option<T>,
    _fields_type: PhantomData<T>,
}

impl<T: Fields + PartialOrd + PartialEq + Debug> IndexWriter<T> {
    pub(crate) async fn new(factory: ClientFactoryAsync, stream: ScopedStream) -> Self {
        let mut byte_writer = factory.create_byte_writer(stream.clone()).await;
        byte_writer.seek_to_tail().await;

        let index_reader = factory.create_index_reader(stream.clone()).await;
        let tail_offset = index_reader.tail_offset().await.expect("get tail offset");
        let head_offset = index_reader
            .head_offset()
            .await
            .expect("get readable head offset");
        let hashed_fields = if head_offset != tail_offset {
            let prev_record_offset = tail_offset - RECORD_SIZE;
            let record = index_reader
                .read_record_from_random_offset(prev_record_offset)
                .await
                .expect("read last record");
            Some(record.fields)
        } else {
            None
        };
        IndexWriter {
            byte_writer,
            hashed_fields,
            fields: None,
            _fields_type: PhantomData,
        }
    }

    /// Append data with a given Fields.
    pub async fn append(&mut self, fields: T, data: Vec<u8>) -> Result<(), IndexWriterError> {
        self.validate_fields(&fields)?;
        self.fields = Some(fields);
        self.hashed_fields = None;
        self.append_internal(data).await
    }

    /// Append data with a given Fields and conditioned on a Fields.
    /// The conditional Fields should match the latest Fields in the stream,
    /// if not, this method will fail with error.
    pub async fn append_conditionally(
        &mut self,
        fields: T,
        condition_on: T,
        data: Vec<u8>,
    ) -> Result<(), IndexWriterError> {
        self.check_condition(condition_on)?;
        self.validate_fields(&fields)?;
        self.fields = Some(fields);
        self.hashed_fields = None;
        self.append_internal(data).await
    }

    /// Flush data.
    pub async fn flush(&mut self) -> Result<(), IndexWriterError> {
        self.byte_writer
            .flush()
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
        let fields_list = self.fields.as_ref().unwrap().get_field_values();
        let record = IndexRecord::new(fields_list, data);
        let encoded = record.write_fields().context(InvalidData {})?;
        let _size = self.byte_writer.write(&encoded).await;
        Ok(())
    }

    // check if the provided Fields matches the previous Fields.
    fn check_condition(&self, condition_fields: T) -> Result<(), IndexWriterError> {
        ensure!(
            *self.fields.as_ref().unwrap() == condition_fields,
            InvalidCondition {
                msg: format!(
                    "Previous fields {:?} doesn't match condition fields {:?}",
                    self.fields, condition_fields
                ),
            }
        );
        Ok(())
    }

    // check if the provided field value is monotonically increasing.
    fn validate_fields(&self, fields: &T) -> Result<(), IndexWriterError> {
        let kv_pairs = fields.get_field_values();
        ensure!(
            kv_pairs.len() <= MAX_FIELDS_SIZE,
            InvalidFields {
                msg: format!(
                    "Fields size {} exceeds max size allowed {}",
                    kv_pairs.len(),
                    MAX_FIELDS_SIZE,
                ),
            }
        );

        if self.fields.is_none() && self.hashed_fields.is_none() {
            return Ok(());
        }
        if let Some(ref prev_fields) = self.fields {
            ensure!(
                *prev_fields <= *fields,
                InvalidFields {
                    msg: format!(
                        "The value of Field should monotonically increasing: prev {:?}, current {:?}",
                        prev_fields, fields,
                    ),
                }
            );
            return Ok(());
        }

        if let Some(ref prev_fields_hash) = self.hashed_fields {
            let fields_hash = IndexRecord::hash_keys(kv_pairs);
            let matching = prev_fields_hash
                .iter()
                .zip(fields_hash.iter())
                .filter(|&(a, b)| a.0 == b.0 && a.1 <= b.1)
                .count();
            ensure!(
                matching == prev_fields_hash.len(),
                InvalidFields {
                    msg: format!(
                        "The value of field should monotonically increasing: prev {:?}, current {:?}",
                        prev_fields_hash, fields_hash,
                    ),
                }
            );
        }
        Ok(())
    }
}
