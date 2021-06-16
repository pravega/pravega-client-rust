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
use crate::index::{hash_key_to_u128, IndexReader, Label, Record, RECORD_SIZE};

use pravega_client_shared::ScopedSegment;

use bincode2::Error as BincodeError;
use snafu::{ensure, Backtrace, ResultExt, Snafu};
use std::io::Error;
use tokio::sync::oneshot;

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

pub struct IndexWriter {
    byte_writer: ByteWriter,
    entries: Option<Vec<(u128, u64)>>,
    event_handle: Option<oneshot::Receiver<Result<(), Error>>>,
}

impl IndexWriter {
    pub(crate) async fn new(factory: ClientFactory, segment: ScopedSegment) -> Self {
        let byte_writer = factory.create_byte_writer_async(segment.clone()).await;
        byte_writer.seek_to_tail_async().await;

        let index_reader = factory.create_index_reader(segment.clone()).await;
        let tail_offset = index_reader.tail_offset().await.expect("get tail offset");
        let head_offset = index_reader
            .head_offset()
            .await
            .expect("get readable head offset");
        if head_offset != tail_offset {
            let prev_record_offset = tail_offset - RECORD_SIZE;
            let record = index_reader
                .read_record(prev_record_offset)
                .await
                .expect("read last record");
            return IndexWriter {
                byte_writer,
                entries: Some(record.entries),
                event_handle: None,
            };
        }
        IndexWriter {
            byte_writer,
            entries: None,
            event_handle: None,
        }
    }

    pub async fn append(&mut self, data: Vec<u8>, label: Label) -> Result<(), IndexWriterError> {
        let entries = self.preprocess_label(label)?;
        self.validate_entries(&entries)?;
        self.entries = Some(entries);
        append_internal().await
    }

    pub async fn append_conditionally(
        &mut self,
        data: Vec<u8>,
        label: Label,
        condition_on: Label,
    ) -> Result<(), IndexWriterError> {
        self.check_condition(condition_on)?;
        let entries = self.preprocess_label(label)?;
        self.validate_entries(&entries)?;
        self.entries = Some(entries);
        append_internal().await
    }

    pub async fn flush(&mut self) -> Result<(), IndexWriterError> {
        if let Some(handle) = self.event_handle.take() {
            match handle.await {
                Ok(res) => {
                    if let Err(e) = res {
                        return Err(IndexWriterError::Internal {
                            msg: format!("{:?}", e),
                        });
                    }
                }
                Err(e) => {
                    return Err(IndexWriterError::Internal {
                        msg: format!("{:?}", e),
                    })
                }
            }
        }
        Ok(())
    }

    async fn append_internal(&mut self) -> Result<(), IndexWriterError> {
        let record = Record::new(self.entries.as_ref().unwrap().clone(), data);
        let encoded = record.write_fields().context(InvalidData {})?;
        let (_size, handle) = self.byte_writer.write_async(&encoded).await;
        self.event_handle = Some(handle);
        Ok(())
    }

    // check if the provided label matches the previous label in this writer.
    fn check_condition(&self, mut condition_on: Label) -> Result<(), IndexWriterError> {
        ensure!(
            self.entries.is_some()
            InvalidCondition {
                msg: format!(
                    "Index Writer doesn't have a previous label",
                ),
            }
        );

        let entries_hash = self.preprocess_label(condition_on)?;
        if let Some(ref prev_entries) = self.entries {
            let matching = entries_hash
                .iter()
                .zip(prev_entries.iter())
                .filter(|&(a, b)| a.0 == b.0 && a.1 == b.1)
                .count();
            ensure!(
                matching == entries_hash.len(),
                InvalidCondition {
                    msg: format!("Current condition label does not match the previous one",),
                }
            );
        }
        Ok(())
    }

    // sort the label entries by key in alphabetical order and hash the entry key.
    fn preprocess_label(&self, mut label: Label) -> Result<Vec<(u128, u64)>, IndexWriterError> {
        // label should contain some entries.
        ensure!(
            !label.entries.is_empty(),
            InvalidLabel {
                msg: format!("Label entry should not be empty",),
            }
        );

        // sort label alphabetically by entry key.
        label.entries.sort_by_key(|k| k.0);

        // check if there are duplicate keys in the label
        // calculate the hash for key
        let mut entries_hash = vec![];
        let mut prev_key = label.entries.get(0).unwrap().0;
        for (i, (key, val)) in label.entries.iter().enumerate() {
            if i > 0 {
                ensure!(
                    &prev_key != key,
                    InvalidLabel {
                        msg: format!("Duplicate label entries is not allowed {}", key),
                    }
                );
                prev_key = key;
            }
            let num = hash_key_to_u128(*key);
            entries_hash.push((num, *val));
        }

        Ok(entries_hash)
    }

    // check if the provided entry value is monotonically increasing.
    fn validate_entries(&self, entries_hash: &Vec<(u128, u64)>) -> Result<(), IndexWriterError> {
        // if previous label exists, ensure new label entry value is monotonically increasing.
        if let Some(ref prev_entries) = self.entries {
            // new label should not contain less number of entry than previous label
            // deletion of label entry is not supported
            ensure!(
                prev_entries.len() <= entries_hash.len(),
                InvalidLabel {
                    msg: format!(
                        "Label deletion is not supported. Previous label size {}, current label size {}",
                        prev_entries.len(),
                        entries_hash.len()
                    ),
                }
            );

            for (i, (prev_key, prev_val)) in prev_entries.iter().enumerate() {
                let entry = entries_hash.get(i).expect("should get entry");
                let key = entry.0;
                let val = entry.1;
                ensure!(
                    prev_key == &key,
                    InvalidLabel {
                        msg: format!(
                            "label entry key {} does not match with previous label entry key {}",
                            key, prev_key
                        ),
                    }
                );
                ensure!(
                    prev_val <= &val,
                    InvalidLabel {
                        msg: format!(
                            "label key/value {}/{} should not be less than the previous value {}",
                            key, val, prev_val
                        ),
                    }
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    #[test]
    fn test_update_labels() {}
}
