//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

pub(crate) mod pinger;
mod transactional_event_segment_writer;
pub mod transactional_event_stream_writer;

use crate::client_factory::ClientFactoryInternal;
use crate::error::*;
use crate::get_random_f64;
use crate::transaction::pinger::PingerHandle;
use pravega_rust_client_shared::{
    ScopedSegment, ScopedStream, StreamSegments, Timestamp, TransactionStatus, TxId, WriterId,
};
use snafu::ResultExt;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info_span};
use tracing_futures::Instrument;
use transactional_event_segment_writer::TransactionalEventSegmentWriter;

// contains the metadata of Transaction
#[derive(new)]
struct TransactionInfo {
    txn_id: TxId,
    writer_id: WriterId,
    stream: ScopedStream,
    closed: bool,
}

/// Transaction is an abstract of the Pravega Transaction. It can be used to write, commit and abort
/// a Pravega Transaction.
pub struct Transaction {
    info: TransactionInfo,
    inner: HashMap<ScopedSegment, TransactionalEventSegmentWriter>,
    segments: StreamSegments,
    handle: PingerHandle,
    factory: Arc<ClientFactoryInternal>,
}

impl Transaction {
    // Transaction should be created by transactional event stream writer, so this new method
    // is not public.
    fn new(
        info: TransactionInfo,
        transactions: HashMap<ScopedSegment, TransactionalEventSegmentWriter>,
        segments: StreamSegments,
        handle: PingerHandle,
        factory: Arc<ClientFactoryInternal>,
    ) -> Self {
        Transaction {
            info,
            inner: transactions,
            segments,
            handle,
            factory,
        }
    }

    /// get the transaction id.
    pub fn get_txn_id(&self) -> TxId {
        self.info.txn_id
    }

    /// get transactional event stream writer id.
    pub fn get_writer_id(&self) -> WriterId {
        self.info.writer_id
    }

    /// get the stream that this transaction is based on.
    pub fn get_stream(&self) -> ScopedStream {
        self.info.stream.clone()
    }

    /// write_event accepts a vec of bytes as the input event and an optional routing key which is used
    /// to determine which segment to write to. It calls the corresponding transactional event segment
    /// writer to write the data to segmentstore server.
    pub async fn write_event(&mut self, key: Option<String>, event: Vec<u8>) -> Result<(), TransactionError> {
        self.error_if_closed()?;

        let segment = if let Some(key) = key {
            self.segments.get_segment_for_string(&key)
        } else {
            self.segments.get_segment(get_random_f64())
        };
        let transaction = self
            .inner
            .get_mut(&segment)
            .expect("must get segment from transaction");
        let span = info_span!("Transaction", transactionId = %self.info.txn_id);
        transaction
            .write_event(event, &self.factory)
            .instrument(span)
            .await
            .context(TxnSegmentWriterError {})?;
        Ok(())
    }

    /// commit accepts a timestamp and will send a commit request to Pravega controller.
    pub async fn commit(&mut self, timestamp: Timestamp) -> Result<(), TransactionError> {
        debug!("committing transaction {:?}", self.info.txn_id);

        self.error_if_closed()?;
        self.info.closed = true;

        for writer in self.inner.values_mut() {
            writer
                .flush(&self.factory)
                .await
                .context(TxnSegmentWriterError {})?;
        }
        self.inner.clear(); // release the ownership of all event segment writers

        // remove this transaction from ping list
        self.handle
            .remove(self.info.txn_id)
            .await
            .context(TxnStreamWriterError {})?;

        self.factory
            .get_controller_client()
            .commit_transaction(
                &self.info.stream,
                self.info.txn_id,
                self.info.writer_id,
                timestamp,
            )
            .await
            .map_err(|e| e.error)
            .context(TxnControllerError {})?;

        debug!("transaction {:?} committed", self.info.txn_id);
        Ok(())
    }

    /// abort will send abort request to Pravega controller.
    pub async fn abort(&mut self) -> Result<(), TransactionError> {
        debug!("aborting transaction {:?}", self.info.txn_id);

        self.error_if_closed()?;
        self.info.closed = true;

        // remove this transaction from ping list
        self.handle
            .remove(self.info.txn_id)
            .await
            .context(TxnStreamWriterError {})?;

        self.inner.clear(); // release the ownership of all event segment writer
        self.factory
            .get_controller_client()
            .abort_transaction(&self.info.stream, self.info.txn_id)
            .await
            .map_err(|e| e.error)
            .context(TxnControllerError {})?;

        debug!("transaction {:?} aborted", self.info.txn_id);
        Ok(())
    }

    /// check the current Transaction status by sending request to Pravega controller.
    pub async fn check_status(&self) -> Result<TransactionStatus, TransactionError> {
        self.factory
            .get_controller_client()
            .check_transaction_status(&self.info.stream, self.info.txn_id)
            .await
            .map_err(|e| e.error)
            .context(TxnControllerError {})
    }

    fn error_if_closed(&self) -> Result<(), TransactionError> {
        if self.info.closed {
            Err(TransactionError::TxnClosed { id: self.info.txn_id })
        } else {
            Ok(())
        }
    }
}
