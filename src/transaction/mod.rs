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
pub mod transactional_event_stream_writer;

use crate::client_factory::ClientFactory;
use crate::error::*;
use crate::reactor::event::{Incoming, PendingEvent};
use crate::reactor::reactors::Reactor;
use crate::transaction::pinger::PingerHandle;
use pravega_client_channel::{create_channel, ChannelSender};
use pravega_client_shared::{ScopedStream, StreamSegments, Timestamp, TransactionStatus, TxId, WriterId};
use snafu::ResultExt;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::{debug, error, info_span};
use tracing_futures::Instrument;

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
    sender: ChannelSender<Incoming>,
    handle: PingerHandle,
    factory: ClientFactory,
    event_handles: Vec<EventHandle>,
}

type EventHandle = oneshot::Receiver<Result<(), SegmentWriterError>>;

impl Transaction {
    // maximum 16 MB total size of events could be held in memory
    const CHANNEL_CAPACITY: usize = 16 * 1024 * 1024;

    // Transaction should be created by transactional event stream writer, so this new method
    // is not public.
    async fn new(
        info: TransactionInfo,
        stream_segments: StreamSegments,
        handle: PingerHandle,
        factory: ClientFactory,
        closed: bool,
    ) -> Self {
        let (tx, rx) = create_channel(Self::CHANNEL_CAPACITY);
        if closed {
            return Transaction {
                info,
                sender: tx,
                handle,
                factory,
                event_handles: vec![],
            };
        }
        let rt_handle = factory.get_runtime_handle();
        let writer_id = info.writer_id;
        let txn_id = info.txn_id;
        let span = info_span!("StreamReactor", txn_id = %txn_id, event_stream_writer = %writer_id);
        // tokio::spawn is tied to the factory runtime.
        rt_handle.enter(|| {
            tokio::spawn(
                Reactor::run(
                    info.stream.clone(),
                    tx.clone(),
                    rx,
                    factory.clone(),
                    Some(stream_segments),
                )
                .instrument(span),
            )
        });
        Transaction {
            info,
            sender: tx,
            handle,
            factory,
            event_handles: vec![],
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
    ///
    /// This method has a backpressure mechanism. Internally, it uses [`Channel`] to send event to
    /// Reactor for processing. [`Channel`] can has a limited [`capacity`], when its capacity
    /// is reached, any further write will not be acceptedgit  until enough space has been freed in the [`Channel`].
    ///
    ///
    /// [`channel`]: pravega_client_channel
    /// [`capacity`]: Transaction::CHANNEL_CAPACITY
    ///
    pub async fn write_event(
        &mut self,
        routing_key: Option<String>,
        event: Vec<u8>,
    ) -> Result<(), TransactionError> {
        self.error_if_closed()?;

        let size = event.len();
        let (tx, rx) = oneshot::channel();
        if let Some(pending_event) = PendingEvent::with_header(routing_key, event, None, tx) {
            let append_event = Incoming::AppendEvent(pending_event);
            if let Err(e) = self.sender.send((append_event, size)).await {
                error!(
                    "failed to write to transaction {:?} due to {:?}",
                    self.info.txn_id, e
                );
                return Err(TransactionError::TxnSegmentWriterError {
                    error_msg: format!("{:?}", e),
                });
            }
            self.event_handles.push(rx);
        } else {
            let e = rx.await.expect("get error");
            error!(
                "failed to write to transaction {:?} due to {:?}",
                self.info.txn_id, e
            );
            return Err(TransactionError::TxnSegmentWriterError {
                error_msg: format!("{:?}", e),
            });
        }

        let mut ack_up_to = 0;
        for event in &mut self.event_handles {
            match event.try_recv() {
                Ok(res) => {
                    if let Err(e) = res {
                        return Err(TransactionError::TxnSegmentWriterError {
                            error_msg: format!("error when writing event: {:?}", e),
                        });
                    } else {
                        ack_up_to += 1;
                    }
                }
                Err(TryRecvError::Empty) => {
                    break;
                }
                Err(TryRecvError::Closed) => {
                    return Err(TransactionError::TxnSegmentWriterError {
                        error_msg: "event handle closed, cannot get result for event".to_string(),
                    });
                }
            }
        }
        self.event_handles.drain(0..ack_up_to);
        Ok(())
    }

    /// commit accepts a timestamp and will send a commit request to Pravega controller.
    pub async fn commit(&mut self, timestamp: Timestamp) -> Result<(), TransactionError> {
        debug!("committing transaction {:?}", self.info.txn_id);

        self.error_if_closed()?;
        self.info.closed = true;

        for event in &mut self.event_handles {
            match event.await {
                Ok(res) => {
                    if let Err(e) = res {
                        return Err(TransactionError::TxnSegmentWriterError {
                            error_msg: format!("error when writing event: {:?}", e),
                        });
                    }
                }
                Err(e) => {
                    return Err(TransactionError::TxnSegmentWriterError {
                        error_msg: format!("event handle closed, cannot get result for event: {:?}", e),
                    });
                }
            }
        }
        self.event_handles.clear();

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

impl Drop for Transaction {
    fn drop(&mut self) {
        let _res = self.sender.send((Incoming::Close(), 0));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::transaction::transactional_event_stream_writer::test::create_txn_stream_writer;
    use tokio::runtime::Runtime;

    #[test]
    fn test_txn_commit() {
        let mut rt = Runtime::new().unwrap();
        let mut txn_stream_writer = rt.block_on(create_txn_stream_writer());
        let mut txn = rt
            .block_on(txn_stream_writer.begin())
            .expect("begin a transaction");

        let status = rt.block_on(txn.check_status()).unwrap();
        assert_eq!(status, TransactionStatus::Open);

        rt.block_on(txn.write_event(None, vec![1; 1024])).unwrap();

        rt.block_on(txn.commit(Timestamp(0))).unwrap();
        let status = rt.block_on(txn.check_status()).unwrap();
        assert_eq!(status, TransactionStatus::Committed);
    }

    #[test]
    fn test_txn_abort() {
        let mut rt = Runtime::new().unwrap();
        let mut txn_stream_writer = rt.block_on(create_txn_stream_writer());
        let mut txn = rt
            .block_on(txn_stream_writer.begin())
            .expect("begin a transaction");

        let status = rt.block_on(txn.check_status()).unwrap();
        assert_eq!(status, TransactionStatus::Open);

        rt.block_on(txn.write_event(None, vec![1; 1024])).unwrap();

        rt.block_on(txn.abort()).unwrap();
        let status = rt.block_on(txn.check_status()).unwrap();
        assert_eq!(status, TransactionStatus::Aborted);
    }

    #[test]
    fn test_txn_write_event() {
        let mut rt = Runtime::new().unwrap();
        let mut txn_stream_writer = rt.block_on(create_txn_stream_writer());
        let mut txn = rt
            .block_on(txn_stream_writer.begin())
            .expect("begin a transaction");
        rt.block_on(txn.write_event(None, vec![1; 1024])).unwrap();
        rt.block_on(txn.write_event(None, vec![1; 1024])).unwrap();
        rt.block_on(txn.write_event(None, vec![1; 1024])).unwrap();
        assert!(!txn.event_handles.is_empty());

        rt.block_on(txn.commit(Timestamp(0))).unwrap();
        assert!(txn.event_handles.is_empty());
    }
}
