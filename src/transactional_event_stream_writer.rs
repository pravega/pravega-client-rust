//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactoryInternal;
use crate::event_stream_writer::{EventSegmentWriter, Incoming, PendingEvent};
use crate::error::TransactionalEventStreamWriterError;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_rust_client_shared::{ScopedStream, ScopedSegment, TxId, StreamSegments, PingStatus, TransactionStatus};
use pravega_wire_protocol::client_config::ClientConfig;
use uuid::Uuid;
use std::collections::{HashSet, HashMap, BTreeMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
use log::{info, error, debug};
use tokio::time::delay_for;
use tokio::sync::oneshot;

pub struct TransactionalEventStreamWriter {
    stream: ScopedStream,
    writer_id: Uuid,
    factory: Arc<ClientFactoryInternal>,
    config: ClientConfig,
    pinger_handle: PingerHandle,
}

impl TransactionalEventStreamWriter {
    pub(crate) async fn new(stream: ScopedStream, factory: Arc<ClientFactoryInternal>, config: ClientConfig) -> Self {
        let (mut pinger, handle) = Pinger::new(stream.clone(), config.transaction_timeout_time, factory.clone());
        tokio::spawn( async move {
                pinger.start_ping().await;
            }
        );
        TransactionalEventStreamWriter {
            stream,
            writer_id: Uuid::new_v4(),
            factory,
            config,
            pinger_handle: handle,
        }
    }

    async fn begin(&mut self) -> Result<Transaction, TransactionalEventStreamWriterError> {
        let txn_segments = self.factory.controller_client.create_transaction(&self.stream, Duration::from_millis(self.config.transaction_timeout_time)).await.expect("failed to create transaction");
        info!("Transaction {} created", txn_segments.tx_id);
        let txn_id = txn_segments.tx_id;
        let mut transactions = HashMap::new();
        for s in txn_segments.stream_segments.get_segments() {
            let writer = TransactionalEventSegmentWriter::new(s.clone(), self.config.retry_policy);
            transactions.insert(s, writer);
        }
        self.pinger_handle.add(txn_id).await?;
        Ok(Transaction::new(self.writer_id, txn_id, transactions, txn_segments.stream_segments, self.stream.clone(), self.pinger_handle.clone()))

    }

    async fn get_txn(&self, txn_id: TxId) -> Transaction {
        let segments = self.factory.controller_client.get_current_segments(&self.stream).await.expect("get current segments");
        let status = self.factory.controller_client.check_transaction_status(&self.stream, txn_id).await.expect("get transaction status");
        if status != TransactionStatus::Open {
            return Transaction::new(self.writer_id, txn_id, HashMap::new(), StreamSegments::new(BTreeMap::new()), self.stream.clone(), self.pinger_handle.clone())
        }
        let mut transactions = HashMap::new();
        for s in segments.get_segments() {
            let writer = TransactionalEventSegmentWriter::new(s.clone(), self.config.retry_policy);
            transactions.insert(s, writer);
        }
        Transaction::new(self.writer_id, txn_id, transactions, segments, self.stream.clone(), self.pinger_handle.clone())
    }

    fn get_config(&self) -> ClientConfig {
        self.config.clone()
    }
}

struct TransactionalEventSegmentWriter {
    segment: ScopedSegment,
    event_segment_writer: EventSegmentWriter,
    recevier: Receiver<Incoming>,

}

impl TransactionalEventSegmentWriter {
    const CHANNEL_CAPACITY: usize = 100;

    fn new(segment: ScopedSegment, retry_policy: RetryWithBackoff) -> Self {
        let (tx, rx) = channel(TransactionalEventSegmentWriter::CHANNEL_CAPACITY);
        let event_segment_writer = EventSegmentWriter::new(segment.clone(), tx, retry_policy);
        TransactionalEventSegmentWriter {
            segment,
            event_segment_writer,
            recevier: rx,
        }
    }

    fn write_event(&mut self, event: Vec<u8>) {
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        if let Some(pending_event) = PendingEvent::with_header(None, event, oneshot_tx) {
            self.event_segment_writer.write(pendingEvent);

        }
        outstanding.addLast(ack);
        removeCompleted();
    }
}


pub struct Transaction {
    writer_id: Uuid,
    txn_id: TxId,
    inner: HashMap<ScopedSegment, TransactionalEventSegmentWriter>,
    segments: StreamSegments,
    stream: ScopedStream,
    handle: PingerHandle,
}

impl Transaction {
    fn new(writer_id: Uuid, txn_id: TxId, transactions: HashMap<ScopedSegment, TransactionalEventSegmentWriter>, segments: StreamSegments, stream: ScopedStream, handle: PingerHandle) -> Self {
        Transaction {
            writer_id,
            txn_id,
            inner: transactions,
            segments,
            stream,
            handle,
        }
    }
}

pub struct Pinger {
    stream: ScopedStream,
    txn_lease_millis: u64,
    ping_interval_millis: u64,
    factory: Arc<ClientFactoryInternal>,
    receiver: Receiver<PingerEvent>
}

#[derive(Clone)]
pub struct PingerHandle (Sender<PingerEvent>);

impl PingerHandle {
    pub async fn add(&mut self, txn_id: TxId) -> Result<(), TransactionalEventStreamWriterError> {
        if let Err(_) = self.0.send(PingerEvent::Add(txn_id)).await {
            error!("Pinger has gone");
            Err(TransactionalEventStreamWriterError::PingerGone {})
        } else {
            Ok(())
        }
    }

    pub async fn remove(&mut self, txn_id: TxId) -> Result<(), TransactionalEventStreamWriterError> {
        if let Err(_) = self.0.send(PingerEvent::Remove(txn_id)).await {
            error!("Pinger has gone");
            Err(TransactionalEventStreamWriterError::PingerGone {})
        } else {
            Ok(())
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), TransactionalEventStreamWriterError> {
        if let Err(_) = self.0.send(PingerEvent::Terminate).await {
            error!("Pinger has gone");
            Err(TransactionalEventStreamWriterError::PingerGone {})
        } else {
            Ok(())
        }
    }
}

impl Pinger {
    fn new(stream: ScopedStream, txn_lease_millis: u64, factory: Arc<ClientFactoryInternal>) -> (Self, PingerHandle) {
        let (tx, rx) = channel(100);
        let pinger = Pinger {
            stream,
            txn_lease_millis,
            ping_interval_millis: Pinger::get_ping_interval(txn_lease_millis),
            factory,
            receiver: rx,
        };
        let handle = PingerHandle(tx);
        (pinger, handle)
    }

    async fn start_ping(&mut self) {
        let mut txn_list: HashSet<TxId> = HashSet::new();
        let mut completed_txns: HashSet<TxId> = HashSet::new();
        loop {
            match self.receiver.try_recv() {
                Ok(event) => {
                    match event {
                        PingerEvent::Add(id) => {
                            txn_list.insert(id);
                        }
                        PingerEvent::Remove(id) => {
                            txn_list.remove(&id);
                        }
                        PingerEvent::Terminate => {
                            return;
                        }
                    }
                }
                Err(e) => {
                    if e != TryRecvError::Empty {
                        error!("Pinger exist with error: {:?}", e);
                    }
                }
            }

            info!("Start sending transaction pings.");
            txn_list.retain(|i| !completed_txns.contains(i));
            completed_txns.clear();
            for txn_id in txn_list.iter() {
                debug!("Sending ping request for txn ID: {:?} with lease: {:?}", txn_id, self.txn_lease_millis);
                let status = self.factory.controller_client.ping_transaction(&self.stream, txn_id.clone(), Duration::from_millis(self.txn_lease_millis)).await.expect("Ping transaction");

                if let PingStatus::Ok = status {
                    debug!("Successfully pinged transaction {:?}", txn_id);
                } else {
                    debug!("Transaction {:?} is committed/aborted", txn_id);
                    completed_txns.insert(txn_id.clone());
                }
            }
            info!("Completed sending transaction pings.");

            // delay for transaction lease milliseconds.
            delay_for(Duration::from_millis(self.txn_lease_millis)).await;
        }
    }

    fn get_ping_interval(txn_lease_millis: u64) -> u64 {
        //Provides a good number of attempts: 1 for <4s, 2 for <9s, 3 for <16s, 4 for <25s, ... 10 for <100s
        //while at the same time allowing the interval to grow as the timeout gets larger.
        let target_num_pings = if txn_lease_millis > 1000u64 {
            f64::sqrt(txn_lease_millis as f64 / 1000f64)
        } else {
            1f64
        };
        (txn_lease_millis as f64 / target_num_pings).round() as u64
    }
}

enum PingerEvent {
    Add(TxId),
    Remove(TxId),
    Terminate,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_pinger() {
    }
}