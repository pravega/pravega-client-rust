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
use crate::error::*;
use crate::event_stream_writer::{hash_string_to_f64, EventSegmentWriter, Incoming, PendingEvent};
use log::{debug, error, info, warn};
use pravega_controller_client::ControllerClient;
use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
use pravega_rust_client_shared::{
    PingStatus, ScopedSegment, ScopedStream, StreamSegments, Timestamp, TransactionStatus, TxId, WriterId,
};
use pravega_wire_protocol::client_config::ClientConfig;
use pravega_wire_protocol::wire_commands::Replies;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use snafu::ResultExt;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::delay_for;
use uuid::Uuid;

pub struct TransactionalEventStreamWriter {
    stream: ScopedStream,
    writer_id: WriterId,
    factory: Arc<ClientFactoryInternal>,
    config: ClientConfig,
    pinger_handle: PingerHandle,
}

impl TransactionalEventStreamWriter {
    pub(crate) async fn new(
        stream: ScopedStream,
        factory: Arc<ClientFactoryInternal>,
        config: ClientConfig,
    ) -> Self {
        let (mut pinger, handle) =
            Pinger::new(stream.clone(), config.transaction_timeout_time, factory.clone());
        tokio::spawn(async move {
            pinger.start_ping().await;
        });
        TransactionalEventStreamWriter {
            stream,
            writer_id: WriterId(Uuid::new_v4().as_u128() as u64),
            factory,
            config,
            pinger_handle: handle,
        }
    }

    async fn begin(&mut self) -> Result<Transaction, TransactionalEventStreamWriterError> {
        let txn_segments = self
            .factory
            .controller_client
            .create_transaction(
                &self.stream,
                Duration::from_millis(self.config.transaction_timeout_time),
            )
            .await
            .expect("failed to create transaction");
        info!("Transaction {} created", txn_segments.tx_id);
        let txn_id = txn_segments.tx_id;
        let mut transactions = HashMap::new();
        for s in txn_segments.stream_segments.get_segments() {
            let writer = TransactionalEventSegmentWriter::new(s.clone(), self.config.retry_policy);
            transactions.insert(s, writer);
        }
        self.pinger_handle.add(txn_id).await?;
        Ok(Transaction::new(
            self.writer_id,
            txn_id,
            transactions,
            txn_segments.stream_segments,
            self.stream.clone(),
            self.pinger_handle.clone(),
            self.factory.clone(),
            false,
        ))
    }

    async fn get_txn(&self, txn_id: TxId) -> Transaction {
        let segments = self
            .factory
            .controller_client
            .get_current_segments(&self.stream)
            .await
            .expect("get current segments");
        let status = self
            .factory
            .controller_client
            .check_transaction_status(&self.stream, txn_id)
            .await
            .expect("get transaction status");
        if status != TransactionStatus::Open {
            return Transaction::new(
                self.writer_id,
                txn_id,
                HashMap::new(),
                StreamSegments::new(BTreeMap::new()),
                self.stream.clone(),
                self.pinger_handle.clone(),
                self.factory.clone(),
                true,
            );
        }
        let mut transactions = HashMap::new();
        for s in segments.get_segments() {
            let writer = TransactionalEventSegmentWriter::new(s.clone(), self.config.retry_policy);
            transactions.insert(s, writer);
        }
        Transaction::new(
            self.writer_id,
            txn_id,
            transactions,
            segments,
            self.stream.clone(),
            self.pinger_handle.clone(),
            self.factory.clone(),
            false,
        )
    }

    fn get_config(&self) -> ClientConfig {
        self.config.clone()
    }
}

struct TransactionalEventSegmentWriter {
    segment: ScopedSegment,
    event_segment_writer: EventSegmentWriter,
    recevier: Receiver<Incoming>,
    outstanding: VecDeque<oneshot::Receiver<Result<(), EventStreamWriterError>>>,
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
            outstanding: VecDeque::new(),
        }
    }

    async fn receive(
        &mut self,
        factory: &ClientFactoryInternal,
    ) -> Result<(), TransactionalEventStreamWriterError> {
        loop {
            match self.recevier.try_recv() {
                Ok(event) => {
                    if let Incoming::ServerReply(reply) = event {
                        match reply.reply {
                            Replies::DataAppended(cmd) => {
                                debug!(
                                    "data appended for writer {:?}, latest event id is: {:?}",
                                    self.event_segment_writer.get_uuid(),
                                    cmd.event_number
                                );
                                self.event_segment_writer.ack(cmd.event_number);
                                match self.event_segment_writer.flush().await {
                                    Ok(()) => {
                                        continue;
                                    }
                                    Err(e) => {
                                        warn!("writer {:?} failed to flush data to segment {:?} due to {:?}, reconnecting", self.event_segment_writer.get_uuid(), self.event_segment_writer.get_segment_name(), e);
                                        self.event_segment_writer.reconnect(factory).await;
                                    }
                                }
                            }
                            _ => {
                                error!(
                                    "unexpected reply from segmentstore, transaction failed due to {:?}",
                                    reply
                                );
                                return Err(TransactionalEventStreamWriterError::UnexpectedReply {
                                    error: reply.reply,
                                });
                            }
                        }
                    } else {
                        panic!("should always be ServerReply");
                    }
                }
                Err(e) => match e {
                    TryRecvError::Empty => {
                        return Ok(());
                    }
                    _ => {
                        return Err(TransactionalEventStreamWriterError::MpscError { source: e });
                    }
                },
            }
        }
    }

    async fn write_event(
        &mut self,
        event: Vec<u8>,
        factory: &ClientFactoryInternal,
    ) -> Result<(), TransactionalEventStreamWriterError> {
        self.receive(factory).await?;
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        if let Some(pending_event) = PendingEvent::with_header(None, event, oneshot_tx) {
            self.event_segment_writer
                .write(pending_event)
                .await
                .context(EventSegmentWriterError {})?;
        }
        self.outstanding.push_back(oneshot_rx);
        self.remove_completed()?;
        Ok(())
    }

    async fn flush(
        &mut self,
        factory: &ClientFactoryInternal,
    ) -> Result<(), TransactionalEventStreamWriterError> {
        while !self.outstanding.is_empty() {
            self.receive(factory).await?;
            self.remove_completed()?;
        }
        Ok(())
    }

    fn remove_completed(&mut self) -> Result<(), TransactionalEventStreamWriterError> {
        loop {
            if self.outstanding.is_empty() {
                return Ok(());
            }
            let mut rx = self.outstanding.pop_front().expect("pop front");
            match rx.try_recv() {
                Err(oneshot::error::TryRecvError::Empty) => {
                    self.outstanding.push_front(rx);
                    return Ok(());
                }
                Ok(reply) => {
                    if let Err(e) = reply {
                        return Err(TransactionalEventStreamWriterError::EventSegmentWriterError {
                            source: e,
                        });
                    } else {
                        return Ok(());
                    }
                }
                Err(e) => {
                    return Err(TransactionalEventStreamWriterError::OneshotError { source: e });
                }
            }
        }
    }
}

pub struct Transaction {
    writer_id: WriterId,
    txn_id: TxId,
    inner: HashMap<ScopedSegment, TransactionalEventSegmentWriter>,
    segments: StreamSegments,
    stream: ScopedStream,
    handle: PingerHandle,
    factory: Arc<ClientFactoryInternal>,
    closed: bool,
}

impl Transaction {
    fn new(
        writer_id: WriterId,
        txn_id: TxId,
        transactions: HashMap<ScopedSegment, TransactionalEventSegmentWriter>,
        segments: StreamSegments,
        stream: ScopedStream,
        handle: PingerHandle,
        factory: Arc<ClientFactoryInternal>,
        closed: bool,
    ) -> Self {
        Transaction {
            writer_id,
            txn_id,
            inner: transactions,
            segments,
            stream,
            handle,
            factory,
            closed,
        }
    }

    pub async fn write_event(&mut self, key: Option<String>, event: Vec<u8>) -> Result<(), TransactionError> {
        self.error_if_closed()?;

        let mut small_rng = SmallRng::from_entropy();
        let segment = if let Some(key) = key {
            self.segments.get_segment(hash_string_to_f64(&key))
        } else {
            self.segments.get_segment(small_rng.gen::<f64>())
        };
        let transaction = self
            .inner
            .get_mut(&segment)
            .expect("must get segment from transaction");
        transaction
            .write_event(event, &self.factory)
            .await
            .context(TransactionFailed {})?;
        Ok(())
    }

    pub async fn commit(&mut self, timestamp: Timestamp) -> Result<(), TransactionError> {
        self.error_if_closed()?;

        for (_, writer) in &mut self.inner {
            writer.flush(&self.factory).await.context(TransactionFailed {})?;
        }
        self.inner.clear(); // release the ownership of all event segment writer
        self.factory
            .controller_client
            .commit_transaction(&self.stream, self.txn_id, self.writer_id, timestamp)
            .await
            .expect("commit transaction");
        self.closed = true;
        Ok(())
    }

    pub async fn abort(&mut self) -> Result<(), TransactionError> {
        self.error_if_closed()?;

        self.handle
            .remove(self.txn_id)
            .await
            .context(TransactionFailed {})?;

        for (_, writer) in &mut self.inner {
            writer.flush(&self.factory).await.context(TransactionFailed {})?;
        }
        self.inner.clear(); // release the ownership of all event segment writer
        self.factory
            .controller_client
            .abort_transaction(&self.stream, self.txn_id)
            .await
            .expect("abort transaction");
        self.closed = true;
        Ok(())
    }

    pub async fn check_status(&self) -> Result<TransactionStatus, TransactionError> {
        self.factory
            .controller_client
            .check_transaction_status(&self.stream, self.txn_id)
            .await
            .context(TransactionControllerError {})
    }

    fn error_if_closed(&self) -> Result<(), TransactionError> {
        if self.closed {
            Err(TransactionError::TransactionClosed { id: self.txn_id })
        } else {
            Ok(())
        }
    }
}

pub struct Pinger {
    stream: ScopedStream,
    txn_lease_millis: u64,
    ping_interval_millis: u64,
    factory: Arc<ClientFactoryInternal>,
    receiver: Receiver<PingerEvent>,
}

#[derive(Clone)]
pub struct PingerHandle(Sender<PingerEvent>);

impl PingerHandle {
    pub async fn add(&mut self, txn_id: TxId) -> Result<(), TransactionalEventStreamWriterError> {
        if let Err(_) = self.0.send(PingerEvent::Add(txn_id)).await {
            error!("pinger has gone");
            Err(TransactionalEventStreamWriterError::PingerGone {})
        } else {
            Ok(())
        }
    }

    pub async fn remove(&mut self, txn_id: TxId) -> Result<(), TransactionalEventStreamWriterError> {
        if let Err(_) = self.0.send(PingerEvent::Remove(txn_id)).await {
            error!("pinger has gone");
            Err(TransactionalEventStreamWriterError::PingerGone {})
        } else {
            Ok(())
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), TransactionalEventStreamWriterError> {
        if let Err(_) = self.0.send(PingerEvent::Terminate).await {
            error!("pinger has gone");
            Err(TransactionalEventStreamWriterError::PingerGone {})
        } else {
            Ok(())
        }
    }
}

impl Pinger {
    fn new(
        stream: ScopedStream,
        txn_lease_millis: u64,
        factory: Arc<ClientFactoryInternal>,
    ) -> (Self, PingerHandle) {
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
                Ok(event) => match event {
                    PingerEvent::Add(id) => {
                        txn_list.insert(id);
                    }
                    PingerEvent::Remove(id) => {
                        txn_list.remove(&id);
                    }
                    PingerEvent::Terminate => {
                        return;
                    }
                },
                Err(e) => {
                    if e != TryRecvError::Empty {
                        error!("pinger exit with error: {:?}", e);
                    }
                }
            }

            info!("start sending transaction pings.");
            txn_list.retain(|i| !completed_txns.contains(i));
            completed_txns.clear();
            for txn_id in txn_list.iter() {
                debug!(
                    "sending ping request for txn ID: {:?} with lease: {:?}",
                    txn_id, self.txn_lease_millis
                );
                let status = self
                    .factory
                    .controller_client
                    .ping_transaction(
                        &self.stream,
                        txn_id.clone(),
                        Duration::from_millis(self.txn_lease_millis),
                    )
                    .await
                    .expect("ping transaction");

                if let PingStatus::Ok = status {
                    debug!("successfully pinged transaction {:?}", txn_id);
                } else {
                    debug!("transaction {:?} is committed/aborted", txn_id);
                    completed_txns.insert(txn_id.clone());
                }
            }
            info!("completed sending transaction pings.");

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
}
