//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactoryAsync;
use crate::segment::event::{Incoming, PendingEvent, RoutingInfo};
use crate::segment::reactor::Reactor;

use pravega_client_auth::DelegationTokenProvider;
use pravega_client_channel::{create_channel, ChannelSender};
use pravega_client_shared::{
    PingStatus, ScopedStream, StreamSegments, Timestamp, TransactionStatus, TxId, WriterId,
};
use pravega_controller_client::ControllerError;

use futures::FutureExt;
use snafu::{ResultExt, Snafu};
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::io::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::time::sleep;
use tracing::{debug, error, info, info_span};
use tracing_futures::Instrument;

/// Write events to a stream transactionally.
///
/// Events that are written to the transaction can be committed atomically,
/// which means that reader cannot see any writes prior to committing
/// and will not see any writes if the transaction is aborted.
///
/// # Example
///
/// ```no_run
/// use tokio;
/// use pravega_client_shared::{Timestamp, ScopedStream, Scope, Stream, WriterId, PravegaNodeUri};
/// use pravega_client::client_factory::ClientFactory;
/// use pravega_client_config::ClientConfigBuilder;
///
/// #[tokio::main]
/// async fn main() {
///     let scope_name = Scope::from("txnScope".to_owned());
///     let stream_name = Stream::from("txnStream".to_owned());
///     let scoped_stream = ScopedStream {
///          scope: scope_name.clone(),
///          stream: stream_name.clone(),
///      };
///     // omit the step to create scope and stream in Pravega
///
///     let config = ClientConfigBuilder::default()
///         .controller_uri(PravegaNodeUri::from("tcp://127.0.0.2:9091".to_string()))
///         .build()
///         .expect("creating config");
///     let client_factory = ClientFactory::new(config);
///     let mut writer = client_factory
///         .create_transactional_event_writer(scoped_stream.clone(), WriterId(0))
///         .await;
///
///     // start a transaction
///     let mut transaction = writer.begin().await.expect("begin transaction");
///
///     // do something with it
///     transaction.write_event(None, String::from("hello").into_bytes()).await.unwrap();
///
///     // commit the transaction
///     transaction.commit(Timestamp(0u64)).await;
/// }
/// ```
pub struct TransactionalEventWriter {
    stream: ScopedStream,
    writer_id: WriterId,
    factory: ClientFactoryAsync,
    pinger_handle: PingerHandle,
    delegation_token_provider: Arc<DelegationTokenProvider>,
    shutdown: oneshot::Sender<()>,
}

impl TransactionalEventWriter {
    // use ClientFactory to initialize a TransactionalEventStreamWriter.
    pub(crate) async fn new(stream: ScopedStream, writer_id: WriterId, factory: ClientFactoryAsync) -> Self {
        let (mut pinger, pinger_handle, shutdown) = Pinger::new(
            stream.clone(),
            factory.config().transaction_timeout_time,
            factory.clone(),
        );
        let delegation_token_provider =
            Arc::new(factory.create_delegation_token_provider(stream.clone()).await);
        let span = info_span!("Pinger", transactional_event_stream_writer = %writer_id);
        factory
            .runtime_handle()
            .spawn(async move { pinger.start_ping().instrument(span).await });
        TransactionalEventWriter {
            stream,
            writer_id,
            factory,
            pinger_handle,
            delegation_token_provider,
            shutdown,
        }
    }

    /// This method opens a transaction by sending a request to Pravega controller.
    pub async fn begin(&mut self) -> Result<Transaction, TransactionalEventWriterError> {
        let txn_segments = self
            .factory
            .controller_client()
            .create_transaction(
                &self.stream,
                Duration::from_millis(self.factory.config().transaction_timeout_time),
            )
            .await
            .map_err(|e| e.error)
            .context(TxnStreamControllerError {})?;
        info!("Transaction {} created", txn_segments.tx_id);
        let txn_id = txn_segments.tx_id;
        self.pinger_handle.add(txn_id)?;
        Ok(Transaction::new(
            TransactionInfo::new(txn_id, self.writer_id, self.stream.clone(), false),
            txn_segments.stream_segments,
            self.pinger_handle.clone(),
            self.factory.clone(),
            false,
        )
        .await)
    }

    /// This method returns the Transaction based on the given transaction id.
    /// If the current transaction is not in open status, meaning it has been committed
    /// or aborted, this method will create a closed transaction that only contains the meta data
    /// of this transaction.
    pub async fn get_txn(&self, txn_id: TxId) -> Result<Transaction, TransactionalEventWriterError> {
        let status = self
            .factory
            .controller_client()
            .check_transaction_status(&self.stream, txn_id)
            .await
            .map_err(|e| e.error)
            .context(TxnStreamControllerError {})?;
        if status != TransactionStatus::Open {
            return Ok(Transaction::new(
                TransactionInfo::new(txn_id, self.writer_id, self.stream.clone(), true),
                StreamSegments::new(BTreeMap::new()),
                self.pinger_handle.clone(),
                self.factory.clone(),
                true,
            )
            .await);
        }
        let segments = self
            .factory
            .controller_client()
            .get_epoch_segments(&self.stream, txn_id.get_epoch())
            .await
            .map_err(|e| e.error)
            .context(TxnStreamControllerError {})?;
        Ok(Transaction::new(
            TransactionInfo::new(txn_id, self.writer_id, self.stream.clone(), true),
            segments,
            self.pinger_handle.clone(),
            self.factory.clone(),
            false,
        )
        .await)
    }
}

// contains the metadata of Transaction
#[derive(new)]
struct TransactionInfo {
    txn_id: TxId,
    writer_id: WriterId,
    stream: ScopedStream,
    closed: bool,
}

/// Pravega Transaction support.
///
/// It can be used to write, commit and abort a Pravega Transaction.
pub struct Transaction {
    info: TransactionInfo,
    sender: ChannelSender<Incoming>,
    handle: PingerHandle,
    factory: ClientFactoryAsync,
    event_handles: Vec<EventHandle>,
}

type EventHandle = oneshot::Receiver<Result<(), Error>>;

impl Transaction {
    // maximum 16 MB total size of events could be held in memory
    const CHANNEL_CAPACITY: usize = 16 * 1024 * 1024;

    // Transaction should be created by transactional event stream writer, so this new method
    // is not public.
    async fn new(
        info: TransactionInfo,
        stream_segments: StreamSegments,
        handle: PingerHandle,
        factory: ClientFactoryAsync,
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
        let rt_handle = factory.runtime_handle();
        let writer_id = info.writer_id;
        let txn_id = info.txn_id;
        let span = info_span!("StreamReactor", txn_id = %txn_id, event_stream_writer = %writer_id);
        rt_handle.spawn(
            Reactor::run(
                info.stream.clone(),
                tx.clone(),
                rx,
                factory.clone(),
                Some(stream_segments),
            )
            .instrument(span),
        );
        Transaction {
            info,
            sender: tx,
            handle,
            factory,
            event_handles: vec![],
        }
    }

    /// get the transaction id.
    pub fn txn_id(&self) -> TxId {
        self.info.txn_id
    }

    /// get the stream that this transaction is based on.
    pub fn stream(&self) -> ScopedStream {
        self.info.stream.clone()
    }

    /// write_event accepts a vec of bytes as the input event and an optional routing key which is used
    /// to determine which segment to write to. It calls the corresponding transactional event segment
    /// writer to write the data to the server.
    pub async fn write_event(
        &mut self,
        routing_key: Option<String>,
        event: Vec<u8>,
    ) -> Result<(), TransactionError> {
        self.error_if_closed()?;

        let size = event.len();
        let (tx, rx) = oneshot::channel();
        let routing_info = RoutingInfo::RoutingKey(routing_key);
        if let Some(pending_event) = PendingEvent::with_header(routing_info, event, None, tx) {
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
            .context(TxnStreamWriterError {})?;

        self.factory
            .controller_client()
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
            .context(TxnStreamWriterError {})?;

        self.factory
            .controller_client()
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
            .controller_client()
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
        let _res = self.sender.send_without_bp(Incoming::Close());
    }
}

#[derive(Debug)]
enum PingerEvent {
    Add(TxId),
    Remove(TxId),
}

/// Pinger is used to ping transactions periodically. It spawns a task that runs in the background
/// to ping transactions without blocking the current thread. The spawned task uses a loop to run
/// the ping logic and sleeps(does not block other tasks) for a given time before next iteration.
pub(crate) struct Pinger {
    stream: ScopedStream,
    txn_lease_millis: u64,
    ping_interval_millis: u64,
    factory: ClientFactoryAsync,
    receiver: UnboundedReceiver<PingerEvent>,
    shutdown: oneshot::Receiver<()>,
}

/// PingerHandle is just a wrapped channel sender which is used to communicate with the Pinger.
/// It can be used to add or remove a transaction from Pinger's ping list.
#[derive(Clone)]
pub(crate) struct PingerHandle(UnboundedSender<PingerEvent>);

impl PingerHandle {
    pub(crate) fn add(&mut self, txn_id: TxId) -> Result<(), TransactionalEventWriterError> {
        if let Err(e) = self.0.send(PingerEvent::Add(txn_id)) {
            error!("pinger failed to add transaction: {:?}", e);
            Err(TransactionalEventWriterError::PingerError {
                msg: format!("failed to add transaction due to: {:?}", e),
            })
        } else {
            Ok(())
        }
    }

    pub(crate) fn remove(&mut self, txn_id: TxId) -> Result<(), TransactionalEventWriterError> {
        if let Err(e) = self.0.send(PingerEvent::Remove(txn_id)) {
            error!("pinger failed to remove transaction: {:?}", e);
            Err(TransactionalEventWriterError::PingerError {
                msg: format!("failed to remove transaction due to: {:?}", e),
            })
        } else {
            Ok(())
        }
    }
}

impl Pinger {
    pub(crate) fn new(
        stream: ScopedStream,
        txn_lease_millis: u64,
        factory: ClientFactoryAsync,
    ) -> (Self, PingerHandle, oneshot::Sender<()>) {
        let (tx, rx) = unbounded_channel();
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        let pinger = Pinger {
            stream,
            txn_lease_millis,
            ping_interval_millis: Pinger::ping_interval(txn_lease_millis),
            factory,
            receiver: rx,
            shutdown: oneshot_rx,
        };
        let handle = PingerHandle(tx);
        (pinger, handle, oneshot_tx)
    }

    pub(crate) async fn start_ping(&mut self) {
        // this set is used to store the transactions that are alive and
        // needs to be pinged periodically.
        let mut txn_list: HashSet<TxId> = HashSet::new();

        // this set is used to store the transaction that are aborted or committed
        let mut completed_txns: HashSet<TxId> = HashSet::new();

        loop {
            // try receive any incoming events
            if let Some(option) = self.receiver.recv().now_or_never() {
                if let Some(event) = option {
                    match event {
                        PingerEvent::Add(id) => {
                            txn_list.insert(id);
                        }
                        PingerEvent::Remove(id) => {
                            txn_list.remove(&id);
                        }
                    }
                } else {
                    panic!("pinger sender gone");
                }
            }

            // remove completed transactions from the ping list
            txn_list.retain(|i| !completed_txns.contains(i));
            completed_txns.clear();

            info!("start sending pings to {} transactions.", txn_list.len());
            // for each transaction in the ping list, send ping to the server
            for txn_id in txn_list.iter() {
                debug!(
                    "sending ping request for txn ID: {:?} with lease: {:?}",
                    txn_id, self.txn_lease_millis
                );
                let status = self
                    .factory
                    .controller_client()
                    .ping_transaction(
                        &self.stream,
                        txn_id.to_owned(),
                        Duration::from_millis(self.txn_lease_millis),
                    )
                    .await
                    .expect("ping transaction");

                if let PingStatus::Ok = status {
                    debug!("successfully pinged transaction {:?}", txn_id);
                } else {
                    debug!("transaction {:?} is committed/aborted", txn_id);
                    completed_txns.insert(txn_id.to_owned());
                }
            }
            info!("sending transaction pings complete.");

            // delay for transaction lease milliseconds.
            tokio::select! {
                _ = sleep(Duration::from_millis(self.txn_lease_millis)) => {
                    debug!("pinger wake up after {}ms", self.txn_lease_millis);
                }
                _ = &mut self.shutdown => {
                    info!("shut down pinger");
                    return;
                }
            }
        }
    }

    fn ping_interval(txn_lease_millis: u64) -> u64 {
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

#[derive(Debug, Snafu)]
pub enum TransactionalEventWriterError {
    #[snafu(display("Pinger failed to {:?}", msg))]
    PingerError { msg: String },

    #[snafu(display("Controller client failed with error {:?}", source))]
    TxnStreamControllerError { source: ControllerError },
}

#[derive(Debug, Snafu)]
pub enum TransactionError {
    #[snafu(display("Transactional failed to write due to {:?}", error_msg))]
    TxnSegmentWriterError { error_msg: String },

    #[snafu(display("Transactional stream writer failed due to {:?}", source))]
    TxnStreamWriterError { source: TransactionalEventWriterError },

    #[snafu(display("Transaction {:?} already closed", id))]
    TxnClosed { id: TxId },

    #[snafu(display("Transaction failed due to controller error: {:?}", source))]
    TxnControllerError { source: ControllerError },

    #[snafu(display("Commit Transaction {:?} error due to Transaction {:?}", id, status))]
    TxnCommitError { id: TxId, status: TransactionStatus },

    #[snafu(display("Abort Transaction {:?} error due to Transaction {:?}", id, status))]
    TxnAbortError { id: TxId, status: TransactionStatus },
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::client_factory::ClientFactory;
    use crate::util::create_stream;
    use pravega_client_config::connection_type::{ConnectionType, MockType};
    use pravega_client_config::ClientConfigBuilder;
    use pravega_client_shared::{PravegaNodeUri, ScopedSegment};
    use tokio::runtime::Runtime;

    #[test]
    fn test_get_ping_interval() {
        assert_eq!(Pinger::ping_interval(1000u64), 1000u64);
        assert_eq!(Pinger::ping_interval(4000u64), 2000u64);
        assert_eq!(Pinger::ping_interval(9000u64), 3000u64);
        assert_eq!(Pinger::ping_interval(16000u64), 4000u64);
        assert_eq!(Pinger::ping_interval(25000u64), 5000u64);
    }

    #[test]
    fn test_txn_stream_writer() {
        let rt = Runtime::new().unwrap();
        let (mut txn_stream_writer, _factory) = rt.block_on(create_txn_stream_writer());
        let transaction = rt.block_on(txn_stream_writer.begin()).expect("open transaction");
        let fetched_transaction = rt
            .block_on(txn_stream_writer.get_txn(transaction.txn_id()))
            .expect("get transaction");
        assert_eq!(transaction.txn_id(), fetched_transaction.txn_id());
    }

    #[test]
    fn test_txn_commit() {
        let rt = Runtime::new().unwrap();
        let (mut txn_stream_writer, _factory) = rt.block_on(create_txn_stream_writer());
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
        let rt = Runtime::new().unwrap();
        let (mut txn_stream_writer, _factory) = rt.block_on(create_txn_stream_writer());
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
        let rt = Runtime::new().unwrap();
        let (mut txn_stream_writer, _factory) = rt.block_on(create_txn_stream_writer());
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

    // helper function
    pub(crate) async fn create_txn_stream_writer() -> (TransactionalEventWriter, ClientFactory) {
        let txn_segment = ScopedSegment::from("scope/stream/0");
        let writer_id = WriterId(123);
        let config = ClientConfigBuilder::default()
            .connection_type(ConnectionType::Mock(MockType::Happy))
            .mock(true)
            .controller_uri(PravegaNodeUri::from("127.0.0.2:9091"))
            .build()
            .unwrap();
        let factory = ClientFactory::new(config);
        create_stream(&factory, "scope", "stream", 1).await;
        let writer = factory
            .create_transactional_event_writer(ScopedStream::from(&txn_segment), writer_id)
            .await;
        (writer, factory)
    }
}
