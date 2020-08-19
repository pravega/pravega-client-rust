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
use crate::transaction::pinger::{Pinger, PingerHandle};
use crate::transaction::transactional_event_segment_writer::TransactionalEventSegmentWriter;
use crate::transaction::{Transaction, TransactionInfo};
use pravega_rust_client_auth::DelegationTokenProvider;
use pravega_rust_client_config::ClientConfig;
use pravega_rust_client_shared::{ScopedStream, StreamSegments, TransactionStatus, TxId, WriterId};
use snafu::ResultExt;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, info_span};
use tracing_futures::Instrument;

/// A writer that writes Events to an Event stream transactionally. Events that are written to the
/// transaction can be committed atomically, which means that reader cannot see any writes prior to committing.
/// # Example
///
/// ```no_run
/// use std::net::SocketAddr;
/// use tokio;
/// use pravega_rust_client_shared::{Timestamp, ScopedStream, Scope, Stream, WriterId};
/// use pravega_client_rust::client_factory::ClientFactory;
/// use pravega_rust_client_config::ClientConfigBuilder;
///
/// #[tokio::main]
/// async fn main() {
///     let scope_name = Scope::from("testScope".to_owned());
///     let stream_name = Stream::from("testStream".to_owned());
///     let scoped_stream = ScopedStream {
///          scope: scope_name.clone(),
///          stream: stream_name.clone(),
///      };
///     // omit the step to create scope and stream in Pravega
///
///     let config = ClientConfigBuilder::default()
///         .controller_uri("127.0.0.1:9090".parse::<SocketAddr>().unwrap())
///         .build()
///         .expect("creating config");
///     let client_factory = ClientFactory::new(config.clone());
///     let mut writer = client_factory
///         .create_transactional_event_stream_writer(scoped_stream.clone(), WriterId(0))
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
pub struct TransactionalEventStreamWriter {
    stream: ScopedStream,
    writer_id: WriterId,
    factory: Arc<ClientFactoryInternal>,
    config: ClientConfig,
    pinger_handle: PingerHandle,
    delegation_token_provider: Arc<Box<dyn DelegationTokenProvider>>,
}

impl TransactionalEventStreamWriter {
    // use ClientFactory to initialize a TransactionalEventStreamWriter, so this new method is
    // marked as pub(crate).
    pub(crate) async fn new(
        stream: ScopedStream,
        writer_id: WriterId,
        factory: Arc<ClientFactoryInternal>,
        config: ClientConfig,
    ) -> Self {
        let (mut pinger, pinger_handle) =
            Pinger::new(stream.clone(), config.transaction_timeout_time, factory.clone());
        let delegation_token_provider = Arc::new(factory.create_delegation_token_provider(stream.clone()));
        let runtime_handle = factory.get_runtime_handle();
        let span = info_span!("Pinger", transactional_event_stream_writer = %writer_id);
        runtime_handle.enter(|| tokio::spawn(async move { pinger.start_ping().instrument(span).await }));
        TransactionalEventStreamWriter {
            stream,
            writer_id,
            factory,
            config,
            pinger_handle: handle,
            delegation_token_provider,
        }
    }

    /// This method opens a transaction by sending a request to Pravega controller.
    pub async fn begin(&mut self) -> Result<Transaction, TransactionalEventStreamWriterError> {
        let txn_segments = self
            .factory
            .get_controller_client()
            .create_transaction(
                &self.stream,
                Duration::from_millis(self.config.transaction_timeout_time),
            )
            .await
            .map_err(|e| e.error)
            .context(TxnStreamControllerError {})?;
        info!("Transaction {} created", txn_segments.tx_id);
        let txn_id = txn_segments.tx_id;
        let mut transactions = HashMap::new();
        for s in txn_segments.stream_segments.get_segments() {
            let mut txn_segment = s.clone();
            txn_segment.segment.tx_id = Some(txn_id);
            let mut writer = TransactionalEventSegmentWriter::new(
                txn_segment,
                self.config.retry_policy,
                self.delegation_token_provider.clone(),
            );
            writer.initialize(&self.factory).await;
            transactions.insert(s, writer);
        }
        self.pinger_handle.add(txn_id).await?;
        Ok(Transaction::new(
            TransactionInfo::new(txn_id, self.writer_id, self.stream.clone(), false),
            transactions,
            txn_segments.stream_segments,
            self.pinger_handle.clone(),
            self.factory.clone(),
        ))
    }

    /// This method returns the Transaction based on the given transaction id.
    /// If the current transaction is not in open status, meaning it has been committed
    /// or aborted, this method will create a closed transaction that only contains the meta data
    /// of this transaction.
    pub async fn get_txn(&self, txn_id: TxId) -> Result<Transaction, TransactionalEventStreamWriterError> {
        let status = self
            .factory
            .get_controller_client()
            .check_transaction_status(&self.stream, txn_id)
            .await
            .map_err(|e| e.error)
            .context(TxnStreamControllerError {})?;
        if status != TransactionStatus::Open {
            return Ok(Transaction::new(
                TransactionInfo::new(txn_id, self.writer_id, self.stream.clone(), true),
                HashMap::new(),
                StreamSegments::new(BTreeMap::new()),
                self.pinger_handle.clone(),
                self.factory.clone(),
            ));
        }
        let mut transactions = HashMap::new();
        let segments = self
            .factory
            .get_controller_client()
            .get_epoch_segments(&self.stream, txn_id.get_epoch())
            .await
            .map_err(|e| e.error)
            .context(TxnStreamControllerError {})?;
        for s in segments.get_segments() {
            let writer = TransactionalEventSegmentWriter::new(
                s.clone(),
                self.config.retry_policy,
                self.delegation_token_provider.clone(),
            );
            transactions.insert(s, writer);
        }
        Ok(Transaction::new(
            TransactionInfo::new(txn_id, self.writer_id, self.stream.clone(), true),
            transactions,
            segments,
            self.pinger_handle.clone(),
            self.factory.clone(),
        ))
    }

    /// This method gets a copy of the ClientConfig
    pub fn get_config(&self) -> ClientConfig {
        self.config.clone()
    }
}
