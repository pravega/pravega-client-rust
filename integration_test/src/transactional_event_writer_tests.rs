//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_client::event::writer::EventWriter;
use pravega_client_config::{ClientConfigBuilder, MOCK_CONTROLLER_URI};
use pravega_client_shared::*;
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_wire_protocol::connection_factory::{ConnectionFactory, SegmentConnectionManager};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::net::SocketAddr;

use pravega_client::client_factory::{ClientFactory, ClientFactoryAsync};
use pravega_client::event::transactional_writer::Transaction;
use pravega_client::event::transactional_writer::TransactionalEventWriter;
use pravega_client::test_utils::{MetadataWrapper, RawClientWrapper};
use tracing::{error, info};

use crate::pravega_service::PravegaStandaloneServiceConfig;
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::commands::{
    Command, EventCommand, GetStreamSegmentInfoCommand, StreamSegmentInfoCommand,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::time::sleep;

pub fn test_transactional_event_stream_writer(config: PravegaStandaloneServiceConfig) {
    info!("test TransactionalEventStreamWriter");
    // spin up Pravega standalone
    let scope_name = Scope::from("testScopeTxnWriter".to_owned());
    let stream_name = Stream::from("testStreamTxnWriter".to_owned());
    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    let config = ClientConfigBuilder::default()
        .controller_uri(MOCK_CONTROLLER_URI)
        .is_auth_enabled(config.auth)
        .is_tls_enabled(config.tls)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config);
    let handle = client_factory.runtime();
    handle.block_on(setup_test(
        &scope_name,
        &stream_name,
        client_factory.controller_client(),
    ));

    let mut writer =
        handle.block_on(client_factory.create_transactional_event_writer(scoped_stream.clone(), WriterId(0)));

    handle.block_on(test_commit_transaction(&mut writer));
    handle.block_on(test_abort_transaction(&mut writer));
    handle.block_on(test_write_and_read_transaction(&mut writer, &client_factory));
    handle.block_on(test_multiple_transactions(&client_factory, scoped_stream));

    info!("test TransactionalEventStreamWriter passed");
}

async fn test_commit_transaction(writer: &mut TransactionalEventWriter) {
    info!("test commit transaction");

    let mut transaction = writer.begin().await.expect("begin transaction");

    assert_eq!(
        transaction.check_status().await.expect("get transaction status"),
        TransactionStatus::Open
    );

    transaction
        .commit(Timestamp(0u64))
        .await
        .expect("commit transaction");

    wait_for_transaction_with_timeout(&transaction, TransactionStatus::Committed, 10).await;

    info!("test commit transaction passed");
}

async fn test_abort_transaction(writer: &mut TransactionalEventWriter) {
    info!("test abort transaction");

    let mut transaction = writer.begin().await.expect("begin transaction");

    assert_eq!(
        transaction.check_status().await.expect("get transaction status"),
        TransactionStatus::Open
    );

    transaction.abort().await.expect("abort transaction");

    wait_for_transaction_with_timeout(&transaction, TransactionStatus::Aborted, 10).await;

    info!("test abort transaction passed");
}

async fn test_write_and_read_transaction(writer: &mut TransactionalEventWriter, factory: &ClientFactory) {
    info!("test write transaction");

    let mut transaction = writer.begin().await.expect("begin transaction");

    assert_eq!(
        transaction.check_status().await.expect("get transaction status"),
        TransactionStatus::Open
    );

    // write to server
    let num_events: i32 = 100;
    for _ in 0..num_events {
        transaction
            .write_event(None, String::from("hello").into_bytes())
            .await
            .expect("write to transaction");
    }
    let segments = factory
        .controller_client()
        .get_current_segments(&transaction.stream())
        .await
        .expect("get segments");
    // should not be able to see the write before commit
    for segment in segments.get_segments() {
        let segment_info = get_segment_info(&segment, factory).await;
        assert_eq!(segment_info.write_offset, 0);
    }
    transaction
        .commit(Timestamp(0u64))
        .await
        .expect("commit transaction");

    wait_for_transaction_with_timeout(&transaction, TransactionStatus::Committed, 10).await;

    // read from server after commit
    let mut count: i32 = 0;
    let data: &str = "hello";
    for segment in segments.get_segments() {
        info!("creating reader for segment {:?}", segment);
        let reader = factory.create_segment_reader_wrapper(segment.clone()).await;
        let segment_info = get_segment_info(&segment, factory).await;
        let mut offset = 0;
        let end_offset = segment_info.write_offset;
        loop {
            if offset >= end_offset {
                break;
            }
            match reader.read(offset, data.len() as i32 + 8).await {
                Ok(reply) => {
                    count += 1;
                    offset += data.len() as i64 + 8;
                    let expected = EventCommand {
                        data: String::from("hello").into_bytes(),
                    }
                    .write_fields()
                    .expect("serialize cmd");
                    assert_eq!(reply.data, expected);
                }
                Err(e) => {
                    error!("error {:?} when reading from segment", e);
                    panic!("failed to read data from segmentstore");
                }
            }
        }
    }

    assert_eq!(count, num_events);
    info!("test write transaction passed");
}

async fn test_multiple_transactions(factory: &ClientFactory, stream: ScopedStream) {
    // multiple transaction writers
    let mut transactions = vec![];
    let mut transaction_writers = vec![];
    for writer_id in 1..100 {
        let mut transaction_writer = factory
            .create_transactional_event_writer(stream.clone(), WriterId(writer_id as u128))
            .await;
        let transaction = transaction_writer.begin().await.expect("begin transaction");
        transaction_writers.push(transaction_writer);
        transactions.push(transaction);
    }

    for mut transaction in transactions {
        let event = vec![1; 100];
        transaction.write_event(None, event).await.expect("write event");
        let timestamp = Timestamp(0);
        transaction.commit(timestamp).await.expect("commit");
    }

    // one transaction writer with multiple transactions
    let mut transactions = vec![];
    let mut transaction_writer = factory
        .create_transactional_event_writer(stream.clone(), WriterId(100))
        .await;
    for _i in 0..100 {
        let transaction = transaction_writer.begin().await.expect("begin transaction");
        transactions.push(transaction);
    }

    for mut transaction in transactions {
        let event = vec![1; 100];
        transaction.write_event(None, event).await.expect("write event");
        let timestamp = Timestamp(0);
        transaction.commit(timestamp).await.expect("commit");
    }
}

// helper function
async fn setup_test(scope_name: &Scope, stream_name: &Stream, controller_client: &dyn ControllerClient) {
    controller_client
        .create_scope(scope_name)
        .await
        .expect("create scope");
    info!("Scope created");
    let request = StreamConfiguration {
        scoped_stream: ScopedStream {
            scope: scope_name.clone(),
            stream: stream_name.clone(),
        },
        scaling: Scaling {
            scale_type: ScaleType::FixedNumSegments,
            target_rate: 0,
            scale_factor: 0,
            min_num_segments: 2,
        },
        retention: Retention {
            retention_type: RetentionType::None,
            retention_param: 0,
        },
        tags: None,
    };
    controller_client
        .create_stream(&request)
        .await
        .expect("create stream");
    info!("Stream created");
}

async fn get_segment_info(segment: &ScopedSegment, factory: &ClientFactory) -> SegmentInfo {
    let meta = MetadataWrapper::new(segment.clone(), factory).await;
    meta.get_segment_info().await.expect("get segment info")
}

async fn wait_for_transaction_with_timeout(
    transaction: &Transaction,
    expected_status: TransactionStatus,
    timeout_second: i32,
) {
    for _i in 0..timeout_second {
        if expected_status == transaction.check_status().await.expect("get transaction status") {
            return;
        }
        sleep(Duration::from_secs(1)).await;
    }
    panic!(
        "timeout {:?} exceeded, Transaction is not {:?}",
        timeout_second, expected_status
    );
}
