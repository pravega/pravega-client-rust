//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_client_rust::event_stream_writer::{EventStreamWriter, Processor};
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::{ClientConfigBuilder, TEST_CONTROLLER_URI};
use pravega_wire_protocol::connection_factory::{ConnectionFactory, SegmentConnectionManager};
use pravega_wire_protocol::wire_commands::Requests;
use std::net::SocketAddr;

use log::info;
use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::transactional_event_stream_writer::TransactionalEventStreamWriter;
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_client_rust::segment_reader::AsyncSegmentReader;

pub async fn test_transactional_event_stream_writer() {
    info!("test TransactionalEventStreamWriter");
    // spin up Pravega standalone
    let scope_name = Scope::new("testScopeTxnWriter".into());
    let stream_name = Stream::new("testStreamTxnWriter".into());
    setup_test(&scope_name, &stream_name).await;

    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    let config = ClientConfigBuilder::default()
        .controller_uri(TEST_CONTROLLER_URI)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config.clone());
    let mut writer = client_factory
        .create_transactional_event_stream_writer(scoped_stream.clone(), config)
        .await;
    test_commit_transaction(&mut writer).await;
    test_abort_transaction(&mut writer).await;
    test_write_transaction(&mut writer, &client_factory);

    info!("test TransactionalEventStreamWriter passed");
}

async fn test_commit_transaction(writer: &mut TransactionalEventStreamWriter) {
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

    assert_eq!(
        transaction.check_status().await.expect("commit transaction"),
        TransactionStatus::Committed
    );

    info!("test commit transaction passed");
}

async fn test_abort_transaction(writer: &mut TransactionalEventStreamWriter) {
    info!("test abort transaction");

    let mut transaction = writer.begin().await.expect("begin transaction");

    assert_eq!(
        transaction.check_status().await.expect("get transaction status"),
        TransactionStatus::Open
    );

    transaction.abort().await.expect("abort transaction");

    assert_eq!(
        transaction.check_status().await.expect("abort transaction"),
        TransactionStatus::Aborted
    );

    info!("test abort transaction passed");
}

async fn test_write_transaction(writer: &mut TransactionalEventStreamWriter, factory: &ClientFactory) {
    info!("test write transaction");

    let mut transaction = writer.begin().await.expect("begin transaction");

    assert_eq!(
        transaction.check_status().await.expect("get transaction status"),
        TransactionStatus::Open
    );

    for i in 0..100 {
        transaction.write_event(None, String::from("hello").into_bytes())
    }
    transaction.commit(Timestamp(0u64)).await.expect("abort transaction");

    let segments = factory.get_controller_client().get_current_segments(&transaction.get_stream()).await.expect("get segments");
    let mut readers = vec!();
    for segment in segments {
        let reader = factory.create_async_event_reader(segment).await;
        reader.
    }


    assert_eq!(
        transaction.check_status().await.expect("abort transaction"),
        TransactionStatus::Aborted
    );

    info!("test write transaction passed");
}

// helper function
async fn setup_test(scope_name: &Scope, stream_name: &Stream) -> ControllerClientImpl {
    let config = ClientConfigBuilder::default()
        .controller_uri(TEST_CONTROLLER_URI)
        .build()
        .expect("build client config");

    let controller_client = ControllerClientImpl::new(config);
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
            min_num_segments: 1,
        },
        retention: Retention {
            retention_type: RetentionType::None,
            retention_param: 0,
        },
    };
    controller_client
        .create_stream(&request)
        .await
        .expect("create stream");
    info!("Stream created");
    controller_client
}
