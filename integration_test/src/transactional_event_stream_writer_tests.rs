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
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};

pub async fn test_transactional_event_stream_writer() {
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
    let mut writer = client_factory.create_event_stream_writer(scoped_stream.clone(), config);

    test_simple_transaction(&mut writer).await;
}

async fn test_simple_transaction(writer: &mut EventStreamWriter) {
    info!("test simple write");
    let mut receivers = vec![];
    let count = 10;
    let mut i = 0;
    while i < count {
        let rx = writer.write_event(String::from("hello").into_bytes()).await;
        receivers.push(rx);
        i += 1;
    }
    assert_eq!(receivers.len(), count);

    for rx in receivers {
        let _reply = rx.await.expect("wait for result from oneshot");
    }
    info!("test simple write passed");
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
            min_num_segments: 2,
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
