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
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::ClientConfigBuilder;
use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryImpl};
use pravega_wire_protocol::connection_pool::{ConnectionPool, SegmentConnectionManager};
use pravega_wire_protocol::wire_commands::Requests;
use std::net::SocketAddr;

use log::info;
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};

pub async fn test_event_stream_writer() {
    // spin up Pravega standalone
    let scope_name = Scope::new("testScopeWriter".into());
    let stream_name = Stream::new("testStreamWriter".into());

    let controller_client = setup_test(&scope_name, &stream_name).await;

    let pool = get_connection_pool_for_segment().await;

    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };

    let (mut writer, processor) = EventStreamWriter::new(
        scoped_stream.clone(),
        ClientConfigBuilder::default()
            .build()
            .expect("build client config"),
    )
    .await;
    tokio::spawn(Processor::run(processor, Box::new(controller_client), pool));

    test_simple_write(&mut writer).await;

    test_scaling_up(&mut writer).await;

    test_segment_sealed(&mut writer).await;
}

async fn test_simple_write(writer: &mut EventStreamWriter) {
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

async fn test_scaling_up(writer: &mut EventStreamWriter) {
    info!("test event stream writer with segment scaled up");
    let controller_client = ControllerClientImpl::new(
        "127.0.0.1:9090"
            .parse::<SocketAddr>()
            .expect("parse to socketaddr"),
    );

    let mut receivers = vec![];
    let count = 1000;
    let mut i = 0;
    while i < count {
        if i == 500 {
            // scaling down the segment number
            let new_config = StreamConfiguration {
                scoped_stream: ScopedStream {
                    scope: Scope::new("testScopeWriter".into()),
                    stream: Stream::new("testStreamWriter".into()),
                },
                scaling: Scaling {
                    scale_type: ScaleType::FixedNumSegments,
                    target_rate: 0,
                    scale_factor: 0,
                    min_num_segments: 4,
                },
                retention: Retention {
                    retention_type: RetentionType::None,
                    retention_param: 0,
                },
            };
            controller_client
                .update_stream(&new_config)
                .await
                .expect("scale down the segments");
        }
        let rx = writer.write_event(String::from("hello").into_bytes()).await;
        receivers.push(rx);
        i += 1;
    }
    assert_eq!(receivers.len(), count);

    for rx in receivers {
        let _reply = rx.await.expect("wait for result from oneshot");
    }

    info!("test event stream writer with segment scaled up passed");
}

async fn test_segment_sealed(writer: &mut EventStreamWriter) {
    info!("test event stream writer with segment sealed");
    let controller_client = ControllerClientImpl::new(
        "127.0.0.1:9090"
            .parse::<SocketAddr>()
            .expect("parse to socketaddr"),
    );

    let mut receivers = vec![];
    let count = 1000;
    let mut i = 0;
    while i < count {
        if i == 500 {
            // scaling down the segment number
            let new_config = StreamConfiguration {
                scoped_stream: ScopedStream {
                    scope: Scope::new("testScopeWriter".into()),
                    stream: Stream::new("testStreamWriter".into()),
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
                .update_stream(&new_config)
                .await
                .expect("scale down the segments");
        }
        let rx = writer.write_event(String::from("hello").into_bytes()).await;
        receivers.push(rx);
        i += 1;
    }
    assert_eq!(receivers.len(), count);

    for rx in receivers {
        let _reply = rx.await.expect("wait for result from oneshot");
    }

    info!("test event stream writer with segment sealed passed");
}

// helper function
async fn setup_test(scope_name: &Scope, stream_name: &Stream) -> ControllerClientImpl {
    let controller_client = ControllerClientImpl::new(
        "127.0.0.1:9090"
            .parse::<SocketAddr>()
            .expect("parse to socketaddr"),
    );

    controller_client
        .create_scope(scope_name)
        .await
        .expect("create scope");

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
    controller_client
}

async fn get_connection_pool_for_segment() -> ConnectionPool<SegmentConnectionManager> {
    let config = ClientConfigBuilder::default()
        .build()
        .expect("build client config");
    let cf = Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>;
    let manager = SegmentConnectionManager::new(cf, config);
    ConnectionPool::new(manager)
}
