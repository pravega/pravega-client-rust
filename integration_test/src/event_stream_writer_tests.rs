//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use log::info;
use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::error::EventStreamWriterError;
use pravega_client_rust::event_stream_writer::{EventStreamWriter, Processor};
use pravega_client_rust::raw_client::RawClient;
use pravega_client_rust::segment_reader::AsyncSegmentReader;
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::{ClientConfigBuilder, TEST_CONTROLLER_URI};
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::commands::{
    Command, EventCommand, GetStreamSegmentInfoCommand, SealSegmentCommand,
};
use pravega_wire_protocol::connection_factory::{
    ConnectionFactory, ConnectionType, SegmentConnectionManager,
};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::net::SocketAddr;

pub async fn test_event_stream_writer() {
    // spin up Pravega standalone
    let scope_name = Scope::new("testScopeWriter".into());
    let stream_name = Stream::new("testStreamWriter".into());
    let config = ClientConfigBuilder::default()
        .controller_uri(TEST_CONTROLLER_URI)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config.clone());
    let controller_client = client_factory.get_controller_client();
    create_scope_stream(controller_client, &scope_name, &stream_name, 2).await;

    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    let mut writer = client_factory.create_event_stream_writer(scoped_stream.clone(), config.clone());

    test_simple_write(&mut writer).await;

    test_segment_scaling_up(&mut writer, &client_factory).await;

    test_segment_scaling_down(&mut writer, &client_factory).await;

    let scope_name = Scope::new("testScopeWriter2".into());
    let stream_name = Stream::new("testStreamWriter2".into());
    create_scope_stream(controller_client, &scope_name, &stream_name, 1).await;
    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    let mut writer = client_factory.create_event_stream_writer(scoped_stream.clone(), config.clone());

    test_write_correctness(&mut writer, &client_factory).await;
    test_write_correctness_while_scaling(&mut writer, &client_factory).await;

    let scope_name = Scope::new("testScopeWriter3".into());
    let stream_name = Stream::new("testStreamWriter3".into());
    create_scope_stream(controller_client, &scope_name, &stream_name, 2).await;
    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    let mut writer = client_factory.create_event_stream_writer(scoped_stream.clone(), config.clone());
    test_write_correctness_with_routing_key(&mut writer, &client_factory).await;
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
        let reply: Result<(), EventStreamWriterError> = rx.await.expect("wait for result from oneshot");
        assert_eq!(reply.is_ok(), true);
    }
    info!("test simple write passed");
}

async fn test_segment_scaling_up(writer: &mut EventStreamWriter, factory: &ClientFactory) {
    info!("test event stream writer with segment scaled up");

    let mut receivers = vec![];
    let count = 1000;
    let mut i = 0;
    while i < count {
        if i == 500 {
            // scaling up the segment number
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
            let result = factory
                .get_controller_client()
                .update_stream(&new_config)
                .await
                .expect("scale up the segments");
            assert_eq!(result, true);
        }
        let rx = writer.write_event(String::from("hello").into_bytes()).await;
        receivers.push(rx);
        i += 1;
    }
    assert_eq!(receivers.len(), count);

    for rx in receivers {
        let reply: Result<(), EventStreamWriterError> = rx.await.expect("wait for result from oneshot");
        assert_eq!(reply.is_ok(), true);
    }

    info!("test event stream writer with segment scaled up passed");
}

async fn test_segment_scaling_down(writer: &mut EventStreamWriter, factory: &ClientFactory) {
    info!("test event stream writer with segment sealed");

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
            let result = factory
                .get_controller_client()
                .update_stream(&new_config)
                .await
                .expect("scale down the segments");
            assert_eq!(result, true);
        }
        let rx = writer.write_event(String::from("hello").into_bytes()).await;
        receivers.push(rx);
        i += 1;
    }
    assert_eq!(receivers.len(), count);

    for rx in receivers {
        let reply: Result<(), EventStreamWriterError> = rx.await.expect("wait for result from oneshot");
        assert_eq!(reply.is_ok(), true);
    }
    info!("test event stream writer with segment sealed passed");
}

async fn test_write_correctness(writer: &mut EventStreamWriter, factory: &ClientFactory) {
    info!("test read and write");
    let rx = writer.write_event(String::from("event0").into_bytes()).await;
    let reply: Result<(), EventStreamWriterError> = rx.await.expect("wait for result from oneshot");
    assert_eq!(reply.is_ok(), true);
    let scope_name = Scope::new("testScopeWriter2".into());
    let stream_name = Stream::new("testStreamWriter2".into());
    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 0 },
    };

    let async_segment_reader = factory.create_async_event_reader(segment_name).await;
    // The "event1" would be serialize into |type_code(4 bytes)|length(4 bytes)|content(6 bytes)
    let reply = async_segment_reader
        .read(0, 14)
        .await
        .expect("read event from segment");
    let data = String::from("event0").into_bytes();
    let test_event = EventCommand { data };
    let test_data = test_event.write_fields().expect("serialize the even");
    assert_eq!(reply.data, test_data);

    info!("test read and write passed");
}

async fn test_write_correctness_while_scaling(writer: &mut EventStreamWriter, factory: &ClientFactory) {
    info!("test event stream writer writes without loss or duplicate when scaling up");
    let count = 1000;
    let mut i = 1;
    let scope_name = Scope::new("testScopeWriter2".into());
    let stream_name = Stream::new("testStreamWriter2".into());

    let mut receivers = vec![];
    while i < count {
        if i == 500 {
            // scaling up the segment number
            let new_config = StreamConfiguration {
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
            let result = factory
                .get_controller_client()
                .update_stream(&new_config)
                .await
                .expect("scale up the segments");
            assert_eq!(result, true);
        }
        let data = format!("event{}", i);
        let rx = writer.write_event(data.into_bytes()).await;
        receivers.push(rx);
        i += 1;
    }
    // the data should write successfully.
    for rx in receivers {
        let reply: Result<(), EventStreamWriterError> = rx.await.expect("wait for result from oneshot");
        assert_eq!(reply.is_ok(), true);
    }

    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };

    let async_segment_reader = factory.create_async_event_reader(segment_name.clone()).await;
    let mut i = 0;
    let mut offset = 0;
    while i < count {
        let expect_string = format!("event{}", i);
        let length = (expect_string.len() + 8) as i32;
        let reply = async_segment_reader
            .read(offset, length)
            .await
            .expect("read event from segment");
        //println!("reply is {:?}", reply);
        let data = EventCommand::read_from(&reply.data).expect("deserialize data");
        assert_eq!(expect_string, String::from_utf8(data.data).unwrap());
        i += 1;
        offset += length as i64;
    }

    let raw_client = factory.create_raw_client(&segment_name).await;

    let request = Requests::GetStreamSegmentInfo(GetStreamSegmentInfoCommand {
        request_id: 0,
        segment_name: segment_name.to_string(),
        delegation_token: String::from(""),
    });

    let reply = raw_client
        .send_request(&request)
        .await
        .expect("fail to get reply");
    //println!("reply is {:?}", reply);

    if let Replies::StreamSegmentInfo(info) = reply {
        assert_eq!(info.write_offset, offset);
    } else {
        panic!("Wrong reply type");
    }
    info!("test event stream writer writes without loss or duplicate when scaling up passed");
}

async fn test_write_correctness_with_routing_key(writer: &mut EventStreamWriter, factory: &ClientFactory) {
    info!("test event stream writer writes to a stream with routing key");
    let count = 1000;
    let mut i = 0;
    let scope_name = Scope::new("testScopeWriter3".into());
    let stream_name = Stream::new("testStreamWriter3".into());
    let mut receivers = vec![];
    while i < count {
        if i % 2 == 0 {
            let data = format!("event{}", i);
            let rx = writer
                .write_event_by_routing_key(String::from("even"), data.into_bytes())
                .await;
            receivers.push(rx);
        } else {
            let data = format!("event{}", i);
            let rx = writer
                .write_event_by_routing_key(String::from("odd"), data.into_bytes())
                .await;
            receivers.push(rx);
        }
        i += 1;
    }
    let first_segment = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };

    let second_segment = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 1 },
    };
    let reader1 = factory.create_async_event_reader(first_segment.clone()).await;
    let reader2 = factory.create_async_event_reader(second_segment.clone()).await;

    let mut i = 0;
    let mut offset1 = 0;
    let mut offset2 = 0;
    while i < count {
        let expect_string = format!("event{}", i);
        let length = (expect_string.len() + 8) as i32;
        if i % 2 == 0 {
            let reply = reader2
                .read(offset2, length)
                .await
                .expect("read event from segment");
            let data = EventCommand::read_from(&reply.data).expect("deserialize data");
            assert_eq!(expect_string, String::from_utf8(data.data).unwrap());
            offset2 += length as i64;
        } else {
            let reply = reader1
                .read(offset1, length)
                .await
                .expect("read event from segment");
            let data = EventCommand::read_from(&reply.data).expect("deserialize data");
            assert_eq!(expect_string, String::from_utf8(data.data).unwrap());
            offset1 += length as i64;
        }
        i += 1;
    }
    info!("test event stream writer writes to a stream with routing key  passed");
}

/// helper function
async fn create_scope_stream(
    controller_client: &dyn ControllerClient,
    scope_name: &Scope,
    stream_name: &Stream,
    segment_number: i32,
) {
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
            min_num_segments: segment_number,
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
}
