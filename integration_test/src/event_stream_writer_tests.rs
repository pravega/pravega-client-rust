//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::pravega_service::PravegaStandaloneServiceConfig;
use crate::utils;
use pravega_client::client_factory::ClientFactory;
use pravega_client::error::SegmentWriterError;
use pravega_client::event_stream_writer::EventStreamWriter;
use pravega_client::raw_client::RawClient;
use pravega_client::segment_reader::AsyncSegmentReader;
use pravega_client_config::{connection_type::ConnectionType, ClientConfigBuilder, MOCK_CONTROLLER_URI};
use pravega_client_shared::*;
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::commands::{
    Command, EventCommand, GetStreamSegmentInfoCommand, SealSegmentCommand,
};
use pravega_wire_protocol::connection_factory::{ConnectionFactory, SegmentConnectionManager};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::net::SocketAddr;
use tracing::info;

pub fn test_event_stream_writer(config: PravegaStandaloneServiceConfig) {
    // spin up Pravega standalone
    let scope_name = Scope::from("testScopeWriter".to_owned());
    let stream_name = Stream::from("testStreamWriter".to_owned());
    let config = ClientConfigBuilder::default()
        .controller_uri(MOCK_CONTROLLER_URI)
        .is_auth_enabled(config.auth)
        .is_tls_enabled(config.tls)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config);
    let handle = client_factory.get_runtime();
    handle.block_on(utils::create_scope_stream(
        client_factory.get_controller_client(),
        &scope_name,
        &stream_name,
        1,
    ));

    let scoped_stream = ScopedStream {
        scope: scope_name,
        stream: stream_name,
    };
    let mut writer = client_factory.create_event_stream_writer(scoped_stream);

    handle.block_on(test_simple_write(&mut writer));

    handle.block_on(test_segment_scaling_up(&mut writer, &client_factory));

    handle.block_on(test_segment_scaling_down(&mut writer, &client_factory));

    let scope_name = Scope::from("testScopeWriter2".to_owned());
    let stream_name = Stream::from("testStreamWriter2".to_owned());
    handle.block_on(utils::create_scope_stream(
        client_factory.get_controller_client(),
        &scope_name,
        &stream_name,
        1,
    ));
    let scoped_stream = ScopedStream {
        scope: scope_name,
        stream: stream_name,
    };
    let mut writer = client_factory.create_event_stream_writer(scoped_stream);

    handle.block_on(test_write_correctness(&mut writer, &client_factory));
    handle.block_on(test_write_correctness_while_scaling(&mut writer, &client_factory));

    let scope_name = Scope::from("testScopeWriter3".to_owned());
    let stream_name = Stream::from("testStreamWriter3".to_owned());
    handle.block_on(utils::create_scope_stream(
        client_factory.get_controller_client(),
        &scope_name,
        &stream_name,
        2,
    ));
    let scoped_stream = ScopedStream {
        scope: scope_name,
        stream: stream_name,
    };
    let mut writer = client_factory.create_event_stream_writer(scoped_stream);
    handle.block_on(test_write_correctness_with_routing_key(
        &mut writer,
        &client_factory,
    ));
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
        let reply: Result<(), SegmentWriterError> = rx.await.expect("wait for result from oneshot");
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
            let scoped_stream = ScopedStream {
                scope: Scope::from("testScopeWriter".to_owned()),
                stream: Stream::from("testStreamWriter".to_owned()),
            };
            let sealed_segments = [Segment::from(0)];

            let new_range = [(0.0, 0.5), (0.5, 1.0)];

            let scale_result = factory
                .get_controller_client()
                .scale_stream(&scoped_stream, &sealed_segments, &new_range)
                .await;
            assert!(scale_result.is_ok());
            let current_segments_result = factory
                .get_controller_client()
                .get_current_segments(&scoped_stream)
                .await;
            assert_eq!(2, current_segments_result.unwrap().key_segment_map.len());
        }
        let rx = writer.write_event(String::from("hello").into_bytes()).await;
        receivers.push(rx);
        i += 1;
    }
    assert_eq!(receivers.len(), count);

    for rx in receivers {
        let reply: Result<(), SegmentWriterError> = rx.await.expect("wait for result from oneshot");
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
            let scoped_stream = ScopedStream {
                scope: Scope::from("testScopeWriter".to_owned()),
                stream: Stream::from("testStreamWriter".to_owned()),
            };
            let sealed_segments = [Segment::from(4_294_967_297), Segment::from(4_294_967_298)];
            let new_range = [(0.0, 1.0)];
            let scale_result = factory
                .get_controller_client()
                .scale_stream(&scoped_stream, &sealed_segments, &new_range)
                .await;
            assert!(scale_result.is_ok());
            let current_segments_result = factory
                .get_controller_client()
                .get_current_segments(&scoped_stream)
                .await;
            assert_eq!(1, current_segments_result.unwrap().key_segment_map.len());
        }
        let rx = writer.write_event(String::from("hello").into_bytes()).await;
        receivers.push(rx);
        i += 1;
    }
    assert_eq!(receivers.len(), count);

    for rx in receivers {
        let reply: Result<(), SegmentWriterError> = rx.await.expect("wait for result from oneshot");
        assert_eq!(reply.is_ok(), true);
    }
    info!("test event stream writer with segment sealed passed");
}

async fn test_write_correctness(writer: &mut EventStreamWriter, factory: &ClientFactory) {
    info!("test read and write");
    let rx = writer.write_event(String::from("event0").into_bytes()).await;
    let reply: Result<(), SegmentWriterError> = rx.await.expect("wait for result from oneshot");
    assert_eq!(reply.is_ok(), true);
    let scope_name = Scope::from("testScopeWriter2".to_owned());
    let stream_name = Stream::from("testStreamWriter2".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment::from(0),
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
    let count: i32 = 1000;
    let mut i: i32 = 1;
    let scope_name = Scope::from("testScopeWriter2".to_owned());
    let stream_name = Stream::from("testStreamWriter2".to_owned());

    let mut receivers = vec![];
    info!("writing to two segments");
    while i < count {
        if i == count / 2 {
            // scaling up the segment number
            let scoped_stream = ScopedStream {
                scope: scope_name.clone(),
                stream: stream_name.clone(),
            };
            let sealed_segments = [Segment::from(0)];

            let new_range = [(0.0, 0.5), (0.5, 1.0)];

            let scale_result = factory
                .get_controller_client()
                .scale_stream(&scoped_stream, &sealed_segments, &new_range)
                .await;
            assert!(scale_result.is_ok());
            let current_segments_result = factory
                .get_controller_client()
                .get_current_segments(&scoped_stream)
                .await;
            assert_eq!(2, current_segments_result.unwrap().key_segment_map.len());
        }

        if i % 2 == 0 {
            let data = format!("event{}", i);
            // Routing key "even" and "odd" works is because their hashed value are in
            // 0~0.5 and 0.5~1.0 respectively. If the routing key hashing algorithm changed, this
            // test might fail.
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
    // the data should write successfully.
    for rx in receivers {
        let reply: Result<(), SegmentWriterError> = rx.await.expect("wait for result from oneshot");
        assert_eq!(reply.is_ok(), true);
    }

    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(0),
    };

    let async_segment_reader = factory.create_async_event_reader(segment_name.clone()).await;
    let mut i: i32 = 0;
    let mut offset = 0;
    info!("reading from first segment");
    // the first half go to the first segment.
    while i < count / 2 {
        let expect_string = format!("event{}", i);
        let length = (expect_string.len() + 8) as i32;
        let reply = async_segment_reader
            .read(offset, length)
            .await
            .expect("read event from segment");
        let data = EventCommand::read_from(&reply.data).expect("deserialize data");
        assert_eq!(expect_string, String::from_utf8(data.data).unwrap());
        i += 1;
        offset += length as i64;
    }

    let segment_name1 = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(4_294_967_297),
    };
    let reader1 = factory.create_async_event_reader(segment_name1.clone()).await;
    let segment_name2 = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(4_294_967_298),
    };
    let reader2 = factory.create_async_event_reader(segment_name2.clone()).await;

    let mut offset1 = 0;
    let mut offset2 = 0;
    info!("reading from scaled segments");
    while i < count {
        let expect_string = format!("event{}", i);
        let length = (expect_string.len() + 8) as i32;
        if i % 2 != 0 {
            let reply = reader1
                .read(offset1, length)
                .await
                .expect("read event from segment");
            let data = EventCommand::read_from(&reply.data).expect("deserialize data");
            assert_eq!(expect_string, String::from_utf8(data.data).unwrap());
            offset1 += length as i64;
        } else {
            let reply = reader2
                .read(offset2, length)
                .await
                .expect("read event from segment");
            let data = EventCommand::read_from(&reply.data).expect("deserialize data");
            assert_eq!(expect_string, String::from_utf8(data.data).unwrap());
            offset2 += length as i64;
        }
        i += 1;
    }

    info!("test event stream writer writes without loss or duplicate when scaling up passed");
}

async fn test_write_correctness_with_routing_key(writer: &mut EventStreamWriter, factory: &ClientFactory) {
    info!("test event stream writer writes to a stream with routing key");
    let count: i32 = 1000;
    let mut i: i32 = 0;
    let scope_name = Scope::from("testScopeWriter3".to_owned());
    let stream_name = Stream::from("testStreamWriter3".to_owned());
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
        segment: Segment::from(0),
    };

    let second_segment = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(1),
    };
    let reader1 = factory.create_async_event_reader(first_segment.clone()).await;
    let reader2 = factory.create_async_event_reader(second_segment.clone()).await;

    let mut i: i32 = 0;
    let mut offset1: i64 = 0;
    let mut offset2: i64 = 0;
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

async fn test_write_with_stream_sealed(writer: &mut EventStreamWriter, factory: &ClientFactory) {
    info!("test event stream writer with stream sealed");

    let mut receivers = vec![];
    let count = 1000;
    let mut i = 0;
    while i < count {
        if i == 500 {
            let scoped_stream = ScopedStream {
                scope: Scope::from("testScopeWriterStreamSealed".to_owned()),
                stream: Stream::from("testStreamWriterStreamSealed".to_owned()),
            };
            let seal_result = factory.get_controller_client().seal_stream(&scoped_stream).await;
            assert!(seal_result.is_ok());
        }
        let rx = writer.write_event(String::from("hello").into_bytes()).await;
        receivers.push(rx);
        i += 1;
    }
    assert_eq!(receivers.len(), count);

    for (i, rx) in receivers.into_iter().enumerate() {
        let reply: Result<(), SegmentWriterError> = rx.await.expect("wait for result from oneshot");
        if i < 500 {
            assert!(reply.is_ok());
        } else {
            assert!(reply.is_err());
        }
    }

    info!("test event stream writer with stream sealed passed");
}
