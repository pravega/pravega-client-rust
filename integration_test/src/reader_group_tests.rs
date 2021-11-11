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

use pravega_client::client_factory::{ClientFactory, ClientFactoryAsync};
use pravega_client::event::reader_group::{ReaderGroup, ReaderGroupConfigBuilder};
use pravega_client_config::{ClientConfigBuilder, MOCK_CONTROLLER_URI};
use pravega_client_shared::{
    Retention, RetentionType, ScaleType, Scaling, Scope, ScopedSegment, ScopedStream, Segment, Stream,
    StreamConfiguration,
};
use pravega_controller_client::ControllerClient;

use pravega_client::event::reader::SegmentSlice;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tracing::{debug, error, info, warn};

pub fn test_reader_group(config: PravegaStandaloneServiceConfig) {
    info!("test reader group");
    let config = ClientConfigBuilder::default()
        .controller_uri(MOCK_CONTROLLER_URI)
        .is_auth_enabled(config.auth)
        .is_tls_enabled(config.tls)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config);
    let async_client_factory = client_factory.to_async();
    test_read_offline_stream(&async_client_factory);

    info!("test reader group finished");
}

fn test_read_offline_stream(client_factory: &ClientFactoryAsync) {
    let h = client_factory.runtime_handle();
    let scope_name = Scope::from("testReaderOffline".to_owned());
    let stream_name = Stream::from("test1".to_owned());
    let str = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    const NUM_EVENTS: usize = 10;
    const EVENT_SIZE: usize = 10;

    h.block_on(async {
        let new_stream =
            create_scope_stream(client_factory.controller_client(), &scope_name, &stream_name, 1).await;
        new_stream
    });

    let rg_config = ReaderGroupConfigBuilder::default()
        .read_from_head_of_stream(str)
        .build();

    let rg = h.block_on(client_factory.create_reader_group_with_config(
        scope_name.clone(),
        "rg_offline".to_string(),
        rg_config,
    ));
    let reader_id = "r1";
    let mut reader1 = h.block_on(rg.create_reader(reader_id.to_string()));

    assert!(
        h.block_on(reader1.acquire_segment()).unwrap().is_none(),
        "No events are expected to be read"
    );

    // Write events to the stream.
    h.block_on(write_events(
        scope_name,
        stream_name,
        client_factory.clone(),
        NUM_EVENTS,
        EVENT_SIZE,
    ));
    // Verify that we are able to read events from the stream.
    let res = h
        .block_on(reader1.acquire_segment())
        .expect("Failed to acquire a segment");
    let mut events_read = 0;
    match res {
        None => {
            panic!("Expected a segment slice to be returned")
        }
        Some(slice) => {
            for event in slice {
                assert_eq!(
                    vec![1; EVENT_SIZE],
                    event.value.as_slice(),
                    "Corrupted event read"
                );
                events_read += 1;
            }
        }
    };
    // Segment slice is dropped here and it will update the RG state with the offsets.
    // Now mark the reader offline
    let offline_res = h.block_on(rg.reader_offline(reader_id.to_string(), None));
    assert!(offline_res.is_ok(), "Reader offline did not succeed");

    // Attempt reading the remaining events.
    while let Some(slice) = h
        .block_on(reader1.acquire_segment())
        .expect("Failed to acquire segment since the reader is offline")
    {
        // read all events in the slice.
        for event in slice {
            assert_eq!(
                vec![1; EVENT_SIZE],
                event.value.as_slice(),
                "Corrupted event read"
            );
            events_read += 1;
        }
        if events_read == NUM_EVENTS {
            break;
        }
    }
    assert_eq!(NUM_EVENTS, events_read);
}

// helper method to write events to Pravega
async fn write_events(
    scope_name: Scope,
    stream_name: Stream,
    client_factory: ClientFactoryAsync,
    num_events: usize,
    event_size: usize,
) {
    let scoped_stream = ScopedStream {
        scope: scope_name,
        stream: stream_name,
    };
    let mut writer = client_factory.create_event_writer(scoped_stream);
    for x in 0..num_events {
        let rx = writer.write_event(vec![1; event_size]).await;
        rx.await.expect("Failed to write Event").unwrap();
        info!("write count {}", x);
    }
}

// helper method to create scope and stream.
async fn create_scope_stream(
    controller_client: &dyn ControllerClient,
    scope_name: &Scope,
    stream_name: &Stream,
    segment_count: i32,
) -> bool {
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
            min_num_segments: segment_count,
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
        .expect("create stream failed")
}
