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
use pravega_client_rust::client_factory::ClientFactory;
use pravega_controller_client::ControllerClient;
use pravega_rust_client_config::{ClientConfigBuilder, MOCK_CONTROLLER_URI};
use pravega_rust_client_shared::{
    Retention, RetentionType, ScaleType, Scaling, Scope, ScopedSegment, ScopedStream, Segment, Stream,
    StreamConfiguration,
};
use tracing::{debug, error, info, warn};

pub fn test_event_stream_reader(config: PravegaStandaloneServiceConfig) {
    let config = ClientConfigBuilder::default()
        .controller_uri(MOCK_CONTROLLER_URI)
        .is_auth_enabled(config.auth)
        .is_tls_enabled(config.tls)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config);
    let handle = client_factory.get_runtime_handle();
    handle.block_on(test_read_api(&client_factory));
    handle.block_on(test_stream_scaling(&client_factory));
    handle.block_on(test_release_segment(&client_factory));
    handle.block_on(test_release_segment_at(&client_factory));
}

async fn test_release_segment(client_factory: &ClientFactory) {
    let scope_name = Scope::from("testReaderScaling".to_owned());
    let stream_name = Stream::from("testReaderRelease1".to_owned());

    const NUM_EVENTS: usize = 10;
    // write 100 events.

    let new_stream = create_scope_stream(
        client_factory.get_controller_client(),
        &scope_name,
        &stream_name,
        1,
    )
    .await;
    // write events only if the stream is created.
    if new_stream {
        write_events_before_and_after_scale(
            scope_name.clone(),
            stream_name.clone(),
            &client_factory,
            NUM_EVENTS,
        )
        .await;
    }
    let str = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };

    let rg = client_factory
        .create_reader_group("rg-read-api".to_string(), str)
        .await;
    let mut reader = rg.create_reader("r1".to_string()).await;

    let mut event_count = 0;
    let mut release_invoked = false;
    loop {
        if event_count == NUM_EVENTS {
            // all events have been read. Exit test.
            break;
        }
        if let Some(mut slice) = reader.acquire_segment().await {
            loop {
                if !release_invoked && event_count == 5 {
                    reader.release_segment(slice);
                    release_invoked = true;
                    break;
                } else if let Some(event) = slice.next() {
                    assert_eq!(b"aaa", event.value.as_slice(), "Corrupted event read");
                    event_count += 1;
                } else {
                    info!(
                        "Finished reading from segment {:?}, segment is auto released",
                        slice.meta.scoped_segment
                    );
                    break; // try to acquire the next segment.
                }
            }
        }
    }
    assert_eq!(event_count, NUM_EVENTS);
}

async fn test_release_segment_at(client_factory: &ClientFactory) {
    let scope_name = Scope::from("testReaderScaling".to_owned());
    let stream_name = Stream::from("testReaderReleaseat".to_owned());

    const NUM_EVENTS: usize = 10;
    // write 100 events.

    let new_stream = create_scope_stream(
        client_factory.get_controller_client(),
        &scope_name,
        &stream_name,
        1,
    )
    .await;
    // write events only if the stream is created.
    if new_stream {
        write_events_before_and_after_scale(
            scope_name.clone(),
            stream_name.clone(),
            &client_factory,
            NUM_EVENTS,
        )
        .await;
    }
    let str = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };

    let rg = client_factory
        .create_reader_group("rg-release-segment".to_string(), str)
        .await;
    let mut reader = rg.create_reader("r1".to_string()).await;
    let mut event_count = 0;
    let mut release_invoked = false;
    loop {
        if event_count == NUM_EVENTS + NUM_EVENTS + 5 {
            // all events have been read. Exit test.
            break;
        }
        if let Some(mut slice) = reader.acquire_segment().await {
            loop {
                if !release_invoked && event_count == 5 {
                    reader.release_segment_at(slice, 0); // release segment @ the beginning, so that the reader reads all the data.
                    release_invoked = true;
                    break;
                } else if let Some(event) = slice.next() {
                    assert_eq!(b"aaa", event.value.as_slice(), "Corrupted event read");
                    event_count += 1;
                } else {
                    info!(
                        "Finished reading from segment {:?}, segment is auto released",
                        slice.meta.scoped_segment
                    );
                    break; // try to acquire the next segment.
                }
            }
        }
    }
    assert_eq!(event_count, NUM_EVENTS + NUM_EVENTS + 5); // 5 additional events.
}

async fn test_stream_scaling(client_factory: &ClientFactory) {
    let scope_name = Scope::from("testReaderScaling".to_owned());
    let stream_name = Stream::from("testReaderStream".to_owned());

    const NUM_EVENTS: usize = 10;
    // write 100 events.

    let new_stream = create_scope_stream(
        client_factory.get_controller_client(),
        &scope_name,
        &stream_name,
        1,
    )
    .await;
    // write events only if the stream is created.
    if new_stream {
        write_events_before_and_after_scale(
            scope_name.clone(),
            stream_name.clone(),
            &client_factory,
            NUM_EVENTS,
        )
        .await;
    }
    let str = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };

    let rg = client_factory
        .create_reader_group("rg_stream_scaling".to_string(), str)
        .await;
    let mut reader = rg.create_reader("r1".to_string()).await;
    let mut event_count = 0;
    loop {
        if event_count == NUM_EVENTS + NUM_EVENTS {
            // all events have been read. Exit test.
            break;
        }
        if let Some(mut slice) = reader.acquire_segment().await {
            loop {
                if let Some(event) = slice.next() {
                    assert_eq!(b"aaa", event.value.as_slice(), "Corrupted event read");
                    event_count += 1;
                } else {
                    info!(
                        "Finished reading from segment {:?}, segment is auto released",
                        slice.meta.scoped_segment
                    );
                    break; // try to acquire the next segment.
                }
            }
        }
    }
    assert_eq!(event_count, NUM_EVENTS + NUM_EVENTS);
}

//Test reading out data from a stream.
async fn test_read_api(client_factory: &ClientFactory) {
    let scope_name = Scope::from("testReaderScope".to_owned());
    let stream_name = Stream::from("testReaderStream".to_owned());

    const NUM_EVENTS: usize = 10;

    let new_stream = create_scope_stream(
        client_factory.get_controller_client(),
        &scope_name,
        &stream_name,
        4,
    )
    .await;
    // write events only if the stream is not created.
    if new_stream {
        // write 100 events.
        write_events(
            scope_name.clone(),
            stream_name.clone(),
            &client_factory,
            NUM_EVENTS,
        )
        .await;
    }
    let str = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    let rg = client_factory
        .create_reader_group("rg-read-api".to_string(), str)
        .await;
    let mut reader = rg.create_reader("r1".to_string()).await;
    let mut event_count = 0;
    while let Some(mut slice) = reader.acquire_segment().await {
        loop {
            if let Some(event) = slice.next() {
                assert_eq!(b"aaa", event.value.as_slice(), "Corrupted event read");
                event_count += 1;
            } else {
                println!(
                    "Finished reading from segment {:?}, segment is auto released",
                    slice.meta.scoped_segment
                );
                break; // try to acquire the next segment.
            }
        }
        if event_count == NUM_EVENTS {
            // all events have been read. Exit test.
            break;
        }
    }
}

// helper method to write events to Pravega
async fn write_events(
    scope_name: Scope,
    stream_name: Stream,
    client_factory: &ClientFactory,
    num_events: usize,
) {
    let scoped_stream = ScopedStream {
        scope: scope_name,
        stream: stream_name,
    };
    let mut writer = client_factory.create_event_stream_writer(scoped_stream);

    for _x in 0..num_events {
        let rx = writer.write_event(String::from("aaa").into_bytes()).await;
        rx.await.expect("Failed to write Event").unwrap();
    }
}

// helper function to write events into a stream before and after a stream scale operation.
async fn write_events_before_and_after_scale(
    scope_name: Scope,
    stream_name: Stream,
    client_factory: &ClientFactory,
    num_events: usize,
) {
    write_events(
        scope_name.clone(),
        stream_name.clone(),
        client_factory,
        num_events,
    )
    .await;
    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    let new_range = [(0.0, 0.5), (0.5, 1.0)];
    let to_seal_segments: Vec<Segment> = vec![Segment::from(0)];
    let controller = client_factory.get_controller_client();
    let scale_result = controller
        .scale_stream(&scoped_stream, to_seal_segments.as_slice(), &new_range)
        .await;
    assert!(scale_result.is_ok(), "Stream scaling should complete");
    let current_segments_result = controller.get_current_segments(&scoped_stream).await;
    assert_eq!(2, current_segments_result.unwrap().key_segment_map.len());
    // write events post stream scaling.
    write_events(scope_name, stream_name, client_factory, num_events).await;
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
    };
    controller_client
        .create_stream(&request)
        .await
        .expect("create stream")
}
