//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::client_factory::ClientFactoryInternal;
use pravega_controller_client::ControllerClient;
use pravega_rust_client_shared::{
    Retention, RetentionType, ScaleType, Scaling, Scope, ScopedSegment, ScopedStream, Segment, Stream,
    StreamConfiguration,
};
use pravega_wire_protocol::client_config::{ClientConfigBuilder, TEST_CONTROLLER_URI};
use tracing::{debug, error, info, warn};

pub fn test_event_stream_reader() {
    let config = ClientConfigBuilder::default()
        .controller_uri(TEST_CONTROLLER_URI)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config);
    let handle = client_factory.get_runtime_handle();
    handle.block_on(test_read_api(&client_factory))
}

//Test reading out data from a stream.
async fn test_read_api(client_factory: &ClientFactory) {
    let scope_name = Scope::from("testReaderScope".to_owned());
    let stream_name = Stream::from("testReaderStream".to_owned());

    const NUM_EVENTS: usize = 100;
    // write 100 events.
    write_event(
        scope_name.clone(),
        stream_name.clone(),
        &client_factory,
        NUM_EVENTS,
    )
    .await;
    let str = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };

    let mut reader = client_factory.create_event_stream_reader(str).await;
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
async fn write_event(
    scope_name: Scope,
    stream_name: Stream,
    client_factory: &ClientFactory,
    num_events: usize,
) {
    let controller_client = client_factory.get_controller_client();
    let new_stream = create_scope_stream(controller_client, &scope_name, &stream_name, 4).await;
    // write events only if this stream is not present.
    if new_stream {
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
