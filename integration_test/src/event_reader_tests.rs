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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tracing::{debug, error, info, warn};

pub fn test_event_stream_reader(config: PravegaStandaloneServiceConfig) {
    info!("test event stream reader");
    let config = ClientConfigBuilder::default()
        .controller_uri(MOCK_CONTROLLER_URI)
        .is_auth_enabled(config.auth)
        .is_tls_enabled(config.tls)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config);
    let async_client_factory = client_factory.to_async();
    let runtime = client_factory.runtime();
    test_read_from_tail_of_stream(&async_client_factory);
    test_read_from_head_of_stream(&async_client_factory);
    test_read_large_events(&async_client_factory);
    test_multi_reader_multi_segments_tail_read(&async_client_factory);
    runtime.block_on(test_read_api(&async_client_factory));
    runtime.block_on(test_stream_scaling(&async_client_factory));
    runtime.block_on(test_release_segment(&async_client_factory));
    runtime.block_on(test_release_segment_at(&async_client_factory));
    test_multiple_readers(&async_client_factory);
    test_reader_offline(&async_client_factory);
    test_segment_rebalance(&async_client_factory);
    info!("test event stream reader finished");
}

fn test_read_large_events(client_factory: &ClientFactoryAsync) {
    let h = client_factory.runtime_handle();
    let scope_name = Scope::from("testReaderScaling".to_owned());
    let stream_name = Stream::from("testReadLargeEvents".to_owned());

    const NUM_EVENTS: usize = 1000;
    const EVENT_SIZE: usize = 1000;

    let new_stream = h.block_on(create_scope_stream(
        client_factory.controller_client(),
        &scope_name,
        &stream_name,
        1,
    ));
    // write events only if the stream is created. This is useful if we are running the reader tests
    // multiple times.
    if new_stream {
        h.block_on(write_events(
            scope_name.clone(),
            stream_name.clone(),
            client_factory.clone(),
            NUM_EVENTS,
            EVENT_SIZE,
        ))
    }
    let stream = ScopedStream {
        scope: scope_name,
        stream: stream_name,
    };

    let rg: ReaderGroup =
        h.block_on(client_factory.create_reader_group("rg-large-event".to_string(), stream));
    let mut reader = h.block_on(rg.create_reader("r1".to_string()));

    let mut event_count = 0;
    while event_count < NUM_EVENTS {
        if let Some(mut slice) = h.block_on(reader.acquire_segment()) {
            for event in &mut slice {
                assert_eq!(
                    vec![1; EVENT_SIZE],
                    event.value.as_slice(),
                    "Corrupted event read"
                );
                event_count += 1;
                info!("read count {}", event_count);
            }
            h.block_on(reader.release_segment(slice));
        }
    }
    assert_eq!(event_count, NUM_EVENTS);
}

fn test_multi_reader_multi_segments_tail_read(client_factory: &ClientFactoryAsync) {
    let h = client_factory.runtime_handle();
    let scope_name = Scope::from("testMultiReaderMultiSegmentsTailRead".to_owned());
    let stream_name = Stream::from("testMultiReaderMultiSegmentsTailRead".to_owned());

    const NUM_EVENTS: usize = 2000;
    const EVENT_SIZE: usize = 1024;

    let new_stream = h.block_on(create_scope_stream(
        client_factory.controller_client(),
        &scope_name,
        &stream_name,
        2,
    ));
    // write events only if the stream is created. This is useful if we are running the reader tests
    // multiple times.
    let scope_name_clone = scope_name.clone();
    let stream_name_clone = stream_name.clone();
    let factory = client_factory.clone();
    if new_stream {
        h.spawn(async {
            write_events(
                scope_name_clone,
                stream_name_clone,
                factory,
                NUM_EVENTS,
                EVENT_SIZE,
            )
            .await
        });
    }
    let stream = ScopedStream {
        scope: scope_name,
        stream: stream_name,
    };

    let rg: ReaderGroup =
        h.block_on(client_factory.create_reader_group("rg-single-reader-multi-segments".to_string(), stream));
    let mut reader1 = h.block_on(rg.create_reader("r1".to_string()));
    let mut reader2 = h.block_on(rg.create_reader("r2".to_string()));
    let read_count = Arc::new(AtomicUsize::new(0));
    let read_count1 = read_count.clone();
    let read_count2 = read_count.clone();
    let handle1 = h.spawn(async move {
        while read_count1.load(Ordering::Relaxed) < NUM_EVENTS {
            if let Some(mut slice) = reader1.acquire_segment().await {
                info!("acquire segment for reader r1, {:?}", slice);
                for event in &mut slice {
                    assert_eq!(
                        vec![1; EVENT_SIZE],
                        event.value.as_slice(),
                        "Corrupted event read"
                    );
                    let prev = read_count1.fetch_add(1, Ordering::SeqCst);
                    info!("read count {}", prev + 1);
                }
                reader1.release_segment(slice).await;
            }
        }
    });
    let handle2 = h.spawn(async move {
        while read_count2.load(Ordering::Relaxed) < NUM_EVENTS {
            if let Some(mut slice) = reader2.acquire_segment().await {
                info!("acquire segment for reader r2 {:?}", slice);
                for event in &mut slice {
                    assert_eq!(
                        vec![1; EVENT_SIZE],
                        event.value.as_slice(),
                        "Corrupted event read"
                    );
                    let prev = read_count2.fetch_add(1, Ordering::SeqCst);
                    info!("read count {}", prev + 1);
                }
                reader2.release_segment(slice).await;
            }
        }
    });
    h.block_on(handle1).expect("wait for reader1");
    h.block_on(handle2).expect("wait for reader2");
    assert_eq!(read_count.load(Ordering::Relaxed), NUM_EVENTS);
}

async fn test_release_segment(client_factory: &ClientFactoryAsync) {
    let scope_name = Scope::from("testReaderScaling".to_owned());
    let stream_name = Stream::from("testReaderRelease1".to_owned());

    const NUM_EVENTS: usize = 10;
    const EVENT_SIZE: usize = 10;

    let new_stream =
        create_scope_stream(client_factory.controller_client(), &scope_name, &stream_name, 1).await;
    // write events only if the stream is created. This is useful if we are running the reader tests
    // multiple times.
    if new_stream {
        write_events_before_and_after_scale(
            scope_name.clone(),
            stream_name.clone(),
            client_factory,
            NUM_EVENTS,
            EVENT_SIZE,
        )
        .await;
    }
    let stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };

    let rg: ReaderGroup = client_factory
        .create_reader_group("rg-release".to_string(), stream)
        .await;
    let mut reader = rg.create_reader("r1".to_string()).await;

    let mut event_count = 0;
    let mut release_invoked = false;
    while event_count < NUM_EVENTS {
        if let Some(mut slice) = reader.acquire_segment().await {
            loop {
                if !release_invoked && event_count == 5 {
                    reader.release_segment(slice).await;
                    release_invoked = true;
                    break;
                } else if let Some(event) = slice.next() {
                    assert_eq!(
                        vec![1; EVENT_SIZE],
                        event.value.as_slice(),
                        "Corrupted event read"
                    );
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

async fn test_release_segment_at(client_factory: &ClientFactoryAsync) {
    let scope_name = Scope::from("testReaderScaling".to_owned());
    let stream_name = Stream::from("testReaderReleaseat".to_owned());

    const NUM_EVENTS: usize = 10;
    const EVENT_SIZE: usize = 10;

    let new_stream =
        create_scope_stream(client_factory.controller_client(), &scope_name, &stream_name, 1).await;
    // write events only if the stream is created.
    if new_stream {
        write_events_before_and_after_scale(
            scope_name.clone(),
            stream_name.clone(),
            client_factory,
            NUM_EVENTS,
            EVENT_SIZE,
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
                    reader.release_segment_at(slice, 0).await; // release segment @ the beginning, so that the reader reads all the data.
                    release_invoked = true;
                    break;
                } else if let Some(event) = slice.next() {
                    assert_eq!(
                        vec![1; EVENT_SIZE],
                        event.value.as_slice(),
                        "Corrupted event read"
                    );
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

async fn test_stream_scaling(client_factory: &ClientFactoryAsync) {
    let scope_name = Scope::from("testScope".to_owned());
    let stream_name = Stream::from("testReaderStream".to_owned());

    const NUM_EVENTS: usize = 10;
    const EVENT_SIZE: usize = 10;

    let new_stream =
        create_scope_stream(client_factory.controller_client(), &scope_name, &stream_name, 1).await;
    // write events only if the stream is created.
    if new_stream {
        write_events_before_and_after_scale(
            scope_name.clone(),
            stream_name.clone(),
            client_factory,
            NUM_EVENTS,
            EVENT_SIZE,
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
                    assert_eq!(
                        vec![1; EVENT_SIZE],
                        event.value.as_slice(),
                        "Corrupted event read"
                    );
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
    info!("test event stream reader scaling passed");
}

//Test reading out data from a stream.
async fn test_read_api(client_factory: &ClientFactoryAsync) {
    info!("test event stream reader read api");
    let scope_name = Scope::from("testReaderScope".to_owned());
    let stream_name = Stream::from("testReaderStream".to_owned());

    const NUM_EVENTS: usize = 10;
    const EVNET_SIZE: usize = 10;

    let new_stream =
        create_scope_stream(client_factory.controller_client(), &scope_name, &stream_name, 4).await;
    // write events only if the stream is not created.
    if new_stream {
        // write 100 events.
        write_events(
            scope_name.clone(),
            stream_name.clone(),
            client_factory.clone(),
            NUM_EVENTS,
            EVNET_SIZE,
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
                assert_eq!(
                    vec![1; EVNET_SIZE],
                    event.value.as_slice(),
                    "Corrupted event read"
                );
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
    info!("test event stream reader read api passed");
}

fn test_multiple_readers(client_factory: &ClientFactoryAsync) {
    let h = client_factory.runtime_handle();
    let scope_name = Scope::from("testScope".to_owned());
    let stream_name = Stream::from("testMultiReader".to_owned());
    let str = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    const NUM_EVENTS: usize = 50;
    const EVENT_SIZE: usize = 10;

    h.block_on(async {
        let new_stream =
            create_scope_stream(client_factory.controller_client(), &scope_name, &stream_name, 4).await;
        // write events only if the stream is created.
        if new_stream {
            // write events
            write_events(
                scope_name.clone(),
                stream_name.clone(),
                client_factory.clone(),
                NUM_EVENTS,
                EVENT_SIZE,
            )
            .await;
        }
    });

    let rg = h.block_on(client_factory.create_reader_group("rg_multi_reader".to_string(), str));
    // reader 1 will be assigned all the segments.
    let mut reader1 = h.block_on(rg.create_reader("r1".to_string()));
    // no segments will be assigned to reader2
    let mut reader2 = h.block_on(rg.create_reader("r2".to_string()));

    if let Some(mut slice) = h.block_on(reader1.acquire_segment()) {
        if let Some(event) = slice.next() {
            assert_eq!(
                vec![1; EVENT_SIZE],
                event.value.as_slice(),
                "Corrupted event read"
            );
            // wait for release timeout.
            thread::sleep(Duration::from_secs(20));
            h.block_on(reader1.release_segment(slice));
        } else {
            panic!("A valid slice is expected");
        }
    }

    if let Some(mut slice) = h.block_on(reader2.acquire_segment()) {
        if let Some(event) = slice.next() {
            assert_eq!(
                vec![1; EVENT_SIZE],
                event.value.as_slice(),
                "Corrupted event read"
            );
            h.block_on(reader2.release_segment(slice));
        } else {
            panic!("A valid slice is expected for reader2");
        }
    }
}

fn test_segment_rebalance(client_factory: &ClientFactoryAsync) {
    let h = client_factory.runtime_handle();
    let scope_name = Scope::from("testScope".to_owned());
    let stream_name = Stream::from("testsegrebalance".to_owned());
    let str = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    const NUM_EVENTS: usize = 50;
    const EVENT_SIZE: usize = 10;

    h.block_on(async {
        let new_stream =
            create_scope_stream(client_factory.controller_client(), &scope_name, &stream_name, 4).await;
        // write events only if the stream is created.
        if new_stream {
            // write events with random routing keys.
            write_events(
                scope_name.clone(),
                stream_name.clone(),
                client_factory.clone(),
                NUM_EVENTS,
                EVENT_SIZE,
            )
            .await;
        }
    });

    let rg = h.block_on(client_factory.create_reader_group("rg_reblance_reader".to_string(), str));
    // reader 1 will be assigned all the segments.
    let mut reader1 = h.block_on(rg.create_reader("r1".to_string()));
    // no segments will be assigned to reader2 until a rebalance
    let mut reader2 = h.block_on(rg.create_reader("r2".to_string()));

    // change the last seg acquire and release time to ensure segment balance is triggered.
    let last_acquire_release_time = Instant::now() - Duration::from_secs(20);
    reader1.set_last_acquire_release_time(last_acquire_release_time);
    reader2.set_last_acquire_release_time(last_acquire_release_time);
    let mut events_read = 0;
    if let Some(mut slice) = h.block_on(reader1.acquire_segment()) {
        if let Some(event) = slice.next() {
            assert_eq!(
                vec![1; EVENT_SIZE],
                event.value.as_slice(),
                "Corrupted event read"
            );
            events_read += 1;
            // this should trigger a release.
            h.block_on(reader1.release_segment(slice));
        } else {
            panic!("A valid slice is expected");
        }
    }

    // try acquiring a segment on reader 2 and verify segments are acquired.
    if let Some(mut slice) = h.block_on(reader2.acquire_segment()) {
        if let Some(event) = slice.next() {
            // validate that reader 2 acquired a segment.
            assert_eq!(
                vec![1; EVENT_SIZE],
                event.value.as_slice(),
                "Corrupted event read"
            );
            events_read += 1;
            h.block_on(reader2.release_segment(slice));
        } else {
            panic!("A valid slice is expected for reader2");
        }
    }
    // set reader 2 offline. This should ensure all the segments assigned to reader2 are available to be assigned by reader1.else {  }
    h.block_on(reader2.reader_offline());
    //reset the time to ensure reader1 acquires segment in the next cycle.
    reader1.set_last_acquire_release_time(Instant::now() - Duration::from_secs(20));

    while let Some(slice) = h.block_on(reader1.acquire_segment()) {
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

    println!("{}", events_read);
}

fn test_reader_offline(client_factory: &ClientFactoryAsync) {
    let h = client_factory.runtime_handle();
    let scope_name = Scope::from("testScope".to_owned());
    let stream_name = Stream::from("testReaderOffline".to_owned());
    let str = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    const NUM_EVENTS: usize = 10;
    const EVENT_SIZE: usize = 10;

    h.block_on(async {
        let new_stream =
            create_scope_stream(client_factory.controller_client(), &scope_name, &stream_name, 4).await;
        // write events only if the stream is created.
        if new_stream {
            // write events
            write_events(
                scope_name.clone(),
                stream_name.clone(),
                client_factory.clone(),
                NUM_EVENTS,
                EVENT_SIZE,
            )
            .await;
        }
    });

    let rg = h.block_on(client_factory.create_reader_group("rg_reader_offline".to_string(), str));
    // reader 1 will be assigned all the segments.
    let mut reader1 = h.block_on(rg.create_reader("r1".to_string()));

    // read one event using reader1 and release it back.
    // A drop of segment slice does the same .
    if let Some(mut slice) = h.block_on(reader1.acquire_segment()) {
        if let Some(event) = slice.next() {
            assert_eq!(
                vec![1; EVENT_SIZE],
                event.value.as_slice(),
                "Corrupted event read"
            );
            // wait for release timeout.
            thread::sleep(Duration::from_secs(10));
            h.block_on(reader1.release_segment(slice));
        } else {
            panic!("A valid slice is expected");
        }
    }
    // reader offline.
    h.block_on(reader1.reader_offline());

    let mut reader2 = h.block_on(rg.create_reader("r2".to_string()));

    let mut events_read = 1; // one event has been already read by reader 1.
    while let Some(slice) = h.block_on(reader2.acquire_segment()) {
        // read from a Segment slice.
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

fn test_read_from_head_of_stream(client_factory: &ClientFactoryAsync) {
    let h = client_factory.runtime_handle();
    let scope_name = Scope::from("testReadHeadScopeRG".to_owned());
    let stream_name = Stream::from("testHeadRG".to_owned());
    let str = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    const NUM_EVENTS: usize = 10;
    const EVENT_SIZE: usize = 10;

    h.block_on(async {
        let new_stream =
            create_scope_stream(client_factory.controller_client(), &scope_name, &stream_name, 4).await;
        // write events only if the stream is created.
        if new_stream {
            // write events
            write_events(
                scope_name.clone(),
                stream_name.clone(),
                client_factory.clone(),
                NUM_EVENTS,
                EVENT_SIZE,
            )
            .await;
        }
    });

    let rg_config = ReaderGroupConfigBuilder::default()
        .read_from_head_of_stream(str)
        .build();
    let rg = h.block_on(client_factory.create_reader_group_with_config(
        scope_name,
        "rg_reader_offline".to_string(),
        rg_config,
    ));

    let mut reader1 = h.block_on(rg.create_reader("r1".to_string()));

    let mut events_read = 0;
    while let Some(slice) = h.block_on(reader1.acquire_segment()) {
        // read from a Segment slice.
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

fn test_read_from_tail_of_stream(client_factory: &ClientFactoryAsync) {
    let h = client_factory.runtime_handle();
    let scope_name = Scope::from("testReadTailScopeRG".to_owned());
    let stream_name = Stream::from("testTailRG".to_owned());
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
        .read_from_tail_of_stream(str)
        .build();
    let rg = h.block_on(client_factory.create_reader_group_with_config(
        scope_name.clone(),
        "rg_reader_offline".to_string(),
        rg_config,
    ));

    let mut reader1 = h.block_on(rg.create_reader("r1".to_string()));

    let mut events_read = 0;
    assert!(
        h.block_on(reader1.acquire_segment()).is_none(),
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
    while let Some(slice) = h.block_on(reader1.acquire_segment()) {
        // read from a Segment slice.
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

// helper function to write events into a stream before and after a stream scale operation.
async fn write_events_before_and_after_scale(
    scope_name: Scope,
    stream_name: Stream,
    client_factory: &ClientFactoryAsync,
    num_events: usize,
    event_size: usize,
) {
    write_events(
        scope_name.clone(),
        stream_name.clone(),
        client_factory.clone(),
        num_events,
        event_size,
    )
    .await;
    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    let new_range = [(0.0, 0.5), (0.5, 1.0)];
    let to_seal_segments: Vec<Segment> = vec![Segment::from(0)];
    let controller = client_factory.controller_client();
    let scale_result = controller
        .scale_stream(&scoped_stream, to_seal_segments.as_slice(), &new_range)
        .await;
    assert!(scale_result.is_ok(), "Stream scaling should complete");
    let current_segments_result = controller.get_current_segments(&scoped_stream).await;
    assert_eq!(2, current_segments_result.unwrap().key_segment_map.len());
    // write events post stream scaling.
    write_events(
        scope_name,
        stream_name,
        client_factory.clone(),
        num_events,
        event_size,
    )
    .await;
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
