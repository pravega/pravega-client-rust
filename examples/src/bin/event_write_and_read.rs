/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

use pravega_client::client_factory::ClientFactory;
use pravega_client_config::ClientConfigBuilder;
use pravega_client_shared::{
    Retention, RetentionType, ScaleType, Scaling, Scope, ScopedStream, Stream, StreamConfiguration,
};
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    env_logger::init();
    println!("start event write and read example");
    // assuming Pravega standalone is listening at localhost:9090
    let config = ClientConfigBuilder::default()
        .controller_uri("localhost:9090")
        .build()
        .unwrap();

    let client_factory = ClientFactory::new(config);
    println!("client factory created");

    client_factory.runtime().block_on(async {
        let controller_client = client_factory.controller_client();

        // create a scope
        let scope = Scope::from("fooScope".to_owned());
        controller_client
            .create_scope(&scope)
            .await
            .expect("create scope");
        println!("scope created");

        // create a stream containing only one segment
        let stream = Stream::from("barStream".to_owned());
        let stream_config = StreamConfiguration {
            scoped_stream: ScopedStream {
                scope: scope.clone(),
                stream: stream.clone(),
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
            tags: None,
        };
        controller_client
            .create_stream(&stream_config)
            .await
            .expect("create stream");
        println!("stream created");

        // create event stream writer
        let stream = ScopedStream::from("fooScope/barStream");
        let mut event_writer = client_factory.create_event_writer(stream.clone());
        println!("event writer created");

        // write payload
        let payload = "hello world".to_string().into_bytes();
        let result = event_writer.write_event(payload).await;
        assert!(result.await.is_ok());
        println!("event writer sent and flushed data");

        // write large payload
        let payload = vec![54; 9437184];
        let result = event_writer.write_event(payload).await;
        assert!(result.await.is_ok());
        println!("event writer sent and flushed large data");

        // create event stream reader
        let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let rg = format!("rg_{}", n.as_secs());
        let rg = client_factory.create_reader_group(rg, stream).await;
        let mut reader = rg.create_reader("r1".to_string()).await;
        println!("event reader created");

        // read from segment
        if let Some(mut slice) = reader
            .acquire_segment()
            .await
            .expect("Failed to acquire segment since the reader is offline")
        {
            let read_event = slice.next();
            assert!(read_event.is_some(), "event slice should have event to read");
            assert_eq!(b"hello world", read_event.unwrap().value.as_slice());
            println!("event reader read data");

            loop {
                let read_event = slice.next();
                if read_event.is_some() {
                    assert_eq!(read_event.unwrap().value.as_slice().len(), 9437184);
                    println!("event reader read large data");
                    break;
                } else {
                    reader.release_segment(slice).await.unwrap();
                    if let Some(new_slice) = reader
                        .acquire_segment()
                        .await
                        .expect("Failed to acquire segment since the reader is offline")
                    {
                        slice = new_slice;
                    } else {
                        println!("no data to read from the Pravega stream");
                        assert!(false, "read should return the written event.");
                        break;
                    }
                }
            }
        } else {
            println!("no data to read from the Pravega stream");
            assert!(false, "read should return the written event.")
        }

        reader
            .reader_offline()
            .await
            .expect("failed to mark the reader offline");
        println!("event write and read example finished");

    });
}
