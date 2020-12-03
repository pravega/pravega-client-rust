/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

use pravega_client_rust::client_factory::ClientFactory;
use pravega_rust_client_config::ClientConfigBuilder;
use pravega_rust_client_shared::{
    Retention, RetentionType, ScaleType, Scaling, Scope, ScopedStream, Stream, StreamConfiguration,
};

#[tokio::main]
async fn main() {
    // Assuming Pravega standalone is listening at localhost:9090
    let config = ClientConfigBuilder::default()
        .controller_uri("localhost:9090")
        .build()
        .unwrap();

    let client_factory = ClientFactory::new(config);
    let controller_client = client_factory.get_controller_client();

    // create a scope
    let scope = Scope::from("my_scope".to_owned());
    controller_client.create_scope(&scope).await.unwrap();

    // create a stream containing only one segment
    let stream = Stream::from("my_stream".to_owned());
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
    };
    controller_client.create_stream(&stream_config).await.unwrap();

    // create event stream writer
    let stream = ScopedStream::from("my_scope/my_stream");
    let mut event_stream_writer = client_factory.create_event_stream_writer(stream.clone());

    // write payload
    let payload = "hello world".to_string().into_bytes();
    let result = event_stream_writer.write_event(payload).await;
    assert!(result.await.is_ok());

    // create event stream reader
    let rg = client_factory.create_reader_group("rg".to_string(), stream).await;
    let mut reader = rg.create_reader("r1".to_string()).await;

    // read from segment
    let mut slice = reader.acquire_segment().await.unwrap();
    let read_event = slice.next().unwrap();
    assert_eq!(b"hello world", read_event.value.as_slice());
}
