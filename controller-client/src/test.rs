/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
use pravega_wire_protocol::client_config::ClientConfigBuilder;

// Note this useful idiom: importing names from outer (for mod tests) scope.
use super::*;
use std::net::SocketAddr;

#[tokio::test]
async fn test_create_scope_error() {
    let config = ClientConfigBuilder::default()
        .controller_uri("127.0.0.1:9090".parse::<SocketAddr>().unwrap())
        .build()
        .expect("build client config");

    let client = ControllerClientImpl::new(config);
    let mut ch = client.channel;

    let request = Scope::new("testScope124".into());
    let create_scope_result = create_scope(&request, &mut ch).await;
    assert!(create_scope_result.is_err());
    match create_scope_result {
        Ok(_) => assert!(false, "Failure excepted"),
        Err(ControllerError::ConnectionError {
            can_retry,
            error_msg: _,
        }) => assert!(can_retry),
        _ => assert!(false, "Invalid Error"),
    };
}

#[tokio::test]
async fn test_create_stream_error() {
    let config = ClientConfigBuilder::default()
        .controller_uri("127.0.0.1:9090".parse::<SocketAddr>().unwrap())
        .build()
        .expect("build client config");
    let client = ControllerClientImpl::new(config);

    let request = StreamConfiguration {
        scoped_stream: ScopedStream {
            scope: Scope::new("testScope123".into()),
            stream: Stream {
                name: "testStream".into(),
            },
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
    let create_stream_result = client.create_stream(&request).await;
    assert!(create_stream_result.is_err());
    match create_stream_result {
        Ok(_) => assert!(false, "Failure excepted"),
        Err(ControllerError::ConnectionError {
            can_retry,
            error_msg: _,
        }) => assert!(can_retry),
        _ => assert!(false, "Invalid Error"),
    };
}
