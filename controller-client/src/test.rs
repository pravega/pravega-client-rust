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

use super::*;
use std::net::SocketAddr;
use tokio::runtime::Runtime;
#[test]
fn test_create_scope_error() {
    let mut rt = Runtime::new().unwrap();
    let config = ClientConfigBuilder::default()
        .controller_uri("127.0.0.1:9090".parse::<SocketAddr>().unwrap())
        .build()
        .expect("build client config");

    let client = ControllerClientImpl::new(config, rt.handle().clone());

    let request = Scope::new("testScope124".into());
    let create_scope_result = rt.block_on(client.create_scope(&request));
    assert!(create_scope_result.is_err());
    match create_scope_result {
        Ok(_) => assert!(false, "Failure excepted"),
        Err(ControllerError::ConnectionError {
            can_retry,
            error_msg: _,
        }) => assert_eq!(true, can_retry),
        _ => assert!(false, "Invalid Error"),
    };
}

#[test]
fn test_create_stream_error() {
    let mut rt = Runtime::new().unwrap();
    let config = ClientConfigBuilder::default()
        .controller_uri("127.0.0.1:9090".parse::<SocketAddr>().unwrap())
        .build()
        .expect("build client config");
    let client = ControllerClientImpl::new(config, rt.handle().clone());

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
    let create_stream_result = rt.block_on(client.create_stream(&request));
    assert!(create_stream_result.is_err());
    match create_stream_result {
        Ok(_) => assert!(false, "Failure excepted"),
        Err(ControllerError::ConnectionError {
            can_retry,
            error_msg: _,
        }) => assert_eq!(true, can_retry),
        _ => assert!(false, "Invalid Error"),
    };
}
