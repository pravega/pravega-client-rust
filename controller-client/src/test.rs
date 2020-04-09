/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
use pravega_wire_protocol::client_config::{ClientConfigBuilder, TEST_CONTROLLER_URI};
use tokio::runtime::Runtime;

// Note this useful idiom: importing names from outer (for mod tests) scope.
use super::*;

#[test]
#[should_panic] // since the controller is not running.
fn test_create_scope_error() {
    let mut rt = Runtime::new().unwrap();
    let config = ClientConfigBuilder::default()
        .build()
        .expect("build client config");
    let manager = ControllerConnectionManager::new(config);
    let pool = ConnectionPool::new(manager);
    let connection = rt
        .block_on(pool.get_connection(TEST_CONTROLLER_URI.into()))
        .expect("get connection");

    let request = Scope::new("testScope124".into());
    let fut = create_scope(&request, connection);

    rt.block_on(fut).unwrap();
}

#[test]
#[should_panic] // since the controller is not running.
fn test_create_stream_error() {
    let mut rt = Runtime::new().unwrap();

    let config = ClientConfigBuilder::default()
        .build()
        .expect("build client config");
    let manager = ControllerConnectionManager::new(config);
    let pool = ConnectionPool::new(manager);
    let connection = rt
        .block_on(pool.get_connection(TEST_CONTROLLER_URI.into()))
        .expect("get connection");

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
    let fut = create_stream(&request, connection);

    rt.block_on(fut).unwrap();
}
