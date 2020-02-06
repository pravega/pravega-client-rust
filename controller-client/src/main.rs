/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
use pravega_controller_client::*;
use pravega_rust_client_shared::*;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error + 'static>> {
    // start Pravega standalone before invoking this function.
    let client = create_connection("http://[::1]:9090").await;
    let mut controller_client = ControllerClientImpl { channel: client };

    let request1 = Scope::new("testScope123".into());

    let scope_result = controller_client.create_scope(request1).await;
    println!("Response for create_scope is {:?}", scope_result);

    let request2 = StreamConfiguration {
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

    let stream_result = controller_client.create_stream(request2).await;
    println!("Response for create_stream is {:?}", stream_result);

    let request3 = ScopedSegment {
        scope: Scope::new("testScope123".into()),
        stream: Stream {
            name: "testStream".into(),
        },
        segment: Segment { number: 0 },
    };

    let result_final = controller_client.get_endpoint_for_segment(request3).await;
    println!("Response for get_endpoint_for_segment is {:?}", result_final);

    let req_new_config = StreamConfiguration {
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
            retention_type: RetentionType::Size,
            retention_param: 100000,
        },
    };
    let result_update_config = controller_client.update_stream(req_new_config).await;
    println!("Response for update_stream is {:?}", result_update_config);

    let request5 = ScopedStream::new(
        Scope::new("testScope123".into()),
        Stream::new("testStream".into()),
    );

    let result_truncate = controller_client
        .truncate_stream(pravega_rust_client_shared::StreamCut::new(
            request5,
            HashMap::new(),
        ))
        .await;
    println!("Response for truncate stream is {:?}", result_truncate);

    let request5 = ScopedStream::new(
        Scope::new("testScope123".into()),
        Stream::new("testStream".into()),
    );
    let seal_result = controller_client.seal_stream(request5).await;
    println!("Response for seal stream is {:?}", seal_result);

    let request6 = ScopedStream::new(
        Scope::new("testScope123".into()),
        Stream::new("testStream".into()),
    );
    let delete_result = controller_client.delete_stream(request6).await;
    println!("Response for delete stream is {:?}", delete_result);

    let request4 = Scope::new("testScope456".into());
    let scope_result = controller_client.create_scope(request4).await;
    println!("Response for create_scope is {:?}", scope_result);

    let delete_scope_result = controller_client
        .delete_scope(Scope::new("testScope456".into()))
        .await;
    println!("Response for delete scope is {:?}", delete_scope_result);
    Ok(())
}
