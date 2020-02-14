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

    let scope_name = Scope::new("testScope123".into());
    let stream_name = Stream::new("testStream".into());

    let scope_result = controller_client.create_scope(&scope_name).await;
    println!("Response for create_scope is {:?}", scope_result);

    let stream_cfg = StreamConfiguration {
        scoped_stream: ScopedStream {
            scope: scope_name.clone(),
            stream: stream_name.clone(),
        },
        scaling: Scaling {
            scale_type: ScaleType::FixedNumSegments,
            target_rate: 0,
            scale_factor: 0,
            min_num_segments: 2,
        },
        retention: Retention {
            retention_type: RetentionType::None,
            retention_param: 0,
        },
    };

    let stream_result = controller_client.create_stream(&stream_cfg).await;
    println!("Response for create_stream is {:?}", stream_result);

    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };

    let endpoint_result = controller_client.get_endpoint_for_segment(&segment_name).await;
    println!("Response for get_endpoint_for_segment is {:?}", endpoint_result);

    let scoped_stream = ScopedStream::new(
        Scope::new("testScope123".into()),
        Stream::new("testStream".into()),
    );

    let current_segments_result = controller_client.get_current_segments(&scoped_stream).await;
    println!(
        "Response for get_current_segments is {:?}",
        current_segments_result
    );

    let stream_config_modified = StreamConfiguration {
        scoped_stream: ScopedStream {
            scope: scope_name.clone(),
            stream: stream_name.clone(),
        },
        scaling: Scaling {
            scale_type: ScaleType::FixedNumSegments,
            target_rate: 0,
            scale_factor: 0,
            min_num_segments: 1,
        },
        retention: Retention {
            retention_type: RetentionType::Size,
            retention_param: 100_000,
        },
    };
    let result_update_config = controller_client.update_stream(&stream_config_modified).await;
    println!("Response for update_stream is {:?}", result_update_config);

    let result_truncate = controller_client
        .truncate_stream(&pravega_rust_client_shared::StreamCut::new(
            scoped_stream.clone(),
            HashMap::new(),
        ))
        .await;
    println!("Response for truncate stream is {:?}", result_truncate);

    let seal_result = controller_client.seal_stream(&scoped_stream).await;
    println!("Response for seal stream is {:?}", seal_result);

    let delete_result = controller_client.delete_stream(&scoped_stream).await;
    println!("Response for delete stream is {:?}", delete_result);

    let scope_name_1 = Scope::new("testScope456".into());
    let scope_result = controller_client.create_scope(&scope_name_1).await;
    println!("Response for create_scope is {:?}", scope_result);

    let delete_scope_result = controller_client.delete_scope(&scope_name_1).await;
    println!("Response for delete scope is {:?}", delete_scope_result);
    Ok(())
}
