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

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error + 'static>> {
    // start Pravega standalone before invoking this function.
    let client = create_connection("http://[::1]:9090").await;
    let mut controller_client = ControllerClientImpl { channel: client };

    let request1 = Scope {
        name: "testScope123".into(),
    };
    let scope_result = controller_client.create_scope(request1).await;
    println!("Response for create_scope is {:?}", scope_result);

    let request2 = StreamConfiguration {
        scoped_stream: ScopedStream {
            scope: Scope {
                name: "testScope123".into(),
            },
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
        scope: Scope {
            name: "testScope123".into(),
        },
        stream: Stream {
            name: "testStream".into(),
        },
        segment: Segment { number: 0 },
    };

    let result_final = controller_client.get_endpoint_for_segment(request3).await;
    println!("Response for get_endpoint_for_segment is {:?}", result_final);

    Ok(())
}
