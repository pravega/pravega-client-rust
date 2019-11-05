/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
pub mod controller {
    #![allow(non_camel_case_types)]
    tonic::include_proto!("io.pravega.controller.stream.api.grpc.v1");
    // this is the rs file name generated after compiling the proto file, located inside the target folder.
}

use controller::{
    client::ControllerServiceClient, /*scaling_policy::ScalingPolicyType,*/ CreateScopeStatus,
    CreateStreamStatus, ScalingPolicy, ScopeInfo, StreamConfig, StreamInfo,
    scaling_policy::ScalingPolicyType,
};
use tonic::transport::channel::Channel;

/// create_connection with the given controller uri.
fn create_connection(uri: &'static str) -> ControllerServiceClient<Channel> {
    // Placeholder to add authentication headers.
    let connection: ControllerServiceClient<Channel> =
        ControllerServiceClient::connect(uri).expect("Failed to create a channel");
    connection
}

/// Async function to create scope
async fn create_scope(
    request: ScopeInfo,
    ch: &mut ControllerServiceClient<Channel>,
) -> CreateScopeStatus {
    let op_status: tonic::Response<CreateScopeStatus> = ch
        .create_scope(tonic::Request::new(request))
        .await
        .expect("Failed to create Scope");
    op_status.into_inner() // return the scope status
}

async fn create_stream(
    request: StreamConfig,
    ch: &mut ControllerServiceClient<Channel>,
) -> CreateStreamStatus {
    let op_status: tonic::Response<CreateStreamStatus> = ch
        .create_stream(tonic::Request::new(request))
        .await
        .expect("Failed to create Stream");
    op_status.into_inner() // return create Stream status
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    // start Pravega standalone before invoking this function.
    let mut client = create_connection("http://[::1]:9090");
    let request = ScopeInfo {
        scope: "testScope123".into(),
    };
    let response: CreateScopeStatus = create_scope(request, &mut client).await;
    println!("Response for create_scope is {:?}", response);

    let request2 = StreamConfig {
        stream_info: Some(StreamInfo {
            scope: "testScope123".into(),
            stream: "testStream".into(),
        }),
        scaling_policy: Some(ScalingPolicy {
            scale_type: ScalingPolicyType::FixedNumSegments as i32,
            target_rate: 0,
            scale_factor: 0,
            min_num_segments: 1,
        }),
        retention_policy: None,
    };
    let response2: CreateStreamStatus = create_stream(request2, &mut client).await;
    println!("Response 2 for create_stream is {:?}", response2);

    Ok(())
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    #[should_panic] // since the controller is not running.
    fn test_create_scope_error() {
        let rt = Runtime::new().unwrap();

        let mut client = create_connection("http://[::1]:9090");

        let request = ScopeInfo {
            scope: "testScope124".into(),
        };
        let fut = create_scope(request, &mut client);

        rt.block_on(fut);
    }

    #[test]
    #[should_panic] // since the controller is not running.
    fn test_create_stream_error() {
        let rt = Runtime::new().unwrap();

        let mut client = create_connection("http://[::1]:9090");

        let request = StreamConfig {
            stream_info: Some(StreamInfo {
                scope: "testScope123".into(),
                stream: "testStream".into(),
            }),
            scaling_policy: Some(ScalingPolicy {
                scale_type: ScalingPolicyType::FixedNumSegments as i32, /* Fixed Segments*/
                target_rate: 0,
                scale_factor: 0,
                min_num_segments: 1,
            }),
            retention_policy: None,
        };
        let fut = create_stream(request, &mut client);

        rt.block_on(fut);
    }
}
