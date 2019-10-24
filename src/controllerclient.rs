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

use controller::{client::ControllerServiceClient, CreateScopeStatus, ScopeInfo};
use tonic::transport::channel::Channel;

/// create_connection with the given controller uri.
fn create_connection(uri: &'static str) -> ControllerServiceClient<Channel> {
    let connection: ControllerServiceClient<Channel> =
        ControllerServiceClient::connect(uri).expect("Failed to create a channel");
    connection
}

/// Async function to create scope
async fn create_scope(
    request: tonic::Request<ScopeInfo>,
    ch: &mut ControllerServiceClient<Channel>,
) -> Result<tonic::Response<CreateScopeStatus>, tonic::Status> {
    ch.create_scope(request).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    // start Pravega standalone before invoking this function.
    let mut client = create_connection("http://[::1]:9090");
    let request = tonic::Request::new(ScopeInfo {
        scope: "testScope123".into(),
    });
    let response = create_scope(request, &mut client).await?;
    println!("Response for create_scope is {:?}", response);
    Ok(())
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_create_scope_error() {
        let rt = Runtime::new().unwrap();

        let mut client = create_connection("http://[::1]:9090");

        let request = tonic::Request::new(ScopeInfo {
            scope: "testScope124".into(),
        });
        let fut = create_scope(request, &mut client);

        let r: Result<tonic::Response<controller::CreateScopeStatus>, tonic::Status> =
            rt.block_on(fut);

        assert!(
            r.is_err(),
            "connection should fail since controller is not running"
        );
    }
}
