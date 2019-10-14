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
    tonic::include_proto!("io.pravega.controller.stream.api.grpc.v1");
    // this is the rs file name generated after compiling the proto file, located inside the
    // target folder.
}

use controller::{client::ControllerServiceClient, ScopeInfo};

//TODO: find a way to remove camel case errors for the auto generated
// file io.pravega.controller .stream.api.grpc.v1.rs
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // start Pravega standalone before invoking this function.
    let mut client1 = ControllerServiceClient::connect("http://[::1]:9090")?;

    // Hard coding the input for testing purposes.
    ScopeInfo {
        scope: "testScope123".into(),
    };

    let request = tonic::Request::new(ScopeInfo {
        scope: "testScope123".into(),
    });

    let response = client1.create_scope(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
