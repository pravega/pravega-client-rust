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
    //include!()
    // this is the rs file name generated after compiling the proto file, located inside the
    // target folder.
}

use controller::{client::ControllerServiceClient, ScopeInfo, CreateScopeStatus};

// establish_connection with the given controller uri.
fn establish_connection(
    uri: &'static str,
) -> Result<ControllerServiceClient<tonic::transport::channel::Channel>, tonic::transport::Error> {
    // retry on errors.
    ControllerServiceClient::connect(uri) // lifetime of uri outlives the
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // start Pravega standalone before invoking this function.
    //let mut client1 = ControllerServiceClient::connect("http://[::1]:9090")?;
    //    let mut client1 = establish_connection("http://[::1]:9090")?;
    //    // Hard coding the input for testing purposes.
    //    ScopeInfo {
    //        scope: "testScope123".into(),
    //    };
    //
    //    let request = tonic::Request::new(ScopeInfo {
    //        scope: "testScope123".into(),
    //    });
    //
    //    let response = client1.create_scope(request).await?;
    //
    //    println!("RESPONSE={:?}", response);
    //

    let client = establish_connection("http://[::1]:9090");
    let mut c1 = client.unwrap();
    let request = tonic::Request::new(ScopeInfo {
        scope: "testScope123".into(),
    });

    let response_future = c1.create_scope(request).await?;
    println!("RESPONSE={:?}", response_future);
    Ok(())
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use tokio_test::{
        assert_pending, assert_ready, assert_ready_ok, async_await, block_on, clock, task::MockTask,
    };

    #[test]
    fn test_create_scope() {
        let client = establish_connection("http://[::1]:9090");
        let mut c1 = client.unwrap();
        let request = tonic::Request::new(ScopeInfo {
            scope: "testScope124".into(),
        });
        let response_future = c1.create_scope(request);
        let resp = block_on(response_future);
        println!("RESPONSE={:?}", resp);
        // assert the value returned.
    }
}
