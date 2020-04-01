//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::pravega_service::{PravegaService, PravegaStandaloneService};
use pravega_client_rust::setup_logger;
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use std::process::Command;
use std::{thread, time};

#[allow(clippy::all)]
mod event_stream_writer_tests;
#[allow(clippy::all)]
mod pravega_service;
#[allow(clippy::all)]
mod wirecommand_tests;

fn wait_for_standalone_with_timeout(expected_status: bool, timeout_second: i32) {
    for _i in 0..timeout_second {
        if expected_status == check_standalone_status() {
            return;
        }
        thread::sleep(time::Duration::from_secs(1));
    }
    panic!(
        "timeout {} exceeded, Pravega standalone is in status {} while expected {}",
        timeout_second, !expected_status, expected_status
    );
}

fn check_standalone_status() -> bool {
    let output = Command::new("sh")
        .arg("-c")
        .arg("netstat -ltn 2> /dev/null | grep 9090 || ss -ltn 2> /dev/null | grep 9090")
        .output()
        .expect("failed to execute process");
    // if length is not zero, controller is listening on port 9090
    !output.stdout.is_empty()
}

#[allow(clippy::all)]
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[allow]
    fn integration_test() {
        let mut rt = tokio::runtime::Runtime::new().expect("create runtime");

        setup_logger().expect("setup logger");
        let mut pravega = PravegaStandaloneService::start(false);
        wait_for_standalone_with_timeout(true, 30);

        wirecommand_tests::test_wirecommand();
        rt.block_on(event_stream_writer_tests::test_event_stream_writer());

        // Shut down Pravega standalone
        pravega.stop().unwrap();
        wait_for_standalone_with_timeout(false, 30);
    }
}
