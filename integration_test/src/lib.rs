//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

#![allow(dead_code)]
#![allow(unused_imports)]

mod byte_stream_tests;
mod controller_tests;
#[cfg(test)]
mod disconnection_tests;
mod event_stream_writer_tests;
mod pravega_service;
mod tablemap_tests;
mod tablesynchronizer_tests;
mod transactional_event_stream_writer_tests;
mod utils;
mod wirecommand_tests;

use crate::pravega_service::{PravegaService, PravegaStandaloneService};
use lazy_static::*;
use std::process::Command;
use std::{thread, time};
use tracing::{debug, error, info, span, warn, Level};

#[macro_use]
extern crate derive_new;

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

#[cfg(test)]
mod test {
    use super::*;
    use crate::pravega_service::PravegaStandaloneServiceConfig;
    use pravega_client_rust::trace;
    use wirecommand_tests::*;

    #[test]
    fn integration_test() {
        trace::init();
        let span = span!(Level::DEBUG, "integration test");
        let _enter = span.enter();
        let config = PravegaStandaloneServiceConfig::new(false, false, false);
        run_tests(config);

        // test again with auth enabled
        let config = PravegaStandaloneServiceConfig::new(false, true, true);
        run_tests(config);

        // disconnection test will start its own Pravega Standalone.
        disconnection_tests::disconnection_test_wrapper();
    }

    fn run_tests(config: PravegaStandaloneServiceConfig) {
        let mut pravega = PravegaStandaloneService::start(config.clone());
        wait_for_standalone_with_timeout(true, 30);
        controller_tests::test_controller_apis(config.clone());

        tablemap_tests::test_tablemap(config.clone());

        event_stream_writer_tests::test_event_stream_writer(config.clone());

        tablesynchronizer_tests::test_tablesynchronizer(config.clone());

        transactional_event_stream_writer_tests::test_transactional_event_stream_writer(config.clone());

        byte_stream_tests::test_byte_stream(config.clone());

        // Shut down Pravega standalone
        pravega.stop().unwrap();
        wait_for_standalone_with_timeout(false, 30);
    }
}
