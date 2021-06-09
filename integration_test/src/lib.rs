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
#![allow(bare_trait_objects)]

use std::{thread, time};
// use pravega_client_rust::metric;
use std::process::Command;

use lazy_static::*;
use tracing::{error, info, info_span, warn};

use crate::pravega_service::{PravegaService, PravegaStandaloneService};
use tracing_subscriber::FmtSubscriber;

mod byte_reader_writer_tests;
mod controller_tests;
#[cfg(test)]
mod disconnection_tests;
mod event_reader_tests;
mod event_writer_tests;
mod pravega_service;
mod synchronizer_tests;
mod table_tests;
mod transactional_event_writer_tests;
mod utils;
mod wirecommand_tests;

#[macro_use]
extern crate derive_new;

const PROMETHEUS_SCRAPE_PORT: &str = "127.0.0.1:8081";

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
    use std::env;
    use std::net::SocketAddr;

    use wirecommand_tests::*;

    use crate::pravega_service::PravegaStandaloneServiceConfig;

    use super::*;
    use tracing::{dispatcher, Dispatch, Level};

    #[test]
    fn integration_test() {
        let subscriber = FmtSubscriber::builder()
            .with_ansi(true)
            .with_max_level(Level::DEBUG)
            .finish();

        let my_dispatch = Dispatch::new(subscriber);
        // this function can only be called once.
        dispatcher::set_global_default(my_dispatch)
            .map_err(|e| {
                format!(
                    "failed to set tracing level globally, probably you tried to set it multiple times: {:?}",
                    e
                )
            })
            .unwrap();

        // metric::metric_init(PROMETHEUS_SCRAPE_PORT.parse::<SocketAddr>().unwrap());
        info!("Running integration test");
        let config = PravegaStandaloneServiceConfig::new(false, true, true);
        run_tests(config);

        let config = PravegaStandaloneServiceConfig::new(true, false, false);
        run_tests(config);

        // disconnection test will start its own Pravega Standalone.
        disconnection_tests::disconnection_test_wrapper();
    }

    fn run_tests(config: PravegaStandaloneServiceConfig) {
        let mut pravega = PravegaStandaloneService::start(config.clone());
        wait_for_standalone_with_timeout(true, 30);
        if config.auth {
            env::set_var("pravega_client_auth_method", "Basic");
            env::set_var("pravega_client_auth_username", "admin");
            env::set_var("pravega_client_auth_password", "1111_aaaa");
        }
        let span = info_span!("controller test", auth = config.auth, tls = config.tls);
        span.in_scope(|| {
            info!("Running controller test");
            controller_tests::test_controller_apis(config.clone());
        });
        let span = info_span!("table map test", auth = config.auth, tls = config.tls);
        span.in_scope(|| {
            info!("Running table map test");
            table_tests::test_table(config.clone());
        });
        let span = info_span!("event stream writer test", auth = config.auth, tls = config.tls);
        span.in_scope(|| {
            info!("Running event stream writer test");
            event_writer_tests::test_event_stream_writer(config.clone());
        });
        let span = info_span!("synchronizer test", auth = config.auth, tls = config.tls);
        span.in_scope(|| {
            info!("Running synchronizer test");
            synchronizer_tests::test_tablesynchronizer(config.clone());
        });
        let span = info_span!("transaction test", auth = config.auth, tls = config.tls);
        span.in_scope(|| {
            info!("Running transaction test");
            transactional_event_writer_tests::test_transactional_event_stream_writer(config.clone());
        });
        let span = info_span!("byte stream test", auth = config.auth, tls = config.tls);
        span.in_scope(|| {
            info!("Running byte stream test");
            byte_reader_writer_tests::test_byte_stream(config.clone());
        });
        let span = info_span!("event reader test", auth = config.auth, tls = config.tls);
        span.in_scope(|| {
            info!("Running event reader test");
            event_reader_tests::test_event_stream_reader(config.clone());
        });
        // Shut down Pravega standalone
        pravega.stop().unwrap();
        wait_for_standalone_with_timeout(false, 30);
    }
}
