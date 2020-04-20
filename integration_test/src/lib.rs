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

#[cfg(test)]
mod disconnection_tests;
mod event_stream_writer_tests;
mod pravega_service;
mod wirecommand_tests;

use crate::pravega_service::{PravegaService, PravegaStandaloneService};
use pravega_client_rust::setup_logger;
use std::process::Command;
use std::{thread, time};

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
    use lazy_static::*;
    use log::info;
    use pravega_client_rust::client_factory::ClientFactory;
    use pravega_client_rust::tablemap::{TableError, TableMap};
    use pravega_connection_pool::connection_pool::ConnectionPool;
    use pravega_controller_client::{ControllerClient, ControllerClientImpl};
    use pravega_wire_protocol::client_config::{ClientConfig, ClientConfigBuilder, TEST_CONTROLLER_URI};
    use pravega_wire_protocol::connection_factory::{ConnectionFactory, SegmentConnectionManager};

    use pravega_wire_protocol::commands::TableKey;
    use wirecommand_tests::*;

    #[test]
    fn integration_test() {
        let mut rt = tokio::runtime::Runtime::new().expect("create runtime");

        setup_logger().expect("setup logger");
        rt.block_on(test_tablemap());
        // let mut pravega = PravegaStandaloneService::start(false);
        // wait_for_standalone_with_timeout(true, 30);
        // thread::sleep(time::Duration::from_secs(20));

        // rt.block_on(wirecommand_tests::wirecommand_test_wrapper());
        //
        // rt.block_on(event_stream_writer_tests::test_event_stream_writer());
        //
        // // Shut down Pravega standalone
        // pravega.stop().unwrap();
        // wait_for_standalone_with_timeout(false, 30);
        //
        // // disconnection test will start its own Pravega Standalone.
        // rt.block_on(disconnection_tests::disconnection_test_wrapper());
    }

    async fn test_tablemap() {
        let config = ClientConfigBuilder::default()
            .controller_uri(TEST_CONTROLLER_URI)
            .build()
            .expect("creating config");

        let client_factory = ClientFactory::new(config.clone());
        let map = client_factory.create_table_map("t1".into()).await;

        let k: String = "key".into();
        let v: String = "valu".into();
        let r = map.insert(k.clone(), v).await;
        info!("==> PUT {:?}", r);
        let r: Result<Option<(String, i64)>, TableError> = map.get(k.clone()).await;
        info!("==> GET {:?}", r);

        // versioning test
        let k: String = "k".into();
        let v: String = "v_0".into();
        let rr: Result<Option<(String, i64)>, TableError> = map.get(k.clone()).await;
        assert!(rr.is_ok() && rr.unwrap().is_none());

        let r = map
            .insert_conditional(k.clone(), TableKey::NOT_EXISTS, v.clone())
            .await;
        assert!(r.is_ok());
        let version = r.unwrap();
        let rr: Result<Option<(String, i64)>, TableError> = map.get(k.clone()).await;
        assert!(rr.is_ok());
        let temp = rr.unwrap();
        assert!(temp.is_some());
        assert_eq!(temp.unwrap().0, v);

        // second update with not exists
        let r = map
            .insert_conditional(k.clone(), TableKey::NOT_EXISTS, "v_1".to_string())
            .await;
        assert!(r.is_err());
        match r {
            Ok(_v) => assert!(false, "Bad version error expected"),
            Err(TableError::BadKeyVersion { error_msg: _ }) => assert!(true),
            _ => assert!(false, "Invalid Error message"),
        }

        // update with the write version.
        let r = map
            .insert_conditional(k.clone(), version, "v_1".to_string()) // specify the correct key version
            .await;
        assert!(r.is_ok());

        // verify if the write was successful.
        let rr: Result<Option<(String, i64)>, TableError> = map.get(k.clone()).await;
        assert!(rr.is_ok());
        let temp = rr.unwrap();
        assert!(temp.is_some());
        assert_eq!(temp.unwrap().0, "v_1".to_string());

        // insert unconditional
        let r = map
            .insert_conditional(k.clone(), TableKey::KEY_NO_VERSION, "v_100".to_string()) // specify the correct key version
            .await;
        assert!(r.is_ok());

        // verify if the write was successful.
        let rr: Result<Option<(String, i64)>, TableError> = map.get(k.clone()).await;
        assert!(rr.is_ok());
        let temp = rr.unwrap();
        assert!(temp.is_some());
        assert_eq!(temp.unwrap().0, "v_100".to_string());
    }
}
