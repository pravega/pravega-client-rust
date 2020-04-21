//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use log::info;
use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::tablemap::{TableError, TableMap};
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_wire_protocol::client_config::{ClientConfig, ClientConfigBuilder, TEST_CONTROLLER_URI};
use pravega_wire_protocol::connection_factory::{ConnectionFactory, SegmentConnectionManager};

use pravega_wire_protocol::commands::TableKey;
pub async fn test_tablemap() {
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
        Ok(_v) => panic!("Bad version error expected"),
        Err(TableError::BadKeyVersion { .. }) => (), // this is expected
        _ => panic!("Invalid Error message"),
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

    // verify delete with a non-existent key
    let key: String = "non-existent-key".into();
    let r = map.remove_conditional(key, TableKey::KEY_NO_VERSION).await;
    assert!(r.is_ok());

    // verify conditional delete
    let key: String = k.clone();
    let r = map.remove_conditional(key, 0i64).await;
    assert!(r.is_err());
    match r {
        Ok(_v) => panic!("Bad version error expected"),
        Err(TableError::BadKeyVersion { .. }) => (), // this is expected
        _ => panic!("Invalid Error message"),
    }

    //verify unconditional delete
    let key: String = k.clone();
    let r = map.remove(key).await;
    assert!(r.is_ok());

    // verify with get.
    let rr: Result<Option<(String, i64)>, TableError> = map.get(k.clone()).await;
    assert!(rr.is_ok());
    assert!(rr.unwrap().is_none());

    // verify conditional delete post delete.
    let key: String = k.clone();
    let r = map.remove_conditional(key, 0i64).await;
    assert!(r.is_err());
    match r {
        Ok(_v) => panic!("Key does not exist error expected"),
        Err(TableError::KeyDoesNotExist { .. }) => (), // this is expected
        _ => panic!("Invalid Error message"),
    }
}
