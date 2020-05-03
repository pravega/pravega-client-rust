//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use futures::pin_mut;
use futures::stream::Stream;
use futures::stream::StreamExt;
use log::info;
use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::tablemap::{TableError, TableMap};
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_wire_protocol::client_config::{ClientConfig, ClientConfigBuilder, TEST_CONTROLLER_URI};
use pravega_wire_protocol::connection_factory::{ConnectionFactory, SegmentConnectionManager};

use futures::async_await::assert_fused_future;
use pravega_wire_protocol::commands::TableKey;

pub async fn test_tablemap() {
    let config = ClientConfigBuilder::default()
        .controller_uri(TEST_CONTROLLER_URI)
        .build()
        .expect("creating config");

    let client_factory = ClientFactory::new(config.clone());
    test_single_key_operations(&client_factory).await;
    test_multiple_key_operations(&client_factory).await;
    test_iterators(&client_factory).await;
}

async fn test_single_key_operations(client_factory: &ClientFactory) {
    let map = client_factory.create_table_map("t1".into()).await;
    let k: String = "key".into();
    let v: String = "val".into();
    let r = map.insert(&k, &v).await;
    info!("==> PUT {:?}", r);
    let r: Result<Option<(String, i64)>, TableError> = map.get(&k).await;
    info!("==> GET {:?}", r);

    // versioning test
    let k: String = "k".into();
    let v: String = "v_0".into();
    let v_1: String = "v_1".to_string();
    let v_100: String = "v_100".to_string();

    let rr: Result<Option<(String, i64)>, TableError> = map.get(&k).await;
    assert!(rr.is_ok() && rr.unwrap().is_none());
    let r = map.insert_conditionally(&k, &v, TableKey::NOT_EXISTS).await;
    assert!(r.is_ok());
    let version = r.unwrap();
    let rr: Result<Option<(String, i64)>, TableError> = map.get(&k).await;
    assert!(rr.is_ok());
    let temp = rr.unwrap();
    assert!(temp.is_some());
    assert_eq!(temp.unwrap().0, v);

    // second update with not exists
    let r = map.insert_conditionally(&k, &v_1, TableKey::NOT_EXISTS).await;
    assert!(r.is_err());
    match r {
        Ok(_v) => panic!("Bad version error expected"),
        Err(TableError::IncorrectKeyVersion { .. }) => (), // this is expected
        _ => panic!("Invalid Error message"),
    }
    // update with the write version.
    let r = map
        .insert_conditionally(&k,  &v_1, version) // specify the correct key version
        .await;
    assert!(r.is_ok());
    // verify if the write was successful.
    let rr: Result<Option<(String, i64)>, TableError> = map.get(&k).await;
    assert!(rr.is_ok());
    let temp = rr.unwrap();
    assert!(temp.is_some());
    assert_eq!(temp.unwrap().0, "v_1".to_string());

    // insert unconditional
    let r = map
        .insert_conditionally(&k, &v_100, TableKey::KEY_NO_VERSION) // specify the correct key version
        .await;
    assert!(r.is_ok());
    // verify if the write was successful.
    let rr: Result<Option<(String, i64)>, TableError> = map.get(&k).await;
    assert!(rr.is_ok());
    let temp = rr.unwrap();
    assert!(temp.is_some());
    assert_eq!(temp.unwrap().0, v_100);

    // verify delete with a non-existent key
    let key: String = "non-existent-key".into();
    let r = map.remove_conditionally(&key, TableKey::KEY_NO_VERSION).await;
    assert!(r.is_ok());

    // verify conditional delete
    let key: String = k.clone();
    let r = map.remove_conditionally(&key, 0i64).await;
    assert!(r.is_err());
    match r {
        Ok(_v) => panic!("Bad version error expected"),
        Err(TableError::IncorrectKeyVersion { .. }) => (), // this is expected
        _ => panic!("Invalid Error message"),
    }
    //verify unconditional delete
    let key: String = k.clone();
    let r = map.remove(&key).await;
    assert!(r.is_ok());
    // verify with get.
    let rr: Result<Option<(String, i64)>, TableError> = map.get(&k).await;
    assert!(rr.is_ok());
    assert!(rr.unwrap().is_none());
    // verify conditional delete post delete.
    let key: String = k.clone();
    let r = map.remove_conditionally(&key, 0i64).await;
    assert!(r.is_err());
    match r {
        Ok(_v) => panic!("Key does not exist error expected"),
        Err(TableError::KeyDoesNotExist { .. }) => (), // this is expected
        _ => panic!("Invalid Error message"),
    }
}

async fn test_multiple_key_operations(client_factory: &ClientFactory) {
    let map = client_factory.create_table_map("t2".into()).await;
    let k1: String = "k1".into();
    let k2: String = "k2".into();
    let k3: String = "k3".into();
    let v1: String = "v".into();
    let v2: String = "v".into();

    let data = vec![(&k1, &v1), (&k2, &v2)];
    let versions = map.insert_all(data).await;
    let r: Result<Option<(String, i64)>, TableError> = map.get(&k1).await;
    info!("==> GET {:?}", r);
    assert!(r.is_ok());
    let temp = r.unwrap();
    assert!(temp.is_some());
    assert_eq!(temp.unwrap().0, "v".to_string());

    let r: Result<Option<(String, i64)>, TableError> = map.get(&k2).await;
    info!("==> GET {:?}", r);
    assert!(r.is_ok());
    let temp = r.unwrap();
    assert!(temp.is_some());
    assert_eq!(temp.unwrap().0, "v".to_string());

    // conditional insert
    let v1 = "v1".to_string();
    let data = vec![
        (&k1, &v1, versions.unwrap()[1]), // incorrect version
        (&k3, &v1, TableKey::NOT_EXISTS),
    ];
    let versions = map.insert_conditionally_all(data).await;
    info!("==> Insert_all {:?}", versions);
    assert!(versions.is_err());
    match versions {
        Ok(_v) => panic!("Bad version error expected"),
        Err(TableError::IncorrectKeyVersion { .. }) => (), // this is expected
        _ => panic!("Invalid Error message"),
    }
    // verify no new updates have happened.
    let r: Result<Option<(String, i64)>, TableError> = map.get(&k1).await;
    info!("==> GET {:?}", r);
    assert!(r.is_ok());
    let temp = r.unwrap();
    assert!(temp.is_some());
    assert_eq!(temp.unwrap().0, "v".to_string());

    let r: Result<Option<(String, i64)>, TableError> = map.get(&k3).await;
    info!("==> GET {:?}", r);
    assert!(r.is_ok());
    let temp = r.unwrap();
    assert!(temp.is_none());

    let keys = vec![&k1, &k2];
    let r: Result<Vec<Option<(String, i64)>>, TableError> = map.get_all(keys).await;
    info!("==> GET ALL {:?}", r);
    assert!(r.is_ok());
    let test = r.unwrap();
    for x in test {
        assert!(x.is_some());
        let (data, _) = x.unwrap();
        assert_eq!(data, "v".to_string());
    }

    let r: Result<Vec<Option<(String, i64)>>, TableError> = map.get_all(vec![&k1, &k3]).await;
    info!("==> GET ALL {:?}", r);
    assert!(r.is_ok());
    let result = r.unwrap();
    assert!(result[0].is_some());
    assert!(result[1].is_none()); // key k3 is not present.
}

async fn test_iterators(client_factory: &ClientFactory) {
    let map = client_factory.create_table_map("stream_test".into()).await;
    let k1: String = "k1".into();
    let k2: String = "k2".into();
    let k3: String = "k3".into();
    let k4: String = "k4".into();
    let k5: String = "k5".into();
    let k6: String = "k6".into();
    let v: String = "val".into();
    // insert data.
    let r = map
        .insert_all(vec![
            (&k1, &v),
            (&k2, &v),
            (&k3, &v),
            (&k4, &v),
            (&k5, &v),
            (&k6, &v),
        ])
        .await;
    assert!(r.is_ok());

    let key_stream = map.read_keys_raw_stream(2);
    pin_mut!(key_stream);

    let mut key_count: i32 = 0;
    while let Some(value) = key_stream.next().await {
        match value {
            Ok(t) => {
                let k: Vec<u8> = t.0;
                assert_eq!(false, k.is_empty());
                key_count += 1;
            }
            Err(_e) => panic!("Failed fetch keys."),
        }
    }
    assert_eq!(6, key_count);

    let key_set: Result<(Vec<(String, i64)>, Vec<u8>), TableError> = map.get_keys(6, &[]).await;
    assert!(key_set.is_ok());
    assert_eq!(6, key_set.unwrap().0.len());

    let entry_stream = map.read_entries_raw_stream(2);
    pin_mut!(entry_stream);

    let mut entry_count: i32 = 0;
    while let Some(value) = entry_stream.next().await {
        match value {
            Ok((k, v, _ver)) => {
                assert_eq!(false, k.is_empty());
                assert_eq!(false, v.is_empty());
                entry_count += 1;
            }
            Err(_e) => panic!("Failed fetch entries."),
        }
    }
    assert_eq!(6, entry_count);

    let entry_set: Result<(Vec<(String, String, i64)>, Vec<u8>), TableError> = map.get_entries(6, &[]).await;
    assert!(entry_set.is_ok());
    assert_eq!(6, entry_set.unwrap().0.len());
}
