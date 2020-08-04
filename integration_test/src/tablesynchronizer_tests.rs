//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::table_synchronizer::{deserialize_from, Key, TableSynchronizer};
use pravega_wire_protocol::client_config::{ClientConfig, ClientConfigBuilder, TEST_CONTROLLER_URI};
use pravega_wire_protocol::commands::TableKey;
use serde::{Deserialize, Serialize};
use tracing::info;

pub fn test_tablesynchronizer() {
    let config = ClientConfigBuilder::default()
        .controller_uri(TEST_CONTROLLER_URI)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config);
    let handle = client_factory.get_runtime_handle();
    handle.block_on(test_insert(&client_factory));
    handle.block_on(test_remove(&client_factory));
    handle.block_on(test_insert_with_two_table_synchronizers(&client_factory));
    handle.block_on(test_remove_with_two_table_synchronizers(&client_factory));
    handle.block_on(test_insert_and_get_with_customize_struct(&client_factory));
    handle.block_on(test_fetching_updates_delta(&client_factory));
}

async fn test_insert(client_factory: &ClientFactory) {
    info!("test insert");
    let mut synchronizer: TableSynchronizer = client_factory
        .create_table_synchronizer("synchronizer".to_owned())
        .await;

    let mut synchronizer2: TableSynchronizer = client_factory
        .create_table_synchronizer("synchronizer".to_owned())
        .await;

    let result = synchronizer
        .insert(|table| {
            table.insert(
                "outer_key".to_owned(),
                "inner_key".to_owned(),
                "i32".to_owned(),
                Box::new(1),
            );
            Ok(None)
        })
        .await;

    assert!(result.is_ok());
    let result = synchronizer2
        .insert(|table| {
            table.insert(
                "outer_key".to_owned(),
                "inner_key".to_owned(),
                "i32".to_owned(),
                Box::new(1),
            );
            Ok(None)
        })
        .await;
    assert!(result.is_ok());

    let value_option = synchronizer.get("outer_key", "inner_key");
    assert!(value_option.is_some());

    let value_option2 = synchronizer2.get("outer_key", "inner_key");
    assert!(value_option2.is_some());

    //check if fetchUpdates work correctly.
    let mut synchronizer2: TableSynchronizer = client_factory
        .create_table_synchronizer("synchronizer".to_owned())
        .await;
    let entries_num = synchronizer2.fetch_updates().await.expect("fetch updates");
    assert_eq!(entries_num, 2);
    let value_option = synchronizer2.get("outer_key", "inner_key");
    assert!(value_option.is_some());
    info!("test insert passed");
}

async fn test_remove(client_factory: &ClientFactory) {
    info!("test remove");
    let mut synchronizer: TableSynchronizer = client_factory
        .create_table_synchronizer("synchronizer1".to_owned())
        .await;
    let result = synchronizer
        .insert(|table| {
            if table.is_empty() {
                table.insert(
                    "outer_key".to_owned(),
                    "inner_key".to_owned(),
                    "i32".to_owned(),
                    Box::new(2),
                );
            }
            Ok(None)
        })
        .await;
    assert!(result.is_ok());

    let result = synchronizer
        .remove(|table| {
            if table.get("outer_key", "inner_key").is_some() {
                table.insert_tombstone("outer_key".to_owned(), "inner_key".to_owned())?;
            }
            Ok(None)
        })
        .await;
    assert!(result.is_ok());

    let value_option = synchronizer.get("outer_key", "inner_key");
    assert!(value_option.is_none());

    //check if fetchUpdates work correctly.
    let mut synchronizer2: TableSynchronizer = client_factory
        .create_table_synchronizer("synchronizer1".to_owned())
        .await;
    let result = synchronizer2.fetch_updates().await;
    assert!(result.is_ok());
    let value_option = synchronizer2.get("outer_key", "inner_key");
    assert!(value_option.is_none());
    info!("test remove passed");
}

async fn test_insert_with_two_table_synchronizers(client_factory: &ClientFactory) {
    info!("test insert with two table synchronizers");
    let mut synchronizer: TableSynchronizer = client_factory
        .create_table_synchronizer("synchronizer".to_owned())
        .await;

    let mut synchronizer2: TableSynchronizer = client_factory
        .create_table_synchronizer("synchronizer".to_owned())
        .await;
    synchronizer.fetch_updates().await.expect("fetch updates");
    synchronizer2.fetch_updates().await.expect("fetch updates");

    let result = synchronizer
        .insert(|table| {
            if table.contains_key("outer_key", "inner_key") {
                let value = table.get("outer_key", "inner_key").expect("get value");
                let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
                if data == 1 {
                    table.insert(
                        "outer_key".to_owned(),
                        "inner_key".to_owned(),
                        "i32".to_owned(),
                        Box::new(2),
                    );
                }
            }
            Ok(None)
        })
        .await;
    assert!(result.is_ok());

    let result = synchronizer2
        .insert(|table| {
            if table.contains_key("outer_key", "inner_key") {
                let value = table.get("outer_key", "inner_key").expect("get value");
                let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
                // Incorrect, because the value is already changed to 2.
                if data == 1 {
                    table.insert(
                        "outer_key".to_owned(),
                        "inner_key".to_owned(),
                        "i32".to_owned(),
                        Box::new(4),
                    );
                }
                // Correct
                if data == 2 {
                    table.insert(
                        "outer_key".to_owned(),
                        "inner_key".to_owned(),
                        "i32".to_owned(),
                        Box::new(3),
                    );
                }
            }
            Ok(None)
        })
        .await;
    assert!(result.is_ok());

    let result = synchronizer.fetch_updates().await;
    assert!(result.is_ok());
    let value = synchronizer.get("outer_key", "inner_key").expect("get value");
    let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
    assert_eq!(data, 3);
    info!("test insert with two table synchronizers passed");
}

async fn test_remove_with_two_table_synchronizers(client_factory: &ClientFactory) {
    info!("test remove with two table synchronizers");
    let mut synchronizer: TableSynchronizer = client_factory
        .create_table_synchronizer("synchronizer".to_owned())
        .await;

    let mut synchronizer2: TableSynchronizer = client_factory
        .create_table_synchronizer("synchronizer".to_owned())
        .await;
    synchronizer.fetch_updates().await.expect("fetch updates");
    synchronizer2.fetch_updates().await.expect("fetch updates");

    let result = synchronizer
        .insert(|table| {
            let value = table.get("outer_key", "inner_key").expect("get value");
            let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
            if data == 3 {
                table.insert_tombstone("outer_key".to_owned(), "inner_key".to_owned())?;
            }
            Ok(None)
        })
        .await;
    assert!(result.is_ok());

    info!("start to update a non-existing key");
    let result = synchronizer2
        .insert(|table| {
            if !table.is_empty() {
                // Even if it matches in in_memory map.
                // This update should failed, because the key is already removed.
                table.insert(
                    "outer_key".to_owned(),
                    "inner_key".to_owned(),
                    "i32".to_owned(),
                    Box::new(4),
                );
            }
            Ok(None)
        })
        .await;

    assert!(result.is_ok());
    let value_option = synchronizer.get("outer_key", "inner_key");
    assert!(value_option.is_none());
    info!("test remove with two table synchronizers passed");
}

async fn test_insert_and_get_with_customize_struct(client_factory: &ClientFactory) {
    info!("test insert and get with customize struct");
    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct Test1 {
        name: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct Test2 {
        age: i32,
    }

    let mut synchronizer: TableSynchronizer = client_factory
        .create_table_synchronizer("synchronizer2".to_owned())
        .await;

    let result = synchronizer
        .insert(|table| {
            table.insert(
                "outer_key".to_owned(),
                "inner_key1".to_owned(),
                "Test1".to_owned(),
                Box::new(Test1 {
                    name: "test1".to_owned(),
                }),
            );

            table.insert(
                "outer_key".to_owned(),
                "inner_key2".to_owned(),
                "Test2".to_owned(),
                Box::new(Test2 { age: 10 }),
            );
            Ok(None)
        })
        .await;
    assert!(result.is_ok());

    let mut synchronizer2: TableSynchronizer = client_factory
        .create_table_synchronizer("synchronizer2".to_owned())
        .await;

    synchronizer2.fetch_updates().await.expect("fetch updates");

    let value = synchronizer2.get("outer_key", "inner_key1").expect("get value");

    match value.type_id.as_str() {
        "Test1" => {
            let result: Test1 = deserialize_from(&value.data).expect("deserialize");
            assert_eq!(result.name, "test1".to_owned());
        }
        _ => panic!("Wrong type id"),
    }
    info!("test_insert_and_get_with_customize_struct passed");
}

async fn test_fetching_updates_delta(client_factory: &ClientFactory) {
    info!("test fetching updates delta");
    let mut synchronizer: TableSynchronizer = client_factory
        .create_table_synchronizer("fetch_updates".to_owned())
        .await;

    let result = synchronizer
        .insert(|table| {
            table.insert(
                "outer_key".to_owned(),
                "inner_key".to_owned(),
                "i32".to_owned(),
                Box::new(1),
            );
            Ok(None)
        })
        .await;
    assert!(result.is_ok());
    let updates1 = synchronizer.fetch_updates().await.expect("fetch updates");
    assert_eq!(updates1, 2);
    let result = synchronizer
        .insert(|table| {
            table.insert(
                "outer_key".to_owned(),
                "inner_key".to_owned(),
                "i32".to_owned(),
                Box::new(2),
            );
            Ok(None)
        })
        .await;
    let updates2 = synchronizer.fetch_updates().await.expect("fetch updates");
    assert!(result.is_ok());
    assert_eq!(updates2, 2);
    let value = synchronizer.get("outer_key", "inner_key").expect("get value");
    let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
    assert_eq!(data, 2);

    let updates3 = synchronizer.fetch_updates().await.expect("fetch updates");
    assert_eq!(updates3, 0);
    info!("test insert with two table synchronizers passed");
}
