use log::info;
use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::table_synchronizer::{
    deserialize_from, Get, Insert, Key, Remove, TableSynchronizer,
};
use pravega_wire_protocol::client_config::{ClientConfig, ClientConfigBuilder, TEST_CONTROLLER_URI};
use pravega_wire_protocol::commands::TableKey;
use serde::{Deserialize, Serialize};

pub async fn test_tablesynchronizer() {
    let config = ClientConfigBuilder::default()
        .controller_uri(TEST_CONTROLLER_URI)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config.clone());
    test_insert(&client_factory).await;
    test_remove(&client_factory).await;
    test_get_remote(&client_factory).await;
    test_insert_with_two_table_synchronizers(&client_factory).await;
    test_remove_with_two_table_synchronizers(&client_factory).await;
    test_insert_and_get_with_customize_struct(&client_factory).await;
}

async fn test_insert(client_factory: &ClientFactory) {
    let mut synchronizer: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer".into())
        .await;

    let result = synchronizer
        .insert(|to_insert, map| {
            if map.is_empty() {
                let update = Insert {
                    key: "test".to_string(),
                    type_id: "i32".into(),
                    new_value: Box::new(1),
                };
                to_insert.push(update);
            }
        })
        .await;

    assert!(result.is_ok());
    let value_option = synchronizer.get(&"test".to_string());
    assert!(value_option.is_some());

    let version = synchronizer
        .get_key_version(&"test".into())
        .expect("get the key version");
    assert_eq!(version, 0);

    //check if fetchUpdates work correctly.
    let mut synchronizer2: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer".into())
        .await;
    let result = synchronizer2.fetch_updates().await;
    assert!(result.is_ok());
    let value_option = synchronizer2.get(&"test".to_string());
    assert!(value_option.is_some());
    let version = synchronizer2
        .get_key_version(&"test".into())
        .expect("get the key version");
    assert_eq!(version, 0);
}

async fn test_remove(client_factory: &ClientFactory) {
    let mut synchronizer: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer1".into())
        .await;
    let result = synchronizer
        .insert(|to_insert, map| {
            if map.is_empty() {
                let update = Insert {
                    key: "test".to_string(),
                    type_id: "i32".into(),
                    new_value: Box::new(2),
                };
                to_insert.push(update);
            }
        })
        .await;
    assert!(result.is_ok());

    let result = synchronizer
        .remove(|to_remove, map| {
            if map.get("test").is_some() {
                let remove = Remove {
                    key: "test".to_string(),
                };
                to_remove.push(remove);
            }
        })
        .await;
    assert!(result.is_ok());

    let value_option = synchronizer.get(&"test".to_string());
    assert!(value_option.is_none());

    //check if fetchUpdates work correctly.
    let mut synchronizer2: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer1".into())
        .await;
    let result = synchronizer2.fetch_updates().await;
    assert!(result.is_ok());
    let value_option = synchronizer2.get(&"test".to_string());
    assert!(value_option.is_none());
}

async fn test_get_remote(client_factory: &ClientFactory) {
    let mut synchronizer: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer2".into())
        .await;

    let result = synchronizer
        .insert(|to_insert, map| {
            if map.is_empty() {
                let update = Insert {
                    key: "test".to_string(),
                    type_id: "i32".into(),
                    new_value: Box::new(4),
                };
                to_insert.push(update);
            }
        })
        .await;
    assert!(result.is_ok());

    let mut synchronizer2: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer2".into())
        .await;

    let result = synchronizer2
        .get_remote(|to_get, map| {
            // the local map is empty
            if map.is_empty() {
                // try to get from remote.
                let get = Get {
                    key: "test".to_string(),
                };
                to_get.push(get);
            }
        })
        .await;
    assert!(result.is_ok());
    let value_option = synchronizer2.get(&"test".to_string());
    assert!(value_option.is_some());
}

async fn test_insert_with_two_table_synchronizers(client_factory: &ClientFactory) {
    let mut synchronizer: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer".into())
        .await;

    let mut synchronizer2: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer".into())
        .await;
    synchronizer.fetch_updates().await.expect("fetch updates");
    synchronizer2.fetch_updates().await.expect("fetch updates");

    let result = synchronizer
        .insert(|to_update, map| {
            if map.contains_key(&"test".to_string()) {
                let value = map.get(&"test".to_string()).expect("get value");
                let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
                if data == 1 {
                    let update = Insert {
                        key: "test".to_string(),
                        type_id: "i32".into(),
                        new_value: Box::new(2),
                    };
                    to_update.push(update);
                }
            }
        })
        .await;
    assert!(result.is_ok());

    let result = synchronizer2
        .insert(|to_update, map| {
            if map.contains_key(&"test".to_string()) {
                let map = synchronizer.get_current_map();
                let value = map.get(&"test".to_string()).expect("get value");
                let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
                // Incorrect, because the value is already changed to 2.
                if data == 1 {
                    let update = Insert {
                        key: "test".to_string(),
                        type_id: "i32".into(),
                        new_value: Box::new(4),
                    };
                    to_update.push(update);
                }
                // Correct
                if data == 2 {
                    let update = Insert {
                        key: "test".to_string(),
                        type_id: "i32".into(),
                        new_value: Box::new(3),
                    };
                    to_update.push(update);
                }
            }
        })
        .await;
    assert!(result.is_ok());

    let result = synchronizer.fetch_updates().await;
    assert!(result.is_ok());
    let value = synchronizer.get(&"test".to_string()).expect("get value");
    let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
    assert_eq!(data, 3);
}

async fn test_remove_with_two_table_synchronizers(client_factory: &ClientFactory) {
    let mut synchronizer: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer".into())
        .await;

    let mut synchronizer2: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer".into())
        .await;
    synchronizer.fetch_updates().await.expect("fetch updates");
    synchronizer2.fetch_updates().await.expect("fetch updates");

    let result = synchronizer
        .remove(|to_remove, map| {
            let value = map.get(&"test".to_string()).expect("get value");
            let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
            if data == 3 {
                let remove = Remove {
                    key: "test".to_string(),
                };
                to_remove.push(remove);
            }
        })
        .await;
    assert!(result.is_ok());

    let result = synchronizer2
        .insert(|to_update, map| {
            if !map.is_empty() {
                // Even if it matches in in_memory map.
                // This update should failed, because the key is already removed.
                let update = Insert {
                    key: "test".to_string(),
                    type_id: "i32".into(),
                    new_value: Box::new(4),
                };
                to_update.push(update);
            }
        })
        .await;

    assert!(result.is_ok());
    let value_option = synchronizer.get(&"test".to_string());
    assert!(value_option.is_none());
}

async fn test_insert_and_get_with_customize_struct(client_factory: &ClientFactory) {
    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct Test1 {
        name: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct Test2 {
        age: i32,
    }

    let mut synchronizer: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer2".into())
        .await;

    let result = synchronizer
        .insert(|to_update, _map| {
            let insert = Insert {
                key: "test1".to_string(),
                type_id: "Test1".into(),
                new_value: Box::new(Test1 {
                    name: "test1".to_string(),
                }),
            };
            to_update.push(insert);
            let insert = Insert {
                key: "test2".to_string(),
                type_id: "Test2".into(),
                new_value: Box::new(Test2 { age: 10 }),
            };
            to_update.push(insert);
        })
        .await;
    assert!(result.is_ok());

    let mut synchronizer2: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer2".into())
        .await;

    synchronizer2.fetch_updates().await.expect("fetch updates");

    let value = synchronizer2.get(&"test1".to_string()).expect("get value");

    match value.type_id.as_str() {
        "Test1" => {
            let result: Test1 = deserialize_from(&value.data).expect("deserialize");
            assert_eq!(result.name, "test1".to_string());
        }
        _ => panic!("Wrong type id"),
    }
}
