use log::info;
use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::table_synchronizer::{deserialize_from, Insert, Key, Remove, TableSynchronizer};
use pravega_wire_protocol::client_config::{ClientConfig, ClientConfigBuilder, TEST_CONTROLLER_URI};
use pravega_wire_protocol::commands::TableKey;
use serde::{Deserialize, Serialize};

pub async fn test_tablesynchronizer() {
    let config = ClientConfigBuilder::default()
        .controller_uri(TEST_CONTROLLER_URI)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config.clone());
    test_insert_conditionally(&client_factory).await;
    test_remove_conditionally(&client_factory).await;
    test_insert_with_two_table_synchronizers(&client_factory).await;
    test_remove_with_two_table_synchronizers(&client_factory).await;
    test_insert_and_get_with_customize_struct(&client_factory).await;
}

async fn test_insert_conditionally(client_factory: &ClientFactory) {
    let mut synchronizer: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer".into())
        .await;

    let result = synchronizer
        .insert_conditionally(|map| {
            let mut to_update = Vec::new();
            if map.is_empty() {
                let update = Insert {
                    key: "test".to_string(),
                    type_id: 1,
                    new_value: Box::new(1),
                };
                to_update.push(update);
            }
            to_update
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

async fn test_remove_conditionally(client_factory: &ClientFactory) {
    let mut synchronizer: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer1".into())
        .await;
    let result = synchronizer
        .insert_conditionally(|map| {
            let mut to_update = Vec::new();
            if map.is_empty() {
                let update = Insert {
                    key: "test".to_string(),
                    type_id: 1,
                    new_value: Box::new(2),
                };
                to_update.push(update);
            }
            to_update
        })
        .await;

    assert!(result.is_ok());
    let result = synchronizer
        .remove_conditionally(|map| {
            let mut to_remove = Vec::new();
            if map.get("test").is_some() {
                let remove = Remove {
                    key: "test".to_string(),
                };
                to_remove.push(remove);
            }
            to_remove
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
        .insert_conditionally(|map| {
            let mut to_update = Vec::new();
            if map.contains_key(&"test".to_string()) {
                let value = map.get(&"test".to_string()).expect("get value");
                let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
                if data == 1 {
                    let update = Insert {
                        key: "test".to_string(),
                        type_id: 1,
                        new_value: Box::new(2),
                    };
                    to_update.push(update);
                }
            }
            to_update
        })
        .await;
    assert!(result.is_ok());

    let result = synchronizer2
        .insert_conditionally(|map| {
            let mut to_update = Vec::new();
            if map.contains_key(&"test".to_string()) {
                let value = map.get(&"test".to_string()).expect("get value");
                let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
                // Incorrect, because the value is already changed to 2.
                if data == 1 {
                    let update = Insert {
                        key: "test".to_string(),
                        type_id: 1,
                        new_value: Box::new(4),
                    };
                    to_update.push(update);
                }
                // Correct
                if data == 2 {
                    let update = Insert {
                        key: "test".to_string(),
                        type_id: 1,
                        new_value: Box::new(3),
                    };
                    to_update.push(update);
                }
            }
            to_update
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
        .remove_conditionally(|map| {
            let mut to_remove = Vec::new();
            let value = map.get(&"test".to_string()).expect("get value");
            let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
            if data == 3 {
                let remove = Remove {
                    key: "test".to_string(),
                };
                to_remove.push(remove);
            }
            to_remove
        })
        .await;
    assert!(result.is_ok());

    let result = synchronizer2
        .insert_conditionally(|map| {
            let mut to_update = Vec::new();
            if map.is_empty() {
                // Even if it matches in in_memory map.
                // This update should failed, because the key is already removed.
                let update = Insert {
                    key: "test".to_string(),
                    type_id: 1,
                    new_value: Box::new(4),
                };
                to_update.push(update);
            }
            to_update
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
        .insert_conditionally(|_map| {
            let mut to_update = Vec::new();
            let insert = Insert {
                key: "test1".to_string(),
                type_id: 1,
                new_value: Box::new(Test1 {
                    name: "test1".to_string(),
                }),
            };
            to_update.push(insert);
            let insert = Insert {
                key: "test2".to_string(),
                type_id: 2,
                new_value: Box::new(Test2 { age: 10 }),
            };
            to_update.push(insert);
            to_update
        })
        .await;
    assert!(result.is_ok());

    let mut synchronizer2: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer2".into())
        .await;

    synchronizer2.fetch_updates().await.expect("fetch updates");

    let value = synchronizer2.get(&"test1".to_string()).expect("get value");

    match value.type_id {
        1 => {
            let result: Test1 = deserialize_from(&value.data).expect("deserialize");
            assert_eq!(result.name, "test1".to_string());
        }
        _ => panic!("Wrong type id"),
    }
}
