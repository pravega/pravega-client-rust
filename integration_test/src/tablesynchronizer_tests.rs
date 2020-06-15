use log::info;
use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::table_synchronizer::{deserialize_from, Key, TableSynchronizer};
use pravega_wire_protocol::client_config::{ClientConfig, ClientConfigBuilder, TEST_CONTROLLER_URI};
use pravega_wire_protocol::commands::TableKey;
use serde::{Deserialize, Serialize};

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
}

async fn test_insert(client_factory: &ClientFactory) {
    let mut synchronizer: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer".into())
        .await;

    let result = synchronizer
        .insert(|table| {
            if table.is_empty() {
                table.insert("test".to_string(), "i32".into(), Box::new(1));
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
    info!("test_insert passed");
}

async fn test_remove(client_factory: &ClientFactory) {
    let mut synchronizer: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer1".into())
        .await;
    let result = synchronizer
        .insert(|table| {
            if table.is_empty() {
                table.insert("test".to_string(), "i32".into(), Box::new(2));
            }
        })
        .await;
    assert!(result.is_ok());

    let result = synchronizer
        .remove(|table| {
            if table.get(&"test".to_string()).is_some() {
                table.remove("test".to_string());
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
    info!("test_remove passed");
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
        .insert(|table| {
            if table.contains_key(&"test".to_string()) {
                let value = table.get(&"test".to_string()).expect("get value");
                let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
                if data == 1 {
                    table.insert("test".to_string(), "i32".into(), Box::new(2));
                }
            }
        })
        .await;
    assert!(result.is_ok());

    let result = synchronizer2
        .insert(|table| {
            if table.contains_key(&"test".to_string()) {
                let value = table.get(&"test".to_string()).expect("get value");
                let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
                // Incorrect, because the value is already changed to 2.
                if data == 1 {
                    table.insert("test".to_string(), "i32".into(), Box::new(4));
                }
                // Correct
                if data == 2 {
                    table.insert("test".to_string(), "i32".into(), Box::new(3));
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
    info!("test_insert_with_two_table_synchronizers passed");
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
        .remove(|table| {
            let value = table.get(&"test".to_string()).expect("get value");
            let data: i32 = deserialize_from(&value.data).expect("deserialize value data");
            if data == 3 {
                table.remove("test".to_string());
            }
        })
        .await;
    assert!(result.is_ok());

    info!("start to update a non-existed key");
    let result = synchronizer2
        .insert(|table| {
            if !table.is_empty() {
                // Even if it matches in in_memory map.
                // This update should failed, because the key is already removed.
                table.insert("test".to_string(), "i32".into(), Box::new(4));
            }
        })
        .await;

    assert!(result.is_ok());
    let value_option = synchronizer.get(&"test".to_string());
    assert!(value_option.is_none());
    info!("test_remove_with_two_table_synchronizers passed");
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
        .insert(|table| {
            table.insert(
                "test1".to_string(),
                "Test1".into(),
                Box::new(Test1 {
                    name: "test1".to_string(),
                }),
            );

            table.insert("test2".to_string(), "Test2".into(), Box::new(Test2 { age: 10 }));
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
    info!("test_insert_and_get_with_customize_struct passed");
}
