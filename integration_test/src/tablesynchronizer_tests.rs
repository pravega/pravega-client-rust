use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::table_synchronizer::{Insert, Key, Remove, TableSynchronizer};
use pravega_wire_protocol::client_config::{ClientConfig, ClientConfigBuilder, TEST_CONTROLLER_URI};
use pravega_wire_protocol::commands::TableKey;

pub async fn test_tablesynchronizer() {
    let config = ClientConfigBuilder::default()
        .controller_uri(TEST_CONTROLLER_URI)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config.clone());
    test_insert_conditionally(&client_factory).await;
    test_remove_conditionally(&client_factory).await;
}

async fn test_insert_conditionally(client_factory: &ClientFactory) {
    let mut synchronizer: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer".into())
        .await;

    let result = synchronizer
        .insert_conditionally(|map| {
            let mut to_update = Vec::new();
            if map.len() == 0 {
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
    let value_option = synchronizer.get(&"test".into());
    assert!(value_option.is_some());
    let version = synchronizer
        .get_key_version(&"test".into())
        .expect("get the key and value");
    assert_eq!(version, 0);
}

async fn test_remove_conditionally(client_factory: &ClientFactory) {
    let mut synchronizer: TableSynchronizer<String> = client_factory
        .create_table_synchronizer("synchronizer1".into())
        .await;
    let result = synchronizer
        .insert_conditionally(|map| {
            let mut to_update = Vec::new();
            if map.len() == 0 {
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

    let value_option = synchronizer.get(&"test".into());
    assert!(value_option.is_none());
}
