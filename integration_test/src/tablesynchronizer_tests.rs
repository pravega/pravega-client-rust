use pravega_wire_protocol::client_config::{ClientConfig, ClientConfigBuilder,TEST_CONTROLLER_URI};
use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::table_synchronizer::{UpdateOrInsert, Key, ValueType, TableSynchronizer};

pub async fn test_tablesynchronizer() {
    let config = ClientConfigBuilder::default()
        .controller_uri(TEST_CONTROLLER_URI)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config.clone());
    test_insert_conditionally(&client_factory).await;
}

async fn test_insert_conditionally(client_factory: &ClientFactory) {
    let mut synchronizer: TableSynchronizer<String> = client_factory.create_table_synchronizer("synchronizer".into()).await;

    let result = synchronizer.insert_map_conditionally(|map| {
        let mut to_update = Vec::new();
        if map.len() == 0 {
            let key = Key {
                key: "test".to_string(),
                key_version: 0
            };
            let update = UpdateOrInsert {
                key,
                type_code: ValueType::Integer,
                new_value: Box::new(1)
            };
            to_update.push(update);
        }
        to_update
    }).await;
    assert!(result.is_ok());
}