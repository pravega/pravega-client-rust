use std::collections::HashMap;
use crate::tablemap::{TableMap, TableError};
use crate::client_factory::ClientFactoryInternal;
use std::any::Any;
use tracing::{debug, error, warn, info};

pub struct TableSynchronizer<'a> {
    name: String,
    table_maps: TableMap<'a>,
    in_memory_map: HashMap<Key, Box<dyn Any>>,
}

/// Fixme: Do we also need to support Any trait for Key?
pub struct Key {
    key: String,
    key_version: i64, /// TableKey::KEY_NO_VERSION means inserting a new value,
}

pub struct Update {
    key: Key,
    update_type: UpdateType,
    value: Box<dyn Any>,
}

pub enum UpdateType {
    UpdatesOrInsert,
    Remove,
}

pub struct Return {
    key: String,
    value: Box<dyn Any>,
}

impl<'a> TableSynchronizer<'a> {
    pub fn new(name: String, factory: &'a ClientFactoryInternal)  -> TableSynchronizer<'a> {
        TableSynchronizer{
            name,
            table_maps: TableMap::new(name.clone(), factory),
            in_memory_map: HashMap::new(),
        }
    }
    /// Gets the map object currently held in memory.
    /// This is a non-blocking call.
    pub fn get_current_map(&self) -> HashMap<String, Box<dyn Any>> {
        self.current_maps.clone()
    }

    ///Gets the name of the table_sychronizer, the name is same as the stream name.
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// Fetch and apply updates needs to the map object held in memory up to date.
    pub fn fetch_updates(&self) {

    }

    /// Create a list of updates and applies it atomically.
    pub fn update_map_conditionally(&self, mut updates_generator: impl FnMut(HashMap<String, Box<dyn Any>>) -> Vec<Update>) -> Result<(), TableError>{
        conditionally_write(updates_generator, &self);
    }

    /// Create a list of deletes and applies it atomically.
    pub fn remove_map_conditionally(&self, mut deletes_generateor: impl FnMut(HashMap<String, Box<dyn Any>>) -> Vec<Update>) {

    }

    ///
}

async fn conditionally_write(updates_generator: impl FnMut(HashMap<String, Box<dyn Any>>) -> Vec<Update>, table_synchronizer: &TableSynchronizer) -> Result<(), TableError>{
    loop {
        let map = table_synchronizer.get_current_map();
        let to_updates = updates_generator(map);
        debug!("Conditionally Write {}", toUpdates);
        if toUpdates.len() == 0 {
            debug!("Conditionally Write to {} completed, as there is nothing to update for map {}",
                   table_synchronizer.get_name(), map);
            break;
        }
        let vec = to_updates.into_iter().map(|x| (x.key.key.as_str(), x.value.as_ref(), x.key.key_version)).rev().collect();

        let result = table_synchronizer.table_maps.insert_conditionally_all(vec).await;

        match result {
            Err(e) => {
                match e {
                    TableError::ConnectionError  =>  {
                        return Err(e);
                    },
                    TableError::IncorrectKeyVersion => {
                        table_synchronizer.fetch_updates();
                    }
                    TableError::KeyDoesNotExist  => {
                        table_synchronizer.fetch_updates();
                    }
                }
            }
            Ok(res) => {
                applyUpdatesToLocalMap(to_updates, res, table_synchronizer);
            }
        }
    }
    Ok(())
}

fn applyUpdatesToLocalMap(to_updates: vec<Update>, new_version: Vec<i64>, table_synchronizer: &TableSynchronizer) {
    for update in to_updates {
        let
    }
}

async fn conditinally_remove()