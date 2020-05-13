use std::collections::HashMap;
use crate::tablemap::{TableMap, TableError, Version};
use crate::client_factory::ClientFactoryInternal;
use std::any::Any;
use std::cmp::{PartialEq, Eq};
use std::hash::{Hash, Hasher};
use tracing::{debug, info};
use serde::Serialize;
use serde::de::DeserializeOwned;
use futures::stream::StreamExt;
use futures::pin_mut;

pub struct TableSynchronizer<'a, K: Serialize + DeserializeOwned + std::marker::Unpin> {
    name: String,
    table_maps: TableMap<'a>,
    in_memory_map: HashMap<Key<K>, Box<dyn Any>>,
}


pub struct Key<K: Serialize + DeserializeOwned + std::marker::Unpin> {
    key: K,
    key_version: Version,
}

impl<K: std::fmt::Debug> std::fmt::Debug for Key<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "key: {} key_version:{}", self.key, self.key_version)?;
        Ok(())
    }
}

impl<K> PartialEq for Key<K> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K> Eq for Key<K> {}

impl<K> Hash for Key<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state)
    }
}

pub struct UpdateOrInsert<K: Serialize + DeserializeOwned + std::marker::Unpin> {
    key: Key<K>,
    new_value: Box<dyn Any>,
}

pub struct Remove<K: Serialize + DeserializeOwned + std::marker::Unpin> {
    key: Key<K>
}

impl<'a, K: Serialize + DeserializeOwned + std::marker::Unpin> TableSynchronizer<'a, K> {
    pub fn new(name: String, factory: &'a ClientFactoryInternal)  -> TableSynchronizer<'a, K> {
        TableSynchronizer{
            name,
            table_maps: TableMap::new(name.clone(), factory),
            in_memory_map: HashMap::new(),
        }
    }
    /// Gets the map object currently held in memory.
    /// This is a non-blocking call.
    pub fn get_current_map(&self) -> HashMap<Key<K>, Box<dyn Any>> {
        self.in_memory_map.clone()
    }

    ///Gets the name of the table_synchronizer, the name is same as the stream name.
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// Fetch the latest map and apply it to the local map.
    pub async fn fetch_updates(&mut self) -> Result<(), TableError>{
        debug!("fetch the latest map and apply to the local map");
        let reply = self.table_maps.read_entries_stream(3);
        pin_mut!(reply);
        let mut entry_count: i32 = 0;
        while let Some(entry) = reply.next().await {
            match entry {
                Ok(t) => {
                    let k = t.0;
                    let v = t.1;
                    let version = t.2;
                    let key = Key {
                        key: k,
                        key_version: version,
                    };
                    self.in_memory_map.insert(key, v);
                    entry_count += 1;
                }
                _ => return Err(TableError::OperationError {
                    operation: "fetch_updates".to_string(),
                    error_msg: "fetch entries error".to_string()
                }),
            }
        }
        debug!("finish fetch updates, now the map has {} entries", entry_count);
        Ok(())
    }

    /// Insert/Updates a list of keys and applies it atomically.
    pub async fn insert_map_conditionally(&mut self, mut updates_generator: impl FnMut(HashMap<Key<K>, Box<dyn Any>>) -> Vec<UpdateOrInsert<K>>) -> Result<(), TableError> {
        conditionally_write(updates_generator, self).await
    }

    /// Remove a list of keys and applies it atomically.
    pub async fn remove_map_conditionally(&mut self, mut deletes_generateor: impl FnMut(HashMap<Key<K>, Box<dyn Any>>) -> Vec<Remove<K>>) -> Result<(), TableError> {
        conditionally_remove(deletes_generateor, self).await
    }

    /// Insert/update a list of keys unconditionally.
    pub async fn insert_map_unconditionally(&mut self, mut updates_generator: impl FnMut(HashMap<Key<K>, Box<dyn Any>>) -> Vec<UpdateOrInsert<K>>) -> Result<(), TableError> {
        unconditionally_write(updates_generator, self).await
    }

    /// Remove a list of keys unconditionally.
    pub async fn remove_map_unconditionally(&mut self, mut deletes_generateor: impl FnMut(HashMap<Key<K>, Box<dyn Any>>) -> Vec<Remove<K>>) -> Result<(), TableError> {
        unconditionally_remove(deletes_generateor, self).await
    }

    /// Fixme: do we need to implement this?
    pub fn compact() {
        unimplemented!()
    }
}

async fn conditionally_write<K>(updates_generator: impl FnMut(HashMap<Key<K>, Box<dyn Any>>) -> Vec<UpdateOrInsert<K>>, table_synchronizer: &mut TableSynchronizer<'_, K>) -> Result<(), TableError>
    where
        K: Serialize + DeserializeOwned + std::marker::Unpin,
{
    loop {
        let map = table_synchronizer.get_current_map();
        let to_update = updates_generator(map);
        if to_update.len() == 0 {
            debug!("Conditionally Write to {} completed, as there is nothing to update for map",
                   table_synchronizer.get_name());
            break;
        }
        let vec = to_update.into_iter().map(|x| (&x.key.key, x.new_value.as_ref(), x.key.key_version)).rev().collect();

        let result = table_synchronizer.table_maps.insert_conditionally_all(vec).await;

        match result {
            Err(e) => {
                match e {
                    TableError::IncorrectKeyVersion{
                        operation, error_msg
                    } => {
                        table_synchronizer.fetch_updates();
                    }
                    TableError::KeyDoesNotExist{
                        operation, error_msg
                    } => {
                        table_synchronizer.fetch_updates();
                    }
                    _ => {
                        return Err(e);
                    }
                }
            }
            Ok(res) => {
                apply_updates_to_localmap(to_update, res, table_synchronizer);
            }
        }
    }
    Ok(())
}

async fn unconditionally_write<K>(updates_generator: impl FnMut(HashMap<Key<K>, Box<dyn Any>>) -> Vec<UpdateOrInsert<K>>, table_synchronizer: &mut TableSynchronizer<'_, K>) -> Result<(), TableError>
    where
        K: Serialize + DeserializeOwned + std::marker::Unpin,
{
    loop {
        let map = table_synchronizer.get_current_map();
        let to_update = updates_generator(map);
        if to_update.len() == 0 {
            debug!("Conditionally Write to {} completed, as there is nothing to update for map",
                   table_synchronizer.get_name());
            break;
        }
        let vec = to_update.into_iter().map(|x| (&x.key.key, x.new_value.as_ref())).rev().collect();
        let result = table_synchronizer.table_maps.insert_all(vec).await;
        match result {
            Err(e) => {
                match e {
                    TableError::IncorrectKeyVersion{
                        operation, error_msg
                    } => {
                        table_synchronizer.fetch_updates();
                    }
                    TableError::KeyDoesNotExist {
                        operation, error_msg
                    }  => {
                        table_synchronizer.fetch_updates();
                    }
                    _ => {
                        return Err(e);
                    }
                }
            }
            Ok(res) => {
                apply_updates_to_localmap(to_update, res, table_synchronizer);
            }
        }
    }
    Ok(())

}

fn apply_updates_to_localmap<K>(to_update: Vec<UpdateOrInsert<K>>, new_version: Vec<i64>, table_synchronizer: &mut TableSynchronizer<'_, K>)
    where
        K: Serialize + DeserializeOwned + std::marker::Unpin,
{
    let mut i = 0;
    for update in to_update {
        let new_key = Key{
            key: update.key.key,
            key_version: new_version.get(i).expect("get new version").clone(),
        };
        let new_value = update.new_value;
        table_synchronizer.in_memory_map.insert(new_key, new_value);
        i += 1;
    }
    debug!("Updates {} entries in local map ", i);
}

async fn conditionally_remove<K>(delete_generator: impl FnMut(HashMap<Key<K>, Box<dyn Any>>) -> Vec<Remove<K>>, table_synchronizer: &mut TableSynchronizer<'_, K>) -> Result<(), TableError>
    where
        K: Serialize + DeserializeOwned + std::marker::Unpin,
{
    loop {
        let map = table_synchronizer.get_current_map();
        let to_delete = delete_generator(map);
        if to_delete.len() == 0 {
            debug!("Conditionally remove to {} completed, as there is nothing to remove for map",
                   table_synchronizer.get_name());
            break;
        }

        let vec = to_delete.into_iter().map(|x| (&x.key.key, x.key.key_version)).rev().collect();

        let result = table_synchronizer.table_maps.remove_conditionally_all(vec).await;
        match result {
            Err(e) => {
                match e {
                    TableError::IncorrectKeyVersion{
                        operation, error_msg
                    } => {
                        table_synchronizer.fetch_updates();
                    }
                    TableError::KeyDoesNotExist{
                        operation, error_msg
                    } => {
                        table_synchronizer.fetch_updates();
                    }
                    _ => {
                        return Err(e);
                    }
                }
            }
            Ok(()) => {
                apply_deletes_to_localmap(to_delete, table_synchronizer);
            }
        }
    }
    Ok(())
}

async fn unconditionally_remove<K>(delete_generator: impl FnMut(HashMap<Key<K>, Box<dyn Any>>) -> Vec<Remove<K>>, table_synchronizer: &mut TableSynchronizer<'_, K>) -> Result<(), TableError>
    where
        K: Serialize + DeserializeOwned + std::marker::Unpin,
{
    loop {
        let map = table_synchronizer.get_current_map();
        let to_delete = delete_generator(map);
        if to_delete.len() == 0 {
            debug!("Conditionally remove to {} completed, as there is nothing to remove for map",
                   table_synchronizer.get_name());
            break;
        }
        let vec = to_delete.into_iter().map(|x| &x.key.key).rev().collect();
        let result = table_synchronizer.table_maps.remove_all(vec).await;

        match result {
            Err(e) => {
                match e {
                    TableError::IncorrectKeyVersion{
                        operation, error_msg
                    } => {
                        table_synchronizer.fetch_updates();
                    }
                    TableError::KeyDoesNotExist{
                        operation, error_msg
                    } => {
                        table_synchronizer.fetch_updates();
                    }
                    _ => {
                        return Err(e);
                    }
                }
            }
            Ok(()) => {
                apply_deletes_to_localmap(to_delete, table_synchronizer);
            }
        }
    }
    Ok(())
}

fn apply_deletes_to_localmap<K>(to_delete: Vec<Remove<K>>, table_synchronizer: &mut TableSynchronizer<K>)
    where
        K: Serialize + DeserializeOwned + std::marker::Unpin,
{
    let mut i = 0;
    for delete in to_delete {
        table_synchronizer.in_memory_map.remove(&delete.key);
        i += 1;
    }
    debug!("Deletes {} entries in local map ", i);
}