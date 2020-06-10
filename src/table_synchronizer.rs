use crate::client_factory::ClientFactoryInternal;
use crate::tablemap::{TableError, TableMap, Version};
use futures::pin_mut;
use futures::stream::StreamExt;
use pravega_wire_protocol::commands::TableKey;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_cbor::ser::Serializer as CborSerializer;
use std::clone::Clone;
use std::cmp::{Eq, PartialEq};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::Unpin;
use std::slice::Iter;
use tracing::{debug, info};

/// Provides a mean to have a map that is synchronized between many processes.
/// The pattern is to have a map that can be update by Insert or Remove,
/// Each host can perform logic based on its in_memory map and apply updates by supplying a
/// function to create Insert/Remove objects.
/// Update from other hosts can be obtained by calling fetchUpdates().

/// TableSynchronizer contains a name, a table_map that send the map entries to server
/// and an in_memory map.
pub struct TableSynchronizer<'a, K>
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    name: String,
    table_map: TableMap<'a>,
    in_memory_map: HashMap<Key<K>, Value>,
}

impl<'a, K> TableSynchronizer<'a, K>
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    pub async fn new(name: String, factory: &'a ClientFactoryInternal) -> TableSynchronizer<'a, K> {
        let table_map = TableMap::new(name.clone(), factory)
            .await
            .expect("create table map");
        TableSynchronizer {
            name: name.clone(),
            table_map,
            in_memory_map: HashMap::new(),
        }
    }

    /// Gets the map object currently held in memory.
    /// it will remove the version information of key.
    pub fn get_current_map(&self) -> HashMap<K, Value> {
        let mut result = HashMap::new();
        for (key, value) in self.in_memory_map.iter() {
            let new_value = value.clone();
            result.insert(key.key.clone(), new_value);
        }
        result
    }

    ///Gets the name of the table_synchronizer, the name is same as the stream name.
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// Gets the Value of the GivenKey in the local map,
    /// This is a non-blocking call.
    /// The data in Value is not deserialized and the caller should call deserialize_from to deserialize.
    pub fn get(&self, key: &K) -> Option<&Value> {
        let search_key = Key {
            key: key.clone(),
            key_version: TableKey::KEY_NO_VERSION,
        };
        self.in_memory_map.get(&search_key)
    }

    /// Gets the Key version of the GivenKey,
    /// This is a non-blocking call.
    pub fn get_key_version(&self, key: &K) -> Option<Version> {
        let search_key = Key {
            key: key.clone(),
            key_version: TableKey::KEY_NO_VERSION,
        };
        let option = self.in_memory_map.get_key_value(&search_key);
        if let Some((key, _value)) = option {
            Some(key.key_version)
        } else {
            None
        }
    }

    /// Gets the Key Value Pair of the GivenKey,
    /// This is a non-blocking call.
    /// It will return a copy of the key-value pair.
    pub fn get_key_value(&self, key: &K) -> Option<(K, Value)> {
        let search_key = Key {
            key: key.clone(),
            key_version: TableKey::KEY_NO_VERSION,
        };
        let entry = self.in_memory_map.get_key_value(&search_key);

        if let Some((key, value)) = entry {
            Some((key.key.clone(), value.clone()))
        } else {
            None
        }
    }

    /// Fetch the latest map in remote server and apply it to the local map.
    pub async fn fetch_updates(&mut self) -> Result<(), TableError> {
        debug!("fetch the latest map and apply to the local map");
        let reply = self.table_map.read_entries_stream(3);
        pin_mut!(reply);
        self.in_memory_map.clear();
        let mut entry_count: i32 = 0;
        while let Some(entry) = reply.next().await {
            match entry {
                Ok((k, v, version)) => {
                    let key = Key {
                        key: k,
                        key_version: version,
                    };
                    self.in_memory_map.insert(key, v);
                    entry_count += 1;
                }
                _ => {
                    return Err(TableError::OperationError {
                        operation: "fetch_updates".to_string(),
                        error_msg: "fetch entries error".to_string(),
                    });
                }
            }
        }
        info!("finish fetch updates, now the map has {} entries", entry_count);
        Ok(())
    }

    /// Insert/Updates a list of keys and applies it atomically to local map.
    /// This will update the local_map to latest version.
    pub async fn insert(&mut self, updates_generator: impl FnMut(&mut Table<K>)) -> Result<(), TableError> {
        conditionally_write(updates_generator, self).await
    }

    /// Remove a list of keys and applies it atomically to local map.
    /// This will update the local_map to latest version.
    pub async fn remove(&mut self, deletes_generateor: impl FnMut(&mut Table<K>)) -> Result<(), TableError> {
        conditionally_remove(deletes_generateor, self).await
    }
}

/// The Key struct in the in_memory map. it contains two fields, the key and key_version.
/// key shoulda.
#[derive(Debug, Clone)]
pub struct Key<K: Debug + Eq + Hash + PartialEq + Clone + Serialize + DeserializeOwned> {
    pub key: K,
    pub key_version: Version,
}

impl<K: Debug + Eq + Hash + PartialEq + Clone + Serialize + DeserializeOwned> PartialEq for Key<K> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K: Debug + Eq + Hash + PartialEq + Clone + Serialize + DeserializeOwned> Eq for Key<K> {}

impl<K: Debug + Eq + Hash + PartialEq + Clone + Serialize + DeserializeOwned> Hash for Key<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state)
    }
}

/// The Value struct in the in_memory map. It contains two fields.
/// type_id of the data. It is used for deserializing data between different processes..
/// data: the Value data after serialized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Value {
    pub type_id: String,
    pub data: Vec<u8>,
}

/// The Updates struct. It would be supplied to insert/remove methods.
pub struct Table<K> {
    map: HashMap<K, Value>,
    insert: Vec<Insert<K>>,
    remove: Vec<K>,
}

impl<K> Table<K>
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    pub fn insert(&mut self, key: K, type_id: String, new_value: Box<dyn ValueData>) {
        let data = serialize(&*new_value).expect("serialize value");
        let insert = Insert {
            key: key.clone(),
            type_id: type_id.clone(),
        };

        self.insert.push(insert);
        //Also insert into map.
        self.map.insert(key, Value{
            type_id,
            data,
        });
    }

    pub fn remove(&mut self, key: K) {
        //Also remove from the map.
        self.map.remove(&key);
        self.remove.push(key);
    }

    pub fn get(&self, key: &K) -> Option<&Value> {
        self.map.get(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub(crate) fn insert_is_empty(&self) -> bool {
        self.insert.is_empty()
    }

    pub(crate) fn remove_is_empty(&self) -> bool {
        self.remove.is_empty()
    }

    pub(crate) fn get_insert_iter(&self) -> Iter<Insert<K>> {
        self.insert.iter()
    }

    pub(crate) fn get_remove_iter(&self) -> Iter<K> {
        self.remove.iter()
    }
}

/// The Insert struct. It contains two fields.
/// The key to insert or update,
/// the type_id for the data.
/// The data is already insert into the map.
pub(crate) struct Insert<K> {
    key: K,
    type_id: String,
}

/// The trait bound for the ValueData
pub trait ValueData: ValueSerialize + ValueClone + Debug {}

impl<T> ValueData for T where T: 'static + Serialize + DeserializeOwned + Clone + Debug {}

/// Clone trait helper.
pub trait ValueClone {
    fn clone_box(&self) -> Box<dyn ValueData>;
}

impl<T> ValueClone for T
where
    T: 'static + ValueData + Clone,
{
    fn clone_box(&self) -> Box<dyn ValueData> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn ValueData> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Serialize trait helper, we need to serialize the ValueData in Insert struct into Vec<u8>.
pub trait ValueSerialize {
    fn serialize_value(
        &self,
        seralizer: &mut CborSerializer<&mut Vec<u8>>,
    ) -> Result<(), serde_cbor::error::Error>;
}

impl<T> ValueSerialize for T
where
    T: Serialize,
{
    fn serialize_value(
        &self,
        serializer: &mut CborSerializer<&mut Vec<u8>>,
    ) -> Result<(), serde_cbor::error::Error> {
        self.serialize(serializer)
    }
}

/// Serialize the <dyn ValueData> into the Vec<u8> by using cbor serializer.
/// This method would be used by the insert method in table_synchronizer.
pub(crate) fn serialize(value: &dyn ValueData) -> Result<Vec<u8>, serde_cbor::error::Error> {
    let mut vec = Vec::new();
    value.serialize_value(&mut CborSerializer::new(&mut vec))?;
    Ok(vec)
}

/// Deserialize the Value into the type T by using cbor deserializer.
/// THis method would be used by the user after calling get() of table_synchronizer.
pub fn deserialize_from<T>(reader: &[u8]) -> Result<T, serde_cbor::error::Error>
where
    T: DeserializeOwned,
{
    serde_cbor::de::from_slice(reader)
}

async fn conditionally_write<K>(
    mut updates_generator: impl FnMut(&mut Table<K>),
    table_synchronizer: &mut TableSynchronizer<'_, K>,
) -> Result<(), TableError>
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    loop {
        let map = table_synchronizer.get_current_map();
        let mut to_update = Table {
            map,
            insert: Vec::new(),
            remove: Vec::new(),
        };

        updates_generator(&mut to_update);

        if to_update.insert_is_empty() {
            debug!(
                "Conditionally Write to {} completed, as there is nothing to update for map",
                table_synchronizer.get_name()
            );
            break;
        }

        let mut to_send = Vec::new();
        for update in to_update.get_insert_iter() {
            let value = to_update.get(&update.key).expect("get the insert data");
            let key_version = table_synchronizer.get_key_version(&update.key);

            if key_version.is_some() {
                to_send.push((&update.key, value, key_version.expect("get key version")));
            } else {
                to_send.push((&update.key, value, TableKey::KEY_NO_VERSION));
            }
        }

        let result = table_synchronizer.table_map.insert_conditionally_all(to_send).await;
        match result {
            Err(TableError::IncorrectKeyVersion { operation, error_msg }) => {
                debug!("IncorrectKeyVersion {}, {}", operation, error_msg);
                table_synchronizer.fetch_updates().await.expect("fetch update");
            }
            Err(TableError::KeyDoesNotExist { operation, error_msg }) => {
                debug!("KeyDoesNotExist {}, {}", operation, error_msg);
                table_synchronizer.fetch_updates().await.expect("fetch update");
            }
            Err(e) => {
                debug!("Error message is {}", e);
                return Err(e);
            }
            Ok(res) => {
                apply_inserts_to_localmap(to_update, res, table_synchronizer);
                break;
            }
        }
    }
    Ok(())
}

async fn conditionally_remove<K>(
    mut delete_generator: impl FnMut(&mut Table<K>),
    table_synchronizer: &mut TableSynchronizer<'_, K>,
) -> Result<(), TableError>
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    loop {
        let map = table_synchronizer.get_current_map();
        let mut to_delete = Table {
            map,
            insert: Vec::new(),
            remove: Vec::new(),
        };
        delete_generator(&mut to_delete);

        if to_delete.remove_is_empty() {
            debug!(
                "Conditionally remove to {} completed, as there is nothing to remove for map",
                table_synchronizer.get_name()
            );
            break;
        }

        let mut send = Vec::new();
        for delete in to_delete.get_remove_iter() {
            let key_version = table_synchronizer
                .get_key_version(delete)
                .expect("get key version");
            send.push((delete, key_version))
        }

        let result = table_synchronizer.table_map.remove_conditionally_all(send).await;

        match result {
            Err(TableError::IncorrectKeyVersion { operation, error_msg }) => {
                debug!("IncorrectKeyVersion {}, {}", operation, error_msg);
                table_synchronizer.fetch_updates().await.expect("fetch update");
            }
            Err(TableError::KeyDoesNotExist { operation, error_msg }) => {
                debug!("KeyDoesNotExist {}, {}", operation, error_msg);
                table_synchronizer.fetch_updates().await.expect("fetch update");
            }
            Err(e) => {
                debug!("Error message is {}", e);
                return Err(e);
            }
            Ok(()) => {
                apply_deletes_to_localmap(to_delete, table_synchronizer);
                break;
            }
        }
    }
    Ok(())
}

fn apply_inserts_to_localmap<K>(
    to_update: Table<K>,
    new_version: Vec<Version>,
    table_synchronizer: &mut TableSynchronizer<'_, K>,
) where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    let mut i = 0;
    for update in to_update.get_insert_iter() {
        let new_key = Key {
            key: update.key.clone(),
            key_version: *new_version.get(i).expect("get new version"),
        };
        let new_value = to_update.map.get(&update.key).expect("get the Value").clone();
        table_synchronizer.in_memory_map.insert(new_key, new_value);
        i += 1;
    }
    debug!("Updates {} entries in local map ", i);
}

fn apply_deletes_to_localmap<K>(to_delete: Table<K>, table_synchronizer: &mut TableSynchronizer<K>)
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    let mut i = 0;
    for delete in to_delete.get_remove_iter() {
        let delete_key = Key {
            key: delete.clone(),
            key_version: TableKey::KEY_NO_VERSION,
        };
        table_synchronizer.in_memory_map.remove(&delete_key);
        i += 1;
    }
    debug!("Deletes {} entries in local map ", i);
}

#[cfg(test)]
mod test {
    use super::Key;
    use crate::table_synchronizer::{deserialize_from, Table};
    use crate::table_synchronizer::{serialize, Insert, Value};
    use bincode2::{deserialize_from as bincode_deserialize, serialize as bincode_serialize};
    use std::collections::HashMap;

    #[test]
    fn test_insert_keys() {
        let mut map: HashMap<Key<String>, Value> = HashMap::new();
        let key1 = Key {
            key: "a".to_string(),
            key_version: 0,
        };
        let data = serialize(&"value".to_string()).expect("serialize");
        let value1 = Value {
            type_id: "String".into(),
            data,
        };

        let key2 = Key {
            key: "b".to_string(),
            key_version: 0,
        };

        let data = serialize(&1).expect("serialize");
        let value2 = Value {
            type_id: "i32".into(),
            data,
        };
        let result = map.insert(key1, value1);
        assert!(result.is_none());
        let result = map.insert(key2, value2);
        assert!(result.is_none());
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_insert_key_with_different_key_version() {
        let mut map: HashMap<Key<String>, Value> = HashMap::new();
        let key1 = Key {
            key: "a".to_string(),
            key_version: 0,
        };

        let data = serialize(&"value".to_string()).expect("serialize");
        let value1 = Value {
            type_id: "String".into(),
            data,
        };
        let key2 = Key {
            key: "a".to_string(),
            key_version: 1,
        };
        let data = serialize(&1).expect("serialize");
        let value2 = Value {
            type_id: "i32".into(),
            data,
        };

        let result = map.insert(key1.clone(), value1);
        assert!(result.is_none());
        let result = map.insert(key2.clone(), value2);
        assert!(result.is_some());
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_clone_map() {
        let mut map: HashMap<Key<String>, Value> = HashMap::new();
        let key1 = Key {
            key: "a".to_string(),
            key_version: 0,
        };

        let data = serialize(&"value".to_string()).expect("serialize");
        let value1 = Value {
            type_id: "String".into(),
            data,
        };

        let key2 = Key {
            key: "a".to_string(),
            key_version: 1,
        };

        let data = serialize(&1).expect("serialize");
        let value2 = Value {
            type_id: "i32".into(),
            data,
        };

        map.insert(key1.clone(), value1.clone());
        map.insert(key2.clone(), value2.clone());
        let new_map = map.clone();
        let result = new_map.get(&key1).expect("get value");
        assert_eq!(new_map.len(), 1);
        assert_eq!(result.clone(), value2);
    }

    #[test]
    fn test_insert_and_get() {
        let mut table = Table{
            map: HashMap::new(),
            insert: Vec::new(),
            remove: Vec::new(),
        };
        table.insert("test".into_string(), "i32".into_string(),Box::new(1));
        let value = table.get(&"test".into_string()).expect("get value");
        let deserialized_data: i32 = deserialize_from(&value.data).expect("deserialize");
        assert_eq!(deserialized_data, 1);
    }
}
