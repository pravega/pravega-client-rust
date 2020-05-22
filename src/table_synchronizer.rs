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
use tracing::debug;

/// Provides a means to have map that is synchronized between many processes.
/// The pattern is to have a map that can be update by Insert or Remove,
/// Each host can perform logic based on its in_memory map and apply updates by supplying a
/// function to create Insert/Remove objects.
/// Update from other hosts can be obtained by calling fetchUpdate().

/// The Key struct in the in_memory map. it contains two fields, the key and key_version.
/// key should be serialized and deserialized.
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

/// The Value struct in the in_memory map. It contains three fields.
/// type_id of the data. It is used for deserialize data between different process.
/// type_version of the data.
/// data: the Value data after serialized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Value {
    type_id: u64,
    data: Vec<u8>,
}

/// The Insert struct. It contains three fields.
/// The key to insert or update, the type_id for the data, and the data.
pub struct Insert<K>
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    pub key: K,
    pub type_id: u64,
    pub new_value: Box<dyn ValueData>,
}

/// The key they wants to remove.
pub struct Remove<K>
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    pub key: K,
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

/// Serialize trait helper, we need to serialize the ValueData in Insert into Vec<u8>.
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
/// This method would be used by the update method in table_synchronizer.
pub(crate) fn serialize(value: &dyn ValueData) -> Result<Vec<u8>, serde_cbor::error::Error> {
    let mut vec = Vec::new();
    value.serialize_value(&mut CborSerializer::new(&mut vec))?;
    Ok(vec)
}

/// Deserialize the Value into the type T bt using cbor deserializer.
pub fn deserialize_from<T>(reader: &[u8]) -> Result<T, serde_cbor::error::Error>
where
    T: DeserializeOwned,
{
    serde_cbor::de::from_slice(reader)
}

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
    pub(crate) fn get_current_map(&self) -> HashMap<K, Value> {
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

    /// Gets the Value of the GivenKey,
    /// This is a non-blocking call.
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

    pub async fn fetch_updates(&mut self) -> Result<(), TableError> {
        debug!("fetch the latest map and apply to the local map");
        let reply = self.table_map.read_entries_stream(3);
        pin_mut!(reply);
        self.in_memory_map.clear();
        let mut entry_count: i32 = 0;
        while let Some(entry) = reply.next().await {
            match entry {
                Ok(t) => {
                    let k = t.0;
                    let v = t.1;
                    let version: Version = t.2;
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
        debug!("finish fetch updates, now the map has {} entries", entry_count);
        Ok(())
    }

    /// Insert/Updates a list of keys and applies it atomically.
    pub async fn insert_conditionally(
        &mut self,
        updates_generator: impl FnMut(HashMap<K, Value>) -> Vec<Insert<K>>,
    ) -> Result<(), TableError> {
        conditionally_write(updates_generator, self).await
    }

    /// Remove a list of keys unconditionally.
    pub async fn remove_conditionally(
        &mut self,
        deletes_generateor: impl FnMut(HashMap<K, Value>) -> Vec<Remove<K>>,
    ) -> Result<(), TableError> {
        conditionally_remove(deletes_generateor, self).await
    }
}

async fn conditionally_write<K>(
    mut updates_generator: impl FnMut(HashMap<K, Value>) -> Vec<Insert<K>>,
    table_synchronizer: &mut TableSynchronizer<'_, K>,
) -> Result<(), TableError>
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    loop {
        let map = table_synchronizer.get_current_map();
        let to_update = updates_generator(map);
        if to_update.len() == 0 {
            debug!(
                "Conditionally Write to {} completed, as there is nothing to update for map",
                table_synchronizer.get_name()
            );
            break;
        }
        let mut to_send = Vec::new();
        for update in to_update.iter() {
            let data = serialize(&*update.new_value).expect("serialize value");
            let value = Value {
                type_id: update.type_id,
                data,
            };
            let key_version = table_synchronizer.get_key_version(&update.key);

            if key_version.is_some() {
                to_send.push((update.key.clone(), value, key_version.expect("get key version")));
            } else {
                to_send.push((update.key.clone(), value, TableKey::KEY_NO_VERSION));
            }
        }
        let send = to_send.iter().map(|x| (&x.0, &x.1, x.2)).rev().collect();
        let result = table_synchronizer.table_map.insert_conditionally_all(send).await;
        match result {
            Err(e) => match e {
                TableError::IncorrectKeyVersion {
                    operation: _,
                    error_msg: _,
                } => {
                    table_synchronizer.fetch_updates().await.expect("fetch update");
                }
                TableError::KeyDoesNotExist {
                    operation: _,
                    error_msg: _,
                } => {
                    table_synchronizer.fetch_updates().await.expect("fetch update");
                }
                _ => {
                    return Err(e);
                }
            },
            Ok(res) => {
                apply_updates_to_localmap(to_update, res, table_synchronizer);
            }
        }
    }
    Ok(())
}

fn apply_updates_to_localmap<K>(
    to_update: Vec<Insert<K>>,
    new_version: Vec<Version>,
    table_synchronizer: &mut TableSynchronizer<'_, K>,
) where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    let mut i = 0;
    for update in to_update {
        let new_key = Key {
            key: update.key,
            key_version: new_version.get(i).expect("get new version").clone(),
        };
        let new_value = Value {
            type_id: update.type_id,
            data: serialize(&*update.new_value).expect("serialize value"),
        };
        table_synchronizer.in_memory_map.insert(new_key, new_value);
        i += 1;
    }
    debug!("Updates {} entries in local map ", i);
}

async fn conditionally_remove<K>(
    mut delete_generator: impl FnMut(HashMap<K, Value>) -> Vec<Remove<K>>,
    table_synchronizer: &mut TableSynchronizer<'_, K>,
) -> Result<(), TableError>
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    loop {
        let map = table_synchronizer.get_current_map();
        let to_delete = delete_generator(map);
        if to_delete.len() == 0 {
            debug!(
                "Conditionally remove to {} completed, as there is nothing to remove for map",
                table_synchronizer.get_name()
            );
            break;
        }
        let mut to_send = Vec::new();
        for delete in to_delete.iter() {
            let key_version = table_synchronizer
                .get_key_version(&delete.key)
                .expect("get key version");
            to_send.push((&delete.key, key_version))
        }

        let result = table_synchronizer
            .table_map
            .remove_conditionally_all(to_send)
            .await;
        match result {
            Err(e) => match e {
                TableError::IncorrectKeyVersion {
                    operation: _,
                    error_msg: _,
                } => {
                    table_synchronizer.fetch_updates().await.expect("fetch update");
                }
                TableError::KeyDoesNotExist {
                    operation: _,
                    error_msg: _,
                } => {
                    table_synchronizer.fetch_updates().await.expect("fetch update");
                }
                _ => {
                    return Err(e);
                }
            },
            Ok(()) => {
                apply_deletes_to_localmap(to_delete, table_synchronizer);
            }
        }
    }
    Ok(())
}

fn apply_deletes_to_localmap<K>(to_delete: Vec<Remove<K>>, table_synchronizer: &mut TableSynchronizer<K>)
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    let mut i = 0;
    for delete in to_delete {
        let delete_key = Key {
            key: delete.key,
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
    use crate::table_synchronizer::deserialize_from;
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
        let value1 = Value { type_id: 1, data };

        let key2 = Key {
            key: "b".to_string(),
            key_version: 0,
        };

        let data = serialize(&1).expect("serialize");
        let value2 = Value { type_id: 2, data };
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
        let value1 = Value { type_id: 1, data };
        let key2 = Key {
            key: "a".to_string(),
            key_version: 1,
        };
        let data = serialize(&1).expect("serialize");
        let value2 = Value { type_id: 2, data };

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
        let value1 = Value { type_id: 1, data };

        let key2 = Key {
            key: "a".to_string(),
            key_version: 1,
        };

        let data = serialize(&1).expect("serialize");
        let value2 = Value { type_id: 2, data };

        map.insert(key1.clone(), value1.clone());
        map.insert(key2.clone(), value2.clone());
        let new_map = map.clone();
        let result = new_map.get(&key1).expect("get value");
        assert_eq!(new_map.len(), 1);
        assert_eq!(result.clone(), value2);
    }

    #[test]
    fn test_insert_and_get() {
        let mut map: HashMap<Key<String>, Value> = HashMap::new();
        let insert = Insert {
            key: "a".to_string(),
            type_id: 1,
            new_value: Box::new(1),
        };

        // mock the insertion of local map.
        let key = Key {
            key: insert.key,
            key_version: 0,
        };
        let data = serialize(&*insert.new_value).expect("serialize value");
        let value = Value {
            type_id: insert.type_id,
            data,
        };

        map.insert(key, value);

        let insert = Insert {
            key: "b".to_string(),
            type_id: 2,
            new_value: Box::new("abc".to_string()),
        };
        let key = Key {
            key: insert.key,
            key_version: 0,
        };
        let data = serialize(&*insert.new_value).expect("serialize value");
        let value = Value {
            type_id: insert.type_id,
            data,
        };

        map.insert(key.clone(), value);
        let value = map.get(&key).expect("get the value");
        let deserialized_data: String = deserialize_from(&value.data).expect("deserialize");
        assert_eq!(deserialized_data, "abc".to_string());
    }

    #[test]
    fn test_insert_and_get_with_serialize() {
        let insert = Insert {
            key: "a".to_string(),
            type_id: 1,
            new_value: Box::new(1),
        };

        let data = serialize(&*insert.new_value).expect("serialize value");
        let value = Value {
            type_id: insert.type_id,
            data,
        };

        // mock the send in table_segment(it uses bincode2 serialize/deserialize);
        let serialized_key = bincode_serialize(&insert.key).expect("serialize key");
        let serialized_value = bincode_serialize(&value).expect("serialize value");

        // mock the received in table_segment.
        let deserialized_key: String =
            bincode_deserialize(serialized_key.as_slice()).expect("deserialize key");
        let deserialized_value: Value =
            bincode_deserialize(serialized_value.as_slice()).expect("deserialize value");
        assert_eq!(deserialized_key, insert.key);
        assert_eq!(deserialized_value, value);
    }
}
