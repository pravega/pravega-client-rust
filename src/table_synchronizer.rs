use std::collections::HashMap;
use crate::tablemap::{TableMap, TableError, Version};
use crate::client_factory::ClientFactoryInternal;
use std::cmp::{PartialEq, Eq};
use std::hash::{Hash, Hasher};
use tracing::{debug};
use serde::de::DeserializeOwned;
use futures::stream::StreamExt;
use futures::pin_mut;
use std::marker::{Sized, Unpin};
use std::fmt::{Debug};
use std::clone::Clone;
use serde_cbor::ser::Serializer as CborSerializer;
use pravega_wire_protocol::commands::TableKey;
use serde::{Deserialize, Serialize};

/// The trait bound for the Key. The Key should
/// support Debug, Equal Hash, Clone, Serialize and DeserialzeOwned.
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Value {
    type_code: ValueType,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValueType{
    Integer,
    Long,
    Float,
    Double,
    String,
}

pub struct UpdateOrInsert<K>
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone
{
    pub key: Key<K>,
    pub type_code: ValueType,
    pub new_value: Box<dyn ValueData>,
}

pub struct Remove<K>
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone
{
    pub key: Key<K>
}

/// The trait bound for the ValueData.
pub trait ValueData: ValueSerialize + ValueDeserialize + ValueClone + Debug {}

impl<T> ValueData for T where T: 'static + Serialize + DeserializeOwned + Clone + Debug {}

/// Clone trait helper.
pub trait ValueClone  {
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


/// Serialize trait helper, we need to serialize the ValueData.
pub trait ValueSerialize {
    fn serialize_value(&self, seralizer: &mut CborSerializer<&mut Vec<u8>>) -> Result<(), serde_cbor::error::Error>;
}

impl<T> ValueSerialize for T
where
    T: Serialize
{
    fn serialize_value(&self, serializer: &mut CborSerializer<&mut Vec<u8>>) -> Result<(), serde_cbor::error::Error> {
        self.serialize(serializer)

    }
}

/// Deserialize Trait Helper.
pub trait ValueDeserialize {
    fn deserialize_value(reader: &[u8]) -> Result<Self, serde_cbor::error::Error> where Self:Sized;
}

impl<T> ValueDeserialize for T
where
    T: DeserializeOwned + Debug,
{
    fn deserialize_value(reader: &[u8]) -> Result<Self, serde_cbor::error::Error>
    {
        serde_cbor::from_slice(reader)
    }
}

/// Serialize the ValueSerialize into an Vec<u8> by using the CborSerializer.
pub fn serialize(value: &dyn ValueData) -> Result<Vec<u8>, serde_cbor::error::Error>
{
    let mut vec = Vec::new();
    value.serialize_value(&mut CborSerializer::new(&mut vec))?;
    Ok(vec)
}

/// Deserialize the [u8] to Box<dyn Value> by using the CborDserializer.
pub fn deserialize_from<T>(reader: &[u8]) -> Result<T, serde_cbor::error::Error>
where
    T: ValueData
{
    ValueDeserialize::deserialize_value(reader)
}


pub struct TableSynchronizer<'a, K>
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone
{
    name: String,
    table_maps: TableMap<'a>,
    in_memory_map: HashMap<Key<K>, Value>,
}


impl<'a, K> TableSynchronizer<'a, K>
where
    K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone
{
    pub async fn new(name: String, factory: &'a ClientFactoryInternal)  -> TableSynchronizer<'a, K> {
        TableSynchronizer{
            name: name.clone(),
            table_maps: TableMap::new(name.clone(), factory).await.expect("create table map"),
            in_memory_map: HashMap::new(),
        }
    }

    /// Gets the map object currently held in memory.
    /// This is a non-blocking call.
    /// used
    fn get_current_map(&self) -> HashMap<Key<K>, Box<dyn ValueData>> {
        let mut result = HashMap::new();
        for (key, value) in self.in_memory_map.iter() {
            let new_value = deserialize_value(value);
            result.insert(key.clone(), new_value);
        }
        result
    }

    ///Gets the name of the table_synchronizer, the name is same as the stream name.
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    ///Gets the current map
    /// Gets the Value of the GivenKey,
    /// This is a non-blocking call.
    pub fn get(&self, key: K) -> Option<Box<dyn ValueData>> {
        let search_key = Key {
            key,
            key_version: TableKey::KEY_NO_VERSION,
        };
        let value = self.in_memory_map.get(&search_key);

        if let Some(data) = value  {
            let result = deserialize_value(data);
            Some(result)
        } else {
            None
        }
    }

    /// Gets the Key Value Pair of the GivenKey,
    /// This is a non-blocking call.
    pub fn get_key_value(&self, key: K) -> Option<(Key<K>, Box<dyn ValueData>)> {
        let search_key = Key {
            key,
            key_version: TableKey::KEY_NO_VERSION,
        };
        let entry  = self.in_memory_map.get_key_value(&search_key);

        if let Some(data) = entry {
            let result = deserialize_value(data.1);
            Some((data.0.clone(), result))
        } else {
            None
        }
    }

    pub async fn fetch_updates(&mut self) -> Result<(), TableError> {
        debug!("fetch the latest map and apply to the local map");
        let reply = self.table_maps.read_entries_stream(3);
        pin_mut!(reply);
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
                        error_msg: "fetch entries error".to_string()
                    });
                }
            }
        }
        debug!("finish fetch updates, now the map has {} entries", entry_count);
        Ok(())
    }

    /// Insert/Updates a list of keys and applies it atomically.
    pub async fn insert_map_conditionally(&mut self, updates_generator: impl FnMut(HashMap<Key<K>, Box<dyn ValueData>>)
        -> Vec<UpdateOrInsert<K>>) -> Result<(), TableError> {
        conditionally_write(updates_generator, self).await
    }

    /// Remove a list of keys unconditionally.
    pub async fn remove_map_conditionally(&mut self, deletes_generateor: impl FnMut(HashMap<Key<K>, Box<dyn ValueData>>)
        -> Vec<Remove<K>>) -> Result<(), TableError> {
        conditionally_remove(deletes_generateor, self).await
    }

}

async fn conditionally_write<K>(mut updates_generator: impl FnMut(HashMap<Key<K>, Box<dyn ValueData>>) -> Vec<UpdateOrInsert<K>>, table_synchronizer: &mut TableSynchronizer<'_, K>) -> Result<(), TableError>
    where
        K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    loop {
        let map = table_synchronizer.get_current_map();
        let to_update = updates_generator(map);
        if to_update.len() == 0 {
            debug!("Conditionally Write to {} completed, as there is nothing to update for map",
                   table_synchronizer.get_name());
            break;
        }
        let mut to_send = Vec::new();
        for update in to_update.iter() {
            let data = serialize(&*update.new_value).expect("serialize value");
            let value = Value {
                type_code: update.type_code.clone(),
                data,
            };
            to_send.push((update.key.key.clone(), value, update.key.key_version))
        }
        let send = to_send.iter().map(|x| (&x.0, &x.1, x.2)).rev().collect();
        let result = table_synchronizer.table_maps.insert_conditionally_all(send).await;
        match result {
            Err(e) => {
                match e {
                    TableError::IncorrectKeyVersion{
                        operation: _, error_msg: _,
                    } => {
                        table_synchronizer.fetch_updates().await.expect("fetch update");
                    }
                    TableError::KeyDoesNotExist{
                        operation: _, error_msg: _,
                    } => {
                        table_synchronizer.fetch_updates().await.expect("fetch update");
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

fn apply_updates_to_localmap<K>(to_update: Vec<UpdateOrInsert<K>>, new_version: Vec<Version>, table_synchronizer: &mut TableSynchronizer<'_, K>)
    where
        K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    let mut i = 0;
    for update in to_update {
        let new_key = Key{
            key: update.key.key,
            key_version: new_version.get(i).expect("get new version").clone(),
        };
        let new_value = Value {
            type_code: update.type_code,
            data: serialize(&*update.new_value).expect("serialize value"),
        };
        table_synchronizer.in_memory_map.insert(new_key, new_value);
        i += 1;
    }
    debug!("Updates {} entries in local map ", i);
}

async fn conditionally_remove<K>(mut delete_generator: impl FnMut(HashMap<Key<K>, Box<dyn ValueData>>) -> Vec<Remove<K>>, table_synchronizer: &mut TableSynchronizer<'_, K>) -> Result<(), TableError>
    where
        K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    loop {
        let map = table_synchronizer.get_current_map();
        let to_delete = delete_generator(map);
        if to_delete.len() == 0 {
            debug!("Conditionally remove to {} completed, as there is nothing to remove for map",
                   table_synchronizer.get_name());
            break;
        }

        let vec = to_delete.iter().map(|x| (&x.key.key, x.key.key_version)).rev().collect();

        let result = table_synchronizer.table_maps.remove_conditionally_all(vec).await;
        match result {
            Err(e) => {
                match e {
                    TableError::IncorrectKeyVersion{
                        operation: _, error_msg: _,
                    } => {
                        table_synchronizer.fetch_updates().await.expect("fetch update");
                    }
                    TableError::KeyDoesNotExist{
                        operation: _, error_msg: _,
                    } => {
                        table_synchronizer.fetch_updates().await.expect("fetch update");
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
        K: Serialize + DeserializeOwned + Unpin + Debug + Eq + Hash + PartialEq + Clone,
{
    let mut i = 0;
    for delete in to_delete {
        table_synchronizer.in_memory_map.remove(&delete.key);
        i += 1;
    }
    debug!("Deletes {} entries in local map ", i);
}

/// If uses type information to choose the type to deserialize,
/// that means the Value could not support struct type.
/// But do we need to support struct is a thing need to carefully consider.
/// The scenario is if one user create a new struct and insert into map,
/// how does another user know this struct?
fn deserialize_value(value: &Value) -> Box<dyn ValueData>
{
    match value.type_code {
        ValueType::Integer => {
            let data: i32 = deserialize_from(&value.data).expect("deserialize i32");
            Box::new(data)
        }

        ValueType::Long => {
            let data: i64 = deserialize_from(&value.data).expect("deserialize i64");
            Box::new(data)
        }

        ValueType::Float => {
            let data: f32 = deserialize_from(&value.data).expect("deserialize f32");
            Box::new(data)
        }

        ValueType::Double => {
            let data: f64 = deserialize_from(&value.data).expect("deserializer f64");
            Box::new(data)
        }

        ValueType::String => {
            let data: String = deserialize_from(&value.data).expect("deserialize String");
            Box::new(data)
        }
    }
}

#[cfg(test)]
mod test {
    use super::Key;
    use std::collections::HashMap;
    use crate::table_synchronizer::{serialize, UpdateOrInsert, ValueType, Value};
    use crate::table_synchronizer::{deserialize_from, deserialize_value};
    use bincode2::{deserialize_from as bincode_deserialize, serialize as bincode_serialize};
    #[test]
    fn test_insert_keys() {
        let mut map: HashMap<Key<String>, i32> = HashMap::new();
        let key1 = Key {
            key: "a".to_string(),
            key_version: 0,
        };
        let value1 = 1;

        let key2 = Key {
            key: "b".to_string(),
            key_version: 0,
        };
        let value2 = 2;
        let result = map.insert(key1, value1);
        assert!(result.is_none());
        let result = map.insert(key2, value2);
        assert!(result.is_none());
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_insert_key_with_different_key_version() {
        let mut map: HashMap<Key<String>, i32> = HashMap::new();
        let key1 = Key {
            key: "a".to_string(),
            key_version: 0,
        };
        let value1 = 1;

        let key2 = Key {
            key: "a".to_string(),
            key_version: 1,
        };
        let value2 = 2;
        let result = map.insert(key1, value1);
        assert!(result.is_none());
        let result = map.insert(key2.clone(), value2);
        assert!(result.is_some());
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_clone_map() {
        let mut map: HashMap<Key<String>, i32> = HashMap::new();
        let key1 = Key {
            key: "a".to_string(),
            key_version: 0,
        };
        let value1 = 1;

        let key2 = Key {
            key: "a".to_string(),
            key_version: 1,
        };
        let value2 =2;
        map.insert(key1.clone(), value1);
        map.insert(key2.clone(), value2);
        let new_map = map.clone();
        let result = new_map.get(&key1);
        assert!(result.is_some());
        let result = new_map.get(&key2);
        assert!(result.is_some());
    }

    #[test]
    fn test_serialize_deserialize_value() {
        let value1 = "1".to_string();
        let value2 = 2;
        let value3 = vec![1, 2, 3];

        let t = serialize(&value1).expect("serialize value1");
        //At here we must define the type. (The deserializer must know the type to deserialize)
        let result: String = deserialize_from(&t).expect("deserialize value");
        assert_eq!(value1, result);

        let t  = serialize(&value2).expect("serialize value2");
        let result: i32 = deserialize_from(&t).expect("deserialize value");
        assert_eq!(value2, result);

        let t  = serialize(&value3).expect("serialize value2");
        let result: Vec<i32> = deserialize_from(&t).expect("deserialize value");
        assert_eq!(value3, result);
    }

    #[test]
    fn test_update_or_insert() {
        let key = Key {
            key: "a".to_string(),
            key_version: 0,
        };
        let insert = UpdateOrInsert {
            key: key.clone(),
            type_code: ValueType::Integer,
            new_value: Box::new(1)
        };
        let data = serialize(&*insert.new_value).expect("serialize value");
        let value = Value {
            type_code: insert.type_code,
            data
        };
        // mock serialize/deserialize in table_map
        let serialized_key = bincode_serialize(&key.key).expect("serialize key");
        let serialized_value = bincode_serialize(&value).expect("serialize value");

        let deserialized_k: String = bincode_deserialize(serialized_key.as_slice()).expect("deserialize key");
        assert_eq!(deserialized_k, key.key);
        let deserialized_v: Value = bincode_deserialize(serialized_value.as_slice()).expect("deserialize value");
        let deserialize_data = deserialize_value(&deserialized_v);
        let data = format!("{:?}", deserialize_data);
        assert_eq!(data, String::from("1"));
    }
 }