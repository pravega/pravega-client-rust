//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactory;
use crate::error::*;
use crate::tablemap::{TableError, TableMap, Version};
use futures::pin_mut;
use futures::stream::StreamExt;
use pravega_wire_protocol::commands::TableKey;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_cbor::ser::Serializer as CborSerializer;
use serde_cbor::to_vec;
use std::clone::Clone;
use std::cmp::{Eq, PartialEq};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::slice::Iter;
use std::time::Duration;
use tokio::time::delay_for;
use tracing::debug;

/// Provides a map that is synchronized across different processes.
/// The pattern is to have a map that can be updated by using Insert or Remove.
/// Each process can perform logic based on its in memory map and apply updates by supplying a
/// function to create Insert/Remove objects.
/// Updates from other processes can be obtained by calling fetchUpdates().
pub struct TableSynchronizer {
    /// The name of the TableSynchronizer. This is also the name of the stream name of table segment.
    /// Different instances of TableSynchronizer with same name will point to the same table segment.
    name: String,

    /// TableMap is the table segment client.
    table_map: TableMap,

    /// in_memory_map is a two-level nested hash map that uses two keys to identify a value.
    /// The reason to make it a nested map is that the actual data structures shared across
    /// different processes are often more complex than a simple hash map. The problem of using a
    /// simple hash map to model a complex data structure is that the key will be coarse-grained
    /// and every update will incur a lot of overhead.
    /// A two-level hash map is fine for now, maybe it will need more nested layers in the future.
    in_memory_map: HashMap<String, HashMap<Key, Value>>,

    /// in_memory_map_version is used to monitor the versions of each second level hash maps.
    /// The idea is to monitor the changes of the second level hash maps besides individual keys
    /// since some logic may depend on a certain map not being changed during an update.
    in_memory_map_version: HashMap<Key, Value>,

    /// An offset that is used to make conditional updates.
    table_segment_offset: i64,

    /// The latest fetch position on the server side.
    fetch_position: i64,
}

// Max number of retries by the table synchronizer in case of a failure.
const MAX_RETRIES: i32 = 10;
// Wait until next attempt.
const DELAY_MILLIS: u64 = 1000;

impl TableSynchronizer {
    pub async fn new(name: String, factory: ClientFactory) -> TableSynchronizer {
        let table_map = TableMap::new(name.clone(), factory)
            .await
            .expect("create table map");
        TableSynchronizer {
            name: name.clone(),
            table_map,
            in_memory_map: HashMap::new(),
            in_memory_map_version: HashMap::new(),
            table_segment_offset: -1,
            fetch_position: 0,
        }
    }

    /// Gets the outer map currently held in memory.
    /// The return type does not contain the version information.
    pub fn get_outer_map(&self) -> HashMap<String, HashMap<String, Value>> {
        self.in_memory_map
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.iter()
                        .filter(|(_k2, v2)| v2.type_id != TOMBSTONE)
                        .map(|(k2, v2)| (k2.key.clone(), v2.clone()))
                        .collect::<HashMap<String, Value>>(),
                )
            })
            .collect()
    }

    /// Gets the inner map currently held in memory.
    /// The return type does not contain the version information.
    pub fn get_inner_map(&self, outer_key: &str) -> HashMap<String, Value> {
        self.in_memory_map
            .get(outer_key)
            .map_or_else(HashMap::new, |inner| {
                inner
                    .iter()
                    .filter(|(_k, v)| v.type_id != TOMBSTONE)
                    .map(|(k, v)| (k.key.clone(), v.clone()))
                    .collect::<HashMap<String, Value>>()
            })
    }

    fn get_inner_map_version(&self) -> HashMap<String, Value> {
        self.in_memory_map_version
            .iter()
            .map(|(k, v)| (k.key.clone(), v.clone()))
            .collect()
    }

    /// Gets the name of the table_synchronizer, the name is the same as the stream name.
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// Gets the Value associated with the map.
    /// This is a non-blocking call.
    /// The data in Value is not deserialized and the caller should call deserialize_from to deserialize.
    pub fn get(&self, outer_key: &str, inner_key: &str) -> Option<&Value> {
        let inner_map = self.in_memory_map.get(outer_key)?;

        let search_key_inner = Key {
            key: inner_key.to_owned(),
            key_version: TableKey::KEY_NO_VERSION,
        };

        inner_map.get(&search_key_inner).and_then(
            |val| {
                if val.type_id == TOMBSTONE {
                    None
                } else {
                    Some(val)
                }
            },
        )
    }

    /// Gets the Key version of the given key,
    /// This is a non-blocking call.
    pub fn get_key_version(&self, outer_key: &str, inner_key: &Option<String>) -> Version {
        if let Some(inner) = inner_key {
            let search_key = Key {
                key: inner.to_owned(),
                key_version: TableKey::KEY_NO_VERSION,
            };
            if let Some(inner_map) = self.in_memory_map.get(outer_key) {
                if let Some((key, _value)) = inner_map.get_key_value(&search_key) {
                    return key.key_version;
                }
            }
            TableKey::KEY_NOT_EXISTS
        } else {
            let search_key = Key {
                key: outer_key.to_owned(),
                key_version: TableKey::KEY_NO_VERSION,
            };
            if let Some((key, _value)) = self.in_memory_map_version.get_key_value(&search_key) {
                key.key_version
            } else {
                TableKey::KEY_NOT_EXISTS
            }
        }
    }

    /// Gets the key-value pair of given key,
    /// This is a non-blocking call.
    /// It will return a copy of the key-value pair.
    fn get_key_value(&self, outer_key: &str, inner_key: &str) -> Option<(String, Value)> {
        let inner_map = self.in_memory_map.get(outer_key)?;

        let search_key = Key {
            key: inner_key.to_owned(),
            key_version: TableKey::KEY_NO_VERSION,
        };

        if let Some((key, value)) = inner_map.get_key_value(&search_key) {
            Some((key.key.clone(), value.clone()))
        } else {
            None
        }
    }

    /// Fetches the latest map from remote server and applies it to the local map.
    pub async fn fetch_updates(&mut self) -> Result<i32, TableError> {
        debug!(
            "fetch the latest map and apply to the local map, fetch from position {}",
            self.fetch_position
        );
        let reply = self
            .table_map
            .read_entries_stream_from_position(10, self.fetch_position);
        pin_mut!(reply);

        let mut counter: i32 = 0;
        while let Some(entry) = reply.next().await {
            match entry {
                Ok((k, v, version, last_position)) => {
                    debug!("fetched key with version {}", version);
                    let internal_key = InternalKey { key: k };
                    let (outer_key, inner_key) = internal_key.split();

                    if let Some(inner) = inner_key {
                        // the key is a composite key, update the nested hashmap
                        let inner_map_key = Key {
                            key: inner,
                            key_version: version,
                        };
                        let inner_map = self.in_memory_map.entry(outer_key).or_insert_with(HashMap::new);

                        // this is necessary since insert will not update the Key
                        inner_map.remove(&inner_map_key);
                        inner_map.insert(inner_map_key, v);
                    } else {
                        // the key is an outer key, update the map version
                        let outer_map_key = Key {
                            key: outer_key,
                            key_version: version,
                        };
                        // this is necessary since insert will not update the Key
                        self.in_memory_map_version.remove(&outer_map_key.clone());
                        self.in_memory_map_version.insert(outer_map_key, v);
                    }
                    self.fetch_position = last_position;
                    counter += 1;
                }
                _ => {
                    return Err(TableError::OperationError {
                        operation: "fetch_updates".to_string(),
                        error_msg: "fetch entries error".to_string(),
                    });
                }
            }
        }
        debug!("finished fetching updates");
        Ok(counter)
    }

    /// Inserts/Updates a list of keys and applies it atomically to local map.
    /// This will update the local_map to the latest version.
    pub async fn insert(
        &mut self,
        updates_generator: impl FnMut(&mut Table) -> Result<Option<String>, SynchronizerError>,
    ) -> Result<Option<String>, SynchronizerError> {
        conditionally_write(updates_generator, self, MAX_RETRIES).await
    }

    /// Removes a list of keys and applies it atomically to local map.
    /// This will update the local_map to latest version.
    pub async fn remove(
        &mut self,
        deletes_generator: impl FnMut(&mut Table) -> Result<Option<String>, SynchronizerError>,
    ) -> Result<Option<String>, SynchronizerError> {
        conditionally_remove(deletes_generator, self, MAX_RETRIES).await
    }
}

/// The Key struct in the in memory map. It contains two fields, the key and key_version.
/// The key_version is used for conditional update on server side. If the key_version is i64::MIN,
/// then the update will be unconditional.
#[derive(Debug, Clone)]
pub struct Key {
    pub key: String,
    pub key_version: Version,
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Key {}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state)
    }
}

const PREFIX_LENGTH: usize = 2;

/// This is used to parse the key received from the server.
struct InternalKey {
    pub key: String,
}

impl InternalKey {
    fn split(&self) -> (String, Option<String>) {
        let outer_name_length: usize = self.key[..PREFIX_LENGTH].parse().expect("parse prefix length");
        assert!(self.key.len() >= PREFIX_LENGTH + outer_name_length);

        if self.key.len() > PREFIX_LENGTH + outer_name_length {
            let outer = self.key[PREFIX_LENGTH..PREFIX_LENGTH + outer_name_length]
                .parse::<String>()
                .expect("parse outer key");
            // there is a slash separating outer_key and_inner key
            let inner = self.key[PREFIX_LENGTH + outer_name_length + 1..]
                .parse::<String>()
                .expect("parse inner key");
            (outer, Some(inner))
        } else {
            let outer = self.key[PREFIX_LENGTH..PREFIX_LENGTH + outer_name_length]
                .parse::<String>()
                .expect("parse outer key");
            (outer, None)
        }
    }
}

/// The Value struct in the in memory map. It contains two fields.
/// type_id: it is used by caller to figure out the exact type of the data.
/// data: the serialized Value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Value {
    pub type_id: String,
    pub data: Vec<u8>,
}

const TOMBSTONE: &str = "tombstone";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Tombstone {}

/// The Table struct contains a nested map and a version map, which are the same as in
/// table synchronizer but will be updated instantly when caller calls Insert or Remove method.
/// It is used to update the server side of table map and its updates will be applied to
/// table synchronizer once the updates are successfully stored on the server side.
pub struct Table {
    map: HashMap<String, HashMap<String, Value>>,
    map_version: HashMap<String, Value>,
    insert: Vec<Insert>,
    remove: Vec<Remove>,
}

impl Table {
    pub(crate) fn new(
        map: HashMap<String, HashMap<String, Value>>,
        map_version: HashMap<String, Value>,
        insert: Vec<Insert>,
        remove: Vec<Remove>,
    ) -> Self {
        Table {
            map,
            map_version,
            insert,
            remove,
        }
    }

    /// insert method needs an outer_key and an inner_key to find a value.
    /// It will update the map inside the Table.
    pub fn insert(
        &mut self,
        outer_key: String,
        inner_key: String,
        type_id: String,
        new_value: Box<dyn ValueData>,
    ) {
        let data = serialize(&*new_value).expect("serialize value");
        let insert = Insert::new(outer_key.clone(), Some(inner_key.clone()), type_id.clone());

        self.insert.push(insert);
        // also insert into map.
        let inner_map = self.map.entry(outer_key.clone()).or_insert_with(HashMap::new);
        inner_map.insert(inner_key, Value { type_id, data });

        // increment the version of the map, indicating that this map has changed
        self.increment_map_version(outer_key);
    }

    /// insert_tombstone method replaces the original value with a tombstone, which means that this
    /// key value pair is invalid and will be removed later. The reason of adding a tombstone is
    /// to guarantee the atomicity of a remove-and-insert operation.
    pub fn insert_tombstone(
        &mut self,
        outer_key: String,
        inner_key: String,
    ) -> Result<(), SynchronizerError> {
        let data = to_vec(&Tombstone {}).expect("serialize tombstone");
        let insert = Insert::new(outer_key.clone(), Some(inner_key.clone()), "tombstone".to_owned());

        self.insert.push(insert);

        let inner_map = self
            .map
            .get_mut(&outer_key)
            .ok_or(SynchronizerError::SyncTombstoneError {
                error_msg: format!("outer key {} does not exist", outer_key),
            })?;

        inner_map.get(&inner_key).map_or(
            Err(SynchronizerError::SyncTombstoneError {
                error_msg: format!("inner key {} does not exist", inner_key),
            }),
            |v| {
                if v.type_id == TOMBSTONE {
                    Err(SynchronizerError::SyncTombstoneError {
                        error_msg: format!(
                            "tombstone has already been added for key {}/{}",
                            outer_key, inner_key
                        ),
                    })
                } else {
                    Ok(())
                }
            },
        )?;

        inner_map.insert(
            inner_key.clone(),
            Value {
                type_id: TOMBSTONE.to_owned(),
                data,
            },
        );

        self.increment_map_version(outer_key.clone());

        // also push this key to remove list, this key will be removed after insert is completed.
        let remove = Remove::new(outer_key.clone(), inner_key);
        self.remove.push(remove);

        Ok(())
    }

    /// remove takes an outer_key and an inner_key and removes a particular entry.
    fn remove(&mut self, outer_key: String, inner_key: String) {
        //Also remove from the map.
        let inner_map = self.map.get_mut(&outer_key).expect("should contain outer key");
        inner_map.remove(&inner_key);

        let remove = Remove::new(outer_key.clone(), inner_key);
        self.remove.push(remove);
    }

    /// retain a specific map to make sure it's not altered by other processes when an update
    /// is being made that depends on it.
    /// Notice that this method only needs to be called when this dependent map is not being updated,
    /// since any modifications to a map on server side will use a version to make sure the update
    /// is based on the latest change.
    pub fn retain(&mut self, outer_key: String) {
        self.increment_map_version(outer_key);
    }

    /// get method will take an outer_key and an inner_key and return the valid value.
    /// It will not return value hinted by tombstone.
    pub fn get(&self, outer_key: &str, inner_key: &str) -> Option<&Value> {
        let inner_map = self.map.get(outer_key).expect("should contain outer key");
        inner_map
            .get(inner_key)
            .and_then(|val| if val.type_id == TOMBSTONE { None } else { Some(val) })
    }

    /// get_inner_map method will take an outer_key return the outer map.
    /// The returned outer map will not contain value hinted by tombstone.
    pub fn get_inner_map(&self, outer_key: &str) -> HashMap<String, Value> {
        self.map.get(outer_key).map_or_else(HashMap::new, |inner_map| {
            inner_map
                .iter()
                .filter(|(_k, v)| v.type_id != TOMBSTONE)
                .map(|(k, v)| (k.to_owned(), v.clone()))
                .collect::<HashMap<String, Value>>()
        })
    }

    fn is_tombstoned(&self, outer_key: &str, inner_key: &str) -> bool {
        self.map.get(outer_key).map_or(false, |inner_map| {
            inner_map
                .get(inner_key)
                .map_or(false, |val| val.type_id == TOMBSTONE)
        })
    }

    fn get_internal(&self, outer_key: &str, inner_key: &Option<String>) -> Option<&Value> {
        if let Some(inner) = inner_key {
            let inner_map = self.map.get(outer_key).expect("should contain outer key");
            inner_map.get(inner)
        } else {
            self.map_version.get(outer_key)
        }
    }

    /// Check if an inner key exists. The tombstoned value will return a false.
    pub fn contains_key(&self, outer_key: &str, inner_key: &str) -> bool {
        self.map.get(outer_key).map_or(false, |inner_map| {
            inner_map
                .get(inner_key)
                .map_or(false, |value| value.type_id != TOMBSTONE)
        })
    }

    /// Check if an outer_key exists. The tombstoned value will return a false.
    pub fn contains_outer_key(&self, outer_key: &str) -> bool {
        self.map.contains_key(outer_key)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn insert_is_empty(&self) -> bool {
        self.insert.is_empty()
    }

    fn remove_is_empty(&self) -> bool {
        self.remove.is_empty()
    }

    fn get_insert_iter(&self) -> Iter<Insert> {
        self.insert.iter()
    }

    fn get_remove_iter(&self) -> Iter<Remove> {
        self.remove.iter()
    }

    fn increment_map_version(&mut self, outer_key: String) {
        // the value is just a placeholder, the version information is stored in the Key.
        self.map_version.entry(outer_key.clone()).or_insert(Value {
            type_id: "blob".to_owned(),
            data: vec![0],
        });
        let insert = Insert::new(outer_key, None, "blob".to_owned());
        self.insert.push(insert);
    }
}

/// Insert struct is used internally to update the server side of map.
/// The outer_key and inner_key are combined to identify a value in the nested map.
/// The composite_key is derived from outer_key and inner_key, which is the actual key that's
/// stored on the server side.
/// The type_id is used to identify the type of the value in the map since the value
/// is just a serialized blob that does not contain any type information.
pub(crate) struct Insert {
    outer_key: String,
    inner_key: Option<String>,
    composite_key: String,
    type_id: String,
}

impl Insert {
    pub(crate) fn new(outer_key: String, inner_key: Option<String>, type_id: String) -> Self {
        let composite_key = if inner_key.is_some() {
            format!(
                "{:02}{}/{}",
                outer_key.len(),
                outer_key,
                inner_key.clone().expect("get inner key")
            )
        } else {
            format!("{:02}{}", outer_key.len(), outer_key)
        };

        Insert {
            outer_key,
            inner_key,
            composite_key,
            type_id,
        }
    }
}

/// The remove struct is used internally to remove a value from the server side of map.
/// Unlike the Insert struct, it does not need to have a type_id since we don't care about
/// the value.
pub(crate) struct Remove {
    outer_key: String,
    inner_key: String,
    composite_key: String,
}

impl Remove {
    pub(crate) fn new(outer_key: String, inner_key: String) -> Self {
        Remove {
            outer_key: outer_key.clone(),
            inner_key: inner_key.clone(),
            composite_key: format!("{:02}{}/{}", outer_key.len(), outer_key, inner_key),
        }
    }
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
/// This method would be used by the user after calling get() of table_synchronizer.
pub fn deserialize_from<T>(reader: &[u8]) -> Result<T, serde_cbor::error::Error>
where
    T: DeserializeOwned,
{
    serde_cbor::de::from_slice(reader)
}

async fn conditionally_write(
    mut updates_generator: impl FnMut(&mut Table) -> Result<Option<String>, SynchronizerError>,
    table_synchronizer: &mut TableSynchronizer,
    mut retry: i32,
) -> Result<Option<String>, SynchronizerError> {
    let mut update_result = None;

    while retry > 0 {
        let map = table_synchronizer.get_outer_map();
        let map_version = table_synchronizer.get_inner_map_version();

        let mut to_update = Table {
            map,
            map_version,
            insert: Vec::new(),
            remove: Vec::new(),
        };

        update_result = updates_generator(&mut to_update)?;
        debug!("number of insert is {}", to_update.insert.len());
        if to_update.insert_is_empty() {
            debug!(
                "Conditionally Write to {} completed, as there is nothing to update for map",
                table_synchronizer.get_name()
            );
            break;
        }

        let mut to_send = Vec::new();
        for update in to_update.get_insert_iter() {
            let value = to_update
                .get_internal(&update.outer_key, &update.inner_key)
                .expect("get the insert data");
            let key_version = table_synchronizer.get_key_version(&update.outer_key, &update.inner_key);

            to_send.push((&update.composite_key, value, key_version));
        }
        let result = table_synchronizer
            .table_map
            .insert_conditionally_all(to_send, table_synchronizer.table_segment_offset)
            .await;
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
                if retry > 0 {
                    retry -= 1;
                    delay_for(Duration::from_millis(DELAY_MILLIS)).await;
                } else {
                    return Err(SynchronizerError::SyncTableError {
                        operation: "insert conditionally_all".to_owned(),
                        source: e,
                    });
                }
            }
            Ok(res) => {
                apply_inserts_to_localmap(&mut to_update, res, table_synchronizer);
                clear_tombstone(&mut to_update, table_synchronizer).await?;
                break;
            }
        }
    }
    Ok(update_result)
}

async fn conditionally_remove(
    mut delete_generator: impl FnMut(&mut Table) -> Result<Option<String>, SynchronizerError>,
    table_synchronizer: &mut TableSynchronizer,
    mut retry: i32,
) -> Result<Option<String>, SynchronizerError> {
    let mut delete_result = None;

    while retry > 0 {
        let map = table_synchronizer.get_outer_map();
        let map_version = table_synchronizer.get_inner_map_version();

        let mut to_delete = Table {
            map,
            map_version,
            insert: Vec::new(),
            remove: Vec::new(),
        };
        delete_result = delete_generator(&mut to_delete)?;

        if to_delete.remove_is_empty() {
            debug!(
                "Conditionally remove to {} completed, as there is nothing to remove for map",
                table_synchronizer.get_name()
            );
            break;
        }

        let mut send = Vec::new();
        for delete in to_delete.get_remove_iter() {
            let key_version =
                table_synchronizer.get_key_version(&delete.outer_key, &Some(delete.inner_key.to_owned()));
            send.push((&delete.composite_key, key_version))
        }

        let result = table_synchronizer
            .table_map
            .remove_conditionally_all(send, table_synchronizer.table_segment_offset)
            .await;

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
                if retry > 0 {
                    retry -= 1;
                    delay_for(Duration::from_millis(DELAY_MILLIS)).await;
                } else {
                    return Err(SynchronizerError::SyncTableError {
                        operation: "remove conditionally_all".to_owned(),
                        source: e,
                    });
                }
            }
            Ok(()) => {
                apply_deletes_to_localmap(&mut to_delete, table_synchronizer);
                break;
            }
        }
    }
    Ok(delete_result)
}

async fn clear_tombstone(
    to_remove: &mut Table,
    table_synchronizer: &mut TableSynchronizer,
) -> Result<Option<String>, SynchronizerError> {
    table_synchronizer
        .remove(|table| {
            for remove in to_remove.get_remove_iter() {
                if table.is_tombstoned(&remove.outer_key, &remove.inner_key) {
                    table.remove(remove.outer_key.to_owned(), remove.inner_key.to_owned());
                }
            }
            Ok(None)
        })
        .await
}

fn apply_inserts_to_localmap(
    to_update: &mut Table,
    new_version: Vec<Version>,
    table_synchronizer: &mut TableSynchronizer,
) {
    let mut i = 0;
    for update in to_update.get_insert_iter() {
        if let Some(ref inner_key) = update.inner_key {
            let new_key = Key {
                key: inner_key.to_owned(),
                key_version: *new_version.get(i).expect("get new version"),
            };
            let inner_map = to_update.map.get(&update.outer_key).expect("get inner map");
            let new_value = inner_map.get(inner_key).expect("get the Value").clone();

            let in_mem_inner_map = table_synchronizer
                .in_memory_map
                .entry(update.outer_key.clone())
                .or_insert_with(HashMap::new);
            in_mem_inner_map.insert(new_key, new_value);
        } else {
            let new_key = Key {
                key: update.outer_key.to_owned(),
                key_version: *new_version.get(i).expect("get new version"),
            };
            let new_value = to_update
                .map_version
                .get(&update.outer_key)
                .expect("get the Value")
                .clone();
            table_synchronizer
                .in_memory_map_version
                .insert(new_key, new_value);
        }
        i += 1;
    }
    debug!("Updates {} entries in local map ", i);
}

fn apply_deletes_to_localmap(to_delete: &mut Table, table_synchronizer: &mut TableSynchronizer) {
    let mut i = 0;
    for delete in to_delete.get_remove_iter() {
        let delete_key = Key {
            key: delete.inner_key.clone(),
            key_version: TableKey::KEY_NO_VERSION,
        };
        let in_mem_inner_map = table_synchronizer
            .in_memory_map
            .entry(delete.outer_key.clone())
            .or_insert_with(HashMap::new);
        in_mem_inner_map.remove(&delete_key);
        i += 1;
    }
    debug!("Deletes {} entries in local map ", i);
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::table_synchronizer::{deserialize_from, Table};
    use crate::table_synchronizer::{serialize, Value};
    use pravega_client_config::connection_type::{ConnectionType, MockType};
    use pravega_client_config::ClientConfigBuilder;
    use pravega_client_shared::PravegaNodeUri;
    use std::collections::HashMap;
    use tokio::runtime::Runtime;

    #[test]
    fn test_intern_key_split() {
        let key1 = InternalKey {
            key: "10outer_keys/inner_key".to_owned(),
        };
        let (outer, inner) = key1.split();
        assert_eq!(outer, "outer_keys".to_owned());
        assert_eq!(inner.expect("should contain inner key"), "inner_key".to_owned());

        let key2 = InternalKey {
            key: "05outer/inner_key".to_owned(),
        };
        let (outer, inner) = key2.split();
        assert_eq!(outer, "outer".to_owned());
        assert_eq!(inner.expect("should contain inner key"), "inner_key".to_owned());

        let key3 = InternalKey {
            key: "05outer".to_owned(),
        };
        let (outer, inner) = key3.split();
        assert_eq!(outer, "outer".to_owned());
        assert!(inner.is_none());
    }

    #[test]
    fn test_insert_keys() {
        let mut map: HashMap<Key, Value> = HashMap::new();
        let key1 = Key {
            key: "a".to_owned(),
            key_version: 0,
        };
        let data = serialize(&"value".to_owned()).expect("serialize");
        let value1 = Value {
            type_id: "String".to_owned(),
            data,
        };

        let key2 = Key {
            key: "b".to_owned(),
            key_version: 0,
        };

        let data = serialize(&1).expect("serialize");
        let value2 = Value {
            type_id: "i32".to_owned(),
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
        let mut map: HashMap<Key, Value> = HashMap::new();
        let key1 = Key {
            key: "a".to_owned(),
            key_version: 0,
        };

        let data = serialize(&"value".to_owned()).expect("serialize");
        let value1 = Value {
            type_id: "String".to_owned(),
            data,
        };
        let key2 = Key {
            key: "a".to_owned(),
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
        let mut map: HashMap<Key, Value> = HashMap::new();
        let key1 = Key {
            key: "a".to_owned(),
            key_version: 0,
        };

        let data = serialize(&"value".to_owned()).expect("serialize");
        let value1 = Value {
            type_id: "String".to_owned(),
            data,
        };

        let key2 = Key {
            key: "a".to_owned(),
            key_version: 1,
        };

        let data = serialize(&1).expect("serialize");
        let value2 = Value {
            type_id: "i32".to_owned(),
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
        let mut table = Table {
            map: HashMap::new(),
            map_version: HashMap::new(),
            insert: Vec::new(),
            remove: Vec::new(),
        };
        table.insert(
            "test_outer".to_owned(),
            "test_inner".to_owned(),
            "i32".to_owned(),
            Box::new(1),
        );
        let value = table.get("test_outer", "test_inner").expect("get value");
        let deserialized_data: i32 = deserialize_from(&value.data).expect("deserialize");
        assert_eq!(deserialized_data, 1);
    }

    #[test]
    fn test_integration_with_table_map() {
        let mut rt = Runtime::new().unwrap();
        let config = ClientConfigBuilder::default()
            .connection_type(ConnectionType::Mock(MockType::Happy))
            .mock(true)
            .controller_uri(PravegaNodeUri::from("127.0.0.2:9091".to_string()))
            .build()
            .unwrap();
        let factory = ClientFactory::new(config);
        let mut sync = rt.block_on(factory.create_table_synchronizer("sync".to_string()));
        rt.block_on(sync.insert(|table| {
            table.insert(
                "outer_key".to_owned(),
                "inner_key".to_owned(),
                "i32".to_owned(),
                Box::new(1),
            );
            Ok(None)
        }))
        .unwrap();
        let value_option = sync.get("outer_key", "inner_key");
        assert!(value_option.is_some());

        rt.block_on(sync.remove(|table| {
            table.insert_tombstone("outer_key".to_owned(), "inner_key".to_owned())?;
            Ok(None)
        }))
        .unwrap();
        let value_option = sync.get("outer_key", "inner_key");
        assert!(value_option.is_none());
    }
}
