//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactoryInternal;
use crate::error::RawClientError;
use crate::get_request_id;
use crate::raw_client::RawClient;
use async_stream::try_stream;
use bincode2::{deserialize_from, serialize};
use futures::stream::Stream;
use log::debug;
use log::info;
use pravega_rust_client_shared::Stream as PravegaStream;
use pravega_rust_client_shared::{Scope, ScopedSegment, Segment};
use pravega_wire_protocol::commands::{
    CreateTableSegmentCommand, ReadTableCommand, ReadTableEntriesCommand, ReadTableKeysCommand,
    RemoveTableKeysCommand, TableEntries, TableKey, TableValue, UpdateTableEntriesCommand,
};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::net::SocketAddr;

pub type Version = i64;

pub struct TableMap<'a> {
    /// name of the map
    name: String,
    raw_client: Box<dyn RawClient<'a> + 'a>,
}

// Workaround for issue https://github.com/rust-lang/rust/issues/63066 as specified in
//https://github.com/rust-lang/rust/issues/34511#issuecomment-373423999
// Without the workaround we get the following error
// error[E0700]: hidden type for `impl Trait` captures lifetime that does not appear in bounds
pub trait Captures<'a> {}
impl<'a, T: ?Sized> Captures<'a> for T {}

#[derive(Debug, Snafu)]
pub enum TableError {
    #[snafu(display("Connection error while performing {}: {}", operation, source))]
    ConnectionError {
        can_retry: bool,
        operation: String,
        source: RawClientError,
    },
    #[snafu(display("Key does not exist while performing {}: {}", operation, error_msg))]
    KeyDoesNotExist { operation: String, error_msg: String },
    #[snafu(display(
        "Incorrect Key version observed while performing {}: {}",
        operation,
        error_msg
    ))]
    IncorrectKeyVersion { operation: String, error_msg: String },
    #[snafu(display("Error observed while performing {} due to {}", operation, error_msg,))]
    OperationError { operation: String, error_msg: String },
}
impl<'a> TableMap<'a> {
    /// create a table map
    pub async fn new(name: String, factory: &'a ClientFactoryInternal) -> Result<TableMap<'a>, TableError> {
        let segment = ScopedSegment {
            scope: Scope::new("_tables".into()),
            stream: PravegaStream::new(name),
            segment: Segment::new(0),
        };
        let endpoint = factory
            .get_controller_client()
            .get_endpoint_for_segment(&segment)
            .await
            .expect("get endpoint for segment")
            .parse::<SocketAddr>()
            .expect("Invalid end point returned");
        debug!("EndPoint is {}", endpoint.to_string());

        let table_map = TableMap {
            name: segment.to_string(),
            raw_client: Box::new(factory.create_raw_client(endpoint)),
        };
        let req = Requests::CreateTableSegment(CreateTableSegmentCommand {
            request_id: get_request_id(),
            segment: table_map.name.clone(),
            delegation_token: String::from(""),
        });

        let op = "Create table segment";
        table_map
            .raw_client
            .as_ref()
            .send_request(&req)
            .await
            .map_err(|e| TableError::ConnectionError {
                can_retry: true,
                operation: op.to_string(),
                source: e,
            })
            .and_then(|r| match r {
                Replies::SegmentCreated(..) | Replies::SegmentAlreadyExists(..) => {
                    info!("Table segment {} created", table_map.name);
                    Ok(table_map)
                }
                _ => Err(TableError::OperationError {
                    operation: op.to_string(),
                    error_msg: r.to_string(),
                }),
            })
    }

    ///
    /// Returns the latest value corresponding to the key.
    ///
    /// If the map does not have the key [`None`] is returned. The version number of the Value is
    /// returned by the API.
    ///
    pub async fn get<K, V>(&self, k: &K) -> Result<Option<(V, Version)>, TableError>
    where
        K: Serialize + serde::de::DeserializeOwned,
        V: Serialize + serde::de::DeserializeOwned,
    {
        let key = serialize(k).expect("error during serialization.");
        let read_result = self.get_raw_values(vec![key]).await;
        read_result.map(|v| {
            let (l, version) = &v[0];
            if l.is_empty() {
                None
            } else {
                let value: V = deserialize_from(l.as_slice()).expect("error during deserialization");
                Some((value, *version))
            }
        })
    }

    ///
    /// Unconditionally inserts a new or update an existing entry for the given key.
    /// Once the update is performed the newer version is returned.
    ///
    pub async fn insert<K, V>(&self, k: &K, v: &V, offset: i64) -> Result<Version, TableError>
    where
        K: Serialize + Deserialize<'a>,
        V: Serialize + Deserialize<'a>,
    {
        // use KEY_NO_VERSION to ensure unconditional update.
        self.insert_conditionally(k, v, TableKey::KEY_NO_VERSION, offset)
            .await
    }

    ///
    /// Conditionally inserts a key-value pair into the table map. The Key and Value are serialized to to bytes using
    /// bincode2
    ///
    /// The insert is performed after checking the key_version passed.
    /// Once the update is done the newer version is returned.
    /// TableError::BadKeyVersion is returned incase of an incorrect key version.
    ///
    pub async fn insert_conditionally<K, V>(
        &self,
        k: &K,
        v: &V,
        key_version: Version,
        offset: i64,
    ) -> Result<Version, TableError>
    where
        K: Serialize + Deserialize<'a>,
        V: Serialize + Deserialize<'a>,
    {
        let key = serialize(k).expect("error during serialization.");
        let val = serialize(v).expect("error during serialization.");
        self.insert_raw_values(vec![(key, val, key_version)], offset)
            .await
            .map(|versions| versions[0])
    }

    ///
    ///Unconditionally remove a key from the Tablemap. If the key does not exist an Ok(()) is returned.
    ///
    pub async fn remove<K: Serialize + Deserialize<'a>>(&self, k: &K, offset: i64) -> Result<(), TableError> {
        self.remove_conditionally(k, TableKey::KEY_NO_VERSION, offset)
            .await
    }

    ///
    /// Conditionally remove a key from the Tablemap if it matches the provided key version.
    /// TableError::BadKeyVersion is returned incase the version does not exist.
    ///
    pub async fn remove_conditionally<K>(
        &self,
        k: &K,
        key_version: Version,
        offset: i64,
    ) -> Result<(), TableError>
    where
        K: Serialize + Deserialize<'a>,
    {
        let key = serialize(k).expect("error during serialization.");
        self.remove_raw_values(vec![(key, key_version)], offset).await
    }

    ///
    /// Returns the latest values for a given list of keys. If the tablemap does not have a
    ///key a `None` is returned for the corresponding key. The version number of the Value is also
    ///returned by the API
    ///
    pub async fn get_all<K, V>(&self, keys: Vec<&K>) -> Result<Vec<Option<(V, Version)>>, TableError>
    where
        K: Serialize + serde::de::DeserializeOwned,
        V: Serialize + serde::de::DeserializeOwned,
    {
        let keys_raw: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| serialize(*k).expect("error during serialization."))
            .collect();

        let read_result: Result<Vec<(Vec<u8>, Version)>, TableError> = self.get_raw_values(keys_raw).await;
        read_result.map(|v| {
            v.iter()
                .map(|(data, version)| {
                    if data.is_empty() {
                        None
                    } else {
                        let value: V =
                            deserialize_from(data.as_slice()).expect("error during deserialization");
                        Some((value, *version))
                    }
                })
                .collect()
        })
    }

    ///
    /// Unconditionally inserts a new or updates an existing entry for the given keys.
    /// Once the update is performed the newer versions are returned.
    ///
    pub async fn insert_all<K, V>(&self, kvps: Vec<(&K, &V)>, offset: i64) -> Result<Vec<Version>, TableError>
    where
        K: Serialize + Deserialize<'a>,
        V: Serialize + Deserialize<'a>,
    {
        let r: Vec<(Vec<u8>, Vec<u8>, Version)> = kvps
            .iter()
            .map(|(k, v)| {
                (
                    serialize(k).expect("error during serialization."),
                    serialize(v).expect("error during serialization."),
                    TableKey::KEY_NO_VERSION,
                )
            })
            .collect();
        self.insert_raw_values(r, offset).await
    }

    ///
    /// Conditionally inserts key-value pairs into the table map. The Key and Value are serialized to to bytes using
    /// bincode2
    ///
    /// The insert is performed after checking the key_version passed, incase of a failure none of the key-value pairs
    /// are persisted.
    /// Once the update is done the newer version is returned.
    /// TableError::BadKeyVersion is returned incase of an incorrect key version.
    ///
    pub async fn insert_conditionally_all<K, V>(
        &self,
        kvps: Vec<(&K, &V, Version)>,
        offset: i64,
    ) -> Result<Vec<Version>, TableError>
    where
        K: Serialize + Deserialize<'a>,
        V: Serialize + Deserialize<'a>,
    {
        let r: Vec<(Vec<u8>, Vec<u8>, Version)> = kvps
            .iter()
            .map(|(k, v, ver)| {
                (
                    serialize(k).expect("error during serialization."),
                    serialize(v).expect("error during serialization."),
                    *ver,
                )
            })
            .collect();
        self.insert_raw_values(r, offset).await
    }

    ///
    /// Unconditionally remove the provided keys from the table map.
    ///
    pub async fn remove_all<K>(&self, keys: Vec<&K>, offset: i64) -> Result<(), TableError>
    where
        K: Serialize + Deserialize<'a>,
    {
        let r: Vec<(&K, Version)> = keys.iter().map(|k| (*k, TableKey::KEY_NO_VERSION)).collect();
        self.remove_conditionally_all(r, offset).await
    }

    ///
    /// Conditionally remove keys after checking the key version. In-case of a failure none of the keys
    /// are removed.
    ///
    pub async fn remove_conditionally_all<K>(
        &self,
        keys: Vec<(&K, Version)>,
        offset: i64,
    ) -> Result<(), TableError>
    where
        K: Serialize + Deserialize<'a>,
    {
        let r: Vec<(Vec<u8>, Version)> = keys
            .iter()
            .map(|(k, v)| (serialize(k).expect("error during serialization."), *v))
            .collect();
        self.remove_raw_values(r, offset).await
    }

    ///
    /// Read keys as an Async Stream. This method deserializes the Key based on the type.
    ///
    pub fn read_keys_stream<'stream, 'map: 'stream, K: 'stream>(
        &'map self,
        max_keys_at_once: i32,
    ) -> impl Stream<Item = Result<(K, Version), TableError>> + Captures<'a> + 'stream
    where
        K: Serialize + serde::de::DeserializeOwned + std::marker::Unpin,
    {
        try_stream! {
            let mut token: Vec<u8> = Vec::new();
            loop {
                let res: (Vec<(Vec<u8>, Version)>, Vec<u8>) = self.read_keys_raw(max_keys_at_once, &token).await?;
                let (keys, t) = res;
                if keys.is_empty() {
                    break;
                } else {
                    for (key_raw, version) in keys {
                       let key: K = deserialize_from(key_raw.as_slice()).expect("error during deserialization");
                        yield (key, version)
                    }
                    token = t;
                }
             }
        }
    }

    ///
    /// Read entries as an Async Stream. This method deserialized the Key and Value based on the
    /// inferred type.
    ///
    pub fn read_entries_stream<'stream, 'map: 'stream, K: 'map, V: 'map>(
        &'map self,
        max_entries_at_once: i32,
    ) -> impl Stream<Item = Result<(K, V, Version), TableError>> + Captures<'a> + 'stream
    where
        K: Serialize + serde::de::DeserializeOwned + std::marker::Unpin,
        V: Serialize + serde::de::DeserializeOwned + std::marker::Unpin,
    {
        try_stream! {
            let mut token: Vec<u8> = Vec::new();
            loop {
                let res: (Vec<(Vec<u8>, Vec<u8>,Version)>, Vec<u8>)  = self.read_entries_raw(max_entries_at_once, &token).await?;
                let (entries, t) = res;
                if entries.is_empty() {
                    break;
                } else {
                    for (key_raw, value_raw, version) in entries {
                        let key: K = deserialize_from(key_raw.as_slice()).expect("error during deserialization");
                        let value: V = deserialize_from(value_raw.as_slice()).expect("error during deserialization");
                        yield (key, value, version)
                    }
                    token = t;
                }
            }
        }
    }

    ///
    /// Get a list of keys in the table map for a given continuation token.
    /// It returns a Vector of Key with its version and a continuation token that can be used to
    /// fetch the next set of keys.An empty Vector as the continuation token will result in the keys
    /// being fetched from the beginning.
    ///
    async fn get_keys<K>(
        &self,
        max_keys_at_once: i32,
        token: &[u8],
    ) -> Result<(Vec<(K, Version)>, Vec<u8>), TableError>
    where
        K: Serialize + serde::de::DeserializeOwned,
    {
        let res = self.read_keys_raw(max_keys_at_once, token).await;
        res.map(|(keys, token)| {
            let keys_de: Vec<(K, Version)> = keys
                .iter()
                .map(|(k, version)| {
                    let key: K = deserialize_from(k.as_slice()).expect("error during deserialization");
                    (key, *version)
                })
                .collect();
            (keys_de, token)
        })
    }

    ///
    /// Get a list of entries in the table map for a given continuation token.
    /// It returns a Vector of Key with its version and a continuation token that can be used to
    /// fetch the next set of entries.An empty Vector as the continuation token will result in the keys
    /// being fetched from the beginning.
    ///
    async fn get_entries<K, V>(
        &self,
        max_entries_at_once: i32,
        token: &[u8],
    ) -> Result<(Vec<(K, V, Version)>, Vec<u8>), TableError>
    where
        K: Serialize + serde::de::DeserializeOwned,
        V: Serialize + serde::de::DeserializeOwned,
    {
        let res = self.read_entries_raw(max_entries_at_once, token).await;
        res.map(|(entries, token)| {
            let entries_de: Vec<(K, V, Version)> = entries
                .iter()
                .map(|(k, v, version)| {
                    let key: K = deserialize_from(k.as_slice()).expect("error during deserialization");
                    let value: V = deserialize_from(v.as_slice()).expect("error during deserialization");
                    (key, value, *version)
                })
                .collect();
            (entries_de, token)
        })
    }

    ///
    /// Insert key value pairs without serialization.
    /// The function returns the newer version number post the insert operation.
    ///
    async fn insert_raw_values(
        &self,
        kvps: Vec<(Vec<u8>, Vec<u8>, Version)>,
        offset: i64,
    ) -> Result<Vec<Version>, TableError> {
        let op = "Insert into tablemap";

        let entries: Vec<(TableKey, TableValue)> = kvps
            .iter()
            .map(|(k, v, ver)| {
                let tk = TableKey::new(k.clone(), *ver);
                let tv = TableValue::new(v.clone());
                (tk, tv)
            })
            .collect();
        let te = TableEntries { entries };
        let req = Requests::UpdateTableEntries(UpdateTableEntriesCommand {
            request_id: get_request_id(),
            segment: self.name.clone(),
            delegation_token: "".to_string(),
            table_entries: te,
            table_segment_offset: offset,
        });
        let re = self.raw_client.as_ref().send_request(&req).await;
        debug!("Reply for UpdateTableEntries request {:?}", re);
        re.map_err(|e| TableError::ConnectionError {
            can_retry: true,
            operation: op.into(),
            source: e,
        })
        .and_then(|r| match r {
            Replies::TableEntriesUpdated(c) => Ok(c.updated_versions),
            Replies::TableKeyBadVersion(c) => Err(TableError::IncorrectKeyVersion {
                operation: op.into(),
                error_msg: c.to_string(),
            }),
            Replies::TableKeyDoesNotExist(c) => Err(TableError::KeyDoesNotExist {
                operation: op.into(),
                error_msg: c.to_string(),
            }),
            // unexpected response from Segment store causes a panic.
            _ => Err(TableError::OperationError {
                operation: op.into(),
                error_msg: r.to_string(),
            }),
        })
    }

    ///
    /// Get raw bytes for a givenKey. If not value is present then None is returned.
    /// The read result and the corresponding version is returned as a tuple.
    ///
    async fn get_raw_values(&self, keys: Vec<Vec<u8>>) -> Result<Vec<(Vec<u8>, Version)>, TableError> {
        let table_keys: Vec<TableKey> = keys
            .iter()
            .map(|k| TableKey::new(k.clone(), TableKey::KEY_NO_VERSION))
            .collect();

        let req = Requests::ReadTable(ReadTableCommand {
            request_id: get_request_id(),
            segment: self.name.clone(),
            delegation_token: "".to_string(),
            keys: table_keys,
        });
        let re = self.raw_client.as_ref().send_request(&req).await;
        debug!("Read Response {:?}", re);
        let op = "Read from tablemap";
        re.map_err(|e| TableError::ConnectionError {
            can_retry: true,
            operation: op.into(),
            source: e,
        })
        .and_then(|reply| match reply {
            Replies::TableRead(c) => {
                let v: Vec<(TableKey, TableValue)> = c.entries.entries;
                if v.is_empty() {
                    // partial response from Segment store causes a panic.
                    panic!("Invalid response from the Segment store");
                } else {
                    //fetch value and corresponding version.
                    let result: Vec<(Vec<u8>, Version)> =
                        v.iter().map(|(l, r)| (r.data.clone(), l.key_version)).collect();
                    Ok(result)
                }
            }
            _ => Err(TableError::OperationError {
                operation: op.into(),
                error_msg: reply.to_string(),
            }),
        })
    }

    ///
    /// Remove a list of keys where the key, represented in raw bytes, and version of the corresponding
    /// keys is specified.
    ///
    async fn remove_raw_values(&self, keys: Vec<(Vec<u8>, Version)>, offset: i64) -> Result<(), TableError> {
        let op = "Remove keys from tablemap";
        let tks: Vec<TableKey> = keys
            .iter()
            .map(|(k, ver)| TableKey::new(k.clone(), *ver))
            .collect();
        let req = Requests::RemoveTableKeys(RemoveTableKeysCommand {
            request_id: get_request_id(),
            segment: self.name.clone(),
            delegation_token: "".to_string(),
            keys: tks,
            table_segment_offset: offset,
        });
        let re = self.raw_client.as_ref().send_request(&req).await;
        debug!("Reply for RemoveTableKeys request {:?}", re);
        re.map_err(|e| TableError::ConnectionError {
            can_retry: true,
            operation: op.into(),
            source: e,
        })
        .and_then(|r| match r {
            Replies::TableKeysRemoved(..) => Ok(()),
            Replies::TableKeyBadVersion(c) => Err(TableError::IncorrectKeyVersion {
                operation: op.into(),
                error_msg: c.to_string(),
            }),
            Replies::TableKeyDoesNotExist(c) => Err(TableError::KeyDoesNotExist {
                operation: op.into(),
                error_msg: c.to_string(),
            }),
            _ => Err(TableError::OperationError {
                operation: op.into(),
                error_msg: r.to_string(),
            }),
        })
    }

    ///
    /// Read the raw keys from the table map. It returns a list of keys and its versions with a continuation token.
    ///
    async fn read_keys_raw(
        &self,
        max_keys_at_once: i32,
        token: &[u8],
    ) -> Result<(Vec<(Vec<u8>, Version)>, Vec<u8>), TableError> {
        {
            let op = "Read keys";
            let req = Requests::ReadTableKeys(ReadTableKeysCommand {
                request_id: get_request_id(),
                segment: self.name.clone(),
                delegation_token: "".to_string(),
                suggested_key_count: max_keys_at_once,
                continuation_token: token.to_vec(),
            });
            let re = self.raw_client.as_ref().send_request(&req).await;
            debug!("Reply for read tableKeys request {:?}", re);
            re.map_err(|e| TableError::ConnectionError {
                can_retry: true,
                operation: op.into(),
                source: e,
            })
            .and_then(|r| match r {
                Replies::TableKeysRead(c) => {
                    let keys: Vec<(Vec<u8>, Version)> =
                        c.keys.iter().map(|k| (k.data.clone(), k.key_version)).collect();

                    Ok((keys, c.continuation_token))
                }
                _ => Err(TableError::OperationError {
                    operation: op.into(),
                    error_msg: r.to_string(),
                }),
            })
        }
    }

    ///
    /// Read the raw entries from the table map. It returns a list of key-values and its versions with a continuation token.
    ///
    async fn read_entries_raw(
        &self,
        max_entries_at_once: i32,
        token: &[u8],
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>, Version)>, Vec<u8>), TableError> {
        {
            let op = "Read entries";
            let req = Requests::ReadTableEntries(ReadTableEntriesCommand {
                request_id: get_request_id(),
                segment: self.name.clone(),
                delegation_token: "".to_string(),
                suggested_entry_count: max_entries_at_once,
                continuation_token: token.to_vec(),
            });
            let re = self.raw_client.as_ref().send_request(&req).await;
            debug!("Reply for read tableEntries request {:?}", re);
            re.map_err(|e| TableError::ConnectionError {
                can_retry: true,
                operation: op.into(),
                source: e,
            })
            .and_then(|r| {
                match r {
                    Replies::TableEntriesRead(c) => {
                        let entries: Vec<(Vec<u8>, Vec<u8>, Version)> = c
                            .entries
                            .entries
                            .iter()
                            .map(|(k, v)| (k.data.clone(), v.data.clone(), k.key_version))
                            .collect();

                        Ok((entries, c.continuation_token))
                    }
                    // unexpected response from Segment store causes a panic.
                    _ => Err(TableError::OperationError {
                        operation: op.into(),
                        error_msg: r.to_string(),
                    }),
                }
            })
        }
    }
}
