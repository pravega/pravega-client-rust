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
use crate::error::RawClientError;
use crate::get_request_id;
use crate::raw_client::RawClient;
use async_stream::try_stream;
use futures::stream::Stream;
use pravega_client_auth::DelegationTokenProvider;
use pravega_client_retry::retry_async::retry_async;
use pravega_client_retry::retry_result::RetryResult;
use pravega_client_shared::{PravegaNodeUri, Stream as PravegaStream};
use pravega_client_shared::{Scope, ScopedSegment, ScopedStream, Segment};
use pravega_wire_protocol::commands::{
    CreateTableSegmentCommand, ReadTableCommand, ReadTableEntriesCommand, ReadTableEntriesDeltaCommand,
    ReadTableKeysCommand, RemoveTableKeysCommand, TableEntries, TableKey, TableValue,
    UpdateTableEntriesCommand,
};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use serde::Serialize;
use serde_cbor::from_slice;
use serde_cbor::to_vec;
use snafu::Snafu;
use tracing::{debug, info};

pub type Version = i64;

pub struct TableMap {
    /// name of the map
    name: String,
    endpoint: PravegaNodeUri,
    factory: ClientFactory,
    delegation_token_provider: DelegationTokenProvider,
}

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
impl TableMap {
    /// create a table map
    pub async fn new(scope: Scope, name: String, factory: ClientFactory) -> Result<TableMap, TableError> {
        let segment = ScopedSegment {
            scope: Scope::from(scope),
            stream: PravegaStream::from(format!("_table_{}", name)),
            segment: Segment::from(0),
        };
        info!("creating table map on {:?}", segment);

        let delegation_token_provider = factory
            .create_delegation_token_provider(ScopedStream::from(&segment))
            .await;

        let op = "Create table segment";
        retry_async(factory.get_config().retry_policy, || async {
            let req = Requests::CreateTableSegment(CreateTableSegmentCommand {
                request_id: get_request_id(),
                segment: segment.to_string(),
                delegation_token: delegation_token_provider
                    .retrieve_token(factory.get_controller_client())
                    .await,
            });

            let endpoint = factory
                .get_controller_client()
                .get_endpoint_for_segment(&segment)
                .await
                .expect("get endpoint for segment");
            debug!("endpoint is {}", endpoint.to_string());

            let result = factory
                .create_raw_client_for_endpoint(endpoint.clone())
                .send_request(&req)
                .await;
            match result {
                Ok(reply) => RetryResult::Success((reply, endpoint)),
                Err(e) => {
                    if e.is_token_expired() {
                        delegation_token_provider.signal_token_expiry();
                        info!("auth token needs to refresh");
                    }
                    RetryResult::Retry(e)
                }
            }
        })
        .await
        .map_err(|e| TableError::ConnectionError {
            can_retry: true,
            operation: op.to_string(),
            source: e.error,
        })
        .and_then(|(r, endpoint)| match r {
            Replies::SegmentCreated(..) | Replies::SegmentAlreadyExists(..) => {
                info!("Table segment {:?} created", segment);
                let table_map = TableMap {
                    name: segment.to_string(),
                    endpoint,
                    factory,
                    delegation_token_provider,
                };
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
        let key = to_vec(k).expect("error during serialization.");
        let read_result = self.get_raw_values(vec![key]).await;
        read_result.map(|v| {
            let (l, version) = &v[0];
            if l.is_empty() {
                None
            } else {
                let value: V = from_slice(l.as_slice()).expect("error during deserialization");
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
        K: Serialize + serde::de::DeserializeOwned,
        V: Serialize + serde::de::DeserializeOwned,
    {
        // use KEY_NO_VERSION to ensure unconditional update.
        self.insert_conditionally(k, v, TableKey::KEY_NO_VERSION, offset)
            .await
    }

    ///
    /// Conditionally inserts a key-value pair into the table map. The Key and Value are serialized to bytes using
    /// cbor.
    ///
    /// The insert is performed after checking the key_version passed.
    /// Once the update is done the newer version is returned.
    /// TableError::BadKeyVersion is returned in case of an incorrect key version.
    ///
    pub async fn insert_conditionally<K, V>(
        &self,
        k: &K,
        v: &V,
        key_version: Version,
        offset: i64,
    ) -> Result<Version, TableError>
    where
        K: Serialize + serde::de::DeserializeOwned,
        V: Serialize + serde::de::DeserializeOwned,
    {
        let key = to_vec(k).expect("error during serialization.");
        let val = to_vec(v).expect("error during serialization.");
        self.insert_raw_values(vec![(key, val, key_version)], offset)
            .await
            .map(|versions| versions[0])
    }

    ///
    /// Unconditionally remove a key from the Tablemap. If the key does not exist an Ok(()) is returned.
    ///
    pub async fn remove<K: Serialize + serde::de::DeserializeOwned>(
        &self,
        k: &K,
        offset: i64,
    ) -> Result<(), TableError> {
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
        K: Serialize + serde::de::DeserializeOwned,
    {
        let key = to_vec(k).expect("error during serialization.");
        self.remove_raw_values(vec![(key, key_version)], offset).await
    }

    ///
    /// Returns the latest values for a given list of keys. If the tablemap does not have a
    /// key a `None` is returned for the corresponding key. The version number of the Value is also
    /// returned by the API
    ///
    pub async fn get_all<K, V>(&self, keys: Vec<&K>) -> Result<Vec<Option<(V, Version)>>, TableError>
    where
        K: Serialize + serde::de::DeserializeOwned,
        V: Serialize + serde::de::DeserializeOwned,
    {
        let keys_raw: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| to_vec(*k).expect("error during serialization."))
            .collect();

        let read_result: Result<Vec<(Vec<u8>, Version)>, TableError> = self.get_raw_values(keys_raw).await;
        read_result.map(|v| {
            v.iter()
                .map(|(data, version)| {
                    if data.is_empty() {
                        None
                    } else {
                        let value: V = from_slice(data.as_slice()).expect("error during deserialization");
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
        K: Serialize + serde::de::DeserializeOwned,
        V: Serialize + serde::de::DeserializeOwned,
    {
        let r: Vec<(Vec<u8>, Vec<u8>, Version)> = kvps
            .iter()
            .map(|(k, v)| {
                (
                    to_vec(k).expect("error during serialization."),
                    to_vec(v).expect("error during serialization."),
                    TableKey::KEY_NO_VERSION,
                )
            })
            .collect();
        self.insert_raw_values(r, offset).await
    }

    ///
    /// Conditionally inserts key-value pairs into the table map. The Key and Value are serialized to to bytes using
    /// cbor
    ///
    /// The insert is performed after checking the key_version passed, in case of a failure none of the key-value pairs
    /// are persisted.
    /// Once the update is done the newer version is returned.
    /// TableError::BadKeyVersion is returned in case of an incorrect key version.
    ///
    pub async fn insert_conditionally_all<K, V>(
        &self,
        kvps: Vec<(&K, &V, Version)>,
        offset: i64,
    ) -> Result<Vec<Version>, TableError>
    where
        K: Serialize + serde::de::DeserializeOwned,
        V: Serialize + serde::de::DeserializeOwned,
    {
        let r: Vec<(Vec<u8>, Vec<u8>, Version)> = kvps
            .iter()
            .map(|(k, v, ver)| {
                (
                    to_vec(k).expect("error during serialization."),
                    to_vec(v).expect("error during serialization."),
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
        K: Serialize + serde::de::DeserializeOwned,
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
        K: Serialize + serde::de::DeserializeOwned,
    {
        let r: Vec<(Vec<u8>, Version)> = keys
            .iter()
            .map(|(k, v)| (to_vec(k).expect("error during serialization."), *v))
            .collect();
        self.remove_raw_values(r, offset).await
    }

    ///
    /// Read keys as an Async Stream. This method deserializes the Key based on the type.
    ///
    pub fn read_keys_stream<'stream, 'map: 'stream, K: 'stream>(
        &'map self,
        max_keys_at_once: i32,
    ) -> impl Stream<Item = Result<(K, Version), TableError>> + 'stream
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
                       let key: K = from_slice(key_raw.as_slice()).expect("error during deserialization");
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
    ) -> impl Stream<Item = Result<(K, V, Version), TableError>> + 'stream
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
                        let key: K = from_slice(key_raw.as_slice()).expect("error during deserialization");
                        let value: V = from_slice(value_raw.as_slice()).expect("error during deserialization");
                        yield (key, value, version)
                    }
                    token = t;
                }
            }
        }
    }

    ///
    /// Read entries as an Async Stream from a given position. This method deserialized the Key and Value based on the
    /// inferred type.
    ///
    pub fn read_entries_stream_from_position<'stream, 'map: 'stream, K: 'map, V: 'map>(
        &'map self,
        max_entries_at_once: i32,
        mut from_position: i64,
    ) -> impl Stream<Item = Result<(K, V, Version, i64), TableError>> + 'stream
    where
        K: Serialize + serde::de::DeserializeOwned + std::marker::Unpin,
        V: Serialize + serde::de::DeserializeOwned + std::marker::Unpin,
    {
        try_stream! {
            loop {
                let res: (Vec<(Vec<u8>, Vec<u8>,Version)>, i64)  = self.read_entries_raw_delta(max_entries_at_once, from_position).await?;
                let (entries, last_position) = res;
                if entries.is_empty() {
                    break;
                } else {
                    for (key_raw, value_raw, version) in entries {
                        let key: K = from_slice(key_raw.as_slice()).expect("error during deserialization");
                        let value: V = from_slice(value_raw.as_slice()).expect("error during deserialization");
                        yield (key, value, version, last_position)
                    }
                    from_position = last_position;
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
                    let key: K = from_slice(k.as_slice()).expect("error during deserialization");
                    (key, *version)
                })
                .collect();
            (keys_de, token)
        })
    }

    ///
    /// Get a list of entries in the table map for a given continuation token.
    /// It returns a Vector of Key with its version and a continuation token that can be used to
    /// fetch the next set of entries. An empty Vector as the continuation token will result in the keys
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
                    let key: K = from_slice(k.as_slice()).expect("error during deserialization");
                    let value: V = from_slice(v.as_slice()).expect("error during deserialization");
                    (key, value, *version)
                })
                .collect();
            (entries_de, token)
        })
    }

    ///
    /// Get a list of entries in the table map from a given position.
    ///
    async fn get_entries_delta<K, V>(
        &self,
        max_entries_at_once: i32,
        from_position: i64,
    ) -> Result<(Vec<(K, V, Version)>, i64), TableError>
    where
        K: Serialize + serde::de::DeserializeOwned,
        V: Serialize + serde::de::DeserializeOwned,
    {
        let res = self
            .read_entries_raw_delta(max_entries_at_once, from_position)
            .await;
        res.map(|(entries, token)| {
            let entries_de: Vec<(K, V, Version)> = entries
                .iter()
                .map(|(k, v, version)| {
                    let key: K = from_slice(k.as_slice()).expect("error during deserialization");
                    let value: V = from_slice(v.as_slice()).expect("error during deserialization");
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

        retry_async(self.factory.get_config().retry_policy, || async {
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
                delegation_token: self
                    .delegation_token_provider
                    .retrieve_token(self.factory.get_controller_client())
                    .await,
                table_entries: te,
                table_segment_offset: offset,
            });
            let result = self
                .factory
                .create_raw_client_for_endpoint(self.endpoint.clone())
                .send_request(&req)
                .await;
            match result {
                Ok(reply) => RetryResult::Success(reply),
                Err(e) => {
                    if e.is_token_expired() {
                        self.delegation_token_provider.signal_token_expiry();
                        info!("auth token needs to refresh");
                    }
                    RetryResult::Retry(e)
                }
            }
        })
        .await
        .map_err(|e| TableError::ConnectionError {
            can_retry: true,
            operation: op.into(),
            source: e.error,
        })
        .and_then(|r| match r {
            Replies::TableEntriesUpdated(c) => Ok(c.updated_versions),
            Replies::TableKeyBadVersion(c) => Err(TableError::IncorrectKeyVersion {
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
    /// Get raw bytes for a given Key. If no value is present then None is returned.
    /// The read result and the corresponding version is returned as a tuple.
    ///
    async fn get_raw_values(&self, keys: Vec<Vec<u8>>) -> Result<Vec<(Vec<u8>, Version)>, TableError> {
        let op = "Read from tablemap";

        retry_async(self.factory.get_config().retry_policy, || async {
            let table_keys: Vec<TableKey> = keys
                .iter()
                .map(|k| TableKey::new(k.clone(), TableKey::KEY_NO_VERSION))
                .collect();

            let req = Requests::ReadTable(ReadTableCommand {
                request_id: get_request_id(),
                segment: self.name.clone(),
                delegation_token: self
                    .delegation_token_provider
                    .retrieve_token(self.factory.get_controller_client())
                    .await,
                keys: table_keys,
            });
            let result = self
                .factory
                .create_raw_client_for_endpoint(self.endpoint.clone())
                .send_request(&req)
                .await;
            debug!("Read Response {:?}", result);
            match result {
                Ok(reply) => RetryResult::Success(reply),
                Err(e) => {
                    if e.is_token_expired() {
                        self.delegation_token_provider.signal_token_expiry();
                        info!("auth token needs to refresh");
                    }
                    RetryResult::Retry(e)
                }
            }
        })
        .await
        .map_err(|e| TableError::ConnectionError {
            can_retry: true,
            operation: op.into(),
            source: e.error,
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

        retry_async(self.factory.get_config().retry_policy, || async {
            let tks: Vec<TableKey> = keys
                .iter()
                .map(|(k, ver)| TableKey::new(k.clone(), *ver))
                .collect();

            let req = Requests::RemoveTableKeys(RemoveTableKeysCommand {
                request_id: get_request_id(),
                segment: self.name.clone(),
                delegation_token: self
                    .delegation_token_provider
                    .retrieve_token(self.factory.get_controller_client())
                    .await,
                keys: tks,
                table_segment_offset: offset,
            });
            let result = self
                .factory
                .create_raw_client_for_endpoint(self.endpoint.clone())
                .send_request(&req)
                .await;
            debug!("Reply for RemoveTableKeys request {:?}", result);
            match result {
                Ok(reply) => RetryResult::Success(reply),
                Err(e) => {
                    if e.is_token_expired() {
                        self.delegation_token_provider.signal_token_expiry();
                        info!("auth token needs to refresh");
                    }
                    RetryResult::Retry(e)
                }
            }
        })
        .await
        .map_err(|e| TableError::ConnectionError {
            can_retry: true,
            operation: op.into(),
            source: e.error,
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
        let op = "Read keys";

        retry_async(self.factory.get_config().retry_policy, || async {
            let req = Requests::ReadTableKeys(ReadTableKeysCommand {
                request_id: get_request_id(),
                segment: self.name.clone(),
                delegation_token: self
                    .delegation_token_provider
                    .retrieve_token(self.factory.get_controller_client())
                    .await,
                suggested_key_count: max_keys_at_once,
                continuation_token: token.to_vec(),
            });
            let result = self
                .factory
                .create_raw_client_for_endpoint(self.endpoint.clone())
                .send_request(&req)
                .await;
            debug!("Reply for read tableKeys request {:?}", result);
            match result {
                Ok(reply) => RetryResult::Success(reply),
                Err(e) => {
                    if e.is_token_expired() {
                        self.delegation_token_provider.signal_token_expiry();
                        info!("auth token needs to refresh");
                    }
                    RetryResult::Retry(e)
                }
            }
        })
        .await
        .map_err(|e| TableError::ConnectionError {
            can_retry: true,
            operation: op.into(),
            source: e.error,
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

    ///
    /// Read the raw entries from the table map. It returns a list of key-values and its versions with a continuation token.
    ///
    async fn read_entries_raw(
        &self,
        max_entries_at_once: i32,
        token: &[u8],
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>, Version)>, Vec<u8>), TableError> {
        let op = "Read entries";

        retry_async(self.factory.get_config().retry_policy, || async {
            let req = Requests::ReadTableEntries(ReadTableEntriesCommand {
                request_id: get_request_id(),
                segment: self.name.clone(),
                delegation_token: self
                    .delegation_token_provider
                    .retrieve_token(self.factory.get_controller_client())
                    .await,
                suggested_entry_count: max_entries_at_once,
                continuation_token: token.to_vec(),
            });
            let result = self
                .factory
                .create_raw_client_for_endpoint(self.endpoint.clone())
                .send_request(&req)
                .await;
            debug!("Reply for read tableEntries request {:?}", result);

            match result {
                Ok(reply) => RetryResult::Success(reply),
                Err(e) => {
                    if e.is_token_expired() {
                        self.delegation_token_provider.signal_token_expiry();
                        info!("auth token needs to refresh");
                    }
                    RetryResult::Retry(e)
                }
            }
        })
        .await
        .map_err(|e| TableError::ConnectionError {
            can_retry: true,
            operation: op.into(),
            source: e.error,
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

    ///
    /// Read the raw entries from the table map from a given position.
    /// It returns a list of key-values and its versions with a latest position.
    ///
    async fn read_entries_raw_delta(
        &self,
        max_entries_at_once: i32,
        from_position: i64,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>, Version)>, i64), TableError> {
        let op = "Read entries delta";

        retry_async(self.factory.get_config().retry_policy, || async {
            let req = Requests::ReadTableEntriesDelta(ReadTableEntriesDeltaCommand {
                request_id: get_request_id(),
                segment: self.name.clone(),
                delegation_token: self
                    .delegation_token_provider
                    .retrieve_token(self.factory.get_controller_client())
                    .await,
                from_position,
                suggested_entry_count: max_entries_at_once,
            });
            let result = self
                .factory
                .create_raw_client_for_endpoint(self.endpoint.clone())
                .send_request(&req)
                .await;
            debug!("Reply for read tableEntriesDelta request {:?}", result);

            match result {
                Ok(reply) => RetryResult::Success(reply),
                Err(e) => {
                    if e.is_token_expired() {
                        self.delegation_token_provider.signal_token_expiry();
                        info!("auth token needs to refresh");
                    }
                    RetryResult::Retry(e)
                }
            }
        })
        .await
        .map_err(|e| TableError::ConnectionError {
            can_retry: true,
            operation: op.into(),
            source: e.error,
        })
        .and_then(|r| {
            match r {
                Replies::TableEntriesDeltaRead(c) => {
                    let entries: Vec<(Vec<u8>, Vec<u8>, Version)> = c
                        .entries
                        .entries
                        .iter()
                        .map(|(k, v)| (k.data.clone(), v.data.clone(), k.key_version))
                        .collect();

                    Ok((entries, c.last_position))
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

#[cfg(test)]
mod test {
    use super::*;
    use pravega_client_config::connection_type::{ConnectionType, MockType};
    use pravega_client_config::ClientConfigBuilder;
    use pravega_client_shared::PravegaNodeUri;
    use tokio::runtime::Runtime;

    #[test]
    fn test_table_map_unconditional_insert_and_remove() {
        let mut rt = Runtime::new().unwrap();
        let table_map = create_table_map(&mut rt);

        // unconditionally insert non-existing key
        let version = rt
            .block_on(table_map.insert(&"key".to_string(), &"value".to_string(), -1))
            .expect("unconditionally insert into table map");
        assert_eq!(version, 0);

        // unconditionally insert existing key
        let version = rt
            .block_on(table_map.insert(&"key".to_string(), &"value".to_string(), -1))
            .expect("unconditionally insert into table map");
        assert_eq!(version, 1);

        // unconditionally remove
        rt.block_on(table_map.remove(&"key".to_string(), -1))
            .expect("remove key");

        // get the key, should return None
        let option: Option<(String, Version)> = rt
            .block_on(table_map.get(&"key".to_string()))
            .expect("remove key");
        assert!(option.is_none());
    }

    #[test]
    fn test_table_map_conditional_insert_and_remove() {
        let mut rt = Runtime::new().unwrap();
        let table_map = create_table_map(&mut rt);

        // conditionally insert non-existing key
        let version = rt
            .block_on(table_map.insert_conditionally(&"key".to_string(), &"value".to_string(), -1, -1))
            .expect("unconditionally insert into table map");
        assert_eq!(version, 0);

        // conditionally insert existing key
        let version = rt
            .block_on(table_map.insert_conditionally(&"key".to_string(), &"value".to_string(), 0, -1))
            .expect("conditionally insert into table map");
        assert_eq!(version, 1);

        // conditionally insert key with wrong version
        let result =
            rt.block_on(table_map.insert_conditionally(&"key".to_string(), &"value".to_string(), 0, -1));
        assert!(result.is_err());

        // conditionally remove key
        let result = rt.block_on(table_map.remove_conditionally(&"key".to_string(), 1, -1));
        assert!(result.is_ok());

        // get the key, should return None
        let option: Option<(String, Version)> = rt
            .block_on(table_map.get(&"key".to_string()))
            .expect("remove key");
        assert!(option.is_none());
    }

    #[test]
    fn test_table_map_insert_remove_all() {
        let mut rt = Runtime::new().unwrap();
        let table_map = create_table_map(&mut rt);

        // conditionally insert all
        let mut kvs = vec![];
        let k1 = "k1".to_string();
        let v1 = "v1".to_string();
        let k2 = "k2".to_string();
        let v2 = "v2".to_string();
        kvs.push((&k1, &v1, -1));
        kvs.push((&k2, &v2, -1));

        let version = rt
            .block_on(table_map.insert_conditionally_all(kvs, -1))
            .expect("unconditionally insert all into table map");
        let expected = vec![0, 0];
        assert_eq!(version, expected);

        // conditionally remove all
        let ks = vec![(&k1, 0), (&k2, 0)];
        rt.block_on(table_map.remove_conditionally_all(ks, -1))
            .expect("conditionally remove all from table map");

        // get the key, should return None
        let option: Option<(String, Version)> =
            rt.block_on(table_map.get(&"k1".to_string())).expect("remove key");
        assert!(option.is_none());
        let option: Option<(String, Version)> =
            rt.block_on(table_map.get(&"k2".to_string())).expect("remove key");
        assert!(option.is_none());
    }

    // helper function
    fn create_table_map(rt: &mut Runtime) -> TableMap {
        let config = ClientConfigBuilder::default()
            .connection_type(ConnectionType::Mock(MockType::Happy))
            .mock(true)
            .controller_uri(PravegaNodeUri::from("127.0.0.2:9091"))
            .build()
            .unwrap();
        let factory = ClientFactory::new(config);
        let scope = Scope {
            name: "tablemapScope".to_string(),
        };
        rt.block_on(factory.create_table_map(scope, "tablemap".to_string()))
    }
}
