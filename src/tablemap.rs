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
use bincode2::{deserialize_from, serialize};
use chrono::format::Item;
use futures::pin_mut;
use futures::stream;
use futures::stream::StreamExt;
use log::debug;
use log::info;
use pravega_rust_client_shared::{Scope, ScopedSegment, Segment, Stream};
use pravega_wire_protocol::commands::{
    CreateTableSegmentCommand, ReadTableCommand, ReadTableKeysCommand, RemoveTableKeysCommand, TableEntries,
    TableKey, TableValue, UpdateTableEntriesCommand,
};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::borrow::Borrow;
use std::net::SocketAddr;

pub struct TableMap<'a> {
    /// name of the map
    name: String,
    raw_client: Box<dyn RawClient<'a> + 'a>,
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
}
impl<'a> TableMap<'a> {
    /// create a table map
    pub async fn new(name: String, factory: &'a ClientFactoryInternal) -> Result<TableMap<'a>, TableError> {
        let segment = ScopedSegment {
            scope: Scope::new("_tables".into()),
            stream: Stream::new(name),
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

        table_map
            .raw_client
            .as_ref()
            .send_request(&req)
            .await
            .map_err(|e| TableError::ConnectionError {
                can_retry: true,
                operation: "Create table segment".to_string(),
                source: e,
            })
            .map(|r| {
                match r {
                    Replies::SegmentCreated(..) | Replies::SegmentAlreadyExists(..) => {
                        info!("Table segment {} created", table_map.name);
                        table_map
                    }
                    // unexpected response from Segment store causes a panic.
                    _ => panic!("Invalid response during creation of TableSegment"),
                }
            })
    }

    ///
    /// Returns the latest value corresponding to the key.
    ///
    /// If the map does not have the key [`None`] is returned. The version number of the Value is
    /// returned by the API.
    ///
    pub async fn get<K, V>(&self, k: &K) -> Result<Option<(V, i64)>, TableError>
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
    pub async fn insert<K, V>(&self, k: &K, v: &V) -> Result<i64, TableError>
    where
        K: Serialize + Deserialize<'a>,
        V: Serialize + Deserialize<'a>,
    {
        // use KEY_NO_VERSION to ensure unconditional update.
        self.insert_conditionally(k, v, TableKey::KEY_NO_VERSION).await
    }

    ///
    /// Conditionally inserts a key-value pair into the table map. The Key and Value are serialized to to bytes using
    /// bincode2
    ///
    /// The insert is performed after checking the key_version passed.
    /// Once the update is done the newer version is returned.
    /// TableError::BadKeyVersion is returned incase of an incorrect key version.
    ///
    pub async fn insert_conditionally<K, V>(&self, k: &K, v: &V, key_version: i64) -> Result<i64, TableError>
    where
        K: Serialize + Deserialize<'a>,
        V: Serialize + Deserialize<'a>,
    {
        let key = serialize(k).expect("error during serialization.");
        let val = serialize(v).expect("error during serialization.");
        self.insert_raw_values(vec![(key, val, key_version)])
            .await
            .map(|versions| versions[0])
    }

    ///
    ///Unconditionally remove a key from the Tablemap. If the key does not exist an Ok(()) is returned.
    ///
    pub async fn remove<K: Serialize + Deserialize<'a>>(&self, k: &K) -> Result<(), TableError> {
        let key = serialize(k).expect("error during serialization.");
        self.remove_raw_value(key, TableKey::KEY_NO_VERSION).await
    }

    ///
    /// Conditionally remove a key from the Tablemap if it matches the provided key version.
    /// TableError::BadKeyVersion is returned incase the version does not exist.
    ///
    pub async fn remove_conditionally<K>(&self, k: &K, key_version: i64) -> Result<(), TableError>
    where
        K: Serialize + Deserialize<'a>,
    {
        let key = serialize(k).expect("error during serialization.");
        self.remove_raw_value(key, key_version).await
    }

    ///
    /// Returns the latest values for a given list of keys. If the tablemap does not have a
    ///key a `None` is returned for the corresponding key. The version number of the Value is also
    ///returned by the API
    ///
    pub async fn get_all<K, V>(&self, keys: Vec<&K>) -> Result<Vec<Option<(V, i64)>>, TableError>
    where
        K: Serialize + serde::de::DeserializeOwned,
        V: Serialize + serde::de::DeserializeOwned,
    {
        let keys_raw: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| serialize(*k).expect("error during serialization."))
            .collect();

        let read_result: Result<Vec<(Vec<u8>, i64)>, TableError> = self.get_raw_values(keys_raw).await;
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
    pub async fn insert_all<K, V>(&self, kvps: Vec<(&K, &V)>) -> Result<Vec<i64>, TableError>
    where
        K: Serialize + Deserialize<'a>,
        V: Serialize + Deserialize<'a>,
    {
        let r: Vec<(Vec<u8>, Vec<u8>, i64)> = kvps
            .iter()
            .map(|(k, v)| {
                (
                    serialize(k).expect("error during serialization."),
                    serialize(v).expect("error during serialization."),
                    TableKey::KEY_NO_VERSION,
                )
            })
            .collect();
        self.insert_raw_values(r).await
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
        kvps: Vec<(&K, &V, i64)>,
    ) -> Result<Vec<i64>, TableError>
    where
        K: Serialize + Deserialize<'a>,
        V: Serialize + Deserialize<'a>,
    {
        let r: Vec<(Vec<u8>, Vec<u8>, i64)> = kvps
            .iter()
            .map(|(k, v, ver)| {
                (
                    serialize(k).expect("error during serialization."),
                    serialize(v).expect("error during serialization."),
                    *ver,
                )
            })
            .collect();
        self.insert_raw_values(r).await
    }

    ///
    /// Insert key value pairs without serialization.
    /// The function returns the newer version number post the insert operation.
    ///
    async fn insert_raw_values(&self, kvps: Vec<(Vec<u8>, Vec<u8>, i64)>) -> Result<Vec<i64>, TableError> {
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
            // unexpected response from Segment store causes a panic.
            _ => panic!("Unexpected response for update tableEntries"),
        })
    }

    ///
    /// Get raw bytes for a givenKey. If not value is present then None is returned.
    /// The read result and the corresponding version is returned as a tuple.
    ///
    async fn get_raw_values(&self, keys: Vec<Vec<u8>>) -> Result<Vec<(Vec<u8>, i64)>, TableError> {
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
        re.map_err(|e| TableError::ConnectionError {
            can_retry: true,
            operation: "Read from tablemap".to_string(),
            source: e,
        })
        .map(|reply| match reply {
            Replies::TableRead(c) => {
                let v: Vec<(TableKey, TableValue)> = c.entries.entries;
                if v.is_empty() {
                    // partial response from Segment store causes a panic.
                    panic!("Invalid response from the Segment store");
                } else {
                    //fetch value and corresponding version.
                    let result: Vec<(Vec<u8>, i64)> =
                        v.iter().map(|(l, r)| (r.data.clone(), l.key_version)).collect();
                    result
                }
            }
            // unexpected response from Segment store causes a panic.
            _ => panic!("Unexpected response for update tableEntries"),
        })
    }

    ///
    /// Remove an entry for given key as Vec<u8>
    ///
    async fn remove_raw_value(&self, key: Vec<u8>, key_version: i64) -> Result<(), TableError> {
        let op = "Remove keys from tablemap";
        let tk = TableKey::new(key, key_version);
        let req = Requests::RemoveTableKeys(RemoveTableKeysCommand {
            request_id: get_request_id(),
            segment: self.name.clone(),
            delegation_token: "".to_string(),
            keys: vec![tk],
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
            // unexpected response from Segment store causes a panic.
            _ => panic!("Unexpected response while deleting keys"),
        })
    }

    // pub async fn key_iteratorl<K: Serialize + Deserialize<'a>>(
    //     &self,
    // ) -> impl stream::Stream<Item = Result<(K, i64), TableError>> {
    // }

    pub async fn get_keys(&self, max_keys_per_batch: i32) -> Result<(), TableError> {
        let token = vec![];

        let (op, r1) = self.get_keys_for_token(max_keys_per_batch, token).await;

        let r2 = r1.map(|cmd| {
            let token = cmd.continuation_token;
            println!("token-0 {:?}", &token);
            let v: Vec<TableKey> = cmd.keys;
            let v1: Vec<(Vec<u8>, i64)> = v.iter().map(|k| (k.data.to_owned(), k.key_version)).collect();

            (v1, token)
        });

        let r3 = r2.map(|(list, token)| {
            let lx = list.as_slice();
            let lx1: Vec<String> = lx
                .iter()
                .map(|(l, r)| {
                    let s: String = deserialize_from(l.as_slice()).expect("d");
                    println!("==> {:?}", s);
                    s
                })
                .collect();
            (token.clone())
        });

        let to = r3.unwrap();
        //let to = rc.unwrap().continuation_token;
        let req = Requests::ReadTableKeys(ReadTableKeysCommand {
            request_id: self.id.fetch_add(1, Ordering::SeqCst) + 1,
            segment: self.name.clone(),
            delegation_token: "".to_string(),
            suggested_key_count: 4,
            continuation_token: to,
        });

        let re = self.raw_client.as_ref().send_request(&req).await;
        let r1 = re
            .map_err(|e| TableError::ConnectionError {
                can_retry: true,
                operation: op.into(),
                source: e,
            })
            .and_then(|r| match r {
                Replies::TableKeysRead(c) => Ok(c),
                _ => panic!("Unexpected response while deleting keys"),
            });
        let r2 = r1.map(|cmd| {
            let token = cmd.continuation_token;
            println!("token-1 {:?}", token);
            let v: Vec<TableKey> = cmd.keys;
            let v1: Vec<(Vec<u8>, i64)> = v.iter().map(|k| (k.data.to_owned(), k.key_version)).collect();

            v1
        });

        let r3 = r2.map(|list| {
            let lx = list.as_slice();
            let lx1: Vec<String> = lx
                .iter()
                .map(|(l, r)| {
                    let s: String = deserialize_from(l.as_slice()).expect("d");
                    println!("==> {:?}", s);
                    s
                })
                .collect();
            ()
        });

        Ok(())
    }

    async fn get_keys_for_token(
        &self,
        max_keys_per_batch: i32,
        token: Vec<u8>,
    ) -> (&str, Result<TableKeysReadCommand, TableError>) {
        let op = "get_keys";
        let req = Requests::ReadTableKeys(ReadTableKeysCommand {
            request_id: self.id.fetch_add(1, Ordering::SeqCst) + 1,
            segment: self.name.clone(),
            delegation_token: "".to_string(),
            suggested_key_count: max_keys_per_batch,
            continuation_token: token,
        });
        let re = self.raw_client.as_ref().send_request(&req).await;
        let r1 = re
            .map_err(|e| TableError::ConnectionError {
                can_retry: true,
                operation: op.into(),
                source: e,
            })
            .and_then(|r| match r {
                Replies::TableKeysRead(c) => Ok(c),
                _ => panic!("Unexpected response while deleting keys"),
            });
        (op, r1)
    }
}

#[cfg(test)]
mod tests {
    use crate::client_factory::ClientFactory;
    use crate::tablemap::TableError;
    use pravega_wire_protocol::client_config::ClientConfigBuilder;
    use pravega_wire_protocol::client_config::TEST_CONTROLLER_URI;

    #[tokio::test]
    async fn test_iterator() {
        let config = ClientConfigBuilder::default()
            .controller_uri(TEST_CONTROLLER_URI)
            .build()
            .expect("creating config");

        let client_factory = ClientFactory::new(config.clone());
        let map = client_factory.create_table_map("t1".into()).await;
        map.insert("k1".to_string(), "v1".to_string()).await;
        map.insert("k2".to_string(), "v2".to_string()).await;
        map.insert("k3".to_string(), "v3".to_string()).await;
        map.insert("k4".to_string(), "v4".to_string()).await;
        map.insert("k5".to_string(), "v5".to_string()).await;
        // let v: Result<Option<(String, i64)>, TableError> = map.get("k1".to_string()).await;
        let r1 = map.get_keys(2).await;
        println!("{:?}", r1);
    }
}
