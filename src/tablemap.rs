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
use crate::raw_client::RawClient;
use crate::REQUEST_ID_GENERATOR;
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
use std::sync::atomic::{AtomicI64, Ordering};

pub struct TableMap<'a> {
    /// name of the map
    name: String,
    raw_client: Box<dyn RawClient<'a> + 'a>,
    id: &'static AtomicI64,
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
    #[snafu(display("Bad Key version observed while performing {}: {}", operation, error_msg))]
    BadKeyVersion { operation: String, error_msg: String },
}
impl<'a> TableMap<'a> {
    /// create a table map
    pub async fn new(name: String, factory: &'a ClientFactoryInternal) -> TableMap<'a> {
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

        let map = TableMap {
            name: segment.to_string(),
            raw_client: Box::new(factory.create_raw_client(endpoint)),
            id: &REQUEST_ID_GENERATOR,
        };
        let req = Requests::CreateTableSegment(CreateTableSegmentCommand {
            request_id: map.id.fetch_add(1, Ordering::SeqCst) + 1,
            segment: map.name.clone(),
            delegation_token: String::from(""),
        });

        let resp = map
            .raw_client
            .as_ref()
            .send_request(&req)
            .await
            .expect("Error while creating table segment");
        match resp {
            Replies::SegmentCreated(..) | Replies::SegmentAlreadyExists(..) => {
                info!("Table segment {} created", map.name);
                map
            }
            _ => panic!("Invalid response during creation of TableSegment"),
        }
    }

    ///
    /// Returns the latest value corresponding to the key.
    ///
    /// If the map does not have the key [`None`] is returned. The version number of the Value is
    /// returned by the API.
    ///
    pub async fn get<
        K: Serialize + serde::de::DeserializeOwned,
        V: Serialize + serde::de::DeserializeOwned,
    >(
        &self,
        k: K,
    ) -> Result<Option<(V, i64)>, TableError> {
        let key = serialize(&k).expect("error during serialization.");
        let read_result = self.get_bytes(key).await;
        read_result.map(|v| {
            v.and_then(|(l, version)| {
                if l.is_empty() {
                    None
                } else {
                    let value: V = deserialize_from(l.as_slice()).expect("error during deserialization");
                    Some((value, version))
                }
            })
        })
    }

    ///
    /// Unconditionally inserts a new or update an existing entry for the given key.
    /// Once the update is performed the newer version is returned.
    ///
    pub async fn insert<K: Serialize + Deserialize<'a>, V: Serialize + Deserialize<'a>>(
        &self,
        k: K,
        v: V,
    ) -> Result<i64, TableError> {
        // use KEY_NO_VERSION to ensure unconditional update.
        self.insert_conditional(k, TableKey::KEY_NO_VERSION, v).await
    }

    ///
    /// Conditionally inserts a key-value pair into the table map. The Key and Value are serialized to to bytes using
    /// bincode2
    ///
    /// The insert is performed after checking the key_version passed.
    /// Once the update is done the newer version is returned.
    /// TableError::BadKeyVersion is returned incase of an incorrect key version.
    ///
    pub async fn insert_conditional<K: Serialize + Deserialize<'a>, V: Serialize + Deserialize<'a>>(
        &self,
        k: K,
        key_version: i64,
        v: V,
    ) -> Result<i64, TableError> {
        let key = serialize(&k).expect("error during serialization.");
        let val = serialize(&v).expect("error during serialization.");
        self.insert_bytes(key, key_version, val).await
    }

    ///
    ///Unconditionally remove a key from the Tablemap. If the key does not exist an Ok(()) is returned.
    ///
    pub async fn remove<K: Serialize + Deserialize<'a>>(&self, k: K) -> Result<(), TableError> {
        let key = serialize(&k).expect("error during serialization.");
        self.remove_bytes(key, TableKey::KEY_NO_VERSION).await
    }

    ///
    /// Conditionally remove a key from the Tablemap if it matches the provided key version.
    /// TableError::BadKeyVersion is returned incase the version does not exist.
    ///
    pub async fn remove_conditional<K: Serialize + Deserialize<'a>>(
        &self,
        k: K,
        key_version: i64,
    ) -> Result<(), TableError> {
        let key = serialize(&k).expect("error during serialization.");
        self.remove_bytes(key, key_version).await
    }

    ///
    /// Insert key and value without serialization.
    /// The function returns the newer version number post the insert operation.
    ///
    pub async fn insert_bytes(
        &self,
        key: Vec<u8>,
        key_version: i64,
        value: Vec<u8>,
    ) -> Result<i64, TableError> {
        let op = "Insert into tablemap";
        let tk = TableKey::new(key, key_version);
        let tv = TableValue::new(value);
        let te = TableEntries {
            entries: vec![(tk, tv)],
        };
        let req = Requests::UpdateTableEntries(UpdateTableEntriesCommand {
            request_id: self.id.fetch_add(1, Ordering::SeqCst) + 1,
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
            Replies::TableKeyBadVersion(c) => Err(TableError::BadKeyVersion {
                operation: op.into(),
                error_msg: c.to_string(),
            }),
            _ => panic!("Unexpected response for update tableEntries"),
        })
        .map(|o| *o.get(0).unwrap())
    }

    ///
    /// Get raw bytes for a givenKey. If not value is present then None is returned.
    /// The read result and the corresponding version is returned as a tuple.
    ///
    pub async fn get_bytes(&self, key: Vec<u8>) -> Result<Option<(Vec<u8>, i64)>, TableError> {
        let tk = TableKey::new(key, TableKey::KEY_NO_VERSION);
        let req = Requests::ReadTable(ReadTableCommand {
            request_id: self.id.fetch_add(1, Ordering::SeqCst) + 1,
            segment: self.name.clone(),
            delegation_token: "".to_string(),
            keys: vec![tk],
        });
        let re = self.raw_client.as_ref().send_request(&req).await;
        debug!("Read Response {:?}", re);
        re.map_err(|e| TableError::ConnectionError {
            can_retry: true,
            operation: "Read from tablemap".to_string(),
            source: e,
        })
        .map(|reply| match reply {
            Replies::TableRead(c) => c.entries.entries,
            _ => panic!("Unexpected response for update tableEntries"),
        })
        .map(|v: Vec<(TableKey, TableValue)>| {
            if v.is_empty() {
                None
            } else {
                let (l, r) = v[0].clone(); //Can we eliminate this?
                Some((r.data, l.key_version))
            }
        })
    }

    ///
    /// Remove an entry for given key as Vec<u8>
    ///
    pub async fn remove_bytes(&self, key: Vec<u8>, key_version: i64) -> Result<(), TableError> {
        let op = "Remove keys from tablemap";
        let tk = TableKey::new(key, key_version);
        let req = Requests::RemoveTableKeys(RemoveTableKeysCommand {
            request_id: self.id.fetch_add(1, Ordering::SeqCst) + 1,
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
            Replies::TableKeyBadVersion(c) => Err(TableError::BadKeyVersion {
                operation: op.into(),
                error_msg: c.to_string(),
            }),
            Replies::TableKeyDoesNotExist(c) => Err(TableError::KeyDoesNotExist {
                operation: op.into(),
                error_msg: c.to_string(),
            }),
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
