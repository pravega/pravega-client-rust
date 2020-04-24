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
use log::debug;
use log::info;
use pravega_rust_client_shared::{Scope, ScopedSegment, Segment, Stream};
use pravega_wire_protocol::commands::{
    CreateTableSegmentCommand, ReadTableCommand, RemoveTableKeysCommand, TableEntries, TableKey, TableValue,
    UpdateTableEntriesCommand,
};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
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
        };
        let req = Requests::CreateTableSegment(CreateTableSegmentCommand {
            request_id: get_request_id(),
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
        let read_result = self.get_raw_value(key).await;
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
        self.insert_conditionally(k, TableKey::KEY_NO_VERSION, v).await
    }

    ///
    /// Conditionally inserts a key-value pair into the table map. The Key and Value are serialized to to bytes using
    /// bincode2
    ///
    /// The insert is performed after checking the key_version passed.
    /// Once the update is done the newer version is returned.
    /// TableError::BadKeyVersion is returned incase of an incorrect key version.
    ///
    pub async fn insert_conditionally<K: Serialize + Deserialize<'a>, V: Serialize + Deserialize<'a>>(
        &self,
        k: K,
        key_version: i64,
        v: V,
    ) -> Result<i64, TableError> {
        let key = serialize(&k).expect("error during serialization.");
        let val = serialize(&v).expect("error during serialization.");
        self.insert_raw_value(key, key_version, val).await
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
    pub async fn remove_conditionally<K: Serialize + Deserialize<'a>>(
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
    async fn insert_raw_value(
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
    pub async fn get_raw_value(&self, key: Vec<u8>) -> Result<Option<(Vec<u8>, i64)>, TableError> {
        let tk = TableKey::new(key, TableKey::KEY_NO_VERSION);
        let req = Requests::ReadTable(ReadTableCommand {
            request_id: get_request_id(),
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
}
