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
use log::debug;
use log::info;
use pravega_rust_client_shared::{Scope, ScopedSegment, Segment, Stream};
use pravega_wire_protocol::commands::{
    CreateTableSegmentCommand, ReadTableCommand, TableEntries, TableKey, TableValue,
    UpdateTableEntriesCommand,
};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};

pub struct TableMap<'a> {
    name: String,
    raw_client: Box<dyn RawClient<'a> + 'a>,
    id: &'static AtomicI64,
}

#[derive(Debug, Snafu)]
pub enum TableError {
    #[snafu(display("Non retryable error {} ", error_msg))]
    ConfigError {
        can_retry: bool,
        error_msg: String,
    },
    #[snafu(display("Connection Error while performing {}: {}", operation, source))]
    ConnectionError {
        can_retry: bool,
        operation: String,
        source: RawClientError,
    },
    EmptyError {
        can_retry: bool,
        operation: String,
    },
    KeyDoesNotExist {
        error_msg: String,
    },
    BadKeyVersion {
        error_msg: String,
    },
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
                    let value: V = deserialize_from(l.as_slice()).expect("err");
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
    /// Insert key and value without serialization.
    /// The function returns the newer version number post the insert operation.
    ///
    pub async fn insert_bytes(
        &self,
        key: Vec<u8>,
        key_version: i64,
        value: Vec<u8>,
    ) -> Result<i64, TableError> {
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
            operation: "Insert into tablemap".to_string(),
            source: e,
        })
        .and_then(|r| match r {
            Replies::TableEntriesUpdated(c) => Ok(c.updated_versions),
            Replies::TableKeyBadVersion(c) => Err(TableError::BadKeyVersion {
                error_msg: c.to_string(),
            }),
            _ => panic!("Unexpected response for update tableEntries"),
        })
        .map(|o| *o.get(0).unwrap())
    }

    ///
    /// Get raw bytes for a givenKey. If not value is present then [None] is returned.
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
}

#[cfg(test)]
mod tests {
    use crate::client_factory::ClientFactory;
    use pravega_wire_protocol::client_config::ClientConfigBuilder;
    use pravega_wire_protocol::client_config::TEST_CONTROLLER_URI;

    #[tokio::test]
    async fn integration_test() {
        let config = ClientConfigBuilder::default()
            .controller_uri(TEST_CONTROLLER_URI)
            .build()
            .expect("creating config");

        let client_factory = ClientFactory::new(config);
        let _t = client_factory.create_table_map("t1".into()).await;
    }
}
