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
use bincode2::{deserialize, deserialize_from, serialize};
use log::debug;
use log::info;
use pravega_rust_client_shared::{Scope, ScopedSegment, Segment, Stream};
use pravega_wire_protocol::commands::{
    CreateTableSegmentCommand, ReadTableCommand, TableEntries, TableKey, TableValue,
    UpdateTableEntriesCommand,
};
use pravega_wire_protocol::wire_commands::Replies::{TableEntriesUpdated, TableRead};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
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

        let m = TableMap {
            name: segment.to_string(),
            raw_client: Box::new(factory.create_raw_client(endpoint)),
            id: &REQUEST_ID_GENERATOR,
        };
        let req = Requests::CreateTableSegment(CreateTableSegmentCommand {
            request_id: m.id.fetch_add(1, Ordering::SeqCst) + 1,
            segment: m.name.clone(),
            delegation_token: String::from(""),
        });
        let response = m
            .raw_client
            .as_ref()
            .send_request(&req)
            .await
            .expect("Error while creating table segment");
        match response {
            Replies::SegmentCreated(..) | Replies::SegmentAlreadyExists(..) => {
                info!("Table segment {} created", m.name);
                m
            }
            _ => panic!("Invalid response during creation of TableSegment"),
        }
    }

    ///
    /// Inserts a key-value pair into the table map. The Key and Value are serialized to to bytes using
    /// bincode2
    ///
    /// The insert is performed without checking versioning. Once the update is done the version of the key
    /// is returned.
    ///
    pub async fn insert<K: Serialize + Deserialize<'a>, V: Serialize + Deserialize<'a>>(
        &self,
        k: K,
        v: V,
    ) -> Result<i64, TableError> {
        let key = serialize(&k).expect("error serialize");
        let tk = TableKey::new(key, TableKey::KEY_NO_VERSION);

        let val = serialize(&v).expect("error v serializae");
        let tv = TableValue::new(val);
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
        debug!("== {:?}", re);
        let s = re
            .map(|r| match r {
                Replies::TableEntriesUpdated(c) => c.updated_versions,
                _ => panic!("Unexpected response for update tableEntries"),
            })
            .map_err(|e| TableError::ConnectionError {
                can_retry: true,
                operation: "Insert into tablemap".to_string(),
                source: e,
            });
        s.map(|o| *o.get(0).unwrap())
    }

    ///
    /// Returns the value corresponding to the key.
    ///
    /// If the map does not have the key [`None`] is returned.
    ///
    pub async fn get<
        K: Serialize + serde::de::DeserializeOwned,
        V: Serialize + serde::de::DeserializeOwned,
    >(
        &self,
        k: K,
    ) -> Result<Option<V>, TableError> {
        let key = serialize(&k).expect("error serialize");
        let tk = TableKey::new(key, TableKey::KEY_NO_VERSION);
        let req = Requests::ReadTable(ReadTableCommand {
            request_id: self.id.fetch_add(1, Ordering::SeqCst) + 1,
            segment: self.name.clone(),
            delegation_token: "".to_string(),
            keys: vec![tk],
        });
        let re = self.raw_client.as_ref().send_request(&req).await;
        debug!("==Read Response {:?}", re);
        let s: Result<Replies, TableError> = re.map_err(|e| TableError::ConnectionError {
            can_retry: true,
            operation: "Read from tablemap".to_string(),
            source: e,
        });

        let s: Result<Vec<(TableKey, TableValue)>, TableError> = s.map(|reply| match reply {
            Replies::TableRead(c) => c.entries.entries,
            _ => panic!("Unexpected response for update tableEntries"),
        });

        let rrr = s.map(|v: Vec<(TableKey, TableValue)>| {
            if (v.is_empty()) {
                None
            } else {
                let (l, r) = v[0].clone();
                let s = r.data;
                let r: V = deserialize_from(s.as_slice()).expect("err");
                Some(r)
            }
        });

        rrr
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
