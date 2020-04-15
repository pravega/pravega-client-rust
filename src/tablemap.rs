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
use crate::raw_client::RawClient;
use crate::REQUEST_ID_GENERATOR;
use log::info;
use pravega_rust_client_shared::{Scope, ScopedSegment, Segment, Stream};
use std::net::SocketAddr;
use std::sync::atomic::AtomicI64;

pub(crate) struct TableMap<'a> {
    name: String,
    raw_client: Box<dyn RawClient<'a> + 'a>,
    id: &'static AtomicI64,
}

impl<'a> TableMap<'a> {
    /// create a table map
    pub async fn new(name: String, factory: &'a ClientFactoryInternal) -> TableMap<'a> {
        let segment = ScopedSegment {
            scope: Scope::new("scope".into()),
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
        println!("EndPoint is {}", endpoint.to_string());

        let raw_client = Box::new(factory.create_raw_client(endpoint));
        TableMap {
            name: segment.to_string(),
            raw_client: Box::new(factory.create_raw_client(endpoint)),
            id: &REQUEST_ID_GENERATOR,
        }
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
