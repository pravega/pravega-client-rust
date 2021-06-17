//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::pravega_service::PravegaStandaloneServiceConfig;
use crate::utils;

use pravega_client::byte::reader::ByteReader;
use pravega_client::byte::writer::ByteWriter;
use pravega_client::client_factory::ClientFactory;
use pravega_client::event::writer::EventWriter;
use pravega_client::index::{IndexReader, IndexWriter, Label};
use pravega_client::label;
use pravega_client_config::{connection_type::ConnectionType, ClientConfigBuilder, MOCK_CONTROLLER_URI};
use pravega_client_shared::*;
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::commands::{
    Command, EventCommand, GetStreamSegmentInfoCommand, SealSegmentCommand,
};
use pravega_wire_protocol::connection_factory::{ConnectionFactory, SegmentConnectionManager};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use tokio::runtime::{Handle, Runtime};
use tracing::{error, info};

pub fn test_index_stream(config: PravegaStandaloneServiceConfig) {
    // spin up Pravega standalone
    let scope_name = Scope::from("testScopeIndexStream".to_owned());
    let stream_name = Stream::from("testStreamIndexStream".to_owned());
    let config = ClientConfigBuilder::default()
        .controller_uri(MOCK_CONTROLLER_URI)
        .is_auth_enabled(config.auth)
        .is_tls_enabled(config.tls)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config);
    let handle = client_factory.runtime();
    handle.block_on(utils::create_scope_stream(
        client_factory.controller_client(),
        &scope_name,
        &stream_name,
        1,
    ));

    let scoped_segment = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment::from(0),
    };

    let mut writer = handle.block_on(client_factory.create_index_writer(scoped_segment.clone()));
    let mut reader = handle.block_on(client_factory.create_index_reader(scoped_segment));

    handle.block_on(test_write_and_read(&mut writer, &mut reader));
}

async fn test_write_and_read(writer: &mut IndexWriter, reader: &mut IndexReader) {
    info!("test index stream write and read");

    // test normal append
    for i in 1..10 {
        let label = label! {("uuid", i), ("timestamp", i * 10), ("frame", i)};
        let data = vec![1; i as usize];
        writer
            .append(data, label)
            .await
            .expect("write payload1 to byte stream");
    }

    // test append with invalid label
    let label = label! {("uuid", 10), ("timestamp", 100), ("frame", 9)};
    let data = vec![1; 10];
    let res = writer.append(data, label).await;
    assert!(res.is_err());

    let label = label! {};
    let data = vec![1; 10];
    let res = writer.append(data, label).await;
    assert!(res.is_err());

    let label = label! {("timestamp", 100), ("frame", 10)};
    let data = vec![1; 10];
    let res = writer.append(data, label).await;
    assert!(res.is_err());

    // test normal read
    for i in 1..10 {
        let data = vec![1; i as usize];
        let read = reader.read().await.expect("read data");
        assert_eq!(read, data);
    }

    // test search offset
    let offset = reader.search_offset(("uuid", 5)).await.expect("get offset");
    assert_eq!(offset, 1024 * 4 * 4);

    let offset = reader.search_offset(("uuid", 0)).await.expect("get offset");
    assert_eq!(offset, 0);

    let res = reader.search_offset(("uuid", 11)).await;
    assert!(res.is_err());

    info!("test index stream write and read passed");
}
