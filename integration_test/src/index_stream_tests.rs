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
use pravega_client::index::{IndexReader, IndexWriter, Value};
use pravega_client_config::{connection_type::ConnectionType, ClientConfigBuilder, MOCK_CONTROLLER_URI};
use pravega_client_macros::Label;
use pravega_client_shared::*;
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::commands::{
    Command, EventCommand, GetStreamSegmentInfoCommand, SealSegmentCommand,
};
use pravega_wire_protocol::connection_factory::{ConnectionFactory, SegmentConnectionManager};
use pravega_wire_protocol::wire_commands::{Replies, Requests};

use pravega_client::event::EventReader;
use pravega_client::index::reader::IndexReaderError;
use pravega_client::index::writer::IndexWriterError;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use tokio::runtime::{Handle, Runtime};
use tracing::{error, info};

#[derive(Label, Debug, PartialOrd, PartialEq)]
struct TestRecord0 {
    id: u64,
    timestamp: u64,
}

#[derive(Label, Debug, PartialOrd, PartialEq)]
struct TestRecord1 {
    id: u64,
    timestamp: u64,
    pos: u64,
}

#[derive(Label, Debug, PartialOrd, PartialEq)]
struct TestRecord2 {
    pos: u64,
    id: u64,
    timestamp: u64,
}

pub fn test_index_stream(config: PravegaStandaloneServiceConfig) {
    // spin up Pravega standalone
    let scope = Scope::from("testScopeIndexStream".to_owned());
    let stream = Stream::from("testStreamIndexStream".to_owned());
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
        &scope,
        &stream,
        1,
    ));

    let scoped_segment = ScopedSegment {
        scope: scope.clone(),
        stream: stream.clone(),
        segment: Segment::from(0),
    };

    let mut writer = handle.block_on(client_factory.create_index_writer(scoped_segment.clone()));
    let mut reader = handle.block_on(client_factory.create_index_reader(scoped_segment.clone()));
    let scoped_stream = ScopedStream { scope, stream };
    let reader_group = handle.block_on(client_factory.create_reader_group("rg".to_string(), scoped_stream));
    let mut event_reader = handle.block_on(reader_group.create_reader("my_reader".to_string()));

    handle.block_on(test_write_and_read(&mut writer, &mut reader, &mut event_reader));

    let mut writer = handle.block_on(client_factory.create_index_writer(scoped_segment.clone()));
    let mut reader = handle.block_on(client_factory.create_index_reader(scoped_segment.clone()));
    handle.block_on(test_new_record(&mut writer, &mut reader));
    handle.block_on(test_condition_append(&mut writer));

    let mut writer = handle.block_on(client_factory.create_index_writer(scoped_segment.clone()));
    handle.block_on(test_new_record_out_of_order(&mut writer));
}

async fn test_write_and_read(
    writer: &mut IndexWriter<TestRecord0>,
    reader: &mut IndexReader,
    event_reader: &mut EventReader,
) {
    info!("test index stream write and read");
    const EVENT_NUM: u64 = 10;

    // test normal append
    for i in 1..EVENT_NUM {
        let label = TestRecord0 { id: i, timestamp: i };
        let data = vec![1; i as usize];
        writer
            .append(data, label)
            .await
            .expect("write payload1 to byte stream");
        writer.flush().await.expect("flush data");
    }

    // test append with invalid label
    let label = TestRecord0 { id: 1, timestamp: 1 };
    let data = vec![1; 10];
    let res = writer.append(data, label).await;
    assert!(
        matches! {res.err().expect("append should fail due to invalid label"), IndexWriterError::InvalidLabel{..}}
    );

    // test normal read
    for i in 1..EVENT_NUM {
        let data = vec![1; i as usize];
        let read = reader.read().await.expect("read data");
        assert_eq!(read, data);
    }

    // test search offset
    let offset = reader.search_offset(("id", 5)).await.expect("get offset");
    assert_eq!(offset, SeekFrom::Start(1024 * 4 * 4));

    let offset = reader.search_offset(("id", 0)).await.expect("get offset");
    assert_eq!(offset, SeekFrom::Start(0));

    let res = reader.search_offset(("uuid", 11)).await;
    assert!(
        matches! {res.err().expect("search for a non-existing entry"), IndexReaderError::EntryNotFound{..}}
    );

    // test event reader compatibility
    let mut read_count = 1;
    while let Some(mut slice) = event_reader.acquire_segment().await {
        info!("acquire segment for reader {:?}", slice);
        while let Some(event) = slice.next() {
            assert_eq!(1024 * 4 - 8, event.value.as_slice().len(), "Corrupted event read");
            read_count += 1;
        }
        event_reader.release_segment(slice).await;
    }
    assert_eq!(read_count, EVENT_NUM);

    info!("test index stream write and read passed");
}

async fn test_new_record(writer: &mut IndexWriter<TestRecord1>, reader: &mut IndexReader) {
    info!("test index stream new frame");

    // append
    for i in 10..20 {
        let label = TestRecord1 {
            id: i,
            timestamp: i,
            pos: i,
        };
        let data = vec![1; i as usize];
        writer
            .append(data, label)
            .await
            .expect("write payload1 to byte stream");
        writer.flush().await.expect("flush data");
    }

    // read
    for i in 1..20 {
        let data = vec![1; i as usize];
        let read = reader.read().await.expect("read data");
        assert_eq!(read, data);
    }

    // test search offset
    let offset = reader.search_offset(("pos", 10)).await.expect("get offset");
    assert_eq!(offset, SeekFrom::Start(1024 * 4 * 9));

    let offset = reader.search_offset(("pos", 15)).await.expect("get offset");
    assert_eq!(offset, SeekFrom::Start(1024 * 4 * 14));

    let res = reader.search_offset(("id", 21)).await;
    assert!(
        matches! {res.err().expect("search for a non-existing entry"), IndexReaderError::EntryNotFound{..}}
    );

    info!("test index stream new frame passed");
}

async fn test_condition_append(writer: &mut IndexWriter<TestRecord1>) {
    info!("test index stream condition append");
    // valid conditional append
    let condition_on = TestRecord1 {
        id: 19,
        timestamp: 19,
        pos: 19,
    };
    let label = TestRecord1 {
        id: 20,
        timestamp: 20,
        pos: 20,
    };
    let data = vec![1; 20];
    writer
        .append_conditionally(data, label, condition_on)
        .await
        .expect("append conditionally");
    writer.flush().await.expect("flush data");

    // invalid conditional append
    let condition_on = TestRecord1 {
        id: 19,
        timestamp: 19,
        pos: 19,
    };

    let label = TestRecord1 {
        id: 21,
        timestamp: 21,
        pos: 21,
    };
    let data = vec![1; 20];
    let res = writer.append_conditionally(data, label, condition_on).await;
    assert!(
        matches! {res.err().expect("should have conditional append error"), IndexWriterError::InvalidCondition{..}}
    );
    info!("test index stream condition append passed");
}

async fn test_new_record_out_of_order(writer: &mut IndexWriter<TestRecord2>) {
    info!("test index stream new record out of order");
    let data = vec![1; 20];
    let label = TestRecord2 {
        pos: 20,
        id: 20,
        timestamp: 20,
    };
    let res = writer.append(data, label).await;
    assert!(matches! {res.err().expect("should have append error"), IndexWriterError::InvalidLabel{..}});
    info!("test index stream new record out of order passed");
}
