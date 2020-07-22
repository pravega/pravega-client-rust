//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::utils;
use log::info;
use pravega_client_rust::byte_stream::{ByteStreamReader, ByteStreamWriter};
use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::error::SegmentWriterError;
use pravega_client_rust::event_stream_writer::EventStreamWriter;
use pravega_client_rust::raw_client::RawClient;
use pravega_client_rust::segment_reader::AsyncSegmentReader;
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::{ClientConfigBuilder, TEST_CONTROLLER_URI};
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::commands::{
    Command, EventCommand, GetStreamSegmentInfoCommand, SealSegmentCommand,
};
use pravega_wire_protocol::connection_factory::{
    ConnectionFactory, ConnectionType, SegmentConnectionManager,
};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::io::{Read, Write};
use std::net::SocketAddr;

pub fn test_byte_stream() {
    // spin up Pravega standalone
    let scope_name = Scope::from("testScopeByteStream".to_owned());
    let stream_name = Stream::from("testStreamByteStream".to_owned());
    let config = ClientConfigBuilder::default()
        .controller_uri(TEST_CONTROLLER_URI)
        .build()
        .expect("creating config");
    let client_factory = ClientFactory::new(config);
    let controller_client = client_factory.get_controller_client();
    let handle = client_factory.get_runtime_handle();
    handle.block_on(utils::create_scope_stream(
        controller_client,
        &scope_name,
        &stream_name,
        1,
    ));

    let scoped_segment = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment::from(0),
    };

    let mut writer = client_factory.create_byte_stream_writer(scoped_segment.clone());
    let mut reader = client_factory.create_byte_stream_reader(scoped_segment);

    test_write_and_read(&mut writer, &mut reader);
}

fn test_write_and_read(writer: &mut ByteStreamWriter, reader: &mut ByteStreamReader) {
    info!("test byte stream write and read");
    let payload1 = vec![1, 1, 1, 1];
    let payload2 = vec![2, 2, 2, 2];
    let expected = [&payload1[..], &payload2[..]].concat();

    let size1 = writer.write(&payload1).expect("write payload1 to byte stream");
    assert_eq!(size1, 4);
    info!("wrote first payload {:?}", payload1);

    let size2 = writer.write(&payload2).expect("write payload2 to byte stream");
    assert_eq!(size2, 4);
    info!("wrote second payload {:?}", payload2);

    let mut buf: Vec<u8> = vec![0; 8];
    loop {
        let bytes = reader.read(&mut buf).expect("read from byte stream");
        if bytes > 0 {
            break;
        }
    }
    assert_eq!(buf, expected);

    info!("test byte stream write and read passed");
}
