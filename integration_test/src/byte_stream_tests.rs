//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use log::info;
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
use std::net::SocketAddr;
use crate::utils;

pub fn test_byte_stream() {
    // spin up Pravega standalone
    let scope_name = Scope::from("testScopeByteStream");
    let stream_name = Stream::from("testStreamByteStream");
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

    let scoped_stream = ScopedStream {
        scope: scope_name,
        stream: stream_name,
    };

    let mut writer = client_factory.create_event_stream_writer(scoped_stream);

    handle.block_on(test_simple_write(&mut writer));

    handle.block_on(test_segment_scaling_up(&mut writer, &client_factory));

    handle.block_on(test_segment_scaling_down(&mut writer, &client_factory));

    let scope_name = Scope::new("testScopeWriter2".into());
    let stream_name = Stream::new("testStreamWriter2".into());
    handle.block_on(create_scope_stream(
        controller_client,
        &scope_name,
        &stream_name,
        1,
    ));
    let scoped_stream = ScopedStream {
        scope: scope_name,
        stream: stream_name,
    };
    let mut writer = client_factory.create_event_stream_writer(scoped_stream);

    handle.block_on(test_write_correctness(&mut writer, &client_factory));
    handle.block_on(test_write_correctness_while_scaling(&mut writer, &client_factory));

    let scope_name = Scope::new("testScopeWriter3".into());
    let stream_name = Stream::new("testStreamWriter3".into());
    handle.block_on(create_scope_stream(
        controller_client,
        &scope_name,
        &stream_name,
        2,
    ));
    let scoped_stream = ScopedStream {
        scope: scope_name,
        stream: stream_name,
    };
    let mut writer = client_factory.create_event_stream_writer(scoped_stream);
    handle.block_on(test_write_correctness_with_routing_key(
        &mut writer,
        &client_factory,
    ));
}