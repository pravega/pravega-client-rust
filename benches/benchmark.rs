//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use criterion::{criterion_group, criterion_main, Criterion};

use byteorder::BigEndian;
use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::error::SegmentWriterError;
use pravega_client_rust::event_reader::EventReader;
use pravega_client_rust::event_stream_writer::EventStreamWriter;
use pravega_controller_client::ControllerClient;
use pravega_rust_client_config::connection_type::{ConnectionType, MockType};
use pravega_rust_client_config::{ClientConfig, ClientConfigBuilder};
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_connection::{LENGTH_FIELD_LENGTH, LENGTH_FIELD_OFFSET};
use pravega_wire_protocol::commands::{
    AppendSetupCommand, DataAppendedCommand, EventCommand, SegmentCreatedCommand, SegmentReadCommand,
    TableEntries, TableEntriesDeltaReadCommand, TableEntriesUpdatedCommand,
};
use pravega_wire_protocol::wire_commands::{Decode, Encode, Replies, Requests};
use std::io::Cursor;
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::info;

static EVENT_NUM: usize = 10000;
static EVENT_SIZE: usize = 100;

struct MockServer {
    address: SocketAddr,
    listener: TcpListener,
}

impl MockServer {
    pub async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("local server");
        let address = listener.local_addr().unwrap();
        MockServer { address, listener }
    }

    pub async fn run(mut self) {
        let data_chunk: [u8; 1024] = [0xAAu8; 1024];
        let event_data: Vec<u8> = Requests::Event(EventCommand {
            data: data_chunk.to_vec(),
        })
        .write_fields()
        .expect("Encoding event");
        let (mut stream, _addr) = self.listener.accept().await.expect("get incoming stream");
        loop {
            let mut header: Vec<u8> = vec![0; LENGTH_FIELD_OFFSET as usize + LENGTH_FIELD_LENGTH as usize];
            stream
                .read_exact(&mut header[..])
                .await
                .expect("read header from incoming stream");
            let mut rdr = Cursor::new(&header[4..8]);
            let payload_length =
                byteorder::ReadBytesExt::read_u32::<BigEndian>(&mut rdr).expect("exact size");
            let mut payload: Vec<u8> = vec![0; payload_length as usize];
            stream
                .read_exact(&mut payload[..])
                .await
                .expect("read payload from incoming stream");
            let concatenated = [&header[..], &payload[..]].concat();
            let request: Requests = Requests::read_from(&concatenated).expect("decode wirecommand");
            match request {
                Requests::Hello(cmd) => {
                    let reply = Replies::Hello(cmd).write_fields().expect("encode reply");
                    stream
                        .write_all(&reply)
                        .await
                        .expect("write reply back to client");
                }
                Requests::CreateTableSegment(cmd) => {
                    let reply = Replies::SegmentCreated(SegmentCreatedCommand {
                        request_id: cmd.request_id,
                        segment: cmd.segment,
                    })
                    .write_fields()
                    .expect("encode reply");
                    stream
                        .write_all(&reply)
                        .await
                        .expect("write reply back to client");
                }
                Requests::SetupAppend(cmd) => {
                    let reply = Replies::AppendSetup(AppendSetupCommand {
                        request_id: cmd.request_id,
                        segment: cmd.segment,
                        writer_id: cmd.writer_id,
                        last_event_number: -9223372036854775808, // when there is no previous event in this segment
                    })
                    .write_fields()
                    .expect("encode reply");
                    stream
                        .write_all(&reply)
                        .await
                        .expect("write reply back to client");
                }
                Requests::AppendBlockEnd(cmd) => {
                    let reply = Replies::DataAppended(DataAppendedCommand {
                        writer_id: cmd.writer_id,
                        event_number: cmd.last_event_number,
                        previous_event_number: 0, //not used in event stream writer
                        request_id: cmd.request_id,
                        current_segment_write_offset: 0, //not used in event stream writer
                    })
                    .write_fields()
                    .expect("encode reply");
                    stream
                        .write_all(&reply)
                        .await
                        .expect("write reply back to client");
                }

                Requests::ReadSegment(cmd) => {
                    let reply = Replies::SegmentRead(SegmentReadCommand {
                        segment: cmd.segment,
                        offset: cmd.offset,
                        at_tail: false,
                        end_of_segment: false,
                        data: event_data.clone(),
                        request_id: cmd.request_id,
                    })
                    .write_fields()
                    .expect("error while encoding segment read ");

                    stream
                        .write_all(&reply)
                        .await
                        .expect("Write segment read reply to client");
                }
                Requests::UpdateTableEntries(cmd) => {
                    let new_versions: Vec<i64> = cmd
                        .table_entries
                        .entries
                        .iter()
                        .map(|(k, v)| k.key_version + 1)
                        .collect();
                    let reply = Replies::TableEntriesUpdated(TableEntriesUpdatedCommand {
                        request_id: 0,
                        updated_versions: new_versions,
                    })
                    .write_fields()
                    .expect("error while encoding TableEntriesUpdated");
                    stream
                        .write_all(&reply)
                        .await
                        .expect("Error while sending TableEntriesUpdate");
                }

                Requests::ReadTableEntriesDelta(cmd) => {
                    let reply = Replies::TableEntriesDeltaRead(TableEntriesDeltaReadCommand {
                        request_id: cmd.request_id,
                        segment: cmd.segment,
                        entries: TableEntries { entries: vec![] },
                        should_clear: false,
                        reached_end: false,
                        last_position: cmd.from_position,
                    })
                    .write_fields()
                    .expect("Error while encoding TableEntriesDeltaRead");
                    stream
                        .write_all(&reply)
                        .await
                        .expect("Error while sending DeltaRead");
                }
                _ => {
                    panic!("unsupported request {:?}", request);
                }
            }
        }
    }
}

#[inline]
fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n - 1) + fibonacci(n - 2),
    }
}

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("read 1KB", |b| b.iter(|| fibonacci(20)));
}

async fn run_reader(reader: &mut EventReader) {
    if let Some(mut slice) = reader.acquire_segment().await {
        while let Some(e) = slice.next() {
            println!("Read Event at off set {:?}", e.offset_in_segment);
        }
    } else {
        assert!(false, "No slice acquired");
    }
}

// This benchmark test uses a mock server that replies ok to any requests instantly. It involves
// kernel latency.
// This benchmark test uses a mock server that replies ok to any requests instantly. It involves
// kernel latency.
fn read_mock_server(c: &mut Criterion) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let mock_server = rt.block_on(MockServer::new());
    let config = ClientConfigBuilder::default()
        .controller_uri(mock_server.address)
        .mock(true)
        .build()
        .expect("creating config");
    rt.spawn(async { MockServer::run(mock_server).await });
    let mut reader = rt.block_on(setup_reader(config));

    info!("start reader with mock server performance testing");
    c.bench_function("read 1KB mock server", |b| {
        b.iter(|| {
            rt.block_on(run_reader(&mut reader));
        });
    });
    info!("mock server performance testing finished");
}
// This benchmark test uses a mock server that replies ok to any requests instantly. It involves
// kernel latency.
fn mock_server(c: &mut Criterion) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let mock_server = rt.block_on(MockServer::new());
    let config = ClientConfigBuilder::default()
        .controller_uri(mock_server.address)
        .mock(true)
        .build()
        .expect("creating config");
    let mut writer = rt.block_on(set_up(config));
    rt.spawn(async { MockServer::run(mock_server).await });

    info!("start mock server performance testing");
    c.bench_function("mock server", |b| {
        b.iter(|| {
            rt.block_on(run(&mut writer));
        });
    });
    info!("mock server performance testing finished");
}

// This benchmark test uses a mock server that replies ok to any requests instantly. It involves
// kernel latency. It does not wait for reply.
fn mock_server_no_block(c: &mut Criterion) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let mock_server = rt.block_on(MockServer::new());
    let config = ClientConfigBuilder::default()
        .controller_uri(mock_server.address)
        .mock(true)
        .build()
        .expect("creating config");
    let mut writer = rt.block_on(set_up(config));
    rt.spawn(async { MockServer::run(mock_server).await });

    info!("start mock server(no block) performance testing");
    c.bench_function("mock server(no block)", |b| {
        b.iter(|| {
            rt.block_on(run_no_block(&mut writer));
        });
    });
    info!("mock server(no block) performance testing finished");
}

// This benchmark test uses a mock connection that replies ok to any requests instantly. It does not
// involve kernel latency.
fn mock_connection(c: &mut Criterion) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let config = ClientConfigBuilder::default()
        .controller_uri("127.0.0.1:9090".parse::<SocketAddr>().unwrap())
        .mock(true)
        .connection_type(ConnectionType::Mock(MockType::Happy))
        .build()
        .expect("creating config");
    let mut writer = rt.block_on(set_up(config));

    info!("start mock connection performance testing");
    c.bench_function("mock connection", |b| {
        b.iter(|| {
            rt.block_on(run(&mut writer));
        });
    });
    info!("mock server connection testing finished");
}

// This benchmark test uses a mock connection that replies ok to any requests instantly. It does not
// involve kernel latency. It does not wait for reply.
fn mock_connection_no_block(c: &mut Criterion) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let config = ClientConfigBuilder::default()
        .controller_uri("127.0.0.1:9090".parse::<SocketAddr>().unwrap())
        .mock(true)
        .connection_type(ConnectionType::Mock(MockType::Happy))
        .build()
        .expect("creating config");
    let mut writer = rt.block_on(set_up(config));

    info!("start mock connection(no block) performance testing");
    c.bench_function("mock connection(no block)", |b| {
        b.iter(|| {
            rt.block_on(run_no_block(&mut writer));
        });
    });
    info!("mock server connection(no block) testing finished");
}

// helper functions
async fn set_up(config: ClientConfig) -> EventStreamWriter {
    let scope_name: Scope = Scope::from("testWriterPerf".to_string());
    let stream_name = Stream::from("testWriterPerf".to_string());
    let client_factory = ClientFactory::new(config.clone());
    let controller_client = client_factory.get_controller_client();
    create_scope_stream(controller_client, &scope_name, &stream_name, 1).await;
    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    client_factory.create_event_stream_writer(scoped_stream)
}

async fn setup_reader(config: ClientConfig) -> EventReader {
    let scope_name: Scope = Scope::from("testWriterPerf".to_string());
    let stream_name = Stream::from("testWriterPerf".to_string());
    let client_factory = ClientFactory::new(config.clone());
    let controller_client = client_factory.get_controller_client();
    create_scope_stream(controller_client, &scope_name, &stream_name, 1).await;
    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    let reader_group = client_factory
        .create_reader_group("rg1".to_string(), scoped_stream)
        .await;

    let reader = reader_group.create_reader("r1".to_string()).await;
    reader
}

async fn create_scope_stream(
    controller_client: &dyn ControllerClient,
    scope_name: &Scope,
    stream_name: &Stream,
    segment_number: i32,
) {
    controller_client
        .create_scope(scope_name)
        .await
        .expect("create scope");
    info!("Scope created");
    let request = StreamConfiguration {
        scoped_stream: ScopedStream {
            scope: scope_name.clone(),
            stream: stream_name.clone(),
        },
        scaling: Scaling {
            scale_type: ScaleType::FixedNumSegments,
            target_rate: 0,
            scale_factor: 0,
            min_num_segments: segment_number,
        },
        retention: Retention {
            retention_type: RetentionType::None,
            retention_param: 0,
        },
    };
    controller_client
        .create_stream(&request)
        .await
        .expect("create stream");
    info!("Stream created");
}

// run sends request to server and wait for the reply
async fn run(writer: &mut EventStreamWriter) {
    let mut receivers = vec![];
    for _i in 0..EVENT_NUM {
        let rx = writer.write_event(vec![0; EVENT_SIZE]).await;
        receivers.push(rx);
    }
    assert_eq!(receivers.len(), EVENT_NUM);

    for rx in receivers {
        let reply: Result<(), SegmentWriterError> = rx.await.expect("wait for result from oneshot");
        assert_eq!(reply.is_ok(), true);
    }
}

// run no block sends request to server and does not wait for the reply
async fn run_no_block(writer: &mut EventStreamWriter) {
    let mut receivers = vec![];
    for _i in 0..EVENT_NUM {
        let rx = writer.write_event(vec![0; EVENT_SIZE]).await;
        receivers.push(rx);
    }
    assert_eq!(receivers.len(), EVENT_NUM);
}

criterion_group! {
    name = performance;
    config = Criterion::default().sample_size(10);
    targets = mock_server,mock_server_no_block,mock_connection,mock_connection_no_block
}
criterion_group! {
    name = reader_performance;
    config = Criterion::default().sample_size(10);
    targets = read_mock_server
}
criterion_main!(performance, reader_performance);
