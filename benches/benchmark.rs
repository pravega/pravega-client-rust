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
use pravega_client::byte::ByteReader;
use pravega_client::client_factory::ClientFactory;
use pravega_client::event::EventReader;
use pravega_client::event::EventWriter;
use pravega_client_config::connection_type::{ConnectionType, MockType};
use pravega_client_config::{ClientConfig, ClientConfigBuilder};
use pravega_client_shared::*;
use pravega_controller_client::ControllerClient;
use pravega_wire_protocol::client_connection::{LENGTH_FIELD_LENGTH, LENGTH_FIELD_OFFSET};
use pravega_wire_protocol::commands::{
    AppendSetupCommand, DataAppendedCommand, EventCommand, SegmentCreatedCommand, SegmentReadCommand,
    TableEntries, TableEntriesDeltaReadCommand, TableEntriesUpdatedCommand, TYPE_PLUS_LENGTH_SIZE,
};
use pravega_wire_protocol::wire_commands::{Decode, Encode, Replies, Requests};
use std::io::Read;
use std::io::{Cursor, Error};
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tracing::info;

static EVENT_NUM: usize = 10000;
static EVENT_SIZE: usize = 100;
const READ_EVENT_SIZE_BYTES: usize = 100 * 1024; //100 KB event.

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

    pub async fn run(self) {
        // 100K data chunk
        let data_chunk: [u8; READ_EVENT_SIZE_BYTES] = [0xAAu8; READ_EVENT_SIZE_BYTES];
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
                        last_event_number: i64::MIN, // when there is no previous event in this segment
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
                // Send a mock response for table entry updates.
                Requests::UpdateTableEntries(cmd) => {
                    let new_versions: Vec<i64> = cmd
                        .table_entries
                        .entries
                        .iter()
                        .map(|(k, _v)| k.key_version + 1)
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
                // This ensures the local state of the reader group state is treated as the latest.
                Requests::ReadTableEntriesDelta(cmd) => {
                    let reply = Replies::TableEntriesDeltaRead(TableEntriesDeltaReadCommand {
                        request_id: cmd.request_id,
                        segment: cmd.segment,
                        entries: TableEntries { entries: vec![] }, // no new updates.
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

// Read a segment slice and consume events from the slice.
async fn run_reader(reader: &mut EventReader, last_offset: &mut i64) {
    if let Some(mut slice) = reader.acquire_segment().await {
        while let Some(e) = slice.next() {
            // validate offset in the segment.
            if *last_offset == -1i64 {
                assert_eq!(0, e.offset_in_segment);
            } else {
                assert_eq!(
                    READ_EVENT_SIZE_BYTES + 2 * TYPE_PLUS_LENGTH_SIZE as usize,
                    (e.offset_in_segment - *last_offset) as usize
                );
            }
            // validate the event read length
            assert_eq!(
                READ_EVENT_SIZE_BYTES + TYPE_PLUS_LENGTH_SIZE as usize,
                e.value.len()
            );
            *last_offset = e.offset_in_segment;
        }
    } else {
        assert!(false, "No slice acquired");
    }
}

// This benchmark test uses a mock server that replies ok to any requests instantly. It involves
// kernel latency.
fn event_stream_read_mock_server(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mock_server = rt.block_on(MockServer::new());
    let config = ClientConfigBuilder::default()
        .controller_uri(mock_server.address)
        .mock(true)
        .build()
        .expect("creating config");
    rt.spawn(async { MockServer::run(mock_server).await });
    let mut reader = rt.block_on(set_up_event_stream_reader(config));
    let _ = tracing_subscriber::fmt::try_init();
    info!("start reader with mock server performance testing");
    let mut last_offset: i64 = -1;
    c.bench_function("read 100KB mock server", |b| {
        b.iter(|| {
            rt.block_on(run_reader(&mut reader, &mut last_offset));
        });
    });
    println!("reader performance testing finished");
}
// This benchmark test uses a mock server that replies ok to any requests instantly. It involves
// kernel latency.
fn event_stream_writer_mock_server(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mock_server = rt.block_on(MockServer::new());
    let config = ClientConfigBuilder::default()
        .controller_uri(mock_server.address)
        .mock(true)
        .build()
        .expect("creating config");
    let mut writer = rt.block_on(set_up_event_stream_writer(config));
    rt.spawn(async { MockServer::run(mock_server).await });
    let _ = tracing_subscriber::fmt::try_init();
    info!("start event stream writer mock server performance testing");
    c.bench_function("mock server", |b| {
        b.iter(|| {
            rt.block_on(run(&mut writer));
        });
    });
    info!("event stream writer mock server performance testing finished");
}

// This benchmark test uses a mock server that replies ok to any requests instantly. It involves
// kernel latency. It does not wait for reply.
fn event_stream_writer_mock_server_no_block(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mock_server = rt.block_on(MockServer::new());
    let config = ClientConfigBuilder::default()
        .controller_uri(mock_server.address)
        .mock(true)
        .build()
        .expect("creating config");
    let mut writer = rt.block_on(set_up_event_stream_writer(config));
    rt.spawn(async { MockServer::run(mock_server).await });
    let _ = tracing_subscriber::fmt::try_init();
    info!("start event stream writer mock server(no block) performance testing");
    c.bench_function("mock server(no block)", |b| {
        b.iter(|| {
            rt.block_on(run_no_block(&mut writer));
        });
    });
    info!("event stream writer mock server(no block) performance testing finished");
}

// This benchmark test uses a mock connection that replies ok to any requests instantly. It does not
// involve kernel latency.
fn event_stream_writer_mock_connection(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let config = ClientConfigBuilder::default()
        .controller_uri("127.0.0.1:9090".parse::<SocketAddr>().unwrap())
        .mock(true)
        .connection_type(ConnectionType::Mock(MockType::Happy))
        .build()
        .expect("creating config");
    let mut writer = rt.block_on(set_up_event_stream_writer(config));
    let _ = tracing_subscriber::fmt::try_init();
    info!("start event stream writer mock connection performance testing");
    c.bench_function("mock connection", |b| {
        b.iter(|| {
            rt.block_on(run(&mut writer));
        });
    });
    info!("event stream writer mock server connection testing finished");
}

// This benchmark test uses a mock connection that replies ok to any requests instantly. It does not
// involve kernel latency. It does not wait for reply.
fn event_stream_writer_mock_connection_no_block(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let config = ClientConfigBuilder::default()
        .controller_uri("127.0.0.1:9090".parse::<SocketAddr>().unwrap())
        .mock(true)
        .connection_type(ConnectionType::Mock(MockType::Happy))
        .build()
        .expect("creating config");
    let mut writer = rt.block_on(set_up_event_stream_writer(config));
    let _ = tracing_subscriber::fmt::try_init();
    info!("start event stream writer mock connection(no block) performance testing");
    c.bench_function("mock connection(no block)", |b| {
        b.iter(|| {
            rt.block_on(run_no_block(&mut writer));
        });
    });
    info!("event stream writer mock connection(no block) testing finished");
}

fn byte_stream_reader_mock_server(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mock_server = rt.block_on(MockServer::new());
    let config = ClientConfigBuilder::default()
        .controller_uri(mock_server.address)
        .mock(true)
        .build()
        .expect("creating config");
    rt.spawn(async { MockServer::run(mock_server).await });
    let mut reader = set_up_byte_stream_reader(config, &rt);
    let _ = tracing_subscriber::fmt::try_init();
    info!("start byte stream reader mock server performance testing");
    c.bench_function("byte_stream_reader_mock_server", |b| {
        b.iter(|| {
            run_byte_stream_read(&mut reader);
        });
    });
    info!("byte stream reader mock server testing finished");
}

// helper functions
async fn set_up_event_stream_writer(config: ClientConfig) -> EventWriter {
    let scope_name: Scope = Scope::from("testWriterPerf".to_string());
    let stream_name = Stream::from("testWriterPerf".to_string());
    let client_factory = ClientFactory::new(config.clone());
    let controller_client = client_factory.controller_client();
    create_scope_stream(controller_client, &scope_name, &stream_name, 1).await;
    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    client_factory.create_event_writer(scoped_stream)
}

async fn set_up_event_stream_reader(config: ClientConfig) -> EventReader {
    let scope_name: Scope = Scope::from("testReaderPerf".to_string());
    let stream_name = Stream::from("testReaderPerf".to_string());
    let client_factory = ClientFactory::new(config.clone());
    let controller_client = client_factory.controller_client();
    create_scope_stream(controller_client, &scope_name, &stream_name, 1).await;
    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };
    let reader_group = client_factory
        .create_reader_group(scope_name, "rg1".to_string(), scoped_stream)
        .await;

    let reader = reader_group.create_reader("r1".to_string()).await;
    reader
}

fn set_up_byte_stream_reader(config: ClientConfig, rt: &Runtime) -> ByteReader {
    let scope_name: Scope = Scope::from("testByteReaderPerf".to_string());
    let stream_name = Stream::from("testByteReaderPerf".to_string());
    let client_factory = ClientFactory::new(config.clone());
    let controller_client = client_factory.controller_client();
    rt.block_on(create_scope_stream(
        controller_client,
        &scope_name,
        &stream_name,
        1,
    ));
    let scoped_stream = ScopedStream::from("testByteReaderPerf/testByteReaderPerf");
    client_factory.create_byte_reader(scoped_stream)
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
        retention: Default::default(),
        tags: None,
    };
    controller_client
        .create_stream(&request)
        .await
        .expect("create stream");
    info!("Stream created");
}

// run sends request to server and wait for the reply
async fn run(writer: &mut EventWriter) {
    let mut receivers = Vec::with_capacity(EVENT_NUM);
    for _i in 0..EVENT_NUM {
        let rx = writer.write_event(vec![0; EVENT_SIZE]).await;
        receivers.push(rx);
    }
    assert_eq!(receivers.len(), EVENT_NUM);

    for rx in receivers {
        let reply: Result<(), Error> = rx.await.expect("wait for result from oneshot");
        assert_eq!(reply.is_ok(), true);
    }
}

// run no block sends request to server and does not wait for the reply
async fn run_no_block(writer: &mut EventWriter) {
    let mut receivers = Vec::with_capacity(EVENT_NUM);
    for _i in 0..EVENT_NUM {
        let rx = writer.write_event(vec![0; EVENT_SIZE]).await;
        receivers.push(rx);
    }
    assert_eq!(receivers.len(), EVENT_NUM);
}

fn run_byte_stream_read(reader: &mut ByteReader) {
    for _i in 0..EVENT_NUM {
        let mut read = 0;
        let mut buf = vec![0; EVENT_SIZE];
        while read != EVENT_SIZE {
            let size = reader.read(&mut buf[read..]).expect("byte stream read");
            read += size;
        }
    }
}

criterion_group! {
    name = event_writer_performance;
    config = Criterion::default().sample_size(10);
    targets = event_stream_writer_mock_server,event_stream_writer_mock_server_no_block,event_stream_writer_mock_connection,event_stream_writer_mock_connection_no_block
}
criterion_group! {
    name = event_reader_performance;
    config = Criterion::default().sample_size(10);
    targets = event_stream_read_mock_server
}
criterion_group! {
    name = byte_reader_performance;
    config = Criterion::default().sample_size(10);
    targets = byte_stream_reader_mock_server
}
criterion_main!(
    event_writer_performance,
    event_reader_performance,
    byte_reader_performance
);
