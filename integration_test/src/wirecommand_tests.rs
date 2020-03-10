use super::pravega_service::PravegaStandaloneService;
use crate::pravega_service::PravegaService;
use lazy_static::*;
use pravega_client_rust::raw_client::RawClientImpl;
use pravega_controller_client::{create_connection, ControllerClient, ControllerClientImpl};
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::ClientConfigBuilder;
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::commands::Command as WireCommand;
use pravega_wire_protocol::commands::*;
use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryImpl};
use pravega_wire_protocol::connection_pool::ConnectionPool;
use pravega_wire_protocol::connection_pool::ConnectionPoolImpl;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::net::SocketAddr;
use std::process::Command;
use std::sync::Mutex;
use std::{thread, time};
use tokio::runtime::Runtime;
use uuid::Uuid;
// create a static connection pool for using through tests.
lazy_static! {
    static ref CONNECTION_POOL: ConnectionPoolImpl = {
        let cf = Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>;
        let config = ClientConfigBuilder::default()
            .build()
            .expect("build client config");
        let pool = ConnectionPoolImpl::new(cf, config);
        pool
    };
    static ref RUNTIME: Mutex<Runtime> = Mutex::new(Runtime::new().expect("create integration_test runtime"));
}

fn wait_for_standalone_with_timeout(running: bool, timeout: i32) {
    for _i in 0..timeout {
        let output = Command::new("sh")
            .arg("-c")
            .arg("netstat -ltn 2> /dev/null | grep 9090 || ss -ltn 2> /dev/null | grep 9090")
            .output()
            .expect("failed to execute process");
        // if length not zero, controller is listening on port 9090
        let listening = output.stdout.len() != 0;
        if !(running ^ listening) {
            return;
        }
        thread::sleep(time::Duration::from_secs(1));
    }
    panic!(
        "timeout {} exceeded, Pravega standalone is in status {} while expected {}",
        timeout, running, !running
    );
}

#[test]
fn test_wirecommand() {
    let mut pravega = PravegaStandaloneService::start();
    wait_for_standalone_with_timeout(true, 20);

    test_hello();
    test_keep_alive();
    test_setup_append();
    test_create_segment();
    test_seal_segment();
    test_update_and_get_segment_attribute();
    test_get_stream_segment_info();
    test_delete_segment();
    test_conditional_append_and_read_segment();
    test_update_segment_policy();
    test_merge_segment();
    test_truncate_segment();
    test_update_table_entries();
    test_read_table_key();
    test_read_table();
    test_read_table_entries();
    pravega.stop().unwrap();
    wait_for_standalone_with_timeout(false, 10);
}

fn test_hello() {
    let mut rt = RUNTIME.lock().unwrap();
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    // Create scope and stream
    let client = rt.block_on(create_connection("http://127.0.0.1:9090"));
    let mut controller_client = ControllerClientImpl { channel: client };
    let fut = controller_client.create_scope(&scope_name);
    rt.block_on(fut).expect("create scope");
    let request = StreamConfiguration {
        scoped_stream: ScopedStream {
            scope: scope_name.clone(),
            stream: stream_name.clone(),
        },
        scaling: Scaling {
            scale_type: ScaleType::FixedNumSegments,
            target_rate: 0,
            scale_factor: 0,
            min_num_segments: 1,
        },
        retention: Retention {
            retention_type: RetentionType::None,
            retention_param: 0,
        },
    };
    let fut = controller_client.create_stream(&request);
    rt.block_on(fut).expect("create stream");

    // send hello wirecommand to standalone segmentstore
    let request = Requests::Hello(HelloCommand {
        low_version: 5,
        high_version: 9,
    });

    let reply = Replies::Hello(HelloCommand {
        low_version: 5,
        high_version: 9,
    });

    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

fn test_setup_append() {
    let mut rt = RUNTIME.lock().unwrap();
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };
    // send setup_append to standalone SegmentStore
    let sname = segment_name.to_string() + ".#epoch.0";
    let request = Requests::SetupAppend(SetupAppendCommand {
        request_id: 0,
        writer_id: 0,
        segment: sname.clone(),
        delegation_token: String::from(""),
    });

    let reply = Replies::AppendSetup(AppendSetupCommand {
        request_id: 0,
        writer_id: 0,
        segment: sname.clone(),
        last_event_number: i64::min_value(),
    });

    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));

    // A wrong segment name.
    let request = Requests::SetupAppend(SetupAppendCommand {
        request_id: 1,
        writer_id: 1,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
    });

    let reply = Replies::NoSuchSegment(NoSuchSegmentCommand {
        request_id: 1,
        segment: segment_name.to_string(),
        server_stack_trace: String::from(""),
        offset: -1,
    });
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

fn test_create_segment() {
    let mut rt = RUNTIME.lock().unwrap();
    //directly create the segment through raw client.
    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));

    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };
    let request = Requests::CreateSegment(CreateSegmentCommand {
        request_id: 2,
        segment: segment_name.to_string(),
        target_rate: 0,
        scale_type: ScaleType::FixedNumSegments as u8,
        delegation_token: String::from(""),
    });
    let reply = Replies::SegmentCreated(SegmentCreatedCommand {
        request_id: 2,
        segment: segment_name.to_string(),
    });

    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));

    let client = rt.block_on(create_connection("http://127.0.0.1:9090"));
    let mut controller_client = ControllerClientImpl { channel: client };
    let fut = controller_client.get_endpoint_for_segment(&segment_name);
    let endpoint2 = rt
        .block_on(fut)
        .expect("get segment endpoint")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");
    assert_eq!(endpoint, endpoint2);
}

fn test_seal_segment() {
    let mut rt = RUNTIME.lock().unwrap();
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };

    let request = Requests::SealSegment(SealSegmentCommand {
        segment: segment_name.to_string(),
        request_id: 3,
        delegation_token: String::from(""),
    });

    let reply = Replies::SegmentSealed(SegmentSealedCommand {
        request_id: 3,
        segment: segment_name.to_string(),
    });

    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

fn test_update_and_get_segment_attribute() {
    let mut rt = RUNTIME.lock().unwrap();
    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));

    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };
    let sname = segment_name.to_string() + ".#epoch.0";

    let uid = Uuid::new_v4().as_u128();
    let request = Requests::UpdateSegmentAttribute(UpdateSegmentAttributeCommand {
        request_id: 4,
        segment_name: sname.clone(),
        attribute_id: uid,
        new_value: 1,
        expected_value: i64::min_value(),
        delegation_token: String::from(""),
    });
    let reply = Replies::SegmentAttributeUpdated(SegmentAttributeUpdatedCommand {
        request_id: 4,
        success: true,
    });
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));

    let request = Requests::GetSegmentAttribute(GetSegmentAttributeCommand {
        request_id: 5,
        segment_name: sname.clone(),
        attribute_id: uid,
        delegation_token: String::from(""),
    });

    let reply = Replies::SegmentAttribute(SegmentAttributeCommand {
        request_id: 5,
        value: 1,
    });
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

fn test_get_stream_segment_info() {
    let mut rt = RUNTIME.lock().unwrap();
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    let stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };

    //seal this stream.
    let client = rt.block_on(create_connection("http://127.0.0.1:9090"));
    let mut controller_client = ControllerClientImpl { channel: client };
    let fut = controller_client.seal_stream(&stream);
    rt.block_on(fut).expect("seal stream");

    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };
    let sname = segment_name.to_string() + ".#epoch.0";

    let request = Requests::GetStreamSegmentInfo(GetStreamSegmentInfoCommand {
        request_id: 6,
        segment_name: sname.clone(),
        delegation_token: String::from(""),
    });

    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));
    let reply = rt
        .block_on(raw_client.send_request(request))
        .expect("fail to get reply");
    if let Replies::StreamSegmentInfo(info) = reply {
        assert!(info.is_sealed, true);
    } else {
        panic!("Wrong reply type");
    }
}

fn test_delete_segment() {
    let mut rt = RUNTIME.lock().unwrap();
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };

    let request = Requests::DeleteSegment(DeleteSegmentCommand {
        request_id: 7,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
    });
    let reply = Replies::SegmentDeleted(SegmentDeletedCommand {
        request_id: 7,
        segment: segment_name.to_string(),
    });
    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

fn test_conditional_append_and_read_segment() {
    let mut rt = RUNTIME.lock().unwrap();
    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));

    // create a segment.
    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());

    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 0 },
    };

    let request = Requests::CreateSegment(CreateSegmentCommand {
        request_id: 8,
        segment: segment_name.to_string(),
        target_rate: 0,
        scale_type: ScaleType::FixedNumSegments as u8,
        delegation_token: String::from(""),
    });
    rt.block_on(raw_client.send_request(request))
        .expect("create segment");

    // Setup Append.
    let request = Requests::SetupAppend(SetupAppendCommand {
        request_id: 9,
        writer_id: 1,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
    });
    rt.block_on(raw_client.send_request(request))
        .expect("setup append");

    // Conditional Append.
    let data = String::from("event1").into_bytes();
    let test_event = EventCommand { data };
    let request = Requests::ConditionalAppend(ConditionalAppendCommand {
        request_id: 10,
        writer_id: 1,
        event_number: 1,
        expected_offset: 0,
        event: test_event.clone(),
    });
    let reply = Replies::DataAppended(DataAppendedCommand {
        writer_id: 1,
        event_number: 1,
        previous_event_number: i64::min_value(),
        request_id: 10,
        current_segment_write_offset: 14,
    });
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));

    //read the event.
    let request = Requests::ReadSegment(ReadSegmentCommand {
        segment: segment_name.to_string(),
        offset: 0,
        suggested_length: 14,
        delegation_token: String::from(""),
        request_id: 11,
    });

    let data = test_event.write_fields().unwrap();

    let reply = Replies::SegmentRead(SegmentReadCommand {
        segment: segment_name.to_string(),
        offset: 0,
        at_tail: false,
        end_of_segment: false,
        data,
        request_id: 11,
    });

    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

fn test_update_segment_policy() {
    let mut rt = RUNTIME.lock().unwrap();
    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));

    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());

    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 0 },
    };

    let request = Requests::UpdateSegmentPolicy(UpdateSegmentPolicyCommand {
        request_id: 12,
        segment: segment_name.to_string(),
        target_rate: 1,
        scale_type: ScaleType::ByRateInEventsPerSec as u8,
        delegation_token: String::from(""),
    });

    let reply = Replies::SegmentPolicyUpdated(SegmentPolicyUpdatedCommand {
        request_id: 12,
        segment: segment_name.to_string(),
    });
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

fn test_merge_segment() {
    let mut rt = RUNTIME.lock().unwrap();
    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));

    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());

    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 1 },
    };

    let request = Requests::CreateSegment(CreateSegmentCommand {
        request_id: 13,
        segment: segment_name.to_string(),
        target_rate: 0,
        scale_type: ScaleType::FixedNumSegments as u8,
        delegation_token: String::from(""),
    });
    rt.block_on(raw_client.send_request(request))
        .expect("create segment");

    // Setup Append.
    let request = Requests::SetupAppend(SetupAppendCommand {
        request_id: 14,
        writer_id: 2,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
    });
    rt.block_on(raw_client.send_request(request))
        .expect("setup append");

    // Conditional Append.
    let data = String::from("event2").into_bytes();
    let test_event = EventCommand { data };
    let request = Requests::ConditionalAppend(ConditionalAppendCommand {
        request_id: 15,
        writer_id: 2,
        event_number: 1,
        expected_offset: 0,
        event: test_event.clone(),
    });
    let reply = Replies::DataAppended(DataAppendedCommand {
        writer_id: 2,
        event_number: 1,
        previous_event_number: i64::min_value(),
        request_id: 15,
        current_segment_write_offset: 14,
    });
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));

    // Merge with scope/stream/0.
    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());

    let target_segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 0 },
    };

    let request = Requests::MergeSegments(MergeSegmentsCommand {
        request_id: 16,
        source: segment_name.to_string(),
        target: target_segment_name.to_string(),
        delegation_token: String::from(""),
    });

    let reply = Replies::SegmentsMerged(SegmentsMergedCommand {
        request_id: 16,
        target: target_segment_name.to_string(),
        source: segment_name.to_string(),
        new_target_write_offset: 28,
    });

    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

fn test_truncate_segment() {
    let mut rt = RUNTIME.lock().unwrap();
    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));

    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());

    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 0 },
    };
    // truncate the first event1.
    let request = Requests::TruncateSegment(TruncateSegmentCommand {
        request_id: 17,
        segment: segment_name.to_string(),
        truncation_offset: 14,
        delegation_token: String::from(""),
    });
    let reply = Replies::SegmentTruncated(SegmentTruncatedCommand {
        request_id: 17,
        segment: segment_name.to_string(),
    });
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

fn test_update_table_entries() {
    let mut rt = RUNTIME.lock().unwrap();
    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));

    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());
    // create a new segment.
    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 2 },
    };

    let request = Requests::CreateSegment(CreateSegmentCommand {
        request_id: 18,
        segment: segment_name.to_string(),
        target_rate: 0,
        scale_type: ScaleType::FixedNumSegments as u8,
        delegation_token: String::from(""),
    });

    rt.block_on(raw_client.send_request(request))
        .expect("create segment");

    //create a table.
    let mut entries = Vec::new();
    entries.push((
        TableKey::new(String::from("key1").into_bytes(), i64::min_value()),
        TableValue::new(String::from("value1").into_bytes()),
    ));
    entries.push((
        TableKey::new(String::from("key2").into_bytes(), i64::min_value()),
        TableValue::new(String::from("value2").into_bytes()),
    ));
    let table = TableEntries { entries };
    let request = Requests::UpdateTableEntries(UpdateTableEntriesCommand {
        request_id: 19,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
        table_entries: table,
    });
    let mut versions = Vec::new();
    versions.push(0 as i64);
    versions.push(27 as i64); //  why return version is 27.
    let reply = Replies::TableEntriesUpdated(TableEntriesUpdatedCommand {
        request_id: 19,
        updated_versions: versions,
    });

    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));

    //test table key not exist.
    let mut entries = Vec::new();
    entries.push((
        TableKey::new(String::from("key3").into_bytes(), 1),
        TableValue::new(String::from("value3").into_bytes()),
    ));
    let table = TableEntries { entries };
    let request = Requests::UpdateTableEntries(UpdateTableEntriesCommand {
        request_id: 20,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
        table_entries: table,
    });
    let reply = Replies::TableKeyDoesNotExist(TableKeyDoesNotExistCommand {
        request_id: 20,
        segment: segment_name.to_string(),
        server_stack_trace: String::from(""),
    });
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));

    //test table key bad version.
    let mut entries = Vec::new();
    entries.push((
        TableKey::new(String::from("key1").into_bytes(), 10),
        TableValue::new(String::from("value1").into_bytes()),
    ));
    let table = TableEntries { entries };
    let request = Requests::UpdateTableEntries(UpdateTableEntriesCommand {
        request_id: 21,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
        table_entries: table,
    });
    let reply = Replies::TableKeyBadVersion(TableKeyBadVersionCommand {
        request_id: 21,
        segment: segment_name.to_string(),
        server_stack_trace: String::from(""),
    });
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

fn test_read_table_key() {
    let mut rt = RUNTIME.lock().unwrap();
    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));

    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());
    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 2 },
    };

    let request = Requests::ReadTableKeys(ReadTableKeysCommand {
        request_id: 22,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
        suggested_key_count: 2,
        continuation_token: Vec::new(),
    });

    let mut keys = Vec::new();
    keys.push(TableKey::new(String::from("key1").into_bytes(), 0));
    keys.push(TableKey::new(String::from("key2").into_bytes(), 27));

    let reply = rt
        .block_on(raw_client.send_request(request))
        .expect("read table key");

    if let Replies::TableKeysRead(t) = reply {
        assert_eq!(t.segment, segment_name.to_string());
        assert_eq!(t.keys, keys);
    } else {
        panic!("Wrong reply type");
    }
}

fn test_read_table() {
    let mut rt = RUNTIME.lock().unwrap();
    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));

    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());
    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 2 },
    };
    let mut keys = Vec::new();
    keys.push(TableKey::new(String::from("key1").into_bytes(), i64::min_value()));
    keys.push(TableKey::new(String::from("key2").into_bytes(), i64::min_value()));

    let request = Requests::ReadTable(ReadTableCommand {
        request_id: 23,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
        keys,
    });

    let mut entries = Vec::new();
    entries.push((
        TableKey::new(String::from("key1").into_bytes(), 0),
        TableValue::new(String::from("value1").into_bytes()),
    ));
    entries.push((
        TableKey::new(String::from("key2").into_bytes(), 27),
        TableValue::new(String::from("value2").into_bytes()),
    ));
    let table = TableEntries { entries };

    let reply = Replies::TableRead(TableReadCommand {
        request_id: 23,
        segment: segment_name.to_string(),
        entries: table,
    });

    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

fn test_read_table_entries() {
    let mut rt = RUNTIME.lock().unwrap();
    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let raw_client = rt.block_on(RawClientImpl::new(&*CONNECTION_POOL, endpoint));

    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());
    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 2 },
    };

    let request = Requests::ReadTableEntries(ReadTableEntriesCommand {
        request_id: 22,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
        suggested_entry_count: 2,
        continuation_token: Vec::new(),
    });

    let mut entries = Vec::new();
    entries.push((
        TableKey::new(String::from("key1").into_bytes(), 0),
        TableValue::new(String::from("value1").into_bytes()),
    ));
    entries.push((
        TableKey::new(String::from("key2").into_bytes(), 27),
        TableValue::new(String::from("value2").into_bytes()),
    ));
    let table = TableEntries { entries };

    let reply = rt
        .block_on(raw_client.send_request(request))
        .expect("read table entries");
    if let Replies::TableEntriesRead(t) = reply {
        assert_eq!(t.segment, segment_name.to_string());
        assert_eq!(table, t.entries);
    } else {
        panic!("Wrong reply type");
    }
}

// KeepALive would not send back reply.
fn test_keep_alive() {
    let mut rt = RUNTIME.lock().unwrap();
    let endpoint: SocketAddr = "127.0.1.1:6000".parse().expect("fail to parse uri");
    let request = Requests::KeepAlive(KeepAliveCommand {});
    let connection = rt
        .block_on((&*CONNECTION_POOL).get_connection(endpoint))
        .expect("get connection");
    let mut client_connection = ClientConnectionImpl::new(connection);
    rt.block_on(client_connection.write(&request))
        .expect("send request");
}
