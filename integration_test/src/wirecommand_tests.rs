use super::pravega_service::PravegaStandaloneService;
use crate::pravega_service::PravegaService;
use lazy_static::*;
use pravega_client_rust::raw_client::RawClientImpl;
use pravega_controller_client::{
    create_connection, ControllerClient, ControllerClientImpl, ControllerConnectionManager,
};
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::ClientConfigBuilder;
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::commands::Command as WireCommand;
use pravega_wire_protocol::commands::*;
use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryImpl};
use pravega_wire_protocol::connection_pool::{ConnectionPool, SegmentConnectionManager};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::net::SocketAddr;
use std::process::Command;
use std::{thread, time};
use tokio::runtime::Runtime;
use tokio::time::timeout;
use uuid::Uuid;

// create a static connection pool for using through tests.
lazy_static! {
    static ref CONNECTION_POOL: ConnectionPool<SegmentConnectionManager> = {
        let cf = Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>;
        let config = ClientConfigBuilder::default()
            .build()
            .expect("build client config");
        let manager = SegmentConnectionManager::new(cf, config);
        let pool = ConnectionPool::new(manager);
        pool
    };
    static ref CONTROLLER_CLIENT: ControllerClientImpl = {
        ControllerClientImpl::new(
            "127.0.0.1:9090"
                .parse::<SocketAddr>()
                .expect("parse to socketaddr"),
        )
    };
}

fn wait_for_standalone_with_timeout(expected_status: bool, timeout_second: i32) {
    for _i in 0..timeout_second {
        if expected_status == check_standalone_status() {
            return;
        }
        thread::sleep(time::Duration::from_secs(1));
    }
    panic!(
        "timeout {} exceeded, Pravega standalone is in status {} while expected {}",
        timeout_second, !expected_status, expected_status
    );
}
fn check_standalone_status() -> bool {
    let output = Command::new("sh")
        .arg("-c")
        .arg("netstat -ltn 2> /dev/null | grep 9090 || ss -ltn 2> /dev/null | grep 9090")
        .output()
        .expect("failed to execute process");
    // if length not zero, controller is listening on port 9090
    let listening = output.stdout.len() != 0;
    listening
}
#[test]
fn test_wirecommand() {
    let mut pravega = PravegaStandaloneService::start(false);
    wait_for_standalone_with_timeout(true, 20);
    let mut rt = Runtime::new().unwrap();
    let timeout_second = time::Duration::from_secs(30);
    rt.block_on(async {
        timeout(timeout_second, test_hello()).await.unwrap();
    });
    rt.block_on(async {
        timeout(timeout_second, test_keep_alive()).await.unwrap();
    });
    rt.block_on(async {
        timeout(timeout_second, test_setup_append()).await.unwrap();
    });
    rt.block_on(async {
        timeout(timeout_second, test_create_segment()).await.unwrap();
    });
    rt.block_on(async {
        timeout(timeout_second, test_seal_segment()).await.unwrap();
    });
    rt.block_on(async {
        timeout(timeout_second, test_update_and_get_segment_attribute())
            .await
            .unwrap();
    });

    rt.block_on(async {
        timeout(timeout_second, test_get_stream_segment_info())
            .await
            .unwrap();
    });

    rt.block_on(async {
        timeout(timeout_second, test_delete_segment()).await.unwrap();
    });
    rt.block_on(async {
        timeout(timeout_second, test_conditional_append_and_read_segment())
            .await
            .unwrap();
    });
    rt.block_on(async {
        timeout(timeout_second, test_update_segment_policy())
            .await
            .unwrap();
    });
    rt.block_on(async {
        timeout(timeout_second, test_merge_segment()).await.unwrap();
    });
    rt.block_on(async {
        timeout(timeout_second, test_truncate_segment()).await.unwrap();
    });
    rt.block_on(async {
        timeout(timeout_second, test_update_table_entries())
            .await
            .unwrap();
    });
    rt.block_on(async {
        timeout(timeout_second, test_read_table_key()).await.unwrap();
    });
    rt.block_on(async {
        timeout(timeout_second, test_read_table()).await.unwrap();
    });
    rt.block_on(async {
        timeout(timeout_second, test_read_table_entries()).await.unwrap();
    });
    pravega.stop().unwrap();
    wait_for_standalone_with_timeout(false, 10);
}

async fn test_hello() {
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    // Create scope and stream
    CONTROLLER_CLIENT
        .create_scope(&scope_name)
        .await
        .expect("create scope");

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
    CONTROLLER_CLIENT
        .create_stream(&request)
        .await
        .expect("create stream");
    //Get the endpoint.
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };
    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    // send hello to Pravega standalone
    let request = Requests::Hello(HelloCommand {
        low_version: 5,
        high_version: 9,
    });

    let reply = Replies::Hello(HelloCommand {
        low_version: 5,
        high_version: 9,
    });

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;
    raw_client
        .send_request(request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

// KeepAlive would not send back reply.
async fn test_keep_alive() {
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let request = Requests::KeepAlive(KeepAliveCommand {});
    let connection = (&*CONNECTION_POOL)
        .get_connection(endpoint)
        .await
        .expect("get connection");
    let mut client_connection = ClientConnectionImpl::new(connection);
    client_connection.write(&request).await.expect("send request");
}

async fn test_setup_append() {
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    // send setup_append to standalone SegmentStore
    let sname = segment_name.to_string();
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

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;
    raw_client
        .send_request(request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));

    // A wrong segment name.
    let request = Requests::SetupAppend(SetupAppendCommand {
        request_id: 1,
        writer_id: 1,
        segment: segment_name.to_string() + "foo",
        delegation_token: String::from(""),
    });

    let reply = Replies::NoSuchSegment(NoSuchSegmentCommand {
        request_id: 1,
        segment: segment_name.to_string() + "foo",
        server_stack_trace: String::from(""),
        offset: -1,
    });
    raw_client
        .send_request(request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_create_segment() {
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 1 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

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

    raw_client
        .send_request(request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_seal_segment() {
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 1 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

    let request = Requests::SealSegment(SealSegmentCommand {
        segment: segment_name.to_string(),
        request_id: 3,
        delegation_token: String::from(""),
    });

    let reply = Replies::SegmentSealed(SegmentSealedCommand {
        request_id: 3,
        segment: segment_name.to_string(),
    });

    raw_client
        .send_request(request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_update_and_get_segment_attribute() {
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };
    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

    let sname = segment_name.to_string();
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
    raw_client
        .send_request(request)
        .await
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
    raw_client
        .send_request(request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_get_stream_segment_info() {
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    let stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };

    //seal this stream.
    CONTROLLER_CLIENT.seal_stream(&stream).await.expect("seal stream");

    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

    let sname = segment_name.to_string();
    let request = Requests::GetStreamSegmentInfo(GetStreamSegmentInfoCommand {
        request_id: 6,
        segment_name: sname.clone(),
        delegation_token: String::from(""),
    });
    let reply = raw_client.send_request(request).await.expect("fail to get reply");
    if let Replies::StreamSegmentInfo(info) = reply {
        assert!(info.is_sealed, true);
    } else {
        panic!("Wrong reply type");
    }
}

async fn test_delete_segment() {
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

    let request = Requests::DeleteSegment(DeleteSegmentCommand {
        request_id: 7,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
    });
    let reply = Replies::SegmentDeleted(SegmentDeletedCommand {
        request_id: 7,
        segment: segment_name.to_string(),
    });
    raw_client
        .send_request(request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_conditional_append_and_read_segment() {
    // create a segment.
    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());

    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 0 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

    let request = Requests::CreateSegment(CreateSegmentCommand {
        request_id: 8,
        segment: segment_name.to_string(),
        target_rate: 0,
        scale_type: ScaleType::FixedNumSegments as u8,
        delegation_token: String::from(""),
    });
    raw_client.send_request(request).await.expect("create segment");

    // Setup Append.
    let request = Requests::SetupAppend(SetupAppendCommand {
        request_id: 9,
        writer_id: 1,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
    });
    raw_client.send_request(request).await.expect("setup append");

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
    raw_client
        .send_request(request)
        .await
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

    raw_client
        .send_request(request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_update_segment_policy() {
    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());

    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 0 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

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
    raw_client
        .send_request(request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_merge_segment() {
    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());

    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 1 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

    let request = Requests::CreateSegment(CreateSegmentCommand {
        request_id: 13,
        segment: segment_name.to_string(),
        target_rate: 0,
        scale_type: ScaleType::FixedNumSegments as u8,
        delegation_token: String::from(""),
    });

    raw_client.send_request(request).await.expect("create segment");

    // Setup Append.
    let request = Requests::SetupAppend(SetupAppendCommand {
        request_id: 14,
        writer_id: 2,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
    });
    raw_client.send_request(request).await.expect("setup append");

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
    raw_client
        .send_request(request)
        .await
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

    raw_client
        .send_request(request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_truncate_segment() {
    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());

    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 0 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

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
    raw_client
        .send_request(request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_update_table_entries() {
    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());
    // create a new segment.
    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 2 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

    let request = Requests::CreateSegment(CreateSegmentCommand {
        request_id: 18,
        segment: segment_name.to_string(),
        target_rate: 0,
        scale_type: ScaleType::FixedNumSegments as u8,
        delegation_token: String::from(""),
    });

    raw_client.send_request(request).await.expect("create segment");

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

    raw_client
        .send_request(request)
        .await
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
    raw_client
        .send_request(request)
        .await
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
    raw_client
        .send_request(request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_read_table_key() {
    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());
    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 2 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

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

    let reply = raw_client.send_request(request).await.expect("read table key");

    if let Replies::TableKeysRead(t) = reply {
        assert_eq!(t.segment, segment_name.to_string());
        assert_eq!(t.keys, keys);
    } else {
        panic!("Wrong reply type");
    }
}

async fn test_read_table() {
    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());
    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 2 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

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

    raw_client
        .send_request(request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_read_table_entries() {
    let scope_name = Scope::new("scope".into());
    let stream_name = Stream::new("stream".into());
    let segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment { number: 2 },
    };

    let endpoint = CONTROLLER_CLIENT
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

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

    let reply = raw_client
        .send_request(request)
        .await
        .expect("read table entries");
    if let Replies::TableEntriesRead(t) = reply {
        assert_eq!(t.segment, segment_name.to_string());
        assert_eq!(table, t.entries);
    } else {
        panic!("Wrong reply type");
    }
}
