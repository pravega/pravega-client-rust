//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use lazy_static::*;
use pravega_client_rust::client_factory::ClientFactory;
use pravega_client_rust::raw_client::RawClient;
use pravega_client_rust::raw_client::RawClientImpl;
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_rust_client_config::{ClientConfig, ClientConfigBuilder, MOCK_CONTROLLER_URI};
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::commands::Command as WireCommand;
use pravega_wire_protocol::commands::*;
use pravega_wire_protocol::connection_factory::{
    ConnectionFactory, ConnectionFactoryConfig, SegmentConnectionManager,
};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time;
use tokio::runtime::{Handle, Runtime};
use tokio::time::timeout;
use uuid::Uuid;

// create a static connection pool for using through tests.
lazy_static! {
    static ref CONNECTION_POOL: ConnectionPool<SegmentConnectionManager> = {
        let config = ClientConfigBuilder::default()
            .controller_uri(MOCK_CONTROLLER_URI)
            .build()
            .expect("build client config");
        let cf = ConnectionFactory::create(ConnectionFactoryConfig::from(&config));
        let manager = SegmentConnectionManager::new(cf, config.max_connections_in_pool);
        ConnectionPool::new(manager)
    };
}

pub fn wirecommand_test_wrapper() {
    let config = ClientConfigBuilder::default()
        .controller_uri(MOCK_CONTROLLER_URI)
        .build()
        .expect("build client config");
    let cf = ClientFactory::new(config);
    let h = cf.get_runtime_handle();
    h.block_on(wirecommand_tests(&cf));
}

pub async fn wirecommand_tests(factory: &ClientFactory) {
    let timeout_second = time::Duration::from_secs(30);

    timeout(timeout_second, test_hello(factory)).await.unwrap();

    timeout(timeout_second, test_keep_alive(factory)).await.unwrap();

    timeout(timeout_second, test_setup_append(factory)).await.unwrap();

    timeout(timeout_second, test_create_segment(factory))
        .await
        .unwrap();

    timeout(timeout_second, test_update_and_get_segment_attribute(factory))
        .await
        .unwrap();

    timeout(timeout_second, test_get_stream_segment_info(factory))
        .await
        .unwrap();

    timeout(timeout_second, test_seal_segment(factory)).await.unwrap();

    timeout(timeout_second, test_delete_segment(factory))
        .await
        .unwrap();

    timeout(timeout_second, test_conditional_append_and_read_segment(factory))
        .await
        .unwrap();

    timeout(timeout_second, test_update_segment_policy(factory))
        .await
        .unwrap();

    timeout(timeout_second, test_merge_segment(factory))
        .await
        .unwrap();

    timeout(timeout_second, test_truncate_segment(factory))
        .await
        .unwrap();

    timeout(timeout_second, test_update_table_entries(factory))
        .await
        .unwrap();

    timeout(timeout_second, test_read_table_key(factory))
        .await
        .unwrap();

    timeout(timeout_second, test_read_table(factory)).await.unwrap();

    timeout(timeout_second, test_read_table_entries(factory))
        .await
        .unwrap();
}

async fn test_hello(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("testScope".to_owned());
    let stream_name = Stream::from("testStream".to_owned());
    // Create scope and stream

    controller_client
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
    controller_client
        .create_stream(&request)
        .await
        .expect("create stream");
    //Get the endpoint.
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(0),
    };
    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    // send hello to Pravega standalone
    let request = Requests::Hello(HelloCommand {
        low_version: 5,
        high_version: 10,
    });

    let reply = Replies::Hello(HelloCommand {
        low_version: 5,
        high_version: 10,
    });

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );
    raw_client
        .send_request(&request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

// KeepAlive would not send back reply.
async fn test_keep_alive(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("testScope".to_owned());
    let stream_name = Stream::from("testStream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(0),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let request = Requests::KeepAlive(KeepAliveCommand {});
    let connection = (&*CONNECTION_POOL)
        .get_connection(endpoint)
        .await
        .expect("get connection");
    let mut client_connection = ClientConnectionImpl::new(connection);
    client_connection.write(&request).await.expect("send request");
}

async fn test_setup_append(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("testScope".to_owned());
    let stream_name = Stream::from("testStream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(0),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

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

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );
    raw_client
        .send_request(&request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));

    // A wrong segment name.
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(1),
    };
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
        .send_request(&request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_create_segment(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("testScope".to_owned());
    let stream_name = Stream::from("testStream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(1),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );

    let request1 = Requests::CreateSegment(CreateSegmentCommand {
        request_id: 1,
        segment: segment_name.to_string(),
        target_rate: 0,
        scale_type: ScaleType::FixedNumSegments as u8,
        delegation_token: String::from(""),
    });
    let reply1 = Replies::SegmentCreated(SegmentCreatedCommand {
        request_id: 1,
        segment: segment_name.to_string(),
    });
    let request2 = Requests::CreateSegment(CreateSegmentCommand {
        request_id: 2,
        segment: segment_name.to_string(),
        target_rate: 0,
        scale_type: ScaleType::FixedNumSegments as u8,
        delegation_token: String::from(""),
    });
    let reply2 = Replies::SegmentAlreadyExists(SegmentAlreadyExistsCommand {
        request_id: 2,
        segment: segment_name.to_string(),
        server_stack_trace: "".to_string(),
    });

    raw_client.send_request(&request1).await.map_or_else(
        |e| panic!("failed to get reply: {}", e),
        |r| assert_eq!(reply1, r),
    );
    raw_client.send_request(&request2).await.map_or_else(
        |e| panic!("failed to get reply: {}", e),
        |r| assert_eq!(reply2, r),
    );
}

async fn test_seal_segment(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("testScope".to_owned());
    let stream_name = Stream::from("testStream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(1),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );

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
        .send_request(&request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_update_and_get_segment_attribute(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("testScope".to_owned());
    let stream_name = Stream::from("testStream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(0),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );

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
        .send_request(&request)
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
        .send_request(&request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_get_stream_segment_info(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("testScope".to_owned());
    let stream_name = Stream::from("testStream".to_owned());
    let stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };

    //seal this stream.
    controller_client.seal_stream(&stream).await.expect("seal stream");

    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(0),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );

    let sname = segment_name.to_string();
    let request = Requests::GetStreamSegmentInfo(GetStreamSegmentInfoCommand {
        request_id: 6,
        segment_name: sname.clone(),
        delegation_token: String::from(""),
    });
    let reply = raw_client
        .send_request(&request)
        .await
        .expect("fail to get reply");
    if let Replies::StreamSegmentInfo(info) = reply {
        assert!(info.is_sealed, true);
    } else {
        panic!("Wrong reply type");
    }
}

async fn test_delete_segment(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("testScope".to_owned());
    let stream_name = Stream::from("testStream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(0),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );

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
        .send_request(&request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_conditional_append_and_read_segment(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("scope".to_owned());
    let stream_name = Stream::from("stream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(0),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );

    let request = Requests::CreateSegment(CreateSegmentCommand {
        request_id: 8,
        segment: segment_name.to_string(),
        target_rate: 0,
        scale_type: ScaleType::FixedNumSegments as u8,
        delegation_token: String::from(""),
    });
    raw_client.send_request(&request).await.expect("create segment");

    // Setup Append.
    let request = Requests::SetupAppend(SetupAppendCommand {
        request_id: 9,
        writer_id: 1,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
    });
    raw_client.send_request(&request).await.expect("setup append");

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
        .send_request(&request)
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
        .send_request(&request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_update_segment_policy(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("scope".to_owned());
    let stream_name = Stream::from("stream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(0),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );

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
        .send_request(&request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_merge_segment(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("scope".to_owned());
    let stream_name = Stream::from("stream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(1),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );

    let request = Requests::CreateSegment(CreateSegmentCommand {
        request_id: 13,
        segment: segment_name.to_string(),
        target_rate: 0,
        scale_type: ScaleType::FixedNumSegments as u8,
        delegation_token: String::from(""),
    });

    raw_client.send_request(&request).await.expect("create segment");

    // Setup Append.
    let request = Requests::SetupAppend(SetupAppendCommand {
        request_id: 14,
        writer_id: 2,
        segment: segment_name.to_string(),
        delegation_token: String::from(""),
    });
    raw_client.send_request(&request).await.expect("setup append");

    // Conditional Append.
    let data = String::from("event2").into_bytes();
    let test_event = EventCommand { data };
    let request = Requests::ConditionalAppend(ConditionalAppendCommand {
        request_id: 15,
        writer_id: 2,
        event_number: 1,
        expected_offset: 0,
        event: test_event,
    });
    let reply = Replies::DataAppended(DataAppendedCommand {
        writer_id: 2,
        event_number: 1,
        previous_event_number: i64::min_value(),
        request_id: 15,
        current_segment_write_offset: 14,
    });
    raw_client
        .send_request(&request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));

    // Merge with scope/stream/0.
    let scope_name = Scope::from("scope".to_owned());
    let stream_name = Stream::from("stream".to_owned());

    let target_segment_name = ScopedSegment {
        scope: scope_name,
        stream: stream_name,
        segment: Segment::from(0),
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
        .send_request(&request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_truncate_segment(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("scope".to_owned());
    let stream_name = Stream::from("stream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(0),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );

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
        .send_request(&request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_update_table_entries(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("scope".to_owned());
    let stream_name = Stream::from("stream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(2),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );

    let request = Requests::CreateSegment(CreateSegmentCommand {
        request_id: 18,
        segment: segment_name.to_string(),
        target_rate: 0,
        scale_type: ScaleType::FixedNumSegments as u8,
        delegation_token: String::from(""),
    });

    raw_client.send_request(&request).await.expect("create segment");

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
        table_segment_offset: -1,
    });
    let mut versions = Vec::new();
    versions.push(0 as i64);
    versions.push(27 as i64); //  why return version is 27.
    let reply = Replies::TableEntriesUpdated(TableEntriesUpdatedCommand {
        request_id: 19,
        updated_versions: versions,
    });

    raw_client
        .send_request(&request)
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
        table_segment_offset: -1,
    });
    let reply = Replies::TableKeyDoesNotExist(TableKeyDoesNotExistCommand {
        request_id: 20,
        segment: segment_name.to_string(),
        server_stack_trace: String::from(""),
    });
    raw_client
        .send_request(&request)
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
        table_segment_offset: -1,
    });
    let reply = Replies::TableKeyBadVersion(TableKeyBadVersionCommand {
        request_id: 21,
        segment: segment_name.to_string(),
        server_stack_trace: String::from(""),
    });
    raw_client
        .send_request(&request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_read_table_key(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("scope".to_owned());
    let stream_name = Stream::from("stream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(2),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );

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

    let reply = raw_client.send_request(&request).await.expect("read table key");

    if let Replies::TableKeysRead(t) = reply {
        assert_eq!(t.segment, segment_name.to_string());
        assert_eq!(t.keys, keys);
    } else {
        panic!("Wrong reply type");
    }
}

async fn test_read_table(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("scope".to_owned());
    let stream_name = Stream::from("stream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(2),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );

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
        .send_request(&request)
        .await
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
}

async fn test_read_table_entries(factory: &ClientFactory) {
    let controller_client = factory.get_controller_client();
    let scope_name = Scope::from("scope".to_owned());
    let stream_name = Stream::from("stream".to_owned());
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment::from(2),
    };

    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment");

    let raw_client = RawClientImpl::new(
        &*CONNECTION_POOL,
        endpoint,
        factory.get_config().request_timeout(),
    );

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
        .send_request(&request)
        .await
        .expect("read table entries");
    if let Replies::TableEntriesRead(t) = reply {
        assert_eq!(t.segment, segment_name.to_string());
        assert_eq!(table, t.entries);
    } else {
        panic!("Wrong reply type");
    }
}
