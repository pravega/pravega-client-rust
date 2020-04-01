use super::pravega_service::PravegaStandaloneService;
use crate::pravega_service::PravegaService;
use pravega_client_rust::event_stream_writer::{EventStreamWriter, Processor};
use pravega_client_rust::setup_logger;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::ClientConfigBuilder;
use pravega_wire_protocol::commands::MergeSegmentsCommand;
use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryImpl};
use pravega_wire_protocol::connection_pool::{ConnectionPool, SegmentConnectionManager};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::net::SocketAddr;

use log::info;
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::wire_commands::Requests::SealSegment;
use std::process::Command;
use std::{thread, time};

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
    // if length is not zero, controller is listening on port 9090
    let listening = !output.stdout.is_empty();
    listening
}

#[tokio::test(core_threads = 4)]
async fn test_event_stream_writer() {
    setup_logger();
    // spin up Pravega standalone
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());

    let mut pravega = PravegaStandaloneService::start(false);

    wait_for_standalone_with_timeout(true, 20);

    let controller_client = setup_test(&scope_name, &stream_name).await;

    let pool = get_connection_pool_for_segment().await;

    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };

    let (mut writer, processor) = EventStreamWriter::new(
        scoped_stream.clone(),
        ClientConfigBuilder::default()
            .build()
            .expect("build client config"),
    )
    .await;
    tokio::spawn(Processor::run(processor, Box::new(controller_client), pool));

    test_simple_write(&mut writer).await;

    test_segment_sealed(&mut writer, &scoped_stream).await;

    // Shut down Pravega standalone
    pravega.stop().unwrap();
    wait_for_standalone_with_timeout(false, 5);
}

async fn test_simple_write(writer: &mut EventStreamWriter) {
    info!("test simple write");
    let mut receivers = vec![];
    let count = 10;
    let mut i = 0;
    while i < count {
        let rx = writer.write_event(String::from("hello").into_bytes()).await;
        receivers.push(rx);
        i += 1;
    }
    assert_eq!(receivers.len(), count);

    for rx in receivers {
        let _reply = rx.await.expect("wait for result from oneshot");
    }
    info!("test simple write passed");
}

async fn test_segment_sealed(writer: &mut EventStreamWriter, stream: &ScopedStream) {
    let controller_client = ControllerClientImpl::new(
        "127.0.0.1:9090"
            .parse::<SocketAddr>()
            .expect("parse to socketaddr"),
    );
    let pool = get_connection_pool_for_segment().await;
    let mut segments = controller_client
        .get_current_segments(&stream)
        .await
        .expect("get current segments")
        .get_segments();
    let from = segments.pop().expect("get source segment");
    let to = segments.pop().expect("get target segment");
    let endpoint = controller_client
        .get_endpoint_for_segment(&from)
        .await
        .expect("get endpoint")
        .0
        .parse::<SocketAddr>()
        .expect("parse to socketaddr");

    let mut receivers = vec![];
    let count = 1000;
    let mut i = 0;
    while i < count {
        let rx = writer.write_event(String::from("hello").into_bytes()).await;
        receivers.push(rx);
        i += 1;
    }
    assert_eq!(receivers.len(), count);

    let cmd = Requests::MergeSegments(MergeSegmentsCommand {
        request_id: 0,
        target: to.to_string(),
        source: from.to_string(),
        delegation_token: "".to_string(),
    });

    let conn = pool
        .get_connection(endpoint)
        .await
        .expect("get connection from pool");
    let mut client_connection = ClientConnectionImpl::new(conn);
    client_connection.write(&cmd).await.expect("write to segment");
    let _reply = client_connection.read().await.expect("read from segment");

    for rx in receivers {
        let _reply = rx.await.expect("wait for result from oneshot");
    }
}

// helper function
async fn setup_test(scope_name: &Scope, stream_name: &Stream) -> ControllerClientImpl {
    let controller_client = ControllerClientImpl::new(
        "127.0.0.1:9090"
            .parse::<SocketAddr>()
            .expect("parse to socketaddr"),
    );

    controller_client
        .create_scope(scope_name)
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
            min_num_segments: 2,
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
    controller_client
}

async fn get_connection_pool_for_segment() -> ConnectionPool<SegmentConnectionManager> {
    let config = ClientConfigBuilder::default()
        .build()
        .expect("build client config");
    let cf = Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>;
    let manager = SegmentConnectionManager::new(cf, config);
    ConnectionPool::new(manager)
}
