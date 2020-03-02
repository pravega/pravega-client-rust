use super::pravega_service::PravegaStandaloneService;
use crate::pravega_service::PravegaService;
use pravega_client_rust::raw_client::RawClientImpl;
use pravega_controller_client::{create_connection, ControllerClient, ControllerClientImpl};
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::ClientConfigBuilder;
use pravega_wire_protocol::commands::{HelloCommand, SetupAppendCommand};
use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryImpl};
use pravega_wire_protocol::connection_pool::ConnectionPoolImpl;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::net::SocketAddr;
use std::{thread, time};
use tokio::runtime::Runtime;

#[test]
fn test_start_pravega_standalone() {
    let mut pravega = PravegaStandaloneService::start();
    let two_secs = time::Duration::from_secs(20);
    thread::sleep(two_secs);
    assert_eq!(true, pravega.check_status().unwrap());
    pravega.stop().unwrap();
    thread::sleep(two_secs);
    assert_eq!(false, pravega.check_status().unwrap());
}

#[test]
fn test_raw_client() {
    let mut rt = Runtime::new().expect("create runtime");

    // spin up Pravega standalone
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());

    let mut pravega = PravegaStandaloneService::start();
    thread::sleep(time::Duration::from_secs(20));
    assert_eq!(true, pravega.check_status().unwrap());

    // Create scope and stream
    let client = rt.block_on(create_connection("http://[::1]:9090"));
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

    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };
    let fut = controller_client.get_endpoint_for_segment(&segment_name);
    let endpoint = rt
        .block_on(fut)
        .expect("get segment endpoint")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    // create raw client
    let cf = Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>;
    let config = ClientConfigBuilder::default()
        .build()
        .expect("build client config");
    let pool = ConnectionPoolImpl::new(cf, config);
    let raw_client = rt.block_on(RawClientImpl::new(&pool, endpoint));

    // write hello to standalone controller
    let request = Requests::Hello(HelloCommand {
        low_version: 5,
        high_version: 9,
    });

    let reply = Replies::Hello(HelloCommand {
        low_version: 5,
        high_version: 9,
    });
    rt.block_on(raw_client.send_request(request))
        .map_or_else(|e| panic!("failed to get reply: {}", e), |r| assert_eq!(reply, r));
    pravega.stop().unwrap();
    thread::sleep(time::Duration::from_secs(5));
    assert_eq!(false, pravega.check_status().unwrap());
}
