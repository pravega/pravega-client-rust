use super::check_standalone_status;
use super::wait_for_standalone_with_timeout;
use crate::pravega_service::{PravegaService, PravegaStandaloneService};
use log::info;
use pravega_client_rust::raw_client::{RawClient, RawClientImpl};
use pravega_client_rust::setup_logger;
use pravega_controller_client::{ControllerClient, ControllerClientImpl};
use pravega_rust_client_retry::retry_async::retry_async;
use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
use pravega_rust_client_retry::retry_result::RetryResult;
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::ClientConfigBuilder;
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::commands::{HelloCommand, SealSegmentCommand};
use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionType};
use pravega_wire_protocol::connection_pool::{ConnectionPool, SegmentConnectionManager};
use pravega_wire_protocol::wire_commands::Requests;
use pravega_wire_protocol::wire_commands::{Encode, Replies};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener};
use std::process::Command;
use std::time::Duration;
use std::{thread, time};
use tokio::runtime::Runtime;

pub async fn disconnection_test_wrapper() {
    test_retry_with_no_connection().await;
    let mut pravega = PravegaStandaloneService::start(false);
    test_retry_while_start_pravega().await;
    assert_eq!(check_standalone_status(), true);
    test_retry_with_unexpected_reply().await;
    pravega.stop().unwrap();
    wait_for_standalone_with_timeout(false, 10);
    test_with_mock_server().await;
}

async fn test_retry_with_no_connection() {
    let retry_policy = RetryWithBackoff::default().max_tries(4);
    // give a wrong endpoint
    let endpoint = "127.0.0.1:12345"
        .parse::<SocketAddr>()
        .expect("Unable to parse socket address");

    let cf = ConnectionFactory::create(ConnectionType::Tokio);
    let manager = SegmentConnectionManager::new(cf, 1);
    let pool = ConnectionPool::new(manager);

    let raw_client = RawClientImpl::new(&pool, endpoint);

    let result = retry_async(retry_policy, || async {
        let request = Requests::Hello(HelloCommand {
            low_version: 5,
            high_version: 9,
        });
        let reply = raw_client.send_request(&request).await;
        match reply {
            Ok(r) => RetryResult::Success(r),
            Err(error) => RetryResult::Retry(error),
        }
    })
    .await;
    if let Err(e) = result {
        assert_eq!(e.tries, 5);
    } else {
        panic!("Test failed.")
    }
}

async fn test_retry_while_start_pravega() {
    let retry_policy = RetryWithBackoff::default().max_tries(10);
    let controller_uri = "127.0.0.1:9090"
        .parse::<SocketAddr>()
        .expect("parse to socketaddr");
    let config = ClientConfigBuilder::default()
        .controller_uri(controller_uri)
        .build()
        .expect("build client config");
    let controller_client = ControllerClientImpl::new(config);

    let scope_name = Scope::new("retryScope".into());

    let result = retry_async(retry_policy, || async {
        let result = controller_client.create_scope(&scope_name).await;
        match result {
            Ok(created) => RetryResult::Success(created),
            Err(error) => RetryResult::Retry(error),
        }
    })
    .await
    .expect("create scope");
    assert!(result, true);

    let stream_name = Stream::new("testStream".into());
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
    let retry_policy = RetryWithBackoff::default().max_tries(10);
    let result = retry_async(retry_policy, || async {
        let result = controller_client.create_stream(&request).await;
        match result {
            Ok(created) => RetryResult::Success(created),
            Err(error) => RetryResult::Retry(error),
        }
    })
    .await
    .expect("create stream");
    assert!(result, true);
}

async fn test_retry_with_unexpected_reply() {
    let retry_policy = RetryWithBackoff::default().max_tries(4);
    let scope_name = Scope::new("retryScope".into());
    let stream_name = Stream::new("retryStream".into());
    let controller_uri = "127.0.0.1:9090"
        .parse::<SocketAddr>()
        .expect("parse to socketaddr");
    let config = ClientConfigBuilder::default()
        .controller_uri(controller_uri)
        .build()
        .expect("build client config");

    let controller_client = ControllerClientImpl::new(config);

    //Get the endpoint.
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment { number: 0 },
    };
    let endpoint = controller_client
        .get_endpoint_for_segment(&segment_name)
        .await
        .expect("get endpoint for segment")
        .parse::<SocketAddr>()
        .expect("convert to socketaddr");

    let cf = ConnectionFactory::create(ConnectionType::Tokio);
    let manager = SegmentConnectionManager::new(cf, 1);
    let pool = ConnectionPool::new(manager);
    let raw_client = RawClientImpl::new(&pool, endpoint);
    let result = retry_async(retry_policy, || async {
        let request = Requests::SealSegment(SealSegmentCommand {
            segment: segment_name.to_string(),
            request_id: 0,
            delegation_token: String::from(""),
        });
        let reply = raw_client.send_request(&request).await;
        match reply {
            Ok(r) => match r {
                Replies::SegmentSealed(_) => RetryResult::Success(r),
                Replies::NoSuchSegment(_) => RetryResult::Retry("No Such Segment"),
                _ => RetryResult::Fail("Wrong reply type"),
            },
            Err(_error) => RetryResult::Retry("Connection Refused"),
        }
    })
    .await;
    if let Err(e) = result {
        assert_eq!(e.error, "No Such Segment");
    } else {
        panic!("Test failed.")
    }
}

struct Server {
    listener: TcpListener,
    address: SocketAddr,
}

impl Server {
    pub fn new() -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").expect("local server");
        let address = listener.local_addr().expect("get listener address");
        Server { address, listener }
    }
}

async fn test_with_mock_server() {
    let server = Server::new();
    let endpoint = server.address;

    thread::spawn(move || {
        for stream in server.listener.incoming() {
            let mut client = stream.expect("get a new client connection");
            let mut buffer = [0u8; 100];
            let _size = client.read(&mut buffer).unwrap();
            let reply = Replies::Hello(HelloCommand {
                high_version: 9,
                low_version: 5,
            });
            let data = reply.write_fields().expect("serialize");
            client.write(&data).expect("send back the reply");
            // close connection immediately to mock the connection failed.
            client.shutdown(Shutdown::Both).expect("shutdown the connection");
        }
        drop(server);
    });

    let cf = ConnectionFactory::create(ConnectionType::Mock);
    let manager = SegmentConnectionManager::new(cf, 3);
    let pool = ConnectionPool::new(manager);

    // test with 3 requests, they should be all succeed.
    for _i in 0..3 {
        let retry_policy = RetryWithBackoff::default().max_tries(5);
        let result = retry_async(retry_policy, || async {
            let connection = pool
                .get_connection(endpoint)
                .await
                .expect("get connection from pool");
            let mut client_connection = ClientConnectionImpl { connection };
            let request = Requests::Hello(HelloCommand {
                high_version: 9,
                low_version: 5,
            });
            let reply = client_connection.write(&request).await;
            if let Err(error) = reply {
                return RetryResult::Retry(error);
            }

            let reply = client_connection.read().await;
            match reply {
                Ok(r) => RetryResult::Success(r),
                Err(error) => RetryResult::Retry(error),
            }
        })
        .await;

        if let Ok(r) = result {
            println!("reply is {:?}", r);
        } else {
            panic!("Test failed.")
        }
    }
}
