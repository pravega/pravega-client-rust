//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
use super::check_standalone_status;
use super::wait_for_standalone_with_timeout;
use crate::pravega_service::{PravegaService, PravegaStandaloneService, PravegaStandaloneServiceConfig};
use pravega_client::client_factory::ClientFactory;
use pravega_client::raw_client::{RawClient, RawClientImpl};
use pravega_client_config::connection_type::MockType;
use pravega_client_config::{connection_type::ConnectionType, ClientConfigBuilder, MOCK_CONTROLLER_URI};
use pravega_client_retry::retry_async::retry_async;
use pravega_client_retry::retry_policy::RetryWithBackoff;
use pravega_client_retry::retry_result::RetryResult;
use pravega_client_shared::*;
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_controller_client::{ControllerClient, ControllerClientImpl, ControllerError};
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::commands::{HelloCommand, SealSegmentCommand};
use pravega_wire_protocol::connection_factory::{
    ConnectionFactory, ConnectionFactoryConfig, SegmentConnectionManager,
};
use pravega_wire_protocol::wire_commands::Requests;
use pravega_wire_protocol::wire_commands::{Encode, Replies};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener};
use std::process::Command;
use std::time::Duration;
use std::{thread, time};
use tokio::runtime::Runtime;
use tracing::info;

pub fn disconnection_test_wrapper() {
    let mut rt = tokio::runtime::Runtime::new().expect("create runtime");
    rt.block_on(test_retry_with_no_connection());
    rt.shutdown_timeout(Duration::from_millis(100));

    let config = PravegaStandaloneServiceConfig::new(false, false, false);
    let mut pravega = PravegaStandaloneService::start(config);
    test_retry_while_start_pravega();
    assert_eq!(check_standalone_status(), true);
    test_retry_with_unexpected_reply();
    pravega.stop().unwrap();
    wait_for_standalone_with_timeout(false, 10);

    let mut rt = tokio::runtime::Runtime::new().expect("create runtime");
    rt.block_on(test_with_mock_server());
}

async fn test_retry_with_no_connection() {
    let retry_policy = RetryWithBackoff::default().max_tries(4);
    // give a wrong endpoint
    let endpoint = PravegaNodeUri::from("127.0.0.1:0");
    let config = ConnectionFactoryConfig::new(ConnectionType::Tokio);
    let cf = ConnectionFactory::create(config);
    let manager = SegmentConnectionManager::new(cf, 1);
    let pool = ConnectionPool::new(manager);

    let raw_client = RawClientImpl::new(&pool, endpoint, Duration::from_secs(3600));

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

fn test_retry_while_start_pravega() {
    let config = ClientConfigBuilder::default()
        .controller_uri(PravegaNodeUri::from("127.0.0.1:9090"))
        .build()
        .expect("build client config");
    let cf = ClientFactory::new(config);
    let controller_client = cf.get_controller_client();

    cf.get_runtime_handle()
        .block_on(create_scope_stream(controller_client));
}

async fn create_scope_stream(controller_client: &dyn ControllerClient) {
    let retry_policy = RetryWithBackoff::default().max_tries(10);
    let scope_name = Scope::from("retryScope".to_owned());

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

    let stream_name = Stream::from("testStream".to_owned());
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

fn test_retry_with_unexpected_reply() {
    let retry_policy = RetryWithBackoff::default().max_tries(4);
    let scope_name = Scope::from("retryScope".to_owned());
    let stream_name = Stream::from("retryStream".to_owned());
    let config = ClientConfigBuilder::default()
        .controller_uri(PravegaNodeUri::from("127.0.0.1:9090"))
        .connection_type(ConnectionType::Tokio)
        .build()
        .expect("build client config");
    let cf = ClientFactory::new(config.clone());
    let controller_client = cf.get_controller_client();

    //Get the endpoint.
    let segment_name = ScopedSegment {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
        segment: Segment {
            number: 0,
            tx_id: None,
        },
    };
    let endpoint = cf
        .get_runtime_handle()
        .block_on(controller_client.get_endpoint_for_segment(&segment_name))
        .expect("get endpoint for segment");

    let connection_factory = ConnectionFactory::create(ConnectionFactoryConfig::from(&config));
    let manager = SegmentConnectionManager::new(connection_factory, 1);
    let pool = ConnectionPool::new(manager);
    let raw_client = RawClientImpl::new(&pool, endpoint, Duration::from_secs(3600));
    let result = cf
        .get_runtime_handle()
        .block_on(retry_async(retry_policy, || async {
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
        }));
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
    let endpoint = PravegaNodeUri::from(format!("{}:{}", server.address.ip(), server.address.port()));
    let config = ConnectionFactoryConfig::new(ConnectionType::Mock(MockType::Happy));
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

    let cf = ConnectionFactory::create(config);
    let manager = SegmentConnectionManager::new(cf, 3);
    let pool = ConnectionPool::new(manager);

    // test with 3 requests, they should be all succeed.
    for _i in 0i32..3 {
        let retry_policy = RetryWithBackoff::default().max_tries(5);
        let result = retry_async(retry_policy, || async {
            let connection = pool
                .get_connection(endpoint.clone())
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
