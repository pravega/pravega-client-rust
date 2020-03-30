use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
use pravega_rust_client_retry::retry_async::retry_async;
use pravega_client_rust::raw_client::RawClientImpl;
use pravega_wire_protocol::client_config::ClientConfigBuilder;
use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryImpl};
use pravega_wire_protocol::connection_pool::ConnectionPoolImpl;
use std::net::SocketAddr;
use pravega_wire_protocol::wire_commands::Requests;
use pravega_wire_protocol::wire_commands::Replies;
use pravega_wire_protocol::commands::{HelloCommand, SealSegmentCommand};
use pravega_rust_client_retry::retry_result::RetryResult;
use std::{thread, time};
use std::process::Command;
use pravega_controller_client::{ControllerClientImpl, ControllerClient, create_connection};
use tokio::runtime::Runtime;
use crate::pravega_service::{PravegaStandaloneService, PravegaService};
use pravega_rust_client_shared::*;
use lazy_static::*;

lazy_static! {
    static ref CONNECTION_POOL: ConnectionPoolImpl = {
        let cf = Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>;
        let config = ClientConfigBuilder::default()
            .build()
            .expect("build client config");
        let pool = ConnectionPoolImpl::new(cf, config);
        pool
    };
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

#[test]
fn test_wrapper() {
    let mut rt = Runtime::new().unwrap();
    rt.block_on(test_retry_with_no_connection());
    let mut pravega = PravegaStandaloneService::start();
    rt.block_on(test_retry_while_start_pravega());
    assert_eq!(check_standalone_status(), true);
    rt.block_on(test_retry_with_unexpected_reply());
    pravega.stop().unwrap();
    wait_for_standalone_with_timeout(false, 10);
}

async fn test_retry_with_no_connection() {
    let retry_policy = RetryWithBackoff::default().max_tries(4);
    // give a wrong endpoint
    let endpoint = "127.0.0.1:12345".parse::<SocketAddr>().expect("Unable to parse socket address");
    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

    let result = retry_async(retry_policy, || async {
        let request = Requests::Hello(HelloCommand {
            low_version: 5,
            high_version: 9,
        });
        let reply = raw_client.send_request(request).await;
        match reply {
            Ok(r)  => {
                RetryResult::Success(r)
            },
            Err(error) => RetryResult::Retry(error)
        }
    }).await;
    if let Err(e) = result {
        assert_eq!(e.tries, 5);
    } else {
        panic!("Test failed.")
    }
}

async fn test_retry_while_start_pravega() {
    let retry_policy = RetryWithBackoff::default().max_tries(10);
    let client = retry_async(retry_policy, || async {
        let result = create_connection("http://127.0.0.1:9090").await;
        match result {
            Ok(connection)  => RetryResult::Success(connection),
            Err(error) => RetryResult::Retry(error),
        }
    }).await.expect("create controller connection");

    let mut controller_client = ControllerClientImpl { channel: client };
    let scope_name = Scope::new("retryScope".into());
    let stream_name = Stream::new("retryStream".into());

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
}

async fn test_retry_with_unexpected_reply() {
    let retry_policy = RetryWithBackoff::default().max_tries(4);
    let scope_name = Scope::new("retryScope".into());
    let stream_name = Stream::new("retryStream".into());
    let client = create_connection("http://127.0.0.1:9090").await.expect("create controller connection");
    let mut controller_client = ControllerClientImpl { channel: client };

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

    let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;
    let result = retry_async(retry_policy, || async {
        let request = Requests::SealSegment(SealSegmentCommand {
            segment: segment_name.to_string(),
            request_id: 0,
            delegation_token: String::from(""),
        });
        let reply =  raw_client.send_request(request).await;
        match reply {
            Ok(r)  => {
                match r {
                    Replies::SegmentSealed(_) => RetryResult::Success(r),
                    Replies::NoSuchSegment(_) => RetryResult::Retry("No Such Segment"),
                    _  => RetryResult::Fail("Wrong reply type")
                }
            },
            Err(_error) => RetryResult::Retry("Connection Refused")
        }
    }).await;
    if let Err(e) = result {
        assert_eq!(e.error, "No Such Segment");
    } else {
        panic!("Test failed.")
    }
}

