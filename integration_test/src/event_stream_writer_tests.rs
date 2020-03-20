use super::pravega_service::PravegaStandaloneService;
use crate::pravega_service::PravegaService;
use pravega_client_rust::event_stream_writer::{EventStreamWriter, EventStreamWriterImpl, Processor};
use pravega_client_rust::raw_client::RawClientImpl;
use pravega_controller_client::{create_connection, ControllerClient, ControllerClientImpl};
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::ClientConfigBuilder;
use pravega_wire_protocol::commands::HelloCommand;
use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryImpl};
use pravega_wire_protocol::connection_pool::{ConnectionPool, ConnectionPoolImpl};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::net::SocketAddr;

use std::process::Command;
use std::{thread, time};
use tokio::runtime;

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

//TODO: controller client will fail using multiple threads
#[tokio::test(core_threads = 1)]
async fn test_event_stream_writer() {
    // spin up Pravega standalone
    let scope_name = Scope::new("testScope".into());
    let stream_name = Stream::new("testStream".into());

    let mut pravega = PravegaStandaloneService::start();

    wait_for_standalone_with_timeout(true, 20);

    // Create scope and stream
    let client = create_connection("http://127.0.0.1:9090").await;
    let mut controller_client = ControllerClientImpl { channel: client };

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

    let scoped_stream = ScopedStream {
        scope: scope_name.clone(),
        stream: stream_name.clone(),
    };

    let cf = Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>;
    let config = ClientConfigBuilder::default()
        .build()
        .expect("build client config");
    let pool = ConnectionPoolImpl::new(cf, config);
    let (mut writer, mut processor) =
        EventStreamWriterImpl::new(scoped_stream.clone(), Box::new(controller_client), Box::new(pool)).await;
    tokio::spawn(Processor::run(processor));

    let rx = &mut writer.write_event("hello".to_owned()).await.expect("write event");
    rx.wait.unwrap();
//    loop {
//        let reply = rx.try_recv();
//        match reply {
//            Ok(o) => {
//                break;
//            }
//            Err(e) => {
//                print!("{:?}", e);
//                tokio::task::yield_now().await;
//            }
//        }
//    }
    pravega.stop().unwrap();
    wait_for_standalone_with_timeout(false, 5);
}
