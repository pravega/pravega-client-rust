//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::raw_client::{RawClient, RawClientImpl};
use async_trait::async_trait;
use pravega_controller_client::*;
use pravega_rust_client_shared::{EventRead, ScopedSegment};
use pravega_wire_protocol::commands::{Command, EventCommand, ReadSegmentCommand};
use pravega_wire_protocol::connection_pool::ConnectionPoolImpl;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use snafu::Snafu;
use std::net::SocketAddr;
use std::result::Result as StdResult;

#[derive(Debug, Snafu)]
pub enum ClientError {
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    OperationError {
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Could not connect to controller {}", endpoint))]
    ConnectionError {
        can_retry: bool,
        endpoint: String,
        error_msg: String,
    },
}

#[async_trait]
pub trait AsyncSegmentReader {
    async fn read(&self, offset: i64) -> StdResult<EventCommand, ClientError>;
}

struct AsyncSegmentReaderImpl<'a> {
    segment: &'a ScopedSegment,
    raw_client: Box<dyn RawClient<'a> + 'a>,
}

impl<'a> AsyncSegmentReaderImpl<'a> {
    pub async fn new(
        segment: &'a ScopedSegment,
        connection_pool: &'a ConnectionPoolImpl,
        controller_uri: &'a str,
    ) -> AsyncSegmentReaderImpl<'a> {
        let mut controller_client = ControllerClientImpl {
            channel: create_connection(controller_uri).await,
        };
        let endpoint = controller_client
            .get_endpoint_for_segment(&segment)
            .await
            .expect("get endpoint for segment")
            .parse::<SocketAddr>()
            .expect("Invalid end point returned");

        // let s = &*RawClientImpl::new(&*connection_pool, endpoint).await;

        AsyncSegmentReaderImpl {
            segment,
            raw_client: RawClientImpl::new(&*connection_pool, endpoint).await, // the object should be moved here right ?
        }
    }
}

#[async_trait]
impl AsyncSegmentReader for AsyncSegmentReaderImpl<'_> {
    async fn read(&self, offset: i64) -> StdResult<EventCommand, ClientError> {
        let request = Requests::ReadSegment(ReadSegmentCommand {
            segment: self.segment.to_string(),
            offset,
            suggested_length: 14,
            delegation_token: String::from(""),
            request_id: 11,
        });

        let s = self.raw_client.as_ref().send_request(request).await;
        assert!(s.is_ok(), "Error response from server");
        let s1 = s.unwrap();
        println!("{:?}", s1);

        match s1 {
            Replies::SegmentRead(cmd) => {
                assert_eq!(cmd.segment, "examples/someStream/0.#epoch.0".to_string());
                assert_eq!(cmd.offset, 0);
                // TODO: modify EventCommand to and array instead of Vec.
                let er = EventCommand::read_from(cmd.data.as_slice()).expect("Invalid msg");
                println!("Event Command {:?}", er);
                Ok(er)
                // let data = std::str::from_utf8(er.data.as_slice()).unwrap();
                // assert_eq!("abc", data); // read from the standalone.
                // println!("result data {:?}", data);
            }
            _ => Err(ClientError::OperationError {
                can_retry: false,
                operation: "".to_string(),
                error_msg: "".to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pravega_rust_client_shared::Segment;
    use pravega_rust_client_shared::*;
    use pravega_wire_protocol::client_config::ClientConfigBuilder;
    use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryImpl};

    #[tokio::test]
    async fn test_read() {
        let ref CONNECTION_POOL: ConnectionPoolImpl = {
            let cf = Box::new(ConnectionFactoryImpl {}) as Box<dyn ConnectionFactory>;
            let config = ClientConfigBuilder::default()
                .build()
                .expect("build client config");
            let pool = ConnectionPoolImpl::new(cf, config);
            pool
        };
        // create a segment.
        let scope_name = Scope::new("examples".into());
        let stream_name = Stream::new("someStream".into());

        let segment_name = ScopedSegment {
            scope: scope_name,
            stream: stream_name,
            segment: Segment { number: 0 },
        };

        let reader =
            AsyncSegmentReaderImpl::new(&segment_name, CONNECTION_POOL, "http://127.0.0.1:9090").await;
        let result = reader.read(0).await;
        let result = result.unwrap();
        let data = std::str::from_utf8(result.data.as_slice()).unwrap();
        assert_eq!("abc", data); // read from the standalone.
        println!("result data {:?}", data);

        // let client = create_connection("http://127.0.0.1:9090").await;
        // let mut controller_client = ControllerClientImpl { channel: client };
        // let endpoint = controller_client
        //     .get_endpoint_for_segment(&segment_name)
        //     .await
        //     .expect("get endpoint for segment")
        //     .parse::<SocketAddr>()
        //     .expect("convert to socketaddr");
        //
        // let raw_client = RawClientImpl::new(&*CONNECTION_POOL, endpoint).await;

        // let request = Requests::ReadSegment(ReadSegmentCommand {
        //     segment: segment_name.to_string(),
        //     offset: 0,
        //     suggested_length: 14,
        //     delegation_token: String::from(""),
        //     request_id: 11,
        // });
        //
        // let s = raw_client.send_request(request).await;
        // assert!(s.is_ok(), "Error response from server");
        // let s1 = s.unwrap();
        // println!("{:?}", s1);
        // match s1 {
        //     Replies::SegmentRead(cmd) => {
        //         assert_eq!(cmd.segment, "examples/someStream/0.#epoch.0".to_string());
        //         assert_eq!(cmd.offset, 0);
        //         // TODO: modify EventCommand to and array instead of Vec.
        //         let er = EventCommand::read_from(cmd.data.as_slice()).expect("Invalid msg");
        //         println!("Event Command {:?}", er);
        //         let data = std::str::from_utf8(er.data.as_slice()).unwrap();
        //         assert_eq!("abc", data); // read from the standalone.
        //         println!("result data {:?}", data);
        //     }
        //     _ => assert!(false, "Invalid response"),
        // }
    }
}
