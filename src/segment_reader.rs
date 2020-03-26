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
use crate::REQUEST_ID_GENERATOR;
use async_trait::async_trait;
use pravega_controller_client::*;
use pravega_rust_client_shared::ScopedSegment;
use pravega_wire_protocol::commands::{Command, EventCommand, ReadSegmentCommand, SegmentReadCommand};
use pravega_wire_protocol::connection_pool::ConnectionPoolImpl;
use pravega_wire_protocol::error::RawClientError;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use snafu::Snafu;
use std::net::SocketAddr;
use std::result::Result as StdResult;
use std::sync::atomic::{AtomicI64, Ordering};

#[derive(Debug, Snafu)]
//TODO: Improve error handling.
pub enum ReaderError {
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    SegmentTruncated {
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    SegmentSealed {
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    OperationError {
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Could not connect due to {}", error_msg))]
    ConnectionError {
        can_retry: bool,
        source: RawClientError,
        error_msg: String,
    },
}

///
/// AsyncSegmentReader is used to read from a given segment given the connection pool and the Controller URI
/// The reads given the offset and the length are processed asynchronously.
/// e.g: usage pattern is
/// AsyncSegmentReaderImpl::new(&segment_name, connection_pool, "http://controller uri").await
///
#[async_trait]
pub trait AsyncSegmentReader {
    async fn read(&self, offset: i64, length: i32) -> StdResult<SegmentReadCommand, ReaderError>;
}

struct AsyncSegmentReaderImpl<'a> {
    segment: &'a ScopedSegment,
    raw_client: Box<dyn RawClient<'a> + 'a>,
    id: &'static AtomicI64,
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
            .get_endpoint_for_segment(segment)
            .await
            .expect("get endpoint for segment")
            .parse::<SocketAddr>()
            .expect("Invalid end point returned");

        AsyncSegmentReaderImpl {
            segment,
            raw_client: RawClientImpl::new(&*connection_pool, endpoint).await,
            id: &REQUEST_ID_GENERATOR,
        }
    }
}

#[async_trait]
impl AsyncSegmentReader for AsyncSegmentReaderImpl<'_> {
    async fn read(&self, offset: i64, length: i32) -> StdResult<SegmentReadCommand, ReaderError> {
        let request = Requests::ReadSegment(ReadSegmentCommand {
            segment: self.segment.to_string(),
            offset,
            suggested_length: length,
            delegation_token: String::from(""),
            request_id: self.id.fetch_add(1, Ordering::SeqCst) + 1, // add_fetch
        });

        let reply = self.raw_client.as_ref().send_request(request).await;
        match reply {
            Ok(reply) => {
                match reply {
                    Replies::SegmentRead(cmd) => {
                        // TODO: modify EventCommand to and array instead of Vec.
                        let er = EventCommand::read_from(cmd.data.as_slice()).expect("Invalid msg");
                        println!("Event Command {:?}", er);
                        Ok(cmd)
                    }
                    Replies::NoSuchSegment(_cmd) => Err(ReaderError::SegmentTruncated {
                        can_retry: false,
                        operation: "Read segment".to_string(),
                        //TODO: to string method to all commands , Display trait
                        error_msg: "No Such Segment".to_string(),
                    }),
                    Replies::SegmentTruncated(_cmd) => Err(ReaderError::SegmentTruncated {
                        can_retry: false,
                        operation: "Read segment".to_string(),
                        error_msg: "Segment truncated".into(),
                    }),
                    Replies::SegmentSealed(cmd) => Ok(SegmentReadCommand {
                        segment: self.segment.to_string(),
                        offset,
                        at_tail: true,
                        end_of_segment: true,
                        data: vec![],
                        request_id: cmd.request_id,
                    }),
                    _ => Err(ReaderError::OperationError {
                        can_retry: false,
                        operation: "Read segment".to_string(),
                        error_msg: "".to_string(),
                    }),
                }
            }
            Err(error) => Err(ReaderError::ConnectionError {
                can_retry: true,
                source: error,
                error_msg: "RawClient error".to_string(),
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
        let ref connection_pool: ConnectionPoolImpl = {
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
            AsyncSegmentReaderImpl::new(&segment_name, connection_pool, "http://127.0.0.1:9090").await;
        let result = reader.read(0, 11).await;
        let result = result.unwrap();
        assert_eq!(result.segment, "examples/someStream/0.#epoch.0".to_string());
        assert_eq!(result.offset, 0);
        let event_data = EventCommand::read_from(result.data.as_slice()).unwrap();
        // Assuming events are written into Pravega  using UTF8Serializer
        let data = std::str::from_utf8(event_data.data.as_slice()).unwrap();
        println!("result data {:?}", data);
        assert_eq!("abc", data); // read from the standalone.
    }
}
