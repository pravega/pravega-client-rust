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
use pravega_controller_client::{create_connection, ControllerClient, ControllerClientImpl};
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

#[derive(new)]
struct AsyncSegmentReaderImpl<'a> {
    segment: ScopedSegment,
    raw_client: Box<dyn RawClient<'a> + 'a>,
    id: &'static AtomicI64,
}

impl<'a> AsyncSegmentReaderImpl<'a> {
    pub async fn init(
        segment: ScopedSegment,
        connection_pool: &'a ConnectionPoolImpl,
        controller_uri: String,
    ) -> AsyncSegmentReaderImpl<'a> {
        let mut controller_client = ControllerClientImpl {
            channel: create_connection(controller_uri.as_str()).await,
        };
        let endpoint = controller_client
            .get_endpoint_for_segment(&segment)
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
            request_id: self.id.fetch_add(1, Ordering::SeqCst) + 1, // add_fetch implementation
        });

        let reply = self.raw_client.as_ref().send_request(request).await;
        match reply {
            Ok(reply) => match reply {
                Replies::SegmentRead(cmd) => {
                    assert_eq!(
                        cmd.offset, offset,
                        "Offset of SegmentRead response different from the request"
                    );
                    let er = EventCommand::read_from(cmd.data.as_slice()).expect("Invalid msg");
                    println!("Event Command {:?}", er);
                    Ok(cmd)
                }
                Replies::NoSuchSegment(_cmd) => Err(ReaderError::SegmentTruncated {
                    can_retry: false,
                    operation: "Read segment".to_string(),
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
            },
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
    use mockall::predicate::*;
    use mockall::*;
    use pravega_rust_client_shared::Segment;
    use pravega_rust_client_shared::*;
    use pravega_wire_protocol::client_connection::ClientConnection;
    use pravega_wire_protocol::commands::{
        NoSuchSegmentCommand, SegmentSealedCommand, SegmentTruncatedCommand,
    };
    use std::time::Duration;
    use tokio::time::delay_for;

    // Setup mock.
    mock! {
        pub RawClientImpl {
            fn send_request(&self, request: Requests) -> Result<Replies, RawClientError>{
            }
        }
    }

    #[async_trait]
    impl<'a> RawClient<'a> for MockRawClientImpl {
        async fn send_request(&self, request: Requests) -> Result<Replies, RawClientError> {
            delay_for(Duration::from_nanos(1)).await;
            self.send_request(request)
        }

        async fn send_setup_request(
            &self,
            _request: Requests,
        ) -> Result<(Replies, Box<dyn ClientConnection>), RawClientError> {
            unimplemented!() // Not required for this test.
        }
    }

    #[tokio::test]
    async fn test_read_happy_path() {
        let scope_name = Scope::new("examples".into());
        let stream_name = Stream::new("someStream".into());

        let segment_name = ScopedSegment {
            scope: scope_name,
            stream: stream_name,
            segment: Segment { number: 0 },
        };

        let segment_name_copy = segment_name.clone();
        let mut raw_client = MockRawClientImpl::new();
        raw_client.expect_send_request().returning(move |req: Requests| {
            //let s: Result<Replies, RawClientError> =
            match req {
                Requests::ReadSegment(cmd) => {
                    if cmd.request_id == 1 {
                        Ok(Replies::SegmentRead(SegmentReadCommand {
                            segment: segment_name_copy.to_string(),
                            offset: 0,
                            at_tail: false,
                            end_of_segment: false,
                            data: vec![0, 0, 0, 0, 0, 0, 0, 3, 97, 98, 99],
                            request_id: 1,
                        }))
                    } else if cmd.request_id == 2 {
                        Ok(Replies::NoSuchSegment(NoSuchSegmentCommand {
                            segment: segment_name_copy.to_string(),
                            server_stack_trace: "".to_string(),
                            offset: 0,
                            request_id: 2,
                        }))
                    } else if cmd.request_id == 3 {
                        Ok(Replies::SegmentTruncated(SegmentTruncatedCommand {
                            request_id: 3,
                            segment: segment_name_copy.to_string(),
                        }))
                    } else {
                        Ok(Replies::SegmentSealed(SegmentSealedCommand {
                            request_id: 4,
                            segment: segment_name_copy.to_string(),
                        }))
                    }
                }
                _ => Ok(Replies::NoSuchSegment(NoSuchSegmentCommand {
                    segment: segment_name_copy.to_string(),
                    server_stack_trace: "".to_string(),
                    offset: 0,
                    request_id: 1,
                })),
            }
        });
        let async_segment_reader =
            AsyncSegmentReaderImpl::new(segment_name, Box::new(raw_client), &REQUEST_ID_GENERATOR);
        let data = async_segment_reader.read(0, 11).await;
        let segment_read_result: SegmentReadCommand = data.unwrap();
        assert_eq!(
            segment_read_result.segment,
            "examples/someStream/0.#epoch.0".to_string()
        );
        assert_eq!(segment_read_result.offset, 0);
        assert_eq!(segment_read_result.at_tail, false);
        assert_eq!(segment_read_result.end_of_segment, false);
        let event_data = EventCommand::read_from(segment_read_result.data.as_slice()).unwrap();
        let data = std::str::from_utf8(event_data.data.as_slice()).unwrap();
        assert_eq!("abc", data);

        // simulate NoSuchSegment
        let data = async_segment_reader.read(11, 1024).await;
        let segment_read_result: ReaderError = data.err().unwrap();
        match segment_read_result {
            ReaderError::SegmentTruncated {
                can_retry,
                operation: _,
                error_msg: _,
            } => assert_eq!(can_retry, false),
            _ => assert!(false, "Segment truncated excepted"),
        }

        // simulate SegmentTruncated
        let data = async_segment_reader.read(12, 1024).await;
        let segment_read_result: ReaderError = data.err().unwrap();
        match segment_read_result {
            ReaderError::SegmentTruncated {
                can_retry,
                operation: _,
                error_msg: _,
            } => assert_eq!(can_retry, false),
            _ => assert!(false, "Segment truncated excepted"),
        }

        // simulate SealedSegment
        let data = async_segment_reader.read(13, 1024).await;
        let segment_read_result: SegmentReadCommand = data.unwrap();
        assert_eq!(
            segment_read_result.segment,
            "examples/someStream/0.#epoch.0".to_string()
        );
        assert_eq!(segment_read_result.offset, 13);
        assert_eq!(segment_read_result.at_tail, true);
        assert_eq!(segment_read_result.end_of_segment, true);
        assert_eq!(segment_read_result.data.len(), 0);
    }
}
