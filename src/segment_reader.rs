//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use async_trait::async_trait;
use pravega_rust_client_auth::DelegationTokenProvider;
use pravega_rust_client_shared::{PravegaNodeUri, ScopedSegment};
use pravega_wire_protocol::commands::{ReadSegmentCommand, SegmentReadCommand};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use snafu::Snafu;
use std::result::Result as StdResult;

use crate::client_factory::ClientFactory;
use crate::error::RawClientError;
use crate::get_request_id;
use crate::raw_client::RawClient;
use pravega_rust_client_retry::retry_async::retry_async;
use pravega_rust_client_retry::retry_result::RetryResult;
use pravega_rust_client_retry::retry_result::Retryable;
use tokio::sync::Mutex;

#[derive(Debug, Snafu)]
pub enum ReaderError {
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    SegmentTruncated {
        segment: String,
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    SegmentSealed {
        segment: String,
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    OperationError {
        segment: String,
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Could not connect due to {}", error_msg))]
    ConnectionError {
        segment: String,
        can_retry: bool,
        source: RawClientError,
        error_msg: String,
    },
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    AuthTokenCheckFailed {
        segment: String,
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Could not connect due to {}", error_msg))]
    AuthTokenExpired {
        segment: String,
        can_retry: bool,
        source: RawClientError,
        error_msg: String,
    },
    #[snafu(display("Could not connect due to {}", error_msg))]
    WrongHost {
        segment: String,
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
}

///
/// Fetch the segment from a given ReaderError
///
impl ReaderError {
    pub(crate) fn get_segment(&self) -> String {
        use ReaderError::*;
        match self {
            SegmentTruncated {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => segment.clone(),
            SegmentSealed {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => segment.clone(),
            OperationError {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => segment.clone(),
            ConnectionError {
                segment,
                can_retry: _,
                source: _,
                error_msg: _,
            } => segment.clone(),
            AuthTokenCheckFailed {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => segment.clone(),
            AuthTokenExpired {
                segment,
                can_retry: _,
                source: _,
                error_msg: _,
            } => segment.clone(),
            WrongHost {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => segment.clone(),
        }
    }

    fn refresh_token(&self) -> bool {
        match self {
            ReaderError::AuthTokenExpired { .. } => true,
            _ => false,
        }
    }
}
// Implementation of Retryable trait for the error thrown by the Controller.
// this ensures we can use the wrap_with_async_retry macros.
impl Retryable for ReaderError {
    fn can_retry(&self) -> bool {
        use ReaderError::*;
        match self {
            SegmentTruncated {
                segment: _,
                can_retry,
                operation: _,
                error_msg: _,
            } => *can_retry,
            SegmentSealed {
                segment: _,
                can_retry,
                operation: _,
                error_msg: _,
            } => *can_retry,
            OperationError {
                segment: _,
                can_retry,
                operation: _,
                error_msg: _,
            } => *can_retry,
            ConnectionError {
                segment: _,
                can_retry,
                source: _,
                error_msg: _,
            } => *can_retry,
            AuthTokenCheckFailed {
                segment: _,
                can_retry,
                operation: _,
                error_msg: _,
            } => *can_retry,
            AuthTokenExpired {
                segment: _,
                can_retry,
                source: _,
                error_msg: _,
            } => *can_retry,
            WrongHost {
                segment: _,
                can_retry,
                operation: _,
                error_msg: _,
            } => *can_retry,
        }
    }
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
pub struct AsyncSegmentReaderImpl {
    segment: ScopedSegment,
    endpoint: Mutex<PravegaNodeUri>,
    factory: ClientFactory,
    delegation_token_provider: DelegationTokenProvider,
}

#[async_trait]
impl AsyncSegmentReader for AsyncSegmentReaderImpl {
    async fn read(&self, offset: i64, length: i32) -> StdResult<SegmentReadCommand, ReaderError> {
        retry_async(self.factory.get_config().retry_policy, || async {
            let raw_client = self
                .factory
                .create_raw_client_for_endpoint(self.endpoint.lock().await.clone());
            match self.read_inner(offset, length, &raw_client).await {
                Ok(cmd) => RetryResult::Success(cmd),
                Err(e) => {
                    if e.can_retry() {
                        let controller = self.factory.get_controller_client();
                        let endpoint = controller
                            .get_endpoint_for_segment(&self.segment)
                            .await
                            .expect("get endpoint for async semgnet reader");
                        let mut guard = self.endpoint.lock().await;
                        *guard = endpoint;
                        if e.refresh_token() {
                            self.delegation_token_provider.signal_token_expiry();
                        }
                        RetryResult::Retry(e)
                    } else {
                        RetryResult::Fail(e)
                    }
                }
            }
        })
        .await
        .map_err(|e| e.error)
    }
}

impl AsyncSegmentReaderImpl {
    pub async fn init(
        segment: ScopedSegment,
        factory: ClientFactory,
        delegation_token_provider: DelegationTokenProvider,
    ) -> AsyncSegmentReaderImpl {
        let endpoint = factory
            .get_controller_client()
            .get_endpoint_for_segment(&segment)
            .await
            .expect("get endpoint for segment");

        AsyncSegmentReaderImpl {
            segment,
            endpoint: Mutex::new(endpoint),
            factory: factory.clone(),
            delegation_token_provider,
        }
    }

    async fn read_inner(
        &self,
        offset: i64,
        length: i32,
        raw_client: &dyn RawClient<'_>,
    ) -> StdResult<SegmentReadCommand, ReaderError> {
        let request = Requests::ReadSegment(ReadSegmentCommand {
            segment: self.segment.to_string(),
            offset,
            suggested_length: length,
            delegation_token: self
                .delegation_token_provider
                .retrieve_token(self.factory.get_controller_client())
                .await,
            request_id: get_request_id(),
        });

        let reply = raw_client.send_request(&request).await;
        match reply {
            Ok(reply) => match reply {
                Replies::SegmentRead(cmd) => {
                    assert_eq!(
                        cmd.offset, offset,
                        "Offset of SegmentRead response different from the request"
                    );
                    Ok(cmd)
                }
                Replies::AuthTokenCheckFailed(_cmd) => Err(ReaderError::AuthTokenCheckFailed {
                    segment: self.segment.to_string(),
                    can_retry: false,
                    operation: "Read segment".to_string(),
                    error_msg: "Auth token expired".to_string(),
                }),
                Replies::NoSuchSegment(_cmd) => Err(ReaderError::SegmentTruncated {
                    segment: self.segment.to_string(),
                    can_retry: false,
                    operation: "Read segment".to_string(),
                    error_msg: "No Such Segment".to_string(),
                }),
                Replies::SegmentTruncated(_cmd) => Err(ReaderError::SegmentTruncated {
                    segment: self.segment.to_string(),
                    can_retry: false,
                    operation: "Read segment".to_string(),
                    error_msg: "Segment truncated".into(),
                }),
                Replies::WrongHost(_cmd) => Err(ReaderError::WrongHost {
                    segment: self.segment.to_string(),
                    can_retry: true,
                    operation: "Read segment".to_string(),
                    error_msg: "Wrong host".to_string(),
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
                    segment: self.segment.to_string(),
                    can_retry: false,
                    operation: "Read segment".to_string(),
                    error_msg: "".to_string(),
                }),
            },
            Err(error) => match error {
                RawClientError::AuthTokenExpired { .. } => Err(ReaderError::AuthTokenExpired {
                    segment: self.segment.to_string(),
                    can_retry: true,
                    source: error,
                    error_msg: "Auth token expired".to_string(),
                }),
                _ => Err(ReaderError::ConnectionError {
                    segment: self.segment.to_string(),
                    can_retry: true,
                    source: error,
                    error_msg: "RawClient error".to_string(),
                }),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mockall::predicate::*;
    use mockall::*;
    use tokio::time::delay_for;

    use pravega_rust_client_shared::*;
    use pravega_wire_protocol::client_connection::ClientConnection;
    use pravega_wire_protocol::commands::{
        Command, EventCommand, NoSuchSegmentCommand, SegmentSealedCommand, SegmentTruncatedCommand,
    };

    use super::*;
    use crate::client_factory::ClientFactory;
    use pravega_rust_client_config::ClientConfigBuilder;

    // Setup mock.
    mock! {
        pub RawClientImpl {
            fn send_request(&self, request: &Requests) -> Result<Replies, RawClientError>{
            }
        }
    }

    #[async_trait]
    impl RawClient<'static> for MockRawClientImpl {
        async fn send_request(&self, request: &Requests) -> Result<Replies, RawClientError> {
            delay_for(Duration::from_nanos(1)).await;
            self.send_request(request)
        }

        async fn send_setup_request(
            &self,
            _request: &Requests,
        ) -> Result<(Replies, Box<dyn ClientConnection + 'static>), RawClientError> {
            unimplemented!() // Not required for this test.
        }
    }

    #[test]
    fn test_read_happy_path() {
        let config = ClientConfigBuilder::default()
            .controller_uri(pravega_rust_client_config::MOCK_CONTROLLER_URI)
            .is_auth_enabled(false)
            .mock(true)
            .build()
            .expect("creating config");
        let factory = ClientFactory::new(config);
        let runtime = factory.get_runtime_handle();

        let scope_name = Scope::from("examples".to_owned());
        let stream_name = Stream::from("someStream".to_owned());

        let segment_name = ScopedSegment {
            scope: scope_name,
            stream: stream_name,
            segment: Segment {
                number: 0,
                tx_id: None,
            },
        };

        let segment_name_copy = segment_name.clone();
        let mut raw_client = MockRawClientImpl::new();
        raw_client.expect_send_request().returning(move |req: &Requests| {
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
        let async_segment_reader = runtime.block_on(factory.create_async_event_reader(segment_name));
        let data = runtime.block_on(async_segment_reader.read_inner(0, 11, &raw_client));
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
        let data = runtime.block_on(async_segment_reader.read_inner(11, 1024, &raw_client));
        let segment_read_result: ReaderError = data.err().unwrap();
        match segment_read_result {
            ReaderError::SegmentTruncated {
                segment: _,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => assert_eq!(segment_read_result.can_retry(), false),
            _ => assert!(false, "Segment truncated excepted"),
        }

        // simulate SegmentTruncated
        let data = runtime.block_on(async_segment_reader.read_inner(12, 1024, &raw_client));
        let segment_read_result: ReaderError = data.err().unwrap();
        match segment_read_result {
            ReaderError::SegmentTruncated {
                segment: _,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => assert_eq!(segment_read_result.can_retry(), false),
            _ => assert!(false, "Segment truncated excepted"),
        }

        // simulate SealedSegment
        let data = runtime.block_on(async_segment_reader.read_inner(13, 1024, &raw_client));
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
