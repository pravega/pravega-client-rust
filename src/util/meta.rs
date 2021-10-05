//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactoryAsync;
use crate::segment::raw_client::{RawClient, RawClientError};
use crate::util::get_request_id;
use im::HashMap;
use pravega_client_auth::DelegationTokenProvider;
use pravega_client_retry::retry_async::retry_async;
use pravega_client_retry::retry_result::{RetryResult, Retryable};
use pravega_client_shared::{ScopedSegment, ScopedStream, Segment};
use pravega_controller_client::ControllerError;
use pravega_wire_protocol::commands::{GetStreamSegmentInfoCommand, StreamSegmentInfoCommand};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use snafu::Snafu;
use tracing::{debug, error};

///
/// A client to fetch meta data of a Stream.
///
#[derive(new)]
pub struct MetaClient {
    scoped_stream: ScopedStream,
    factory: ClientFactoryAsync,
    delegation_token_provider: DelegationTokenProvider,
}

#[derive(Debug, Snafu)]
pub enum MetaClientError {
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    StreamSealed {
        stream: String,
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
    #[snafu(display("Could not connect due to {}", error_msg))]
    ControllerConnectionError {
        stream: String,
        can_retry: bool,
        source: ControllerError,
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
impl MetaClientError {
    fn refresh_token(&self) -> bool {
        matches!(self, MetaClientError::AuthTokenExpired { .. })
    }
}

impl Retryable for MetaClientError {
    fn can_retry(&self) -> bool {
        use MetaClientError::*;
        match self {
            StreamSealed {
                stream: _,
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
            ControllerConnectionError {
                stream: _,
                can_retry,
                source: _,
                error_msg: _,
            } => *can_retry,
        }
    }
}

impl MetaClient {
    ///
    /// Fetch the current Head Segments and the corresponding offsets for the given Stream.
    ///
    pub async fn fetch_current_head_segments(&self) -> Result<HashMap<Segment, i64>, MetaClientError> {
        let segments = self
            .factory
            .controller_client()
            .get_head_segments(&self.scoped_stream)
            .await;
        segments.map_err(|e| {
            MetaClientError::ControllerConnectionError {
                stream: self.scoped_stream.to_string(),
                can_retry: false, // the controller client has retried internally
                source: e.error,
                error_msg: "Failed to fetch Stream's Head segments from controller".to_string(),
            }
        })
    }

    ///
    /// Fetch the Current Tail Segments of a given Stream.
    ///
    pub async fn fetch_current_tail_segments(&self) -> Result<HashMap<Segment, i64>, MetaClientError> {
        match self
            .factory
            .controller_client()
            .get_current_segments(&self.scoped_stream)
            .await
        {
            Ok(segments) => {
                let key_map = segments.key_segment_map;
                if key_map.is_empty() {
                    Err(MetaClientError::StreamSealed {
                        stream: self.scoped_stream.to_string(),
                        can_retry: false,
                        operation: "Get current segments for a stream".to_string(),
                        error_msg: "Zero current segments for the stream".to_string(),
                    })
                } else {
                    let mut map = HashMap::new();
                    for (_, segment_range) in key_map {
                        let info = self.fetch_segment_info(&segment_range.scoped_segment).await;
                        match info {
                            Ok(cmd) => {
                                map.insert(segment_range.get_segment(), cmd.write_offset);
                            }
                            Err(e) => {
                                error!(
                                    "Error while fetching segment info for segment {:?} after retries. {:?}",
                                    segment_range.scoped_segment, e
                                );
                                return Err(e);
                            }
                        }
                    }
                    Ok(map)
                }
            }
            Err(e) => {
                Err(MetaClientError::ControllerConnectionError {
                    stream: self.scoped_stream.to_string(),
                    can_retry: false, // Controller client internally retries
                    source: e.error,
                    error_msg: "Failed to fetch Stream's current segments from controller".to_string(),
                })
            }
        }
    }

    ///
    /// Check if the stream is sealed.
    ///
    pub async fn is_stream_sealed(&self) -> bool {
        match self.fetch_current_tail_segments().await {
            Ok(_) => false,
            Err(e) => {
                matches!(e, MetaClientError::StreamSealed { .. })
            }
        }
    }

    // Helper method to fetch Segment information for a given Segment.
    // This ensures we retry with the provided retry configuration incase of errors.
    async fn fetch_segment_info(
        &self,
        scoped_segment: &ScopedSegment,
    ) -> Result<StreamSegmentInfoCommand, MetaClientError> {
        retry_async(self.factory.config().retry_policy, || async {
            let raw_client = self.factory.create_raw_client(scoped_segment).await;

            let result = self.fetch_segment_info_inner(scoped_segment, &raw_client).await;
            match result {
                Ok(cmd) => RetryResult::Success(cmd),
                Err(e) => {
                    if e.can_retry() {
                        if e.refresh_token() {
                            self.delegation_token_provider.signal_token_expiry();
                        }
                        debug!(
                            "Retry sending GetStreamSegmentInfo for segment {:?} due to {:?}",
                            scoped_segment, e
                        );
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

    // Method to initiate a GetStreamSegmentInfo wirecommand to the segment store.
    async fn fetch_segment_info_inner(
        &self,
        scoped_segment: &ScopedSegment,
        raw_client: &dyn RawClient<'_>,
    ) -> Result<StreamSegmentInfoCommand, MetaClientError> {
        let request = Requests::GetStreamSegmentInfo(GetStreamSegmentInfoCommand {
            delegation_token: self
                .delegation_token_provider
                .retrieve_token(self.factory.controller_client())
                .await,
            request_id: get_request_id(),
            segment_name: scoped_segment.to_string(),
        });

        let reply = raw_client.send_request(&request).await;
        match reply {
            Ok(reply) => match reply {
                Replies::StreamSegmentInfo(cmd) => Ok(cmd),
                Replies::AuthTokenCheckFailed(_cmd) => Err(MetaClientError::AuthTokenCheckFailed {
                    segment: scoped_segment.to_string(),
                    can_retry: false,
                    operation: "Get Stream Segment Info".to_string(),
                    error_msg: "Auth token expired".to_string(),
                }),
                Replies::WrongHost(_cmd) => Err(MetaClientError::WrongHost {
                    segment: scoped_segment.to_string(),
                    can_retry: true,
                    operation: "Get Stream Segment Info".to_string(),
                    error_msg: "Wrong host".to_string(),
                }),

                _ => {
                    error!("Observed unexpected reply for GetStreamSegmentInfo {:?}", reply);
                    Err(MetaClientError::OperationError {
                        segment: scoped_segment.to_string(),
                        can_retry: false,
                        operation: "Get Stream Segment Info".to_string(),
                        error_msg: "".to_string(),
                    })
                }
            },
            Err(error) => match error {
                RawClientError::AuthTokenExpired { .. } => Err(MetaClientError::AuthTokenExpired {
                    segment: scoped_segment.to_string(),
                    can_retry: true,
                    source: error,
                    error_msg: "Auth token expired".to_string(),
                }),
                _ => Err(MetaClientError::ConnectionError {
                    segment: scoped_segment.to_string(),
                    can_retry: true,
                    source: error,
                    error_msg: "RawClient error".to_string(),
                }),
            },
        }
    }
}
