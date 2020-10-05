/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#![deny(
    clippy::all,
    clippy::cargo,
    clippy::else_if_without_else,
    clippy::empty_line_after_outer_attr,
    clippy::multiple_inherent_impl,
    clippy::mut_mut,
    clippy::path_buf_push_overwrite
)]
#![warn(
    clippy::cargo_common_metadata,
    clippy::mutex_integer,
    clippy::needless_borrow,
    clippy::similar_names
)]
#![allow(clippy::multiple_crate_versions)]
#![allow(dead_code)]
#![allow(clippy::similar_names)]

use std::result::Result as StdResult;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use controller::{
    controller_service_client::ControllerServiceClient, create_scope_status, create_stream_status,
    delete_scope_status, delete_stream_status, ping_txn_status, scale_request, scale_response,
    scale_status_response, txn_state, txn_status, update_stream_status, CreateScopeStatus,
    CreateStreamStatus, CreateTxnRequest, CreateTxnResponse, DeleteScopeStatus, DeleteStreamStatus,
    GetEpochSegmentsRequest, GetSegmentsRequest, NodeUri, PingTxnRequest, PingTxnStatus, ScaleRequest,
    ScaleResponse, ScaleStatusRequest, ScaleStatusResponse, ScopeInfo, SegmentId, SegmentRanges,
    SegmentsAtTime, StreamConfig, StreamInfo, SuccessorResponse, TxnId, TxnRequest, TxnState, TxnStatus,
    UpdateStreamStatus,
};
use im::HashMap as ImHashMap;
use log::debug;
use pravega_rust_client_retry::retry_async::retry_async;
use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
use pravega_rust_client_retry::retry_result::{RetryError, RetryResult, Retryable};
use pravega_rust_client_retry::wrap_with_async_retry;
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::ClientConfig;
use snafu::Snafu;
use std::convert::{From, Into};
use std::str::FromStr;
use std::sync::RwLock;
use tokio::runtime::Handle;
use tonic::codegen::http::uri::InvalidUri;
use tonic::transport::channel::Channel;
use tonic::transport::Uri;
use tonic::{Code, Status};

#[allow(non_camel_case_types)]
pub mod controller {
    tonic::include_proto!("io.pravega.controller.stream.api.grpc.v1");
    // this is the rs file name generated after compiling the proto file, located inside the target folder.
}

pub mod mock_controller;
mod model_helper;
#[cfg(test)]
mod test;

// Max number of retries by the controller in case of a retryable failure.
const MAX_RETRIES: i32 = 10;

#[derive(Debug, Snafu)]
pub enum ControllerError {
    #[snafu(display(
        "Controller client failed to perform operation {} due to {}",
        operation,
        error_msg,
    ))]
    OperationError {
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Could not connect to controller due to {}", error_msg))]
    ConnectionError { can_retry: bool, error_msg: String },
    #[snafu(display("Invalid configuration passed to the Controller client. Error {}", error_msg))]
    InvalidConfiguration { can_retry: bool, error_msg: String },
}

// Implementation of Retryable trait for the error thrown by the Controller.
// this ensures we can use the wrap_with_async_retry macros.
impl Retryable for ControllerError {
    fn can_retry(&self) -> bool {
        use ControllerError::*;
        match self {
            OperationError {
                can_retry,
                operation: _,
                error_msg: _,
            } => *can_retry,
            ConnectionError {
                can_retry,
                error_msg: _,
            } => *can_retry,
            InvalidConfiguration {
                can_retry,
                error_msg: _,
            } => *can_retry,
        }
    }
}

pub type Result<T> = StdResult<T, ControllerError>;
pub type ResultRetry<T> = StdResult<T, RetryError<ControllerError>>;

/// Controller APIs for administrative action for streams
#[async_trait]
pub trait ControllerClient: Send + Sync {
    /**
     * API to create a scope. The future completes with true in the case the scope did not exist
     * when the controller executed the operation. In the case of a re-attempt to create the
     * same scope, the future completes with false to indicate that the scope existed when the
     * controller executed the operation.
     */
    async fn create_scope(&self, scope: &Scope) -> ResultRetry<bool>;

    async fn list_streams(&self, scope: &Scope) -> ResultRetry<Vec<String>>;

    /**
     * API to delete a scope. Note that a scope can only be deleted in the case is it empty. If
     * the scope contains at least one stream, then the delete request will fail.
     */
    async fn delete_scope(&self, scope: &Scope) -> ResultRetry<bool>;

    /**
     * API to create a stream. The future completes with true in the case the stream did not
     * exist when the controller executed the operation. In the case of a re-attempt to create
     * the same stream, the future completes with false to indicate that the stream existed when
     * the controller executed the operation.
     */
    async fn create_stream(&self, stream_config: &StreamConfiguration) -> ResultRetry<bool>;

    /**
     * API to update the configuration of a Stream.
     */
    async fn update_stream(&self, stream_config: &StreamConfiguration) -> ResultRetry<bool>;

    /**
     * API to Truncate stream. This api takes a stream cut point which corresponds to a cut in
     * the stream segments which is consistent and covers the entire key range space.
     */
    async fn truncate_stream(&self, stream_cut: &StreamCut) -> ResultRetry<bool>;

    /**
     * API to seal a Stream.
     */
    async fn seal_stream(&self, stream: &ScopedStream) -> ResultRetry<bool>;

    /**
     * API to delete a stream. Only a sealed stream can be deleted.
     */
    async fn delete_stream(&self, stream: &ScopedStream) -> ResultRetry<bool>;

    // Controller APIs called by Pravega producers for getting stream specific information

    /**
     * API to get list of current segments for the stream to write to.
     */
    async fn get_current_segments(&self, stream: &ScopedStream) -> ResultRetry<StreamSegments>;

    /**
     * API to get list of segments for a given stream and epoch.
     */
    async fn get_epoch_segments(&self, stream: &ScopedStream, epoch: i32) -> ResultRetry<StreamSegments>;

    /**
     * API to get segments pointing to the HEAD of the stream..
     */
    async fn get_head_segments(&self, stream: &ScopedStream) -> ResultRetry<ImHashMap<Segment, i64>>;

    /**
     * API to create a new transaction. The transaction timeout is relative to the creation time.
     */
    async fn create_transaction(&self, stream: &ScopedStream, lease: Duration) -> ResultRetry<TxnSegments>;

    /**
     * API to send transaction heartbeat and increase the transaction timeout by lease amount of milliseconds.
     */
    async fn ping_transaction(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
        lease: Duration,
    ) -> ResultRetry<PingStatus>;

    /**
     * Commits a transaction, atomically committing all events to the stream, subject to the
     * ordering guarantees specified in {@link EventStreamWriter}. Will fail if the transaction has
     * already been committed or aborted.
     */
    async fn commit_transaction(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
        writer_id: WriterId,
        time: Timestamp,
    ) -> ResultRetry<()>;

    /**
     * Aborts a transaction. No events written to it may be read, and no further events may be
     * written. Will fail with if the transaction has already been committed or aborted.
     */
    async fn abort_transaction(&self, stream: &ScopedStream, tx_id: TxId) -> ResultRetry<()>;

    /**
     * Returns the status of the specified transaction.
     */
    async fn check_transaction_status(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
    ) -> ResultRetry<TransactionStatus>;

    // Controller APIs that are called by readers

    /**
     * Given a segment return the endpoint that currently is the owner of that segment.
     *
     * This is called when a reader or a writer needs to determine which host/server it needs to contact to
     * read and write, respectively. The result of this function can be cached until the endpoint is
     * unreachable or indicates it is no longer the owner.
     */
    async fn get_endpoint_for_segment(&self, segment: &ScopedSegment) -> ResultRetry<PravegaNodeUri>;

    /**
     * Refreshes an expired/non-existent delegation token.
     * @param scope         Scope of the stream.
     * @param streamName    Name of the stream.
     * @return              The delegation token for the given stream.
     */
    async fn get_or_refresh_delegation_token_for(&self, stream: ScopedStream)
        -> ResultRetry<DelegationToken>;

    ///
    /// Fetch the successors for a given Segment.
    ///
    async fn get_successors(&self, segment: &ScopedSegment) -> ResultRetry<StreamSegmentsWithPredecessors>;

    ///
    /// Scale a Stream to the new key ranges. This API returns a result once the scale operation has completed.
    /// This internally uses the check_scale API to verify the Stream Scaling status.
    ///
    async fn scale_stream(
        &self,
        stream: &ScopedStream,
        sealed_segments: &[Segment],
        new_key_ranges: &[(f64, f64)],
    ) -> ResultRetry<()>;

    ///
    ///Check the status of a Stream Scale operation for a given scale epoch. It returns a
    ///`true` if the stream scaling operation has completed and `false` if the stream scaling is
    ///in  progress.
    ///
    async fn check_scale(&self, stream: &ScopedStream, scale_epoch: i32) -> ResultRetry<bool>;
}

pub struct ControllerClientImpl {
    config: ClientConfig,
    channel: RwLock<ControllerServiceClient<Channel>>,
}

async fn get_channel(config: &ClientConfig) -> Channel {
    const HTTP_PREFIX: &str = "http://";

    // Placeholder to add authentication headers.
    let s = format!("{}{}", HTTP_PREFIX, &config.controller_uri.to_string());
    let uri_result = Uri::from_str(s.as_str())
        .map_err(|e1: InvalidUri| ControllerError::InvalidConfiguration {
            can_retry: false,
            error_msg: e1.to_string(),
        })
        .unwrap();

    let iterable_endpoints =
        (0..config.max_controller_connections).map(|_a| Channel::builder(uri_result.clone()));

    async { Channel::balance_list(iterable_endpoints) }.await
}

#[allow(unused_variables)]
#[async_trait]
impl ControllerClient for ControllerClientImpl {
    async fn create_scope(&self, scope: &Scope) -> ResultRetry<bool> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_create_scope(scope)
        )
    }

    async fn list_streams(&self, scope: &Scope) -> ResultRetry<Vec<String>> {
        unimplemented!()
    }

    async fn delete_scope(&self, scope: &Scope) -> ResultRetry<bool> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_delete_scope(scope)
        )
    }

    async fn create_stream(&self, stream_config: &StreamConfiguration) -> ResultRetry<bool> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_create_stream(stream_config)
        )
    }

    async fn update_stream(&self, stream_config: &StreamConfiguration) -> ResultRetry<bool> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_update_stream(stream_config)
        )
    }

    async fn truncate_stream(&self, stream_cut: &StreamCut) -> ResultRetry<bool> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_truncate_stream(stream_cut)
        )
    }

    async fn seal_stream(&self, stream: &ScopedStream) -> ResultRetry<bool> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_seal_stream(stream)
        )
    }

    async fn delete_stream(&self, stream: &ScopedStream) -> ResultRetry<bool> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_delete_stream(stream)
        )
    }

    async fn get_current_segments(&self, stream: &ScopedStream) -> ResultRetry<StreamSegments> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_get_current_segments(stream)
        )
    }

    async fn get_epoch_segments(&self, stream: &ScopedStream, epoch: i32) -> ResultRetry<StreamSegments> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_get_epoch_segments(stream, epoch)
        )
    }
    async fn get_head_segments(&self, stream: &ScopedStream) -> ResultRetry<ImHashMap<Segment, i64>> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_get_head_segments(stream)
        )
    }

    async fn create_transaction(&self, stream: &ScopedStream, lease: Duration) -> ResultRetry<TxnSegments> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_create_transaction(stream, lease)
        )
    }

    async fn ping_transaction(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
        lease: Duration,
    ) -> ResultRetry<PingStatus> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_ping_transaction(stream, tx_id, lease)
        )
    }

    async fn commit_transaction(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
        writer_id: WriterId,
        time: Timestamp,
    ) -> ResultRetry<()> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_commit_transaction(stream, tx_id, writer_id, time.clone())
        )
    }

    async fn abort_transaction(&self, stream: &ScopedStream, tx_id: TxId) -> ResultRetry<()> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_abort_transaction(stream, tx_id)
        )
    }

    async fn check_transaction_status(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
    ) -> ResultRetry<TransactionStatus> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_check_transaction_status(stream, tx_id)
        )
    }

    async fn get_endpoint_for_segment(&self, segment: &ScopedSegment) -> ResultRetry<PravegaNodeUri> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_get_endpoint_for_segment(segment)
        )
    }

    async fn get_or_refresh_delegation_token_for(
        &self,
        stream: ScopedStream,
    ) -> ResultRetry<DelegationToken> {
        unimplemented!()
    }

    async fn get_successors(&self, segment: &ScopedSegment) -> ResultRetry<StreamSegmentsWithPredecessors> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_get_successors(segment)
        )
    }

    async fn scale_stream(
        &self,
        stream: &ScopedStream,
        sealed_segment_ids: &[Segment],
        new_ranges: &[(f64, f64)],
    ) -> ResultRetry<()> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_scale_stream(stream, sealed_segment_ids, new_ranges)
        )
    }

    async fn check_scale(&self, stream: &ScopedStream, scale_epoch: i32) -> ResultRetry<bool> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_check_scale(stream, scale_epoch)
        )
    }
}

impl ControllerClientImpl {
    ///
    /// Create a pooled connection to the controller. The pool size is decided by the ClientConfig.
    /// The requests will be load balanced across multiple connections and every connection supports
    /// multiplexing of requests.
    ///
    pub fn new(config: ClientConfig, h: Handle) -> Self {
        // actual connection is established lazily.
        let ch = h.block_on(get_channel(&config));
        ControllerClientImpl {
            config,
            channel: RwLock::new(ControllerServiceClient::new(ch)),
        }
    }

    ///
    /// reset method needs to be invoked in the case of ConnectionError.
    /// This logic can be removed once https://github.com/tower-rs/tower/issues/383 is fixed.
    ///
    pub async fn reset(&self) {
        let ch = get_channel(&self.config).await;
        let mut x = self.channel.write().unwrap();
        *x = ControllerServiceClient::new(ch);
    }

    ///
    /// Tonic library suggests we clone the channel to enable multiplexing of requests.
    /// This is because at the very top level the channel is backed by a `tower_buffer::Buffer`
    /// which runs the connection in a background task and provides a `mpsc` channel interface.
    /// Due to this cloning the `Channel` type is cheap and encouraged.
    ///
    fn get_controller_client(&self) -> ControllerServiceClient<Channel> {
        self.channel.read().unwrap().clone()
    }

    // Method used to translate grpc errors to ControllerError.
    async fn map_grpc_error(&self, operation_name: &str, status: Status) -> ControllerError {
        match status.code() {
            Code::InvalidArgument
            | Code::NotFound
            | Code::AlreadyExists
            | Code::PermissionDenied
            | Code::OutOfRange
            | Code::Unimplemented
            | Code::Unauthenticated => ControllerError::OperationError {
                can_retry: false,
                operation: operation_name.into(),
                error_msg: status.to_string(),
            },
            Code::Unknown => {
                self.reset().await;
                ControllerError::ConnectionError {
                    can_retry: true,
                    error_msg: status.to_string(),
                }
            }
            _ => ControllerError::OperationError {
                can_retry: true, // retry is enabled for all other errors
                operation: operation_name.into(),
                error_msg: format!("{:?}", status.code()),
            },
        }
    }

    // Helper method which checks for the completion of the Stream scale for the given epoch with the specified retry policy.
    async fn check_scale_status(
        &self,
        stream: &ScopedStream,
        epoch: i32,
        retry_policy: RetryWithBackoff,
    ) -> Result<()> {
        let operation_name = "check_scale post scale_stream";
        retry_async(retry_policy, || async {
            let r = self.check_scale(stream, epoch).await;
            match r {
                Ok(status) => {
                    if status {
                        RetryResult::Success(())
                    } else {
                        RetryResult::Retry(ControllerError::OperationError {
                            can_retry: true,
                            operation: operation_name.into(),
                            error_msg: "Stream Scaling in progress".to_string(),
                        })
                    }
                }
                Err(RetryError {
                    error,
                    total_delay: _,
                    tries: _,
                }) => RetryResult::Fail(error),
            }
        })
        .await
        .map_err(|retry_error| ControllerError::OperationError {
            can_retry: false,
            operation: operation_name.to_string(),
            error_msg: format!("{:?}", retry_error),
        })
    }

    async fn call_create_scope(&self, scope: &Scope) -> Result<bool> {
        use create_scope_status::Status;
        let operation_name = "CreateScope";
        let request: ScopeInfo = ScopeInfo::from(scope);

        let op_status: StdResult<tonic::Response<CreateScopeStatus>, tonic::Status> = self
            .get_controller_client()
            .create_scope(tonic::Request::new(request))
            .await;
        match op_status {
            Ok(code) => match code.into_inner().status() {
                Status::Success => Ok(true),
                Status::ScopeExists => Ok(false),
                Status::InvalidScopeName => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: operation_name.into(),
                    error_msg: "Invalid scope".into(),
                }),
                _ => Err(ControllerError::OperationError {
                    can_retry: true,
                    operation: operation_name.into(),
                    error_msg: "Operation failed".into(),
                }),
            },
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_delete_scope(&self, scope: &Scope) -> Result<bool> {
        use delete_scope_status::Status;

        let op_status: StdResult<tonic::Response<DeleteScopeStatus>, tonic::Status> = self
            .get_controller_client()
            .delete_scope(tonic::Request::new(ScopeInfo::from(scope)))
            .await;
        let operation_name = "DeleteScope";
        match op_status {
            Ok(code) => match code.into_inner().status() {
                Status::Success => Ok(true),
                Status::ScopeNotFound => Ok(false),
                Status::ScopeNotEmpty => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: operation_name.into(),
                    error_msg: "Scope not empty".into(),
                }),
                _ => Err(ControllerError::OperationError {
                    can_retry: true,
                    operation: operation_name.into(),
                    error_msg: "Operation failed".into(),
                }),
            },
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_create_stream(&self, stream_config: &StreamConfiguration) -> Result<bool> {
        use create_stream_status::Status;

        let request: StreamConfig = StreamConfig::from(stream_config);
        let op_status: StdResult<tonic::Response<CreateStreamStatus>, tonic::Status> = self
            .get_controller_client()
            .create_stream(tonic::Request::new(request))
            .await;
        let operation_name = "CreateStream";
        match op_status {
            Ok(code) => match code.into_inner().status() {
                Status::Success => Ok(true),
                Status::StreamExists => Ok(false),
                Status::InvalidStreamName | Status::ScopeNotFound => {
                    Err(ControllerError::OperationError {
                        can_retry: false, // do not retry.
                        operation: operation_name.into(),
                        error_msg: "Invalid Stream/Scope Not Found".into(),
                    })
                }
                _ => Err(ControllerError::OperationError {
                    can_retry: true, // retry for all other errors
                    operation: operation_name.into(),
                    error_msg: "Operation failed".into(),
                }),
            },
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_update_stream(&self, stream_config: &StreamConfiguration) -> Result<bool> {
        use update_stream_status::Status;

        let request: StreamConfig = StreamConfig::from(stream_config);
        let op_status: StdResult<tonic::Response<UpdateStreamStatus>, tonic::Status> = self
            .get_controller_client()
            .update_stream(tonic::Request::new(request))
            .await;
        let operation_name = "updateStream";
        match op_status {
            Ok(code) => match code.into_inner().status() {
                Status::Success => Ok(true),
                Status::ScopeNotFound | Status::StreamNotFound => {
                    Err(ControllerError::OperationError {
                        can_retry: false, // do not retry.
                        operation: operation_name.into(),
                        error_msg: "Stream/Scope Not Found".into(),
                    })
                }
                _ => Err(ControllerError::OperationError {
                    can_retry: true, // retry for all other errors
                    operation: operation_name.into(),
                    error_msg: "Operation failed".into(),
                }),
            },
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_truncate_stream(&self, stream_cut: &StreamCut) -> Result<bool> {
        use update_stream_status::Status;

        let request: controller::StreamCut = controller::StreamCut::from(stream_cut);
        let op_status: StdResult<tonic::Response<UpdateStreamStatus>, tonic::Status> = self
            .get_controller_client()
            .truncate_stream(tonic::Request::new(request))
            .await;
        let operation_name = "truncateStream";
        match op_status {
            Ok(code) => match code.into_inner().status() {
                Status::Success => Ok(true),
                Status::ScopeNotFound | Status::StreamNotFound => {
                    Err(ControllerError::OperationError {
                        can_retry: false, // do not retry.
                        operation: operation_name.into(),
                        error_msg: "Stream/Scope Not Found".into(),
                    })
                }
                _ => Err(ControllerError::OperationError {
                    can_retry: true, // retry for all other errors
                    operation: operation_name.into(),
                    error_msg: "Operation failed".into(),
                }),
            },
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_seal_stream(&self, stream: &ScopedStream) -> Result<bool> {
        use update_stream_status::Status;

        let request: StreamInfo = StreamInfo::from(stream);
        let op_status: StdResult<tonic::Response<UpdateStreamStatus>, tonic::Status> = self
            .get_controller_client()
            .seal_stream(tonic::Request::new(request))
            .await;
        let operation_name = "SealStream";
        match op_status {
            Ok(code) => match code.into_inner().status() {
                Status::Success => Ok(true),
                Status::StreamNotFound | Status::ScopeNotFound => {
                    Err(ControllerError::OperationError {
                        can_retry: false, // do not retry.
                        operation: operation_name.into(),
                        error_msg: "Stream/Scope Not Found".into(),
                    })
                }
                _ => Err(ControllerError::OperationError {
                    can_retry: true, // retry for all other errors
                    operation: operation_name.into(),
                    error_msg: "Operation failed".into(),
                }),
            },
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_delete_stream(&self, stream: &ScopedStream) -> Result<bool> {
        use delete_stream_status::Status;

        let request: StreamInfo = StreamInfo::from(stream);
        let op_status: StdResult<tonic::Response<DeleteStreamStatus>, tonic::Status> = self
            .get_controller_client()
            .delete_stream(tonic::Request::new(request))
            .await;
        let operation_name = "DeleteStream";
        match op_status {
            Ok(code) => match code.into_inner().status() {
                Status::Success => Ok(true),
                Status::StreamNotFound => Ok(false),
                Status::StreamNotSealed => {
                    Err(ControllerError::OperationError {
                        can_retry: false, // do not retry.
                        operation: operation_name.into(),
                        error_msg: "Stream Not Sealed".into(),
                    })
                }
                _ => Err(ControllerError::OperationError {
                    can_retry: true, // retry for all other errors
                    operation: operation_name.into(),
                    error_msg: "Operation failed".into(),
                }),
            },
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_get_head_segments(&self, stream: &ScopedStream) -> Result<ImHashMap<Segment, i64>> {
        let request: StreamInfo = StreamInfo::from(stream);
        let op_status: StdResult<tonic::Response<SegmentsAtTime>, tonic::Status> = self
            .get_controller_client()
            .get_segments(tonic::Request::new(GetSegmentsRequest {
                stream_info: Some(request),
                timestamp: 0,
            }))
            .await;
        let operation_name = "getHeadSegments";
        match op_status {
            Ok(segment_ranges) => {
                let head_segments = segment_ranges
                    .into_inner()
                    .segments
                    .iter()
                    .map(|seg_data| {
                        let seg = seg_data.segment_id.as_ref().unwrap();
                        let segment: Segment = Segment::from(seg.segment_id);
                        let start_offset: i64 = seg_data.offset;
                        (segment, start_offset)
                    })
                    .collect();
                Ok(head_segments)
            }
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_get_epoch_segments(&self, stream: &ScopedStream, epoch: i32) -> Result<StreamSegments> {
        let request: StreamInfo = StreamInfo::from(stream);
        let op_status: StdResult<tonic::Response<SegmentRanges>, tonic::Status> = self
            .get_controller_client()
            .get_epoch_segments(tonic::Request::new(GetEpochSegmentsRequest {
                stream_info: Some(request),
                epoch,
            }))
            .await;
        let operation_name = "getEpochSegments";
        match op_status {
            Ok(segment_ranges) => Ok(StreamSegments::from(segment_ranges.into_inner())),
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_get_current_segments(&self, stream: &ScopedStream) -> Result<StreamSegments> {
        let request: StreamInfo = StreamInfo::from(stream);
        let op_status: StdResult<tonic::Response<SegmentRanges>, tonic::Status> = self
            .get_controller_client()
            .get_current_segments(tonic::Request::new(request))
            .await;
        let operation_name = "getCurrentSegments";
        match op_status {
            Ok(segment_ranges) => Ok(StreamSegments::from(segment_ranges.into_inner())),
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_create_transaction(&self, stream: &ScopedStream, lease: Duration) -> Result<TxnSegments> {
        let request = CreateTxnRequest {
            stream_info: Some(StreamInfo::from(stream)),
            lease: lease.as_millis() as i64,
            scale_grace_period: 0,
        };
        let op_status: StdResult<tonic::Response<CreateTxnResponse>, tonic::Status> = self
            .get_controller_client()
            .create_transaction(tonic::Request::new(request))
            .await;
        let operation_name = "createTransaction";
        match op_status {
            Ok(create_txn_response) => Ok(TxnSegments::from(create_txn_response.into_inner())),
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_ping_transaction(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
        lease: Duration,
    ) -> Result<PingStatus> {
        use ping_txn_status::Status;
        let request = PingTxnRequest {
            stream_info: Some(StreamInfo::from(stream)),
            txn_id: Some(TxnId::from(tx_id)),
            lease: lease.as_millis() as i64,
        };
        let op_status: StdResult<tonic::Response<PingTxnStatus>, tonic::Status> = self
            .get_controller_client()
            .ping_transaction(tonic::Request::new(request))
            .await;
        let operation_name = "pingTransaction";
        match op_status {
            Ok(code) => match code.into_inner().status() {
                Status::Ok => Ok(PingStatus::Ok),
                Status::Committed => Ok(PingStatus::Committed),
                Status::Aborted => Ok(PingStatus::Aborted),
                Status::LeaseTooLarge => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: operation_name.into(),
                    error_msg: "Ping transaction failed, Reason:LeaseTooLarge".into(),
                }),
                Status::MaxExecutionTimeExceeded => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: operation_name.into(),
                    error_msg: "Ping transaction failed, Reason:MaxExecutionTimeExceeded".into(),
                }),
                Status::ScaleGraceTimeExceeded => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: operation_name.into(),
                    error_msg: "Ping transaction failed, Reason:ScaleGraceTimeExceeded".into(),
                }),
                Status::Disconnected => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: operation_name.into(),
                    error_msg: "Ping transaction failed, Reason:ScaleGraceTimeExceeded".into(),
                }),
                _ => Err(ControllerError::OperationError {
                    can_retry: true, // retry for all other errors
                    operation: operation_name.into(),
                    error_msg: "Operation failed".into(),
                }),
            },
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_commit_transaction(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
        writer_id: WriterId,
        time: Timestamp,
    ) -> Result<()> {
        use txn_status::Status;
        let request = TxnRequest {
            stream_info: Some(StreamInfo::from(stream)),
            txn_id: Some(TxnId::from(tx_id)),
            writer_id: writer_id.0.to_string(),
            timestamp: time.0 as i64,
        };
        let op_status: StdResult<tonic::Response<TxnStatus>, tonic::Status> = self
            .get_controller_client()
            .commit_transaction(tonic::Request::new(request))
            .await;
        let operation_name = "commitTransaction";
        match op_status {
            Ok(code) => match code.into_inner().status() {
                Status::Success => Ok(()),
                Status::TransactionNotFound => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: operation_name.into(),
                    error_msg: "Commit transaction failed, Reason:TransactionNotFound".into(),
                }),
                Status::StreamNotFound => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: operation_name.into(),
                    error_msg: "Commit transaction failed, Reason:StreamNotFound".into(),
                }),
                _ => Err(ControllerError::OperationError {
                    can_retry: true, // retry for all other errors
                    operation: operation_name.into(),
                    error_msg: "Operation failed".into(),
                }),
            },
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_abort_transaction(&self, stream: &ScopedStream, tx_id: TxId) -> Result<()> {
        use txn_status::Status;
        let request = TxnRequest {
            stream_info: Some(StreamInfo::from(stream)),
            txn_id: Some(TxnId::from(tx_id)),
            writer_id: "".to_string(),
            timestamp: 0,
        };
        let op_status: StdResult<tonic::Response<TxnStatus>, tonic::Status> = self
            .get_controller_client()
            .abort_transaction(tonic::Request::new(request))
            .await;
        let operation_name = "abortTransaction";
        match op_status {
            Ok(code) => match code.into_inner().status() {
                Status::Success => Ok(()),
                Status::TransactionNotFound => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: operation_name.into(),
                    error_msg: "Abort transaction failed, Reason:TransactionNotFound".into(),
                }),
                Status::StreamNotFound => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: operation_name.into(),
                    error_msg: "Abort transaction failed, Reason:StreamNotFound".into(),
                }),
                _ => Err(ControllerError::OperationError {
                    can_retry: true, // retry for all other errors
                    operation: operation_name.into(),
                    error_msg: "Operation failed".into(),
                }),
            },
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_check_transaction_status(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
    ) -> Result<TransactionStatus> {
        use txn_state::State;
        let request = TxnRequest {
            stream_info: Some(StreamInfo::from(stream)),
            txn_id: Some(TxnId::from(tx_id)),
            writer_id: "".to_string(),
            timestamp: 0,
        };
        let op_status: StdResult<tonic::Response<TxnState>, tonic::Status> = self
            .get_controller_client()
            .check_transaction_state(tonic::Request::new(request))
            .await;
        let operation_name = "checkTransactionStatus";
        match op_status {
            Ok(code) => match code.into_inner().state() {
                State::Open => Ok(TransactionStatus::Open),
                State::Committing => Ok(TransactionStatus::Committing),
                State::Committed => Ok(TransactionStatus::Committed),
                State::Aborting => Ok(TransactionStatus::Aborting),
                State::Aborted => Ok(TransactionStatus::Aborted),
                _ => Err(ControllerError::OperationError {
                    can_retry: true, // retry for all other errors
                    operation: operation_name.into(),
                    error_msg: "Operation failed".into(),
                }),
            },
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_get_endpoint_for_segment(&self, segment: &ScopedSegment) -> Result<PravegaNodeUri> {
        let op_status: StdResult<tonic::Response<NodeUri>, tonic::Status> = self
            .get_controller_client()
            .get_uri(tonic::Request::new(segment.into()))
            .await;
        let operation_name = "get_endpoint";
        match op_status {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
        .map(PravegaNodeUri::from)
    }

    async fn call_get_successors(&self, segment: &ScopedSegment) -> Result<StreamSegmentsWithPredecessors> {
        let scoped_stream = ScopedStream {
            scope: segment.scope.clone(),
            stream: segment.stream.clone(),
        };
        let segment_id_request = SegmentId {
            stream_info: Some(StreamInfo::from(&scoped_stream)),
            segment_id: segment.segment.number,
        };
        debug!("sending get successors request for {:?}", segment);
        let op_status: StdResult<tonic::Response<SuccessorResponse>, tonic::Status> = self
            .get_controller_client()
            .get_segments_immediately_following(tonic::Request::new(segment_id_request))
            .await;
        let operation_name = "get_successors_segment";
        match op_status {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
        .map(StreamSegmentsWithPredecessors::from)
    }

    async fn call_scale_stream(
        &self,
        stream: &ScopedStream,
        sealed_segment_ids: &[Segment],
        new_ranges: &[(f64, f64)],
    ) -> Result<()> {
        use scale_response::ScaleStreamStatus;
        let scale_request = ScaleRequest {
            stream_info: Some(StreamInfo::from(stream)),
            sealed_segments: sealed_segment_ids.iter().map(|s| s.number).collect(),
            new_key_ranges: new_ranges
                .iter()
                .map(|(l, r)| scale_request::KeyRangeEntry { start: *l, end: *r })
                .collect(),
            scale_timestamp: Instant::now().elapsed().as_millis() as i64,
        };
        // start the scale Stream operation.
        let op_status: StdResult<tonic::Response<ScaleResponse>, tonic::Status> = self
            .get_controller_client()
            .scale(tonic::Request::new(scale_request))
            .await;
        let operation_name = "scale_stream";

        match op_status {
            Ok(response) => {
                let scale_response = response.into_inner();
                match scale_response.status() {
                    ScaleStreamStatus::Started => {
                        // scale Stream has started. check for its completion.
                        self.check_scale_status(stream, scale_response.epoch, RetryWithBackoff::default())
                            .await
                    }
                    ScaleStreamStatus::PreconditionFailed => Err(ControllerError::OperationError {
                        can_retry: false, // do not retry.
                        operation: operation_name.into(),
                        error_msg: "PreconditionFailed".into(),
                    }),
                    _ => Err(ControllerError::OperationError {
                        can_retry: false, // do not retry.
                        operation: operation_name.into(),
                        error_msg: "Operation failed".into(),
                    }),
                }
            }
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_check_scale(&self, stream: &ScopedStream, scale_epoch: i32) -> Result<bool> {
        use scale_status_response::ScaleStatus;

        let request = ScaleStatusRequest {
            stream_info: Some(StreamInfo::from(stream)),
            epoch: scale_epoch,
        };
        let op_status: StdResult<tonic::Response<ScaleStatusResponse>, tonic::Status> = self
            .get_controller_client()
            .check_scale(tonic::Request::new(request))
            .await;

        let operation_name = "check_scale";
        debug!("Check Stream scale status {:?}", op_status);
        match op_status {
            Ok(response) => match response.into_inner().status() {
                ScaleStatus::Success => Ok(true),
                ScaleStatus::InProgress => Ok(false),
                ScaleStatus::InvalidInput => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: operation_name.into(),
                    error_msg: "Invalid Input".into(),
                }),
                _ => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: operation_name.into(),
                    error_msg: "Operation failed".into(),
                }),
            },
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }
}
