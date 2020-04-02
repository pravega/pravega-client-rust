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

use std::result::Result as StdResult;
use std::time::Duration;

use snafu::Snafu;
use tonic::transport::channel::Channel;
use tonic::{Code, Status};

use async_trait::async_trait;
use controller::{
    controller_service_client::ControllerServiceClient, create_scope_status, create_stream_status,
    delete_scope_status, delete_stream_status, ping_txn_status, txn_state, txn_status, update_stream_status,
    CreateScopeStatus, CreateStreamStatus, CreateTxnRequest, CreateTxnResponse, DeleteScopeStatus,
    DeleteStreamStatus, NodeUri, PingTxnRequest, PingTxnStatus, ScopeInfo, SegmentRanges, StreamConfig,
    StreamInfo, SuccessorResponse, TxnId, TxnRequest, TxnState, TxnStatus, UpdateStreamStatus,
};
use pravega_rust_client_shared::*;
use std::convert::{From, Into};
use std::str::FromStr;
use tonic::transport::Uri;

#[allow(non_camel_case_types)]
pub mod controller {
    //tonic::include_proto!("io.pravega.controller.stream.api.grpc.v1");
    include!(concat!(
        env!("OUT_DIR"),
        concat!("/", "io.pravega.controller.stream.api.grpc.v1", ".rs")
    ));
    // this is the rs file name generated after compiling the proto file, located inside the target folder.
}

mod mock_controller;
mod model_helper;
#[cfg(test)]
mod test;

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
    #[snafu(display("Could not connect to controller {}", endpoint))]
    ConnectionError {
        source: tonic::transport::Error,
        can_retry: bool,
        endpoint: String,
        error_msg: String,
    },
}

pub type Result<T> = StdResult<T, ControllerError>;

/// Controller APIs for administrative action for streams
#[async_trait]
pub trait ControllerClient {
    /**
     * API to create a scope. The future completes with true in the case the scope did not exist
     * when the controller executed the operation. In the case of a re-attempt to create the
     * same scope, the future completes with false to indicate that the scope existed when the
     * controller executed the operation.
     */
    async fn create_scope(&self, scope: &Scope) -> Result<bool>;

    async fn list_streams(&self, scope: &Scope) -> Result<Vec<String>>;

    /**
     * API to delete a scope. Note that a scope can only be deleted in the case is it empty. If
     * the scope contains at least one stream, then the delete request will fail.
     */
    async fn delete_scope(&self, scope: &Scope) -> Result<bool>;

    /**
     * API to create a stream. The future completes with true in the case the stream did not
     * exist when the controller executed the operation. In the case of a re-attempt to create
     * the same stream, the future completes with false to indicate that the stream existed when
     * the controller executed the operation.
     */
    async fn create_stream(&self, stream_config: &StreamConfiguration) -> Result<bool>;

    /**
     * API to update the configuration of a Stream.
     */
    async fn update_stream(&self, stream_config: &StreamConfiguration) -> Result<bool>;

    /**
     * API to Truncate stream. This api takes a stream cut point which corresponds to a cut in
     * the stream segments which is consistent and covers the entire key range space.
     */
    async fn truncate_stream(&self, stream_cut: &StreamCut) -> Result<bool>;

    /**
     * API to seal a Stream.
     */
    async fn seal_stream(&self, stream: &ScopedStream) -> Result<bool>;

    /**
     * API to delete a stream. Only a sealed stream can be deleted.
     */
    async fn delete_stream(&self, stream: &ScopedStream) -> Result<bool>;

    // Controller APIs called by Pravega producers for getting stream specific information

    /**
     * API to get list of current segments for the stream to write to.
     */
    async fn get_current_segments(&self, stream: &ScopedStream) -> Result<StreamSegments>;

    /**
     * API to create a new transaction. The transaction timeout is relative to the creation time.
     */
    async fn create_transaction(&self, stream: &ScopedStream, lease: Duration) -> Result<TxnSegments>;

    /**
     * API to send transaction heartbeat and increase the transaction timeout by lease amount of milliseconds.
     */
    async fn ping_transaction(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
        lease: Duration,
    ) -> Result<PingStatus>;

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
    ) -> Result<()>;

    /**
     * Aborts a transaction. No events written to it may be read, and no further events may be
     * written. Will fail with if the transaction has already been committed or aborted.
     */
    async fn abort_transaction(&self, stream: &ScopedStream, tx_id: TxId) -> Result<()>;

    /**
     * Returns the status of the specified transaction.
     */
    async fn check_transaction_status(&self, stream: &ScopedStream, tx_id: TxId)
        -> Result<TransactionStatus>;

    // Controller APIs that are called by readers

    /**
     * Given a segment return the endpoint that currently is the owner of that segment.
     *
     * This is called when a reader or a writer needs to determine which host/server it needs to contact to
     * read and write, respectively. The result of this function can be cached until the endpoint is
     * unreachable or indicates it is no longer the owner.
     */
    async fn get_endpoint_for_segment(&self, segment: &ScopedSegment) -> Result<PravegaNodeUri>;

    /**
     * Refreshes an expired/non-existent delegation token.
     * @param scope         Scope of the stream.
     * @param streamName    Name of the stream.
     * @return              The delegation token for the given stream.
     */
    async fn get_or_refresh_delegation_token_for(&self, stream: ScopedStream) -> Result<DelegationToken>;

    async fn get_successors(&self, segment: &ScopedSegment) -> Result<StreamSegmentsWithPredecessors>;
}

#[derive(Clone)]
pub struct ControllerClientImpl {
    pub channel: ControllerServiceClient<Channel>,
}

impl ControllerClientImpl {
    /// create_connection with a single controller uri.
    pub async fn create_connection(uri: &str) -> Result<ControllerClientImpl> {
        // Placeholder to add authentication headers.
        let connection_result = ControllerServiceClient::connect(uri.to_string()).await;
        match connection_result {
            Ok(connection) => Ok(ControllerClientImpl { channel: connection }),
            Err(e) => Err(ControllerError::ConnectionError {
                source: e,
                can_retry: true,
                endpoint: uri.to_string(),
                error_msg: "Failed to connect to the controller".to_string(),
            }),
        }
    }

    ///
    /// Create a pool of connections to a controller.
    /// The requests will be load balanced across multiple connections and every underlying connection
    /// can handle multiplexing as supported by http2.
    ///
    pub async fn create_pooled_connection(uri: &str, pool_size: u8) -> Result<ControllerClientImpl> {
        let uri = Uri::from_str(uri).unwrap();
        let list_connections = (0..pool_size).map(|_a| Channel::builder(uri.clone()));

        // Placeholder to add authentication headers.
        let ch = Channel::balance_list(list_connections);

        Ok(ControllerClientImpl {
            channel: ControllerServiceClient::new(ch),
        })
    }

    ///
    /// Tonic library suggests we clone the channel to enable multiplexing of requests.
    /// This is because at the very top level the channel is backed by a `tower_buffer::Buffer`
    /// which runs the connection in a background task and provides a `mpsc` channel interface.
    /// Due to this cloning the `Channel` type is cheap and encouraged.
    ///
    fn get_controller_client(&self) -> ControllerServiceClient<Channel> {
        self.channel.clone()
    }
}
#[allow(unused_variables)]
#[async_trait]
impl ControllerClient for ControllerClientImpl {
    async fn create_scope(&self, scope: &Scope) -> Result<bool> {
        create_scope(scope, &mut self.get_controller_client()).await
    }

    async fn list_streams(&self, scope: &Scope) -> Result<Vec<String>> {
        unimplemented!()
    }

    async fn delete_scope(&self, scope: &Scope) -> Result<bool> {
        delete_scope(scope, &mut self.get_controller_client()).await
    }

    async fn create_stream(&self, stream_config: &StreamConfiguration) -> Result<bool> {
        create_stream(stream_config, &mut self.get_controller_client()).await
    }

    async fn update_stream(&self, stream_config: &StreamConfiguration) -> Result<bool> {
        update_stream(stream_config, &mut self.get_controller_client()).await
    }

    async fn truncate_stream(&self, stream_cut: &StreamCut) -> Result<bool> {
        truncate_stream(stream_cut, &mut self.get_controller_client()).await
    }

    async fn seal_stream(&self, stream: &ScopedStream) -> Result<bool> {
        seal_stream(stream, &mut self.get_controller_client()).await
    }

    async fn delete_stream(&self, stream: &ScopedStream) -> Result<bool> {
        delete_stream(stream, &mut self.get_controller_client()).await
    }

    async fn get_current_segments(&self, stream: &ScopedStream) -> Result<StreamSegments> {
        get_current_segments(stream, &mut self.get_controller_client()).await
    }

    async fn create_transaction(&self, stream: &ScopedStream, lease: Duration) -> Result<TxnSegments> {
        create_transaction(stream, lease, &mut self.get_controller_client()).await
    }

    async fn ping_transaction(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
        lease: Duration,
    ) -> Result<PingStatus> {
        ping_transaction(stream, tx_id, lease, &mut self.get_controller_client()).await
    }

    async fn commit_transaction(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
        writer_id: WriterId,
        time: Timestamp,
    ) -> Result<()> {
        commit_transaction(stream, tx_id, writer_id, time, &mut self.get_controller_client()).await
    }

    async fn abort_transaction(&self, stream: &ScopedStream, tx_id: TxId) -> Result<()> {
        abort_transaction(stream, tx_id, &mut self.get_controller_client()).await
    }

    async fn check_transaction_status(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
    ) -> Result<TransactionStatus> {
        check_transaction_status(stream, tx_id, &mut self.get_controller_client()).await
    }

    async fn get_endpoint_for_segment(&self, segment: &ScopedSegment) -> Result<PravegaNodeUri> {
        get_uri_segment(segment, &mut self.get_controller_client()).await
    }

    async fn get_or_refresh_delegation_token_for(&self, stream: ScopedStream) -> Result<DelegationToken> {
        unimplemented!()
    }

    async fn get_successors(&self, segment: &ScopedSegment) -> Result<StreamSegmentsWithPredecessors> {
        get_successors(segment, &mut self.get_controller_client()).await
    }
}

pub fn map_grpc_error(operation_name: &str, status: Status) -> ControllerError {
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
        _ => ControllerError::OperationError {
            can_retry: true, // retry is enabled for all other errors
            operation: operation_name.into(),
            error_msg: status.to_string(),
        },
    }
}

/// Async helper function to create scope
async fn create_scope(scope: &Scope, ch: &mut ControllerServiceClient<Channel>) -> Result<bool> {
    use create_scope_status::Status;

    let request: ScopeInfo = ScopeInfo::from(scope);

    let op_status: StdResult<tonic::Response<CreateScopeStatus>, tonic::Status> =
        ch.create_scope(tonic::Request::new(request)).await;
    let operation_name = "CreateScope";
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to create stream.
async fn create_stream(cfg: &StreamConfiguration, ch: &mut ControllerServiceClient<Channel>) -> Result<bool> {
    use create_stream_status::Status;

    let request: StreamConfig = StreamConfig::from(cfg);
    let op_status: StdResult<tonic::Response<CreateStreamStatus>, tonic::Status> =
        ch.create_stream(tonic::Request::new(request)).await;
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to get segment URI.
async fn get_uri_segment(
    request: &ScopedSegment,
    ch: &mut ControllerServiceClient<Channel>,
) -> Result<PravegaNodeUri> {
    let op_status: StdResult<tonic::Response<NodeUri>, tonic::Status> =
        ch.get_uri(tonic::Request::new(request.into())).await;
    let operation_name = "get_endpoint";
    match op_status {
        Ok(response) => Ok(response.into_inner()),
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
    .map(PravegaNodeUri::from)
}

/// Async helper function to delete Stream.
async fn delete_scope(scope: &Scope, ch: &mut ControllerServiceClient<Channel>) -> Result<bool> {
    use delete_scope_status::Status;

    let op_status: StdResult<tonic::Response<DeleteScopeStatus>, tonic::Status> =
        ch.delete_scope(tonic::Request::new(ScopeInfo::from(scope))).await;
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to seal Stream.
async fn seal_stream(stream: &ScopedStream, ch: &mut ControllerServiceClient<Channel>) -> Result<bool> {
    use update_stream_status::Status;

    let request: StreamInfo = StreamInfo::from(stream);
    let op_status: StdResult<tonic::Response<UpdateStreamStatus>, tonic::Status> =
        ch.seal_stream(tonic::Request::new(request)).await;
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to delete Stream.
async fn delete_stream(stream: &ScopedStream, ch: &mut ControllerServiceClient<Channel>) -> Result<bool> {
    use delete_stream_status::Status;

    let request: StreamInfo = StreamInfo::from(stream);
    let op_status: StdResult<tonic::Response<DeleteStreamStatus>, tonic::Status> =
        ch.delete_stream(tonic::Request::new(request)).await;
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to update Stream.
async fn update_stream(
    stream_config: &StreamConfiguration,
    ch: &mut ControllerServiceClient<Channel>,
) -> Result<bool> {
    use update_stream_status::Status;

    let request: StreamConfig = StreamConfig::from(stream_config);
    let op_status: StdResult<tonic::Response<UpdateStreamStatus>, tonic::Status> =
        ch.update_stream(tonic::Request::new(request)).await;
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to truncate Stream.
async fn truncate_stream(stream_cut: &StreamCut, ch: &mut ControllerServiceClient<Channel>) -> Result<bool> {
    use update_stream_status::Status;

    let request: controller::StreamCut = controller::StreamCut::from(stream_cut);
    let op_status: StdResult<tonic::Response<UpdateStreamStatus>, tonic::Status> =
        ch.truncate_stream(tonic::Request::new(request)).await;
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to get current Segments in a Stream.
async fn get_current_segments(
    stream: &ScopedStream,
    ch: &mut ControllerServiceClient<Channel>,
) -> Result<StreamSegments> {
    let request: StreamInfo = StreamInfo::from(stream);
    let op_status: StdResult<tonic::Response<SegmentRanges>, tonic::Status> =
        ch.get_current_segments(tonic::Request::new(request)).await;
    let operation_name = "getCurrentSegments";
    match op_status {
        Ok(segment_ranges) => Ok(StreamSegments::from(segment_ranges.into_inner())),
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to create a Transaction.
async fn create_transaction(
    stream: &ScopedStream,
    lease: Duration,
    ch: &mut ControllerServiceClient<Channel>,
) -> Result<TxnSegments> {
    let request = CreateTxnRequest {
        stream_info: Some(StreamInfo::from(stream)),
        lease: lease.as_millis() as i64,
        scale_grace_period: 0,
    };
    let op_status: StdResult<tonic::Response<CreateTxnResponse>, tonic::Status> =
        ch.create_transaction(tonic::Request::new(request)).await;
    let operation_name = "createTransaction";
    match op_status {
        Ok(create_txn_response) => Ok(TxnSegments::from(create_txn_response.into_inner())),
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to ping a Transaction.
async fn ping_transaction(
    stream: &ScopedStream,
    tx_id: TxId,
    lease: Duration,
    ch: &mut ControllerServiceClient<Channel>,
) -> Result<PingStatus> {
    use ping_txn_status::Status;
    let request = PingTxnRequest {
        stream_info: Some(StreamInfo::from(stream)),
        txn_id: Some(TxnId::from(tx_id)),
        lease: lease.as_millis() as i64,
    };
    let op_status: StdResult<tonic::Response<PingTxnStatus>, tonic::Status> =
        ch.ping_transaction(tonic::Request::new(request)).await;
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to commit a Transaction.
async fn commit_transaction(
    stream: &ScopedStream,
    tx_id: TxId,
    writerid: WriterId,
    time: Timestamp,
    ch: &mut ControllerServiceClient<Channel>,
) -> Result<()> {
    use txn_status::Status;
    let request = TxnRequest {
        stream_info: Some(StreamInfo::from(stream)),
        txn_id: Some(TxnId::from(tx_id)),
        writer_id: writerid.0.to_string(),
        timestamp: time.0 as i64,
    };
    let op_status: StdResult<tonic::Response<TxnStatus>, tonic::Status> =
        ch.commit_transaction(tonic::Request::new(request)).await;
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to abort a Transaction.
async fn abort_transaction(
    stream: &ScopedStream,
    tx_id: TxId,
    ch: &mut ControllerServiceClient<Channel>,
) -> Result<()> {
    use txn_status::Status;
    let request = TxnRequest {
        stream_info: Some(StreamInfo::from(stream)),
        txn_id: Some(TxnId::from(tx_id)),
        writer_id: "".to_string(),
        timestamp: 0,
    };
    let op_status: StdResult<tonic::Response<TxnStatus>, tonic::Status> =
        ch.commit_transaction(tonic::Request::new(request)).await;
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to check Transaction status.
async fn check_transaction_status(
    stream: &ScopedStream,
    tx_id: TxId,
    ch: &mut ControllerServiceClient<Channel>,
) -> Result<TransactionStatus> {
    use txn_state::State;
    let request = TxnRequest {
        stream_info: Some(StreamInfo::from(stream)),
        txn_id: Some(TxnId::from(tx_id)),
        writer_id: "".to_string(),
        timestamp: 0,
    };
    let op_status: StdResult<tonic::Response<TxnState>, tonic::Status> =
        ch.check_transaction_state(tonic::Request::new(request)).await;
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to get successors
async fn get_successors(
    request: &ScopedSegment,
    ch: &mut ControllerServiceClient<Channel>,
) -> Result<StreamSegmentsWithPredecessors> {
    let op_status: StdResult<tonic::Response<SuccessorResponse>, tonic::Status> = ch
        .get_segments_immediately_following(tonic::Request::new(request.into()))
        .await;
    let operation_name = "get_successors_segment";
    match op_status {
        Ok(response) => Ok(response.into_inner()),
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
    .map(StreamSegmentsWithPredecessors::from)
}
