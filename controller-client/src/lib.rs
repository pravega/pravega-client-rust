/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
use std::result::Result as StdResult;
use std::time::Duration;

use snafu::{Backtrace, Snafu};
use tonic::transport::channel::Channel;
use tonic::{Code, Status};

use async_trait::async_trait;
pub use controller::{
    controller_service_client::ControllerServiceClient, create_scope_status, create_stream_status,
    scaling_policy::ScalingPolicyType, CreateScopeStatus, CreateStreamStatus, ScalingPolicy, ScopeInfo,
    StreamConfig, StreamInfo,
};
use pravega_rust_client_shared::*;

#[allow(non_camel_case_types)]
mod controller {
    tonic::include_proto!("io.pravega.controller.stream.api.grpc.v1");
    // this is the rs file name generated after compiling the proto file, located inside the target folder.
}

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
        can_retry: bool,
        endpoint: String,
        error_msg: String,
        backtrace: Backtrace,
    },
}

type Result<T> = StdResult<T, ControllerError>;

pub enum PingStatus {
    //TODO
}

pub enum TransactionStatus {
    //TODO
}

/// Controller Apis for administrative action for streams
#[async_trait]
pub trait ControllerClient {
    async fn create_scope(&self, scope: &Scope) -> Result<bool>;

    async fn list_streams(&self, scope: &Scope) -> Result<Vec<String>>;

    async fn delete_scope(&self, scope: &Scope) -> Result<bool>;

    /**
     * API to create a stream. The future completes with true in the case the stream did not
     * exist when the controller executed the operation. In the case of a re-attempt to create
     * the same stream, the future completes with false to indicate that the stream existed when
     * the controller executed the operation.
     */
    async fn create_stream(&self, stream: &ScopedStream, stream_config: &StreamConfiguration)
        -> Result<bool>;

    /**
     * API to update the configuration of a stream.
     */
    async fn update_stream(&self, stream: &ScopedStream, stream_config: &StreamConfiguration)
        -> Result<bool>;

    /**
     * API to Truncate stream. This api takes a stream cut point which corresponds to a cut in
     * the stream segments which is consistent and covers the entire key range space.
     */
    async fn truncate_stream(&self, stream: &ScopedStream, stream_cut: &StreamCut) -> Result<bool>;

    /**
     * API to seal a stream.
     */
    async fn seal_stream(&self, stream: &ScopedStream) -> Result<bool>;

    /**
     * API to delete a stream. Only a sealed stream can be deleted.
     */
    async fn delete_stream(&self, stream: &ScopedStream) -> Result<bool>;

    // Controller Apis called by pravega producers for getting stream specific information

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
     * ordering guarantees specified in {@link EventStreamWriter}. Will fail with
     * //TODO
     * if the transaction has already been committed or aborted.
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
     * written. Will fail with
     * //TODO
     * if the transaction has already been committed or aborted.
     */
    async fn abort_transaction(&self, stream: &ScopedStream, tx_id: TxId) -> Result<()>;

    /**
     * Returns the status of the specified transaction.
     */
    async fn check_transaction_status(&self, stream: &ScopedStream, tx_id: TxId)
        -> Result<TransactionStatus>;

    // Controller Apis that are called by readers
    //TODO

    /**
     * Given a segment return the endpoint that currently is the owner of that segment.
     *
     * This is called when a reader or a writer needs to determine which host/server it needs to contact to
     * read and write, respectively. The result of this function can be cached until the endpoint is
     * unreachable or indicates it is no longer the owner.
     */
    async fn get_endpoint_for_segment(&self, segment: ScopedSegment) -> Result<PravegaNodeUri>;

    /**
     * Refreshes an expired/non-existent delegation token.
     * @param scope         Scope of the stream.
     * @param streamName    Name of the stream.
     * @return              The delegation token for the given stream.
     */
    async fn get_or_refresh_delegation_token_for(&self, stream: ScopedStream) -> Result<DelegationToken>;
}

/// create_connection with the given controller uri.
pub async fn create_connection(uri: &'static str) -> ControllerServiceClient<Channel> {
    // Placeholder to add authentication headers.
    let connection: ControllerServiceClient<Channel> = ControllerServiceClient::connect(uri.to_string())
        .await
        .expect("Failed to create a channel");
    connection
}

/// Async function to create scope
pub async fn create_scope(
    request: ScopeInfo,
    ch: &mut ControllerServiceClient<Channel>,
) -> StdResult<bool, ControllerError> {
    let op_status: StdResult<tonic::Response<CreateScopeStatus>, tonic::Status> =
        ch.create_scope(tonic::Request::new(request)).await;
    let operation_name = "CreateScope";
    match op_status {
        Ok(code) => match code.into_inner().status() {
            create_scope_status::Status::Success => Ok(true),
            create_scope_status::Status::ScopeExists => Ok(false),
            create_scope_status::Status::InvalidScopeName => Err(ControllerError::OperationError {
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

// Method used to translate grpc errors to custom error.
fn map_grpc_error(operation_name: &str, status: Status) -> ControllerError {
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

pub async fn create_stream(
    request: StreamConfig,
    ch: &mut ControllerServiceClient<Channel>,
) -> StdResult<bool, ControllerError> {
    let op_status: StdResult<tonic::Response<CreateStreamStatus>, tonic::Status> =
        ch.create_stream(tonic::Request::new(request)).await;
    let operation_name = "CreateStream";
    match op_status {
        Ok(code) => match code.into_inner().status() {
            create_stream_status::Status::Success => Ok(true),
            create_stream_status::Status::StreamExists => Ok(false),
            create_stream_status::Status::InvalidStreamName | create_stream_status::Status::ScopeNotFound => {
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
