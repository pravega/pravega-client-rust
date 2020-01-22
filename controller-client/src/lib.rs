/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
#[allow(non_camel_case_types)]
mod controller {
    tonic::include_proto!("io.pravega.controller.stream.api.grpc.v1");
    // this is the rs file name generated after compiling the proto file, located inside the target folder.
}

#[cfg(test)]
mod test;

pub use controller::{
    controller_service_client::ControllerServiceClient, create_scope_status, create_stream_status,
    scaling_policy::ScalingPolicyType, CreateScopeStatus, CreateStreamStatus, ScalingPolicy,
    ScopeInfo, StreamConfig, StreamInfo,
};
use snafu::{Backtrace, Snafu};
use tonic::transport::channel::Channel;
use tonic::{Code, Status};

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
        backtrace: Backtrace,
    },
    #[snafu(display("Could not connect to controller {}", endpoint))]
    ConnectionError {
        can_retry: bool,
        endpoint: String,
        error_msg: String,
        backtrace: Backtrace,
    },
}

/// create_connection with the given controller uri.
pub async fn create_connection(uri: &'static str) -> ControllerServiceClient<Channel> {
    // Placeholder to add authentication headers.
    let connection: ControllerServiceClient<Channel> =
        ControllerServiceClient::connect(uri.to_string())
            .await
            .expect("Failed to create a channel");
    connection
}

/// Async function to create scope
pub async fn create_scope(
    request: ScopeInfo,
    ch: &mut ControllerServiceClient<Channel>,
) -> Result<bool, ControllerError> {
    let op_status: Result<tonic::Response<CreateScopeStatus>, tonic::Status> =
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
) -> Result<bool, ControllerError> {
    let op_status: Result<tonic::Response<CreateStreamStatus>, tonic::Status> =
        ch.create_stream(tonic::Request::new(request)).await;
    let operation_name = "CreateStream";
    match op_status {
        Ok(code) => match code.into_inner().status() {
            create_stream_status::Status::Success => Ok(true),
            create_stream_status::Status::StreamExists => Ok(false),
            create_stream_status::Status::InvalidStreamName
            | create_stream_status::Status::ScopeNotFound => Err(ControllerError::OperationError {
                can_retry: false, // do not retry.
                operation: operation_name.into(),
                error_msg: "Invalid Stream/Scope Not Found".into(),
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
