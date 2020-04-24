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

use snafu::ResultExt;
use snafu::Snafu;
use tonic::transport::channel::Channel;
use tonic::transport::Error as tonicError;
use tonic::{Code, Status};

use async_trait::async_trait;
use controller::{
    controller_service_client::ControllerServiceClient, create_scope_status, create_stream_status,
    delete_scope_status, delete_stream_status, ping_txn_status, txn_state, txn_status, update_stream_status,
    CreateScopeStatus, CreateStreamStatus, CreateTxnRequest, CreateTxnResponse, DeleteScopeStatus,
    DeleteStreamStatus, NodeUri, PingTxnRequest, PingTxnStatus, ScopeInfo, SegmentId, SegmentRanges,
    StreamConfig, StreamInfo, SuccessorResponse, TxnId, TxnRequest, TxnState, TxnStatus, UpdateStreamStatus,
};
use log::debug;
use pravega_connection_pool::connection_pool::{
    ConnectionPool, ConnectionPoolError, Manager, PooledConnection,
};
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::ClientConfig;
use std::convert::{From, Into};
use std::net::SocketAddr;
use uuid::Uuid;

#[allow(non_camel_case_types)]
pub mod controller {
    tonic::include_proto!("io.pravega.controller.stream.api.grpc.v1");
    // this is the rs file name generated after compiling the proto file, located inside the target folder.
}

pub mod mock_controller;
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
        can_retry: bool,
        endpoint: String,
        error_msg: String,
        source: tonicError,
    },
    #[snafu(display("Could not get connection from connection pool"))]
    PoolError {
        can_retry: bool,
        endpoint: String,
        source: ConnectionPoolError,
    },
}

pub type Result<T> = StdResult<T, ControllerError>;

/// Controller APIs for administrative action for streams
#[async_trait]
pub trait ControllerClient: Send + Sync {
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

pub struct ControllerClientImpl {
    endpoint: SocketAddr,
    pool: ConnectionPool<ControllerConnectionManager>,
}

impl ControllerClientImpl {
    pub fn new(config: ClientConfig) -> Self {
        let endpoint = config.controller_uri;
        let manager = ControllerConnectionManager::new(config);
        let pool = ConnectionPool::new(manager);
        ControllerClientImpl { endpoint, pool }
    }
}

#[allow(unused_variables)]
#[async_trait]
impl ControllerClient for ControllerClientImpl {
    async fn create_scope(&self, scope: &Scope) -> Result<bool> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        create_scope(scope, connection).await
    }

    async fn list_streams(&self, scope: &Scope) -> Result<Vec<String>> {
        unimplemented!()
    }

    async fn delete_scope(&self, scope: &Scope) -> Result<bool> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        delete_scope(scope, connection).await
    }

    async fn create_stream(&self, stream_config: &StreamConfiguration) -> Result<bool> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        create_stream(stream_config, connection).await
    }

    async fn update_stream(&self, stream_config: &StreamConfiguration) -> Result<bool> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        update_stream(stream_config, connection).await
    }

    async fn truncate_stream(&self, stream_cut: &StreamCut) -> Result<bool> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        truncate_stream(stream_cut, connection).await
    }

    async fn seal_stream(&self, stream: &ScopedStream) -> Result<bool> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        seal_stream(stream, connection).await
    }

    async fn delete_stream(&self, stream: &ScopedStream) -> Result<bool> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        delete_stream(stream, connection).await
    }

    async fn get_current_segments(&self, stream: &ScopedStream) -> Result<StreamSegments> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        get_current_segments(stream, connection).await
    }

    async fn create_transaction(&self, stream: &ScopedStream, lease: Duration) -> Result<TxnSegments> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        create_transaction(stream, lease, connection).await
    }

    async fn ping_transaction(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
        lease: Duration,
    ) -> Result<PingStatus> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        ping_transaction(stream, tx_id, lease, connection).await
    }

    async fn commit_transaction(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
        writer_id: WriterId,
        time: Timestamp,
    ) -> Result<()> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        commit_transaction(stream, tx_id, writer_id, time, connection).await
    }

    async fn abort_transaction(&self, stream: &ScopedStream, tx_id: TxId) -> Result<()> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        abort_transaction(stream, tx_id, connection).await
    }

    async fn check_transaction_status(
        &self,
        stream: &ScopedStream,
        tx_id: TxId,
    ) -> Result<TransactionStatus> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        check_transaction_status(stream, tx_id, connection).await
    }

    async fn get_endpoint_for_segment(&self, segment: &ScopedSegment) -> Result<PravegaNodeUri> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        get_uri_segment(segment, connection).await
    }

    async fn get_or_refresh_delegation_token_for(&self, stream: ScopedStream) -> Result<DelegationToken> {
        unimplemented!()
    }

    async fn get_successors(&self, segment: &ScopedSegment) -> Result<StreamSegmentsWithPredecessors> {
        let connection = self.pool.get_connection(self.endpoint).await.context(PoolError {
            can_retry: true,
            endpoint: self.endpoint.to_string(),
        })?;
        get_successors(segment, connection).await
    }
}

pub struct ControllerConnection {
    uuid: Uuid,
    endpoint: SocketAddr,
    channel: ControllerServiceClient<Channel>,
}

impl ControllerConnection {
    fn new(endpoint: SocketAddr, channel: ControllerServiceClient<Channel>) -> Self {
        ControllerConnection {
            uuid: Uuid::new_v4(),
            endpoint,
            channel,
        }
    }
}

pub struct ControllerConnectionManager {
    /// The client configuration.
    config: ClientConfig,
}

impl ControllerConnectionManager {
    pub fn new(config: ClientConfig) -> Self {
        ControllerConnectionManager { config }
    }
}

#[async_trait]
impl Manager for ControllerConnectionManager {
    type Conn = ControllerConnection;

    async fn establish_connection(
        &self,
        endpoint: SocketAddr,
    ) -> std::result::Result<Self::Conn, ConnectionPoolError> {
        let result = create_connection(&format!("{}{}", "http://", &endpoint.to_string())).await;
        match result {
            Ok(channel) => Ok(ControllerConnection::new(endpoint, channel)),
            Err(_e) => Err(ConnectionPoolError::EstablishConnection {
                endpoint: endpoint.to_string(),
                error_msg: String::from("Could not establish connection"),
            }),
        }
    }

    fn is_valid(&self, _conn: &Self::Conn) -> bool {
        true
    }

    fn get_max_connections(&self) -> u32 {
        self.config.max_connections_in_pool
    }
}

/// create_connection with the given controller uri.
async fn create_connection(uri: &str) -> Result<ControllerServiceClient<Channel>> {
    // Placeholder to add authentication headers.
    let connection = ControllerServiceClient::connect(uri.to_string())
        .await
        .context(ConnectionError {
            can_retry: true,
            endpoint: String::from(uri),
            error_msg: String::from("Connection Refused"),
        })?;
    Ok(connection)
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
            error_msg: format!("{:?}", status.code()),
        },
    }
}

/// Async helper function to create scope
async fn create_scope(
    scope: &Scope,
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<bool> {
    use create_scope_status::Status;

    let request: ScopeInfo = ScopeInfo::from(scope);

    let op_status: StdResult<tonic::Response<CreateScopeStatus>, tonic::Status> = connection
        .channel
        .create_scope(tonic::Request::new(request))
        .await;
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
async fn create_stream(
    cfg: &StreamConfiguration,
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<bool> {
    use create_stream_status::Status;

    let request: StreamConfig = StreamConfig::from(cfg);
    let op_status: StdResult<tonic::Response<CreateStreamStatus>, tonic::Status> = connection
        .channel
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to get segment URI.
async fn get_uri_segment(
    request: &ScopedSegment,
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<PravegaNodeUri> {
    let op_status: StdResult<tonic::Response<NodeUri>, tonic::Status> = connection
        .channel
        .get_uri(tonic::Request::new(request.into()))
        .await;
    let operation_name = "get_endpoint";
    match op_status {
        Ok(response) => Ok(response.into_inner()),
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
    .map(PravegaNodeUri::from)
}

/// Async helper function to delete Stream.
async fn delete_scope(
    scope: &Scope,
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<bool> {
    use delete_scope_status::Status;

    let op_status: StdResult<tonic::Response<DeleteScopeStatus>, tonic::Status> = connection
        .channel
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to seal Stream.
async fn seal_stream(
    stream: &ScopedStream,
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<bool> {
    use update_stream_status::Status;

    let request: StreamInfo = StreamInfo::from(stream);
    let op_status: StdResult<tonic::Response<UpdateStreamStatus>, tonic::Status> =
        connection.channel.seal_stream(tonic::Request::new(request)).await;
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
async fn delete_stream(
    stream: &ScopedStream,
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<bool> {
    use delete_stream_status::Status;

    let request: StreamInfo = StreamInfo::from(stream);
    let op_status: StdResult<tonic::Response<DeleteStreamStatus>, tonic::Status> = connection
        .channel
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to update Stream.
async fn update_stream(
    stream_config: &StreamConfiguration,
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<bool> {
    use update_stream_status::Status;

    let request: StreamConfig = StreamConfig::from(stream_config);
    let op_status: StdResult<tonic::Response<UpdateStreamStatus>, tonic::Status> = connection
        .channel
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to truncate Stream.
async fn truncate_stream(
    stream_cut: &StreamCut,
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<bool> {
    use update_stream_status::Status;

    let request: controller::StreamCut = controller::StreamCut::from(stream_cut);
    let op_status: StdResult<tonic::Response<UpdateStreamStatus>, tonic::Status> = connection
        .channel
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to get current Segments in a Stream.
async fn get_current_segments(
    stream: &ScopedStream,
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<StreamSegments> {
    let request: StreamInfo = StreamInfo::from(stream);
    let op_status: StdResult<tonic::Response<SegmentRanges>, tonic::Status> = connection
        .channel
        .get_current_segments(tonic::Request::new(request))
        .await;
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
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<TxnSegments> {
    let request = CreateTxnRequest {
        stream_info: Some(StreamInfo::from(stream)),
        lease: lease.as_millis() as i64,
        scale_grace_period: 0,
    };
    let op_status: StdResult<tonic::Response<CreateTxnResponse>, tonic::Status> = connection
        .channel
        .create_transaction(tonic::Request::new(request))
        .await;
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
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<PingStatus> {
    use ping_txn_status::Status;
    let request = PingTxnRequest {
        stream_info: Some(StreamInfo::from(stream)),
        txn_id: Some(TxnId::from(tx_id)),
        lease: lease.as_millis() as i64,
    };
    let op_status: StdResult<tonic::Response<PingTxnStatus>, tonic::Status> = connection
        .channel
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to commit a Transaction.
async fn commit_transaction(
    stream: &ScopedStream,
    tx_id: TxId,
    writerid: WriterId,
    time: Timestamp,
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<()> {
    use txn_status::Status;
    let request = TxnRequest {
        stream_info: Some(StreamInfo::from(stream)),
        txn_id: Some(TxnId::from(tx_id)),
        writer_id: writerid.0.to_string(),
        timestamp: time.0 as i64,
    };
    let op_status: StdResult<tonic::Response<TxnStatus>, tonic::Status> = connection
        .channel
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to abort a Transaction.
async fn abort_transaction(
    stream: &ScopedStream,
    tx_id: TxId,
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<()> {
    use txn_status::Status;
    let request = TxnRequest {
        stream_info: Some(StreamInfo::from(stream)),
        txn_id: Some(TxnId::from(tx_id)),
        writer_id: "".to_string(),
        timestamp: 0,
    };
    let op_status: StdResult<tonic::Response<TxnStatus>, tonic::Status> = connection
        .channel
        .commit_transaction(tonic::Request::new(request))
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to check Transaction status.
async fn check_transaction_status(
    stream: &ScopedStream,
    tx_id: TxId,
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<TransactionStatus> {
    use txn_state::State;
    let request = TxnRequest {
        stream_info: Some(StreamInfo::from(stream)),
        txn_id: Some(TxnId::from(tx_id)),
        writer_id: "".to_string(),
        timestamp: 0,
    };
    let op_status: StdResult<tonic::Response<TxnState>, tonic::Status> = connection
        .channel
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
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
}

/// Async helper function to get successors
async fn get_successors(
    request: &ScopedSegment,
    mut connection: PooledConnection<'_, ControllerConnection>,
) -> Result<StreamSegmentsWithPredecessors> {
    let scoped_stream = ScopedStream {
        scope: request.scope.clone(),
        stream: request.stream.clone(),
    };
    let segment_id_request = SegmentId {
        stream_info: Some(StreamInfo::from(&scoped_stream)),
        segment_id: request.segment.number,
    };
    debug!("sending get successors request for {:?}", request);
    let op_status: StdResult<tonic::Response<SuccessorResponse>, tonic::Status> = connection
        .channel
        .get_segments_immediately_following(tonic::Request::new(segment_id_request))
        .await;
    let operation_name = "get_successors_segment";
    match op_status {
        Ok(response) => Ok(response.into_inner()),
        Err(status) => Err(map_grpc_error(operation_name, status)),
    }
    .map(StreamSegmentsWithPredecessors::from)
}
