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
    scale_status_response, txn_state, txn_status, update_stream_status, ContinuationToken, CreateScopeStatus,
    CreateStreamStatus, CreateTxnRequest, CreateTxnResponse, DelegationToken, DeleteScopeStatus,
    DeleteStreamStatus, ExistsResponse, GetEpochSegmentsRequest, GetSegmentsRequest, NodeUri, PingTxnRequest,
    PingTxnStatus, ScaleRequest, ScaleResponse, ScaleStatusRequest, ScaleStatusResponse, ScopeInfo,
    ScopesRequest, ScopesResponse, SegmentId, SegmentRanges, SegmentsAtTime, StreamConfig, StreamInfo,
    StreamsInScopeRequest, StreamsInScopeResponse, StreamsInScopeWithTagRequest, SuccessorResponse, TxnId,
    TxnRequest, TxnState, TxnStatus, UpdateStreamStatus,
};
use im::OrdMap;
use ordered_float::OrderedFloat;
use pravega_client_config::credentials::AUTHORIZATION;
use pravega_client_config::ClientConfig;
use pravega_client_retry::retry_async::retry_async;
use pravega_client_retry::retry_policy::RetryWithBackoff;
use pravega_client_retry::retry_result::{RetryError, RetryResult, Retryable};
use pravega_client_retry::wrap_with_async_retry;
use pravega_client_shared::*;
use snafu::Snafu;
use std::convert::{From, Into};
use std::io::BufReader;
use std::str::FromStr;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tokio_rustls::rustls::ClientConfig as RustlsClientConfig;
use tonic::codegen::http::uri::InvalidUri;
use tonic::codegen::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint, Uri};
use tonic::{metadata::MetadataValue, Code, Status};
use tracing::{debug, info, warn};

#[allow(non_camel_case_types)]
pub mod controller {
    tonic::include_proto!("io.pravega.controller.stream.api.grpc.v1");
    // this is the rs file name generated after compiling the proto file, located inside the target folder.
}

pub mod mock_controller;
mod model_helper;
pub mod paginator;

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
    #[snafu(display("Invalid response from the Controller. Error {}", error_msg))]
    InvalidResponse { can_retry: bool, error_msg: String },
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
            InvalidResponse {
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

    /**
     * API to check if the scope exists. The future completes with true in case the scope exists
     * and a false if it does not exist.
     */
    async fn check_scope_exists(&self, scope: &Scope) -> ResultRetry<bool>;

    /**
     * API to list scopes given a continuation token..
     * Use the pravega_controller_client::paginator::list_scopes to paginate over all the scopes.
     */
    async fn list_scopes(&self, token: &CToken) -> ResultRetry<Option<(Vec<Scope>, CToken)>>;

    /**
     * API to list streams under a given scope and continuation token.
     * Use the pravega_controller_client::paginator::list_streams to paginate over all the streams.
     */
    async fn list_streams(
        &self,
        scope: &Scope,
        token: &CToken,
    ) -> ResultRetry<Option<(Vec<ScopedStream>, CToken)>>;

    /**
     * API to list streams associated with the given tag under a given scope and continuation token.
     * Use the pravega_controller_client::paginator::list_streams_for_tag to paginate over all the streams.
     */
    async fn list_streams_for_tag(
        &self,
        scope: &Scope,
        tag: &str,
        token: &CToken,
    ) -> ResultRetry<Option<(Vec<ScopedStream>, CToken)>>;

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
     * API to check if the stream exists. The future completes with true in case the stream exists
     * and a false if it does not exist.
     */
    async fn check_stream_exists(&self, stream: &ScopedStream) -> ResultRetry<bool>;

    /**
     * API to update the configuration of a Stream.
     */
    async fn update_stream(&self, stream_config: &StreamConfiguration) -> ResultRetry<bool>;

    /**
     * API to fetch the Stream Configuration of a Stream.
     */
    async fn get_stream_configuration(&self, stream: &ScopedStream) -> ResultRetry<StreamConfiguration>;

    /**
     * API to fetch the Tags for a Stream.
     */
    async fn get_stream_tags(&self, stream: &ScopedStream) -> ResultRetry<Option<Vec<String>>>;

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
    async fn get_head_segments(
        &self,
        stream: &ScopedStream,
    ) -> ResultRetry<std::collections::HashMap<Segment, i64>>;

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
    async fn get_or_refresh_delegation_token_for(&self, stream: ScopedStream) -> ResultRetry<String>;

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
    /// Check the status of a Stream Scale operation for a given scale epoch. It returns a
    /// `true` if the stream scaling operation has completed and `false` if the stream scaling is
    /// in progress.
    ///
    async fn check_scale(&self, stream: &ScopedStream, scale_epoch: i32) -> ResultRetry<bool>;
}

#[derive(Clone)]
struct AuthInterceptor {
    token: Option<String>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> std::result::Result<tonic::Request<()>, Status> {
        if let Some(ref token_string) = self.token {
            let meta_token = MetadataValue::from_str(token_string).expect("convert to metadata value");
            request.metadata_mut().insert(AUTHORIZATION, meta_token);
        }
        Ok(request)
    }
}

pub struct ControllerClientImpl {
    config: ClientConfig,
    channel: RwLock<ControllerServiceClient<InterceptedService<Channel, AuthInterceptor>>>,
}

fn get_channel(config: &ClientConfig) -> Channel {
    const HTTP_PREFIX: &str = "http://";
    const HTTPS_PREFIX: &str = "https://";

    // Placeholder to add authentication headers.
    let s = if config.is_tls_enabled {
        format!(
            "{}{}:{}",
            HTTPS_PREFIX,
            &config.controller_uri.domain_name(),
            config.controller_uri.port()
        )
    } else {
        format!(
            "{}{}:{}",
            HTTP_PREFIX,
            &config.controller_uri.domain_name(),
            config.controller_uri.port()
        )
    };
    let uri_result = Uri::from_str(s.as_str())
        .map_err(|e1: InvalidUri| ControllerError::InvalidConfiguration {
            can_retry: false,
            error_msg: e1.to_string(),
        })
        .unwrap();

    let endpoints = if config.is_tls_enabled {
        info!(
            "getting channel for uri {:?} with controller TLS enabled",
            uri_result
        );
        let mut rustls_client_config = RustlsClientConfig::new();
        rustls_client_config.alpn_protocols.push(Vec::from("h2"));
        if config.disable_cert_verification {
            rustls_client_config
                .dangerous()
                .set_certificate_verifier(Arc::new(NoVerifier));
        }
        for cert in &config.trustcerts {
            let mut wrapper = BufReader::new(cert.as_bytes());
            let res = rustls_client_config.root_store.add_pem_file(&mut wrapper);
            match res {
                Ok((valid, invalid)) => {
                    debug!(
                        "pem file contains {} valid certs and {} invalid certs",
                        valid, invalid
                    );
                }
                Err(_e) => {
                    debug!("failed to add cert files {}", cert);
                }
            }
        }
        debug!("{} cert files added", rustls_client_config.root_store.len());
        let tls = ClientTlsConfig::new()
            .rustls_client_config(rustls_client_config)
            .domain_name(config.controller_uri.domain_name());
        (0..config.max_controller_connections)
            .map(|_a| {
                Channel::builder(uri_result.clone())
                    .tls_config(tls.clone())
                    .expect("build tls for channel")
            })
            .collect::<Vec<Endpoint>>()
    } else {
        (0..config.max_controller_connections)
            .map(|_a| Channel::builder(uri_result.clone()))
            .collect::<Vec<Endpoint>>()
    };

    Channel::balance_list(endpoints.into_iter())
}

#[allow(deprecated)]
#[allow(unused_variables)]
#[async_trait]
impl ControllerClient for ControllerClientImpl {
    async fn create_scope(&self, scope: &Scope) -> ResultRetry<bool> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_create_scope(scope)
        )
    }

    async fn check_scope_exists(&self, scope: &Scope) -> ResultRetry<bool> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_check_scope_exists(scope)
        )
    }

    async fn list_scopes(&self, token: &CToken) -> ResultRetry<Option<(Vec<Scope>, CToken)>> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_list_scopes(token)
        )
    }

    async fn list_streams(
        &self,
        scope: &Scope,
        token: &CToken,
    ) -> ResultRetry<Option<(Vec<ScopedStream>, CToken)>> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_list_streams(scope, token)
        )
    }

    async fn list_streams_for_tag(
        &self,
        scope: &Scope,
        tag: &str,
        token: &CToken,
    ) -> ResultRetry<Option<(Vec<ScopedStream>, CToken)>> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_list_streams_for_tag(scope, tag, token)
        )
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

    async fn check_stream_exists(&self, stream: &ScopedStream) -> ResultRetry<bool> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_check_stream_exists(stream)
        )
    }

    async fn update_stream(&self, stream_config: &StreamConfiguration) -> ResultRetry<bool> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_update_stream(stream_config)
        )
    }

    async fn get_stream_configuration(&self, stream: &ScopedStream) -> ResultRetry<StreamConfiguration> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_get_stream_configuration(stream)
        )
    }

    async fn get_stream_tags(&self, stream: &ScopedStream) -> ResultRetry<Option<Vec<String>>> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_get_stream_tags(stream)
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
    async fn get_head_segments(
        &self,
        stream: &ScopedStream,
    ) -> ResultRetry<std::collections::HashMap<Segment, i64>> {
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

    async fn get_or_refresh_delegation_token_for(&self, stream: ScopedStream) -> ResultRetry<String> {
        wrap_with_async_retry!(
            self.config.retry_policy.max_tries(MAX_RETRIES),
            self.call_get_delegation_token(&stream)
        )
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
    pub fn new(config: ClientConfig, handle: &Handle) -> Self {
        // actual connection is established lazily.
        let _guard = handle.enter();
        let ch = get_channel(&config);
        let client = if config.is_auth_enabled {
            let token = handle.block_on(config.credentials.get_request_metadata());
            let auth_interceptor = AuthInterceptor { token: Some(token) };

            ControllerServiceClient::with_interceptor(ch, auth_interceptor)
        } else {
            let auth_interceptor = AuthInterceptor { token: None };
            ControllerServiceClient::with_interceptor(ch, auth_interceptor)
        };

        ControllerClientImpl {
            config,
            channel: RwLock::new(client),
        }
    }

    ///
    /// reset method needs to be invoked in the case of ConnectionError.
    /// This logic can be removed once https://github.com/tower-rs/tower/issues/383 is fixed.
    ///
    pub async fn reset(&self) {
        if self.config.is_auth_enabled {
            self.refresh_token_if_needed().await;
        } else {
            let ch = get_channel(&self.config);
            let mut x = self.channel.write().await;
            let auth_interceptor = AuthInterceptor { token: None };
            *x = ControllerServiceClient::with_interceptor(ch, auth_interceptor);
        }
    }

    ///
    /// Tonic library suggests we clone the channel to enable multiplexing of requests.
    /// This is because at the very top level the channel is backed by a `tower_buffer::Buffer`
    /// which runs the connection in a background task and provides a `mpsc` channel interface.
    /// Due to this cloning the `Channel` type is cheap and encouraged.
    ///
    async fn get_controller_client(
        &self,
    ) -> ControllerServiceClient<InterceptedService<Channel, AuthInterceptor>> {
        if self.config.is_auth_enabled && self.config.credentials.is_expired() {
            // get_request_metadata internally checks if token expired before sending request to the server,
            // race condition might happen here but eventually only one request will be sent.
            self.refresh_token_if_needed().await;
        }
        self.channel.read().await.clone()
    }

    async fn refresh_token_if_needed(&self) {
        let ch = get_channel(&self.config);
        let mut x = self.channel.write().await;
        let token = self.config.credentials.get_request_metadata().await;
        let auth_interceptor = AuthInterceptor { token: Some(token) };
        *x = ControllerServiceClient::with_interceptor(ch, auth_interceptor);
    }

    // Method used to translate grpc errors to ControllerError.
    async fn map_grpc_error(&self, operation_name: &str, status: Status) -> ControllerError {
        warn!(
            "controller operation {:?} gets grpc error {:?}",
            operation_name, status
        );
        match status.code() {
            Code::InvalidArgument
            | Code::NotFound
            | Code::AlreadyExists
            | Code::PermissionDenied
            | Code::OutOfRange
            | Code::Unimplemented => ControllerError::OperationError {
                can_retry: false,
                operation: operation_name.into(),
                error_msg: status.to_string(),
            },
            Code::Unauthenticated => {
                self.reset().await;
                ControllerError::OperationError {
                    can_retry: true,
                    operation: operation_name.into(),
                    error_msg: status.to_string(),
                }
            }
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

    async fn call_list_scopes(&self, token: &CToken) -> Result<Option<(Vec<Scope>, CToken)>> {
        let operation_name = "ListScopes";
        let request: ScopesRequest = ScopesRequest {
            continuation_token: Some(ContinuationToken::from(token)),
        };
        debug!("Triggering a request to the controller to list scopes");

        let op_status: StdResult<tonic::Response<ScopesResponse>, tonic::Status> =
            self.get_controller_client().await.list_scopes(request).await;
        match op_status {
            Ok(scopes_with_token) => {
                let result = scopes_with_token.into_inner();
                let mut t: Vec<String> = result.scopes;
                if t.is_empty() {
                    // Empty result from the controller implies no further streams present.
                    Ok(None)
                } else {
                    // update state with the new set of scopes.
                    let scopes_list: Vec<Scope> = t.drain(..).map(Scope::from).collect();
                    let token: Option<ContinuationToken> = result.continuation_token;
                    match token.map(|t| t.token) {
                        None => {
                            warn!("None returned for continuation token list scopes API");
                            Err(ControllerError::InvalidResponse {
                                can_retry: false,
                                error_msg: "No continuation token received from Controller".to_string(),
                            })
                        }
                        Some(ct) => {
                            debug!("Returned token {} for list scopes API", ct);
                            Ok(Some((scopes_list, CToken::from(ct.as_str()))))
                        }
                    }
                }
            }
            Err(status) => {
                debug!("Error {} while listing scopes", status);
                Err(self.map_grpc_error(operation_name, status).await)
            }
        }
    }

    async fn call_list_streams(
        &self,
        scope: &Scope,
        token: &CToken,
    ) -> Result<Option<(Vec<ScopedStream>, CToken)>> {
        let operation_name = "ListStreams";
        let request: StreamsInScopeRequest = StreamsInScopeRequest {
            scope: Some(ScopeInfo::from(scope)),
            continuation_token: Some(ContinuationToken::from(token)),
        };
        debug!(
            "Triggering a request to the controller to list streams for scope {}",
            scope
        );

        let op_status: StdResult<tonic::Response<StreamsInScopeResponse>, tonic::Status> = self
            .get_controller_client()
            .await
            .list_streams_in_scope(request)
            .await;
        match op_status {
            Ok(streams_with_token) => {
                let result = streams_with_token.into_inner();
                let mut t: Vec<StreamInfo> = result.streams;
                if t.is_empty() {
                    // Empty result from the controller implies no further streams present.
                    Ok(None)
                } else {
                    // update state with the new set of streams.
                    let stream_list: Vec<ScopedStream> = t.drain(..).map(|i| i.into()).collect();
                    let token: Option<ContinuationToken> = result.continuation_token;
                    match token.map(|t| t.token) {
                        None => {
                            warn!(
                                "None returned for continuation token list streams API for scope {}",
                                scope
                            );
                            Err(ControllerError::InvalidResponse {
                                can_retry: false,
                                error_msg: "No continuation token received from Controller".to_string(),
                            })
                        }
                        Some(ct) => {
                            debug!("Returned token {} for list streams API under scope {}", ct, scope);
                            Ok(Some((stream_list, CToken::from(ct.as_str()))))
                        }
                    }
                }
            }
            Err(status) => {
                debug!("Error {} while listing streams under scope {}", status, scope);
                Err(self.map_grpc_error(operation_name, status).await)
            }
        }
    }

    async fn call_list_streams_for_tag(
        &self,
        scope: &Scope,
        tag: &str,
        token: &CToken,
    ) -> Result<Option<(Vec<ScopedStream>, CToken)>> {
        let operation_name = "ListStreamsForTag";
        let request: StreamsInScopeWithTagRequest = StreamsInScopeWithTagRequest {
            scope: Some(ScopeInfo::from(scope)),
            tag: tag.to_string(),
            continuation_token: Some(ContinuationToken::from(token)),
        };
        debug!(
            "Triggering a request to the controller to list streams with tag {} under scope {}",
            tag, scope
        );

        let op_status: StdResult<tonic::Response<StreamsInScopeResponse>, tonic::Status> = self
            .get_controller_client()
            .await
            .list_streams_in_scope_for_tag(request)
            .await;
        match op_status {
            Ok(streams_with_token) => {
                let result = streams_with_token.into_inner();
                let mut t: Vec<StreamInfo> = result.streams;
                if t.is_empty() {
                    // Empty result from the controller implies no further streams present.
                    Ok(None)
                } else {
                    // update state with the new set of streams.
                    let stream_list: Vec<ScopedStream> = t.drain(..).map(|i| i.into()).collect();
                    let token: Option<ContinuationToken> = result.continuation_token;
                    match token.map(|t| t.token) {
                        None => {
                            warn!(
                                "None returned for continuation token list streams API for scope {}",
                                scope
                            );
                            Err(ControllerError::InvalidResponse {
                                can_retry: false,
                                error_msg: "No continuation token received from Controller".to_string(),
                            })
                        }
                        Some(ct) => {
                            debug!("Returned token {} for list streams API under scope {}", ct, scope);
                            Ok(Some((stream_list, CToken::from(ct.as_str()))))
                        }
                    }
                }
            }
            Err(status) => {
                debug!("Error {} while listing streams under scope {}", status, scope);
                Err(self.map_grpc_error(operation_name, status).await)
            }
        }
    }

    async fn call_create_scope(&self, scope: &Scope) -> Result<bool> {
        use create_scope_status::Status;
        let operation_name = "CreateScope";
        let request: ScopeInfo = ScopeInfo::from(scope);

        let op_status: StdResult<tonic::Response<CreateScopeStatus>, tonic::Status> = self
            .get_controller_client()
            .await
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

    async fn call_check_scope_exists(&self, scope: &Scope) -> Result<bool> {
        let operation_name = "CheckScopeExists";
        let request: ScopeInfo = ScopeInfo::from(scope);

        let op_status: StdResult<tonic::Response<ExistsResponse>, tonic::Status> = self
            .get_controller_client()
            .await
            .check_scope_exists(tonic::Request::new(request))
            .await;
        match op_status {
            Ok(code) => Ok(code.into_inner().exists),
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_delete_scope(&self, scope: &Scope) -> Result<bool> {
        use delete_scope_status::Status;

        let op_status: StdResult<tonic::Response<DeleteScopeStatus>, tonic::Status> = self
            .get_controller_client()
            .await
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
            .await
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

    async fn call_check_stream_exists(&self, stream: &ScopedStream) -> Result<bool> {
        let request: StreamInfo = StreamInfo::from(stream);
        let op_status: StdResult<tonic::Response<ExistsResponse>, tonic::Status> = self
            .get_controller_client()
            .await
            .check_stream_exists(tonic::Request::new(request))
            .await;
        let operation_name = "CheckStreamExists";
        match op_status {
            Ok(code) => Ok(code.into_inner().exists),
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_update_stream(&self, stream_config: &StreamConfiguration) -> Result<bool> {
        use update_stream_status::Status;

        let request: StreamConfig = StreamConfig::from(stream_config);
        let op_status: StdResult<tonic::Response<UpdateStreamStatus>, tonic::Status> = self
            .get_controller_client()
            .await
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

    async fn call_get_stream_configuration(&self, stream: &ScopedStream) -> Result<StreamConfiguration> {
        let request: StreamInfo = StreamInfo::from(stream);
        let op_status: StdResult<tonic::Response<StreamConfig>, tonic::Status> = self
            .get_controller_client()
            .await
            .get_stream_configuration(tonic::Request::new(request))
            .await;
        let operation_name = "get_stream_configuration";

        match op_status {
            Ok(config) => Ok(StreamConfiguration::from(config.into_inner())),
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }

    async fn call_get_stream_tags(&self, stream: &ScopedStream) -> Result<Option<Vec<String>>> {
        self.call_get_stream_configuration(stream)
            .await
            .map(|cfg| cfg.tags)
    }

    async fn call_truncate_stream(&self, stream_cut: &StreamCut) -> Result<bool> {
        use update_stream_status::Status;

        let request: controller::StreamCut = controller::StreamCut::from(stream_cut);
        let op_status: StdResult<tonic::Response<UpdateStreamStatus>, tonic::Status> = self
            .get_controller_client()
            .await
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
            .await
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
            .await
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

    async fn call_get_head_segments(
        &self,
        stream: &ScopedStream,
    ) -> Result<std::collections::HashMap<Segment, i64>> {
        let request: StreamInfo = StreamInfo::from(stream);
        let op_status: StdResult<tonic::Response<SegmentsAtTime>, tonic::Status> = self
            .get_controller_client()
            .await
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
            .await
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
            .await
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
        };
        let op_status: StdResult<tonic::Response<CreateTxnResponse>, tonic::Status> = self
            .get_controller_client()
            .await
            .create_transaction(tonic::Request::new(request))
            .await;
        let operation_name = "createTransaction";
        match op_status {
            Ok(create_txn_response) => {
                let raw = TxnSegments::from(create_txn_response.into_inner());
                let txn_id = raw.tx_id;
                let processed_map: OrdMap<OrderedFloat<f64>, SegmentWithRange> = raw
                    .stream_segments
                    .key_segment_map
                    .iter()
                    .map(|(k, v)| {
                        let segment_with_range = SegmentWithRange {
                            scoped_segment: ScopedSegment {
                                scope: v.scoped_segment.scope.clone(),
                                stream: v.scoped_segment.stream.clone(),
                                segment: Segment {
                                    number: v.scoped_segment.segment.number,
                                    tx_id: Some(txn_id),
                                },
                            },
                            min_key: v.min_key,
                            max_key: v.max_key,
                        };
                        (k.to_owned(), segment_with_range)
                    })
                    .collect();
                Ok(TxnSegments {
                    stream_segments: StreamSegments {
                        key_segment_map: processed_map,
                    },
                    tx_id: txn_id,
                })
            }
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
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
            .await
            .scale(tonic::Request::new(scale_request))
            .await;
        let operation_name = "scale_stream";

        match op_status {
            Ok(response) => {
                let scale_response = response.into_inner();
                match scale_response.status() {
                    ScaleStreamStatus::Started => {
                        // scale Stream has started. check for its completion.
                        self.check_scale_status(
                            stream,
                            scale_response.epoch,
                            RetryWithBackoff::default_setting(),
                        )
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
            .await
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

    async fn call_get_delegation_token(&self, stream: &ScopedStream) -> Result<String> {
        let op_status: StdResult<tonic::Response<DelegationToken>, tonic::Status> = self
            .get_controller_client()
            .await
            .get_delegation_token(tonic::Request::new(StreamInfo::from(stream)))
            .await;
        let operation_name = "get_delegation_token";
        match op_status {
            Ok(response) => Ok(response.into_inner().delegation_token),
            Err(status) => Err(self.map_grpc_error(operation_name, status).await),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use tonic::{transport::Server, Request, Response, Status};

    use controller::controller_service_server::{ControllerService, ControllerServiceServer};
    use pravega_client_config::connection_type::{ConnectionType, MockType};
    use pravega_client_config::ClientConfigBuilder;
    use std::collections::HashMap;
    use tokio::runtime::Runtime;

    #[test]
    fn test_controller_client() {
        let rt = Runtime::new().unwrap();
        rt.spawn(run_server());

        let config = ClientConfigBuilder::default()
            .connection_type(ConnectionType::Mock(MockType::Happy))
            .controller_uri(PravegaNodeUri::from("127.0.0.2:9091".to_string()))
            .build()
            .unwrap();
        let controller = ControllerClientImpl::new(config.clone(), &rt.handle());
        let scope = Scope {
            name: "scope".to_string(),
        };

        // test create scope
        let res = rt
            .block_on(controller.create_scope(&scope))
            .expect("create scope");
        assert!(res);

        // test create stream
        let scoped_stream = ScopedStream {
            scope: scope.clone(),
            stream: Stream {
                name: "stream".to_string(),
            },
        };
        let stream_config = StreamConfiguration {
            scoped_stream: scoped_stream.clone(),
            scaling: Scaling {
                scale_type: ScaleType::FixedNumSegments,
                target_rate: 0,
                scale_factor: 0,
                min_num_segments: 0,
            },
            retention: Retention {
                retention_type: RetentionType::None,
                retention_param: 0,
            },
            tags: None,
        };
        let res = rt
            .block_on(controller.create_stream(&stream_config))
            .expect("create stream");
        assert!(res);

        // test update stream
        let res = rt
            .block_on(controller.update_stream(&stream_config))
            .expect("update stream");
        assert!(res);

        // test truncate stream
        let cut = StreamCut {
            scoped_stream: scoped_stream.clone(),
            segment_offset_map: HashMap::new(),
        };
        let res = rt
            .block_on(controller.truncate_stream(&cut))
            .expect("truncate stream");
        assert!(res);

        // test seal stream
        let res = rt
            .block_on(controller.seal_stream(&scoped_stream))
            .expect("seal stream");
        assert!(res);

        // test delete stream
        let res = rt
            .block_on(controller.delete_stream(&scoped_stream))
            .expect("delete stream");
        assert!(res);

        // test get current segments
        let res = rt
            .block_on(controller.get_current_segments(&scoped_stream))
            .expect("get current segments");
        assert!(res.key_segment_map.is_empty());

        // test get epoch segments
        let scoped_segment = ScopedSegment {
            scope: scope.clone(),
            stream: Stream {
                name: "stream".to_string(),
            },
            segment: Segment {
                number: 0,
                tx_id: None,
            },
        };
        let res = rt
            .block_on(controller.get_endpoint_for_segment(&scoped_segment))
            .expect("get epoch segments");
        assert_eq!(
            res,
            PravegaNodeUri {
                0: "127.0.0.1:9090".to_string()
            }
        );

        // test get head segments
        let res = rt
            .block_on(controller.get_head_segments(&scoped_stream))
            .expect("get head segments");
        assert!(res.is_empty());

        // test get successors
        let res = rt
            .block_on(controller.get_successors(&scoped_segment))
            .expect("get successors segments");
        assert!(res.segment_with_predecessors.is_empty());

        // test scale
        let segments = vec![];
        let ranges = vec![];
        rt.block_on(controller.scale_stream(&scoped_stream, &segments, &ranges))
            .expect("scale");

        // test check scale
        let res = rt
            .block_on(controller.check_scale(&scoped_stream, 0))
            .expect("check scale");
        assert!(res);

        // test get endpoint
        let uri = rt
            .block_on(controller.get_endpoint_for_segment(&scoped_segment))
            .expect("get node uri");
        assert_eq!(
            uri,
            PravegaNodeUri {
                0: "127.0.0.1:9090".to_string()
            }
        );

        // test create transaction
        let res = rt
            .block_on(controller.create_transaction(&scoped_stream, Duration::from_secs(1)))
            .expect("create transaction");
        assert!(res.stream_segments.key_segment_map.is_empty());

        // test commit transaction
        rt.block_on(controller.commit_transaction(
            &scoped_stream,
            TxId { 0: 0 },
            WriterId { 0: 0 },
            Timestamp { 0: 0 },
        ))
        .expect("commit transaction");

        // test abort transaction
        rt.block_on(controller.abort_transaction(&scoped_stream, TxId { 0: 0 }))
            .expect("abort transaction");

        // test ping transaction
        let res = rt
            .block_on(controller.ping_transaction(&scoped_stream, TxId { 0: 0 }, Duration::from_secs(1)))
            .expect("ping transaction");
        assert_eq!(res, PingStatus::Ok);

        // test check transaction status
        let res = rt
            .block_on(controller.check_transaction_status(&scoped_stream, TxId { 0: 0 }))
            .expect("check transaction status");
        assert_eq!(res, TransactionStatus::Open);

        // test create scope
        let res = rt
            .block_on(controller.create_scope(&scope))
            .expect("create scope");
        assert!(res);

        // test delete scope
        let res = rt
            .block_on(controller.delete_scope(&scope))
            .expect("delete scope");
        assert!(res);

        // test get delegation token
        let res = rt
            .block_on(controller.get_or_refresh_delegation_token_for(scoped_stream))
            .expect("get delegation token");
        assert_eq!(res, "123".to_string());

        // test list streams
        let res = rt
            .block_on(controller.list_streams(&scope, &CToken::empty()))
            .expect("list streams");
        let expected_stream = ScopedStream {
            scope,
            stream: Stream {
                name: String::from("s1"),
            },
        };
        assert_eq!(res, Some((vec![expected_stream], CToken::from("123"))))
    }

    #[derive(Default)]
    pub struct MockServerImpl {}

    #[tonic::async_trait]
    impl ControllerService for MockServerImpl {
        async fn create_stream(
            &self,
            _request: Request<StreamConfig>,
        ) -> std::result::Result<Response<CreateStreamStatus>, Status> {
            let reply = CreateStreamStatus { status: 0 };
            Ok(Response::new(reply))
        }
        async fn update_stream(
            &self,
            _request: Request<StreamConfig>,
        ) -> std::result::Result<Response<UpdateStreamStatus>, Status> {
            let reply = UpdateStreamStatus { status: 0 };
            Ok(Response::new(reply))
        }
        async fn truncate_stream(
            &self,
            _request: Request<controller::StreamCut>,
        ) -> std::result::Result<Response<UpdateStreamStatus>, Status> {
            let reply = UpdateStreamStatus { status: 0 };
            Ok(Response::new(reply))
        }
        async fn seal_stream(
            &self,
            _request: Request<StreamInfo>,
        ) -> std::result::Result<Response<UpdateStreamStatus>, Status> {
            let reply = UpdateStreamStatus { status: 0 };
            Ok(Response::new(reply))
        }
        async fn delete_stream(
            &self,
            _request: Request<StreamInfo>,
        ) -> std::result::Result<Response<DeleteStreamStatus>, Status> {
            let reply = DeleteStreamStatus { status: 0 };
            Ok(Response::new(reply))
        }
        async fn get_current_segments(
            &self,
            _request: Request<StreamInfo>,
        ) -> std::result::Result<Response<SegmentRanges>, Status> {
            let reply = SegmentRanges {
                segment_ranges: vec![],
                delegation_token: "123".to_string(),
            };
            Ok(Response::new(reply))
        }
        async fn get_epoch_segments(
            &self,
            _request: Request<GetEpochSegmentsRequest>,
        ) -> std::result::Result<Response<SegmentRanges>, Status> {
            let reply = SegmentRanges {
                segment_ranges: vec![],
                delegation_token: "123".to_string(),
            };
            Ok(Response::new(reply))
        }
        async fn get_segments(
            &self,
            _request: Request<GetSegmentsRequest>,
        ) -> std::result::Result<Response<SegmentsAtTime>, Status> {
            let reply = SegmentsAtTime {
                segments: vec![],
                delegation_token: "123".to_string(),
            };
            Ok(Response::new(reply))
        }
        async fn get_segments_immediately_following(
            &self,
            _request: Request<SegmentId>,
        ) -> std::result::Result<Response<SuccessorResponse>, Status> {
            let reply = SuccessorResponse {
                segments: vec![],
                delegation_token: "123".to_string(),
            };
            Ok(Response::new(reply))
        }
        async fn scale(
            &self,
            _request: Request<ScaleRequest>,
        ) -> std::result::Result<Response<ScaleResponse>, Status> {
            let reply = ScaleResponse {
                status: scale_response::ScaleStreamStatus::Started as i32,
                segments: vec![],
                epoch: 0,
            };
            Ok(Response::new(reply))
        }
        async fn check_scale(
            &self,
            _request: Request<ScaleStatusRequest>,
        ) -> std::result::Result<Response<ScaleStatusResponse>, Status> {
            let reply = ScaleStatusResponse {
                status: scale_status_response::ScaleStatus::Success as i32,
            };
            Ok(Response::new(reply))
        }
        async fn get_uri(
            &self,
            _request: Request<controller::SegmentId>,
        ) -> std::result::Result<Response<NodeUri>, Status> {
            let reply = NodeUri {
                endpoint: "127.0.0.1".to_string(),
                port: 9090,
            };
            Ok(Response::new(reply))
        }
        async fn create_transaction(
            &self,
            _request: Request<CreateTxnRequest>,
        ) -> std::result::Result<Response<CreateTxnResponse>, Status> {
            let reply = CreateTxnResponse {
                txn_id: Some(controller::TxnId {
                    high_bits: 0,
                    low_bits: 0,
                }),
                active_segments: vec![],
                delegation_token: "123".to_string(),
            };
            Ok(Response::new(reply))
        }
        async fn commit_transaction(
            &self,
            _request: Request<TxnRequest>,
        ) -> std::result::Result<Response<TxnStatus>, Status> {
            let reply = TxnStatus {
                status: txn_status::Status::Success as i32,
            };
            Ok(Response::new(reply))
        }
        async fn abort_transaction(
            &self,
            _request: Request<TxnRequest>,
        ) -> std::result::Result<Response<TxnStatus>, Status> {
            let reply = TxnStatus {
                status: txn_status::Status::Success as i32,
            };
            Ok(Response::new(reply))
        }
        async fn ping_transaction(
            &self,
            _request: Request<PingTxnRequest>,
        ) -> std::result::Result<Response<PingTxnStatus>, Status> {
            let reply = PingTxnStatus {
                status: ping_txn_status::Status::Ok as i32,
            };
            Ok(Response::new(reply))
        }
        async fn check_transaction_state(
            &self,
            _request: Request<TxnRequest>,
        ) -> std::result::Result<Response<TxnState>, Status> {
            let reply = TxnState {
                state: txn_state::State::Open as i32,
            };
            Ok(Response::new(reply))
        }
        async fn create_scope(
            &self,
            _request: Request<ScopeInfo>,
        ) -> std::result::Result<Response<CreateScopeStatus>, Status> {
            let reply = CreateScopeStatus {
                status: create_scope_status::Status::Success as i32,
            };
            Ok(Response::new(reply))
        }
        async fn delete_scope(
            &self,
            _request: Request<ScopeInfo>,
        ) -> std::result::Result<Response<DeleteScopeStatus>, Status> {
            let reply = DeleteScopeStatus {
                status: delete_scope_status::Status::Success as i32,
            };
            Ok(Response::new(reply))
        }
        async fn get_delegation_token(
            &self,
            _request: Request<controller::StreamInfo>,
        ) -> std::result::Result<Response<DelegationToken>, Status> {
            let reply = controller::DelegationToken {
                delegation_token: "123".to_string(),
            };
            Ok(Response::new(reply))
        }
        async fn get_controller_server_list(
            &self,
            _request: Request<controller::ServerRequest>,
        ) -> std::result::Result<Response<controller::ServerResponse>, Status> {
            let reply = controller::ServerResponse { node_uri: vec![] };
            Ok(Response::new(reply))
        }
        async fn get_segments_immediatly_following(
            &self,
            _request: Request<controller::SegmentId>,
        ) -> std::result::Result<Response<controller::SuccessorResponse>, Status> {
            let reply = controller::SuccessorResponse {
                segments: vec![],
                delegation_token: "123".to_string(),
            };
            Ok(Response::new(reply))
        }
        async fn get_segments_between(
            &self,
            _request: Request<controller::StreamCutRange>,
        ) -> std::result::Result<Response<controller::StreamCutRangeResponse>, Status> {
            let reply = controller::StreamCutRangeResponse {
                segments: vec![],
                delegation_token: "123".to_string(),
            };
            Ok(Response::new(reply))
        }
        async fn is_segment_valid(
            &self,
            _request: Request<controller::SegmentId>,
        ) -> std::result::Result<Response<controller::SegmentValidityResponse>, Status> {
            let reply = controller::SegmentValidityResponse { response: true };
            Ok(Response::new(reply))
        }
        async fn is_stream_cut_valid(
            &self,
            _request: Request<controller::StreamCut>,
        ) -> std::result::Result<Response<controller::StreamCutValidityResponse>, Status> {
            let reply = controller::StreamCutValidityResponse { response: true };
            Ok(Response::new(reply))
        }
        async fn list_streams_in_scope(
            &self,
            request: Request<controller::StreamsInScopeRequest>,
        ) -> std::result::Result<Response<controller::StreamsInScopeResponse>, Status> {
            let req = request.into_inner();
            let s1 = ScopedStream {
                scope: Scope {
                    name: req.scope.unwrap().scope,
                },
                stream: Stream {
                    name: String::from("s1"),
                },
            };
            let request_token: String = req.continuation_token.unwrap().token;
            if request_token.is_empty() {
                // first response
                let reply = controller::StreamsInScopeResponse {
                    streams: vec![StreamInfo::from(&s1)],
                    continuation_token: Some(controller::ContinuationToken {
                        token: "123".to_string(),
                    }),
                    status: controller::streams_in_scope_response::Status::Success as i32,
                };
                Ok(Response::new(reply))
            } else {
                // subsequent response
                let reply = controller::StreamsInScopeResponse {
                    streams: vec![],
                    continuation_token: Some(controller::ContinuationToken {
                        token: "123".to_string(),
                    }),
                    status: controller::streams_in_scope_response::Status::Success as i32,
                };
                Ok(Response::new(reply))
            }
        }
        async fn remove_writer(
            &self,
            _request: Request<controller::RemoveWriterRequest>,
        ) -> std::result::Result<Response<controller::RemoveWriterResponse>, Status> {
            let reply = controller::RemoveWriterResponse {
                result: controller::remove_writer_response::Status::Success as i32,
            };
            Ok(Response::new(reply))
        }
        async fn note_timestamp_from_writer(
            &self,
            _request: Request<controller::TimestampFromWriter>,
        ) -> std::result::Result<Response<controller::TimestampResponse>, Status> {
            let reply = controller::TimestampResponse {
                result: controller::timestamp_response::Status::Success as i32,
            };
            Ok(Response::new(reply))
        }
        async fn list_scopes(
            &self,
            _request: tonic::Request<controller::ScopesRequest>,
        ) -> std::result::Result<Response<controller::ScopesResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }

        async fn check_scope_exists(
            &self,
            _request: tonic::Request<controller::ScopeInfo>,
        ) -> std::result::Result<tonic::Response<controller::ExistsResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }
        async fn check_stream_exists(
            &self,
            _request: tonic::Request<controller::StreamInfo>,
        ) -> std::result::Result<tonic::Response<controller::ExistsResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }

        async fn create_key_value_table(
            &self,
            _request: tonic::Request<controller::KeyValueTableConfig>,
        ) -> std::result::Result<tonic::Response<controller::CreateKeyValueTableStatus>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }
        async fn get_current_segments_key_value_table(
            &self,
            _request: tonic::Request<controller::KeyValueTableInfo>,
        ) -> std::result::Result<tonic::Response<controller::SegmentRanges>, tonic::Status> {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }
        async fn list_key_value_tables_in_scope(
            &self,
            _request: tonic::Request<controller::KvTablesInScopeRequest>,
        ) -> std::result::Result<tonic::Response<controller::KvTablesInScopeResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }
        async fn delete_key_value_table(
            &self,
            _request: tonic::Request<controller::KeyValueTableInfo>,
        ) -> std::result::Result<tonic::Response<controller::DeleteKvTableStatus>, tonic::Status> {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }
        async fn list_subscribers(
            &self,
            _request: tonic::Request<controller::StreamInfo>,
        ) -> std::result::Result<tonic::Response<controller::SubscribersResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }
        async fn update_subscriber_stream_cut(
            &self,
            _request: tonic::Request<controller::SubscriberStreamCut>,
        ) -> std::result::Result<tonic::Response<controller::UpdateSubscriberStatus>, tonic::Status> {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }
        async fn create_reader_group(
            &self,
            _request: tonic::Request<controller::ReaderGroupConfiguration>,
        ) -> std::result::Result<tonic::Response<controller::CreateReaderGroupResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }
        async fn get_reader_group_config(
            &self,
            _request: tonic::Request<controller::ReaderGroupInfo>,
        ) -> std::result::Result<tonic::Response<controller::ReaderGroupConfigResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }
        async fn delete_reader_group(
            &self,
            _request: tonic::Request<controller::ReaderGroupInfo>,
        ) -> std::result::Result<tonic::Response<controller::DeleteReaderGroupStatus>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }
        async fn update_reader_group(
            &self,
            _request: tonic::Request<controller::ReaderGroupConfiguration>,
        ) -> std::result::Result<tonic::Response<controller::UpdateReaderGroupResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }
        async fn get_stream_configuration(
            &self,
            _request: tonic::Request<controller::StreamInfo>,
        ) -> std::result::Result<tonic::Response<controller::StreamConfig>, tonic::Status> {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }
        async fn list_streams_in_scope_for_tag(
            &self,
            _request: tonic::Request<controller::StreamsInScopeWithTagRequest>,
        ) -> std::result::Result<tonic::Response<controller::StreamsInScopeResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("Not Implemented"))
        }
    }

    async fn run_server() {
        let addr = "127.0.0.2:9091".parse().unwrap();
        let server = MockServerImpl::default();

        println!("mock controller server listening on {}", addr);

        Server::builder()
            .add_service(ControllerServiceServer::new(server))
            .serve(addr)
            .await
            .unwrap();
    }
}
