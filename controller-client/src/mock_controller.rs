/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
#![allow(dead_code)]
use super::ControllerClient;
use super::ControllerError;
use crate::ResultRetry;
use async_trait::async_trait;
use im::HashMap as ImHashMap;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use ordered_float::OrderedFloat;
use pravega_client_config::connection_type::{ConnectionType, MockType};
use pravega_client_retry::retry_result::RetryError;
use pravega_client_shared::*;
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::commands::{CreateSegmentCommand, DeleteSegmentCommand, MergeSegmentsCommand};
use pravega_wire_protocol::connection_factory::{
    ConnectionFactory, ConnectionFactoryConfig, SegmentConnectionManager,
};
use pravega_wire_protocol::error::ClientConnectionError;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::time::SystemTime;
use tokio::sync::{RwLock, RwLockReadGuard};
use uuid::Uuid;

static ID_GENERATOR: AtomicUsize = AtomicUsize::new(0);

pub struct MockController {
    endpoint: PravegaNodeUri,
    pool: ConnectionPool<SegmentConnectionManager>,
    created_scopes: RwLock<HashMap<String, HashSet<ScopedStream>>>,
    created_streams: RwLock<HashMap<ScopedStream, StreamConfiguration>>,
    transactions: RwLock<HashMap<TxId, TransactionStatus>>,
}

impl MockController {
    pub fn new(endpoint: PravegaNodeUri) -> Self {
        let config = ConnectionFactoryConfig::new(ConnectionType::Mock(MockType::Happy));
        let cf = ConnectionFactory::create(config) as Box<dyn ConnectionFactory>;
        let manager = SegmentConnectionManager::new(cf, 10);
        let pool = ConnectionPool::new(manager);
        MockController {
            endpoint,
            pool,
            created_scopes: RwLock::new(HashMap::new()),
            created_streams: RwLock::new(HashMap::new()),
            transactions: RwLock::new(HashMap::new()),
        }
    }
}
#[async_trait]
impl ControllerClient for MockController {
    async fn create_scope(&self, scope: &Scope) -> Result<bool, RetryError<ControllerError>> {
        let scope_name = scope.name.clone();
        if self.created_scopes.read().await.contains_key(&scope_name) {
            return Ok(false);
        }

        self.created_scopes
            .write()
            .await
            .insert(scope_name, HashSet::new());
        Ok(true)
    }

    async fn list_streams(&self, scope: &Scope) -> Result<Vec<String>, RetryError<ControllerError>> {
        let map_guard = self.created_scopes.read().await;
        let streams_set = map_guard.get(&scope.name).ok_or(RetryError {
            error: ControllerError::OperationError {
                can_retry: false,
                operation: "listStreams".into(),
                error_msg: "Scope not exist".into(),
            },
            total_delay: Duration::from_millis(1),
            tries: 0,
        })?;
        let mut result = Vec::new();
        for stream in streams_set {
            result.push(stream.stream.name.clone())
        }
        Ok(result)
    }

    async fn delete_scope(&self, scope: &Scope) -> Result<bool, RetryError<ControllerError>> {
        let scope_name = scope.name.clone();
        if self.created_scopes.read().await.get(&scope_name).is_none() {
            return Ok(false);
        }

        if !self
            .created_scopes
            .read()
            .await
            .get(&scope_name)
            .unwrap()
            .is_empty()
        {
            Err(RetryError {
                error: ControllerError::OperationError {
                    can_retry: false,
                    operation: "DeleteScope".into(),
                    error_msg: "Scope not empty".into(),
                },
                total_delay: Duration::from_millis(1),
                tries: 0,
            })
        } else {
            self.created_scopes.write().await.remove(&scope_name);
            Ok(true)
        }
    }

    async fn create_stream(
        &self,
        stream_config: &StreamConfiguration,
    ) -> Result<bool, RetryError<ControllerError>> {
        let stream = stream_config.scoped_stream.clone();
        if self.created_streams.read().await.contains_key(&stream) {
            return Ok(false);
        }
        if self.created_scopes.read().await.get(&stream.scope.name).is_none() {
            return Err(RetryError {
                error: ControllerError::OperationError {
                    can_retry: false,
                    operation: "create stream".into(),
                    error_msg: "Scope does not exist.".into(),
                },
                total_delay: Duration::from_millis(1),
                tries: 0,
            });
        }
        self.created_streams
            .write()
            .await
            .insert(stream.clone(), stream_config.clone());
        self.created_scopes
            .write()
            .await
            .get_mut(&stream.scope.name)
            .unwrap()
            .insert(stream.clone());

        let read_guard = &self.created_streams.read().await;
        for segment in get_segments_for_stream(&stream, read_guard)? {
            let segment_name = segment.to_string();
            create_segment(segment_name, self, false).await?;
        }
        Ok(true)
    }

    async fn update_stream(
        &self,
        _stream_config: &StreamConfiguration,
    ) -> Result<bool, RetryError<ControllerError>> {
        Err(RetryError {
            error: ControllerError::OperationError {
                can_retry: false,
                operation: "update stream".into(),
                error_msg: "unsupported operation.".into(),
            },
            total_delay: Duration::from_millis(1),
            tries: 0,
        })
    }

    async fn truncate_stream(&self, _stream_cut: &StreamCut) -> Result<bool, RetryError<ControllerError>> {
        Err(RetryError {
            error: ControllerError::OperationError {
                can_retry: false,
                operation: "truncate stream".into(),
                error_msg: "unsupported operation.".into(),
            },
            total_delay: Duration::from_millis(1),
            tries: 0,
        })
    }

    async fn seal_stream(&self, _stream: &ScopedStream) -> Result<bool, RetryError<ControllerError>> {
        Err(RetryError {
            error: ControllerError::OperationError {
                can_retry: false,
                operation: "seal stream".into(),
                error_msg: "unsupported operation.".into(),
            },
            total_delay: Duration::from_millis(1),
            tries: 0,
        })
    }

    async fn delete_stream(&self, stream: &ScopedStream) -> Result<bool, RetryError<ControllerError>> {
        if self.created_streams.read().await.get(stream).is_none() {
            return Ok(false);
        }

        for segment in get_segments_for_stream(stream, &self.created_streams.read().await)? {
            let segment_name = segment.to_string();
            delete_segment(segment_name, self, false).await?;
        }

        self.created_streams.write().await.remove(stream);
        self.created_scopes
            .write()
            .await
            .get_mut(&stream.scope.name)
            .unwrap()
            .remove(stream);
        Ok(true)
    }

    async fn get_current_segments(
        &self,
        stream: &ScopedStream,
    ) -> Result<StreamSegments, RetryError<ControllerError>> {
        let segments_in_stream = get_segments_for_stream(stream, &self.created_streams.read().await)?;
        let mut segments = BTreeMap::new();
        let increment = 1.0 / segments_in_stream.len() as f64;
        for (number, segment) in segments_in_stream.into_iter().enumerate() {
            let segment_with_range = SegmentWithRange {
                scoped_segment: segment,
                min_key: OrderedFloat(number as f64 * increment),
                max_key: OrderedFloat((number + 1) as f64 * increment),
            };
            segments.insert(segment_with_range.max_key, segment_with_range);
        }

        Ok(StreamSegments {
            key_segment_map: segments.into(),
        })
    }

    async fn get_epoch_segments(
        &self,
        stream: &ScopedStream,
        _epoch: i32,
    ) -> Result<StreamSegments, RetryError<ControllerError>> {
        let segments_in_stream = get_segments_for_stream(stream, &self.created_streams.read().await)?;
        let mut segments = BTreeMap::new();
        let increment = 1.0 / segments_in_stream.len() as f64;
        for (number, segment) in segments_in_stream.into_iter().enumerate() {
            let segment_with_range = SegmentWithRange {
                scoped_segment: segment,
                min_key: OrderedFloat(number as f64 * increment),
                max_key: OrderedFloat((number + 1) as f64 * increment),
            };
            segments.insert(segment_with_range.max_key, segment_with_range);
        }

        Ok(StreamSegments {
            key_segment_map: segments.into(),
        })
    }

    async fn get_head_segments(&self, stream: &ScopedStream) -> ResultRetry<ImHashMap<Segment, i64>> {
        let segments_in_stream: Vec<ScopedSegment> =
            get_segments_for_stream(stream, &self.created_streams.read().await)?;
        Ok(segments_in_stream
            .iter()
            .map(|t| (t.segment.clone(), 0i64))
            .collect())
    }

    async fn create_transaction(
        &self,
        stream: &ScopedStream,
        _lease: Duration,
    ) -> Result<TxnSegments, RetryError<ControllerError>> {
        let uuid = Uuid::new_v4().as_u128();
        let current_segments = self.get_current_segments(stream).await?;
        let mut guard = self.transactions.write().await;
        guard.insert(TxId(uuid), TransactionStatus::Open);

        Ok(TxnSegments {
            stream_segments: current_segments,
            tx_id: TxId(uuid),
        })
    }

    async fn ping_transaction(
        &self,
        _stream: &ScopedStream,
        tx_id: TxId,
        _lease: Duration,
    ) -> Result<PingStatus, RetryError<ControllerError>> {
        let guard = self.transactions.read().await;
        let status = guard.get(&tx_id).expect("get transaction status");
        match status {
            TransactionStatus::Committed => Ok(PingStatus::Committed),
            TransactionStatus::Aborted => Ok(PingStatus::Aborted),
            _ => Ok(PingStatus::Ok),
        }
    }

    async fn commit_transaction(
        &self,
        _stream: &ScopedStream,
        tx_id: TxId,
        _writer_id: WriterId,
        _time: Timestamp,
    ) -> Result<(), RetryError<ControllerError>> {
        let mut guard = self.transactions.write().await;
        guard.insert(tx_id, TransactionStatus::Committed);
        Ok(())
    }

    async fn abort_transaction(
        &self,
        _stream: &ScopedStream,
        tx_id: TxId,
    ) -> Result<(), RetryError<ControllerError>> {
        let mut guard = self.transactions.write().await;
        guard.insert(tx_id, TransactionStatus::Aborted);
        Ok(())
    }

    async fn check_transaction_status(
        &self,
        _stream: &ScopedStream,
        tx_id: TxId,
    ) -> Result<TransactionStatus, RetryError<ControllerError>> {
        let guard = self.transactions.read().await;
        let status = guard.get(&tx_id).expect("get transaction");
        Ok(status.clone())
    }

    async fn get_endpoint_for_segment(
        &self,
        _segment: &ScopedSegment,
    ) -> Result<PravegaNodeUri, RetryError<ControllerError>> {
        Ok(self.endpoint.clone())
    }

    async fn get_or_refresh_delegation_token_for(
        &self,
        _stream: ScopedStream,
    ) -> Result<String, RetryError<ControllerError>> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("get unix time");
        let timeout = Duration::from_secs(5);
        let expiry_time = now.checked_add(timeout).expect("calculate expiry time");
        let claims = Claims {
            sub: "subject".to_string(),
            aud: "segmentstore".to_string(),
            iat: now.as_secs(),
            exp: expiry_time.as_secs(),
        };

        let mut header = Header::default();
        header.typ = Some("JWT".to_owned());
        header.alg = Algorithm::HS256;

        let key = b"secret";
        let token = encode(&header, &claims, &EncodingKey::from_secret(key)).expect("encode to JWT token");
        Ok(token)
    }

    async fn get_successors(
        &self,
        _segment: &ScopedSegment,
    ) -> Result<StreamSegmentsWithPredecessors, RetryError<ControllerError>> {
        // empty hash map means the stream is sealed
        Ok(StreamSegmentsWithPredecessors {
            segment_with_predecessors: ImHashMap::new(),
            replacement_segments: ImHashMap::new(),
        })
    }

    async fn scale_stream(
        &self,
        _stream: &ScopedStream,
        _sealed_segments: &[Segment],
        _new_key_ranges: &[(f64, f64)],
    ) -> Result<(), RetryError<ControllerError>> {
        Err(RetryError {
            error: ControllerError::OperationError {
                can_retry: false, // do not retry.
                operation: "scale stream".into(),
                error_msg: "unsupported operation.".into(),
            },
            total_delay: Duration::from_millis(1),
            tries: 0,
        })
    }

    async fn check_scale(
        &self,
        _stream: &ScopedStream,
        _scale_epoch: i32,
    ) -> Result<bool, RetryError<ControllerError>> {
        Err(RetryError {
            error: ControllerError::OperationError {
                can_retry: false, // do not retry.
                operation: "check stream scale".into(),
                error_msg: "unsupported operation.".into(),
            },
            total_delay: Duration::from_millis(1),
            tries: 0,
        })
    }
}

fn get_segments_for_stream(
    stream: &ScopedStream,
    created_streams: &RwLockReadGuard<HashMap<ScopedStream, StreamConfiguration>>,
) -> Result<Vec<ScopedSegment>, RetryError<ControllerError>> {
    let stream_config = created_streams.get(stream);
    if stream_config.is_none() {
        return Err(RetryError {
            error: ControllerError::OperationError {
                can_retry: false, // do not retry.
                operation: "get segments for stream".into(),
                error_msg: "stream does not exist.".into(),
            },
            total_delay: Duration::from_millis(1),
            tries: 0,
        });
    }

    let scaling_policy = stream_config.unwrap().scaling.clone();

    if scaling_policy.scale_type != ScaleType::FixedNumSegments {
        return Err(RetryError {
            error: ControllerError::OperationError {
                can_retry: false, // do not retry.
                operation: "get segments for stream".into(),
                error_msg: "Dynamic scaling not supported with a mock controller.".into(),
            },
            total_delay: Duration::from_millis(1),
            tries: 0,
        });
    }
    let mut result = Vec::with_capacity(scaling_policy.min_num_segments as usize);
    for i in 0..scaling_policy.min_num_segments {
        result.push(ScopedSegment {
            scope: stream.scope.clone(),
            stream: stream.stream.clone(),
            segment: Segment::from(i.into()),
        });
    }

    Ok(result)
}

async fn create_segment(
    name: String,
    controller: &MockController,
    call_server: bool,
) -> Result<bool, RetryError<ControllerError>> {
    if !call_server {
        return Ok(true);
    }
    let scale_type = ScaleType::FixedNumSegments;
    let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
    let command = Requests::CreateSegment(CreateSegmentCommand {
        request_id: id,
        segment: name,
        target_rate: 0,
        scale_type: scale_type as u8,
        delegation_token: String::from(""),
    });
    let reply = send_request_over_connection(&command, controller).await;
    match reply {
        Ok(r) => {
            match r {
                Replies::WrongHost(_) => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "create segment".into(),
                    error_msg: "Wrong host.".into(),
                }),
                Replies::SegmentCreated(_) => Ok(true),
                Replies::SegmentAlreadyExists(_) => Ok(false),
                Replies::AuthTokenCheckFailed(_) => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "create segment".into(),
                    error_msg: "authToken check failed,".into(),
                }),
                _ => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "create segment".into(),
                    error_msg: "Unsupported Command".into(),
                }),
            }
        }
        Err(_e) => Err(ControllerError::OperationError {
            can_retry: false, // do not retry.
            operation: "create segment".into(),
            error_msg: "Connection Error".into(),
        }),
    }
    .map_err(|e| RetryError {
        error: e,
        total_delay: Duration::from_millis(1),
        tries: 0,
    })
}

async fn delete_segment(
    name: String,
    controller: &MockController,
    call_server: bool,
) -> Result<bool, RetryError<ControllerError>> {
    if !call_server {
        return Ok(true);
    }
    let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
    let command = Requests::DeleteSegment(DeleteSegmentCommand {
        request_id: id,
        segment: name,
        delegation_token: String::from(""),
    });
    let reply = send_request_over_connection(&command, controller).await;
    match reply {
        Err(_e) => Err(ControllerError::OperationError {
            can_retry: false, // do not retry.
            operation: "delete segment".into(),
            error_msg: "Connection Error".into(),
        }),
        Ok(r) => {
            match r {
                Replies::WrongHost(_) => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "delete segment".into(),
                    error_msg: "Wrong host.".into(),
                }),
                Replies::SegmentDeleted(_) => Ok(true),
                Replies::NoSuchSegment(_) => Ok(false),
                Replies::AuthTokenCheckFailed(_) => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "delete segment".into(),
                    error_msg: "authToken check failed,".into(),
                }),
                _ => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "delete segment".into(),
                    error_msg: "Unsupported Command.".into(),
                }),
            }
        }
    }
    .map_err(|e| RetryError {
        error: e,
        total_delay: Duration::from_millis(1),
        tries: 0,
    })
}

async fn commit_tx_segment(
    uuid: TxId,
    segment: ScopedSegment,
    controller: &MockController,
    call_server: bool,
) -> Result<(), RetryError<ControllerError>> {
    if !call_server {
        return Ok(());
    }
    let source_name = segment.scope.name.clone() + &uuid.to_string();
    let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
    let command = Requests::MergeSegments(MergeSegmentsCommand {
        request_id: id,
        target: segment.to_string(),
        source: source_name,
        delegation_token: String::from(""),
    });
    let reply = send_request_over_connection(&command, controller).await;
    match reply {
        Err(_e) => Err(ControllerError::OperationError {
            can_retry: false, // do not retry.
            operation: "commit tx segment".into(),
            error_msg: "Connection Error".into(),
        }),
        Ok(r) => {
            match r {
                Replies::SegmentsMerged(_) => Ok(()),
                Replies::WrongHost(_) => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "commit tx segment".into(),
                    error_msg: "Wrong host.".into(),
                }),
                Replies::SegmentDeleted(_) => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "commit tx segment".into(),
                    error_msg: "Transaction already aborted.".into(),
                }),
                Replies::AuthTokenCheckFailed(_) => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "commit tx segment".into(),
                    error_msg: "authToken check failed,".into(),
                }),
                _ => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "commit tx segment".into(),
                    error_msg: "Unsupported Command,".into(),
                }),
            }
        }
    }
    .map_err(|e| RetryError {
        error: e,
        total_delay: Duration::from_millis(1),
        tries: 0,
    })
}

async fn abort_tx_segment(
    uuid: TxId,
    segment: ScopedSegment,
    controller: &MockController,
    call_server: bool,
) -> Result<(), RetryError<ControllerError>> {
    if !call_server {
        return Ok(());
    }
    let transaction_name = segment.scope.name.clone() + &uuid.to_string();
    let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
    let command = Requests::DeleteSegment(DeleteSegmentCommand {
        request_id: id,
        segment: transaction_name,
        delegation_token: String::from(""),
    });
    let reply = send_request_over_connection(&command, controller).await;
    match reply {
        Err(_e) => Err(ControllerError::OperationError {
            can_retry: false, // do not retry.
            operation: "abort tx segment".into(),
            error_msg: "Connection Error".into(),
        }),
        Ok(r) => {
            match r {
                Replies::SegmentsMerged(_) => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "abort tx segment".into(),
                    error_msg: "Transaction already committed.".into(),
                }),
                Replies::WrongHost(_) => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "abort tx segment".into(),
                    error_msg: "Wrong host.".into(),
                }),
                Replies::AuthTokenCheckFailed(_) => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "abort tx segment".into(),
                    error_msg: "authToken check failed,".into(),
                }),
                Replies::SegmentDeleted(_) => Ok(()),
                _ => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "abort tx segment".into(),
                    error_msg: "Unsupported Command,".into(),
                }),
            }
        }
    }
    .map_err(|e| RetryError {
        error: e,
        total_delay: Duration::from_millis(1),
        tries: 0,
    })
}

async fn create_tx_segment(
    uuid: TxId,
    segment: ScopedSegment,
    controller: &MockController,
    call_server: bool,
) -> Result<(), RetryError<ControllerError>> {
    if !call_server {
        return Ok(());
    }
    let transaction_name = segment.scope.name.clone() + &uuid.to_string();
    let scale_type = ScaleType::FixedNumSegments;
    let id = ID_GENERATOR.fetch_add(1, Ordering::SeqCst) as i64;
    let command = Requests::CreateSegment(CreateSegmentCommand {
        request_id: id,
        segment: transaction_name,
        target_rate: 0,
        scale_type: scale_type as u8,
        delegation_token: String::from(""),
    });
    let reply = send_request_over_connection(&command, controller).await;
    match reply {
        Err(_e) => Err(ControllerError::OperationError {
            can_retry: false, // do not retry.
            operation: "abort tx segment".into(),
            error_msg: "Connection Error".into(),
        }),
        Ok(r) => {
            match r {
                Replies::SegmentCreated(_) => Ok(()),
                Replies::WrongHost(_) => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "create tx segment".into(),
                    error_msg: "Wrong host.".into(),
                }),
                Replies::AuthTokenCheckFailed(_) => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "create tx segment".into(),
                    error_msg: "authToken check failed,".into(),
                }),
                _ => Err(ControllerError::OperationError {
                    can_retry: false, // do not retry.
                    operation: "create tx segment".into(),
                    error_msg: "Unsupported Command,".into(),
                }),
            }
        }
    }
    .map_err(|e| RetryError {
        error: e,
        total_delay: Duration::from_millis(1),
        tries: 0,
    })
}

async fn send_request_over_connection(
    command: &Requests,
    controller: &MockController,
) -> Result<Replies, ClientConnectionError> {
    let pooled_connection = controller
        .pool
        .get_connection(controller.endpoint.clone())
        .await
        .expect("get connection from pool");
    let mut connection = ClientConnectionImpl {
        connection: pooled_connection,
    };
    connection.write(command).await?;
    connection.read().await
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    aud: String,
    iat: u64,
    exp: u64,
}
