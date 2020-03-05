#![allow(dead_code)]
use super::ControllerClient;
use super::ControllerError;
use async_trait::async_trait;
use ordered_float::OrderedFloat;
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_connection::ClientConnection;
use pravega_wire_protocol::commands::{CreateSegmentCommand, DeleteSegmentCommand, MergeSegmentsCommand};
use pravega_wire_protocol::error::ClientConnectionError;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::collections::HashSet;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use uuid::Uuid;

static ID_GENERATOR: AtomicUsize = AtomicUsize::new(0);

struct MockController {
    endpoint: String,
    port: i32,
    connection: Box<dyn ClientConnection>, // a fake client connection to send wire command.
    created_scopes: HashMap<String, HashSet<ScopedStream>>,
    created_streams: HashMap<ScopedStream, StreamConfiguration>,
}

#[async_trait]
impl ControllerClient for MockController {
    async fn create_scope(&mut self, scope: &Scope) -> Result<bool, ControllerError> {
        let scope_name = scope.name.clone();
        if self.created_scopes.contains_key(&scope_name) {
            return Ok(false);
        }

        self.created_scopes.insert(scope_name, HashSet::new());
        Ok(true)
    }

    async fn list_streams(&self, scope: &Scope) -> Result<Vec<String>, ControllerError> {
        let streams_set = self
            .created_scopes
            .get(&scope.name)
            .ok_or(ControllerError::OperationError {
                can_retry: false,
                operation: "listStreams".into(),
                error_msg: "Scope not exist".into(),
            })?;
        let mut result = Vec::new();
        for stream in streams_set {
            result.push(stream.stream.name.clone())
        }
        Ok(result)
    }

    async fn delete_scope(&mut self, scope: &Scope) -> Result<bool, ControllerError> {
        let scope_name = scope.name.clone();
        if self.created_scopes.get(&scope_name).is_none() {
            return Ok(false);
        }

        let streams_set = self.created_scopes.get(&scope_name).unwrap();
        if !streams_set.is_empty() {
            Err(ControllerError::OperationError {
                can_retry: false,
                operation: "DeleteScope".into(),
                error_msg: "Scope not empty".into(),
            })
        } else {
            self.created_scopes.remove(&scope_name);
            Ok(true)
        }
    }

    async fn create_stream(&mut self, stream_config: &StreamConfiguration) -> Result<bool, ControllerError> {
        let stream = stream_config.scoped_stream.clone();
        if self.created_streams.contains_key(&stream) {
            return Ok(false);
        }
        if self.created_scopes.get(&stream.scope.name).is_none() {
            return Err(ControllerError::OperationError {
                can_retry: false,
                operation: "create stream".into(),
                error_msg: "Scope does not exist.".into(),
            });
        }
        self.created_streams.insert(stream.clone(), stream_config.clone());
        self.created_scopes
            .get_mut(&stream.scope.name)
            .unwrap()
            .insert(stream.clone());

        for segment in get_segments_for_stream(&stream, &self.created_streams)? {
            let segment_name = segment.to_string();
            create_segment(segment_name, self, false).await?;
        }
        Ok(true)
    }

    async fn update_stream(&mut self, _stream_config: &StreamConfiguration) -> Result<bool, ControllerError> {
        Err(ControllerError::OperationError {
            can_retry: false,
            operation: "update stream".into(),
            error_msg: "unsupported operation.".into(),
        })
    }

    async fn truncate_stream(&mut self, _stream_cut: &StreamCut) -> Result<bool, ControllerError> {
        Err(ControllerError::OperationError {
            can_retry: false,
            operation: "truncate stream".into(),
            error_msg: "unsupported operation.".into(),
        })
    }

    async fn seal_stream(&mut self, _stream: &ScopedStream) -> Result<bool, ControllerError> {
        Err(ControllerError::OperationError {
            can_retry: false,
            operation: "seal stream".into(),
            error_msg: "unsupported operation.".into(),
        })
    }

    async fn delete_stream(&mut self, stream: &ScopedStream) -> Result<bool, ControllerError> {
        if self.created_streams.get(stream).is_none() {
            return Ok(false);
        }

        for segment in get_segments_for_stream(stream, &self.created_streams)? {
            let segment_name = segment.to_string();
            delete_segment(segment_name, self, false).await?;
        }

        self.created_streams.remove(stream);
        self.created_scopes
            .get_mut(&stream.scope.name)
            .unwrap()
            .remove(stream);
        Ok(true)
    }

    async fn get_current_segments(
        &mut self,
        stream: &ScopedStream,
    ) -> Result<StreamSegments, ControllerError> {
        let segments_in_stream = get_segments_for_stream(stream, &self.created_streams)?;
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
            key_segment_map: segments,
        })
    }

    async fn create_transaction(
        &mut self,
        stream: &ScopedStream,
        _lease: Duration,
    ) -> Result<TxnSegments, ControllerError> {
        let uuid = Uuid::new_v4().as_u128();
        let current_segments = self.get_current_segments(stream).await?;
        for segment in current_segments.key_segment_map.values() {
            create_tx_segment(TxId(uuid), segment.scoped_segment.clone(), self, false).await?;
        }

        Ok(TxnSegments {
            key_segment_map: current_segments.key_segment_map,
            tx_id: TxId(uuid),
        })
    }

    async fn ping_transaction(
        &mut self,
        _stream: &ScopedStream,
        _tx_id: TxId,
        _lease: Duration,
    ) -> Result<PingStatus, ControllerError> {
        Err(ControllerError::OperationError {
            can_retry: false, // do not retry.
            operation: "ping transaction".into(),
            error_msg: "unsupported operation.".into(),
        })
    }

    async fn commit_transaction(
        &mut self,
        stream: &ScopedStream,
        tx_id: TxId,
        _writer_id: WriterId,
        _time: Timestamp,
    ) -> Result<(), ControllerError> {
        let current_segments = get_segments_for_stream(stream, &self.created_streams)?;
        for segment in current_segments {
            commit_tx_segment(tx_id, segment, self, false).await?;
        }
        Ok(())
    }

    async fn abort_transaction(&mut self, stream: &ScopedStream, tx_id: TxId) -> Result<(), ControllerError> {
        let current_segments = get_segments_for_stream(stream, &self.created_streams)?;
        for segment in current_segments {
            abort_tx_segment(tx_id, segment, self, false).await?;
        }
        Ok(())
    }

    async fn check_transaction_status(
        &mut self,
        _stream: &ScopedStream,
        _tx_id: TxId,
    ) -> Result<TransactionStatus, ControllerError> {
        Err(ControllerError::OperationError {
            can_retry: false, // do not retry.
            operation: "check transaction status".into(),
            error_msg: "unsupported operation.".into(),
        })
    }

    async fn get_endpoint_for_segment(
        &mut self,
        _segment: &ScopedSegment,
    ) -> Result<PravegaNodeUri, ControllerError> {
        let uri = self.endpoint.clone() + ":" + &self.port.to_string();
        Ok(PravegaNodeUri(uri))
    }

    async fn get_or_refresh_delegation_token_for(
        &self,
        _stream: ScopedStream,
    ) -> Result<DelegationToken, ControllerError> {
        Ok(DelegationToken(String::from("")))
    }

    async fn get_successors(
        &mut self,
        _segment: &ScopedSegment,
    ) -> Result<StreamSegmentsWithPredecessors, ControllerError> {
        Err(ControllerError::OperationError {
            can_retry: false, // do not retry.
            operation: "get successors".into(),
            error_msg: "unsupported operation.".into(),
        })
    }
}

fn get_segments_for_stream(
    stream: &ScopedStream,
    created_streams: &HashMap<ScopedStream, StreamConfiguration>,
) -> Result<Vec<ScopedSegment>, ControllerError> {
    let stream_config = created_streams.get(stream);
    if stream_config.is_none() {
        return Err(ControllerError::OperationError {
            can_retry: false, // do not retry.
            operation: "get segments for stream".into(),
            error_msg: "stream does not exist.".into(),
        });
    }

    let scaling_policy = stream_config.unwrap().scaling.clone();

    if scaling_policy.scale_type != ScaleType::FixedNumSegments {
        return Err(ControllerError::OperationError {
            can_retry: false, // do not retry.
            operation: "get segments for stream".into(),
            error_msg: "Dynamic scaling not supported with a mock controller.".into(),
        });
    }
    let mut result = Vec::with_capacity(scaling_policy.min_num_segments as usize);
    for i in 0..scaling_policy.min_num_segments {
        result.push(ScopedSegment {
            scope: stream.scope.clone(),
            stream: stream.stream.clone(),
            segment: Segment { number: i as i64 },
        });
    }

    Ok(result)
}

async fn create_segment(
    name: String,
    controller: &mut MockController,
    call_server: bool,
) -> Result<bool, ControllerError> {
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
}

async fn delete_segment(
    name: String,
    controller: &mut MockController,
    call_server: bool,
) -> Result<bool, ControllerError> {
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
}

async fn commit_tx_segment(
    uuid: TxId,
    segment: ScopedSegment,
    controller: &mut MockController,
    call_server: bool,
) -> Result<(), ControllerError> {
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
}

async fn abort_tx_segment(
    uuid: TxId,
    segment: ScopedSegment,
    controller: &mut MockController,
    call_server: bool,
) -> Result<(), ControllerError> {
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
}

async fn create_tx_segment(
    uuid: TxId,
    segment: ScopedSegment,
    controller: &mut MockController,
    call_server: bool,
) -> Result<(), ControllerError> {
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
}

async fn send_request_over_connection(
    command: &Requests,
    controller: &mut MockController,
) -> Result<Replies, ClientConnectionError> {
    controller.connection.write(command).await?;
    controller.connection.read().await
}
