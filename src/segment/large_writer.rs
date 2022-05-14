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
use crate::error::Error;
use crate::event::writer::EventWriter;
use crate::segment::event::{PendingEvent, RoutingInfo};
use crate::segment::raw_client::{RawClient, RawClientError};
use crate::segment::selector::SegmentSelector;
use crate::util::{current_span, get_random_u128, get_request_id};

use pravega_client_auth::DelegationTokenProvider;
use pravega_client_retry::retry_result::RetryError;
use pravega_client_shared::*;
use pravega_controller_client::ControllerError;
use pravega_wire_protocol::commands::{
    ConditionalBlockEndCommand, CreateTransientSegmentCommand, MergeSegmentsCommand, SetupAppendCommand,
    NULL_ATTRIBUTE_VALUE,
};
use pravega_wire_protocol::error::ClientConnectionError;
use pravega_wire_protocol::wire_commands::{Replies, Requests};

use snafu::{ResultExt, Snafu};
use std::sync::Arc;
use tracing::{debug, field, info, trace};

pub(crate) struct LargeEventWriter {
    /// Unique id for each SegmentWriter.
    pub(crate) id: WriterId,
    // Delegation token provider used to authenticate client when communicating with segmentstore.
    delegation_token_provider: Arc<DelegationTokenProvider>,
}

impl LargeEventWriter {
    pub(crate) fn new(delegation_token_provider: Arc<DelegationTokenProvider>) -> Self {
        LargeEventWriter {
            id: WriterId::from(get_random_u128()),
            delegation_token_provider,
        }
    }

    pub(crate) async fn write(
        &mut self,
        factory: &ClientFactoryAsync,
        selector: &mut SegmentSelector,
        event: PendingEvent,
    ) -> Result<(), LargeEventWriterError> {
        while let Err(err) = self.write_internal(factory, selector, &event).await {
            if let LargeEventWriterError::SegmentSealed { segment } = err {
                let segment = ScopedSegment::from(&*segment);
                if selector
                    .refresh_segment_event_writers_upon_sealed(&segment)
                    .await
                    .is_some()
                {
                    selector.remove_segment_writer(&segment);
                } else {
                    let sealed_err = LargeEventWriterError::StreamSealed {
                        stream: segment.stream.name,
                    };
                    if event
                        .oneshot_sender
                        .send(Err(Error::InternalFailure {
                            msg: sealed_err.to_string(),
                        }))
                        .is_err()
                    {
                        trace!("failed to send ack back to caller using oneshot due to Receiver dropped");
                    }
                    if let Some(flush_sender) = event.flush_oneshot_sender {
                        if flush_sender.send(Result::Ok(())).is_err() {
                            info!("failed to send ack back to caller using oneshot due to Receiver dropped: event id");
                        }
                    }
                    return Err(sealed_err);
                }
            }
        }
        if event.oneshot_sender.send(Result::Ok(())).is_err() {
            trace!("failed to send ack back to caller using oneshot due to Receiver dropped");
        }
        if let Some(flush_sender) = event.flush_oneshot_sender {
            if flush_sender.send(Result::Ok(())).is_err() {
                info!("failed to send ack back to caller using oneshot due to Receiver dropped: event id");
            }
        }
        Ok(())
    }

    async fn write_internal(
        &mut self,
        factory: &ClientFactoryAsync,
        selector: &mut SegmentSelector,
        event: &PendingEvent,
    ) -> Result<(), LargeEventWriterError> {
        let segment = match &event.routing_info {
            RoutingInfo::RoutingKey(key) => selector.get_segment(key),
            RoutingInfo::Segment(segment) => segment,
        };

        let uri = match factory
                .controller_client()
                .get_endpoint_for_segment(segment) // retries are internal to the controller client.
                .await
        {
            Ok(uri) => uri,
            Err(e) => return Err(LargeEventWriterError::RetryControllerWriting { err: e }),
        };
        current_span().record("host", &field::debug(&uri));

        let raw_client = factory.create_raw_client_for_endpoint(uri);

        // create transient segment
        let request = Requests::CreateTransientSegment(CreateTransientSegmentCommand {
            request_id: get_request_id(),
            writer_id: self.id.0,
            segment: segment.to_string(),
            delegation_token: self
                .delegation_token_provider
                .retrieve_token(factory.controller_client())
                .await,
        });
        debug!(
            "creating transient segment for writer:{:?}/segment:{:?}",
            self.id, segment
        );
        let (reply, mut connection) = raw_client
            .send_setup_request(&request)
            .await
            .map_err(|e| LargeEventWriterError::RetryRawClient { err: e })?;
        let created_segment = match reply {
            Replies::SegmentCreated(cmd) => {
                debug!(
                    "transient segment {} created for writer:{:?}/segment:{:?}",
                    cmd.segment, self.id, segment
                );
                cmd.segment
            }
            _ => {
                info!("creating transient segment failed due to {:?}", reply);
                return Err(LargeEventWriterError::WrongReply {
                    expected: String::from("SegmentCreated"),
                    actual: reply,
                });
            }
        };

        // setup append
        let request = Requests::SetupAppend(SetupAppendCommand {
            request_id: get_request_id(),
            writer_id: self.id.0,
            segment: created_segment.clone(),
            delegation_token: self
                .delegation_token_provider
                .retrieve_token(factory.controller_client())
                .await,
        });
        debug!(
            "setting up append for writer:{:?}/segment:{:?}",
            self.id, created_segment
        );
        let reply = raw_client
            .send_request_with_connection(&request, &mut *connection)
            .await
            .map_err(|e| LargeEventWriterError::RetryRawClient { err: e })?;
        match reply {
            Replies::AppendSetup(cmd) => {
                debug!(
                    "append setup completed for writer:{:?}/segment:{:?} with latest event number {}",
                    self.id, created_segment, cmd.last_event_number
                );
                if cmd.last_event_number != NULL_ATTRIBUTE_VALUE {
                    return Err(LargeEventWriterError::IllegalState {
                        segment: created_segment.to_string(),
                    });
                }
            }
            _ => {
                info!("append setup failed due to {:?}", reply);
                return Err(LargeEventWriterError::WrongReply {
                    expected: String::from("AppendSetup"),
                    actual: reply,
                });
            }
        };
        let mut expected_offset: i64 = 0;
        let chunks = event.data.chunks(EventWriter::MAX_EVENT_SIZE);
        for (event_number, chunk) in (0_i64..).zip(chunks) {
            let data = chunk.to_vec();

            // send ConditionalBlockEnd
            let request = Requests::ConditionalBlockEnd(ConditionalBlockEndCommand {
                writer_id: self.id.0,
                event_number,
                expected_offset,
                data,
                request_id: get_request_id(),
            });
            connection.write(&request).await.context(SegmentWriting {})?;

            let reply = connection.read().await.context(SegmentWriting {})?;
            match reply {
                Replies::DataAppended(cmd) => {
                    debug!(
                        "data appended for writer {:?}, latest event id is: {:?}",
                        self.id, cmd.event_number
                    );
                }
                Replies::SegmentIsSealed(cmd) => {
                    debug!(
                        "segment {:?} sealed: stack trace {}",
                        cmd.segment, cmd.server_stack_trace
                    );
                    return Err(LargeEventWriterError::SegmentSealed { segment: cmd.segment });
                }
                Replies::NoSuchSegment(cmd) => {
                    debug!(
                        "no such segment {:?} due to segment truncation: stack trace {}",
                        cmd.segment, cmd.server_stack_trace
                    );
                    return Err(LargeEventWriterError::SegmentSealed { segment: cmd.segment });
                }
                _ => {
                    info!("append data failed due to {:?}", reply);
                    return Err(LargeEventWriterError::WrongReply {
                        expected: String::from("DataAppended"),
                        actual: reply,
                    });
                }
            };
            expected_offset += EventWriter::MAX_EVENT_SIZE as i64;
        }

        // merge segments
        let request = Requests::MergeSegments(MergeSegmentsCommand {
            request_id: get_request_id(),
            target: segment.to_string(),
            source: created_segment.clone(),
            delegation_token: self
                .delegation_token_provider
                .retrieve_token(factory.controller_client())
                .await,
        });
        debug!(
            "merge segments {} for writer:{:?}/segment:{:?}",
            created_segment, self.id, segment
        );
        let reply = raw_client
            .send_request_with_connection(&request, &mut *connection)
            .await
            .map_err(|e| LargeEventWriterError::RetryRawClient { err: e })?;
        match reply {
            Replies::SegmentsMerged(_) => {
                debug!(
                    "merge segments completed for writer:{:?}/segment:{:?}",
                    self.id, segment
                );
            }
            _ => {
                info!("merge segments failed due to {:?}", reply);
                return Err(LargeEventWriterError::WrongReply {
                    expected: String::from("SegmentsMerged"),
                    actual: reply,
                });
            }
        };
        Ok(())
    }
}

#[derive(Debug, Snafu)]
pub enum LargeEventWriterError {
    #[snafu(display("Failed to send request to segmentstore due to: {:?}", source))]
    SegmentWriting { source: ClientConnectionError },

    #[snafu(display("Retry failed due to error: {:?}", err))]
    RetryControllerWriting { err: RetryError<ControllerError> },

    #[snafu(display("Raw client failed due to error {:?}", err))]
    RetryRawClient { err: RawClientError },

    #[snafu(display("Wrong reply, expected {:?} but get {:?}", expected, actual))]
    WrongReply { expected: String, actual: Replies },

    #[snafu(display("Segment {} is either sealed or truncated", segment))]
    SegmentSealed { segment: String },

    #[snafu(display("Stream {} is sealed", stream))]
    StreamSealed { stream: String },

    #[snafu(display("Server indicates that transient segment was already written to: {}", segment))]
    IllegalState { segment: String },
}
