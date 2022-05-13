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
        let mut payload = event.data;
        let mut event_number: i64 = 0;
        let mut expected_offset: i64 = 0;
        loop {
            let payload_left = if payload.len() > EventWriter::MAX_EVENT_SIZE {
                payload.split_off(EventWriter::MAX_EVENT_SIZE)
            } else {
                Vec::new()
            };
            // send ConditionalBlockEnd
            let request = Requests::ConditionalBlockEnd(ConditionalBlockEndCommand {
                writer_id: self.id.0,
                event_number,
                expected_offset,
                data: payload,
                request_id: get_request_id(),
            });
            connection.write(&request).await.context(SegmentWriting {})?;

            let reply = connection.read().await.context(SegmentReading {})?;
            match reply {
                Replies::DataAppended(cmd) => {
                    debug!(
                        "data appended for writer {:?}, latest event id is: {:?}",
                        self.id, cmd.event_number
                    );
                }
                _ => {
                    info!("append data failed due to {:?}", reply);
                    return Err(LargeEventWriterError::WrongReply {
                        expected: String::from("DataAppended"),
                        actual: reply,
                    });
                }
            };
            if payload_left.is_empty() {
                break;
            }
            payload = payload_left;
            event_number += 1;
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
}

#[derive(Debug, Snafu)]
pub enum LargeEventWriterError {
    #[snafu(display("Failed to send request to segmentstore due to: {:?}", source))]
    SegmentWriting { source: ClientConnectionError },

    #[snafu(display("Failed to read response to segmentstore due to: {:?}", source))]
    SegmentReading { source: ClientConnectionError },

    #[snafu(display("Retry failed due to error: {:?}", err))]
    RetryControllerWriting { err: RetryError<ControllerError> },

    #[snafu(display("Raw client failed due to error {:?}", err))]
    RetryRawClient { err: RawClientError },

    #[snafu(display("Wrong reply, expected {:?} but get {:?}", expected, actual))]
    WrongReply { expected: String, actual: Replies },

    #[snafu(display("Wrong host: {:?}", error_msg))]
    WrongHost { error_msg: String },

    #[snafu(display("Conditional check failed: {}", msg))]
    ConditionalCheckFailure { msg: String },

    #[snafu(display("Server indicates that transient segment was already written to: {}", segment))]
    IllegalState { segment: String },

    #[snafu(display("Connection isn't a ClientConnectionImpl"))]
    InvalidConnection {},
}
