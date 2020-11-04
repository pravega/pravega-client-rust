//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
use tokio::sync::oneshot;
use tracing::warn;

use pravega_rust_client_shared::*;
use pravega_wire_protocol::commands::{Command, EventCommand};
use pravega_wire_protocol::wire_commands::Replies;

use crate::error::*;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) enum Incoming {
    AppendEvent(PendingEvent),
    ServerReply(ServerReply),
    Reconnect(WriterInfo),
    Close(),
}

#[derive(new, Debug)]
pub(crate) struct ServerReply {
    pub(crate) segment: ScopedSegment,
    pub(crate) reply: Replies,
}

#[derive(new, Debug)]
pub(crate) struct WriterInfo {
    pub(crate) segment: ScopedSegment,
    pub(crate) connection_id: Uuid,
    pub(crate) writer_id: WriterId,
}

#[derive(Debug)]
pub(crate) struct PendingEvent {
    pub(crate) routing_key: Option<String>,
    pub(crate) data: Vec<u8>,
    pub(crate) oneshot_sender: oneshot::Sender<Result<(), SegmentWriterError>>,
}

impl PendingEvent {
    pub(crate) const MAX_WRITE_SIZE: usize = 8 * 1024 * 1024 + 8;
    pub(crate) fn new(
        routing_key: Option<String>,
        data: Vec<u8>,
        oneshot_sender: oneshot::Sender<Result<(), SegmentWriterError>>,
    ) -> Option<Self> {
        if data.len() > PendingEvent::MAX_WRITE_SIZE {
            warn!(
                "event size {:?} exceeds limit {:?}",
                data.len(),
                PendingEvent::MAX_WRITE_SIZE
            );
            oneshot_sender
                .send(Err(SegmentWriterError::EventSizeTooLarge {
                    limit: PendingEvent::MAX_WRITE_SIZE,
                    size: data.len(),
                }))
                .expect("send error to caller");
            None
        } else {
            Some(PendingEvent {
                routing_key,
                data,
                oneshot_sender,
            })
        }
    }

    pub(crate) fn with_header(
        routing_key: Option<String>,
        data: Vec<u8>,
        oneshot_sender: oneshot::Sender<Result<(), SegmentWriterError>>,
    ) -> Option<PendingEvent> {
        let cmd = EventCommand { data };
        match cmd.write_fields() {
            Ok(data) => PendingEvent::new(routing_key, data, oneshot_sender),
            Err(e) => {
                warn!("failed to serialize event to event command, sending this error back to caller");
                oneshot_sender
                    .send(Err(SegmentWriterError::ParseToEventCommand { source: e }))
                    .expect("send error to caller");
                None
            }
        }
    }

    pub(crate) fn without_header(
        routing_key: Option<String>,
        data: Vec<u8>,
        oneshot_sender: oneshot::Sender<Result<(), SegmentWriterError>>,
    ) -> Option<PendingEvent> {
        PendingEvent::new(routing_key, data, oneshot_sender)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
