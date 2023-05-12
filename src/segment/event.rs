//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
use crate::error::Error;
use pravega_client_shared::*;
use pravega_wire_protocol::commands::{Command, EventCommand};
use pravega_wire_protocol::wire_commands::Replies;

use tokio::sync::oneshot;
use tracing::warn;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) enum Incoming {
    AppendEvent(PendingEvent),
    ServerReply(ServerReply),
    Reconnect(WriterInfo),
    Reset(ScopedSegment),
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
    pub(crate) connection_id: Option<Uuid>,
    pub(crate) writer_id: WriterId,
}

#[derive(Debug)]
pub(crate) enum RoutingInfo {
    RoutingKey(Option<String>),
    Segment(ScopedSegment),
}

#[derive(Debug)]
pub(crate) struct PendingEvent {
    pub(crate) routing_info: RoutingInfo,
    pub(crate) data: Vec<u8>,
    pub(crate) conditional_offset: Option<i64>,
    pub(crate) oneshot_sender: oneshot::Sender<Result<(), Error>>,
    pub(crate) flush_oneshot_sender: Option<oneshot::Sender<Result<(), Error>>>,
}

impl PendingEvent {
    pub(crate) const MAX_WRITE_SIZE: usize = 8 * 1024 * 1024 + 8;
    pub(crate) fn new(
        routing_info: RoutingInfo,
        data: Vec<u8>,
        conditional_offset: Option<i64>,
        oneshot_sender: oneshot::Sender<Result<(), Error>>,
        flush_oneshot_sender: Option<oneshot::Sender<Result<(), Error>>>,
    ) -> Option<Self> {
        Some(PendingEvent {
            routing_info,
            data,
            conditional_offset,
            oneshot_sender,
            flush_oneshot_sender,
        })
    }

    pub(crate) fn with_header_flush(
        routing_info: RoutingInfo,
        data: Vec<u8>,
        conditional_offset: Option<i64>,
        oneshot_sender: oneshot::Sender<Result<(), Error>>,
        flush_oneshot_sender: Option<oneshot::Sender<Result<(), Error>>>,
    ) -> Option<PendingEvent> {
        let cmd = EventCommand { data };
        match cmd.write_fields() {
            Ok(data) => PendingEvent::new(
                routing_info,
                data,
                conditional_offset,
                oneshot_sender,
                flush_oneshot_sender,
            ),
            Err(e) => {
                warn!("failed to serialize event to event command, sending this error back to caller");
                oneshot_sender
                    .send(Err(Error::InternalFailure {
                        msg: format!("Failed to serialize event: {:?}", e),
                    }))
                    .expect("send error to caller");
                None
            }
        }
    }

    pub(crate) fn with_header(
        routing_info: RoutingInfo,
        data: Vec<u8>,
        conditional_offset: Option<i64>,
        oneshot_sender: oneshot::Sender<Result<(), Error>>,
    ) -> Option<PendingEvent> {
        PendingEvent::with_header_flush(routing_info, data, conditional_offset, oneshot_sender, None)
    }

    pub(crate) fn without_header_flush(
        routing_info: RoutingInfo,
        data: Vec<u8>,
        conditional_offset: Option<i64>,
        oneshot_sender: oneshot::Sender<Result<(), Error>>,
        flush_oneshot_sender: Option<oneshot::Sender<Result<(), Error>>>,
    ) -> Option<PendingEvent> {
        PendingEvent::new(
            routing_info,
            data,
            conditional_offset,
            oneshot_sender,
            flush_oneshot_sender,
        )
    }

    pub(crate) fn without_header(
        routing_info: RoutingInfo,
        data: Vec<u8>,
        conditional_offset: Option<i64>,
        oneshot_sender: oneshot::Sender<Result<(), Error>>,
    ) -> Option<PendingEvent> {
        PendingEvent::without_header_flush(routing_info, data, conditional_offset, oneshot_sender, None)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
