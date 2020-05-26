//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactoryInternal;
use crate::error::*;
use crate::event_stream_writer::{EventSegmentWriter, Incoming, PendingEvent};
use log::{debug, error, warn};
use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
use pravega_rust_client_shared::ScopedSegment;
use pravega_wire_protocol::commands::DataAppendedCommand;
use pravega_wire_protocol::wire_commands::Replies;
use snafu::ResultExt;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::oneshot;
use tokio::time::delay_for;

/// TransactionalEventSegmentWriter contains a EventSegmentWriter that writes to a specific
/// transaction segment.
pub(super) struct TransactionalEventSegmentWriter {
    segment: ScopedSegment,
    event_segment_writer: EventSegmentWriter,
    recevier: Receiver<Incoming>,
    outstanding: VecDeque<oneshot::Receiver<Result<(), EventStreamWriterError>>>,
}

impl TransactionalEventSegmentWriter {
    const CHANNEL_CAPACITY: usize = 100;

    pub(super) fn new(segment: ScopedSegment, retry_policy: RetryWithBackoff) -> Self {
        let (tx, rx) = channel(TransactionalEventSegmentWriter::CHANNEL_CAPACITY);
        let event_segment_writer = EventSegmentWriter::new(segment.clone(), tx, retry_policy);
        TransactionalEventSegmentWriter {
            segment,
            event_segment_writer,
            recevier: rx,
            outstanding: VecDeque::new(),
        }
    }

    /// sets up connection for this transaction segment by sending wirecommand SetupAppend.
    /// If an error happened, try to reconnect to the server.
    pub(super) async fn initialize(&mut self, factory: &ClientFactoryInternal) {
        if let Err(_e) = self.event_segment_writer.setup_connection(factory).await {
            self.event_segment_writer.reconnect(factory).await;
        }
    }

    /// writes event to the sever by calling event_segment_writer.
    pub(super) async fn write_event(
        &mut self,
        event: Vec<u8>,
        factory: &ClientFactoryInternal,
    ) -> Result<(), TransactionalEventSegmentWriterError> {
        self.process_waiting_acks(factory).await?;
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        if let Some(pending_event) = PendingEvent::with_header(None, event, oneshot_tx) {
            self.event_segment_writer
                .write(pending_event)
                .await
                .context(WriterError {})?;
        }
        self.outstanding.push_back(oneshot_rx);
        self.remove_completed()?;
        Ok(())
    }

    /// wait until all the outstanding events has been acked. This is a blocking call.
    pub(super) async fn flush(
        &mut self,
        factory: &ClientFactoryInternal,
    ) -> Result<(), TransactionalEventSegmentWriterError> {
        while !self.outstanding.is_empty() {
            self.process_waiting_acks(factory).await?;
            self.remove_completed()?;
            delay_for(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    /// check if there are any acks from the server and call event_segment_writer to write
    /// the remaining pending events if a current inflight event has been acked.
    async fn process_waiting_acks(
        &mut self,
        factory: &ClientFactoryInternal,
    ) -> Result<(), TransactionalEventSegmentWriterError> {
        loop {
            match self.recevier.try_recv() {
                Ok(event) => {
                    // The incoming reply should always be ServerReply type.
                    if let Incoming::ServerReply(reply) = event {
                        // The reply is expected to be DataAppended wirecommand, other wirecommand
                        // indicates that something went wrong.
                        match reply.reply {
                            Replies::DataAppended(cmd) => {
                                self.process_data_appended(factory, cmd).await;
                            }
                            _ => {
                                error!(
                                    "unexpected reply from segmentstore, transaction failed due to {:?}",
                                    reply
                                );
                                return Err(TransactionalEventSegmentWriterError::UnexpectedReply {
                                    error: reply.reply,
                                });
                            }
                        }
                    } else {
                        panic!("should always be ServerReply");
                    }
                }
                Err(e) => match e {
                    // No reply from the server yet, just return ok.
                    TryRecvError::Empty => return Ok(()),
                    _ => return Err(TransactionalEventSegmentWriterError::MpscError { source: e }),
                },
            }
        }
    }

    async fn process_data_appended(&mut self, factory: &ClientFactoryInternal, cmd: DataAppendedCommand) {
        debug!(
            "data appended for writer {:?}, latest event id is: {:?}",
            self.event_segment_writer.get_uuid(),
            cmd.event_number
        );

        self.event_segment_writer.ack(cmd.event_number);
        if let Err(e) = self.event_segment_writer.write_pending_events().await {
            warn!(
                "writer {:?} failed to flush data to segment {:?} due to {:?}, reconnecting",
                self.event_segment_writer.get_uuid(),
                self.event_segment_writer.get_segment_name(),
                e
            );
            self.event_segment_writer.reconnect(factory).await;
        }
    }

    /// removes the completed events from the outstanding queue.
    fn remove_completed(&mut self) -> Result<(), TransactionalEventSegmentWriterError> {
        while let Some(mut rx) = self.outstanding.pop_front() {
            match rx.try_recv() {
                // the first outstanding event hasn't been acked, so we can just return ok
                Err(oneshot::error::TryRecvError::Empty) => {
                    self.outstanding.push_front(rx);
                    return Ok(());
                }

                Ok(reply) => {
                    if let Err(e) = reply {
                        return Err(TransactionalEventSegmentWriterError::WriterError { source: e });
                    }
                }
                Err(e) => {
                    return Err(TransactionalEventSegmentWriterError::OneshotError { source: e });
                }
            }
        }
        Ok(())
    }
}
