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
use pravega_wire_protocol::wire_commands::Replies;
use snafu::ResultExt;
use std::collections::VecDeque;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::oneshot;

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

    pub(super) async fn initialize(&mut self, factory: &ClientFactoryInternal) {
        if let Err(_e) = self.event_segment_writer.setup_connection(factory).await {
            self.event_segment_writer.reconnect(factory).await;
        }
    }

    async fn receive(
        &mut self,
        factory: &ClientFactoryInternal,
    ) -> Result<(), TransactionalEventSegmentWriterError> {
        loop {
            match self.recevier.try_recv() {
                Ok(event) => {
                    if let Incoming::ServerReply(reply) = event {
                        match reply.reply {
                            Replies::DataAppended(cmd) => {
                                debug!(
                                    "data appended for writer {:?}, latest event id is: {:?}",
                                    self.event_segment_writer.get_uuid(),
                                    cmd.event_number
                                );
                                self.event_segment_writer.ack(cmd.event_number);
                                match self.event_segment_writer.flush().await {
                                    Ok(()) => {
                                        continue;
                                    }
                                    Err(e) => {
                                        warn!("writer {:?} failed to flush data to segment {:?} due to {:?}, reconnecting", self.event_segment_writer.get_uuid(), self.event_segment_writer.get_segment_name(), e);
                                        self.event_segment_writer.reconnect(factory).await;
                                    }
                                }
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
                    TryRecvError::Empty => {
                        return Ok(());
                    }
                    _ => {
                        return Err(TransactionalEventSegmentWriterError::MpscError { source: e });
                    }
                },
            }
        }
    }

    pub(super) async fn write_event(
        &mut self,
        event: Vec<u8>,
        factory: &ClientFactoryInternal,
    ) -> Result<(), TransactionalEventSegmentWriterError> {
        self.receive(factory).await?;
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        if let Some(pending_event) = PendingEvent::with_header(None, event, oneshot_tx) {
            self.event_segment_writer
                .write(pending_event)
                .await
                .context(EventSegmentWriterError {})?;
        }
        self.outstanding.push_back(oneshot_rx);
        self.remove_completed()?;
        Ok(())
    }

    pub(super) async fn flush(
        &mut self,
        factory: &ClientFactoryInternal,
    ) -> Result<(), TransactionalEventSegmentWriterError> {
        while !self.outstanding.is_empty() {
            self.receive(factory).await?;
            self.remove_completed()?;
        }
        Ok(())
    }

    fn remove_completed(&mut self) -> Result<(), TransactionalEventSegmentWriterError> {
        while let Some(mut rx) = self.outstanding.pop_front() {
            match rx.try_recv() {
                // the first write hasn't been acked
                Err(oneshot::error::TryRecvError::Empty) => {
                    self.outstanding.push_front(rx);
                    return Ok(());
                }

                Ok(reply) => {
                    if let Err(e) = reply {
                        return Err(TransactionalEventSegmentWriterError::EventSegmentWriterError {
                            source: e,
                        });
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
