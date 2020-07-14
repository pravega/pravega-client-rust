//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

pub(crate) mod event;
pub(crate) mod segment_selector;
pub(crate) mod segment_writer;

use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, error, info, warn};

use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::ClientConfig;
use pravega_wire_protocol::wire_commands::Replies;

use crate::client_factory::ClientFactoryInternal;
use crate::error::*;
use crate::reactor::event::{Incoming, PendingEvent};
use crate::reactor::segment_selector::SegmentSelector;
use crate::reactor::segment_writer::SegmentWriter;

#[derive(new)]
pub(crate) struct StreamReactor {}

impl StreamReactor {
    #[allow(clippy::cognitive_complexity)]
    pub(crate) async fn run(
        stream: ScopedStream,
        sender: Sender<Incoming>,
        mut receiver: Receiver<Incoming>,
        factory: Arc<ClientFactoryInternal>,
        config: ClientConfig,
    ) {
        let mut selector = SegmentSelector::new(stream, sender, config, factory.clone());
        // get the current segments and create corresponding event segment writers
        selector.initialize().await;

        loop {
            let event = receiver.recv().await.expect("sender closed, processor exit");
            match event {
                Incoming::AppendEvent(event) => {
                    let segment = selector.get_segment_for_event(&event.routing_key);
                    let event_segment_writer = selector.writers.get_mut(&segment).expect("must have writer");

                    let option =
                        PendingEvent::with_header(event.routing_key, event.inner, event.oneshot_sender);

                    if let Some(pending_event) = option {
                        if let Err(e) = event_segment_writer.write(pending_event).await {
                            warn!("failed to write append to segment due to {:?}, reconnecting", e);
                            event_segment_writer.reconnect(&factory).await;
                        }
                    }
                }
                Incoming::ServerReply(server_reply) => {
                    // it should always have writer because writer will
                    // not be removed until it receives SegmentSealed reply
                    let writer = selector
                        .writers
                        .get_mut(&server_reply.segment)
                        .expect("should always be able to get event segment writer");

                    match server_reply.reply {
                        Replies::DataAppended(cmd) => {
                            debug!(
                                "data appended for writer {:?}, latest event id is: {:?}",
                                writer.id, cmd.event_number
                            );
                            writer.ack(cmd.event_number);
                            match writer.write_pending_events().await {
                                Ok(()) => {
                                    continue;
                                }
                                Err(e) => {
                                    warn!("writer {:?} failed to flush data to segment {:?} due to {:?}, reconnecting", writer.id ,writer.segment, e);
                                    writer.reconnect(&factory).await;
                                }
                            }
                        }

                        Replies::SegmentIsSealed(cmd) => {
                            debug!("segment {:?} sealed", cmd.segment);
                            let segment = ScopedSegment::from(cmd.segment);
                            let inflight = selector.refresh_segment_event_writers_upon_sealed(&segment).await;
                            selector.resend(inflight).await;
                            selector.remove_segment_event_writer(&segment);
                        }

                        // same handling logic as segment sealed reply
                        Replies::NoSuchSegment(cmd) => {
                            debug!(
                                "no such segment {:?} due to segment truncation: stack trace {}",
                                cmd.segment, cmd.server_stack_trace
                            );
                            let segment = ScopedSegment::from(cmd.segment);
                            let inflight = selector.refresh_segment_event_writers_upon_sealed(&segment).await;
                            selector.resend(inflight).await;
                            selector.remove_segment_event_writer(&segment);
                        }

                        _ => {
                            error!(
                                "receive unexpected reply {:?}, closing event stream writer",
                                server_reply.reply
                            );
                            receiver.close();
                            panic!("{:?}", server_reply.reply);
                        }
                    }
                }
            }
        }
    }
}

#[derive(new)]
pub(crate) struct SegmentReactor {}

impl SegmentReactor {
    #[allow(clippy::cognitive_complexity)]
    pub(crate) async fn run(
        segment: ScopedSegment,
        sender: Sender<Incoming>,
        mut receiver: Receiver<Incoming>,
        factory: Arc<ClientFactoryInternal>,
        config: ClientConfig,
    ) {
        let mut writer = SegmentWriter::new(segment.clone(), sender.clone(), config.retry_policy);
        loop {
            let event = receiver.recv().await.expect("sender closed, processor exit");
            match event {
                Incoming::AppendEvent(event) => {
                    let option =
                        PendingEvent::without_header(event.routing_key, event.inner, event.oneshot_sender);

                    if let Some(pending_event) = option {
                        if let Err(e) = writer.write(pending_event).await {
                            warn!("failed to write append to segment due to {:?}, reconnecting", e);
                            writer.reconnect(&factory).await;
                        }
                    }
                }
                Incoming::ServerReply(server_reply) => {
                    match server_reply.reply {
                        Replies::DataAppended(cmd) => {
                            debug!(
                                "data appended for writer {:?}, latest event id is: {:?}",
                                writer.id, cmd.event_number
                            );
                            writer.ack(cmd.event_number);
                            match writer.write_pending_events().await {
                                Ok(()) => {
                                    continue;
                                }
                                Err(e) => {
                                    warn!("writer {:?} failed to flush data to segment {:?} due to {:?}, reconnecting", writer.id, writer.segment, e);
                                    writer.reconnect(&factory).await;
                                }
                            }
                        }

                        Replies::SegmentIsSealed(cmd) => {
                            info!("segment {:?} sealed", cmd.segment);
                            receiver.close();

                            break;
                        }

                        // same handling logic as segment sealed reply
                        Replies::NoSuchSegment(cmd) => {
                            info!(
                                "no such segment {:?} due to segment truncation: stack trace {}",
                                cmd.segment, cmd.server_stack_trace
                            );
                            receiver.close();
                            break;
                        }

                        _ => {
                            error!(
                                "receive unexpected reply {:?}, closing event stream writer",
                                server_reply.reply
                            );
                            receiver.close();
                            panic!("{:?}", server_reply.reply);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
enum ErrorType {
    SEALED,
    NOTFOUND,
    OTHER,
}

async fn drain_recevier(mut receiver: Receiver<Incoming>, error_type: ErrorType, segment: ScopedSegment) {
    while let Some(remaining) = receiver.recv().await {
        match remaining {
            Incoming::AppendEvent(event) => {
                let err = match error_type.clone() {
                    ErrorType::SEALED => Err(SegmentWriterError::SegmentIsSealed {
                        segment: segment.clone(),
                    }),
                    ErrorType::NOTFOUND => Err(SegmentWriterError::NoSuchSegment {
                        segment: segment.clone(),
                    }),
                    ErrorType::OTHER => Err(SegmentWriterError::Unexpected {
                        segment: segment.clone(),
                    }),
                };
                event.oneshot_sender.send(err).expect("send error");
            }
            _ => {}
        }
    }
}
