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
use crate::reactor::event::Incoming;
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
                Incoming::AppendEvent(pending_event) => {
                    let segment = selector.get_segment_for_event(&pending_event.routing_key);
                    let event_segment_writer = selector.writers.get_mut(&segment).expect("must have writer");

                    if let Err(e) = event_segment_writer.write(pending_event).await {
                        warn!("failed to write append to segment due to {:?}, reconnecting", e);
                        event_segment_writer.reconnect(&factory).await;
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
                            let segment = ScopedSegment::from(&*cmd.segment);
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
                            let segment = ScopedSegment::from(&*cmd.segment);
                            let inflight = selector.refresh_segment_event_writers_upon_sealed(&segment).await;
                            selector.resend(inflight).await;
                            selector.remove_segment_event_writer(&segment);
                        }

                        _ => {
                            error!(
                                "receive unexpected reply {:?}, closing stream reactor",
                                server_reply.reply
                            );
                            receiver.close();
                            drain_recevier(receiver, ErrorType::OTHER, format!("{:?}", server_reply.reply))
                                .await;
                            break;
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
        if let Err(_e) = writer.setup_connection(&factory).await {
            writer.reconnect(&factory).await;
        }

        loop {
            let event = receiver.recv().await.expect("sender closed, processor exit");
            match event {
                Incoming::AppendEvent(pending_event) => {
                    if let Err(e) = writer.write(pending_event).await {
                        warn!("failed to write append to segment due to {:?}, reconnecting", e);
                        writer.reconnect(&factory).await;
                    }
                }
                Incoming::ServerReply(server_reply) => {
                    match server_reply.reply {
                        Replies::DataAppended(cmd) => {
                            info!("data appended");
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
                            drain_recevier(receiver, ErrorType::SEALED, segment.to_string()).await;
                            break;
                        }

                        // same handling logic as segment sealed reply
                        Replies::NoSuchSegment(cmd) => {
                            info!(
                                "no such segment {:?} due to segment truncation: stack trace {}",
                                cmd.segment, cmd.server_stack_trace
                            );
                            receiver.close();
                            drain_recevier(receiver, ErrorType::NOTFOUND, segment.to_string()).await;
                            break;
                        }

                        _ => {
                            error!(
                                "receive unexpected reply {:?}, closing segment reactor",
                                server_reply.reply
                            );
                            receiver.close();
                            drain_recevier(receiver, ErrorType::OTHER, format!("{:?}", server_reply.reply))
                                .await;
                            break;
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

async fn drain_recevier(mut receiver: Receiver<Incoming>, error_type: ErrorType, msg: String) {
    while let Some(remaining) = receiver.recv().await {
        if let Incoming::AppendEvent(event) = remaining {
            let err = match error_type.clone() {
                ErrorType::SEALED => Err(SegmentWriterError::SegmentIsSealed { msg: msg.clone() }),
                ErrorType::NOTFOUND => Err(SegmentWriterError::NoSuchSegment { msg: msg.clone() }),
                ErrorType::OTHER => Err(SegmentWriterError::Unexpected { msg: msg.clone() }),
            };
            event.oneshot_sender.send(err).expect("send error");
        }
    }
}
