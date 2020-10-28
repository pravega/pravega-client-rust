//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_rust_client_channel::ChannelReceiver;
use tracing::{debug, error, info, warn};

use pravega_rust_client_shared::*;
use pravega_wire_protocol::wire_commands::Replies;

use crate::client_factory::ClientFactory;
use crate::reactor::event::{Incoming, ServerReply};
use crate::reactor::segment_selector::SegmentSelector;

#[derive(new)]
pub(crate) struct Reactor {}

impl Reactor {
    pub(crate) async fn run(
        mut selector: SegmentSelector,
        mut receiver: ChannelReceiver<Incoming>,
        factory: ClientFactory,
    ) {
        info!("starting reactor");
        while Reactor::run_once(&mut selector, &mut receiver, &factory)
            .await
            .is_ok()
        {}
        info!("reactor is closed");
    }

    async fn run_once(
        selector: &mut SegmentSelector,
        receiver: &mut Receiver<Incoming>,
        factory: &ClientFactory,
    ) -> Result<(), &'static str> {
        let event = receiver.recv().await.expect("sender closed, processor exit");
        match event {
            Incoming::AppendEvent(pending_event) => {
                let event_segment_writer = selector.get_segment_writer(&pending_event.routing_key);

                if let Err(e) = event_segment_writer.write(pending_event).await {
                    warn!("failed to write append to segment due to {:?}, reconnecting", e);
                    event_segment_writer.reconnect(factory).await;
                }
                Ok(())
            }
            Incoming::ServerReply(server_reply) => {
                if let Err(e) = Reactor::process_server_reply(server_reply, selector, factory).await {
                    error!("failed to process server reply due to {:?}", e);
                    Err(e)
                } else {
                    Ok(())
                }
            }
            Incoming::ConnectionFailure(connection_failure) => {
                let writer = selector
                    .writers
                    .get_mut(&connection_failure.segment)
                    .expect("must have writer");
                writer.reconnect(factory).await;
                Ok(())
            }
        }
    }

    async fn process_server_reply(
        server_reply: ServerReply,
        selector: &mut SegmentSelector,
        factory: &ClientFactory,
    ) -> Result<(), &'static str> {
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
                if let Err(e) = writer.write_pending_events().await {
                    warn!(
                        "writer {:?} failed to flush data to segment {:?} due to {:?}, reconnecting",
                        writer.id, writer.segment, e
                    );
                    writer.reconnect(factory).await;
                }
                Ok(())
            }

            Replies::SegmentIsSealed(cmd) => {
                debug!(
                    "segment {:?} sealed: stack trace {}",
                    cmd.segment, cmd.server_stack_trace
                );
                let segment = ScopedSegment::from(&*cmd.segment);
                if let Some(inflight) = selector.refresh_segment_event_writers_upon_sealed(&segment).await {
                    selector.resend(inflight).await;
                    selector.remove_segment_event_writer(&segment);
                    Ok(())
                } else {
                    Err("Stream is sealed")
                }
            }

            Replies::NoSuchSegment(cmd) => {
                debug!(
                    "no such segment {:?} due to segment truncation: stack trace {}",
                    cmd.segment, cmd.server_stack_trace
                );
                let segment = ScopedSegment::from(&*cmd.segment);
                if let Some(inflight) = selector.refresh_segment_event_writers_upon_sealed(&segment).await {
                    selector.resend(inflight).await;
                    selector.remove_segment_event_writer(&segment);
                    Ok(())
                } else {
                    Err("Stream is sealed")
                }
            }

            Replies::WrongHost(cmd) => {
                warn!(
                    "wrong host {:?} : stack trace {}",
                    cmd.segment, cmd.server_stack_trace
                );
                // reconnect will try to set up connection using updated endpoint
                writer.reconnect(factory).await;
                Ok(())
            }

            _ => {
                error!(
                    "receive unexpected reply {:?}, closing stream reactor",
                    server_reply.reply
                );
                Err("Unexpected reply")
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::error::*;
    use crate::reactor::event::PendingEvent;
    use crate::reactor::segment_selector::test::create_segment_selector;
    use crate::reactor::segment_writer::SegmentWriter;
    use pravega_rust_client_config::connection_type::MockType;
    use tokio::sync::oneshot;

    type EventHandle = oneshot::Receiver<Result<(), SegmentWriterError>>;

    #[test]
    fn test_reactor_happy_run() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let (mut selector, mut receiver, factory) = rt.block_on(create_segment_selector(MockType::Happy));

        // initialize segment selector
        rt.block_on(selector.initialize());
        assert_eq!(selector.writers.len(), 2);

        // write data once and reactor should ack
        rt.block_on(write_once_for_selector(&mut selector, 512));
        let result = rt.block_on(Reactor::run_once(&mut selector, &mut receiver, &factory));
        assert!(result.is_ok());
    }

    #[test]
    fn test_reactor_wrong_host() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let (mut selector, mut receiver, factory) = rt.block_on(create_segment_selector(MockType::WrongHost));

        // initialize segment selector
        rt.block_on(selector.initialize());
        assert_eq!(selector.writers.len(), 2);

        // write data once, should get wrong host reply and writer should retry
        rt.block_on(write_once_for_selector(&mut selector, 512));
        let result = rt.block_on(Reactor::run_once(&mut selector, &mut receiver, &factory));
        assert!(result.is_ok());
    }

    #[test]
    fn test_reactor_stream_is_sealed() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let (mut selector, mut receiver, factory) =
            rt.block_on(create_segment_selector(MockType::SegmentIsSealed));

        // initialize segment selector
        rt.block_on(selector.initialize());
        assert_eq!(selector.writers.len(), 2);

        // write data once, should get segment sealed and reactor will fetch successors to continue
        rt.block_on(write_once_for_selector(&mut selector, 512));
        let result = rt.block_on(Reactor::run_once(&mut selector, &mut receiver, &factory));
        assert!(result.is_err());
    }

    // helper function section
    async fn write_once_for_selector(
        selector: &mut SegmentSelector,
        size: usize,
    ) -> oneshot::Receiver<Result<(), SegmentWriterError>> {
        let (oneshot_sender, oneshot_receiver) = tokio::sync::oneshot::channel();
        let event = PendingEvent::new(Some("routing_key".into()), vec![1; size], oneshot_sender)
            .expect("create pending event");
        let writer = selector.get_segment_writer(&event.routing_key);
        writer.write(event).await.expect("write data");
        oneshot_receiver
    }

    async fn write_once(
        writer: &mut SegmentWriter,
        size: usize,
    ) -> oneshot::Receiver<Result<(), SegmentWriterError>> {
        let (oneshot_sender, oneshot_receiver) = tokio::sync::oneshot::channel();
        let event = PendingEvent::new(Some("routing_key".into()), vec![1; size], oneshot_sender)
            .expect("create pending event");
        writer.write(event).await.expect("write data");
        oneshot_receiver
    }

    fn create_event(size: usize) -> (PendingEvent, EventHandle) {
        let (oneshot_sender, oneshot_receiver) = tokio::sync::oneshot::channel();
        let event = PendingEvent::new(Some("routing_key".into()), vec![1; size], oneshot_sender)
            .expect("create pending event");
        (event, oneshot_receiver)
    }
}
