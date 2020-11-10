//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::reactor::reactors::Reactor;
use pravega_rust_client_channel::{create_channel, ChannelSender};
use pravega_rust_client_shared::*;
use tokio::sync::oneshot;

use crate::client_factory::ClientFactory;
use crate::error::*;
use crate::get_random_u128;
use crate::reactor::event::{Incoming, PendingEvent};
use crate::reactor::segment_selector::SegmentSelector;
use tracing::info_span;
use tracing_futures::Instrument;

/// EventStreamWriter contains a writer id and a mpsc sender which is used to send Event
/// to the StreamWriter
pub struct EventStreamWriter {
    writer_id: WriterId,
    sender: ChannelSender<Incoming>,
}

impl EventStreamWriter {
    pub const MAX_EVENT_SIZE: usize = 8 * 1024 * 1024;
    // maximum 16 MB total size of events could be in memory
    const CHANNEL_CAPACITY: usize = 16 * 1024 * 1024;

    pub(crate) fn new(stream: ScopedStream, factory: ClientFactory) -> Self {
        let handle = factory.get_runtime_handle();
        let (tx, rx) = create_channel(Self::CHANNEL_CAPACITY);
        let writer_id = WriterId::from(get_random_u128());
        let mut selector = handle.block_on(SegmentSelector::new(stream.clone(), tx.clone(), factory.clone()));
        let current_segments = handle
            .block_on(factory.get_controller_client().get_current_segments(&stream))
            .expect("retry failed");
        // get the current segments and create corresponding event segment writers
        handle.block_on(selector.initialize(current_segments));

        let span = info_span!("StreamReactor", event_stream_writer = %writer_id);
        // tokio::spawn is tied to the factory runtime.
        handle.enter(|| {
            tokio::spawn(Reactor::run(stream, tx.clone(), rx, factory.clone(), None).instrument(span))
        });
        EventStreamWriter {
            writer_id,
            sender: tx,
        }
    }

    pub async fn write_event(&mut self, event: Vec<u8>) -> oneshot::Receiver<Result<(), SegmentWriterError>> {
        let size = event.len();
        let (tx, rx) = oneshot::channel();
        if let Some(pending_event) = PendingEvent::with_header(None, event, None, tx) {
            let append_event = Incoming::AppendEvent(pending_event);
            self.writer_event_internal(append_event, size, rx).await
        } else {
            rx
        }
    }

    pub async fn write_event_by_routing_key(
        &mut self,
        routing_key: String,
        event: Vec<u8>,
    ) -> oneshot::Receiver<Result<(), SegmentWriterError>> {
        let size = event.len();
        let (tx, rx) = oneshot::channel();
        if let Some(pending_event) = PendingEvent::with_header(Some(routing_key), event, None, tx) {
            let append_event = Incoming::AppendEvent(pending_event);
            self.writer_event_internal(append_event, size, rx).await
        } else {
            rx
        }
    }

    async fn writer_event_internal(
        &mut self,
        append_event: Incoming,
        size: usize,
        rx: oneshot::Receiver<Result<(), SegmentWriterError>>,
    ) -> oneshot::Receiver<Result<(), SegmentWriterError>> {
        if let Err(_e) = self.sender.send((append_event, size)).await {
            let (tx_error, rx_error) = oneshot::channel();
            tx_error
                .send(Err(SegmentWriterError::SendToProcessor {}))
                .expect("send error");
            rx_error
        } else {
            rx
        }
    }
}

impl Drop for EventStreamWriter {
    fn drop(&mut self) {
        let _res = self.sender.send((Incoming::Close(), 0));
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::Runtime;

    use super::*;
    use crate::reactor::event::PendingEvent;

    #[test]
    fn test_pending_event() {
        // test with legal event size
        let (tx, _rx) = oneshot::channel();
        let data = vec![];
        let routing_key = None;

        let event = PendingEvent::without_header(routing_key, data, None, tx).expect("create pending event");
        assert!(event.is_empty());

        // test with illegal event size
        let (tx, rx) = oneshot::channel();
        let data = vec![0; (PendingEvent::MAX_WRITE_SIZE + 1) as usize];
        let routing_key = None;

        let event = PendingEvent::without_header(routing_key, data, None, tx);
        assert!(event.is_none());

        let mut rt = Runtime::new().expect("get runtime");
        let reply = rt.block_on(rx).expect("get reply");
        assert!(reply.is_err());
    }
}
