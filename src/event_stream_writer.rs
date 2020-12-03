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
use tracing::info_span;
use tracing_futures::Instrument;

/// Writes events exactly once to a given stream with backpressure.
///
/// EventStreamWriter spawns a `Reactor` that runs in the background for processing incoming events.
/// The `write` method sends the event to the `Reactor` asynchronously and returns a `tokio::oneshot::Receiver`
/// that contains the result of the write to the caller.
///
/// # Examples
///
/// ```no_run
/// use pravega_rust_client_config::ClientConfigBuilder;
/// use pravega_client_rust::client_factory::ClientFactory;
/// use pravega_rust_client_shared::ScopedStream;
///
/// #[tokio::main]
/// async fn main() {
///     // assuming Pravega controller is running at endpoint `localhost:9090`
///     let config = ClientConfigBuilder::default()
///         .controller_uri("localhost:9090")
///         .build()
///         .expect("creating config");
///
///     let client_factory = ClientFactory::new(config);
///
///     // assuming scope:myscope and stream:mystream has been created before.
///     let stream = ScopedStream::from("myscope/mystream");
///
///     let mut event_stream_writer = client_factory.create_event_stream_writer(stream);
///
///     let payload = "hello world".to_string().into_bytes();
///     let result = event_stream_writer.write_event(payload).await;
///
///     assert!(result.await.is_ok())
/// }
/// ```
///
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
        let span = info_span!("Reactor", event_stream_writer = %writer_id);
        // spawn is tied to the factory runtime.
        handle.spawn(Reactor::run(stream, tx.clone(), rx, factory, None).instrument(span));
        EventStreamWriter {
            writer_id,
            sender: tx,
        }
    }

    /// Writes an event without routing key.
    ///
    /// A random routing key will be generated in this case.
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

    /// Writes an event with a routing key.
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
