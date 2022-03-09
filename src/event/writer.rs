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
use crate::error::Error;
use crate::segment::event::{Incoming, PendingEvent, RoutingInfo};
use crate::segment::reactor::Reactor;
use crate::util::get_random_u128;

use pravega_client_channel::{create_channel, ChannelSender};
use pravega_client_shared::{ScopedStream, WriterId};

use std::collections::VecDeque;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::info_span;
use tracing_futures::Instrument;

/// Write events exactly once to a given stream.
///
/// EventWriter spawns a `Reactor` that runs in the background for processing incoming events.
/// The `write` method sends the event to the `Reactor` asynchronously and returns a `tokio::oneshot::Receiver`
/// which contains the result of the write to the caller. The maximum size of the serialized event
/// supported is 8MB, writing size larger than that will return an error.
///
/// ## Backpressure
/// Write has a backpressure mechanism. Internally, it uses [`Channel`] to send event to
/// Reactor for processing. [`Channel`] has a limited [`capacity`], when its capacity
/// is reached, any further write will not be accepted until enough space has been freed in the [`Channel`].
///
/// [`channel`]: pravega_client_channel
/// [`capacity`]: EventWriter::CHANNEL_CAPACITY
///
/// ## Retry
///
/// The EventWriter implementation provides [`retry`] logic to handle connection failures and service host
/// failures. Internal retries will not violate the exactly once semantic so it is better to rely on them
/// than to wrap this with custom retry logic.
///
/// [`retry`]: pravega_client_retry
///
/// # Examples
///
/// ```no_run
/// use pravega_client_config::ClientConfigBuilder;
/// use pravega_client::client_factory::ClientFactory;
/// use pravega_client_shared::ScopedStream;
///
/// #[tokio::main]
/// async fn main() {
///     // assuming Pravega controller is listening at endpoint `localhost:9090`
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
///     let mut event_writer = client_factory.create_event_writer(stream);
///
///     let payload = "hello world".to_string().into_bytes();
///     let result = event_writer.write_event(payload).await;
///
///     assert!(result.await.is_ok())
/// }
/// ```
pub struct EventWriter {
    writer_id: WriterId,
    sender: ChannelSender<Incoming>,
    event_handles: VecDeque<oneshot::Receiver<Result<(), Error>>>,
}

impl EventWriter {
    pub(crate) const MAX_EVENT_SIZE: usize = 8 * 1024 * 1024;
    // maximum 16 MB total size of events could be held in memory
    const CHANNEL_CAPACITY: usize = 16 * 1024 * 1024;

    pub(crate) fn new(stream: ScopedStream, factory: ClientFactoryAsync) -> Self {
        let (tx, rx) = create_channel(Self::CHANNEL_CAPACITY);
        let writer_id = WriterId::from(get_random_u128());
        let span = info_span!("Reactor", event_stream_writer = %writer_id);
        // spawn is tied to the factory runtime.
        factory
            .runtime_handle()
            .spawn(Reactor::run(stream, tx.clone(), rx, factory, None).instrument(span));
        EventWriter {
            writer_id,
            sender: tx,
            event_handles: VecDeque::new(),
        }
    }


    pub async fn write_large_event(&mut self, event: Vec<v8>) -> oneshot:Receiver<Result<(), Error>> {
        // create buffer
         let payloads = createBufs(events);
    }

    pub async fn createBufs(event: Vec<v8>) -> Vec<v8> {
        let toWrite = Vec::new();
        for (int i = 0; i < events.size(); i++) {
            ByteBuffer event = events.get(i);
            byte[] header = new byte[WireCommands.TYPE_PLUS_LENGTH_SIZE];
            ByteBuffer wrapped = ByteBuffer.wrap(header);
            wrapped.putInt(WireCommandType.EVENT.getCode());
            wrapped.putInt(event.remaining());
            wrapped.flip();
            toWrite[2 * i] = wrapped;
            toWrite[2 * i + 1] = event;
        }
        ByteBuf master = Unpooled.wrappedBuffer(toWrite);
        ArrayList<ByteBuf> result = new ArrayList<>();
        while (master.isReadable()) {
            int toRead = Math.min(master.readableBytes(), WRITE_SIZE);
            result.add(master.readSlice(toRead));
        }
        return result;
    }

    /// Write an event without routing key.
    ///
    /// A random routing key will be generated in this case.
    ///
    /// This method sends the payload to a background task called reactor to process,
    /// so the success of this method only means the payload has been sent to the reactor.
    /// Applications may want to call await on the returned tokio oneshot to check whether
    /// the payload is successfully persisted or not.
    /// If oneshot returns an error indicating something is going wrong on the server side, then
    /// subsequent calls are also likely to fail.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut event_writer = client_factory.create_event_writer(stream);
    /// // result is a tokio oneshot
    /// let result = event_writer.write_event(payload).await;
    /// result.await.expect("flush to server");
    /// ```
    pub async fn write_event(&mut self, event: Vec<u8>) -> oneshot::Receiver<Result<(), Error>> {
        let size = event.len();
        let (tx, rx) = oneshot::channel();
        let (tx_flush, rx_flush) = oneshot::channel();
        let routing_info = RoutingInfo::RoutingKey(None);
        if (size > MAX_EVENT_SIZE) {
            // create transient segment
            // buffer writes in transient segment
            // batch remaining writes to main segments

            write_large_event(event)

            rx
        } else {
            if let Some(pending_event) =
            PendingEvent::with_header_flush(routing_info, event, None, tx, Some(tx_flush))
            {
                let append_event = Incoming::AppendEvent(pending_event);
                self.writer_event_internal(append_event, size, rx, rx_flush).await
            } else {
                rx
            }
        }
    }

    /// Writes an event with a routing key.
    ///
    /// Same as the write_event.
    pub async fn write_event_by_routing_key(
        &mut self,
        routing_key: String,
        event: Vec<u8>,
    ) -> oneshot::Receiver<Result<(), Error>> {
        let size = event.len();
        let (tx, rx) = oneshot::channel();
        let (tx_flush, rx_flush) = oneshot::channel();
        let routing_info = RoutingInfo::RoutingKey(Some(routing_key));
        if let Some(pending_event) =
            PendingEvent::with_header_flush(routing_info, event, None, tx, Some(tx_flush))
        {
            let append_event = Incoming::AppendEvent(pending_event);
            self.writer_event_internal(append_event, size, rx, rx_flush).await
        } else {
            rx
        }
    }

    async fn writer_event_internal(
        &mut self,
        append_event: Incoming,
        size: usize,
        rx: oneshot::Receiver<Result<(), Error>>,
        rx_flush: oneshot::Receiver<Result<(), Error>>,
    ) -> oneshot::Receiver<Result<(), Error>> {
        if let Err(err) = self.clear_initial_complete_events() {
            // fail fast upon checking previous write events
            let (tx_error, rx_error) = oneshot::channel();
            tx_error.send(Err(err)).expect("send error");
            rx_error
        } else if let Err(_e) = self.sender.send((append_event, size)).await {
            let (tx_error, rx_error) = oneshot::channel();
            tx_error
                .send(Err(Error::InternalFailure {
                    msg: "failed to send request to reactor".to_string(),
                }))
                .expect("send error");
            rx_error
        } else {
            self.event_handles.push_back(rx_flush);
            rx
        }
    }

    /// Flush data.
    ///
    /// It will wait until all pending appends have acknowledgment.
    pub async fn flush(&mut self) -> Result<(), Error> {
        while let Some(receiver) = self.event_handles.pop_front() {
            let recv = receiver.await.map_err(|e| Error::InternalFailure {
                msg: format!("oneshot error {:?}", e),
            })?;

            recv?;
        }
        Ok(())
    }

    /// Clear initial completed events from flush queue.
    fn clear_initial_complete_events(&mut self) -> Result<(), Error> {
        while let Some(mut receiver) = self.event_handles.pop_front() {
            let try_recv = receiver.try_recv();

            match try_recv {
                Err(TryRecvError::Empty) => {
                    self.event_handles.push_front(receiver);
                    break;
                }
                Err(TryRecvError::Closed) => {
                    let res = try_recv.map_err(|e| Error::InternalFailure {
                        msg: format!("Trying to flush a closed channel {:?}", e),
                    })?;

                    return res;
                }
                Ok(_) => {}
            }
        }

        Ok(())
    }
}

impl Drop for EventWriter {
    fn drop(&mut self) {
        let _res = self.sender.send_without_bp(Incoming::Close());
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::Runtime;

    use super::*;
    use crate::segment::event::{PendingEvent, RoutingInfo};

    #[test]
    fn test_pending_event() {
        // test with legal event size
        let (tx, _rx) = oneshot::channel();
        let data = vec![];
        let routing_info = RoutingInfo::RoutingKey(None);

        let event = PendingEvent::without_header(routing_info, data, None, tx).expect("create pending event");
        assert!(event.is_empty());

        // test with illegal event size
        let (tx, rx) = oneshot::channel();
        let data = vec![0; (PendingEvent::MAX_WRITE_SIZE + 1) as usize];
        let routing_info = RoutingInfo::RoutingKey(None);

        let event = PendingEvent::without_header(routing_info, data, None, tx);
        assert!(event.is_none());

        let rt = Runtime::new().expect("get runtime");
        let reply = rt.block_on(rx).expect("get reply");
        assert!(reply.is_err());
    }
}
