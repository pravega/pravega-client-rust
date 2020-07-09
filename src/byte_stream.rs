//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use async_trait::async_trait;
use pravega_rust_client_shared::{ScopedSegment, ScopedStream};
use snafu::Snafu;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::client_factory::{ClientFactory, ClientFactoryInternal};
use crate::error::SegmentWriter;
use crate::segment_writer::{Reactor, SegmentSelector};
use pravega_wire_protocol::client_config::ClientConfig;
use std::sync::Arc;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum WriteError {
    //TODO ...
}

struct ByteStreamWriter {
    writer_id: Uuid,
    sender: Sender<Incoming>,
}

#[async_trait]
impl ByteStreamWriter {
    const CHANNEL_CAPACITY: usize = 100;

    pub(crate) fn new(
        stream: ScopedStream,
        config: ClientConfig,
        factory: Arc<ClientFactoryInternal>,
    ) -> Self {
        let (tx, rx) = channel(ByteStreamWriter::CHANNEL_CAPACITY);
        let handle = factory.get_runtime_handle();

        let selector = SegmentSelector::new(stream, tx.clone(), config, factory.clone());
        let reactor = Reactor::new(rx, selector, factory);
        // tokio::spawn is tied to the factory runtime.
        handle.enter(|| tokio::spawn(Reactor::run(reactor)));
        ByteStreamWriter {
            writer_id: Uuid::new_v4(),
            sender: tx,
        }
    }

    pub async fn write(&mut self, event: Vec<u8>) -> oneshot::Receiver<Result<(), SegmentWriter>> {
        let (tx, rx) = oneshot::channel();
        let append_event = Incoming::AppendEvent(AppendEvent::new(false, event, None, tx));
        self.writer_event_internal().await
    }

    pub async fn write_by_routing_key(
        &mut self,
        routing_key: String,
        event: Vec<u8>,
    ) -> oneshot::Receiver<Result<(), SegmentWriter>> {
        let (tx, rx) = oneshot::channel();
        let append_event = Incoming::AppendEvent(AppendEvent::new(false, event, Some(routing_key), tx));
        self.writer_event_internal().await
    }

    async fn writer_internal(&mut self) -> oneshot::Receiver<Result<(), SegmentWriter>> {
        if let Err(_e) = self.sender.send(append_event).await {
            let (tx_error, rx_error) = oneshot::channel();
            tx_error
                .send(Err(SegmentWriter::SendToProcessor {}))
                .expect("send error");
            rx_error
        } else {
            rx
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum ReadError {
    //TODO ...
}

#[async_trait]
trait ByteStreamReader: TryStream<Ok = Vec<u8>, Error = ReadError> {
    async fn open(segment: ScopedSegment, factory: &ClientFactory) -> Self;
}
