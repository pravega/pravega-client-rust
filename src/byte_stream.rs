//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_rust_client_shared::ScopedSegment;
use uuid::Uuid;

use crate::client_factory::ClientFactoryInternal;
use crate::error::*;
use crate::reactor::event::{AppendEvent, Incoming};
use crate::reactor::SegmentReactor;
use pravega_wire_protocol::client_config::ClientConfig;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

struct ByteStreamWriter {
    writer_id: Uuid,
    sender: Sender<Incoming>,
}

impl ByteStreamWriter {
    const CHANNEL_CAPACITY: usize = 100;

    pub(crate) fn new(
        segment: ScopedSegment,
        config: ClientConfig,
        factory: Arc<ClientFactoryInternal>,
    ) -> Self {
        let (sender, receiver) = channel(ByteStreamWriter::CHANNEL_CAPACITY);
        let handle = factory.get_runtime_handle();
        // tokio::spawn is tied to the factory runtime.
        handle.enter(|| {
            tokio::spawn(SegmentReactor::run(
                segment,
                sender.clone(),
                receiver,
                factory,
                config,
            ))
        });
        ByteStreamWriter {
            writer_id: Uuid::new_v4(),
            sender,
        }
    }

    pub async fn write(&mut self, event: Vec<u8>) -> oneshot::Receiver<Result<(), SegmentWriterError>> {
        let (tx, rx) = oneshot::channel();
        let append_event = Incoming::AppendEvent(AppendEvent::new(event, None, tx));
        self.writer_internal(append_event, rx).await
    }

    pub async fn write_by_routing_key(
        &mut self,
        routing_key: String,
        event: Vec<u8>,
    ) -> oneshot::Receiver<Result<(), SegmentWriterError>> {
        let (tx, rx) = oneshot::channel();
        let append_event = Incoming::AppendEvent(AppendEvent::new(event, Some(routing_key), tx));
        self.writer_internal(append_event, rx).await
    }

    async fn writer_internal(
        &mut self,
        append_event: Incoming,
        rx: oneshot::Receiver<Result<(), SegmentWriterError>>,
    ) -> oneshot::Receiver<Result<(), SegmentWriterError>> {
        if let Err(_e) = self.sender.send(append_event).await {
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

//#[derive(Debug, Snafu)]
//#[snafu(visibility = "pub(crate)")]
//pub enum ReadError {
//    //TODO ...
//}
//
//#[async_trait]
//trait ByteStreamReader: TryStream<Ok = Vec<u8>, Error = ReadError> {
//    async fn open(segment: ScopedSegment, factory: &ClientFactory) -> Self;
//}
