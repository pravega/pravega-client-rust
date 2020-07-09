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

    async fn open(segment: ScopedSegment, factory: &ClientFactory) -> Self {}
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
