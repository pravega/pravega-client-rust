//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactory;
use crate::error::*;
use crate::get_random_u128;
use crate::reactor::event::{Incoming, PendingEvent};
use crate::reactor::reactors::SegmentReactor;
use crate::segment_reader::{AsyncSegmentReader, AsyncSegmentReaderImpl};
use pravega_rust_client_config::ClientConfig;
use pravega_rust_client_shared::{ScopedSegment, WriterId};
use std::cmp;
use std::io::Error;
use std::io::{ErrorKind, Read, Write};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::info_span;
use tracing_futures::Instrument;
use uuid::Uuid;

const BUFFER_SIZE: usize = 4096;
const CHANNEL_CAPACITY: usize = 100;

pub struct ByteStreamWriter {
    writer_id: WriterId,
    sender: Sender<Incoming>,
    runtime_handle: Handle,
    oneshot_receiver: Option<oneshot::Receiver<Result<(), SegmentWriterError>>>,
}

impl Write for ByteStreamWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let oneshot_receiver = self.runtime_handle.block_on(async {
            let mut position = 0;
            let mut oneshot_receiver = loop {
                let advance = std::cmp::min(buf.len() - position, PendingEvent::MAX_WRITE_SIZE);
                let payload = buf[position..position + advance].to_vec();
                let oneshot_receiver = ByteStreamWriter::write_internal(self.sender.clone(), payload).await;
                position += advance;
                if position == buf.len() {
                    break oneshot_receiver;
                }
            };
            match oneshot_receiver.try_recv() {
                // The channel is currently empty
                Err(TryRecvError::Empty) => Ok(Some(oneshot_receiver)),
                Err(e) => Err(Error::new(ErrorKind::Other, format!("oneshot error {:?}", e))),
                Ok(res) => {
                    if let Err(e) = res {
                        Err(Error::new(ErrorKind::Other, format!("{:?}", e)))
                    } else {
                        Ok(None)
                    }
                }
            }
        })?;

        self.oneshot_receiver = oneshot_receiver;
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), Error> {
        if self.oneshot_receiver.is_none() {
            return Ok(());
        }
        let oneshot_receiver = self.oneshot_receiver.take().expect("get oneshot receiver");

        let result = self
            .runtime_handle
            .block_on(oneshot_receiver)
            .map_err(|e| Error::new(ErrorKind::Other, format!("oneshot error {:?}", e)))?;

        if let Err(e) = result {
            Err(Error::new(ErrorKind::Other, format!("{:?}", e)))
        } else {
            Ok(())
        }
    }
}

impl ByteStreamWriter {
    pub(crate) fn new(segment: ScopedSegment, config: ClientConfig, factory: ClientFactory) -> Self {
        let (sender, receiver) = channel(CHANNEL_CAPACITY);
        let handle = factory.get_runtime_handle();
        let writer_id = WriterId::from(get_random_u128());
        let span = info_span!("StreamReactor", event_stream_writer = %writer_id);
        // tokio::spawn is tied to the factory runtime.
        handle.enter(|| {
            tokio::spawn(
                SegmentReactor::run(segment, sender.clone(), receiver, factory.clone(), config)
                    .instrument(span),
            )
        });
        ByteStreamWriter {
            writer_id,
            sender,
            runtime_handle: handle,
            oneshot_receiver: None,
        }
    }

    async fn write_internal(
        mut sender: Sender<Incoming>,
        event: Vec<u8>,
    ) -> oneshot::Receiver<Result<(), SegmentWriterError>> {
        let (tx, rx) = oneshot::channel();
        if let Some(pending_event) = PendingEvent::without_header(None, event, tx) {
            let append_event = Incoming::AppendEvent(pending_event);
            if let Err(_e) = sender.send(append_event).await {
                let (tx_error, rx_error) = oneshot::channel();
                tx_error
                    .send(Err(SegmentWriterError::SendToProcessor {}))
                    .expect("send error");
                return rx_error;
            }
        }
        rx
    }
}

pub struct ByteStreamReader {
    reader_id: Uuid,
    reader: AsyncSegmentReaderImpl,
    offset: i64,
    runtime_handle: Handle,
}

impl Read for ByteStreamReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        let result = self
            .runtime_handle
            .block_on(self.reader.read(self.offset, buf.len() as i32));
        match result {
            Ok(cmd) => {
                if cmd.end_of_segment {
                    Err(Error::new(ErrorKind::Other, "segment is sealed"))
                } else {
                    // Read may have returned more or less than the requested number of bytes.
                    let size_to_return = cmp::min(buf.len(), cmd.data.len());
                    self.offset += size_to_return as i64;
                    buf[..size_to_return].copy_from_slice(&cmd.data[..size_to_return]);
                    Ok(size_to_return)
                }
            }
            Err(e) => Err(Error::new(ErrorKind::Other, format!("Error: {:?}", e))),
        }
    }
}

impl ByteStreamReader {
    pub(crate) fn new(segment: ScopedSegment, factory: &ClientFactory) -> Self {
        let handle = factory.get_runtime_handle();
        let async_reader = handle.block_on(factory.create_async_event_reader(segment));
        ByteStreamReader {
            reader_id: Uuid::new_v4(),
            reader: async_reader,
            offset: 0,
            runtime_handle: handle,
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {}
}
