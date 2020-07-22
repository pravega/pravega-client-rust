//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::{ClientFactory, ClientFactoryInternal};
use crate::error::*;
use crate::reactor::event::{Incoming, PendingEvent};
use crate::reactor::SegmentReactor;
use crate::segment_reader::{AsyncSegmentReader, AsyncSegmentReaderImpl};
use pravega_rust_client_shared::ScopedSegment;
use pravega_wire_protocol::client_config::ClientConfig;
use std::io::Error;
use std::io::{ErrorKind, Read, Write};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use uuid::Uuid;

const BUFFER_SIZE: usize = 4096;
const CHANNEL_CAPACITY: usize = 100;

pub struct ByteStreamWriter {
    writer_id: Uuid,
    sender: Sender<Incoming>,
    runtime_handle: Handle,
}

impl Write for ByteStreamWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let mut position = 0;
        while position < buf.len() {
            let advance = if buf.len() - position > PendingEvent::MAX_WRITE_SIZE {
                PendingEvent::MAX_WRITE_SIZE
            } else {
                buf.len() - position
            };
            let mut payload = vec![];
            payload.extend_from_slice(&buf[position..position + advance]);
            let result = self.runtime_handle.block_on(async {
                let oneshot = ByteStreamWriter::write_internal(self.sender.clone(), payload).await;
                oneshot.await
            });
            match result {
                Ok(res) => match res {
                    Ok(()) => {
                        position += advance;
                    }
                    Err(e) => return Err(Error::new(ErrorKind::Other, format!("{:?}", e))),
                },
                Err(e) => return Err(Error::new(ErrorKind::Other, format!("Oneshot error: {:?}", e))),
            }
        }
        Ok(position)
    }

    // write will flush the data internally, so there is no need to call flush
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl ByteStreamWriter {
    pub(crate) fn new(
        segment: ScopedSegment,
        config: ClientConfig,
        factory: Arc<ClientFactoryInternal>,
    ) -> Self {
        let (sender, receiver) = channel(CHANNEL_CAPACITY);
        let handle = factory.get_runtime_handle();
        // tokio::spawn is tied to the factory runtime.
        handle.enter(|| {
            tokio::spawn(SegmentReactor::run(
                segment,
                sender.clone(),
                receiver,
                factory.clone(),
                config,
            ))
        });
        ByteStreamWriter {
            writer_id: Uuid::new_v4(),
            sender,
            runtime_handle: handle,
        }
    }

    async fn write_internal(
        mut sender: Sender<Incoming>,
        event: Vec<u8>,
    ) -> oneshot::Receiver<Result<(), SegmentWriterError>> {
        let (tx, rx) = oneshot::channel();
        if let Some(pending_event) = PendingEvent::new(None, event, tx) {
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

pub struct ByteStreamReader<'a> {
    reader_id: Uuid,
    reader: AsyncSegmentReaderImpl<'a>,
    offset: i64,
    runtime_handle: Handle,
}

impl Read for ByteStreamReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        let result = self
            .runtime_handle
            .block_on(self.reader.read(self.offset, buf.len() as i32));
        match result {
            Ok(cmd) => {
                if cmd.end_of_segment {
                    Err(Error::new(ErrorKind::Other, "segment is sealed"))
                } else {
                    self.offset += cmd.data.len() as i64;
                    buf.copy_from_slice(&cmd.data);
                    Ok(cmd.data.len())
                }
            }
            Err(e) => Err(Error::new(ErrorKind::Other, format!("Error: {:?}", e))),
        }
    }
}

impl<'a> ByteStreamReader<'a> {
    pub(crate) fn new(segment: ScopedSegment, factory: &'a ClientFactory) -> Self {
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
