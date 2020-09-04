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
use crate::get_random_u128;
use crate::reactor::event::{Incoming, PendingEvent};
use crate::reactor::reactors::SegmentReactor;
use crate::segment_reader::AsyncSegmentReader;
use pravega_rust_client_shared::{ScopedSegment, WriterId};
use pravega_wire_protocol::client_config::ClientConfig;
use pravega_wire_protocol::commands::SegmentReadCommand;
use std::cmp;
use std::io::Error;
use std::io::{ErrorKind, Read, Write};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::info_span;
use tracing_futures::Instrument;
use uuid::Uuid;

cfg_if::cfg_if! {
    if #[cfg(test)] {
        use crate::segment_reader::MockAsyncSegmentReaderImpl as AsyncSegmentReaderImpl;
    } else {
        use crate::segment_reader::AsyncSegmentReaderImpl;
    }
}

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
    pub(crate) fn new(
        segment: ScopedSegment,
        config: ClientConfig,
        factory: Arc<ClientFactoryInternal>,
    ) -> Self {
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
    reader: Box<dyn AsyncSegmentReader>,
    offset: i64,
    runtime_handle: Handle,
    buffer: ReaderBuffer,
}

impl Read for ByteStreamReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        // read from buffer first
        let buffer_read = self.buffer.read(buf.len());
        let buffer_read_length = buffer_read.len();
        buf[..buffer_read_length].copy_from_slice(buffer_read);
        if buf.len() == buffer_read_length {
            return Ok(buffer_read_length);
        }

        // buffer has been read, clear the buffer
        self.buffer.clear();
        let server_read_length = buf.len() - buffer_read_length;
        let cmd = self.read_internal(server_read_length)?;
        if cmd.data.len() == server_read_length {
            buf[buffer_read_length..].copy_from_slice(&cmd.data);
            Ok(buf.len())
        } else if cmd.data.len() > server_read_length {
            buf[buffer_read_length..buffer_read_length + server_read_length].copy_from_slice(&cmd.data);
            Ok(buffer_read_length + server_read_length)
        } else {
            buf[buffer_read_length..].copy_from_slice(&cmd.data[..server_read_length]);
            self.buffer.fill(&cmd.data[server_read_length..]);
            Ok(buf.len())
        }
    }
}

impl ByteStreamReader {
    pub(crate) fn new(segment: ScopedSegment, factory: &ClientFactory) -> Self {
        let handle = factory.get_runtime_handle();
        let async_reader = handle.block_on(factory.create_async_event_reader(segment));
        ByteStreamReader {
            reader_id: Uuid::new_v4(),
            reader: Box::new(async_reader),
            offset: 0,
            runtime_handle: handle,
            buffer: ReaderBuffer::new(),
        }
    }

    fn read_internal(&mut self, length: usize) -> Result<SegmentReadCommand, Error> {
        let result = self
            .runtime_handle
            .block_on(self.reader.read(self.offset, length as i32));
        match result {
            Ok(cmd) => {
                if cmd.end_of_segment {
                    Err(Error::new(ErrorKind::Other, "segment is sealed"))
                } else {
                    Ok(cmd)
                }
            }
            Err(e) => Err(Error::new(ErrorKind::Other, format!("Error: {:?}", e))),
        }
    }
}

// This is the TYPE_PLUS_LENGTH_SIZE on segmentstore side, which is the minimal length that
// segmentstore will try to read, but not necessarily the minimal length that returns to client.
const READER_BUFFER_SIZE: usize = 8192;

struct ReaderBuffer {
    buf: Vec<u8>,
    // the length of the actual data in the buffer
    length: usize,
    // start to read from offset
    offset: usize,
}

impl ReaderBuffer {
    fn new() -> Self {
        // since we know the exact buffer size, initializing Vec with capacity can prevent
        // reallocating memory as Vec size grows.
        let buf = Vec::with_capacity(READER_BUFFER_SIZE);
        ReaderBuffer {
            buf,
            length: 0,
            offset: 0,
        }
    }

    fn read(&mut self, read: usize) -> &[u8] {
        let size = cmp::min(read, self.buf.len() - self.offset);
        let data = &self.buf[self.offset..self.offset + size];
        self.offset += size;
        data
    }

    fn fill(&mut self, data: &[u8]) {
        // should not fill buffer when there are data left unread.
        assert!(self.is_empty());
        // data should not be larger than the buffer capacity.
        assert!(data.len() <= READER_BUFFER_SIZE);

        self.offset = 0;
        self.buf.extend_from_slice(data);
    }

    fn to_read(&self) -> usize {
        self.length - self.offset
    }

    fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn is_full(&self) -> bool {
        self.buf.len() == self.offset
    }

    fn clear(&mut self) {
        self.buf.clear();
        self.offset = 0;
    }
}
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_reader_buffer_happy_read() {
        let mut buffer = ReaderBuffer::new();
        // read small size
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        buffer.fill(&data);
        assert!(!buffer.is_empty());
        assert_eq!(buffer.read(4), &data[..4]);
        assert_eq!(buffer.read(4), &data[4..8]);
        assert!(buffer.is_full());
        buffer.clear();
        assert!(buffer.is_empty());
    }

    #[test]
    #[should_panic]
    fn test_reader_buffer_read_large_event() {
        let mut buffer = ReaderBuffer::new();
        let data = vec![0; 10000];
        buffer.fill(&data);
    }

    #[test]
    fn test_read_buffer_when_full() {
        let mut buffer = ReaderBuffer::new();
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        buffer.fill(&data);
        assert_eq!(buffer.read(8), &data[..8]);
        let read = buffer.read(8);
        assert_eq!(read.len(), 0);
        assert!(buffer.is_full());
    }

    #[test]
    fn test_byte_stream_reader_normal_size_read() {}
}
