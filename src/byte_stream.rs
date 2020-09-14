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
use crate::raw_client::RawClient;
use crate::reactor::event::{Incoming, PendingEvent};
use crate::reactor::reactors::SegmentReactor;
use crate::segment_reader::AsyncSegmentReader;
use crate::{get_random_u128, get_request_id};
use pravega_rust_client_shared::{ScopedSegment, ScopedStream, WriterId};
use pravega_wire_protocol::commands::{SealSegmentCommand, SegmentReadCommand, TruncateSegmentCommand};
use pravega_wire_protocol::wire_commands::Replies;
use pravega_wire_protocol::wire_commands::Requests;
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
    segment: ScopedSegment,
    writer_id: WriterId,
    sender: Sender<Incoming>,
    oneshot_receiver: Option<oneshot::Receiver<Result<(), SegmentWriterError>>>,
    client_factory: ClientFactory,
    runtime_handle: Handle,
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
                Err(e) => Err(Error::new(ErrorKind::Other, format!("oneshot error: {:?}", e))),
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
            .map_err(|e| Error::new(ErrorKind::Other, format!("oneshot error: {:?}", e)))?;

        if let Err(e) = result {
            Err(Error::new(ErrorKind::Other, format!("{:?}", e)))
        } else {
            Ok(())
        }
    }
}

impl ByteStreamWriter {
    pub(crate) fn new(segment: ScopedSegment, factory: ClientFactory) -> Self {
        let (sender, receiver) = channel(CHANNEL_CAPACITY);
        let handle = factory.get_runtime_handle();
        let writer_id = WriterId::from(get_random_u128());
        let span = info_span!("StreamReactor", event_stream_writer = %writer_id);
        // tokio::spawn is tied to the factory runtime.
        handle.enter(|| {
            tokio::spawn(
                SegmentReactor::run(segment.clone(), sender.clone(), receiver, factory.clone())
                    .instrument(span),
            )
        });
        ByteStreamWriter {
            segment,
            writer_id,
            sender,
            runtime_handle: handle,
            oneshot_receiver: None,
            client_factory: factory,
        }
    }

    /// Seal will seal the segment and no further writes are allowed.
    pub async fn seal(&self) -> Result<(), Error> {
        let controller = self.client_factory.get_controller_client();
        let endpoint = controller
            .get_endpoint_for_segment(&self.segment)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!("fetch endpoint for segment error: {:?}", e),
                )
            })?;
        let raw_client = self.client_factory.get_raw_client(endpoint);
        let delegation_token_provider = self
            .client_factory
            .create_delegation_token_provider(ScopedStream::from(&self.segment))
            .await;
        let reply = raw_client
            .send_request(&Requests::SealSegment(SealSegmentCommand {
                request_id: get_request_id(),
                segment: self.segment.to_string(),
                delegation_token: delegation_token_provider
                    .retrieve_token(self.client_factory.get_controller_client())
                    .await,
            }))
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!(
                        "send SealSegmentCommand to segment {:?} error: {:?}",
                        self.segment, e
                    ),
                )
            })?;
        match reply {
            Replies::SegmentSealed(_) => Ok(()),
            _ => Err(Error::new(
                ErrorKind::Other,
                format!("reply is not SegmentSealed {:?}", reply),
            )),
        }
    }

    /// Truncate data before a given offset for the segment. No reads are allowed before
    /// truncation point after calling this method.
    pub async fn truncate_data_before(&self, offset: i64) -> Result<(), Error> {
        let controller = self.client_factory.get_controller_client();
        let endpoint = controller
            .get_endpoint_for_segment(&self.segment)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!("fetch endpoint for segment error: {:?}", e),
                )
            })?;
        let raw_client = self.client_factory.get_raw_client(endpoint);
        let delegation_token_provider = self
            .client_factory
            .create_delegation_token_provider(ScopedStream::from(&self.segment))
            .await;
        let reply = raw_client
            .send_request(&Requests::TruncateSegment(TruncateSegmentCommand {
                request_id: get_request_id(),
                segment: self.segment.to_string(),
                truncation_offset: offset,
                delegation_token: delegation_token_provider
                    .retrieve_token(self.client_factory.get_controller_client())
                    .await,
            }))
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!(
                        "send TruncateSegmentCommand to segment {:?} error: {:?}",
                        self.segment, e
                    ),
                )
            })?;
        match reply {
            Replies::SegmentTruncated(_) => Ok(()),
            _ => Err(Error::new(
                ErrorKind::Other,
                format!("reply is not SegmentTruncated {:?}", reply),
            )),
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
        // if returned data size is smaller that asked, return it all to caller.
        if cmd.data.len() < server_read_length {
            buf[buffer_read_length..buffer_read_length + cmd.data.len()].copy_from_slice(&cmd.data);
            Ok(buffer_read_length + server_read_length)
        } else {
            // if returned data size is larger than asked, put the rest into buffer and return
            // the size that caller wants.
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
        ByteStreamReader::new_internal(handle, Box::new(async_reader))
    }

    fn new_internal(handle: Handle, async_reader: Box<dyn AsyncSegmentReader>) -> Self {
        ByteStreamReader {
            reader_id: Uuid::new_v4(),
            reader: async_reader,
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

// This is the TYPE_PLUS_LENGTH_SIZE in segmentstore, which is the minimal length that
// segmentstore will try to read, but not necessarily the minimal length that returns to the client.
const READER_BUFFER_SIZE: usize = 8192;

#[derive(Debug)]
struct ReaderBuffer {
    buf: Vec<u8>,
    // start to read from offset
    offset: usize,
}

impl ReaderBuffer {
    fn new() -> Self {
        // since we know the exact buffer size, initializing Vec with capacity can prevent
        // reallocating memory as Vec size grows.
        let buf = Vec::with_capacity(READER_BUFFER_SIZE);
        ReaderBuffer { buf, offset: 0 }
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

        self.buf.extend_from_slice(data);
    }

    fn len(&self) -> usize {
        self.buf.len() - self.offset
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
    use crate::segment_reader::ReaderError;
    use async_trait::async_trait;
    use tokio::sync::Mutex;

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

    struct MockAsyncSegmentReaderImpl {
        size: Mutex<i32>,
    }

    impl MockAsyncSegmentReaderImpl {
        fn new(size: i32) -> Self {
            MockAsyncSegmentReaderImpl {
                size: Mutex::new(size),
            }
        }
    }

    #[async_trait]
    impl AsyncSegmentReader for MockAsyncSegmentReaderImpl {
        async fn read(&self, offset: i64, length: i32) -> Result<SegmentReadCommand, ReaderError> {
            let mut guard = self.size.lock().await;
            let ask = cmp::max(length, READER_BUFFER_SIZE as i32);
            let return_size = if ask > *guard {
                let size = *guard;
                *guard = 0;
                size
            } else {
                *guard -= ask;
                ask
            };

            Ok(SegmentReadCommand {
                segment: "".to_string(),
                offset: offset + return_size as i64,
                at_tail: false,
                end_of_segment: false,
                data: vec![1; return_size as usize],
                request_id: 0,
            })
        }
    }

    #[test]
    fn test_byte_stream_reader_normal_size_read() {
        let mock = MockAsyncSegmentReaderImpl::new(1024 * 100);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let mut byte_stream_reader = ByteStreamReader::new_internal(handle, Box::new(mock));

        let mut buf = vec![0; 1024];
        byte_stream_reader.read(&mut buf).unwrap();
        assert_eq!(byte_stream_reader.buffer.len(), READER_BUFFER_SIZE - 1024);
        assert_eq!(buf, vec![1; 1024]);
    }

    #[test]
    fn test_byte_stream_reader_large_size_read() {
        let mock = MockAsyncSegmentReaderImpl::new(1024 * 100);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let mut byte_stream_reader = ByteStreamReader::new_internal(handle, Box::new(mock));

        let mut buf = vec![0; 8192];
        byte_stream_reader.read(&mut buf).unwrap();
        assert!(byte_stream_reader.buffer.is_empty());
        assert_eq!(buf, vec![1; 8192]);
    }

    #[test]
    fn test_byte_stream_reader_continuous_small_size_read() {
        let mock = MockAsyncSegmentReaderImpl::new(1024 * 100);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let mut byte_stream_reader = ByteStreamReader::new_internal(handle, Box::new(mock));

        let mut to_read = 8192;
        for _i in 0..10 {
            let mut buf = vec![0; 512];
            byte_stream_reader.read(&mut buf).unwrap();
            to_read -= 512;
            assert_eq!(byte_stream_reader.buffer.len(), to_read);
            assert_eq!(buf, vec![1; 512]);
        }
    }

    #[test]
    fn test_byte_stream_reader_mixed_size_read() {
        let mock = MockAsyncSegmentReaderImpl::new(1024 * 100);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let mut byte_stream_reader = ByteStreamReader::new_internal(handle, Box::new(mock));

        let mut buf = vec![0; 2048];
        byte_stream_reader.read(&mut buf).unwrap();
        assert_eq!(byte_stream_reader.buffer.len(), 8192 - 2048);
        assert_eq!(buf, vec![1; 2048]);

        let mut buf = vec![0; 8192];
        byte_stream_reader.read(&mut buf).unwrap();
        assert_eq!(byte_stream_reader.buffer.len(), 8192 - 2048);
        assert_eq!(buf, vec![1; 8192]);
    }
}
