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
use crate::event_stream_writer::EventStreamWriter;
use crate::get_random_u128;
use crate::reactor::event::{Incoming, PendingEvent};
use crate::reactor::reactors::Reactor;
use crate::segment_metadata::SegmentMetadataClient;
use crate::segment_reader::{AsyncSegmentReader, AsyncSegmentReaderImpl};
use pravega_rust_client_channel::{create_channel, ChannelSender};
use pravega_rust_client_shared::{ScopedSegment, ScopedStream, WriterId};
use std::cmp;
use std::convert::TryInto;
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};
use tracing::debug;
use tracing::info_span;
use tracing_futures::Instrument;
use uuid::Uuid;

const CHANNEL_CAPACITY: usize = 16 * 1024 * 1024;

type EventHandle = oneshot::Receiver<Result<(), SegmentWriterError>>;

/// Allows for writing raw bytes directly to a segment.
///
/// This class does not frame, attach headers, or otherwise modify the bytes written to it in any
/// way. So unlike EventStreamWriter the data written cannot be split apart when read.
/// As such, any bytes written by this API can ONLY be read using ByteStreamReader.
/// Similarly, unless some sort of framing is added it is probably an error to have multiple
/// ByteStreamWriters write to the same segment as this will result in interleaved data.
pub struct ByteStreamWriter {
    writer_id: WriterId,
    sender: ChannelSender<Incoming>,
    metadata_client: SegmentMetadataClient,
    runtime_handle: Handle,
    event_handle: Option<EventHandle>,
    write_offset: i64,
}

/// ByteStreamWriter implements Write trait in standard library.
/// It can be wrapped by BufWriter to improve performance.
impl Write for ByteStreamWriter {
    /// Writes the given data to the server. It doesn't mean the data is persisted on the server side
    /// when this method returns Ok, user should call flush to ensure all data has been acknowledged
    /// by the server.
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let bytes_to_write = std::cmp::min(buf.len(), EventStreamWriter::MAX_EVENT_SIZE);
        let payload = buf[0..bytes_to_write].to_vec();
        let oneshot_receiver = self
            .runtime_handle
            .block_on(self.write_internal(self.sender.clone(), payload));

        debug!(
            "writing payload of size {} based on offset {}",
            bytes_to_write, self.write_offset
        );
        self.write_offset += bytes_to_write as i64;
        self.event_handle = Some(oneshot_receiver);
        Ok(bytes_to_write)
    }

    /// This is a blocking call that will wait for data to be persisted on the server side.
    fn flush(&mut self) -> Result<(), Error> {
        if let Some(event_handle) = self.event_handle.take() {
            self.runtime_handle.block_on(self.flush_internal(event_handle))
        } else {
            Ok(())
        }
    }
}

impl ByteStreamWriter {
    pub(crate) fn new(segment: ScopedSegment, factory: ClientFactory) -> Self {
        let handle = factory.get_runtime_handle();
        let (sender, receiver) = create_channel(CHANNEL_CAPACITY);
        let metadata_client = handle.block_on(factory.create_segment_metadata_client(segment.clone()));
        let writer_id = WriterId(get_random_u128());
        let stream = ScopedStream::from(&segment);
        let span = info_span!("Reactor", byte_stream_writer = %writer_id);
        // spawn is tied to the factory runtime.
        handle.spawn(Reactor::run(stream, sender.clone(), receiver, factory, None).instrument(span));
        ByteStreamWriter {
            writer_id,
            sender,
            metadata_client,
            runtime_handle: handle,
            event_handle: None,
            write_offset: 0,
        }
    }

    /// Seals the segment and no further writes are allowed.
    pub async fn seal(&mut self) -> Result<(), Error> {
        if let Some(event_handle) = self.event_handle.take() {
            self.flush_internal(event_handle).await?;
        }
        self.metadata_client
            .seal_segment()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("segment seal error: {:?}", e)))
    }

    /// Truncates data before a given offset for the segment. No reads are allowed before
    /// truncation point after calling this method.
    pub async fn truncate_data_before(&self, offset: i64) -> Result<(), Error> {
        self.metadata_client
            .truncate_segment(offset)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("segment truncation error: {:?}", e)))
    }

    /// Tracks the current write position for this writer.
    pub fn current_write_offset(&mut self) -> i64 {
        self.write_offset
    }

    /// Seek to the tail of the segment.
    pub fn seek_to_tail(&mut self) {
        let segment_info = self
            .runtime_handle
            .block_on(self.metadata_client.get_segment_info())
            .expect("failed to get segment info");
        self.write_offset = segment_info.write_offset;
    }

    async fn write_internal(
        &self,
        sender: ChannelSender<Incoming>,
        event: Vec<u8>,
    ) -> oneshot::Receiver<Result<(), SegmentWriterError>> {
        let size = event.len();
        let (tx, rx) = oneshot::channel();
        if let Some(pending_event) = PendingEvent::without_header(None, event, Some(self.write_offset), tx) {
            let append_event = Incoming::AppendEvent(pending_event);
            if let Err(_e) = sender.send((append_event, size)).await {
                let (tx_error, rx_error) = oneshot::channel();
                tx_error
                    .send(Err(SegmentWriterError::SendToProcessor {}))
                    .expect("send error");
                return rx_error;
            }
        }
        rx
    }

    async fn flush_internal(&self, event_handle: EventHandle) -> Result<(), Error> {
        let result = event_handle
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("oneshot error {:?}", e)))?;

        result.map_err(|e| Error::new(ErrorKind::Other, format!("{:?}", e)))
    }
}

/// Allows for reading raw bytes from a segment.
pub struct ByteStreamReader {
    reader_id: Uuid,
    reader: AsyncSegmentReaderImpl,
    metadata_client: SegmentMetadataClient,
    offset: i64,
    runtime_handle: Handle,
    timeout: Duration,
}

impl Read for ByteStreamReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        let read_future = self.reader.read(self.offset, buf.len() as i32);
        let timeout_fut = self.runtime_handle.enter(|| timeout(self.timeout, read_future));
        let result = self.runtime_handle.block_on(timeout_fut);
        match result {
            Ok(result) => match result {
                Ok(cmd) => {
                    // Read may have returned more or less than the requested number of bytes.
                    let size_to_return = cmp::min(buf.len(), cmd.data.len());
                    self.offset += size_to_return as i64;
                    buf[..size_to_return].copy_from_slice(&cmd.data[..size_to_return]);
                    Ok(size_to_return)
                }
                Err(e) => Err(Error::new(ErrorKind::Other, format!("Error: {:?}", e))),
            },
            Err(e) => Err(Error::new(
                ErrorKind::TimedOut,
                format!("Reader timed out after {:?}: {:?}", self.timeout, e),
            )),
        }
    }
}

impl ByteStreamReader {
    pub(crate) fn new(segment: ScopedSegment, factory: &ClientFactory) -> Self {
        let handle = factory.get_runtime_handle();
        let async_reader = handle.block_on(factory.create_async_event_reader(segment.clone()));
        let metadata_client = handle.block_on(factory.create_segment_metadata_client(segment));
        ByteStreamReader {
            reader_id: Uuid::new_v4(),
            reader: async_reader,
            metadata_client,
            offset: 0,
            runtime_handle: handle,
            timeout: Duration::from_secs(3600),
        }
    }

    pub fn current_head(&self) -> std::io::Result<u64> {
        self.runtime_handle
            .block_on(self.metadata_client.fetch_current_starting_head())
            .map(|i| i as u64)
            .map_err(|e| Error::new(ErrorKind::Other, format!("{:?}", e)))
    }

    pub fn current_offset(&self) -> i64 {
        self.offset
    }

    pub fn set_reader_timeout(&mut self, timeout: Option<Duration>) {
        if let Some(time) = timeout {
            self.timeout = time;
        } else {
            self.timeout = Duration::from_secs(3600);
        }
    }
}

/// The Seek implementation for ByteStreamReader allows seeking to a byte offset from the beginning
/// of the stream or a byte offset relative to the current position in the stream.
/// If the stream has been truncated, the byte offset will be relative to the original beginning of the stream.
impl Seek for ByteStreamReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(offset) => {
                self.offset = offset.try_into().map_err(|e| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        format!("Overflowed when converting offset to i64: {:?}", e),
                    )
                })?;
                Ok(self.offset as u64)
            }
            SeekFrom::Current(offset) => {
                let new_offset = self.offset + offset;
                if new_offset < 0 {
                    Err(Error::new(
                        ErrorKind::InvalidInput,
                        "Cannot seek to a negative offset",
                    ))
                } else {
                    self.offset = new_offset;
                    Ok(self.offset as u64)
                }
            }
            SeekFrom::End(offset) => {
                let tail = self
                    .runtime_handle
                    .block_on(self.metadata_client.fetch_current_segment_length())
                    .map_err(|e| Error::new(ErrorKind::Other, format!("{:?}", e)))?;
                if tail + offset < 0 {
                    Err(Error::new(
                        ErrorKind::InvalidInput,
                        "Cannot seek to a negative offset",
                    ))
                } else {
                    self.offset = tail + offset;
                    Ok(self.offset as u64)
                }
            }
        }
    }
}

impl Drop for ByteStreamWriter {
    fn drop(&mut self) {
        let _res = self.sender.send((Incoming::Close(), 0));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::create_stream;
    use pravega_rust_client_config::connection_type::{ConnectionType, MockType};
    use pravega_rust_client_config::ClientConfigBuilder;
    use pravega_rust_client_shared::PravegaNodeUri;
    use tokio::runtime::Runtime;

    #[test]
    fn test_byte_stream_seek() {
        let mut rt = Runtime::new().unwrap();
        let (mut writer, mut reader) = create_reader_and_writer(&mut rt);

        // write 200 bytes
        let payload = vec![1; 200];
        writer.write(&payload).expect("write");
        writer.flush().expect("flush");

        // read 200 bytes from beginning
        let mut buf = vec![0; 200];
        reader.read(&mut buf).expect("read");
        assert_eq!(buf, vec![1; 200]);

        // seek to head
        reader.seek(SeekFrom::Start(0)).expect("seek to head");
        assert_eq!(reader.current_offset(), 0);

        // seek to head with positive offset
        reader.seek(SeekFrom::Start(100)).expect("seek to head");
        assert_eq!(reader.current_offset(), 100);

        // seek to current with positive offset
        assert_eq!(reader.current_offset(), 100);
        reader.seek(SeekFrom::Current(100)).expect("seek to current");
        assert_eq!(reader.current_offset(), 200);

        // seek to current with negative offset
        reader.seek(SeekFrom::Current(-100)).expect("seek to current");
        assert_eq!(reader.current_offset(), 100);

        // seek to current invalid negative offset
        assert!(reader.seek(SeekFrom::Current(-200)).is_err());

        // seek to end
        reader.seek(SeekFrom::End(0)).expect("seek to end");
        assert_eq!(reader.current_offset(), 200);

        // seek to end with positive offset
        assert!(reader.seek(SeekFrom::End(1)).is_ok());

        // seek to end with negative offset
        reader.seek(SeekFrom::End(-100)).expect("seek to end");
        assert_eq!(reader.current_offset(), 100);

        // seek to end with invalid negative offset
        assert!(reader.seek(SeekFrom::End(-300)).is_err());
    }

    #[test]
    fn test_byte_stream_truncate() {
        let mut rt = Runtime::new().unwrap();
        let (mut writer, mut reader) = create_reader_and_writer(&mut rt);

        // write 200 bytes
        let payload = vec![1; 200];
        writer.write(&payload).expect("write");
        writer.flush().expect("flush");

        // truncate to offset 100
        rt.block_on(writer.truncate_data_before(100)).expect("truncate");

        // read truncated offset
        reader.seek(SeekFrom::Start(0)).expect("seek to head");
        let mut buf = vec![0; 100];
        assert!(reader.read(&mut buf).is_err());

        // read from current head
        let offset = reader.current_head().expect("get current head");
        reader.seek(SeekFrom::Start(offset)).expect("seek to new head");
        let mut buf = vec![0; 100];
        assert!(reader.read(&mut buf).is_ok());
        assert_eq!(buf, vec![1; 100]);
    }

    #[test]
    fn test_byte_stream_seal() {
        let mut rt = Runtime::new().unwrap();
        let (mut writer, mut reader) = create_reader_and_writer(&mut rt);

        // write 200 bytes
        let payload = vec![1; 200];
        writer.write(&payload).expect("write");
        writer.flush().expect("flush");

        // seal the segment
        rt.block_on(writer.seal()).expect("seal");

        // read sealed stream
        reader.seek(SeekFrom::Start(0)).expect("seek to new head");
        let mut buf = vec![0; 200];
        assert!(reader.read(&mut buf).is_ok());
        assert_eq!(buf, vec![1; 200]);

        let payload = vec![1; 200];
        let write_result = writer.write(&payload);
        let flush_result = writer.flush();
        assert!(write_result.is_err() || flush_result.is_err());
    }

    fn create_reader_and_writer(runtime: &mut Runtime) -> (ByteStreamWriter, ByteStreamReader) {
        let config = ClientConfigBuilder::default()
            .connection_type(ConnectionType::Mock(MockType::Happy))
            .mock(true)
            .controller_uri(PravegaNodeUri::from("127.0.0.2:9091".to_string()))
            .build()
            .unwrap();
        let factory = ClientFactory::new(config);
        runtime.block_on(create_stream(&factory, "testScope", "testStream"));
        let segment = ScopedSegment::from("testScope/testStream/0.#epoch.0");
        let writer = factory.create_byte_stream_writer(segment.clone());
        let reader = factory.create_byte_stream_reader(segment);
        (writer, reader)
    }
}
