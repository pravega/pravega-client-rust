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
use crate::segment_reader::PrefetchingAsyncSegmentReader;
use pravega_client_channel::{create_channel, ChannelSender};
use pravega_client_shared::{ScopedSegment, ScopedStream, WriterId};
use std::convert::TryInto;
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::info_span;
use tracing_futures::Instrument;
use uuid::Uuid;

type EventHandle = oneshot::Receiver<Result<(), SegmentWriterError>>;

/// Allows for writing raw bytes directly to a segment.
///
/// ByteStreamWriter does not frame, attach headers, or otherwise modify the bytes written to it in any
/// way. So unlike [`EventStreamWriter`] the data written cannot be split apart when read.
/// As such, any bytes written by this API can ONLY be read using [`ByteStreamReader`].
///
/// Similarly, multiple ByteStreamWriters write to the same segment as this will result in interleaved data,
/// which is not desirable in most cases. ByteStreamWriter uses Conditional Append to make sure that writers
/// are aware of the content in the segment. If another process writes data to the segment after this one began writing,
/// all subsequent writes from this writer will not be written and [`flush`] will fail. This prevents data from being accidentally interleaved.
///
/// [`EventStreamWriter`]: crate::event_stream_writer::EventStreamWriter
/// [`ByteStreamReader`]: ByteStreamReader
/// [`flush`]: ByteStreamWriter::flush
///
/// # Note
///
/// The ByteStreamWriter implementation provides [`retry`] logic to handle connection failures and service host
/// failures. Internal retries will not violate the exactly once semantic so it is better to rely on them
/// than to wrap this with custom retry logic.
///
/// [`retry`]: pravega_client_retry
///
/// # Examples
/// ```no_run
/// use pravega_client_config::ClientConfigBuilder;
/// use pravega_client::client_factory::ClientFactory;
/// use pravega_client_shared::ScopedSegment;
/// use std::io::Write;
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
///     // assuming scope:myscope, stream:mystream and segment 0 do exist.
///     let segment = ScopedSegment::from("myscope/mystream/0");
///
///     let mut byte_stream_writer = client_factory.create_byte_stream_writer(segment);
///
///     let payload = "hello world".to_string().into_bytes();
///     byte_stream_writer.write(&payload).expect("write");
///     byte_stream_writer.flush().expect("flush");
/// }
/// ```
pub struct ByteStreamWriter {
    writer_id: WriterId,
    sender: ChannelSender<Incoming>,
    metadata_client: SegmentMetadataClient,
    factory: ClientFactory,
    event_handle: Option<EventHandle>,
    write_offset: i64,
}

/// ByteStreamWriter implements Write trait in standard library.
/// It can be wrapped by BufWriter to improve performance.
impl Write for ByteStreamWriter {
    /// Writes the given data to the server. It doesn't mean the data is persisted on the server side
    /// when this method returns Ok, user should call flush to ensure all data has been acknowledged
    /// by the server.
    ///
    /// Write has a backpressure mechanism. Internally, it uses [`Channel`] to send event to
    /// Reactor for processing. [`Channel`] can has a limited [`capacity`], when its capacity
    /// is reached, any further write will not be accepted until enough space has been freed in the [`Channel`].
    ///
    ///
    /// [`channel`]: pravega_client_channel
    /// [`capacity`]: ByteStreamWriter::CHANNEL_CAPACITY
    ///
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let bytes_to_write = std::cmp::min(buf.len(), EventStreamWriter::MAX_EVENT_SIZE);
        let payload = buf[0..bytes_to_write].to_vec();
        let oneshot_receiver = self
            .factory
            .get_runtime()
            .block_on(self.write_internal(self.sender.clone(), payload));

        self.write_offset += bytes_to_write as i64;
        self.event_handle = Some(oneshot_receiver);
        Ok(bytes_to_write)
    }

    /// This is a blocking call that will wait for data to be persisted on the server side.
    fn flush(&mut self) -> Result<(), Error> {
        if let Some(event_handle) = self.event_handle.take() {
            self.factory
                .get_runtime()
                .block_on(self.flush_internal(event_handle))
        } else {
            Ok(())
        }
    }
}

impl ByteStreamWriter {
    // maximum 16 MB total size of events could be held in memory
    const CHANNEL_CAPACITY: usize = 16 * 1024 * 1024;

    pub(crate) fn new(segment: ScopedSegment, factory: ClientFactory) -> Self {
        let rt = factory.get_runtime();
        let (sender, receiver) = create_channel(Self::CHANNEL_CAPACITY);
        let metadata_client = rt.block_on(factory.create_segment_metadata_client(segment.clone()));
        let writer_id = WriterId(get_random_u128());
        let stream = ScopedStream::from(&segment);
        let span = info_span!("Reactor", byte_stream_writer = %writer_id);
        // spawn is tied to the factory runtime.
        rt.spawn(Reactor::run(stream, sender.clone(), receiver, factory.clone(), None).instrument(span));
        ByteStreamWriter {
            writer_id,
            sender,
            metadata_client,
            factory,
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
            .factory
            .get_runtime()
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
    reader: Option<PrefetchingAsyncSegmentReader>,
    reader_buffer_size: usize,
    metadata_client: SegmentMetadataClient,
    factory: ClientFactory,
}

impl Read for ByteStreamReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        let result = self
            .factory
            .get_runtime()
            .block_on(self.reader.as_mut().unwrap().read(buf));
        result.map_err(|e| Error::new(ErrorKind::Other, format!("Error: {:?}", e)))
    }
}

impl ByteStreamReader {
    pub(crate) fn new(segment: ScopedSegment, factory: ClientFactory, buffer_size: usize) -> Self {
        let async_reader = factory
            .get_runtime()
            .block_on(factory.create_async_event_reader(segment.clone()));
        let async_reader_wrapper = PrefetchingAsyncSegmentReader::new(
            factory.get_runtime().handle().clone(),
            Arc::new(Box::new(async_reader)),
            0,
            buffer_size,
        );
        let metadata_client = factory
            .get_runtime()
            .block_on(factory.create_segment_metadata_client(segment));
        ByteStreamReader {
            reader_id: Uuid::new_v4(),
            reader: Some(async_reader_wrapper),
            reader_buffer_size: buffer_size,
            metadata_client,
            factory,
        }
    }

    /// Returns the head of current readable data in the segment.
    pub fn current_head(&self) -> std::io::Result<u64> {
        self.factory
            .get_runtime()
            .block_on(self.metadata_client.fetch_current_starting_head())
            .map(|i| i as u64)
            .map_err(|e| Error::new(ErrorKind::Other, format!("{:?}", e)))
    }

    /// Returns the read offset.
    pub fn current_offset(&self) -> i64 {
        self.reader.as_ref().unwrap().offset
    }

    /// Returns the bytes that are available to read instantly without fetching from server.
    pub fn available(&self) -> usize {
        self.reader.as_ref().unwrap().available()
    }

    fn recreate_reader_wrapper(&mut self, offset: i64) {
        let internal_reader = self.reader.take().unwrap().extract_reader();
        let new_reader_wrapper = PrefetchingAsyncSegmentReader::new(
            self.factory.get_runtime().handle().clone(),
            internal_reader,
            offset,
            self.reader_buffer_size,
        );
        self.reader = Some(new_reader_wrapper);
    }
}

/// The Seek implementation for ByteStreamReader allows seeking to a byte offset from the beginning
/// of the stream or a byte offset relative to the current position in the stream.
/// If the stream has been truncated, the byte offset will be relative to the original beginning of the stream.
impl Seek for ByteStreamReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(offset) => {
                let offset = offset.try_into().map_err(|e| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        format!("Overflowed when converting offset to i64: {:?}", e),
                    )
                })?;
                self.recreate_reader_wrapper(offset);
                Ok(offset as u64)
            }
            SeekFrom::Current(offset) => {
                let new_offset = self.reader.as_ref().unwrap().offset + offset;
                if new_offset < 0 {
                    Err(Error::new(
                        ErrorKind::InvalidInput,
                        "Cannot seek to a negative offset",
                    ))
                } else {
                    self.recreate_reader_wrapper(new_offset);
                    Ok(new_offset as u64)
                }
            }
            SeekFrom::End(offset) => {
                let tail = self
                    .factory
                    .get_runtime()
                    .block_on(self.metadata_client.fetch_current_segment_length())
                    .map_err(|e| Error::new(ErrorKind::Other, format!("{:?}", e)))?;
                if tail + offset < 0 {
                    Err(Error::new(
                        ErrorKind::InvalidInput,
                        "Cannot seek to a negative offset",
                    ))
                } else {
                    let new_offset = tail + offset;
                    self.recreate_reader_wrapper(new_offset);
                    Ok(new_offset as u64)
                }
            }
        }
    }
}

impl Drop for ByteStreamWriter {
    fn drop(&mut self) {
        let _res = self.sender.send_without_bp(Incoming::Close());
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::create_stream;
    use pravega_client_config::connection_type::{ConnectionType, MockType};
    use pravega_client_config::ClientConfigBuilder;
    use pravega_client_shared::PravegaNodeUri;
    use tokio::runtime::Runtime;

    #[test]
    fn test_byte_stream_seek() {
        let rt = Runtime::new().unwrap();
        let (mut writer, mut reader) = create_reader_and_writer(&rt);

        // write 200 bytes
        let payload = vec![1; 200];
        writer.write(&payload).expect("write");
        writer.flush().expect("flush");

        // read 200 bytes from beginning
        let mut buf = vec![0; 200];
        let mut read = 0;
        while read != 200 {
            let r = reader.read(&mut buf).expect("read");
            read += r;
        }
        assert_eq!(read, 200);
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
        let rt = Runtime::new().unwrap();
        let (mut writer, mut reader) = create_reader_and_writer(&rt);

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
        let rt = Runtime::new().unwrap();
        let (mut writer, mut reader) = create_reader_and_writer(&rt);

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

    fn create_reader_and_writer(runtime: &Runtime) -> (ByteStreamWriter, ByteStreamReader) {
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
