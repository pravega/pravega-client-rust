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
use crate::segment_reader::AsyncSegmentReaderWrapper;
use pravega_rust_client_shared::{ScopedSegment, ScopedStream, WriterId};
use std::convert::TryInto;
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::time::Duration;
use tracing::info_span;
use tracing_futures::Instrument;
use uuid::Uuid;

const BUFFER_SIZE: usize = 4096;
const CHANNEL_CAPACITY: usize = 100;

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
    sender: Sender<Incoming>,
    metadata_client: SegmentMetadataClient,
    runtime_handle: Handle,
    event_handle: Option<EventHandle>,
}

/// ByteStreamWriter implements Write trait in standard library.
/// It can be wrapped by BufWriter to improve performance.
impl Write for ByteStreamWriter {
    /// Writes the given data to the server. It doesn't mean the data is persisted on the server side
    /// when this method returns Ok, user should call flush to ensure all data has been acknowledged
    /// by the server.
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let bytes_to_write = std::cmp::min(buf.len(), EventStreamWriter::MAX_EVENT_SIZE);
        let oneshot_receiver = self.runtime_handle.block_on(async {
            let payload = buf[0..bytes_to_write].to_vec();
            let mut oneshot_receiver = ByteStreamWriter::write_internal(self.sender.clone(), payload).await;
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

        self.event_handle = oneshot_receiver;
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
        let (sender, receiver) = channel(CHANNEL_CAPACITY);
        let handle = factory.get_runtime_handle();
        let metadata_client = handle.block_on(factory.create_segment_metadata_client(segment.clone()));
        let writer_id = WriterId(get_random_u128());
        let span = info_span!("SegmentReactor", event_stream_writer = %writer_id);
        // tokio::spawn is tied to the factory runtime.
        handle.enter(|| {
            tokio::spawn(
                Reactor::run(
                    ScopedStream::from(&segment),
                    sender.clone(),
                    receiver,
                    factory.clone(),
                )
                .instrument(span),
            )
        });
        ByteStreamWriter {
            writer_id,
            sender,
            metadata_client,
            runtime_handle: handle,
            event_handle: None,
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

    async fn flush_internal(&self, event_handle: EventHandle) -> Result<(), Error> {
        let result = event_handle
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("oneshot error {:?}", e)))?;

        if let Err(e) = result {
            Err(Error::new(ErrorKind::Other, format!("{:?}", e)))
        } else {
            Ok(())
        }
    }
}

/// Allows for reading raw bytes from a segment.
pub struct ByteStreamReader {
    reader_id: Uuid,
    reader: Option<AsyncSegmentReaderWrapper>,
    reader_buffer_size: usize,
    metadata_client: SegmentMetadataClient,
    runtime_handle: Handle,
    timeout: Duration,
}

impl Read for ByteStreamReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        let result = self
            .runtime_handle
            .block_on(self.reader.as_mut().unwrap().read(buf));
        result.map_err(|e| Error::new(ErrorKind::Other, format!("Error: {:?}", e)))
    }
}

impl ByteStreamReader {
    pub(crate) fn new(segment: ScopedSegment, factory: &ClientFactory, buffer_size: usize) -> Self {
        let handle = factory.get_runtime_handle();
        let async_reader = handle.block_on(factory.create_async_event_reader(segment.clone()));
        let async_reader_wrapper =
            AsyncSegmentReaderWrapper::new(handle.clone(), Arc::new(Box::new(async_reader)), 0, buffer_size);
        let metadata_client = handle.block_on(factory.create_segment_metadata_client(segment));
        ByteStreamReader {
            reader_id: Uuid::new_v4(),
            reader: Some(async_reader_wrapper),
            reader_buffer_size: buffer_size,
            metadata_client,
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
        self.reader.as_ref().unwrap().offset
    }

    pub fn set_reader_timeout(&mut self, timeout: Option<Duration>) {
        if let Some(time) = timeout {
            self.timeout = time;
        } else {
            self.timeout = Duration::from_secs(3600);
        }
    }

    fn recreate_reader_wrapper(&mut self, offset: i64) {
        let internal_reader = self.reader.take().unwrap().extract_reader();
        let new_reader_wrapper = AsyncSegmentReaderWrapper::new(
            self.runtime_handle.clone(),
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
                    .runtime_handle
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
        writer.write(&payload).expect("write");
        let result = writer.flush();
        assert!(result.is_err());
    }

    fn create_reader_and_writer(runtime: &mut Runtime) -> (ByteStreamWriter, ByteStreamReader) {
        let config = ClientConfigBuilder::default()
            .connection_type(ConnectionType::Mock(MockType::Happy))
            .mock(true)
            .controller_uri(PravegaNodeUri::from("127.0.0.2:9091".to_string()))
            .build()
            .unwrap();
        let factory = ClientFactory::new(config);
        runtime.block_on(create_stream(&factory, "scope", "stream"));
        let segment = ScopedSegment::from("scope/stream/0");
        let writer = factory.create_byte_stream_writer(segment.clone());
        let reader = factory.create_byte_stream_reader(segment);
        (writer, reader)
    }
}
