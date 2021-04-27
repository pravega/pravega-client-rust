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
use crate::event::writer::EventWriter;
use crate::segment::event::{Incoming, PendingEvent};
use crate::segment::metadata::SegmentMetadataClient;
use crate::segment::reactor::Reactor;
use crate::util::get_random_u128;

use pravega_client_channel::{create_channel, ChannelSender};
use pravega_client_shared::{ScopedSegment, ScopedStream, WriterId};

use std::io::{Error, ErrorKind, Write};
use tokio::sync::oneshot;
use tracing::info_span;
use tracing_futures::Instrument;

type EventHandle = oneshot::Receiver<Result<(), Error>>;

/// Allow for writing raw bytes directly to a segment.
///
/// ByteWriter does not frame, attach headers, or otherwise modify the bytes written to it in any
/// way. So unlike [`EventWriter`] the data written cannot be split apart when read.
/// As such, any bytes written by this API can ONLY be read using [`ByteReader`].
///
/// Similarly, multiple ByteWriters write to the same segment as this will result in interleaved data,
/// which is not desirable in most cases. ByteWriter uses Conditional Append to make sure that writers
/// are aware of the content in the segment. If another process writes data to the segment after this one began writing,
/// all subsequent writes from this writer will not be written and [`flush`] will fail. This prevents data from being accidentally interleaved.
///
/// [`EventWriter`]: crate::event::writer::EventWriter
/// [`ByteReader`]: crate::byte::reader::ByteReader
/// [`flush`]: ByteWriter::flush
///
/// # Note
///
/// The ByteWriter implementation provides [`retry`] logic to handle connection failures and service host
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
///     let mut byte_writer = client_factory.create_byte_writer(segment);
///
///     let payload = "hello world".to_string().into_bytes();
///     byte_writer.write(&payload).expect("write");
///     byte_writer.flush().expect("flush");
/// }
/// ```
pub struct ByteWriter {
    writer_id: WriterId,
    sender: ChannelSender<Incoming>,
    metadata_client: SegmentMetadataClient,
    factory: ClientFactory,
    event_handle: Option<EventHandle>,
    write_offset: i64,
}

/// ByteStreamWriter implements Write trait in standard library.
/// It can be wrapped by BufWriter to improve performance.
impl Write for ByteWriter {
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
    /// [`capacity`]: ByteWriter::CHANNEL_CAPACITY
    ///
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let bytes_to_write = std::cmp::min(buf.len(), EventWriter::MAX_EVENT_SIZE);
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

impl ByteWriter {
    // maximum 16 MB total size of events could be held in memory
    const CHANNEL_CAPACITY: usize = 16 * 1024 * 1024;

    pub fn new(segment: ScopedSegment, factory: ClientFactory) -> Self {
        let rt = factory.get_runtime();
        let (sender, receiver) = create_channel(Self::CHANNEL_CAPACITY);
        let metadata_client = rt.block_on(factory.create_segment_metadata_client(segment.clone()));
        let writer_id = WriterId(get_random_u128());
        let stream = ScopedStream::from(&segment);
        let span = info_span!("Reactor", byte_stream_writer = %writer_id);
        // spawn is tied to the factory runtime.
        rt.spawn(Reactor::run(stream, sender.clone(), receiver, factory.clone(), None).instrument(span));
        ByteWriter {
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
    ) -> oneshot::Receiver<Result<(), Error>> {
        let size = event.len();
        let (tx, rx) = oneshot::channel();
        if let Some(pending_event) = PendingEvent::without_header(None, event, Some(self.write_offset), tx) {
            let append_event = Incoming::AppendEvent(pending_event);
            if let Err(_e) = sender.send((append_event, size)).await {
                let (tx_error, rx_error) = oneshot::channel();
                tx_error
                    .send(Err(Error::new(
                        ErrorKind::BrokenPipe,
                        "failed to send request to reactor",
                    )))
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

impl Drop for ByteWriter {
    fn drop(&mut self) {
        let _res = self.sender.send_without_bp(Incoming::Close());
    }
}
