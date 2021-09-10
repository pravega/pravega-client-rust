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
use crate::error::Error;
use crate::event::writer::EventWriter;
use crate::segment::event::{Incoming, PendingEvent, RoutingInfo};
use crate::segment::metadata::SegmentMetadataClient;
use crate::segment::reactor::Reactor;
use crate::util::get_random_u128;

use pravega_client_channel::{create_channel, ChannelSender};
use pravega_client_shared::{ScopedSegment, ScopedStream, WriterId};

use std::collections::VecDeque;
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
/// ## Atomicity
/// If buf length is less than or equal to 8 MiB, the entire buffer will be written atomically.
/// If buf length is greater than 8 MiB, only the first 8 MiB will be written, and it will be written atomically.
/// In either case, the actual number of bytes written will be returned and those bytes are written atomically.
///
/// ## Parallelism
/// Multiple ByteWriters write to the same segment as this will result in interleaved data,
/// which is not desirable in most cases. ByteWriter uses Conditional Append to make sure that writers
/// are aware of the content in the segment. If another process writes data to the segment after this one began writing,
/// all subsequent writes from this writer will not be written and [`flush`] will fail. This prevents data from being accidentally interleaved.
///
/// ## Backpressure
/// Write has a backpressure mechanism. Internally, it uses [`Channel`] to send event to
/// Reactor for processing. [`Channel`] can has a limited [`capacity`], when its capacity
/// is reached, any further write will not be accepted until enough space has been freed in the [`Channel`].
///
/// # Retry
/// The ByteWriter implementation provides [`retry`] logic to handle connection failures and service host
/// failures. Internal retries will not violate the exactly once semantic so it is better to rely on them
/// than to wrap this with custom retry logic.
///
/// [`channel`]: pravega_client_channel
/// [`capacity`]: ByteWriter::CHANNEL_CAPACITY
/// [`EventWriter`]: crate::event::writer::EventWriter
/// [`ByteReader`]: crate::byte::reader::ByteReader
/// [`flush`]: ByteWriter::flush
/// [`retry`]: pravega_client_retry
///
/// # Examples
/// ```no_run
/// use pravega_client_config::ClientConfigBuilder;
/// use pravega_client::client_factory::ClientFactory;
/// use pravega_client_shared::ScopedStream;
/// use std::io::Write;
///
/// fn main() {
///     // assuming Pravega controller is running at endpoint `localhost:9090`
///     let config = ClientConfigBuilder::default()
///         .controller_uri("localhost:9090")
///         .build()
///         .expect("creating config");
///
///     let client_factory = ClientFactory::new(config);
///
///     // assuming scope:myscope, stream:mystream exist.
///     // notice that this stream should be a fixed sized single segment stream
///     let stream = ScopedStream::from("myscope/mystream");
///
///     let mut byte_writer = client_factory.create_byte_writer(stream);
///
///     let payload = "hello world".to_string().into_bytes();
///
///     // It doesn't mean the data is persisted on the server side
///     // when this method returns Ok, user should call flush to ensure
///     // all data has been acknowledged by the server.
///     client_factory.runtime().block_on(byte_writer.write(&payload)).expect("write");
///     client_factory.runtime().block_on(byte_writer.flush()).expect("flush");
/// }
/// ```
pub struct ByteWriter {
    writer_id: WriterId,
    scoped_segment: ScopedSegment,
    sender: ChannelSender<Incoming>,
    metadata_client: SegmentMetadataClient,
    factory: ClientFactory,
    event_handles: VecDeque<EventHandle>,
    write_offset: i64,
}

impl ByteWriter {
    // maximum 16 MB total size of events could be held in memory
    const CHANNEL_CAPACITY: usize = 16 * 1024 * 1024;

    pub(crate) async fn new(stream: ScopedStream, factory: ClientFactory) -> Self {
        let (sender, receiver) = create_channel(Self::CHANNEL_CAPACITY);
        let writer_id = WriterId(get_random_u128());
        let segments = factory
            .controller_client()
            .get_head_segments(&stream)
            .await
            .expect("get head segments");
        assert_eq!(
            segments.len(),
            1,
            "Byte stream is configured with more than one segment"
        );
        let segment = segments.iter().next().unwrap().0.clone();
        let scoped_segment = ScopedSegment {
            scope: stream.scope.clone(),
            stream: stream.stream.clone(),
            segment,
        };
        let metadata_client = factory
            .create_segment_metadata_client(scoped_segment.clone())
            .await;
        let span = info_span!("Reactor", byte_stream_writer = %writer_id);
        // spawn is tied to the factory runtime.
        factory
            .runtime()
            .spawn(Reactor::run(stream, sender.clone(), receiver, factory.clone(), None).instrument(span));
        ByteWriter {
            writer_id,
            scoped_segment,
            sender,
            metadata_client,
            factory,
            event_handles: VecDeque::new(),
            write_offset: 0,
        }
    }

    /// Writes the given data to the server asynchronously. It doesn't mean the data is persisted on the server side
    /// when this method returns Ok, user should call flush to ensure all data has been acknowledged
    /// by the server.
    ///
    /// # Examples
    /// ```ignore
    /// let mut byte_writer = client_factory.create_byte_writer(segment).await;
    /// let payload = vec![0; 8];
    /// let size = byte_writer.write(&payload).await;
    /// ```
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let bytes_to_write = std::cmp::min(buf.len(), EventWriter::MAX_EVENT_SIZE);
        let payload = buf[0..bytes_to_write].to_vec();
        let oneshot_receiver = self.write_internal(self.sender.clone(), payload).await;
        self.write_offset += bytes_to_write as i64;
        self.event_handles.push_back(oneshot_receiver);

        while let Some(handle) = self.event_handles.front_mut() {
            if let Ok(res) = handle.try_recv() {
                match res {
                    Ok(_) => self.event_handles.pop_front(),
                    Err(e) => Err(e),
                }
            } else {
                break;
            }
        }
        Ok(bytes_to_write)
    }

    /// Flush data.
    ///
    /// It will wait until all pending appends have acknowledgment.
    ///
    /// # Examples
    /// ```ignore
    /// let mut byte_writer = client_factory.create_byte_writer(segment).await;
    /// let payload = vec![0; 8];
    /// let size = byte_writer.write(&payload).await;
    /// byte_writer.flush().await;
    /// ```
    pub async fn flush(&mut self) -> Result<(), Error> {
        for handle in self.event_handles.into_iter() {
            match handle.await {
                Ok(res) => {
                    res?;
                }
                Err(e) => {
                    return Err(Error::InternalFailure {
                        msg: format!("oneshot error {:?}", e),
                    });
                }
            }
        }
        Ok(())
    }

    /// Seal the segment and no further writes are allowed.
    ///
    /// # Examples
    /// ```ignore
    /// let mut byte_writer = client_factory.create_byte_writer(segment).await;
    /// byte_writer.seal().await.expect("seal segment");
    /// ```
    pub async fn seal(&mut self) -> Result<(), Error> {
        self.flush().await?;
        self.metadata_client
            .seal_segment()
            .await
            .map_err(|e| Error::InternalFailure {
                msg: format!("segment seal error: {:?}", e),
            })
    }

    /// Truncate data before a given offset for the segment. No reads are allowed before
    /// truncation point after calling this method.
    ///
    /// # Examples
    /// ```ignore
    /// let byte_writer = client_factory.create_byte_writer(segment).await;
    /// byte_writer.truncate_data_before(1024).await.expect("truncate segment");
    /// ```
    pub async fn truncate_data_before(&self, offset: i64) -> Result<(), Error> {
        self.metadata_client
            .truncate_segment(offset)
            .await
            .map_err(|e| Error::InternalFailure {
                msg: format!("segment truncation error: {:?}", e),
            })
    }

    /// Track the current write position for this writer.
    ///
    /// # Examples
    /// ```ignore
    /// let byte_writer = client_factory.create_byte_writer(segment).await;
    /// let offset = byte_writer.current_write_offset();
    /// ```
    pub fn current_offset(&self) -> u64 {
        self.write_offset as u64
    }

    /// Seek to the tail of the segment.
    ///
    /// This method is useful for tail reads.
    /// # Examples
    /// ```ignore
    /// let mut byte_writer = client_factory.create_byte_writer(segment).await;
    /// byte_writer.seek_to_tail_async().await;
    /// ```
    pub async fn seek_to_tail(&mut self) {
        let segment_info = self
            .metadata_client
            .get_segment_info()
            .await
            .expect("failed to get segment info");
        self.write_offset = segment_info.write_offset;
    }

    /// Reset the internal Reactor, making it ready for new appends.
    ///
    /// Use this method if you want to continue to append after ConditionalCheckFailure happens.
    /// It will clear all pending events and set the Reactor ready.
    ///
    /// # Examples
    /// ```ignore
    /// if let Err(Error::ConditionalCheckFailure(_e)) = byte_writer.flush().await {
    ///     byte_writer.reset().await.expect("reset");
    ///     byte_writer.seek_to_tail().await;
    /// }
    /// byte_writer.write(&payload).await.expect("write");
    /// ```
    pub async fn reset(&mut self) -> Result<(), Error> {
        let Err(Error::ConditionalCheckFailure(_e)) = self.flush().await;
        self.event_handles.clear();
        // send reset signal to reactor
        if let Err(e) = self
            .sender
            .send((Incoming::Reset(self.scoped_segment.clone()), 0))
            .await
        {
            Err(Error::InternalFailure {
                msg: "failed to send request to reactor".to_string(),
            })
        }
        Ok(())
    }

    async fn write_internal(
        &self,
        sender: ChannelSender<Incoming>,
        event: Vec<u8>,
    ) -> oneshot::Receiver<Result<(), Error>> {
        let size = event.len();
        let (tx, rx) = oneshot::channel();
        let routing_info = RoutingInfo::Segment(self.scoped_segment.clone());
        if let Some(pending_event) =
            PendingEvent::without_header(routing_info, event, Some(self.write_offset), tx)
        {
            let append_event = Incoming::AppendEvent(pending_event);
            if let Err(_e) = sender.send((append_event, size)).await {
                let (tx_error, rx_error) = oneshot::channel();
                tx_error
                    .send(Err(Error::InternalFailure {
                        msg: "failed to send request to reactor".to_string(),
                    }))
                    .expect("send error");
                return rx_error;
            }
        }
        rx
    }
}

impl Drop for ByteWriter {
    fn drop(&mut self) {
        let _res = self.sender.send_without_bp(Incoming::Close());
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::util::create_stream;
    use pravega_client_config::connection_type::{ConnectionType, MockType};
    use pravega_client_config::ClientConfigBuilder;
    use pravega_client_shared::PravegaNodeUri;

    #[test]
    #[should_panic(expected = "Byte stream is configured with more than one segment")]
    fn test_invalid_stream_config() {
        let config = ClientConfigBuilder::default()
            .connection_type(ConnectionType::Mock(MockType::Happy))
            .mock(true)
            .controller_uri(PravegaNodeUri::from("127.0.0.2:9091".to_string()))
            .build()
            .unwrap();
        let factory = ClientFactory::new(config);
        factory.runtime().block_on(create_stream(
            &factory,
            "testScopeInvalid",
            "testStreamInvalid",
            2,
        ));
        let stream = ScopedStream::from("testScopeInvalid/testStreamInvalid");
        factory.create_byte_writer(stream);
    }
}
