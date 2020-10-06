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
use crate::segment_metadata::SegmentMetadataClient;
use crate::segment_reader::{AsyncSegmentReader, AsyncSegmentReaderImpl};
use pravega_rust_client_config::ClientConfig;
use pravega_rust_client_shared::{ScopedSegment, WriterId};
use std::cmp;
use std::io::Error;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::info_span;
use tracing_futures::Instrument;
use uuid::Uuid;

const BUFFER_SIZE: usize = 4096;
const CHANNEL_CAPACITY: usize = 100;

type EventHandle = oneshot::Receiver<Result<(), SegmentWriterError>>;

pub struct ByteStreamWriter {
    writer_id: WriterId,
    sender: Sender<Incoming>,
    metadata_client: SegmentMetadataClient,
    runtime_handle: Handle,
    event_handle: Option<EventHandle>,
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

        self.event_handle = oneshot_receiver;
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), Error> {
        let event_handle = self.event_handle.take();
        self.runtime_handle.block_on(self.flush_internal(event_handle))
    }
}

impl ByteStreamWriter {
    pub(crate) fn new(segment: ScopedSegment, config: ClientConfig, factory: ClientFactory) -> Self {
        let (sender, receiver) = channel(CHANNEL_CAPACITY);
        let handle = factory.get_runtime_handle();
        let metadata_client = handle.block_on(factory.create_segment_metadata_client(segment.clone()));
        let writer_id = WriterId(get_random_u128());
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
            metadata_client,
            runtime_handle: handle,
            event_handle: None,
        }
    }

    /// Seal will seal the segment and no further writes are allowed.
    pub async fn seal(&mut self) -> Result<(), Error> {
        let event_handle = self.event_handle.take();
        self.flush_internal(event_handle).await?;
        self.metadata_client
            .seal_segment()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("segment seal error: {:?}", e)))
    }

    /// Truncate data before a given offset for the segment. No reads are allowed before
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

    async fn flush_internal(&self, event_handle: Option<EventHandle>) -> Result<(), Error> {
        if event_handle.is_none() {
            return Ok(());
        }

        let result = event_handle
            .unwrap()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("oneshot error {:?}", e)))?;

        if let Err(e) = result {
            Err(Error::new(ErrorKind::Other, format!("{:?}", e)))
        } else {
            Ok(())
        }
    }
}

pub struct ByteStreamReader {
    reader_id: Uuid,
    reader: AsyncSegmentReaderImpl,
    metadata_client: SegmentMetadataClient,
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
        let async_reader = handle.block_on(factory.create_async_event_reader(segment.clone()));
        let metadata_client = handle.block_on(factory.create_segment_metadata_client(segment));
        ByteStreamReader {
            reader_id: Uuid::new_v4(),
            reader: async_reader,
            metadata_client,
            offset: 0,
            runtime_handle: handle,
        }
    }

    pub fn current_head(&self) -> std::io::Result<u64> {
        self.runtime_handle
            .block_on(self.metadata_client.fetch_current_starting_head())
            .map(|i| i as u64)
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
    }
}

/// The Seek implementation for ByteStreamReader allows seeking to a byte offset from the beginning
/// of the stream or a byte offset relative to the current position in the stream.
/// If the stream has been truncated, the byte offset will be relative to the original beginning of the stream.
/// Seek from the end of the stream is not implemented.
impl Seek for ByteStreamReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(offset) => {
                self.offset = offset as i64;
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
                if offset > 0 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "Offset exceeds segment length",
                    ));
                }
                let tail = self
                    .runtime_handle
                    .block_on(self.metadata_client.fetch_current_segment_length())
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
                self.offset = tail + offset;
                Ok(self.offset as u64)
            }
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {}
}
