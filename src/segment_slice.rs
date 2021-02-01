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
use crate::event_reader::SegmentReadResult;
use crate::segment_reader::AsyncSegmentReader;
use crate::segment_reader::ReaderError::SegmentSealed;
use bytes::{Buf, BufMut, BytesMut};
use core::fmt;
use pravega_client_retry::retry_result::Retryable;
use pravega_client_shared::ScopedSegment;
use pravega_wire_protocol::commands::{Command, EventCommand, TYPE_PLUS_LENGTH_SIZE};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::{debug, info, warn};

///
/// This represents an event that was read from a Pravega Segment and the offset at which the event
/// was read from.
///
#[derive(Debug)]
pub struct Event {
    pub offset_in_segment: i64,
    pub value: Vec<u8>,
}

///
/// This represents a Segment slice which can be used to read events from a Pravega segment as an
/// iterator.
///
pub struct SegmentSlice {
    pub meta: SliceMetadata,
    pub(crate) slice_return_tx: Option<oneshot::Sender<Option<SliceMetadata>>>,
}

impl fmt::Debug for SegmentSlice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentSlice").field("meta", &self.meta).finish()
    }
}

///
/// This holds the SegmentSlice metadata. This meta is persisted by the EventReader.
///
#[derive(Clone)]
pub struct SliceMetadata {
    pub start_offset: i64,
    pub scoped_segment: String,
    pub last_event_offset: i64,
    pub read_offset: i64,
    pub end_offset: i64,
    pub(crate) segment_data: SegmentDataBuffer,
    pub partial_data_present: bool,
}

impl fmt::Debug for SliceMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SliceMetadata")
            .field("start_offset", &self.start_offset)
            .field("scoped_segment", &self.scoped_segment)
            .field("last_event_offset", &self.last_event_offset)
            .field("read_offset", &self.read_offset)
            .field("end_offset", &self.end_offset)
            .field("partial_data_present", &self.partial_data_present)
            .finish()
    }
}

///
/// Read Buffer size
///
const READ_BUFFER_SIZE: i32 = 8 * 1024 * 1024; // max size for a single Event

///
/// Structure to track the offset and byte array.
///
#[derive(Debug, Clone)]
pub struct SegmentDataBuffer {
    pub(crate) segment: String,
    pub(crate) offset_in_segment: i64,
    pub(crate) value: BytesMut,
}

impl SegmentDataBuffer {
    ///
    /// Removes the bytes from the current view, returning them in a new `SegmentDataBuffer` handle.
    /// Afterwards, `self` will be empty.
    /// This is identical to `self.split_to(length)`.
    ///
    pub fn split(&mut self) -> SegmentDataBuffer {
        let res = self.value.split();
        let old_offset = self.offset_in_segment;
        let new_offset = old_offset + res.len() as i64;
        self.offset_in_segment = new_offset;
        SegmentDataBuffer {
            segment: self.segment.clone(),
            offset_in_segment: old_offset,
            value: res,
        }
    }

    ///
    /// Splits the buffer into two at the given index.
    ///
    /// Afterwards `self` contains elements `[at, len)`, and the returned `SegmentDataBuffer`
    /// contains elements `[0, at)`.
    ///
    /// `self.offset_in_segment` is updated accordingly.
    ///
    /// # Panics
    ///
    /// Panics if `at > len`.
    ///
    pub fn split_to(&mut self, at: usize) -> SegmentDataBuffer {
        let old_offset = self.offset_in_segment;
        let res = self.value.split_to(at);
        self.offset_in_segment = old_offset + at as i64;

        SegmentDataBuffer {
            segment: self.segment.clone(),
            offset_in_segment: old_offset,
            value: res,
        }
    }

    ///
    /// Gets a signed 32 bit integer from `self` in big-endian byte order.
    ///
    /// The offset in segment is advanced by 4.
    ///
    /// # Panics
    ///
    /// This function panics if there is not enough remaining data in `self`.
    ///
    pub fn get_i32(&mut self) -> i32 {
        let result = self.value.get_i32();
        self.offset_in_segment += 4;
        result
    }

    /// Advance the internal cursor of the buffer.
    pub fn advance(&mut self, cnt: usize) {
        self.value.advance(cnt);
        self.offset_in_segment += cnt as i64;
    }

    ///
    /// Returns an empty SegmentDataBuffer. The offset is set as 0.
    ///
    pub fn empty() -> SegmentDataBuffer {
        SegmentDataBuffer {
            segment: Default::default(),
            offset_in_segment: 0,
            value: BytesMut::with_capacity(0),
        }
    }
}

impl SliceMetadata {
    ///
    /// Method to check if the slice has partial data.
    ///
    fn is_empty(&self) -> bool {
        self.segment_data.value.is_empty()
    }

    ///
    /// Method to verify if the Segment has pending events that can be read.
    ///
    pub fn has_events(&self) -> bool {
        !self.partial_data_present && self.segment_data.value.len() > TYPE_PLUS_LENGTH_SIZE as usize
    }
}

impl Default for SliceMetadata {
    fn default() -> Self {
        SliceMetadata {
            start_offset: Default::default(),
            scoped_segment: Default::default(),
            last_event_offset: Default::default(),
            read_offset: Default::default(),
            end_offset: i64::MAX,
            segment_data: SegmentDataBuffer::empty(),
            partial_data_present: false,
        }
    }
}

impl Default for SegmentSlice {
    fn default() -> Self {
        SegmentSlice {
            meta: Default::default(),
            slice_return_tx: None,
        }
    }
}

impl SegmentSlice {
    ///
    /// Create a new SegmentSlice for a given start_offset, segment.
    /// This spawns an asynchronous task to fetch data from the segment with length of  `READ_BUFFER_SIZE`.
    /// The channel buffer size is 1 which ensure only one outstanding read request to Segment store.
    ///
    pub(crate) fn new(
        segment: ScopedSegment,
        start_offset: i64,
        slice_return_tx: oneshot::Sender<Option<SliceMetadata>>,
    ) -> Self {
        SegmentSlice {
            meta: SliceMetadata {
                start_offset,
                scoped_segment: segment.to_string(),
                last_event_offset: 0,
                read_offset: start_offset,
                end_offset: i64::MAX,
                segment_data: SegmentDataBuffer::empty(),
                partial_data_present: false,
            },
            slice_return_tx: Some(slice_return_tx),
        }
    }

    ///
    /// Method to fetch data from the Segment store from a given start offset.
    ///
    pub(crate) async fn get_segment_data(
        segment: ScopedSegment,
        start_offset: i64,
        tx: Sender<SegmentReadResult>,
        mut drop_fetch: oneshot::Receiver<()>,
        factory: ClientFactory,
    ) {
        let mut offset: i64 = start_offset;
        let segment_reader = factory.create_async_event_reader(segment.clone()).await;
        loop {
            if let Ok(_) | Err(TryRecvError::Closed) = drop_fetch.try_recv() {
                info!("Stop reading from the segment");
                break;
            }
            debug!(
                "Send read request to Segment store at offset {:?} with length {:?}",
                offset, READ_BUFFER_SIZE
            );
            let read = segment_reader.read(offset, READ_BUFFER_SIZE).await;
            match read {
                Ok(reply) => {
                    let len = reply.data.len();
                    if len == 0 && reply.end_of_segment {
                        info!("Reached end of segment {:?} during read ", segment.clone());
                        let data = SegmentSealed {
                            segment: segment.to_string(),
                            can_retry: false,
                            operation: "read segment".to_string(),
                            error_msg: "reached the end of stream".to_string(),
                        };
                        // send data: this waits until there is capacity in the channel.
                        if let Err(e) = tx.send(Err((data, offset))).await {
                            warn!("Error while sending segment data to event parser {:?} ", e);
                            break;
                        }
                        drop(tx);
                        break;
                    } else {
                        let segment_data = bytes::BytesMut::from(reply.data.as_slice());
                        let data = SegmentDataBuffer {
                            segment: segment.to_string(),
                            offset_in_segment: offset,
                            value: segment_data,
                        };
                        // send data: this waits until there is capacity in the channel.
                        if let Err(e) = tx.send(Ok(data)).await {
                            info!("Error while sending segment data to event parser {:?} ", e);
                            break;
                        }
                        offset += len as i64;
                    }
                }
                Err(e) => {
                    warn!("Error while reading from segment {:?}", e);
                    if !e.can_retry() {
                        let _s = tx.send(Err((e, offset))).await;
                        break;
                    }
                }
            }
        }
    }

    ///
    /// Return the starting offset of the SegmentSlice.
    ///
    fn get_starting_offset(&self) -> i64 {
        self.meta.start_offset
    }

    ///
    /// Return the segment associated with the SegmentSlice.
    ///
    fn get_segment(&self) -> String {
        //Returns the name of the segment
        self.meta.scoped_segment.clone()
    }

    ///
    /// Extract the next event from the data received from the Segment store.
    /// Note: This returns a copy of the data received.
    /// Return None in case of a Partial data.
    ///
    fn extract_event(
        &mut self,
        parse_header: fn(&mut SegmentDataBuffer) -> Option<SegmentDataBuffer>,
    ) -> Option<Event> {
        if let Some(mut event_data) = parse_header(&mut self.meta.segment_data) {
            let bytes_to_read = event_data.value.capacity();
            if bytes_to_read == 0 {
                warn!("Found a header with length as zero");
                return None;
            }
            if self.meta.segment_data.value.remaining() >= bytes_to_read + TYPE_PLUS_LENGTH_SIZE as usize {
                self.meta.segment_data.advance(TYPE_PLUS_LENGTH_SIZE as usize);
                // all the data of the event is already present.
                let t = self.meta.segment_data.split_to(bytes_to_read);
                event_data.value.put(t.value);
                info!("extract event data with length {}", event_data.value.len());
                //Convert to Event and send it.
                let event = Event {
                    offset_in_segment: event_data.offset_in_segment,
                    value: event_data.value.freeze().to_vec(),
                };
                Some(event)
            } else {
                // complete data for a given event is not present in the buffer.
                debug!(
                    "partial event read: data read length {}, target read length {}",
                    event_data.value.len(),
                    event_data.value.capacity()
                );
                self.meta.partial_data_present = true;
                None
            }
        } else {
            self.meta.partial_data_present = true;
            None
        }
    }

    ///
    /// This method reads the header and returns a BytesMut whose size is as big as the event.
    /// If complete header is not present return None.
    ///
    fn read_header(data: &mut SegmentDataBuffer) -> Option<SegmentDataBuffer> {
        if data.value.len() >= TYPE_PLUS_LENGTH_SIZE as usize {
            let event_offset = data.offset_in_segment;
            //workaround since we cannot go back in the position using BytesMut
            let mut bytes_temp = data.value.bytes();
            let type_code = bytes_temp.get_i32();
            let len = bytes_temp.get_i32();
            assert_eq!(type_code, EventCommand::TYPE_CODE, "Expected EventCommand here.");
            debug!("Event size is {}", len);
            Some(SegmentDataBuffer {
                segment: data.segment.clone(),
                offset_in_segment: event_offset,
                value: BytesMut::with_capacity(len as usize),
            })
        } else {
            None
        }
    }

    pub fn is_empty(&self) -> bool {
        self.meta.segment_data.value.is_empty() || self.meta.partial_data_present
    }
}

///
/// Iterator implementation of SegmentSlice.
///
impl Iterator for SegmentSlice {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        // extract event from already fetched data.
        let res = self.extract_event(SegmentSlice::read_header);

        match res {
            Some(event) => {
                self.meta.last_event_offset = event.offset_in_segment;
                self.meta.read_offset =
                    event.offset_in_segment + event.value.len() as i64 + TYPE_PLUS_LENGTH_SIZE as i64;
                if !self.meta.is_empty() {
                    assert_eq!(
                        self.meta.read_offset, self.meta.segment_data.offset_in_segment,
                        "Error in offset computation"
                    );
                }
                Some(event)
            }
            None => {
                if self.meta.is_empty() {
                    info!(
                        "Finished reading events from the segment slice of {:?}",
                        self.meta.scoped_segment
                    );
                } else {
                    info!("Partial event present in the segment slice of {:?}, this will be returned post a new read request", self.meta.scoped_segment);
                }
                None
            }
        }
    }
}

// Ensure a Drop of Segment slice releases the segment back to the reader group.
impl Drop for SegmentSlice {
    fn drop(&mut self) {
        if let Some(sender) = self.slice_return_tx.take() {
            self.slice_return_tx = None;
            let _ = sender.send(Some(self.meta.clone()));
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use bytes::{Buf, BufMut, BytesMut};
    use std::iter;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Sender;

    ///
    /// This method reads the header and returns a BytesMut whose size is as big as the event.
    ///
    fn custom_read_header(data: &mut SegmentDataBuffer) -> Option<SegmentDataBuffer> {
        if data.value.remaining() >= 4 {
            let mut temp = data.value.bytes();
            let len = temp.get_i32();
            Some(SegmentDataBuffer {
                segment: data.segment.clone(),
                offset_in_segment: 0,
                value: BytesMut::with_capacity(len as usize),
            })
        } else {
            None
        }
    }

    #[tokio::test]
    async fn test_read_partial_events_buffer_10() {
        let (tx, mut rx) = mpsc::channel(1);
        tokio::spawn(generate_variable_size_events(tx, 10, 20));
        let mut segment_slice = create_segment_slice();
        let mut expected_offset: usize = 0;
        let mut expected_event_len = 1;

        loop {
            if segment_slice.is_empty() {
                if let Some(response) = rx.recv().await {
                    segment_slice.meta.segment_data.value.put(response.value);
                } else {
                    break; // All events are sent.
                }
            }

            while let Some(d) = segment_slice.next() {
                assert_eq!(expected_offset, d.offset_in_segment as usize);
                assert_eq!(expected_event_len, d.value.len());
                assert!(is_all_same(d.value.as_slice()));
                expected_offset += 8 + expected_event_len;
                expected_event_len += 1;
            }
        }
        assert_eq!(20, expected_event_len);
    }

    #[tokio::test]
    async fn test_read_partial_events_buffer_100() {
        let (tx, mut rx) = mpsc::channel(1);
        tokio::spawn(generate_variable_size_events(tx, 100, 200));
        let mut segment_slice = create_segment_slice();
        let mut expected_offset: usize = 0;
        let mut expected_event_len = 1;

        loop {
            if segment_slice.is_empty() {
                if let Some(response) = rx.recv().await {
                    segment_slice.meta.segment_data.value.put(response.value);
                } else {
                    break; // All events are sent.
                }
            }

            while let Some(d) = segment_slice.next() {
                assert_eq!(expected_offset, d.offset_in_segment as usize);
                assert_eq!(expected_event_len, d.value.len());
                assert!(is_all_same(d.value.as_slice()));
                expected_offset += 8 + expected_event_len;
                expected_event_len += 1;
            }
        }
        assert_eq!(200, expected_event_len);
    }

    // create a segment slice for testing.
    fn create_segment_slice() -> SegmentSlice {
        let segment = ScopedSegment::from("test/test/123");
        let segment_slice = SegmentSlice {
            meta: SliceMetadata {
                start_offset: 0,
                scoped_segment: segment.to_string(),
                last_event_offset: 0,
                read_offset: 0,
                end_offset: i64::MAX,
                segment_data: SegmentDataBuffer::empty(),
                partial_data_present: false,
            },
            slice_return_tx: None,
        };
        segment_slice
    }

    // Helper method to verify all events read by Segment slice.
    fn is_all_same<T: Eq>(slice: &[T]) -> bool {
        slice
            .get(0)
            .map(|first| slice.iter().all(|x| x == first))
            .unwrap_or(true)
    }

    // Generate events to simulate Pravega SegmentReadCommand.
    async fn generate_variable_size_events(
        tx: Sender<SegmentDataBuffer>,
        buf_size: usize,
        num_events: usize,
    ) {
        let mut buf = BytesMut::with_capacity(buf_size);
        let segment = ScopedSegment::from("test/test/123").to_string();
        let mut offset: i64 = 0;
        for i in 1..num_events {
            let mut data = event_data(i);
            if data.len() < buf.capacity() - buf.len() {
                buf.put(data);
            } else {
                while data.len() > 0 {
                    let free_space = buf.capacity() - buf.len();
                    if free_space == 0 {
                        tx.send(SegmentDataBuffer {
                            segment: segment.clone(),
                            offset_in_segment: offset,
                            value: buf,
                        })
                        .await
                        .unwrap();
                        offset += buf_size as i64;
                        buf = BytesMut::with_capacity(buf_size);
                    } else if free_space >= data.len() {
                        buf.put(data.split());
                    } else {
                        buf.put(data.split_to(free_space));
                    }
                }
            }
        }
        // send the last event.
        tx.send(SegmentDataBuffer {
            segment,
            offset_in_segment: offset,
            value: buf,
        })
        .await
        .unwrap();
    }

    // Generate event data given the length of the event.
    // The data is 'a'
    fn event_data(len: usize) -> BytesMut {
        let mut buf = BytesMut::with_capacity(len + 8);
        buf.put_i32(EventCommand::TYPE_CODE);
        buf.put_i32(len as i32); // header

        let mut data = Vec::new();
        data.extend(iter::repeat(b'a').take(len));
        buf.put(data.as_slice());
        buf
    }

    // Custom multiple size events.
    async fn generate_multiple_constant_size_events(tx: Sender<SegmentDataBuffer>) {
        let mut buf = BytesMut::with_capacity(10);
        let segment = ScopedSegment::from("test/test/123").to_string();

        buf.put_i32(1);
        buf.put_u8(b'a');
        buf.put_i32(2);
        buf.put(&b"aa"[..]);
        tx.send(SegmentDataBuffer {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(3);
        buf.put(&b"aaa"[..]);
        tx.send(SegmentDataBuffer {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(4);
        buf.put(&b"aaaa"[..]);
        tx.send(SegmentDataBuffer {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(5);
        buf.put(&b"aaaaa"[..]);
        tx.send(SegmentDataBuffer {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(6);
        buf.put(&b"aaaaaa"[..]);
        tx.send(SegmentDataBuffer {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(7);
        buf.put(&b"aaaaaa"[..]);
        tx.send(SegmentDataBuffer {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_u8(b'a');
        buf.put_i32(8);
        buf.put(&b"aaaaa"[..]);
        tx.send(SegmentDataBuffer {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put(&b"aaa"[..]);
        tx.send(SegmentDataBuffer {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();
    }

    ///
    /// This is a test function to generate single events of varying sizes
    ///
    async fn generate_multiple_variable_sized_events(tx: Sender<SegmentDataBuffer>) {
        for i in 1..11 {
            let mut buf = BytesMut::with_capacity(32);
            buf.put_i32(i); // length.
            for _ in 0..i {
                buf.put(&b"a"[..]);
            }
            if let Err(_) = tx
                .send(SegmentDataBuffer {
                    segment: ScopedSegment::from("test/test/123").to_string(),
                    offset_in_segment: 0,
                    value: buf,
                })
                .await
            {
                warn!("receiver dropped");
                return;
            }
        }
    }
}
