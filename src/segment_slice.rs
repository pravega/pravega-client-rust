//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactoryInternal;
use crate::segment_reader::{AsyncSegmentReader, AsyncSegmentReaderImpl};
use bytes::{Buf, BufMut, BytesMut};
use chashmap::CHashMap;
use pravega_rust_client_shared::ScopedSegment;
use pravega_wire_protocol::commands::{Command, EventCommand, TYPE_PLUS_LENGTH_SIZE};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::mpsc::Sender;
use tokio::time::{delay_for, Duration};
use tracing::{debug, error, info, warn};

///
/// This represents an event that was read from a Pravega Segment and the offset at which the event
/// was read from.
///
#[derive(Debug)]
pub struct Event {
    offset_in_segment: i64,
    value: Vec<u8>,
}

///
/// This represents a Segment slice which can be used to read events from a Pravega segment as an
/// iterator.
///
///
#[derive(Debug, Clone)]
pub struct SegmentSlice {
    pub(crate) start_offset: i64,
    pub(crate) segment: ScopedSegment,
    pub(crate) read_offset: i64,
    pub(crate) end_offset: i64,
    pub(crate) segment_data: BytePlaceholder,
    pub(crate) handle: Handle,
    pub(crate) partial_event: BytePlaceholder,
    pub(crate) partial_header: BytePlaceholder,
    pub(crate) reader_meta: Arc<CHashMap<ScopedSegment, SegmentSlice>>,
}

///
/// Read Buffer size
///
const READ_BUFFER_SIZE: i32 = 2048; // TODO: 128K

///
/// Structure to track the offset and byte array.
///
#[derive(Debug, Clone)]
pub struct BytePlaceholder {
    pub(crate) segment: ScopedSegment,
    pub(crate) offset_in_segment: i64,
    pub(crate) value: BytesMut,
}

impl BytePlaceholder {
    ///
    /// Removes the bytes from the current view, returning them in a new `BytePlaceholder` handle.
    /// Afterwards, `self` will be empty.
    /// This is identical to `self.split_to(length)`.
    ///
    pub fn split(&mut self) -> BytePlaceholder {
        let res = self.value.split();
        let old_offset = self.offset_in_segment;
        let new_offset = old_offset + res.len() as i64;
        self.offset_in_segment = new_offset;
        BytePlaceholder {
            segment: self.segment.clone(),
            offset_in_segment: old_offset,
            value: res,
        }
    }

    ///
    /// Splits the buffer into two at the given index.
    ///
    /// Afterwards `self` contains elements `[at, len)`, and the returned `BytePlaceholder`
    /// contains elements `[0, at)`.
    ///
    /// `self.offset_in_segment` is updated accordingly.
    ///
    /// # Panics
    ///
    /// Panics if `at > len`.
    ///
    pub fn split_to(&mut self, at: usize) -> BytePlaceholder {
        let old_offset = self.offset_in_segment;
        let res = self.value.split_to(at);
        self.offset_in_segment = old_offset + at as i64;

        BytePlaceholder {
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

    ///
    /// Returns an empty BytePlaceholder. The offset is set as 0.
    ///
    pub fn empty(segment: ScopedSegment) -> BytePlaceholder {
        BytePlaceholder {
            segment,
            offset_in_segment: 0,
            value: BytesMut::with_capacity(0),
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
        tx: Sender<BytePlaceholder>,
        factory: Arc<ClientFactoryInternal>,
        reader_meta: Arc<CHashMap<ScopedSegment, SegmentSlice>>,
    ) -> Self {
        let handle = factory.get_runtime_handle();
        handle.enter(|| {
            tokio::spawn(SegmentSlice::get_segment_data(
                segment.clone(),
                start_offset,
                tx,
                factory.clone(),
            ))
        });

        SegmentSlice {
            start_offset,
            segment: segment.clone(),
            read_offset: 0,
            end_offset: i64::MAX,
            segment_data: BytePlaceholder::empty(segment.clone()),
            handle,
            partial_event: BytePlaceholder::empty(segment.clone()),
            partial_header: BytePlaceholder::empty(segment),
            reader_meta,
        }
    }

    ///
    /// Method to fetch data from the Segment store from a given start offset.
    ///
    async fn get_segment_data(
        segment: ScopedSegment,
        start_offset: i64,
        mut tx: Sender<BytePlaceholder>,
        factory: Arc<ClientFactoryInternal>,
    ) {
        let mut offset: i64 = start_offset;
        let segment_reader = AsyncSegmentReaderImpl::init(segment.clone(), &factory).await;
        loop {
            debug!(
                "Send read request to Segment store at offset {:?} with length {:?}",
                offset, READ_BUFFER_SIZE
            );
            let read = segment_reader.read(offset, READ_BUFFER_SIZE).await;
            match read {
                Ok(reply) => {
                    debug!("Read Response from Segment store {:?}", reply);
                    let len = reply.data.len();
                    if len == 0 || reply.end_of_segment {
                        drop(tx);
                        break;
                    } else {
                        let segment_data = bytes::BytesMut::from(reply.data.as_slice());
                        let data = BytePlaceholder {
                            segment: segment.clone(),
                            offset_in_segment: offset,
                            value: segment_data,
                        };
                        // send data: this waits until there is capacity in the channel.
                        if let Err(e) = tx.send(data).await {
                            info!("Error while sending segment data to event parser {:?} ", e);
                            break;
                        }
                        offset += len as i64;
                    }
                }
                Err(e) => {
                    warn!("Error while reading from segment {:?}", e);
                    break;
                }
            }
        }
    }

    ///
    /// Return the starting offset of the SegmentSlice.
    ///
    fn get_starting_offset(&self) -> i64 {
        self.start_offset
    }

    ///
    /// Return the segment associated with the SegmentSlice.
    ///
    fn get_segment(&self) -> String {
        //Returns the name of the segment
        self.segment.to_string()
    }

    ///
    /// Extract the next event from the data received from the Segment store.
    /// Return None in case of a Partial data.
    ///
    fn extract_event(
        &mut self,
        parse_header: fn(&mut BytePlaceholder) -> Option<BytePlaceholder>,
    ) -> Option<Event> {
        // check if a partial header was read last.
        if self.partial_header.value.capacity() != 0 {
            let bytes_to_read = TYPE_PLUS_LENGTH_SIZE as usize - self.partial_header.value.capacity();
            let t = self.segment_data.split_to(bytes_to_read);
            self.partial_header.value.put(t.value);
            if let Some(temp_event) = parse_header(&mut self.partial_header) {
                self.partial_event = temp_event;
                self.partial_header = BytePlaceholder::empty(self.segment.clone());
            } else {
                error!("Invalid Header length observed");
                panic!("Invalid Header length");
            }
        }
        // check if a partial event was read the last time.
        if self.partial_event.value.capacity() != 0 {
            let bytes_to_read = self.partial_event.value.capacity() - self.partial_event.value.len();
            if self.segment_data.value.remaining() >= bytes_to_read {
                let t = self.segment_data.split_to(bytes_to_read);
                self.partial_event.value.put(t.value);
                let event = Event {
                    offset_in_segment: self.partial_event.offset_in_segment,
                    value: self.partial_event.value.to_vec(),
                };
                self.partial_event = BytePlaceholder::empty(self.segment.clone());
                Some(event)
            } else {
                debug!("Partial event being returned since there is more to fill");
                self.partial_event.value.put(self.segment_data.value.split());
                None
            }
        } else if let Some(mut event_data) = parse_header(&mut self.segment_data) {
            let bytes_to_read = event_data.value.capacity();
            if bytes_to_read == 0 {
                warn!("Found a header with length as zero");
                return None;
            }
            if self.segment_data.value.remaining() >= bytes_to_read {
                // all the data of the event is already present.
                let t = self.segment_data.split_to(bytes_to_read);
                event_data.value.put(t.value);
                info!(
                    "Event data is {:?} with length {}",
                    event_data,
                    event_data.value.len()
                );
                //Convert to Event and send it.
                let event = Event {
                    offset_in_segment: event_data.offset_in_segment,
                    value: event_data.value.freeze().to_vec(),
                };
                Some(event)
            } else {
                // complete data for a given event is not present in the buffer.
                let t = self.segment_data.split();
                event_data.value.put(t.value);
                debug!(
                    "Partial Event read: Current data read {:?} data_read {} to_read {}",
                    event_data,
                    event_data.value.len(),
                    event_data.value.capacity()
                );
                self.partial_event = event_data;
                None
            }
        } else {
            // partial header was read
            self.partial_header = self.segment_data.split();
            None
        }
    }

    ///
    /// This method reads the header and returns a BytesMut whose size is as big as the event.
    /// If complete header is not present return None.
    ///
    fn read_header(data: &mut BytePlaceholder) -> Option<BytePlaceholder> {
        if data.value.len() >= TYPE_PLUS_LENGTH_SIZE as usize {
            let event_offset = data.offset_in_segment;
            let type_code = data.get_i32();
            assert_eq!(type_code, EventCommand::TYPE_CODE, "Expected EventCommand here.");
            let len = data.get_i32();
            debug!("Event size is {}", len);
            Some(BytePlaceholder {
                segment: data.segment.clone(),
                offset_in_segment: event_offset,
                value: BytesMut::with_capacity(len as usize),
            })
        } else {
            None
        }
    }

    async fn wait_until_empty(&self) {
        while !self.segment_data.value.is_empty() {
            delay_for(Duration::from_millis(100)).await;
        }
    }

    fn is_empty(&self) -> bool {
        self.segment_data.value.is_empty()
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
                self.read_offset = event.offset_in_segment;
                Some(event)
            }
            None => {
                info!("Partial event read by the extract_event method, invoke read again to fetch the complete event.");
                self.reader_meta.insert(self.segment.clone(), self.clone());
                None
            }
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
    fn custom_read_header(data: &mut BytePlaceholder) -> Option<BytePlaceholder> {
        if data.value.remaining() >= 4 {
            let len = data.get_i32();
            Some(BytePlaceholder {
                segment: data.segment.clone(),
                offset_in_segment: 0,
                value: BytesMut::with_capacity(len as usize),
            })
        } else {
            None
        }
    }

    #[tokio::test]
    async fn test_read_events() {
        let (tx, mut rx) = mpsc::channel(1);
        tokio::spawn(generate_multiple_variable_sized_events(tx));
        let mut segment_slice = create_segment_slice();
        let mut event_size: usize = 0;
        loop {
            if segment_slice.is_empty() {
                if let Some(response) = rx.recv().await {
                    segment_slice.segment_data = response;
                } else {
                    break; // All events are sent.
                }
            }

            while let Some(d) = segment_slice.extract_event(custom_read_header) {
                assert!(is_all_same(d.value.as_slice()));
                assert_eq!(event_size + 1, d.value.len(), "Event was missed out");
                event_size += 1;
            }
        }

        assert_eq!(event_size, 10);
    }

    #[tokio::test]
    async fn test_read_partial_events_custom() {
        let (tx, mut rx) = mpsc::channel(1);
        tokio::spawn(generate_multiple_constant_size_events(tx));
        let mut segment_slice = create_segment_slice();

        let mut event_size: usize = 0;
        loop {
            if segment_slice.is_empty() {
                if let Some(response) = rx.recv().await {
                    segment_slice.segment_data = response;
                } else {
                    break; // All events are sent.
                }
            }

            while let Some(d) = segment_slice.extract_event(custom_read_header) {
                assert!(is_all_same(d.value.as_slice()));
                assert_eq!(event_size + 1, d.value.len(), "Event was missed out");
                event_size += 1;
            }
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
                    segment_slice.segment_data = response;
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
                    segment_slice.segment_data = response;
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

    fn create_segment_slice() -> SegmentSlice {
        let segment = ScopedSegment::from("test/test/123");
        let segment_slice = SegmentSlice {
            start_offset: 0,
            segment: segment.clone(),
            read_offset: 0,
            end_offset: i64::MAX,
            segment_data: BytePlaceholder::empty(segment.clone()),
            handle: Handle::current(),
            partial_event: BytePlaceholder::empty(segment.clone()),
            partial_header: BytePlaceholder::empty(segment.clone()),
            reader_meta: Arc::new(CHashMap::new()),
        };
        segment_slice
    }

    ///
    /// Helper method to verify all events read by Segment slice.
    ///
    fn is_all_same<T: Eq>(slice: &[T]) -> bool {
        slice
            .get(0)
            .map(|first| slice.iter().all(|x| x == first))
            .unwrap_or(true)
    }

    // Generate events to simulate Pravega SegmentReadCommand.
    async fn generate_variable_size_events(
        mut tx: Sender<BytePlaceholder>,
        buf_size: usize,
        num_events: usize,
    ) {
        let mut buf = BytesMut::with_capacity(buf_size);
        let segment = ScopedSegment::from("test/test/123");
        let mut offset: i64 = 0;
        for i in 1..num_events {
            let mut data = event_data(i);
            if data.len() < buf.capacity() - buf.len() {
                buf.put(data);
            } else {
                while data.len() > 0 {
                    let free_space = buf.capacity() - buf.len();
                    if free_space == 0 {
                        tx.send(BytePlaceholder {
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
        tx.send(BytePlaceholder {
            segment,
            offset_in_segment: offset,
            value: buf,
        })
        .await
        .unwrap();
    }

    // Generate event data given the lenth of the event.
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
    async fn generate_multiple_constant_size_events(mut tx: Sender<BytePlaceholder>) {
        let mut buf = BytesMut::with_capacity(10);
        let segment = ScopedSegment::from("test/test/123");

        buf.put_i32(1);
        buf.put_u8(b'a');
        buf.put_i32(2);
        buf.put(&b"aa"[..]);
        tx.send(BytePlaceholder {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(3);
        buf.put(&b"aaa"[..]);
        tx.send(BytePlaceholder {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(4);
        buf.put(&b"aaaa"[..]);
        tx.send(BytePlaceholder {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(5);
        buf.put(&b"aaaaa"[..]);
        tx.send(BytePlaceholder {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(6);
        buf.put(&b"aaaaaa"[..]);
        tx.send(BytePlaceholder {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put_i32(7);
        buf.put(&b"aaaaaa"[..]);
        tx.send(BytePlaceholder {
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
        tx.send(BytePlaceholder {
            segment: segment.clone(),
            offset_in_segment: 0,
            value: buf,
        })
        .await
        .unwrap();

        buf = BytesMut::with_capacity(10);
        buf.put(&b"aaa"[..]);
        tx.send(BytePlaceholder {
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
    async fn generate_multiple_variable_sized_events(mut tx: Sender<BytePlaceholder>) {
        for i in 1..11 {
            let mut buf = BytesMut::with_capacity(32);
            buf.put_i32(i); // length.
            for _ in 0..i {
                buf.put(&b"a"[..]);
            }
            if let Err(_) = tx
                .send(BytePlaceholder {
                    segment: ScopedSegment::from("test/test/123"),
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
