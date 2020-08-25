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
use crate::segment_slice::{BytePlaceholder, SegmentSlice, SliceMetadata};
use bytes::BufMut;
use pravega_rust_client_shared::{ScopedSegment, ScopedStream};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{debug, info};

#[derive(new)]
pub struct EventReader {
    stream: ScopedStream,
    factory: Arc<ClientFactoryInternal>,
    slices: Arc<RwLock<HashMap<String, SliceMetadata>>>,
    slice_release_receiver: RwLock<HashMap<String, oneshot::Receiver<bool>>>,
    rx: Receiver<BytePlaceholder>,
    tx: Sender<BytePlaceholder>,
}

impl EventReader {
    pub fn init(stream: ScopedStream, factory: Arc<ClientFactoryInternal>) -> Self {
        let (tx, rx) = mpsc::channel(1);
        // create a reader object.
        // 1. Update the TableSynchronizer indicating a new reader has been added.
        // 2. Fetch the segments assigned for this reader.
        EventReader {
            stream,
            factory,
            slices: Arc::new(RwLock::new(HashMap::new())),
            slice_release_receiver: RwLock::new(HashMap::new()),
            rx,
            tx,
        }
    }

    ///
    /// This method is to simplify testing.
    ///
    pub fn init_event_reader(
        stream: ScopedStream,
        factory: Arc<ClientFactoryInternal>,
        tx: Sender<BytePlaceholder>,
        rx: Receiver<BytePlaceholder>,
        segment_slice_map: Arc<RwLock<HashMap<String, SliceMetadata>>>,
    ) -> Self {
        EventReader {
            stream,
            factory,
            slices: segment_slice_map,
            slice_release_receiver: RwLock::new(HashMap::new()),
            rx,
            tx,
        }
    }

    fn release_segment_at(&mut self, _slice: SegmentSlice, _offset_in_segment: u64) {
        //The above two call this with different offsets.
    }

    ///
    /// This function returns a SegmentSlice from the data received from the SegmentStore(s).
    /// Individual events can be read from the data received using `SegmentSlice.next()`.
    ///
    /// Invoking this function multiple times ensure multiple SegmentSlices corresponding
    /// to different Segments of the stream are received. In-case we receive data for an already
    /// acquired SegmentSlice this method waits until SegmentSlice is completely consumed before
    /// returning the data.
    ///
    pub async fn acquire_segment(&mut self) -> Option<SegmentSlice> {
        if let Some(data) = self.rx.recv().await {
            assert_eq!(
                self.stream,
                ScopedSegment::from(data.segment.as_str()).get_scoped_stream(),
                "Received data from a different segment"
            );

            if let Some(mut slice_meta) = self
                .slices
                .write()
                .expect("RWLock Poisoned")
                .remove(&data.segment)
            {
                // SegmentSlice has not been dished out for consumption, append received data and send it for consumption.
                EventReader::add_data_to_segment_slice(data, &mut slice_meta);
                let (slice_return_tx, slice_return_rx) = oneshot::channel();
                {
                    let mut map = self.slice_release_receiver.write().expect("RWLock Poisoned");
                    map.insert(slice_meta.scoped_segment.clone(), slice_return_rx);
                }

                Some(SegmentSlice {
                    meta: slice_meta,
                    reader_meta: self.slices.clone(),
                    slice_return_tx: Some(slice_return_tx),
                })
            } else {
                // SegmentSlice has already been dished out for consumption.
                debug!("Data received for segment {:?}", &data.segment);
                {
                    let mut map = self.slice_release_receiver.write().expect("RWLock Poisoned");
                    if let Some(receiver) = map.remove(&data.segment) {
                        let _ = receiver.await;
                    }
                }
                let (slice_return_tx, slice_return_rx) = oneshot::channel();
                {
                    let mut map = self.slice_release_receiver.write().expect("RWLock Poisoned");
                    map.insert(data.segment.clone(), slice_return_rx);
                }
                let mut slice_meta = self
                    .slices
                    .write()
                    .expect("RwLock Poisoned")
                    .remove(&data.segment)
                    .expect("segment slice missing");
                EventReader::add_data_to_segment_slice(data, &mut slice_meta);
                Some(SegmentSlice {
                    meta: slice_meta,
                    reader_meta: self.slices.clone(),
                    slice_return_tx: Some(slice_return_tx),
                })
            }
        } else {
            info!("All Segment slices have completed reading from the stream, fetch it from the state synchronizer.");
            None
        }
    }

    fn add_data_to_segment_slice(data: BytePlaceholder, slice: &mut SliceMetadata) {
        if slice.segment_data.value.is_empty() {
            slice.segment_data = data;
        } else {
            slice.segment_data.value.put(data.value); // append to partial data from last read.
        }
    }

    async fn get_next_segment(&mut self) -> (ScopedSegment, i64) {
        // Mock method: Always returns first segment.
        // TODO: This method should check from the ReaderGroupState and return a segment and the corresponding read offset.
        let segments = self
            .factory
            .get_controller_client()
            .get_current_segments(&self.stream)
            .await
            .expect("Failed to talk to controller");
        (segments.get_segment(0.0), 0)
    }
}

#[cfg(test)]
mod tests {
    use crate::client_factory::ClientFactory;
    use crate::event_reader::EventReader;
    use crate::segment_slice::{BytePlaceholder, SegmentSlice, SliceMetadata};
    use bytes::{BufMut, BytesMut};
    use pravega_rust_client_shared::{Scope, ScopedSegment, ScopedStream, Stream};
    use pravega_wire_protocol::client_config::{ClientConfigBuilder, TEST_CONTROLLER_URI};
    use pravega_wire_protocol::commands::{Command, EventCommand};
    use std::collections::HashMap;
    use std::iter;
    use std::sync::{Arc, RwLock};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Sender;
    use tokio::time::{delay_for, Duration};
    use tracing::Level;

    /*
     This test verifies EventReader reads from a stream where only one segment has data while the other segment is empty.
    */
    #[test]
    fn test_read_events_single_segment() {
        const NUM_EVENTS: usize = 100;
        let (tx, rx) = mpsc::channel(1);
        tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
        let cf = ClientFactory::new(
            ClientConfigBuilder::default()
                .controller_uri(TEST_CONTROLLER_URI)
                .build()
                .unwrap(),
        );
        let stream = get_scoped_stream("scope", "test");

        // simulate data being received from Segment store.
        cf.get_runtime_handle().enter(|| {
            tokio::spawn(generate_variable_size_events(
                tx.clone(),
                10,
                NUM_EVENTS,
                0,
                false,
            ));
        });

        // simulate initialization of a Reader
        let map = Arc::new(RwLock::new(HashMap::new()));
        let init_segments = vec![
            create_segment_slice(0, map.clone()),
            create_segment_slice(1, map.clone()),
        ];
        update_segment_slices(&map, init_segments);

        // create a new Event Reader with the segment slice data.
        let mut reader = EventReader::init_event_reader(stream, cf.0.clone(), tx.clone(), rx, map);

        let mut event_count = 0;
        let mut event_size = 0;

        // Attempt to acquire a segment.
        while let Some(mut slice) = cf.get_runtime_handle().block_on(reader.acquire_segment()) {
            loop {
                if let Some(event) = slice.next() {
                    println!("Read event {:?}", event);
                    assert_eq!(event.value.len(), event_size + 1, "Event has been missed");
                    assert!(is_all_same(event.value.as_slice()), "Event has been corrupted");
                    event_size += 1;
                    event_count += 1;
                } else {
                    println!(
                        "Finished reading from segment {:?}, segment is auto released",
                        slice.meta.scoped_segment
                    );
                    break; // try to acquire the next segment.
                }
            }
            if event_count == NUM_EVENTS {
                // all events have been read. Exit test.
                break;
            }
        }
    }

    /*
      This test verifies an EventReader reading from a stream where both the segments are sending data.
    */
    #[test]
    fn test_read_events_multiple_segments() {
        const NUM_EVENTS: usize = 100;
        let (tx, rx) = mpsc::channel(1);
        tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
        let cf = ClientFactory::new(
            ClientConfigBuilder::default()
                .controller_uri(TEST_CONTROLLER_URI)
                .build()
                .unwrap(),
        );
        let stream = get_scoped_stream("scope", "test");

        // simulate data being received from Segment store. 2 async tasks pumping in data.
        cf.get_runtime_handle().enter(|| {
            tokio::spawn(generate_variable_size_events(
                tx.clone(),
                100,
                NUM_EVENTS,
                0,
                false,
            ));
        });
        cf.get_runtime_handle().enter(|| {
            //simulate a delay with data received by this segment.
            tokio::spawn(generate_variable_size_events(
                tx.clone(),
                100,
                NUM_EVENTS,
                1,
                true,
            ));
        });

        // simulate initialization of a Reader
        let map = Arc::new(RwLock::new(HashMap::new()));
        let init_segments = vec![
            create_segment_slice(0, map.clone()),
            create_segment_slice(1, map.clone()),
        ];
        update_segment_slices(&map, init_segments);

        // create a new Event Reader with the segment slice data.
        let mut reader = EventReader::init_event_reader(stream, cf.0.clone(), tx.clone(), rx, map);

        let mut event_count_per_segment: HashMap<String, usize> = HashMap::new();

        let mut total_events_read = 0;
        // Attempt to acquire a segment.
        while let Some(mut slice) = cf.get_runtime_handle().block_on(reader.acquire_segment()) {
            let segment = slice.meta.scoped_segment.clone();
            println!("Received Segment Slice {:?}", segment);
            let mut event_count = 0;
            loop {
                if let Some(event) = slice.next() {
                    println!("Read event {:?}", event);
                    assert!(is_all_same(event.value.as_slice()), "Event has been corrupted");
                    event_count += 1;
                } else {
                    println!(
                        "Finished reading from segment {:?}, segment is auto released",
                        slice.meta.scoped_segment
                    );
                    break; // try to acquire the next segment.
                }
            }
            total_events_read += event_count;
            *event_count_per_segment
                .entry(segment.clone())
                .or_insert(event_count) += event_count;
            if total_events_read == NUM_EVENTS * 2 {
                // all events have been read. Exit test.
                break;
            }
        }
    }

    // Helper method to update slice meta
    fn update_segment_slices(
        map: &Arc<RwLock<HashMap<String, SliceMetadata>>>,
        init_segments: Vec<SegmentSlice>,
    ) {
        let mut slice_map = map.write().expect("RwLock Poisoned");
        for s in init_segments {
            slice_map.insert(s.meta.scoped_segment.clone(), s.meta);
        }
        drop(slice_map); // release write lock.
    }

    fn get_scoped_stream(scope: &str, stream: &str) -> ScopedStream {
        let stream: ScopedStream = ScopedStream {
            scope: Scope {
                name: scope.to_string(),
            },
            stream: Stream {
                name: stream.to_string(),
            },
        };
        stream
    }

    // Generate events to simulate Pravega SegmentReadCommand.
    async fn generate_variable_size_events(
        mut tx: Sender<BytePlaceholder>,
        buf_size: usize,
        num_events: usize,
        segment_id: usize,
        should_delay: bool,
    ) {
        let mut segment_name = "scope/test/".to_owned();
        segment_name.push_str(segment_id.to_string().as_ref());
        let mut buf = BytesMut::with_capacity(buf_size);
        let mut offset: i64 = 0;
        for i in 1..num_events + 1 {
            let mut data = event_data(i);
            if data.len() < buf.capacity() - buf.len() {
                buf.put(data);
            } else {
                while data.len() > 0 {
                    let free_space = buf.capacity() - buf.len();
                    if free_space == 0 {
                        if should_delay {
                            delay_for(Duration::from_millis(100)).await;
                        }
                        tx.send(BytePlaceholder {
                            segment: ScopedSegment::from(segment_name.as_str()).to_string(),
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
            segment: ScopedSegment::from(segment_name.as_str()).to_string(),
            offset_in_segment: offset,
            value: buf,
        })
        .await
        .unwrap();
    }

    //Generate event data given the length of the event. The data is 'a' replicated `len` times.
    fn event_data(len: usize) -> BytesMut {
        let mut buf = BytesMut::with_capacity(len + 8);
        buf.put_i32(EventCommand::TYPE_CODE);
        buf.put_i32(len as i32); // header

        let mut data = Vec::new();
        data.extend(iter::repeat(b'a').take(len));
        buf.put(data.as_slice());
        buf
    }

    // create a segment slice object without spawning a background task.
    fn create_segment_slice(
        segment_id: i64,
        reader_meta: Arc<RwLock<HashMap<String, SliceMetadata>>>,
    ) -> SegmentSlice {
        let mut segment_name = "scope/test/".to_owned();
        segment_name.push_str(segment_id.to_string().as_ref());
        let segment = ScopedSegment::from(segment_name.as_str());
        let segment_slice = SegmentSlice {
            meta: SliceMetadata {
                start_offset: 0,
                scoped_segment: segment.to_string(),
                read_offset: 0,
                end_offset: i64::MAX,
                segment_data: BytePlaceholder::empty(),
                partial_event_length: 0,
                partial_event: BytePlaceholder::empty(),
                partial_header: BytePlaceholder::empty(),
            },
            reader_meta,
            slice_return_tx: None,
        };
        segment_slice
    }

    // Helper method to verify if the bytes read by Segment slice are the same.
    fn is_all_same<T: Eq>(slice: &[T]) -> bool {
        slice
            .get(0)
            .map(|first| slice.iter().all(|x| x == first))
            .unwrap_or(true)
    }
}
