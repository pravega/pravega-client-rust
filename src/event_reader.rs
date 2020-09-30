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
use crate::segment_reader::ReaderError;
use crate::segment_slice::{SegmentDataBuffer, SegmentSlice, SliceMetadata};
use bytes::BufMut;
use im::HashMap as ImHashMap;
use pravega_rust_client_shared::{ScopedSegment, ScopedStream, Segment, SegmentWithRange};
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

pub type SegmentReadResult = Result<SegmentDataBuffer, ReaderError>;

///
/// This represents an event reader. An event readers fetches its assigned segments and provides the
/// following APIs.
/// 1. A method to initialize the event reader [EventReader#init](EventReader#init)
/// 2. A method to obtain the a SegmentSlice to read data from.  A SegmentSlice has data for a given
///Segment. The user can use the SegmentSlice's iterator API to fetch individual events from a given Segment Slice.
/// [EventReader#acquire_segment](EventReader#acquire_segment).
/// 3. A method to release the Segment back at the given offset. [EventReader#release_segment_at](EventReader#release_segment_at).
///    This method needs to be invoked only the user does not consume all the events in a SegmentSlice.
///
/// An example usage pattern is as follows
///
/// ```no_run
/// use pravega_wire_protocol::client_config::{ClientConfigBuilder, TEST_CONTROLLER_URI};
/// use pravega_client_rust::client_factory::ClientFactory;
/// use pravega_rust_client_shared::{ScopedStream, Scope, Stream};
///
/// #[tokio::main]
/// async fn main() {
///    let config = ClientConfigBuilder::default()
///         .controller_uri(TEST_CONTROLLER_URI)
///         .build()
///         .expect("creating config");
///     let client_factory = ClientFactory::new(config);
///     let stream = ScopedStream {
///         scope: Scope::from("scope".to_string()),
///         stream: Stream::from("stream".to_string()),
///     };
///     // Create a reader obtain a segment slice and read events from it.
///     let mut reader = client_factory.create_event_stream_reader(stream).await;
///     // read all events from a given segment slice.
///     if let Some(mut segment_slice) =  reader.acquire_segment().await {
///         while let Some(event) = segment_slice.next() {
///             println!("Event read is {:?}", event);
///         }
///     }
///     // read one event from the a given  segment slice and return it back.
///     if let Some(mut segment_slice) = reader.acquire_segment().await {
///         if let Some(event) = segment_slice.next() {
///             println!("Event read is {:?}", event);
///             // release the segment slice back to the reader.
///             reader.release_segment_at(segment_slice);
///         }
///     }
/// }
///```
///
#[derive(new)]
pub struct EventReader {
    stream: ScopedStream,
    factory: Arc<ClientFactoryInternal>,
    rx: Receiver<SegmentReadResult>,
    tx: Sender<SegmentReadResult>,
    meta: ReaderMeta,
}

/// Reader meta data.
pub struct ReaderMeta {
    slices: HashMap<String, SliceMetadata>,
    future_segments: HashMap<ScopedSegment, HashSet<Segment>>,
    slice_release_receiver: HashMap<String, oneshot::Receiver<SliceMetadata>>,
    slice_stop_reading: HashMap<String, oneshot::Sender<()>>,
}

impl ReaderMeta {
    //
    // Add a release receiver which is used to inform a EventReader when the Segment slice is returned.
    //
    fn add_slice_release_receiver(
        &mut self,
        scoped_segment: String,
        slice_return_rx: oneshot::Receiver<SliceMetadata>,
    ) {
        self.slice_release_receiver
            .insert(scoped_segment, slice_return_rx);
    }

    //
    // Wait until the user application returns the Segment Slice.
    //
    async fn wait_for_segment_slice_return(&mut self, segment: &str) -> SliceMetadata {
        if let Some(receiver) = self.slice_release_receiver.remove(segment) {
            match receiver.await {
                Ok(returned_meta) => {
                    debug!("SegmentSLice returned {:?}", returned_meta);
                    if let Some(meta) = self.slices.remove(segment) {
                        info!("Meta removed for segment {:?}", meta);
                    }
                    returned_meta
                }
                Err(e) => {
                    error!("Error Segment slice was not returned {:?}", e);
                    panic!("A Segment slice was not returned to the Reader.");
                }
            }
        } else {
            panic!("This is unexpected, No receiver for SegmentSlice present.");
        }
    }

    //
    // Remove segment slice from reader meta and return it.
    // If the reader does not have the segment slice it waits for the segment slice which is out
    // for consumption.
    //
    async fn remove_segment(&mut self, segment: String) -> SliceMetadata {
        match self.slices.remove(&segment) {
            Some(meta) => {
                debug!(
                    "Segment slice {:?} has not been dished out for consumption",
                    &segment
                );
                meta
            }
            None => {
                debug!(
                    "Segment slice for {:?} has already been dished out for consumption",
                    &segment
                );
                self.wait_for_segment_slice_return(&segment).await
            }
        }
    }

    //
    // Add Segment Slices to Reader meta data.
    //
    fn add_slices(&mut self, meta: SliceMetadata) {
        if self.slices.insert(meta.scoped_segment.clone(), meta).is_some() {
            panic!("Pre-condition check failure. Segment slice already present");
        }
    }

    //
    // Store a Sender which is used to stop the read task for a given Segment.
    //
    fn add_stop_reading_tx(&mut self, segment: String, tx: oneshot::Sender<()>) {
        if self.slice_stop_reading.insert(segment, tx).is_some() {
            panic!("Pre-condition check failure. Sender used to stop fetching data is already present");
        }
    }

    //
    // Use the stored oneshot::Sender to stop segment reading background task.
    //
    fn stop_reading(&mut self, segment: &str) {
        if let Some(tx) = self.slice_stop_reading.remove(segment) {
            if tx.send(()).is_err() {
                debug!("Channel already closed, ignoring the error");
            }
        }
    }

    //
    // Fetch the next segments that can be read by the Event Reader.
    //
    fn next_segments_to_read(
        &mut self,
        completed_segment: String,
        successors_mapped_to_their_predecessors: ImHashMap<SegmentWithRange, Vec<Segment>>,
    ) -> Vec<ScopedSegment> {
        info!(
            "Completed segments {:?}, successor_to_predecessor: {:?} ",
            completed_segment, successors_mapped_to_their_predecessors
        );
        info!("FutureSegments {:?}", self.future_segments);
        // add missing successors to future_segments
        for (segment, list) in successors_mapped_to_their_predecessors {
            if !self.future_segments.contains_key(&segment.scoped_segment) {
                let required_to_complete = HashSet::from_iter(list.clone().into_iter());
                // update future segments.
                self.future_segments
                    .insert(segment.scoped_segment.to_owned(), required_to_complete);
            }
        }
        debug!("Future Segments after update {:?}", self.future_segments);
        // remove the completed segment from the dependency list
        for required_to_complete in self.future_segments.values_mut() {
            // the hash set needs update
            required_to_complete.remove(&ScopedSegment::from(completed_segment.as_str()).segment);
        }
        debug!(
            "Future Segments after removing the segment which completed. {:?}",
            self.future_segments
        );
        // find successors that are ready to read. A successor is ready to read
        // once all its predecessors are completed.
        self.future_segments
            .iter()
            .filter(|&(_segment, set)| set.is_empty())
            .map(|(segment, _set)| segment.to_owned())
            .collect::<Vec<ScopedSegment>>()
    }

    fn get_segment_id_with_data(&self) -> Option<String> {
        self.slices
            .iter()
            .find_map(|(k, v)| if v.has_events() { Some(k.clone()) } else { None })
    }
}

impl EventReader {
    ///
    /// Initialize the reader. This fetches the initial segments of the stream and spawns background
    /// tasks to start reads from those Segments.
    /// Note: In future the TableSyncrhonizer can be used to fetch the segments assigned to this EventReader.
    ///
    pub async fn init(stream: ScopedStream, factory: Arc<ClientFactoryInternal>) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let slice_meta_map = EventReader::get_slice_meta(&stream, &factory).await;
        let mut stop_reading_map: HashMap<String, oneshot::Sender<()>> = HashMap::new();
        // spawn background fetch tasks.
        slice_meta_map.iter().for_each(|(segment, meta)| {
            let (tx_stop, rx_stop) = oneshot::channel();
            stop_reading_map.insert(segment.clone(), tx_stop);
            factory.get_runtime_handle().enter(|| {
                tokio::spawn(SegmentSlice::get_segment_data(
                    ScopedSegment::from(segment.as_str()),
                    meta.start_offset,
                    tx.clone(),
                    rx_stop,
                    factory.clone(),
                ))
            });
        });

        // initialize the event reader.
        EventReader::init_event_reader(stream, factory, tx, rx, slice_meta_map, stop_reading_map)
    }

    //
    // Fetch slice meta for this Event Reader.
    // At present this data is fetched from the controller. In future this can be read from the Reader group state.
    //
    async fn get_slice_meta(
        stream: &ScopedStream,
        factory: &Arc<ClientFactoryInternal>,
    ) -> HashMap<String, SliceMetadata> {
        let segments = factory
            .get_controller_client()
            .get_head_segments(stream)
            .await
            .expect("Error while fetching  current segments to read from ");
        // slice meta
        let mut slice_meta_map: HashMap<String, SliceMetadata> = HashMap::new();
        slice_meta_map.extend(segments.iter().map(|(seg, offset)| {
            let scoped_segment = ScopedSegment {
                scope: stream.scope.clone(),
                stream: stream.stream.clone(),
                segment: seg.clone(),
            };
            (
                scoped_segment.to_string(),
                SliceMetadata {
                    scoped_segment: scoped_segment.to_string(),
                    start_offset: *offset,
                    ..Default::default()
                },
            )
        }));
        slice_meta_map
    }

    #[doc(hidden)]
    pub fn init_event_reader(
        stream: ScopedStream,
        factory: Arc<ClientFactoryInternal>,
        tx: Sender<SegmentReadResult>,
        rx: Receiver<SegmentReadResult>,
        segment_slice_map: HashMap<String, SliceMetadata>,
        slice_stop_reading: HashMap<String, oneshot::Sender<()>>,
    ) -> Self {
        EventReader {
            stream,
            factory,
            rx,
            tx,
            meta: ReaderMeta {
                slices: segment_slice_map,
                future_segments: HashMap::new(),
                slice_release_receiver: HashMap::new(),
                slice_stop_reading,
            },
        }
    }

    ///
    /// Release a partially read segment slice back to event reader.
    ///
    pub fn release_segment_at(&mut self, slice: SegmentSlice) {
        //stop reading data
        if let Some(tx) = slice.slice_return_tx {
            if let Err(_e) = tx.send(slice.meta.clone()) {
                warn!(
                    "Failed to send segment slice release data for slice {:?}",
                    slice.meta
                );
            }
        } else {
            panic!("This is unexpected, No sender for SegmentSlice present.");
        }
        //update meta data.
        self.meta.add_slices(slice.meta);
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
        // 1.Check if any of the segments have event data
        if let Some(segment_with_data) = self.meta.get_segment_id_with_data() {
            let slice_meta = self.meta.slices.remove(segment_with_data.as_str()).unwrap();
            // Create an one-shot channel to receive SegmentSlice return.
            let (slice_return_tx, slice_return_rx) = oneshot::channel();
            self.meta
                .add_slice_release_receiver(slice_meta.scoped_segment.clone(), slice_return_rx);

            info!(
                "Segment Slice for {:?} is returned for consumption",
                slice_meta.scoped_segment
            );
            Some(SegmentSlice {
                meta: slice_meta,
                slice_return_tx: Some(slice_return_tx),
            })
        } else if let Some(read_result) = self.rx.recv().await {
            match read_result {
                // received segment data
                Ok(data) => {
                    assert_eq!(
                        self.stream,
                        ScopedSegment::from(data.segment.as_str()).get_scoped_stream(),
                        "Received data from a different segment"
                    );
                    let mut slice_meta = self.meta.remove_segment(data.segment.clone()).await;
                    // add received data to Segment slice.
                    EventReader::add_data_to_segment_slice(data, &mut slice_meta);

                    // Create an one-shot channel to receive SegmentSlice return.
                    let (slice_return_tx, slice_return_rx) = oneshot::channel();
                    self.meta
                        .add_slice_release_receiver(slice_meta.scoped_segment.clone(), slice_return_rx);

                    info!(
                        "Segment Slice for {:?} is returned for consumption",
                        slice_meta.scoped_segment
                    );
                    Some(SegmentSlice {
                        meta: slice_meta,
                        slice_return_tx: Some(slice_return_tx),
                    })
                }
                Err(e) => {
                    let segment = e.get_segment();
                    debug!("Reader Error observed {:?} on segment {:?}", e, segment);
                    // Remove the slice from the reader meta and fetch successors.
                    let slice_meta = self.meta.remove_segment(segment.clone()).await;

                    info!("Segment slice {:?} has received error {:?}", slice_meta, e);
                    self.fetch_successors(e).await;

                    debug!("segment Slice meta {:?}", self.meta.slices);
                    None
                }
            }
        } else {
            info!("All Segment slices have completed reading from the stream, fetch it from the state synchronizer.");
            None
        }
    }

    //
    // Fetch successors of the segment where an error was observed.
    // ensure we stop the read task and spawn read tasks for the successor segments.
    // (This logic will be moved to the TableSynchronizer in the subsequent PR).
    async fn fetch_successors(&mut self, e: ReaderError) {
        match e {
            ReaderError::SegmentSealed {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            }
            | ReaderError::SegmentTruncated {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => {
                self.meta.stop_reading(&segment); // stop reading segment.
                                                  // Fetch next segments that can be read from.
                let successors = self.get_successors(&segment).await;
                let s = self.meta.next_segments_to_read(segment, successors);
                debug!("Segments which can be read next are {:?}", s);
                {
                    for seg in s {
                        let meta = SliceMetadata {
                            scoped_segment: seg.to_string(),
                            ..Default::default()
                        };
                        let (tx_drop_fetch, rx_drop_fetch) = oneshot::channel();
                        tokio::spawn(SegmentSlice::get_segment_data(
                            seg.clone(),
                            meta.start_offset,
                            self.tx.clone(),
                            rx_drop_fetch,
                            self.factory.clone(),
                        ));
                        self.meta.add_stop_reading_tx(seg.to_string(), tx_drop_fetch);
                        // update map with newer segments.
                        self.meta.add_slices(meta);
                    }
                }
            }
            _ => error!("Error observed while reading from Pravega {:?}", e),
        };
    }

    // Helper method to append data to SliceMetadata.
    fn add_data_to_segment_slice(data: SegmentDataBuffer, slice: &mut SliceMetadata) {
        if slice.segment_data.value.is_empty() {
            slice.segment_data = data;
        } else {
            slice.segment_data.value.put(data.value); // append to partial data from last read.
        }
    }

    // Fetch the successors for a given segment from the controller.
    async fn get_successors(
        &mut self,
        completed_scoped_segment: &str,
    ) -> ImHashMap<SegmentWithRange, Vec<Segment>> {
        let completed_scoped_segment = ScopedSegment::from(completed_scoped_segment);
        self.factory
            .get_controller_client()
            .get_successors(&completed_scoped_segment)
            .await
            .expect("Failed to fetch successors of the segment")
            .segment_with_predecessors
    }
}

#[cfg(test)]
mod tests {
    use crate::client_factory::ClientFactory;
    use crate::event_reader::{EventReader, SegmentReadResult};
    use crate::segment_slice::{SegmentDataBuffer, SegmentSlice, SliceMetadata};
    use bytes::{BufMut, BytesMut};
    use pravega_rust_client_shared::{Scope, ScopedSegment, ScopedStream, Stream};
    use pravega_wire_protocol::client_config::{ClientConfigBuilder, TEST_CONTROLLER_URI};
    use pravega_wire_protocol::commands::{Command, EventCommand};
    use std::collections::HashMap;
    use std::iter;
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
        let init_segments = vec![create_segment_slice(0), create_segment_slice(1)];

        // create a new Event Reader with the segment slice data.
        let mut reader = EventReader::init_event_reader(
            stream,
            cf.0.clone(),
            tx.clone(),
            rx,
            create_slice_map(init_segments),
            HashMap::new(),
        );

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
        let init_segments = vec![create_segment_slice(0), create_segment_slice(1)];

        // create a new Event Reader with the segment slice data.
        let mut reader = EventReader::init_event_reader(
            stream,
            cf.0.clone(),
            tx.clone(),
            rx,
            create_slice_map(init_segments),
            HashMap::new(),
        );

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

    #[test]
    fn test_return_slice() {
        const NUM_EVENTS: usize = 2;
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
        let init_segments = vec![create_segment_slice(0), create_segment_slice(1)];

        // create a new Event Reader with the segment slice data.
        let mut reader = EventReader::init_event_reader(
            stream,
            cf.0.clone(),
            tx.clone(),
            rx,
            create_slice_map(init_segments),
            HashMap::new(),
        );

        // acquire a segment
        let mut slice = cf
            .get_runtime_handle()
            .block_on(reader.acquire_segment())
            .unwrap();

        // read an event.
        let event = slice.next().unwrap();
        assert_eq!(event.value.len(), 1);
        assert!(is_all_same(event.value.as_slice()), "Event has been corrupted");
        assert_eq!(event.offset_in_segment, 0); // first event.

        // release the segment slice.
        reader.release_segment_at(slice);

        // acquire the next segment
        let slice = cf
            .get_runtime_handle()
            .block_on(reader.acquire_segment())
            .unwrap();
        //Do not read, simply return it back.
        reader.release_segment_at(slice);

        // Try acquiring the segment again.
        let mut slice = cf
            .get_runtime_handle()
            .block_on(reader.acquire_segment())
            .unwrap();
        // Verify a partial event being present. This implies
        let event = slice.next().unwrap();
        assert_eq!(event.value.len(), 2);
        assert!(is_all_same(event.value.as_slice()), "Event has been corrupted");
        assert_eq!(event.offset_in_segment, 8 + 1); // first event.
    }

    fn read_n_events(slice: &mut SegmentSlice, events_to_read: usize) {
        let mut event_count = 0;
        loop {
            if event_count == events_to_read {
                break;
            }
            if let Some(event) = slice.next() {
                println!("Read event {:?}", event);
                assert!(is_all_same(event.value.as_slice()), "Event has been corrupted");
                event_count += 1;
            } else {
                println!(
                    "Finished reading from segment {:?}, segment is auto released",
                    slice.meta.scoped_segment
                );
                break;
            }
        }
    }

    // Helper method to update slice meta
    fn create_slice_map(init_segments: Vec<SegmentSlice>) -> HashMap<String, SliceMetadata> {
        let mut map = HashMap::with_capacity(init_segments.len());
        for s in init_segments {
            map.insert(s.meta.scoped_segment.clone(), s.meta);
        }
        map
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
        mut tx: Sender<SegmentReadResult>,
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
                        tx.send(Ok(SegmentDataBuffer {
                            segment: ScopedSegment::from(segment_name.as_str()).to_string(),
                            offset_in_segment: offset,
                            value: buf,
                        }))
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
        tx.send(Ok(SegmentDataBuffer {
            segment: ScopedSegment::from(segment_name.as_str()).to_string(),
            offset_in_segment: offset,
            value: buf,
        }))
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

    // create a segment slice object without spawning a background task for testing
    fn create_segment_slice(segment_id: i64) -> SegmentSlice {
        let mut segment_name = "scope/test/".to_owned();
        segment_name.push_str(segment_id.to_string().as_ref());
        let segment = ScopedSegment::from(segment_name.as_str());
        let segment_slice = SegmentSlice {
            meta: SliceMetadata {
                start_offset: 0,
                scoped_segment: segment.to_string(),
                read_offset: 0,
                end_offset: i64::MAX,
                segment_data: SegmentDataBuffer::empty(),
                partial_data_present: false,
            },
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
