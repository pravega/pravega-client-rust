//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactoryAsync;
use crate::event::reader_group_state::ReaderGroupStateError::SyncError;
use crate::event::reader_group_state::{Offset, ReaderGroupStateError};
use crate::segment::reader::ReaderError::SegmentSealed;
use crate::segment::reader::{AsyncSegmentReader, ReaderError};
use snafu::{ResultExt, Snafu};

use pravega_client_retry::retry_result::Retryable;
use pravega_client_shared::{Reader, ScopedSegment, Segment, SegmentWithRange};
use pravega_wire_protocol::commands::{Command, EventCommand, TYPE_PLUS_LENGTH_SIZE};

use crate::sync::synchronizer::SynchronizerError;
use bytes::{Buf, BufMut, BytesMut};
use core::fmt;
use im::HashMap as ImHashMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{mpsc, Mutex};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

type ReaderErrorWithOffset = (ReaderError, i64);
type SegmentReadResult = Result<SegmentDataBuffer, ReaderErrorWithOffset>;

const REBALANCE_INTERVAL: Duration = Duration::from_secs(10);

const READ_BUFFER_SIZE: i32 = 8 * 1024 * 1024; // max size for a single Event

cfg_if::cfg_if! {
    if #[cfg(test)] {
        use crate::event::reader_group_state::MockReaderGroupState as ReaderGroupState;
    } else {
        use crate::event::reader_group_state::ReaderGroupState;
    }
}

/// Read events from Stream.
///
/// An event reader fetches data from its assigned segments as a SegmentSlice,
/// where a SegmentSlice represents data from a Pravega Segment. It provides the following APIs.
/// 1. A method to initialize the event reader [EventReader#init](EventReader#init)
/// 2. A method to obtain a SegmentSlice to read events from a Pravega segment. The user can use the
/// SegmentSlice's iterator API to fetch individual events from a given Segment Slice.
/// [EventReader#acquire_segment](EventReader#acquire_segment).
/// 3. A method to release the Segment back at the given offset. [EventReader#release_segment_at](EventReader#release_segment_at).
///    This method needs to be invoked only the user does not consume all the events in a SegmentSlice.
/// 4. A method to mark the reader as offline.[EventReader#reader_offline](EventReader#reader_offline).
///    This method ensures the segments owned by this readers are transferred to other readers
///    in the reader group.
///
/// # Examples
///
/// ```no_run
/// use pravega_client_config::{ClientConfigBuilder, MOCK_CONTROLLER_URI};
/// use pravega_client::client_factory::ClientFactory;
/// use pravega_client_shared::{ScopedStream, Scope, Stream};
///
/// #[tokio::main]
/// async fn main() {
///    let config = ClientConfigBuilder::default()
///         .controller_uri(MOCK_CONTROLLER_URI)
///         .build()
///         .expect("creating config");
///     let client_factory = ClientFactory::new(config);
///     let stream = ScopedStream {
///         scope: Scope::from("scope".to_string()),
///         stream: Stream::from("stream".to_string()),
///     };
///     // Create a reader group to read data from the Pravega stream.
///     let rg = client_factory.create_reader_group("rg".to_string(), stream).await;
///     // Create a reader under the reader group. The segments of the stream are assigned among the
///     // readers which are part of the reader group.
///     let mut reader1 = rg.create_reader("r1".to_string()).await;
///     // read all events from a given segment slice.
///     if let Some(mut segment_slice) =  reader1.acquire_segment().await.expect("Failed to acquire segment since the reader is offline") {
///         while let Some(event) = segment_slice.next() {
///             println!("Event read is {:?}", event);
///         }
///     }
///     // read one event from the a given  segment slice and return it back.
///     if let Some(mut segment_slice) = reader1.acquire_segment().await.expect("Failed to acquire segment since the reader is offline") {
///         if let Some(event) = segment_slice.next() {
///             println!("Event read is {:?}", event);
///             // release the segment slice back to the reader.
///             reader1.release_segment(segment_slice).await;
///         }
///     }
/// }
/// ```
pub struct EventReader {
    pub id: Reader,
    factory: ClientFactoryAsync,
    rx: Receiver<SegmentReadResult>,
    tx: Sender<SegmentReadResult>,
    meta: ReaderState,
    rg_state: Arc<Mutex<ReaderGroupState>>,
}

#[derive(Debug, Snafu)]
pub enum EventReaderError {
    #[snafu(display("ReaderGroup State error: {}", source))]
    StateError { source: ReaderGroupStateError },
}

impl Drop for EventReader {
    /// Destructor for reader invoked. This will automatically invoke reader_offline().
    fn drop(&mut self) {
        info!("Reader {:?} is dropped", self.id);
        // try fetching the currently running Runtime.
        let r = Handle::try_current();
        match r {
            Ok(handle) => {
                // enter the runtime context.
                let _ = handle.enter();
                // ensure we block until the reader_offline method completes.
                let offline_status = futures::executor::block_on(self.reader_offline());
                info!("Reader {:?} is marked as offline. {:?}", self.id, offline_status);
            }
            Err(_) => {
                // ensure we block until the reader_offline executes successfully.
                let rt = tokio::runtime::Runtime::new().expect("Create tokio runtime to drop reader");
                let offline_status = rt.block_on(self.reader_offline());
                info!("Reader {:?} is marked as offline. {:?}", self.id, offline_status);
            }
        }
    }
}

impl EventReader {
    /// Initialize the reader. This fetches the assigned segments from the Synchronizer and
    /// spawns background tasks to start reads from those Segments.
    pub(crate) async fn init_reader(
        id: String,
        rg_state: Arc<Mutex<ReaderGroupState>>,
        factory: ClientFactoryAsync,
    ) -> Self {
        let reader = Reader::from(id);
        let new_segments_to_acquire = rg_state
            .lock()
            .await
            .compute_segments_to_acquire_or_release(&reader)
            .await
            .expect("should compute segments");
        // attempt acquiring the desired number of segments.
        if new_segments_to_acquire > 0 {
            for _ in 0..new_segments_to_acquire {
                if let Some(seg) = rg_state
                    .lock()
                    .await
                    .assign_segment_to_reader(&reader)
                    .await
                    .expect("Error while waiting for segments to be assigned")
                {
                    debug!("Acquiring segment {:?} for reader {:?}", seg, reader);
                } else {
                    // There are no new unassigned segments to be acquired.
                    debug!(
                        "No unassigned segments that can be acquired by the reader {:?}",
                        reader
                    );
                    break;
                }
            }
        }
        // Get all assigned segments for the reader.
        let mut assigned_segments = rg_state
            .lock()
            .await
            .get_segments_for_reader(&reader)
            .await
            .expect("Error while fetching currently assigned segments");

        let mut slice_meta_map: HashMap<ScopedSegment, SliceMetadata> = HashMap::new();
        slice_meta_map.extend(assigned_segments.drain().map(|(seg, offset)| {
            (
                seg.clone(),
                SliceMetadata {
                    scoped_segment: seg.to_string(),
                    start_offset: offset.read,
                    read_offset: offset.read,
                    ..Default::default()
                },
            )
        }));

        let (tx, rx) = mpsc::channel(1);
        let mut stop_reading_map: HashMap<ScopedSegment, oneshot::Sender<()>> = HashMap::new();
        // spawn background fetch tasks.
        slice_meta_map.iter().for_each(|(segment, meta)| {
            let (tx_stop, rx_stop) = oneshot::channel();
            stop_reading_map.insert(segment.clone(), tx_stop);
            factory.runtime_handle().spawn(SegmentSlice::get_segment_data(
                segment.clone(),
                meta.start_offset,
                tx.clone(),
                rx_stop,
                factory.clone(),
            ));
        });

        // initialize the event reader.
        EventReader::init_event_reader(
            rg_state,
            reader,
            factory,
            tx,
            rx,
            slice_meta_map,
            stop_reading_map,
        )
    }

    #[doc(hidden)]
    fn init_event_reader(
        rg_state: Arc<Mutex<ReaderGroupState>>,
        id: Reader,
        factory: ClientFactoryAsync,
        tx: Sender<SegmentReadResult>,
        rx: Receiver<SegmentReadResult>,
        segment_slice_map: HashMap<ScopedSegment, SliceMetadata>,
        slice_stop_reading: HashMap<ScopedSegment, oneshot::Sender<()>>,
    ) -> Self {
        EventReader {
            id,
            factory,
            rx,
            tx,
            meta: ReaderState {
                slices: segment_slice_map,
                slices_dished_out: Default::default(),
                slice_release_receiver: HashMap::new(),
                slice_stop_reading,
                last_segment_release: Instant::now(),
                last_segment_acquire: Instant::now(),
                reader_offline: false,
            },
            rg_state,
        }
    }

    // for testing purposes.
    #[doc(hidden)]
    #[cfg(feature = "integration-test")]
    pub fn set_last_acquire_release_time(&mut self, time: Instant) {
        self.meta.last_segment_release = time;
        self.meta.last_segment_acquire = time;
    }

    /// Release a partially read segment slice back to event reader.
    ///
    /// Note: it may return an error indicating that the reader has already been removed. This means
    /// that another thread removes this reader from the ReaderGroup probably due to the host of this reader
    /// is assumed dead.
    pub async fn release_segment(&mut self, mut slice: SegmentSlice) -> Result<(), EventReaderError> {
        info!(
            "releasing segment slice {} from reader {:?}",
            slice.meta.scoped_segment, self.id
        );
        // check if the reader is already offline.
        if self.meta.reader_offline {
            return Err(EventReaderError::StateError {
                source: ReaderGroupStateError::ReaderAlreadyOfflineError {
                    error_msg: format!(
                        "Reader already marked offline {:?}",
                        self.id
                    ),
                    source: SynchronizerError::SyncPreconditionError {
                        error_msg: String::from("Precondition failure"),
                    },
                },
            });
        }
        //update meta data.
        let scoped_segment = ScopedSegment::from(slice.meta.scoped_segment.clone().as_str());
        self.meta.add_slices(slice.meta.clone());
        self.meta.slices_dished_out.remove(&scoped_segment);
        if self.meta.last_segment_release.elapsed() > REBALANCE_INTERVAL {
            debug!("try to rebalance segments across readers");
            let read_offset = slice.meta.read_offset;
            // Note: reader may not online
            self.release_segment_from_reader(slice, read_offset).await?;
            self.meta.last_segment_release = Instant::now();
        } else {
            //send an indication to the waiting rx that slice has been returned.
            if let Some(tx) = slice.slice_return_tx.take() {
                if let Err(_e) = tx.send(Some(slice.meta.clone())) {
                    warn!(
                        "Failed to send segment slice release data for slice {:?}",
                        slice.meta
                    );
                }
            } else {
                panic!("This is unexpected, No sender for SegmentSlice present.");
            }
        }
        Ok(())
    }

    /// Release a segment back to the reader and also indicate the offset up to which the segment slice is consumed.
    ///
    /// Note: it may return an error indicating that the reader has already been removed. This means
    /// that another thread removes this reader from the ReaderGroup probably due to the host of this reader
    /// is assumed dead.
    pub async fn release_segment_at(
        &mut self,
        slice: SegmentSlice,
        offset: i64,
    ) -> Result<(), EventReaderError> {
        info!(
            "releasing segment slice {} at offset {}",
            slice.meta.scoped_segment, offset
        );
        assert!(
            offset >= 0,
            "the offset where the segment slice is released should be a positive number"
        );
        assert!(
            slice.meta.start_offset <= offset,
            "the offset where the segment slice is released should be greater than the start offset"
        );
        assert!(
            slice.meta.end_offset >= offset,
            "the offset where the segment slice is released should be less than the end offset"
        );
        if self.meta.reader_offline {
            return Err(EventReaderError::StateError {
                source: ReaderGroupStateError::ReaderAlreadyOfflineError {
                    error_msg: format!(
                        "Reader already marked offline {:?} or the ReaderGroup is deleted",
                        self.id
                    ),
                    source: SynchronizerError::SyncPreconditionError {
                        error_msg: String::from("Precondition failure"),
                    },
                },
            });
        }
        let segment = ScopedSegment::from(slice.meta.scoped_segment.as_str());
        if slice.meta.read_offset != offset {
            self.meta.stop_reading(&segment);

            let slice_meta = SliceMetadata {
                start_offset: slice.meta.read_offset,
                scoped_segment: slice.meta.scoped_segment.clone(),
                last_event_offset: slice.meta.last_event_offset,
                read_offset: offset,
                end_offset: slice.meta.end_offset,
                segment_data: SegmentDataBuffer::empty(),
                partial_data_present: false,
            };

            // reinitialize the segment data reactor.
            let (tx_drop_fetch, rx_drop_fetch) = oneshot::channel();
            tokio::spawn(SegmentSlice::get_segment_data(
                segment.clone(),
                slice_meta.read_offset, // start reading from the offset provided.
                self.tx.clone(),
                rx_drop_fetch,
                self.factory.clone(),
            ));
            self.meta.add_stop_reading_tx(segment.clone(), tx_drop_fetch);
            self.meta.add_slices(slice_meta);
            self.meta.slices_dished_out.remove(&segment);
        } else {
            self.release_segment(slice).await?;
        }
        Ok(())
    }

    /// Mark the reader as offline.
    /// This will ensure the segments owned by this reader is distributed to other readers in the ReaderGroup.
    ///
    /// Note: it may return an error indicating that the reader has already been removed. This means
    /// that another thread removes this reader from the ReaderGroup probably due to the host of this reader
    /// is assumed dead.
    pub async fn reader_offline(&mut self) -> Result<(), EventReaderError> {
        if !self.meta.reader_offline && self.rg_state.lock().await.check_online(&self.id).await {
            info!("Putting reader {:?} offline", self.id);
            // stop reading from all the segments.
            self.meta.stop_reading_all();
            // close all slice return Receivers.
            self.meta.close_all_slice_return_channel();
            // use the updated map to return the data.

            let mut offset_map: HashMap<ScopedSegment, Offset> = HashMap::new();
            for (seg, slice_meta) in self.meta.slices_dished_out.drain() {
                offset_map.insert(seg, Offset::new(slice_meta.read_offset));
            }
            for meta in self.meta.slices.values() {
                offset_map.insert(
                    ScopedSegment::from(meta.scoped_segment.as_str()),
                    Offset::new(meta.read_offset),
                );
            }

            match self
                .rg_state
                .lock()
                .await
                .remove_reader(&self.id, offset_map)
                .await
            {
                Ok(()) => {
                    self.meta.reader_offline = true;
                    Ok(())
                }
                Err(e) => match e {
                    ReaderGroupStateError::ReaderAlreadyOfflineError { .. } => {
                        self.meta.reader_offline = true;
                        info!("Reader {:?} is already offline", self.id);
                        Ok(())
                    }
                    state_err => Err(EventReaderError::StateError { source: state_err }),
                },
            }?
        }
        Ok(())
    }

    /// Release the segment of the provided SegmentSlice from the reader. This segment is marked as
    /// unassigned in the reader group state and other reads can acquire it.
    async fn release_segment_from_reader(
        &mut self,
        mut slice: SegmentSlice,
        read_offset: i64,
    ) -> Result<(), EventReaderError> {
        if self.meta.reader_offline {
            return Err(EventReaderError::StateError {
                source: ReaderGroupStateError::ReaderAlreadyOfflineError {
                    error_msg: format!(
                        "Reader already marked offline {:?}",
                        self.id
                    ),
                    source: SynchronizerError::SyncPreconditionError {
                        error_msg: String::from("Precondition failure"),
                    },
                },
            });
        }
        let new_segments_to_release = self
            .rg_state
            .lock()
            .await
            .compute_segments_to_acquire_or_release(&self.id)
            .await
            .map_err(|err| {
                EventReaderError::StateError { source: err }
            })?;
        let segment = ScopedSegment::from(slice.meta.scoped_segment.as_str());
        // check if segments needs to be released from the reader
        if new_segments_to_release < 0 {
            // Stop reading from the segment.
            self.meta.stop_reading(&segment);
            self.meta
                .slices
                .remove(&segment)
                .expect("Segment missing in meta while releasing from reader");
            // Send None to the waiting slice_return_rx.
            if let Some(tx) = slice.slice_return_tx.take() {
                if let Err(_e) = tx.send(None) {
                    warn!(
                        "Failed to send segment slice release data for slice {:?}",
                        slice.meta
                    );
                }
            } else {
                panic!("This is unexpected, No sender for SegmentSlice present.");
            }
            self.rg_state
                .lock()
                .await
                .release_segment(&self.id, &segment, &Offset::new(read_offset))
                .await
                .context(StateError {})?;
        }
        Ok(())
    }

    /// This function returns a SegmentSlice from the data received from the SegmentStore(s).
    /// Individual events can be read from the data received using `SegmentSlice.next()`.
    ///
    /// Invoking this function multiple times ensure multiple SegmentSlices corresponding
    /// to different Segments of the stream are received. In-case we receive data for an already
    /// acquired SegmentSlice this method waits until SegmentSlice is completely consumed before
    /// returning the data.
    ///
    /// Note: it may return an error indicating that the reader is not online. This means
    /// that another thread removes this reader from the ReaderGroup probably because the host of this reader
    /// is assumed dead.
    pub async fn acquire_segment(&mut self) -> Result<Option<SegmentSlice>, EventReaderError> {
        if self.meta.reader_offline || !self.rg_state.lock().await.check_online(&self.id).await {
            return Err(EventReaderError::StateError {
                source: ReaderGroupStateError::ReaderAlreadyOfflineError {
                    error_msg: format!(
                        "Reader already marked offline {:?} or the ReaderGroup is deleted",
                        self.id
                    ),
                    source: SynchronizerError::SyncPreconditionError {
                        error_msg: String::from("Precondition failure"),
                    },
                },
            });
        }
        info!("acquiring segment for reader {:?}", self.id);
        // Check if newer segments should be acquired.
        if self.meta.last_segment_acquire.elapsed() > REBALANCE_INTERVAL {
            info!("need to rebalance segments across readers");
            // assign newer segments to this reader if available.
            // Note: reader may not online.
            let res = self.assign_segments_to_reader().await.context(StateError {})?;
            if let Some(new_segments) = res {
                // fetch current segments.
                // Note: reader may not online.
                let current_segments = self
                    .rg_state
                    .lock()
                    .await
                    .get_segments_for_reader(&self.id)
                    .await
                    .map_err(|e| SyncError {
                        error_msg: format!("failed to get segments for reader {:?}", self.id),
                        source: e,
                    })
                    .context(StateError {})?;
                let new_segments: HashSet<(ScopedSegment, Offset)> = current_segments
                    .into_iter()
                    .filter(|(seg, _off)| new_segments.contains(seg))
                    .collect();
                debug!("segments which can be read next are {:?}", new_segments);
                // Initiate segment reads to the newer segments.
                self.initiate_segment_reads(new_segments);
                self.meta.last_segment_acquire = Instant::now();
            }
        }
        // Check if any of the segments already has event data and return it.
        if let Some(segment_with_data) = self.meta.get_segment_id_with_data() {
            info!("segment {} has data ready to read", segment_with_data);
            let slice_meta = self.meta.slices.remove(&segment_with_data).unwrap();
            let segment = ScopedSegment::from(slice_meta.scoped_segment.as_str());
            // Create an one-shot channel to receive SegmentSlice return.
            let (slice_return_tx, slice_return_rx) = oneshot::channel();
            self.meta.add_slice_release_receiver(segment, slice_return_rx);

            info!(
                "segment slice for {:?} is ready for consumption by reader {}",
                slice_meta.scoped_segment, self.id,
            );
            self.meta
                .slices_dished_out
                .insert(segment_with_data, slice_meta.copy_meta());
            Ok(Some(SegmentSlice {
                meta: slice_meta,
                slice_return_tx: Some(slice_return_tx),
            }))
        } else if let Ok(option) = timeout(Duration::from_millis(1000), self.rx.recv()).await {
            if let Some(read_result) = option {
                match read_result {
                    // received segment data
                    Ok(data) => {
                        let segment = ScopedSegment::from(data.segment.clone().as_str());
                        info!("new data fetched from server for segment {:?}", segment);
                        if let Some(mut slice_meta) = self.meta.remove_segment(segment.clone()).await {
                            if data.offset_in_segment
                                != slice_meta.read_offset + slice_meta.segment_data.value.len() as i64
                            {
                                info!("Data from an invalid offset {:?} observed. Expected offset {:?}. Ignoring this data", data.offset_in_segment, slice_meta.read_offset);
                                Ok(None)
                            } else {
                                // add received data to Segment slice.
                                EventReader::add_data_to_segment_slice(data, &mut slice_meta);

                                // Create an one-shot channel to receive SegmentSlice return.
                                let (slice_return_tx, slice_return_rx) = oneshot::channel();
                                self.meta
                                    .add_slice_release_receiver(segment.clone(), slice_return_rx);
                                self.meta
                                    .slices_dished_out
                                    .insert(segment.clone(), slice_meta.copy_meta());

                                info!(
                                    "segment slice for {:?} is ready for consumption by reader {}",
                                    slice_meta.scoped_segment, self.id,
                                );

                                Ok(Some(SegmentSlice {
                                    meta: slice_meta,
                                    slice_return_tx: Some(slice_return_tx),
                                }))
                            }
                        } else {
                            //None is sent if the the segment is released from the reader.
                            debug!("ignore the received data since None was returned");
                            Ok(None)
                        }
                    }
                    Err((e, offset)) => {
                        let segment = ScopedSegment::from(e.get_segment().as_str());
                        debug!(
                            "Reader Error observed {:?} on segment {:?} at offset {:?} ",
                            e, segment, offset
                        );
                        // Remove the slice from the reader meta and fetch successors.
                        if let Some(slice_meta) = self.meta.remove_segment(segment.clone()).await {
                            if slice_meta.read_offset != offset {
                                info!("Error at an invalid offset {:?} observed. Expected offset {:?}. Ignoring this data", offset, slice_meta.start_offset);
                                self.meta.add_slices(slice_meta);
                                self.meta.slices_dished_out.remove(&segment);
                            } else {
                                info!("Segment slice {:?} has received error {:?}", slice_meta, e);
                                self.fetch_successors(e).await.context(StateError {})?;
                            }
                        }
                        debug!("Segment Slice meta {:?}", self.meta.slices);
                        Ok(None)
                    }
                }
            } else {
                warn!("Error getting updates from segment slice for reader {}", self.id);
                Ok(None)
            }
        } else {
            info!(
                "reader {} owns {} slices but none is ready to read",
                self.id,
                self.meta.slices.len()
            );
            Ok(None)
        }
    }

    // Fetch successors of the segment where an error was observed.
    // ensure we stop the read task and spawn read tasks for the successor segments.
    async fn fetch_successors(&mut self, e: ReaderError) -> Result<(), ReaderGroupStateError> {
        match e {
            ReaderError::SegmentSealed {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            }
            | ReaderError::SegmentIsTruncated {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => {
                let completed_scoped_segment = ScopedSegment::from(segment.as_str());
                self.meta.stop_reading(&completed_scoped_segment); // stop reading segment.

                // Fetch next segments that can be read from.
                let successors = self
                    .factory
                    .controller_client()
                    .get_successors(&completed_scoped_segment)
                    .await
                    .expect("Failed to fetch successors of the segment")
                    .segment_with_predecessors;
                info!("Segment Completed {:?}", segment);
                // Update rg_state with the completed segment and its successors.
                self.rg_state
                    .lock()
                    .await
                    .segment_completed(&self.id, &completed_scoped_segment, &successors)
                    .await?;
                // Assign newer segments to this reader if available.
                let option = self.assign_segments_to_reader().await?;
                if let Some(new_segments) = option {
                    // fetch current segments.
                    let current_segments = self
                        .rg_state
                        .lock()
                        .await
                        .get_segments_for_reader(&self.id)
                        .await
                        .map_err(|e| SyncError {
                            error_msg: format!("Failed to fetch segments for reader {:?}", self.id),
                            source: e,
                        })?;
                    let new_segments: HashSet<(ScopedSegment, Offset)> = current_segments
                        .into_iter()
                        .filter(|(seg, _off)| new_segments.contains(seg))
                        .collect();
                    debug!("Segments which can be read next are {:?}", new_segments);
                    // Initiate segment reads to the newer segments.
                    self.initiate_segment_reads(new_segments);
                }
            }
            _ => error!("Error observed while reading from Pravega {:?}", e),
        };
        Ok(())
    }

    // This function tries to acquire newer segments for the reader.
    async fn assign_segments_to_reader(&self) -> Result<Option<Vec<ScopedSegment>>, ReaderGroupStateError> {
        let mut new_segments: Vec<ScopedSegment> = Vec::new();
        let new_segments_to_acquire = self
            .rg_state
            .lock()
            .await
            .compute_segments_to_acquire_or_release(&self.id)
            .await
            .expect("should compute segments");
        if new_segments_to_acquire <= 0 {
            Ok(None)
        } else {
            for _ in 0..new_segments_to_acquire {
                if let Some(seg) = self
                    .rg_state
                    .lock()
                    .await
                    .assign_segment_to_reader(&self.id)
                    .await?
                {
                    debug!("Acquiring segment {:?} for reader {:?}", seg, self.id);
                    new_segments.push(seg);
                } else {
                    // There are no new unassigned segments to be acquired.
                    break;
                }
            }
            debug!("Segments acquired by reader {:?} are {:?}", self.id, new_segments);
            Ok(Some(new_segments))
        }
    }

    /// Initiate a task to read data from the newly assigned segments.
    fn initiate_segment_reads(&mut self, new_segments: HashSet<(ScopedSegment, Offset)>) {
        for (seg, offset) in new_segments {
            let meta = SliceMetadata {
                scoped_segment: seg.to_string(),
                start_offset: offset.read,
                read_offset: offset.read, // read offset should be same as start_offset.
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
            self.meta.add_stop_reading_tx(seg, tx_drop_fetch);
            // update map with newer segments.
            self.meta.add_slices(meta);
        }
    }

    // Helper method to append data to SliceMetadata.
    fn add_data_to_segment_slice(data: SegmentDataBuffer, slice: &mut SliceMetadata) {
        if slice.segment_data.value.is_empty() {
            slice.segment_data = data;
        } else {
            slice.segment_data.value.put(data.value); // append to partial data from last read.
            slice.partial_data_present = false;
        }
    }

    // Fetch the successors for a given segment from the controller.
    async fn get_successors(
        &mut self,
        completed_scoped_segment: &str,
    ) -> ImHashMap<SegmentWithRange, Vec<Segment>> {
        let completed_scoped_segment = ScopedSegment::from(completed_scoped_segment);
        self.factory
            .controller_client()
            .get_successors(&completed_scoped_segment)
            .await
            .expect("Failed to fetch successors of the segment")
            .segment_with_predecessors
    }
}

// Reader meta data.
struct ReaderState {
    slices: HashMap<ScopedSegment, SliceMetadata>,
    slices_dished_out: HashMap<ScopedSegment, SliceMetadata>,
    slice_release_receiver: HashMap<ScopedSegment, oneshot::Receiver<Option<SliceMetadata>>>,
    slice_stop_reading: HashMap<ScopedSegment, oneshot::Sender<()>>,
    last_segment_release: Instant,
    last_segment_acquire: Instant,
    reader_offline: bool,
}

impl ReaderState {
    // Add a release receiver which is used to inform a EventReader when the Segment slice is returned.
    fn add_slice_release_receiver(
        &mut self,
        scoped_segment: ScopedSegment,
        slice_return_rx: oneshot::Receiver<Option<SliceMetadata>>,
    ) {
        self.slice_release_receiver
            .insert(scoped_segment, slice_return_rx);
    }

    // Wait until the user application returns the Segment Slice.
    async fn wait_for_segment_slice_return(&mut self, segment: &ScopedSegment) -> Option<SliceMetadata> {
        if let Some(receiver) = self.slice_release_receiver.remove(segment) {
            match receiver.await {
                Ok(returned_meta) => {
                    debug!("SegmentSlice returned {:?}", returned_meta);
                    returned_meta
                }
                Err(e) => {
                    error!(
                        "Error Segment slice was not returned for segment {:?}. Error {:?} ",
                        segment, e
                    );
                    self.slices_dished_out.remove(segment)
                }
            }
        } else {
            warn!(
                "Invalid segment {:?} provided for while waiting for segment slice return",
                segment
            );
            None
        }
    }

    fn close_all_slice_return_channel(&mut self) {
        for (_, mut rx) in self.slice_release_receiver.drain() {
            rx.close();
        }
    }

    // Remove segment slice from reader meta and return it.
    // If the reader does not have the segment slice it waits for the segment slice which is out
    // for consumption.
    async fn remove_segment(&mut self, segment: ScopedSegment) -> Option<SliceMetadata> {
        match self.slices.remove(&segment) {
            Some(meta) => {
                debug!(
                    "Segment slice {:?} has not been dished out for consumption",
                    &segment
                );
                Some(meta)
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

    // Add Segment Slices to Reader meta data.
    fn add_slices(&mut self, meta: SliceMetadata) {
        if self
            .slices
            .insert(ScopedSegment::from(meta.scoped_segment.as_str()), meta)
            .is_some()
        {
            panic!("Pre-condition check failure. Segment slice already present");
        }
    }

    // Store a Sender which is used to stop the read task for a given Segment.
    fn add_stop_reading_tx(&mut self, segment: ScopedSegment, tx: oneshot::Sender<()>) {
        if self.slice_stop_reading.insert(segment, tx).is_some() {
            panic!("Pre-condition check failure. Sender used to stop fetching data is already present");
        }
    }

    // Use the stored oneshot::Sender to stop segment reading background task.
    fn stop_reading(&mut self, segment: &ScopedSegment) {
        if let Some(tx) = self.slice_stop_reading.remove(segment) {
            if tx.send(()).is_err() {
                debug!("Channel already closed, ignoring the error");
            }
        }
    }

    // Stop all the background tasks that are trying to read from owned segments.
    fn stop_reading_all(&mut self) {
        for (_, tx) in self.slice_stop_reading.drain() {
            if tx.send(()).is_err() {
                debug!("Channel already closed, ignoring the error");
            }
        }
    }

    fn get_segment_id_with_data(&self) -> Option<ScopedSegment> {
        self.slices
            .iter()
            .find_map(|(k, v)| if v.has_events() { Some(k.clone()) } else { None })
    }
}

/// This represents an event that was read from a Pravega Segment and the offset at which the event
/// was read from.
#[derive(Debug)]
pub struct Event {
    pub offset_in_segment: i64,
    pub value: Vec<u8>,
}

/// This represents a segment slice which can be used to read events from a Pravega segment as an
/// iterator.
#[derive(Default)]
pub struct SegmentSlice {
    pub meta: SliceMetadata,
    pub(crate) slice_return_tx: Option<oneshot::Sender<Option<SliceMetadata>>>,
}

impl SegmentSlice {
    /// Create a new SegmentSlice for a given start_offset, segment.
    /// This spawns an asynchronous task to fetch data from the segment with length of  `READ_BUFFER_SIZE`.
    /// The channel buffer size is 1 which ensure only one outstanding read request to Segment store.
    fn new(
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

    // Method to fetch data from the Segment store from a given start offset.
    async fn get_segment_data(
        segment: ScopedSegment,
        start_offset: i64,
        tx: Sender<SegmentReadResult>,
        mut drop_fetch: oneshot::Receiver<()>,
        factory: ClientFactoryAsync,
    ) {
        let mut offset: i64 = start_offset;
        let segment_reader = factory.create_async_segment_reader(segment.clone()).await;
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
                    debug!("read data length of {}", len);
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

    // Return the starting offset of the SegmentSlice.
    fn get_starting_offset(&self) -> i64 {
        self.meta.start_offset
    }

    // Return the segment associated with the SegmentSlice.
    fn get_segment(&self) -> String {
        //Returns the name of the segment
        self.meta.scoped_segment.clone()
    }

    // Extract the next event from the data received from the Segment store.
    // Note: This returns a copy of the data received.
    // Return None in case of a Partial data.
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

    // This method reads the header and returns a BytesMut whose size is as big as the event.
    // If complete header is not present return None.
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

impl fmt::Debug for SegmentSlice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentSlice").field("meta", &self.meta).finish()
    }
}

// Ensure a Drop of Segment slice releases the segment back to the reader group.
impl Drop for SegmentSlice {
    fn drop(&mut self) {
        if let Some(sender) = self.slice_return_tx.take() {
            let _ = sender.send(Some(self.meta.clone()));
        }
    }
}

// This holds the SegmentSlice metadata. This meta is persisted by the EventReader.
#[derive(Clone)]
pub struct SliceMetadata {
    pub start_offset: i64,
    pub scoped_segment: String,
    pub last_event_offset: i64,
    pub read_offset: i64,
    pub end_offset: i64,
    segment_data: SegmentDataBuffer,
    pub partial_data_present: bool,
}

impl SliceMetadata {
    /// Method to check if the slice has partial data.
    fn is_empty(&self) -> bool {
        self.segment_data.value.is_empty()
    }

    /// Method to verify if the Segment has pending events that can be read.
    pub fn has_events(&self) -> bool {
        !self.partial_data_present && self.segment_data.value.len() > TYPE_PLUS_LENGTH_SIZE as usize
    }

    fn copy_meta(&self) -> SliceMetadata {
        SliceMetadata {
            start_offset: self.start_offset,
            scoped_segment: self.scoped_segment.clone(),
            last_event_offset: self.last_event_offset,
            read_offset: self.read_offset,
            end_offset: self.end_offset,
            segment_data: SegmentDataBuffer::empty(),
            partial_data_present: false,
        }
    }
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

// Structure to track the offset and byte array.
#[derive(Clone)]
struct SegmentDataBuffer {
    pub(crate) segment: String,
    pub(crate) offset_in_segment: i64,
    pub(crate) value: BytesMut,
}

impl fmt::Debug for SegmentDataBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentDataBuffer")
            .field("segment", &self.segment)
            .field("offset in segment", &self.offset_in_segment)
            .field("buffer length", &self.value.len())
            .finish()
    }
}

impl SegmentDataBuffer {
    /// Removes the bytes from the current view, returning them in a new `SegmentDataBuffer` handle.
    /// Afterwards, `self` will be empty.
    /// This is identical to `self.split_to(length)`.
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

    /// Gets a signed 32 bit integer from `self` in big-endian byte order.
    ///
    /// The offset in segment is advanced by 4.
    ///
    /// # Panics
    ///
    /// This function panics if there is not enough remaining data in `self`.
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

    /// Returns an empty SegmentDataBuffer. The offset is set as 0.
    pub fn empty() -> SegmentDataBuffer {
        SegmentDataBuffer {
            segment: Default::default(),
            offset_in_segment: 0,
            value: BytesMut::with_capacity(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_factory::ClientFactory;
    use crate::event::reader_group_state::ReaderGroupStateError;
    use crate::sync::synchronizer::SynchronizerError;

    use bytes::{Buf, BufMut, BytesMut};
    use mockall::predicate;
    use mockall::predicate::*;
    use pravega_client_config::{ClientConfigBuilder, MOCK_CONTROLLER_URI};
    use pravega_client_shared::{Reader, Scope, ScopedSegment, ScopedStream, Stream};
    use pravega_wire_protocol::commands::{Command, EventCommand};
    use std::collections::HashMap;
    use std::iter;
    use std::sync::Arc;
    use tokio::sync::mpsc::Sender;
    use tokio::sync::oneshot;
    use tokio::sync::oneshot::error::TryRecvError;
    use tokio::sync::{mpsc, Mutex};
    use tokio::time::{sleep, Duration};
    use tracing::Level;

    // This test verifies EventReader reads from a stream where only one segment has data while the other segment is empty.
    #[test]
    fn test_read_events_single_segment() {
        const NUM_EVENTS: usize = 100;
        let (tx, rx) = mpsc::channel(1);
        tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
        let cf = ClientFactory::new(
            ClientConfigBuilder::default()
                .controller_uri(MOCK_CONTROLLER_URI)
                .build()
                .unwrap(),
        );

        // simulate data being received from Segment store.
        let _guard = cf.runtime().enter();
        tokio::spawn(generate_variable_size_events(
            tx.clone(),
            10,
            NUM_EVENTS,
            0,
            false,
        ));

        // simulate initialization of a Reader
        let init_segments = vec![create_segment_slice(0), create_segment_slice(1)];
        let mut rg_mock: ReaderGroupState = ReaderGroupState::default();
        rg_mock.expect_check_online().return_const(true);
        rg_mock
            .expect_compute_segments_to_acquire_or_release()
            .return_once(move |_| Ok(0 as isize));
        rg_mock.expect_remove_reader().return_once(move |_, _| Ok(()));
        // create a new Event Reader with the segment slice data.
        let mut reader = EventReader::init_event_reader(
            Arc::new(Mutex::new(rg_mock)),
            Reader::from("r1".to_string()),
            cf.to_async(),
            tx.clone(),
            rx,
            create_slice_map(init_segments),
            HashMap::new(),
        );

        let mut event_count = 0;
        let mut event_size = 0;

        // Attempt to acquire a segment.
        while let Some(mut slice) = cf.runtime().block_on(reader.acquire_segment()).unwrap() {
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

    #[test]
    fn test_acquire_segments() {
        const NUM_EVENTS: usize = 10;
        let (tx, rx) = mpsc::channel(1);
        tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
        let cf = ClientFactory::new(
            ClientConfigBuilder::default()
                .controller_uri(MOCK_CONTROLLER_URI)
                .build()
                .unwrap(),
        );

        // simulate data being received from Segment store.
        let _guard = cf.runtime().enter();
        tokio::spawn(generate_variable_size_events(
            tx.clone(),
            1024,
            NUM_EVENTS,
            0,
            false,
        ));

        // simulate initialization of a Reader
        let init_segments = vec![create_segment_slice(0)];
        let mut rg_mock: ReaderGroupState = ReaderGroupState::default();
        rg_mock
            .expect_compute_segments_to_acquire_or_release()
            .with(predicate::eq(Reader::from("r1".to_string())))
            .return_once(move |_| Ok(1 as isize));
        rg_mock.expect_remove_reader().return_once(move |_, _| Ok(()));
        rg_mock.expect_check_online().return_const(true);

        // mock rg_state.assign_segment_to_reader
        let res: Result<Option<ScopedSegment>, ReaderGroupStateError> =
            Ok(Some(ScopedSegment::from("scope/test/1.#epoch.0")));
        rg_mock
            .expect_assign_segment_to_reader()
            .with(predicate::eq(Reader::from("r1".to_string())))
            .return_once(move |_| res);
        //mock rg_state get_segments for reader
        let mut new_current_segments: HashSet<(ScopedSegment, Offset)> = HashSet::new();
        new_current_segments.insert((ScopedSegment::from("scope/test/1.#epoch.0"), Offset::new(0)));
        new_current_segments.insert((ScopedSegment::from("scope/test/0.#epoch.0"), Offset::new(0)));
        let res: Result<HashSet<(ScopedSegment, Offset)>, SynchronizerError> = Ok(new_current_segments);
        rg_mock
            .expect_get_segments_for_reader()
            .with(predicate::eq(Reader::from("r1".to_string())))
            .return_once(move |_| res);

        // simulate data being received from Segment store.
        tokio::spawn(generate_variable_size_events(
            tx.clone(),
            1024,
            NUM_EVENTS,
            1,
            false,
        ));

        let before_time = Instant::now() - Duration::from_secs(15);
        // create a new Event Reader with the segment slice data.
        let mut reader = EventReader::init_event_reader(
            Arc::new(Mutex::new(rg_mock)),
            Reader::from("r1".to_string()),
            cf.to_async(),
            tx.clone(),
            rx,
            create_slice_map(init_segments),
            HashMap::new(),
        );
        reader.set_last_acquire_release_time(before_time);

        let mut event_count = 0;

        // Attempt to acquire a segment.
        while let Some(mut slice) = cf.runtime().block_on(reader.acquire_segment()).unwrap() {
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
            if event_count == NUM_EVENTS + NUM_EVENTS {
                // all events have been read. Exit test.
                break;
            }
        }
        assert_eq!(event_count, NUM_EVENTS + NUM_EVENTS);
    }

    // This test verifies an EventReader reading from a stream where both the segments are sending data.
    #[test]
    fn test_read_events_multiple_segments() {
        const NUM_EVENTS: usize = 100;
        let (tx, rx) = mpsc::channel(1);
        tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
        let cf = ClientFactory::new(
            ClientConfigBuilder::default()
                .controller_uri(MOCK_CONTROLLER_URI)
                .build()
                .unwrap(),
        );

        // simulate data being received from Segment store. 2 async tasks pumping in data.
        let _guard = cf.runtime().enter();
        tokio::spawn(generate_variable_size_events(
            tx.clone(),
            100,
            NUM_EVENTS,
            0,
            false,
        ));
        //simulate a delay with data received by this segment.
        tokio::spawn(generate_variable_size_events(
            tx.clone(),
            100,
            NUM_EVENTS,
            1,
            true,
        ));

        // simulate initialization of a Reader
        let init_segments = vec![create_segment_slice(0), create_segment_slice(1)];
        let mut rg_mock: ReaderGroupState = ReaderGroupState::default();
        rg_mock
            .expect_compute_segments_to_acquire_or_release()
            .return_once(move |_| Ok(0 as isize));
        rg_mock.expect_check_online().return_const(true);
        rg_mock.expect_remove_reader().return_once(move |_, _| Ok(()));
        // create a new Event Reader with the segment slice data.
        let mut reader = EventReader::init_event_reader(
            Arc::new(Mutex::new(rg_mock)),
            Reader::from("r1".to_string()),
            cf.to_async(),
            tx.clone(),
            rx,
            create_slice_map(init_segments),
            HashMap::new(),
        );

        let mut event_count_per_segment: HashMap<String, usize> = HashMap::new();

        let mut total_events_read = 0;
        // Attempt to acquire a segment.
        while let Some(mut slice) = cf.runtime().block_on(reader.acquire_segment()).unwrap() {
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
                .controller_uri(MOCK_CONTROLLER_URI)
                .build()
                .unwrap(),
        );

        // simulate data being received from Segment store.
        let _guard = cf.runtime().enter();
        tokio::spawn(generate_variable_size_events(
            tx.clone(),
            10,
            NUM_EVENTS,
            0,
            false,
        ));

        // simulate initialization of a Reader
        let init_segments = vec![create_segment_slice(0), create_segment_slice(1)];

        let mut rg_mock: ReaderGroupState = ReaderGroupState::default();
        rg_mock.expect_check_online().return_const(true);
        rg_mock
            .expect_compute_segments_to_acquire_or_release()
            .return_once(move |_| Ok(0 as isize));
        rg_mock.expect_remove_reader().return_once(move |_, _| Ok(()));
        // create a new Event Reader with the segment slice data.
        let mut reader = EventReader::init_event_reader(
            Arc::new(Mutex::new(rg_mock)),
            Reader::from("r1".to_string()),
            cf.to_async(),
            tx.clone(),
            rx,
            create_slice_map(init_segments),
            HashMap::new(),
        );

        // acquire a segment
        let mut slice = cf
            .runtime()
            .block_on(reader.acquire_segment())
            .expect("Failed to acquire segment since the reader is offline")
            .unwrap();

        // read an event.
        let event = slice.next().unwrap();
        assert_eq!(event.value.len(), 1);
        assert!(is_all_same(event.value.as_slice()), "Event has been corrupted");
        assert_eq!(event.offset_in_segment, 0); // first event.

        // release the segment slice.
        let _ = cf.runtime().block_on(reader.release_segment(slice));

        // acquire the next segment
        let slice = cf
            .runtime()
            .block_on(reader.acquire_segment())
            .expect("Failed to acquire segment since the reader is offline")
            .unwrap();

        //Do not read, simply return it back.
        let _ = cf.runtime().block_on(reader.release_segment(slice));

        // Try acquiring the segment again.
        let mut slice = cf
            .runtime()
            .block_on(reader.acquire_segment())
            .expect("Failed to acquire segment")
            .unwrap();
        // Verify a partial event being present. This implies
        let event = slice.next().unwrap();
        assert_eq!(event.value.len(), 2);
        assert!(is_all_same(event.value.as_slice()), "Event has been corrupted");
        assert_eq!(event.offset_in_segment, 8 + 1); // first event.
    }

    #[test]
    fn test_return_slice_at_offset() {
        const NUM_EVENTS: usize = 2;
        let (tx, rx) = mpsc::channel(1);
        let (stop_tx, stop_rx) = oneshot::channel();
        tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
        let cf = ClientFactory::new(
            ClientConfigBuilder::default()
                .controller_uri(MOCK_CONTROLLER_URI)
                .build()
                .unwrap(),
        );

        // simulate data being received from Segment store.
        let _guard = cf.runtime().enter();
        tokio::spawn(generate_constant_size_events(
            tx.clone(),
            20,
            NUM_EVENTS,
            0,
            false,
            stop_rx,
        ));
        let mut stop_reading_map: HashMap<ScopedSegment, oneshot::Sender<()>> = HashMap::new();
        stop_reading_map.insert(ScopedSegment::from("scope/test/0.#epoch.0"), stop_tx);

        // simulate initialization of a Reader
        let init_segments = vec![create_segment_slice(0), create_segment_slice(1)];
        let mut rg_mock: ReaderGroupState = ReaderGroupState::default();
        rg_mock.expect_check_online().return_const(true);
        rg_mock
            .expect_compute_segments_to_acquire_or_release()
            .return_once(move |_| Ok(0 as isize));
        rg_mock.expect_remove_reader().return_once(move |_, _| Ok(()));
        // create a new Event Reader with the segment slice data.
        let mut reader = EventReader::init_event_reader(
            Arc::new(Mutex::new(rg_mock)),
            Reader::from("r1".to_string()),
            cf.to_async(),
            tx.clone(),
            rx,
            create_slice_map(init_segments),
            stop_reading_map,
        );

        // acquire a segment
        let mut slice = cf
            .runtime()
            .block_on(reader.acquire_segment())
            .expect("Failed to acquire segment")
            .unwrap();

        // read an event.
        let event = slice.next().unwrap();
        assert_eq!(event.value.len(), 1);
        assert!(is_all_same(event.value.as_slice()), "Event has been corrupted");
        assert_eq!(event.offset_in_segment, 0); // first event.

        let result = slice.next();
        assert!(result.is_some());
        let event = result.unwrap();
        assert_eq!(event.value.len(), 1);
        assert!(is_all_same(event.value.as_slice()), "Event has been corrupted");
        assert_eq!(event.offset_in_segment, 9); // second event.

        // release the segment slice.
        let _ = cf.runtime().block_on(reader.release_segment_at(slice, 0));

        // simulate a segment read at offset 0.
        let (_stop_tx, stop_rx) = oneshot::channel();
        tokio::spawn(generate_constant_size_events(
            tx.clone(),
            20,
            NUM_EVENTS,
            0,
            false,
            stop_rx,
        ));

        // acquire the next segment
        let mut slice = cf
            .runtime()
            .block_on(reader.acquire_segment())
            .expect("Failed to acquire segment")
            .unwrap();
        // Verify a partial event being present. This implies
        let event = slice.next().unwrap();
        assert_eq!(event.value.len(), 1);
        assert!(is_all_same(event.value.as_slice()), "Event has been corrupted");
        assert_eq!(event.offset_in_segment, 0); // first event.
    }

    #[tokio::test]
    async fn test_read_partial_events_buffer_10() {
        let (tx, mut rx) = mpsc::channel(1);
        tokio::spawn(generate_variable_size_events(tx, 10, 20, 0, false));
        let mut segment_slice = create_segment_slice(0);
        let mut expected_offset: usize = 0;
        let mut expected_event_len = 0;

        loop {
            if segment_slice.is_empty() {
                if let Some(response) = rx.recv().await {
                    segment_slice
                        .meta
                        .segment_data
                        .value
                        .put(response.expect("get response").value);
                } else {
                    break; // All events are sent.
                }
            }

            while let Some(d) = segment_slice.next() {
                assert_eq!(expected_offset, d.offset_in_segment as usize);
                assert_eq!(expected_event_len + 1, d.value.len());
                assert!(is_all_same(d.value.as_slice()));
                expected_offset += 8 + expected_event_len + 1;
                expected_event_len += 1;
            }
        }
        assert_eq!(20, expected_event_len);
    }

    #[tokio::test]
    async fn test_read_partial_events_buffer_100() {
        let (tx, mut rx) = mpsc::channel(1);
        tokio::spawn(generate_variable_size_events(tx, 100, 200, 0, false));
        let mut segment_slice = create_segment_slice(0);
        let mut expected_offset: usize = 0;
        let mut expected_event_len = 0;

        loop {
            if segment_slice.is_empty() {
                if let Some(response) = rx.recv().await {
                    segment_slice
                        .meta
                        .segment_data
                        .value
                        .put(response.expect("get response").value);
                } else {
                    break; // All events are sent.
                }
            }

            while let Some(d) = segment_slice.next() {
                assert_eq!(expected_offset, d.offset_in_segment as usize);
                assert_eq!(expected_event_len + 1, d.value.len());
                assert!(is_all_same(d.value.as_slice()));
                expected_offset += 8 + expected_event_len + 1;
                expected_event_len += 1;
            }
        }
        assert_eq!(200, expected_event_len);
    }

    // Generate event data given the length of the event.
    // The data is 'a'
    fn generate_event_data(len: usize) -> BytesMut {
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

    // This is a test function to generate single events of varying sizes
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

    // This method reads the header and returns a BytesMut whose size is as big as the event.
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
    fn create_slice_map(init_segments: Vec<SegmentSlice>) -> HashMap<ScopedSegment, SliceMetadata> {
        let mut map = HashMap::with_capacity(init_segments.len());
        for s in init_segments {
            map.insert(
                ScopedSegment::from(s.meta.scoped_segment.clone().as_str()),
                s.meta.clone(),
            );
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
    async fn generate_constant_size_events(
        tx: Sender<SegmentReadResult>,
        buf_size: usize,
        num_events: usize,
        segment_id: usize,
        should_delay: bool,
        mut stop_generation: oneshot::Receiver<()>,
    ) {
        let mut segment_name = "scope/test/".to_owned();
        segment_name.push_str(segment_id.to_string().as_ref());
        let mut buf = BytesMut::with_capacity(buf_size);
        let mut offset: i64 = 0;
        for _i in 1..num_events + 1 {
            if let Ok(_) | Err(TryRecvError::Closed) = stop_generation.try_recv() {
                break;
            }
            let mut data = generate_event_data(1); // constant event data.
            if data.len() < buf.capacity() - buf.len() {
                buf.put(data);
            } else {
                while data.len() > 0 {
                    let free_space = buf.capacity() - buf.len();
                    if free_space == 0 {
                        if should_delay {
                            sleep(Duration::from_millis(100)).await;
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

    // Generate events to simulate Pravega SegmentReadCommand.
    async fn generate_variable_size_events(
        tx: Sender<SegmentReadResult>,
        buf_size: usize,
        num_events: usize,
        segment_id: usize,
        should_delay: bool,
    ) {
        let mut segment_name = "scope/test/".to_owned();
        segment_name.push_str(segment_id.to_string().as_ref());
        segment_name.push_str(".#epoch.0");
        let mut buf = BytesMut::with_capacity(buf_size);
        let mut offset: i64 = 0;
        for i in 1..num_events + 1 {
            let mut data = generate_event_data(i);
            if data.len() < buf.capacity() - buf.len() {
                buf.put(data);
            } else {
                while data.len() > 0 {
                    let free_space = buf.capacity() - buf.len();
                    if free_space == 0 {
                        if should_delay {
                            sleep(Duration::from_millis(100)).await;
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

    // create a segment slice object without spawning a background task for testing
    fn create_segment_slice(segment_id: i64) -> SegmentSlice {
        let mut segment_name = "scope/test/".to_owned();
        segment_name.push_str(segment_id.to_string().as_ref());
        let segment = ScopedSegment::from(segment_name.as_str());
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

    // Helper method to verify if the bytes read by Segment slice are the same.
    fn is_all_same<T: Eq>(slice: &[T]) -> bool {
        slice
            .get(0)
            .map(|first| slice.iter().all(|x| x == first))
            .unwrap_or(true)
    }
}
