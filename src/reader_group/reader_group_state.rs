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
use crate::reader_group::reader_group_config::ReaderGroupConfigVersioned;
use crate::table_synchronizer::TableSynchronizer;
use pravega_rust_client_shared::{Reader, ScopedSegment, ScopedStream, SegmentWithRange};
use serde::{Deserialize, Serialize};
use serde_cbor::from_slice;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

const ASSUMED_LAG_MILLIS: u64 = 30000;

/// ReaderGroupState encapsulates all readers states.
pub(crate) struct ReaderGroupState<'a> {
    /// The sync is a TableSynchronizer that provides API to read or write the internal
    /// reader group state stored on the server side. The internal reader group state contains
    /// the following fields.
    ///
    /// Internal stream that is used to store the ReaderGroupState on the server side.
    /// scoped_synchronizer_stream: ScopedStream,
    ///
    /// Reader group config
    /// config: ReaderGroupConfigVersioned
    ///
    /// This is used to balance the workload among readers in this reader group.
    /// distance_to_tail: HashMap<Reader, u64>
    ///
    /// Maps successor segments to their predecessors. A successor segment will ready to be
    /// read if all its predecessors have been read.
    /// future_segments: HashMap<SegmentWithRange, HashSet<i64>>
    ///
    /// Maps active readers to their currently assigned segments.
    /// assigned_segments:  HashMap<Reader, HashMap<SegmentWithRange, Offset>>
    ///
    /// Segments waiting to be assigned to readers.
    /// unassigned_segments: HashMap<SegmentWithRange, Offset>
    sync: TableSynchronizer<'a, String>,
}

impl ReaderGroupState<'_> {
    pub(crate) async fn new(
        scoped_synchronizer_stream: ScopedStream,
        client_facotry: &ClientFactory,
        config: ReaderGroupConfigVersioned,
        segments_to_offsets: HashMap<SegmentWithRange, Offset>,
    ) -> ReaderGroupState<'_> {
        let mut sync = client_facotry
            .create_table_synchronizer("ReaderGroupState".to_owned())
            .await;
        sync.insert(move |table| {
            if table.is_empty() {
                table.insert(
                    "scoped_synchronizer_stream".to_owned(),
                    "ScopedStream".to_owned(),
                    Box::new(scoped_synchronizer_stream.clone()),
                );
                table.insert(
                    "config".to_owned(),
                    "ReaderGroupConfigVersioned".to_owned(),
                    Box::new(config.clone()),
                );
                table.insert(
                    "distance_to_tail".to_owned(),
                    "HashMap<Reader, u64>".to_owned(),
                    Box::new(HashMap::new() as HashMap<Reader, u64>),
                );
                table.insert(
                    "future_segments".to_owned(),
                    "HashMap<SegmentWithRange, HashSet<i64>>".to_owned(),
                    Box::new(HashMap::new() as HashMap<SegmentWithRange, HashSet<i64>>),
                );
                table.insert(
                    "assigned_segments".to_owned(),
                    "HashMap<Reader, HashMap<SegmentWithRange, Offset>>".to_owned(),
                    Box::new(HashMap::new() as HashMap<Reader, HashMap<SegmentWithRange, Offset>>),
                );
                table.insert(
                    "unassigned_segments".to_owned(),
                    "HashMap<SegmentWithRange, Offset>".to_owned(),
                    Box::new(segments_to_offsets.clone()),
                );
            }
        })
        .await
        .expect("should initialize table synchronizer");
        ReaderGroupState { sync }
    }

    /// Adds a reader to the reader group state.
    pub(crate) async fn add_reader(&mut self, reader: &Reader) {
        self.sync
            .insert(|table| {
                let value = table
                    .get(&"assigned_segments".to_owned())
                    .expect("get assigned segments");
                let mut assigned_segments: HashMap<Reader, HashMap<SegmentWithRange, Offset>> =
                    from_slice(&value.data).expect("deserialize assigned segments");
                if assigned_segments.contains_key(reader) {
                    panic!("should not add existing online reader");
                } else {
                    assigned_segments.insert(reader.to_owned(), HashMap::new());
                    table.insert(
                        "assigned_segments".to_owned(),
                        "HashMap<Reader, HashMap<SegmentWithRange, Offset>>".to_owned(),
                        Box::new(assigned_segments),
                    );
                }

                let value = table
                    .get(&"distance_to_tail".to_owned())
                    .expect("get distance to tail");
                let mut distance_to_tail: HashMap<Reader, u64> =
                    from_slice(&value.data).expect("deserialize distance to tail");
                distance_to_tail.insert(reader.to_owned(), u64::MAX);
                table.insert(
                    "distance_to_tail".to_owned(),
                    "HashMap<Reader, u64>".to_owned(),
                    Box::new(distance_to_tail),
                );
            })
            .await
            .expect("should add reader");
    }

    /// Returns the active readers in a vector.
    pub(crate) async fn get_online_readers(&mut self) -> Vec<Reader> {
        self.sync.fetch_updates().await.expect("should fetch updates");
        let in_memory_map = self.sync.get_current_map();
        let value = in_memory_map
            .get("assigned_segments")
            .expect("get assigned segments");
        let assigned_segments: HashMap<Reader, HashMap<SegmentWithRange, Offset>> =
            from_slice(&value.data).expect("deserialize assigned segments");
        assigned_segments
            .keys()
            .map(|k| k.to_owned())
            .collect::<Vec<Reader>>()
    }

    /// Gets the latest positions for the given reader.
    pub(crate) async fn get_reader_positions(
        &mut self,
        reader: &Reader,
    ) -> HashMap<SegmentWithRange, Offset> {
        self.sync.fetch_updates().await.expect("should fetch updates");
        let in_memory_map = self.sync.get_current_map();
        let value = in_memory_map
            .get("assigned_segments")
            .expect("get assigned segments");
        let assigned_segments: HashMap<Reader, HashMap<SegmentWithRange, Offset>> =
            from_slice(&value.data).expect("deserialize assigned segments");
        assigned_segments.get(reader).expect("reader must exist").clone() as HashMap<SegmentWithRange, Offset>
    }

    /// Updates the latest positions for the given reader.
    pub(crate) async fn update_reader_positions(
        &mut self,
        reader: &Reader,
        latest_positions: HashMap<SegmentWithRange, Offset>,
    ) {
        self.sync
            .insert(|table| {
                let value = table
                    .get(&"assigned_segments".to_owned())
                    .expect("get assigned segments");
                let mut assigned_segments: HashMap<Reader, HashMap<SegmentWithRange, Offset>> =
                    from_slice(&value.data).expect("deserialize assigned segments");

                if let Some(segments) = assigned_segments.get_mut(reader) {
                    for (segment, offset) in &latest_positions {
                        segments.entry(segment.to_owned()).and_modify(|v| {
                            v.read = offset.read;
                            v.processed = offset.processed;
                        });
                    }
                    table.insert(
                        "assigned_segments".to_owned(),
                        "HashMap<Reader, HashMap<SegmentWithRange, Offset>>".to_owned(),
                        Box::new(assigned_segments),
                    );
                } else {
                    panic!("reader must exist");
                }
            })
            .await
            .expect("should update reader positions");
    }

    /// Removes the given reader from the reader group state and puts the segments that are previously
    /// owned by the removed reader to the unassigned list for redistribution.
    pub(crate) async fn remove_reader(
        &mut self,
        reader: &Reader,
        owned_segments: HashMap<ScopedSegment, Offset>,
    ) {
        self.sync.fetch_updates().await.expect("should fetch updates");
        self.sync
            .insert(|table| {
                let value = table
                    .get(&"assigned_segments".to_owned())
                    .expect("get assigned segments");
                let mut assigned_segments: HashMap<Reader, HashMap<SegmentWithRange, Offset>> =
                    from_slice(&value.data).expect("deserialize assigned segments");

                let value = table
                    .get(&"unassigned_segments".to_owned())
                    .expect("get unassigned segments");
                let mut unassigned_segments: HashMap<SegmentWithRange, Offset> =
                    from_slice(&value.data).expect("deserialize unassigned segments");

                let value = table
                    .get(&"distance_to_tail".to_owned())
                    .expect("get distance to tail");
                let mut distance_to_tail: HashMap<Reader, u64> =
                    from_slice(&value.data).expect("deserialize distance to tail");

                assigned_segments.remove(reader).map_or((), |segments| {
                    for (segment, pos) in segments {
                        let offset = owned_segments
                            .get(&segment.scoped_segment)
                            .map_or(pos, |v| v.to_owned());
                        unassigned_segments.insert(segment, offset);
                    }
                });
                distance_to_tail.remove(reader);
                table.insert(
                    "assigned_segments".to_owned(),
                    "HashMap<Reader, HashMap<SegmentWithRange, Offset>>".to_owned(),
                    Box::new(assigned_segments),
                );
                table.insert(
                    "unassigned_segments".to_owned(),
                    "HashMap<SegmentWithRange, Offset>".to_owned(),
                    Box::new(unassigned_segments),
                );
                table.insert(
                    "distance_to_tail".to_owned(),
                    "HashMap<Reader, u64>".to_owned(),
                    Box::new(distance_to_tail),
                );
            })
            .await
            .expect("should remove reader");
    }

    /// Returns the list of all segments.
    pub(crate) async fn get_segments(&mut self) -> HashSet<ScopedSegment> {
        self.sync.fetch_updates().await.expect("should fetch updates");

        let in_memory_map = self.sync.get_current_map();

        let value = in_memory_map
            .get("assigned_segments")
            .expect("get assigned segments");
        let assigned_segments: HashMap<Reader, HashMap<SegmentWithRange, Offset>> =
            from_slice(&value.data).expect("deserialize assigned segments");

        let value = in_memory_map
            .get("unassigned_segments")
            .expect("get unassigned segments");
        let unassigned_segments: HashMap<SegmentWithRange, Offset> =
            from_slice(&value.data).expect("deserialize unassigned segments");

        let mut set = HashSet::new();
        for v in assigned_segments.values() {
            set.extend(
                v.keys()
                    .map(|segment| segment.scoped_segment.clone())
                    .collect::<HashSet<ScopedSegment>>(),
            )
        }
        set.extend(
            unassigned_segments
                .keys()
                .map(|segment| segment.scoped_segment.clone())
                .collect::<HashSet<ScopedSegment>>(),
        );
        set
    }

    /// Assigns an unassigned segment to a given reader
    pub(crate) async fn assign_segment_to_reader(&mut self, reader: &Reader, segment: &ScopedSegment) {
        self.sync
            .insert(|table| {
                let value = table
                    .get(&"assigned_segments".to_owned())
                    .expect("get assigned segments");
                let mut assigned_segments: HashMap<Reader, HashMap<SegmentWithRange, Offset>> =
                    from_slice(&value.data).expect("deserialize assigned segments");

                let value = table
                    .get(&"unassigned_segments".to_owned())
                    .expect("get unassigned segments");
                let mut unassigned_segments: HashMap<SegmentWithRange, Offset> =
                    from_slice(&value.data).expect("deserialize unassigned segments");

                ReaderGroupState::assign_segment_to_reader_internal(
                    reader,
                    segment,
                    &mut assigned_segments,
                    &mut unassigned_segments,
                );

                table.insert(
                    "assigned_segments".to_owned(),
                    "HashMap<Reader, HashMap<SegmentWithRange, Offset>>".to_owned(),
                    Box::new(assigned_segments),
                );
                table.insert(
                    "unassigned_segments".to_owned(),
                    "HashMap<SegmentWithRange, Offset>".to_owned(),
                    Box::new(unassigned_segments),
                );
            })
            .await
            .expect("should assign segment to reader");
    }

    fn assign_segment_to_reader_internal(
        reader: &Reader,
        segment: &ScopedSegment,
        assigned_segments: &mut HashMap<Reader, HashMap<SegmentWithRange, Offset>>,
        unassigned_segments: &mut HashMap<SegmentWithRange, Offset>,
    ) {
        let assigned = assigned_segments
            .get_mut(reader)
            .expect("reader doesn't exist in reader group");

        let mut newly_assigned = unassigned_segments
            .keys()
            .filter(|&s| s.scoped_segment == *segment)
            .map(|s| s.to_owned())
            .collect::<Vec<SegmentWithRange>>();
        assert!(
            !newly_assigned.is_empty(),
            "segment does not exist in reader group"
        );
        let new_segment_with_range = newly_assigned.pop().expect("get segment with range");
        let offset = unassigned_segments
            .remove(&new_segment_with_range)
            .expect("remove segment from unassigned list");
        assigned.insert(new_segment_with_range, offset);
    }

    /// Returns the list of segments assigned to the requested reader.
    pub(crate) async fn get_segments_for_reader(&mut self, reader: &Reader) -> HashSet<ScopedSegment> {
        self.sync.fetch_updates().await.expect("should fetch updates");
        let in_memory_map = self.sync.get_current_map();
        let value = in_memory_map
            .get("assigned_segments")
            .expect("get assigned segments");
        let assigned_segments: HashMap<Reader, HashMap<SegmentWithRange, Offset>> =
            from_slice(&value.data).expect("deserialize assigned segments");

        assigned_segments.get(reader).map_or_else(
            || panic!("reader does not exist"),
            |segments| {
                segments
                    .keys()
                    .map(|s| s.scoped_segment.to_owned())
                    .collect::<HashSet<ScopedSegment>>()
            },
        )
    }

    /// Releases a currently assigned segment from the given reader.
    pub(crate) async fn release_segment(
        &mut self,
        reader: &Reader,
        segment: &ScopedSegment,
        offset: &Offset,
    ) {
        self.sync
            .insert(|table| {
                let value = table
                    .get(&"assigned_segments".to_owned())
                    .expect("get assigned segments");
                let mut assigned_segments: HashMap<Reader, HashMap<SegmentWithRange, Offset>> =
                    from_slice(&value.data).expect("deserialize assigned segments");

                let value = table
                    .get(&"unassigned_segments".to_owned())
                    .expect("get unassigned segments");
                let mut unassigned_segments: HashMap<SegmentWithRange, Offset> =
                    from_slice(&value.data).expect("deserialize unassigned segments");

                ReaderGroupState::release_segment_internal(
                    reader,
                    segment,
                    offset,
                    &mut assigned_segments,
                    &mut unassigned_segments,
                );

                table.insert(
                    "assigned_segments".to_owned(),
                    "HashMap<Reader, HashMap<SegmentWithRange, Offset>>".to_owned(),
                    Box::new(assigned_segments),
                );
                table.insert(
                    "unassigned_segments".to_owned(),
                    "HashMap<SegmentWithRange, Offset>".to_owned(),
                    Box::new(unassigned_segments),
                );
            })
            .await
            .expect("should release segment");
    }

    fn release_segment_internal(
        reader: &Reader,
        segment: &ScopedSegment,
        offset: &Offset,
        assigned_segments: &mut HashMap<Reader, HashMap<SegmentWithRange, Offset>>,
        unassigned_segments: &mut HashMap<SegmentWithRange, Offset>,
    ) {
        let assigned = assigned_segments
            .get(reader)
            .expect("reader doesn't exist in reader group");
        let removed = assigned
            .iter()
            .filter(|&(s, _pos)| s.scoped_segment == *segment)
            .map(|(s, _pos)| s.to_owned())
            .collect::<Vec<SegmentWithRange>>();

        assert_eq!(
            removed.len(),
            1,
            "should have one and only one in the assigned segment list"
        );

        for segment in removed {
            unassigned_segments.insert(segment.clone(), offset.clone());
        }
    }

    /// Removes the completed segments and add its successors for next to read.
    pub(crate) async fn segment_completed(
        &mut self,
        reader: &Reader,
        segment_completed: &SegmentWithRange,
        successors_mapped_to_their_predecessors: &HashMap<SegmentWithRange, Vec<i64>>,
    ) {
        self.sync
            .insert(|table| {
                let value = table
                    .get(&"assigned_segments".to_owned())
                    .expect("get assigned segments");
                let mut assigned_segments: HashMap<Reader, HashMap<SegmentWithRange, Offset>> =
                    from_slice(&value.data).expect("deserialize assigned segments");

                let value = table
                    .get(&"unassigned_segments".to_owned())
                    .expect("get unassigned segments");
                let mut unassigned_segments: HashMap<SegmentWithRange, Offset> =
                    from_slice(&value.data).expect("deserialize unassigned segments");

                let value = table
                    .get(&"future segments".to_owned())
                    .expect("get future segments");
                let mut future_segments: HashMap<SegmentWithRange, HashSet<i64>> =
                    from_slice(&value.data).expect("deserialize future segments");

                ReaderGroupState::segment_completed_internal(
                    reader,
                    segment_completed,
                    successors_mapped_to_their_predecessors,
                    &mut assigned_segments,
                    &mut unassigned_segments,
                    &mut future_segments,
                );

                table.insert(
                    "assigned_segments".to_owned(),
                    "HashMap<Reader, HashMap<SegmentWithRange, Offset>>".to_owned(),
                    Box::new(assigned_segments),
                );
                table.insert(
                    "unassigned_segments".to_owned(),
                    "HashMap<SegmentWithRange, Offset>".to_owned(),
                    Box::new(unassigned_segments),
                );
                table.insert(
                    "future_segments".to_owned(),
                    "HashMap<SegmentWithRange, HashSet<i64>>".to_owned(),
                    Box::new(future_segments),
                );
            })
            .await
            .expect("should update segment complete");
    }

    pub(crate) fn segment_completed_internal(
        reader: &Reader,
        segment_completed: &SegmentWithRange,
        successors_mapped_to_their_predecessors: &HashMap<SegmentWithRange, Vec<i64>>,
        assigned_segments: &mut HashMap<Reader, HashMap<SegmentWithRange, Offset>>,
        unassigned_segments: &mut HashMap<SegmentWithRange, Offset>,
        future_segments: &mut HashMap<SegmentWithRange, HashSet<i64>>,
    ) {
        let assigned = assigned_segments
            .get_mut(reader)
            .expect("reader doesn't exist in reader group");
        assigned
            .remove(segment_completed)
            .expect("should have assigned to reader");
        for (segment, list) in successors_mapped_to_their_predecessors {
            if !future_segments.contains_key(segment) {
                let required_to_complete = HashSet::from_iter(list.clone().into_iter());
                future_segments.insert(segment.to_owned(), required_to_complete);
            }
        }
        for required_to_complete in future_segments.values_mut() {
            required_to_complete.remove(&segment_completed.scoped_segment.segment.number);
        }
        let ready_to_read = future_segments
            .iter()
            .filter(|&(_segment, set)| set.is_empty())
            .map(|(segment, _set)| segment.to_owned())
            .collect::<Vec<SegmentWithRange>>();
        for segment in ready_to_read {
            unassigned_segments.insert(segment.to_owned(), Offset::new(0, 0));
        }
        future_segments.retain(|_segment, set| !set.is_empty());
    }
}

#[derive(new, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) struct Offset {
    /// The client has read to this offset and handle the result to the application/caller.
    /// But some events before this offset may not have been processed by application/caller.
    /// In case of failure, those unprocessed events may need to be read from application/caller
    /// again.
    read: u64,
    /// The application/caller has processed up to this offset, this is less than or equal to the
    /// read offset.
    processed: u64,
}

#[cfg(test)]
mod test {}
