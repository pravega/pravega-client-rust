//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::reader_group::reader_group_config::ReaderGroupConfigVersioned;
use pravega_rust_client_shared::{ScopedSegment, ScopedStream, SegmentWithRange};
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

const ASSUMED_LAG_MILLIS: u64 = 30000;

pub(crate) trait ReaderGroupStateUpdate {
    fn update() {}
}

pub(crate) struct ReaderGroupState {
    scoped_synchronizer_stream: String,
    config: ReaderGroupConfigVersioned,
    //    revision Revision,
    distance_to_tail: HashMap<String, i64>,
    future_segments: HashMap<SegmentWithRange, HashSet<i64>>,
    assigned_segments: HashMap<String, HashMap<SegmentWithRange, i64>>,
    // linked hashmap ?
    unassigned_segments: HashMap<SegmentWithRange, i64>,
    last_read_position: HashMap<SegmentWithRange, i64>,
    end_segments: HashMap<ScopedSegment, i64>,
}

impl ReaderGroupState {
    pub(crate) fn new(
        scoped_synchronizer_stream: String,
        config: ReaderGroupConfigVersioned,
        segments_to_offsets: HashMap<SegmentWithRange, i64>,
        end_segments: HashMap<ScopedSegment, i64>,
    ) -> Self {
        ReaderGroupState {
            scoped_synchronizer_stream,
            config,
            distance_to_tail: HashMap::new(),
            future_segments: HashMap::new(),
            assigned_segments: HashMap::new(),
            unassigned_segments: segments_to_offsets,
            last_read_position: HashMap::new(),
            end_segments,
        }
    }

    /// Adds a reader to the reader group state.
    pub(crate) fn add_reader(&mut self, reader: &str) {
        assert!(!self.assigned_segments.contains_key(reader), "should not add an existing online reader");

        self.assigned_segments.insert(reader.to_owned(), HashMap::new());
        self.distance_to_tail.insert(reader.to_owned(), std::i64::MAX);
    }

    /// Checks if the given reader is online or not.
    pub(crate) fn is_reader_online(&self, reader: &str) -> bool {
        self.assigned_segments.contains_key(reader)
    }

    /// Removes the given reader from the reader group state and puts the segments that previously
    /// owned by the removed reader to the unassigned list for redistribution.
    pub(crate) fn remove_reader(&mut self, reader: &str, owned_segments: HashMap<ScopedSegment, i64>) {
        self.assigned_segments.remove(reader).map_or((), |segments| {
            for (segment, pos) in segments {
                let offset = owned_segments
                    .get(&segment.scoped_segment)
                    .map_or(pos, |v| v.to_owned());
                self.unassigned_segments
                    .insert(segment, offset)
                    .expect("reader offline, failed to insert owned segment back to unassigned map");
            }
        });
        self.distance_to_tail.remove(reader);
    }

    /// Returns the number of active readers.
    pub(crate) fn get_number_of_readers(&self) -> usize {
        self.assigned_segments.len()
    }

    /// Returns the active readers in a vector.
    pub(crate) fn get_online_readers(&self) -> Vec<String> {
        self.assigned_segments
            .keys()
            .map(|k| k.to_owned())
            .collect::<Vec<String>>()
    }

    /// Returns the list of segments assigned to the requested reader, or None if this reader does not exist.
    pub(crate) fn get_segments(&self, reader: &str) -> Option<HashSet<ScopedSegment>> {
        self.assigned_segments.get(reader).map_or_else(
            || None,
            |segments| {
                Some(
                    segments
                        .keys()
                        .map(|s| s.scoped_segment.to_owned())
                        .collect::<HashSet<ScopedSegment>>(),
                )
            },
        )
    }

    /// Gets the last read positions for the given stream for all the readers in reader group state.
    pub(crate) fn get_last_read_positions(&self, stream: &ScopedStream) -> HashMap<SegmentWithRange, i64> {
        let mut result = HashMap::new();
        for (key, value) in &self.last_read_position {
            let segment = &key.scoped_segment;
            if segment.scope == stream.scope && segment.stream == stream.stream {
                result.insert(key.to_owned(), value.to_owned());
            }
        }
        result
    }

    /// Gets the last read positions for a given reader.
    pub(crate) fn get_last_read_positions_for_single_reader(
        &self,
        reader: &str,
    ) -> Option<HashMap<SegmentWithRange, i64>> {
        self.get_segments(reader).map_or_else(
            || None,
            |segments| {
                let mut result = HashMap::new();
                for (key, value) in &self.last_read_position {
                    let segment = &key.scoped_segment;
                    if segments.contains(segment) {
                        result.insert(key.to_owned(), value.to_owned());
                    }
                }
                Some(result)
            },
        )
    }

    /// Releases a currently assigned segment from the given reader.
    pub(crate) fn release_segment(&mut self, reader: &str, segment: ScopedSegment, offset: i64) {
        let assigned = self
            .assigned_segments
            .get(reader)
            .expect("reader doesn't exist in reader group");
        let removed = assigned
            .iter()
            .filter(|&(s, _pos)| s.scoped_segment == segment)
            .map(|(s, _pos)| s.to_owned())
            .collect::<Vec<SegmentWithRange>>();

        assert_eq!(
            removed.len(),
            1,
            "should have one and only one in the assigned segment list"
        );

        for segment in removed {
            self.unassigned_segments.insert(segment.clone(), offset);
            self.last_read_position.insert(segment, offset);
        }
    }

    /// Removes the completed segments and add its successors for ready to read.
    pub(crate) fn segment_completed(
        &mut self,
        reader: &str,
        segment_completed: SegmentWithRange,
        successors_mapped_to_their_predecessors: HashMap<SegmentWithRange, Vec<i64>>,
    ) {
        let assigned = self
            .assigned_segments
            .get_mut(reader)
            .expect("reader doesn't exist in reader group");
        assigned
            .remove(&segment_completed)
            .expect("should have assigned to reader");
        self.last_read_position
            .remove(&segment_completed)
            .expect("should have assigned to reader");
        for (segment, list) in successors_mapped_to_their_predecessors {
            if !self.future_segments.contains_key(&segment) {
                let required_to_complete = HashSet::from_iter(list);
                self.future_segments
                    .insert(segment.to_owned(), required_to_complete);
            }
        }
        for required_to_complete in self.future_segments.values_mut() {
            required_to_complete.remove(&segment_completed.scoped_segment.segment.number);
        }
        let ready_to_read = self
            .future_segments
            .iter()
            .filter(|&(_segment, set)| set.is_empty())
            .map(|(segment, _set)| segment.to_owned())
            .collect::<Vec<SegmentWithRange>>();
        for segment in ready_to_read {
            self.unassigned_segments.insert(segment.to_owned(), 0);
            self.last_read_position.insert(segment, 0);
        }
        self.future_segments.retain(|_segment, set| !set.is_empty());
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_reader_group_state() {}
}
