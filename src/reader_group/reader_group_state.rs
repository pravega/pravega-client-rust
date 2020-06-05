//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use super::BINCODE_CONFIG;
use crate::error::*;
use crate::reader_group::reader_group_config::ReaderGroupConfigVersioned;
use pravega_rust_client_shared::{ScopedSegment, ScopedStream, SegmentWithRange};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

const ASSUMED_LAG_MILLIS: u64 = 30000;

/// EventPointerVersioned enum contains all versions of EventPointer
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) enum ReaderGroupStateVersioned {
    V1(ReaderGroupStateV1),
}

impl ReaderGroupStateVersioned {
    fn to_bytes(&self) -> Result<Vec<u8>, SerdeError> {
        let encoded = BINCODE_CONFIG.serialize(&self).context(Serde {
            msg: String::from("serialize ReaderGroupStateVersioned"),
        })?;
        Ok(encoded)
    }

    fn from_bytes(input: &[u8]) -> Result<ReaderGroupStateVersioned, SerdeError> {
        let decoded: ReaderGroupStateVersioned = BINCODE_CONFIG.deserialize(&input[..]).context(Serde {
            msg: String::from("deserialize ReaderGroupStateVersioned"),
        })?;
        Ok(decoded)
    }
}

/// ReaderGroupState encapsulates all readers states.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) struct ReaderGroupStateV1 {
    /// Internal stream that is used to store the ReaderGroupState on the server side.
    scoped_synchronizer_stream: ScopedStream,

    config: ReaderGroupConfigVersioned,

    /// This is used to balance the workload among readers in this reader group.
    distance_to_tail: HashMap<String, u64>,

    /// Maps successor segments to their predecessors. A successor segment will ready to be
    /// read if all its predecessors have been read.
    future_segments: HashMap<SegmentWithRange, HashSet<i64>>,

    /// Maps active readers to their currently assigned segments.
    assigned_segments: HashMap<String, HashMap<SegmentWithRange, Offset>>,

    /// Segments waiting to be assigned to readers.
    unassigned_segments: HashMap<SegmentWithRange, Offset>,
}

impl ReaderGroupStateV1 {
    pub(crate) fn new(
        scoped_synchronizer_stream: ScopedStream,
        config: ReaderGroupConfigVersioned,
        segments_to_offsets: HashMap<SegmentWithRange, Offset>,
    ) -> Self {
        ReaderGroupStateV1 {
            scoped_synchronizer_stream,
            config,
            distance_to_tail: HashMap::new(),
            future_segments: HashMap::new(),
            assigned_segments: HashMap::new(),
            unassigned_segments: segments_to_offsets,
        }
    }

    /// Adds a reader to the reader group state.
    pub(crate) fn add_reader(&mut self, reader: &str) {
        assert!(
            !self.assigned_segments.contains_key(reader),
            "should not add an existing online reader"
        );

        self.assigned_segments.insert(reader.to_owned(), HashMap::new());
        self.distance_to_tail.insert(reader.to_owned(), std::u64::MAX);
    }

    /// Returns the number of active readers.
    pub(crate) fn get_number_of_readers(&self) -> usize {
        self.assigned_segments.len()
    }

    /// Checks if the given reader is online or not.
    pub(crate) fn is_reader_online(&self, reader: &str) -> bool {
        self.assigned_segments.contains_key(reader)
    }

    /// Returns the active readers in a vector.
    pub(crate) fn get_online_readers(&self) -> Vec<String> {
        self.assigned_segments
            .keys()
            .map(|k| k.to_owned())
            .collect::<Vec<String>>()
    }

    /// Gets the latest positions for the given reader.
    pub(crate) fn get_reader_positions(&self, reader: &str) -> HashMap<SegmentWithRange, Offset> {
        self.assigned_segments
            .get(reader)
            .expect("reader must exist")
            .clone()
    }

    /// Updates the latest positions for the given reader.
    pub(crate) fn update_reader_positions(
        &mut self,
        reader: &str,
        latest_positions: HashMap<SegmentWithRange, Offset>,
    ) {
        let current_segments_with_offsets =
            self.assigned_segments.get_mut(reader).expect("reader must exist");
        for (segment, offset) in latest_positions {
            current_segments_with_offsets
                .entry(segment.to_owned())
                .and_modify(|v| {
                    v.read = offset.read;
                    v.processed = offset.processed;
                });
        }
    }

    /// Removes the given reader from the reader group state and puts the segments that are previously
    /// owned by the removed reader to the unassigned list for redistribution.
    pub(crate) fn remove_reader(&mut self, reader: &str, owned_segments: HashMap<ScopedSegment, Offset>) {
        self.assigned_segments.remove(reader).map_or((), |segments| {
            for (segment, pos) in segments {
                let offset = owned_segments
                    .get(&segment.scoped_segment)
                    .map_or(pos, |v| v.to_owned());
                self.unassigned_segments.insert(segment, offset);
            }
        });
        self.distance_to_tail.remove(reader);
    }

    /// Returns the list of all segments.
    pub(crate) fn get_segments(&self) -> HashSet<ScopedSegment> {
        let mut result = HashSet::new();
        for v in self.assigned_segments.values() {
            result.extend(
                v.keys()
                    .map(|segment| segment.scoped_segment.clone())
                    .collect::<HashSet<ScopedSegment>>(),
            )
        }
        result.extend(
            self.unassigned_segments
                .keys()
                .map(|segment| segment.scoped_segment.clone())
                .collect::<HashSet<ScopedSegment>>(),
        );
        result
    }

    /// Assigns an unassigned segment to a given reader
    pub(crate) fn assign_segment_to_reader(&mut self, reader: &str, segment: &ScopedSegment) {
        let assigned = self
            .assigned_segments
            .get_mut(reader)
            .expect("reader doesn't exist in reader group");
        let mut newly_assigned = self
            .unassigned_segments
            .keys()
            .filter(|&s| s.scoped_segment == *segment)
            .map(|s| s.clone())
            .collect::<Vec<SegmentWithRange>>();
        assert!(
            !newly_assigned.is_empty(),
            "segment does not exist in reader group"
        );

        let new_segment_with_range = newly_assigned.pop().expect("get segment with range");
        let offset = self
            .unassigned_segments
            .remove(&new_segment_with_range)
            .expect("remove segment from unassigned list");
        assigned.insert(new_segment_with_range, offset);
    }

    /// Returns the list of segments assigned to the requested reader.
    pub(crate) fn get_segments_for_reader(&self, reader: &str) -> HashSet<ScopedSegment> {
        self.assigned_segments.get(reader).map_or_else(
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
    pub(crate) fn release_segment(&mut self, reader: &str, segment: ScopedSegment, offset: Offset) {
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
            self.unassigned_segments.insert(segment.clone(), offset.clone());
        }
    }

    /// Removes the completed segments and add its successors for next to read.
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
            self.unassigned_segments
                .insert(segment.to_owned(), Offset::new(0, 0));
        }
        self.future_segments.retain(|_segment, set| !set.is_empty());
    }
}

#[derive(new, Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) struct Offset {
    /// The client has read to this offset and handle the result to the caller. But some of the events
    /// before this offset may not have been processed. In case of a failure, some of the events before
    /// this offset may be read again.
    read: u64,
    /// The caller has processed up to this offset, this is less than or equal to the read offset.
    processed: u64,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::reader_group::reader_group_config::ReaderGroupConfigV1;
    use ordered_float::OrderedFloat;
    use pravega_rust_client_shared::{Scope, Segment, Stream};

    fn set_up() -> ReaderGroupStateV1 {
        let internal_stream =
            ScopedStream::new(Scope::new("system".to_owned()), Stream::new("stream".to_owned()));
        let mut segments = HashMap::new();
        segments.insert(
            SegmentWithRange::new(
                ScopedSegment::new(
                    Scope::new("scope".to_owned()),
                    Stream::new("stream".to_owned()),
                    Segment::new(0),
                ),
                OrderedFloat::from(0.0),
                OrderedFloat::from(1.0),
            ),
            Offset::new(0, 0),
        );
        ReaderGroupStateV1::new(
            internal_stream,
            ReaderGroupConfigVersioned::V1(ReaderGroupConfigV1::new()),
            segments,
        )
    }
    #[test]
    fn test_reader_group_state() {
        let mut state = set_up();
        assert_eq!(state.get_segments().len(), 1);

        // test add reader
        let reader = "reader".to_owned();
        state.add_reader(&reader);
        assert!(state.is_reader_online(&reader));
        assert_eq!(state.get_number_of_readers(), 1);
        assert_eq!(state.get_online_readers().len(), 1);

        // test assign segment
        let scoped_segment = ScopedSegment::new(
            Scope::new("scope".to_owned()),
            Stream::new("stream".to_owned()),
            Segment::new(0),
        );
        state.assign_segment_to_reader(&reader, &scoped_segment);
        assert_eq!(state.get_segments_for_reader(&reader).len(), 1);

        // test update reader position
        let mut latest_offsets = HashMap::new();
        let segment_with_range = SegmentWithRange::new(
            ScopedSegment::new(
                Scope::new("scope".to_owned()),
                Stream::new("stream".to_owned()),
                Segment::new(0),
            ),
            OrderedFloat::from(0.0),
            OrderedFloat::from(1.0),
        );
        let offset = Offset::new(10, 10);
        latest_offsets.insert(segment_with_range.clone(), offset.clone());
        state.update_reader_positions(&reader, latest_offsets);
        let updated_offsets = state.get_reader_positions(&reader);
        let updated_offset = updated_offsets
            .get(&segment_with_range)
            .expect("should contain segment");

        assert_eq!(*updated_offset, offset);

        // test remove reader
        state.remove_reader(&reader, HashMap::new());
        assert_eq!(state.get_online_readers().len(), 0);
    }
}
