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

const ASSUMED_LAG_MILLIS: u64 = 30000;

pub(crate) trait ReaderGroupStateUpdate {
    fn update() {}
}

pub(crate) struct ReaderGroupState {
    scoped_synchronizer_stream: String,
    config: ReaderGroupConfigVersioned,
    //    revision Revision,
    distance_to_tail: HashMap<String, u64>,
    future_segments: HashMap<SegmentWithRange, HashSet<u64>>,
    assigned_segments: HashMap<String, HashMap<SegmentWithRange, u64>>,
    // linked hashmap ?
    unassigned_segments: HashMap<SegmentWithRange, u64>,
    last_read_position: HashMap<SegmentWithRange, u64>,
    end_segments: HashMap<ScopedSegment, u64>,
}

impl ReaderGroupState {
    pub(crate) fn new(
        scoped_synchronizer_stream: String,
        config: ReaderGroupConfigVersioned,
        segments_to_offsets: HashMap<SegmentWithRange, u64>,
        end_segments: HashMap<ScopedSegment, u64>,
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

    pub(crate) fn is_reader_online(&self, reader: &str) -> bool {
        self.assigned_segments.contains_key(reader)
    }

    pub(crate) fn get_number_of_readers(&self) -> usize {
        self.assigned_segments.len()
    }

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

    pub(crate) fn get_last_read_positions(&self, stream: &ScopedStream) -> HashMap<SegmentWithRange, u64> {
        let mut result = HashMap::new();
        for (key, value) in &self.last_read_position {
            let segment = &key.scoped_segment;
            if segment.scope == stream.scope && segment.stream == stream.stream {
                result.insert(key.to_owned(), value.to_owned());
            }
        }
        result
    }

    /// Returns the list of segments assigned to the requested reader, or None if this reader does not exist.
    pub(crate) fn get_last_read_positions_for_reader(
        &self,
        reader: &str,
    ) -> Option<HashMap<SegmentWithRange, u64>> {
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
}

#[cfg(test)]
mod test {

    #[test]
    fn test_reader_group_state() {}
}
