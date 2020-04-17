//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
use super::super::CONFIG;
use crate::error::*;
use pravega_rust_client_shared::{Segment, SegmentWithRange};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) struct PositionV1 {
    owned_segments: HashMap<Segment, i64>,
    segment_ranges: HashMap<Segment, SegmentWithRange>,
}

impl PositionV1 {
    fn new(segments: HashMap<SegmentWithRange, i64>) -> Self {
        let mut owned_segments = HashMap::with_capacity(segments.len());
        let mut segment_ranges = HashMap::with_capacity(segments.len());
        for (k, v) in segments {
            owned_segments.insert(k.get_segment(), v);
            segment_ranges.insert(k.get_segment(), k);
        }
        PositionV1 {
            owned_segments,
            segment_ranges,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) enum PositionVersioned {
    V1(PositionV1),
}

impl PositionVersioned {
    fn to_bytes(&self) -> Result<Vec<u8>, SerdeError> {
        let encoded = CONFIG.serialize(&self).context(Position {})?;
        Ok(encoded)
    }

    fn from_bytes(input: &[u8]) -> Result<PositionVersioned, SerdeError> {
        let decoded: PositionVersioned = CONFIG.deserialize(&input[..]).context(Position {})?;
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pravega_rust_client_shared::{Scope, ScopedSegment, Stream};

    #[test]
    fn test_position_serde() {
        let mut segments = HashMap::new();
        let segment_with_range = SegmentWithRange {
            scoped_segment: ScopedSegment {
                scope: Scope {
                    name: "test".to_string(),
                },
                stream: Stream {
                    name: "test".to_string(),
                },
                segment: Segment { number: 0 },
            },
            min_key: Default::default(),
            max_key: Default::default(),
        };
        segments.insert(segment_with_range, 0);
        let v1 = PositionV1::new(segments);
        let position = PositionVersioned::V1(v1.clone());

        let encoded = position.to_bytes().expect("encode to byte array");
        let decoded = PositionVersioned::from_bytes(&encoded).expect("decode from byte array");
        assert_eq!(PositionVersioned::V1(v1), decoded);
    }
}
