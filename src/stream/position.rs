//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
use crate::error::*;
use pravega_rust_client_shared::{Segment, SegmentWithRange};
use serde::{Deserialize, Serialize};
use serde_cbor::from_slice;
use serde_cbor::to_vec;
use snafu::ResultExt;
use std::collections::HashMap;

/// PositionedVersioned enum contains all versions of Position struct
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) enum PositionVersioned {
    V1(PositionV1),
}

impl PositionVersioned {
    fn to_bytes(&self) -> Result<Vec<u8>, SerdeError> {
        let encoded = to_vec(&self).context(Cbor {
            msg: String::from("serialize PositionVersioned"),
        })?;
        Ok(encoded)
    }

    fn from_bytes(input: &[u8]) -> Result<PositionVersioned, SerdeError> {
        let decoded: PositionVersioned = from_slice(&input[..]).context(Cbor {
            msg: String::from("serialize PositionVersioned"),
        })?;
        Ok(decoded)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) struct PositionV1 {
    owned_segments: HashMap<Segment, i64>,
    segment_ranges: HashMap<Segment, SegmentWithRange>,
}

impl PositionV1 {
    pub(crate) fn new(segments: HashMap<SegmentWithRange, i64>) -> Self {
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

    pub(crate) fn get_owned_segments_with_offsets(&self) -> HashMap<Segment, i64> {
        self.owned_segments.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordered_float::OrderedFloat;
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
                segment: Segment {
                    number: 0,
                    tx_id: None,
                },
            },
            min_key: OrderedFloat::from(0.0),
            max_key: OrderedFloat::from(1.0),
        };
        segments.insert(segment_with_range, 0);
        let v1 = PositionV1::new(segments);
        let position = PositionVersioned::V1(v1.clone());

        let encoded = position.to_bytes().expect("encode to byte array");
        let decoded = PositionVersioned::from_bytes(&encoded).expect("decode from byte array");
        assert_eq!(PositionVersioned::V1(v1), decoded);
    }
}
