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
use pravega_client_shared::{ScopedSegment, ScopedStream};
use serde::{Deserialize, Serialize};
use serde_cbor::from_slice;
use serde_cbor::to_vec;
use snafu::ResultExt;
use std::collections::HashMap;

/// StreamCutVersioned enum contains all versions of StreamCut struct
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) enum StreamCutVersioned {
    V1(StreamCutV1),
    UNBOUNDED,
}

impl StreamCutVersioned {
    fn to_bytes(&self) -> Result<Vec<u8>, SerdeError> {
        let encoded = to_vec(&self).context(Cbor {
            msg: "serialize StreamCutVersioned".to_owned(),
        })?;
        Ok(encoded)
    }

    fn from_bytes(input: &[u8]) -> Result<StreamCutVersioned, SerdeError> {
        let decoded: StreamCutVersioned = from_slice(&input[..]).context(Cbor {
            msg: "serialize StreamCutVersioned".to_owned(),
        })?;
        Ok(decoded)
    }
}

/// A set of segment/offset pairs for a single stream that represent a consistent position in the
/// stream. (IE: Segment 1 and 2 will not both appear in the set if 2 succeeds 1, and if 0 appears
/// and is responsible for keyspace 0-0.5 then other segments covering the range 0.5-1.0 will also be
/// included.)
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) struct StreamCutV1 {
    stream: ScopedStream,
    positions: HashMap<ScopedSegment, i64>,
}

impl StreamCutV1 {
    pub(crate) fn new(stream: ScopedStream, positions: HashMap<ScopedSegment, i64>) -> Self {
        StreamCutV1 { stream, positions }
    }

    /// gets a clone of the internal scoped stream
    pub(crate) fn get_stream(&self) -> ScopedStream {
        self.stream.clone()
    }

    /// gets a clone of the internal positions
    pub(crate) fn get_positions(&self) -> HashMap<ScopedSegment, i64> {
        self.positions.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pravega_client_shared::{Scope, Segment, Stream};

    #[test]
    fn test_position_serde() {
        let scope = Scope::from("scope".to_owned());
        let stream = Stream::from("stream".to_owned());
        let scoped_stream = ScopedStream::new(scope.clone(), stream.clone());
        let mut positions = HashMap::new();

        let segment0 = ScopedSegment::new(scope.clone(), stream.clone(), Segment::from(0));
        let segment1 = ScopedSegment::new(scope, stream, Segment::from(1));

        positions.insert(segment0, 0);
        positions.insert(segment1, 100);

        let v1 = StreamCutV1::new(scoped_stream, positions);
        let stream_cut = StreamCutVersioned::V1(v1.clone());

        let encoded = stream_cut.to_bytes().expect("encode to byte array");
        let decoded = StreamCutVersioned::from_bytes(&encoded).expect("decode from byte array");
        assert_eq!(StreamCutVersioned::V1(v1), decoded);
    }
}
