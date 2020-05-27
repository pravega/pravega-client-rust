//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

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
use crate::stream::stream_cut::{StreamCutV1, StreamCutVersioned};
use pravega_rust_client_shared::{ScopedSegment, ScopedStream};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::collections::HashMap;

/// StreamCutVersioned enum contains all versions of StreamCut struct
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) enum CheckpointVersioned {
    V1(CheckpointV1),
}

impl CheckpointVersioned {
    fn to_bytes(&self) -> Result<Vec<u8>, SerdeError> {
        let encoded = BINCODE_CONFIG.serialize(&self).context(Serde {
            msg: String::from("serialize CheckpointVersioned"),
        })?;
        Ok(encoded)
    }

    fn from_bytes(input: &[u8]) -> Result<CheckpointVersioned, SerdeError> {
        let decoded: CheckpointVersioned = BINCODE_CONFIG.deserialize(&input[..]).context(Serde {
            msg: String::from("serialize CheckpointVersioned"),
        })?;
        Ok(decoded)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) struct CheckpointV1 {
    name: String,
    positions: HashMap<ScopedStream, StreamCutVersioned>,
}

impl CheckpointV1 {
    pub(crate) fn new(name: String, segment_positions: HashMap<ScopedSegment, i64>) -> Self {
        let mut stream_positions = HashMap::new();
        for (k, v) in segment_positions {
            stream_positions
                .entry(k.get_scoped_stream())
                .or_insert_with(|| HashMap::new())
                .insert(k, v);
        }

        let mut positions = HashMap::new();
        for (k, v) in stream_positions {
            positions.insert(k.clone(), StreamCutVersioned::V1(StreamCutV1::new(k, v)));
        }

        CheckpointV1 { name, positions }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pravega_rust_client_shared::{Scope, Segment, Stream};

    #[test]
    fn test_checkpoint_serde() {
        let scope = Scope::new("scope".into());
        let stream = Stream::new("stream".into());
        let mut positions = HashMap::new();

        let segment0 = ScopedSegment::new(scope.clone(), stream.clone(), Segment::new(0));
        let segment1 = ScopedSegment::new(scope, stream, Segment::new(1));

        positions.insert(segment0, 0);
        positions.insert(segment1, 100);

        let v1 = CheckpointV1::new("checkpoint".into(), positions);
        let stream_cut = CheckpointVersioned::V1(v1.clone());

        let encoded = stream_cut.to_bytes().expect("encode to byte array");
        let decoded = CheckpointVersioned::from_bytes(&encoded).expect("decode from byte array");
        assert_eq!(CheckpointVersioned::V1(v1), decoded);
    }
}
