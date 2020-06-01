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
use crate::stream::stream_cut::StreamCutVersioned;
use pravega_rust_client_shared::ScopedStream;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::collections::HashMap;

/// ReaderGroupConfigVersioned enum contains all versions of Position struct
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) enum ReaderGroupConfigVersioned {
    V1(ReaderGroupConfigV1),
}

impl ReaderGroupConfigVersioned {
    fn to_bytes(&self) -> Result<Vec<u8>, SerdeError> {
        let encoded = BINCODE_CONFIG.serialize(&self).context(Serde {
            msg: String::from("serialize ReaderGroupConfigVersioned"),
        })?;
        Ok(encoded)
    }

    fn from_bytes(input: &[u8]) -> Result<ReaderGroupConfigVersioned, SerdeError> {
        let decoded: ReaderGroupConfigVersioned = BINCODE_CONFIG.deserialize(&input[..]).context(Serde {
            msg: String::from("serialize ReaderGroupConfigVersioned"),
        })?;
        Ok(decoded)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) struct ReaderGroupConfigV1 {
    group_refresh_time_millis: u64,
    automatic_checkpoint_interval_millis: u64,

    starting_stream_cuts: HashMap<ScopedStream, StreamCutVersioned>,
    ending_stream_cuts: HashMap<ScopedStream, StreamCutVersioned>,

    max_outstanding_checkpoint_request: i32,
}

impl ReaderGroupConfigV1 {
    pub(crate) fn new() -> Self {
        ReaderGroupConfigV1 {
            group_refresh_time_millis: 3000,
            automatic_checkpoint_interval_millis: 30000,
            starting_stream_cuts: HashMap::new(),
            ending_stream_cuts: HashMap::new(),
            max_outstanding_checkpoint_request: 3,
        }
    }

    pub(crate) fn stream(
        mut self,
        stream: ScopedStream,
        starting_stream_cuts: Option<StreamCutVersioned>,
        ending_stream_cuts: Option<StreamCutVersioned>,
    ) -> ReaderGroupConfigV1 {
        if let Some(cut) = starting_stream_cuts {
            self.starting_stream_cuts.insert(stream.clone(), cut);
        } else {
            self.starting_stream_cuts
                .insert(stream.clone(), StreamCutVersioned::UNBOUNDED);
        }

        if let Some(cut) = ending_stream_cuts {
            self.ending_stream_cuts.insert(stream, cut);
        } else {
            self.ending_stream_cuts
                .insert(stream, StreamCutVersioned::UNBOUNDED);
        }

        self
    }

    pub(crate) fn start_from_stream_cuts(
        mut self,
        stream_cuts: HashMap<ScopedStream, StreamCutVersioned>,
    ) -> ReaderGroupConfigV1 {
        self.starting_stream_cuts = stream_cuts;
        self
    }

    pub(crate) fn start_from_checkpoint(
        mut self,
        stream_cuts: HashMap<ScopedStream, StreamCutVersioned>,
    ) -> ReaderGroupConfigV1 {
        self.starting_stream_cuts = stream_cuts;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pravega_rust_client_shared::{Scope, Stream};

    #[test]
    fn test_reader_group_config_serde() {
        let scope = Scope::new("scope".into());
        let stream = Stream::new("stream".into());
        let scoped_stream = ScopedStream::new(scope.clone(), stream.clone());

        let mut v1 = ReaderGroupConfigV1::new();
        v1 = v1.stream(scoped_stream, None, None);

        let config = ReaderGroupConfigVersioned::V1(v1.clone());

        let encoded = config.to_bytes().expect("encode to byte array");
        let decoded = ReaderGroupConfigVersioned::from_bytes(&encoded).expect("decode from byte array");
        assert_eq!(ReaderGroupConfigVersioned::V1(v1), decoded);
    }
}
