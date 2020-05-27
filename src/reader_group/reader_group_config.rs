//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
use crate::stream::stream_cut::StreamCutVersioned;
use pravega_rust_client_shared::ScopedStream;
use std::collections::HashMap;

pub(crate) struct ReaderGroupConfig {
    group_refresh_time_millis: u64,
    automatic_checkpoint_interval_millis: u64,

    starting_stream_cuts: HashMap<ScopedStream, StreamCutVersioned>,
    ending_stream_cuts: HashMap<ScopedStream, StreamCutVersioned>,

    max_outstanding_checkpoint_request: i32,
}

impl ReaderGroupConfig {
    pub(crate) fn new() -> Self {
        ReaderGroupConfig {
            group_refresh_time_millis: 3000,
            automatic_checkpoint_interval_millis: 30000,
            starting_stream_cuts: HashMap::new(),
            ending_stream_cuts: HashMap::new(),
            max_outstanding_checkpoint_request: 3,
        }
    }

    pub(crate) fn stream(
        &mut self,
        stream: ScopedStream,
        starting_stream_cuts: Option<StreamCutVersioned>,
        ending_stream_cuts: Option<StreamCutVersioned>,
    ) -> &mut ReaderGroupConfig {
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
        &mut self,
        stream_cuts: HashMap<ScopedStream, StreamCutVersioned>,
    ) -> &mut ReaderGroupConfig {
        self.starting_stream_cuts = stream_cuts;
        self
    }

    pub(crate) fn start_from_checkpoint(
        &mut self,
        stream_cuts: HashMap<ScopedStream, StreamCutVersioned>,
    ) -> &mut ReaderGroupConfig {
        self.starting_stream_cuts = stream_cuts;
        self
    }
}
