//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactoryInternal;
use crate::segment_slice::SegmentSlice;
use pravega_rust_client_shared::{ScopedSegment, ScopedStream};

#[derive(new)]
struct EventReader<'a> {
    stream: ScopedStream,
    factory: &'a ClientFactoryInternal, //config: ClientConfig,
}

impl<'a> EventReader<'a> {
    pub fn init(stream: ScopedStream, factory: &'a ClientFactoryInternal) -> EventReader<'a> {
        // create a reader object.
        //
        EventReader { stream, factory }
    }

    fn release_segment_at(&mut self, _slice: SegmentSlice, _offset_in_segment: u64) {
        //The above two call this with different offsets.
    }

    // async fn acquire_segment<'a>(&mut self) -> SegmentSlice<'a> {
    //     //Returns a segment which is now owned by this process.
    //     //Because it returns a reference the slice cannot outlive the reader.
    //     let (segment, read_offset) = self.get_next_segment().await;
    //     info!("Acquire segment {} with read offset {}", segment, read_offset);
    //     SegmentSlice::new(segment, read_offset, self.factory).await
    // }

    async fn get_next_segment(&mut self) -> (ScopedSegment, i64) {
        // Mock method: Always returns first segment.
        // TODO: This method should check from the ReaderGroupState and return a segment and the corresponding read offset.
        let segments = self
            .factory
            .get_controller_client()
            .get_current_segments(&self.stream)
            .await
            .expect("Failed to talk to controller");
        (segments.get_segment(0.0), 0)
    }
}
