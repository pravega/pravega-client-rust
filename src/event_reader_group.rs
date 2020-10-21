//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactory;
use crate::event_reader::EventReader;
use crate::reader_group::reader_group_config::{ReaderGroupConfigV1, ReaderGroupConfigVersioned};
use crate::reader_group::reader_group_state::{Offset, ReaderGroupState, ReaderGroupStateError};
use pravega_rust_client_shared::{Reader, ScopedSegment, ScopedStream};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(new)]
pub struct ReaderGroup {
    name: String,
    config: ReaderGroupConfigVersioned,
    pub state: Arc<Mutex<ReaderGroupState>>,
}

impl ReaderGroup {
    ///
    /// Create a reader group.
    ///
    pub async fn create(
        name: String,
        stream: ScopedStream,
        rg_config: ReaderGroupConfigVersioned,
        factory: ClientFactory,
    ) -> ReaderGroup {
        let controller = factory.get_controller_client();
        let segments = factory
            .get_controller_client()
            .get_head_segments(&stream)
            .await
            .expect("Error while fetching stream's starting segments to read from ");
        let init_segments: HashMap<ScopedSegment, Offset> = segments
            .iter()
            .map(|(seg, off)| {
                (
                    ScopedSegment {
                        scope: stream.scope.clone(),
                        stream: stream.stream.clone(),
                        segment: seg.clone(),
                    },
                    Offset::new(*off),
                )
            })
            .collect();
        ReaderGroup {
            name: name.clone(),
            config: rg_config.clone(),
            state: Arc::new(Mutex::new(
                ReaderGroupState::new(name, &factory, rg_config, init_segments).await,
            )),
        }
    }

    // pub async fn create_reader(&mut self, reader_id: String) -> EventReader {
    //     let r: Reader = Reader::from(reader_id.clone());
    //     self.state
    //         .lock()
    //         .await
    //         .add_reader(&r)
    //         .await
    //         .expect("Error while creating the reader");
    //     EventReader::init_reader(reader_id, self.state.clone()).await
    // }

    pub async fn delete_reader(&mut self, reader_id: String) -> Result<(), ReaderGroupStateError> {
        let r: Reader = Reader::from(reader_id);
        let owned_segments: HashMap<ScopedSegment, Offset> = HashMap::new(); // TODO
        self.state.lock().await.remove_reader(&r, owned_segments).await
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::reader_group::reader_group_state::MockReaderGroupState;
    #[tokio::test]
    async fn test_create() {
        let state = MockReaderGroupState::default();
        state.expect_add_reader().returning(Ok(()));
        let v1 = ReaderGroupConfigV1::new();
        let rg_config = ReaderGroupConfigVersioned::V1(v1);
        let mut rg = ReaderGroup {
            name: "rg".to_string(),
            config: rg_config,
            state: state,
        };
        rg.create_reader("r1".to_string()).await;
    }
}
