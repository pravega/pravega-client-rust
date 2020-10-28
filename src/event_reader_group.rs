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
use crate::reader_group::reader_group_config::ReaderGroupConfigVersioned;
use crate::reader_group::reader_group_state::Offset;
use pravega_rust_client_shared::{Reader, ScopedSegment, ScopedStream};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
cfg_if::cfg_if! {
    if #[cfg(test)] {
        use crate::reader_group::reader_group_state::MockReaderGroupState as ReaderGroupState;
    } else {
        use crate::reader_group::reader_group_state::ReaderGroupState;
    }
}

#[derive(new)]
pub struct ReaderGroup {
    name: String,
    config: ReaderGroupConfigVersioned,
    pub state: Arc<Mutex<ReaderGroupState>>,
    client_factory: ClientFactory,
}

impl ReaderGroup {
    // This ensures the mock reader group state object is used for unit tests.
    cfg_if::cfg_if! {
        if #[cfg(test)] {
            async fn create_rg_state(
            _name: String,
            _rg_config: ReaderGroupConfigVersioned,
            _client_factory: &ClientFactory,
            _init_segments: HashMap<ScopedSegment, Offset>,
        ) -> ReaderGroupState {
            ReaderGroupState::default()
        }
        } else {
            async fn create_rg_state (
        name: String,
        rg_config: ReaderGroupConfigVersioned,
        client_factory: &ClientFactory,
        init_segments: HashMap<ScopedSegment, Offset>,
    ) -> ReaderGroupState {
        ReaderGroupState::new(name, client_factory, rg_config, init_segments).await
    }
        }
    }

    ///
    /// Create a reader group that will be used the read events from a provided Pravega stream.
    /// This function is idempotent and invoking it multiple times does not re-initialize an already
    /// existing reader group.
    ///
    pub async fn create(
        name: String,
        stream: ScopedStream,
        rg_config: ReaderGroupConfigVersioned,
        client_factory: ClientFactory,
    ) -> ReaderGroup {
        let segments = client_factory
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
        let rg_state =
            ReaderGroup::create_rg_state(name.clone(), rg_config.clone(), &client_factory, init_segments)
                .await;
        ReaderGroup {
            name: name.clone(),
            config: rg_config.clone(),
            state: Arc::new(Mutex::new(rg_state)),
            client_factory,
        }
    }

    ///
    /// Create a new EventReader under the ReaderGroup. This method panics if the reader is
    /// already part of the reader group.
    ///
    pub async fn create_reader(&self, reader_id: String) -> EventReader {
        let r: Reader = Reader::from(reader_id.clone());
        self.state
            .lock()
            .await
            .add_reader(&r)
            .await
            .expect("Error while creating the reader");
        EventReader::init_reader(reader_id, self.state.clone(), self.client_factory.clone()).await
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::error::SynchronizerError::SyncUpdateError;
    use crate::reader_group::reader_group_config::ReaderGroupConfigV1;
    use crate::reader_group::reader_group_state::ReaderGroupStateError;
    use pravega_rust_client_config::ClientConfigBuilder;
    use pravega_rust_client_config::MOCK_CONTROLLER_URI;

    #[tokio::test]
    #[should_panic]
    async fn test_create_reader_error() {
        let client_factory = ClientFactory::new(
            ClientConfigBuilder::default()
                .controller_uri(MOCK_CONTROLLER_URI)
                .build()
                .unwrap(),
        );
        let mut mock_rg_state = ReaderGroupState::default();

        //Configure mock.
        let err: Result<(), ReaderGroupStateError> = Result::Err(ReaderGroupStateError::SyncError {
            error_msg: "Reader already exists".to_string(),
            source: SyncUpdateError {
                error_msg: "update failed".to_string(),
            },
        });
        mock_rg_state.expect_add_reader().return_once(move |_| err);

        let v1 = ReaderGroupConfigV1::new();
        let rg_config = ReaderGroupConfigVersioned::V1(v1);
        let rg = ReaderGroup {
            name: "rg".to_string(),
            config: rg_config,
            state: Arc::new(Mutex::new(mock_rg_state)),
            client_factory,
        };
        rg.create_reader("r1".to_string()).await;
    }
}
