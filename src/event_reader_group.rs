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
use crate::reader_group::reader_group_state::Offset;
use crate::reader_group_config::ReaderGroupConfig;
use pravega_client_shared::{Reader, Scope, ScopedSegment, ScopedStream};
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

///
/// A reader group is a collection of readers that collectively read all the events in the stream.
/// The events are distributed among the readers in the group such that each event goes to only one reader.
///
/// The readers in the group may change over time. Readers are added to the group by invoking the
/// [`ReaderGroup::create_reader`] API.
///
/// [`ReaderGroup::create_reader`]: ReaderGroup::create_reader
/// An example usage pattern is as follows
///
/// ```no_run
/// use pravega_client_config::{ClientConfigBuilder, MOCK_CONTROLLER_URI};
/// use pravega_client::client_factory::ClientFactory;
/// use pravega_client_shared::{ScopedStream, Scope, Stream};
///
/// #[tokio::main]
/// async fn main() {
///    let config = ClientConfigBuilder::default()
///         .controller_uri(MOCK_CONTROLLER_URI)
///         .build()
///         .expect("creating config");
///     let client_factory = ClientFactory::new(config);
///     let scope = scope: Scope::from("scope".to_string());
///     let stream = ScopedStream {
///         scope: scope.clone(),
///         stream: Stream::from("stream".to_string()),
///     };
///     // Create a reader group to read data from the Pravega stream.
///     let rg = client_factory.create_reader_group(scope, "rg".to_string(), stream).await;
///     // Create a reader under the reader group.
///     let mut reader1 = rg.create_reader("r1".to_string()).await;
///     let mut reader2 = rg.create_reader("r2".to_string()).await;
///     // EventReader APIs can be used to read events.
/// }
/// ```
#[derive(new)]
pub struct ReaderGroup {
    name: String,
    config: ReaderGroupConfig,
    pub state: Arc<Mutex<ReaderGroupState>>,
    client_factory: ClientFactory,
}

impl ReaderGroup {
    // This ensures the mock reader group state object is used for unit tests.
    cfg_if::cfg_if! {
        if #[cfg(test)] {
            async fn create_rg_state(
            _scope: Scope,
            _name: String,
            _rg_config: ReaderGroupConfig,
            _client_factory: &ClientFactory,
            _init_segments: HashMap<ScopedSegment, Offset>,
        ) -> ReaderGroupState {
            ReaderGroupState::default()
        }
        } else {
            async fn create_rg_state (
        scope: Scope,
        name: String,
        rg_config: ReaderGroupConfig,
        client_factory: &ClientFactory,
        init_segments: HashMap<ScopedSegment, Offset>,
    ) -> ReaderGroupState {
        ReaderGroupState::new(scope, name, client_factory, rg_config.config, init_segments).await
    }
        }
    }

    ///
    /// Create a reader group that will be used the read events from a provided Pravega stream.
    /// This function is idempotent and invoking it multiple times does not re-initialize an already
    /// existing reader group.
    ///
    pub async fn create(
        scope: Scope,
        name: String,
        rg_config: ReaderGroupConfig,
        client_factory: ClientFactory,
    ) -> ReaderGroup {
        let streams: Vec<ScopedStream> = rg_config.get_streams();
        let mut init_segments: HashMap<ScopedSegment, Offset> = HashMap::new();
        for stream in streams {
            let segments = client_factory
                .get_controller_client()
                .get_head_segments(&stream)
                .await
                .expect("Error while fetching stream's starting segments to read from ");
            init_segments.extend(segments.iter().map(|(seg, off)| {
                (
                    ScopedSegment {
                        scope: stream.scope.clone(),
                        stream: stream.stream.clone(),
                        segment: seg.clone(),
                    },
                    Offset::new(*off),
                )
            }));
        }
        let rg_state = ReaderGroup::create_rg_state(
            scope,
            name.clone(),
            rg_config.clone(),
            &client_factory,
            init_segments,
        )
        .await;
        ReaderGroup {
            name: name.clone(),
            config: rg_config.clone(),
            state: Arc::new(Mutex::new(rg_state)),
            client_factory,
        }
    }

    ///
    /// Get the reader name.
    ///
    pub fn get_name(&self) -> String {
        self.name.clone()
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
    use crate::reader_group::reader_group_state::ReaderGroupStateError;
    use crate::reader_group_config::ReaderGroupConfigBuilder;
    use pravega_client_config::ClientConfigBuilder;
    use pravega_client_config::MOCK_CONTROLLER_URI;

    // test to validate creation of an already existing reader.
    #[test]
    #[should_panic]
    fn test_create_reader_error() {
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
        let rg = ReaderGroup {
            name: "rg".to_string(),
            config: ReaderGroupConfigBuilder::default()
                .add_stream(ScopedStream::from("scope/s1"))
                .build(),
            state: Arc::new(Mutex::new(mock_rg_state)),
            client_factory: client_factory.clone(),
        };
        client_factory
            .get_runtime_handle()
            .block_on(rg.create_reader("r1".to_string()));
    }
}
