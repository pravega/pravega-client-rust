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
use crate::event::reader::EventReader;
use crate::event::reader_group_state::Offset;

use pravega_client_shared::{Reader, Scope, ScopedSegment, ScopedStream};

use serde::{Deserialize, Serialize};
use serde_cbor::Error as CborError;
use serde_cbor::{from_slice, to_vec};
use snafu::ResultExt;
use snafu::Snafu;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

cfg_if::cfg_if! {
    if #[cfg(test)] {
        use crate::event::reader_group_state::MockReaderGroupState as ReaderGroupState;
    } else {
        use crate::event::reader_group_state::ReaderGroupState;
    }
}

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
///     let scope = Scope::from("scope".to_string());
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

    /// Create a reader group that will be used the read events from a provided Pravega stream.
    /// This function is idempotent and invoking it multiple times does not re-initialize an already
    /// existing reader group.
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

    /// Get the reader name.
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /// Create a new EventReader under the ReaderGroup. This method panics if the reader is
    /// already part of the reader group.
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

// Specifies the ReaderGroupConfig.
// ReaderGroupConfig::default() ensures the group refresh interval is set to 3 seconds.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ReaderGroupConfig {
    pub(crate) config: ReaderGroupConfigVersioned,
}

impl ReaderGroupConfig {
    /// Create a new ReaderGroupConfig by specifying the group refresh interval in millis.
    pub fn new(group_refresh_time_millis: u64) -> Self {
        let conf_v1 = ReaderGroupConfigV1 {
            group_refresh_time_millis,
            starting_stream_cuts: HashMap::new(),
            ending_stream_cuts: HashMap::new(),
        };
        ReaderGroupConfig {
            config: ReaderGroupConfigVersioned::V1(conf_v1),
        }
    }

    /// Method to serialize the ReaderGroupConfig into bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, SerdeError> {
        self.config.to_bytes()
    }

    /// Method to de-serialize the ReaderGroupConfig object from bytes.
    pub fn from_bytes(input: &[u8]) -> Result<Self, SerdeError> {
        let decoded = ReaderGroupConfigVersioned::from_bytes(input);
        decoded.map(|config| ReaderGroupConfig { config })
    }

    /// Method to obtain the streams in a ReaderGroupConfig.
    pub fn get_streams(&self) -> Vec<ScopedStream> {
        let ReaderGroupConfigVersioned::V1(v1) = &self.config;
        v1.starting_stream_cuts
            .keys()
            .cloned()
            .collect::<Vec<ScopedStream>>()
    }
}

pub struct ReaderGroupConfigBuilder {
    group_refresh_time_millis: u64,
    starting_stream_cuts: HashMap<ScopedStream, StreamCutVersioned>,
}

impl ReaderGroupConfigBuilder {
    /// Set reader group refresh time.
    pub fn set_group_refresh_time(&mut self, group_refresh_time_millis: u64) -> &mut Self {
        self.group_refresh_time_millis = group_refresh_time_millis;
        self
    }

    /// Add a Pravega Stream to the reader group.
    pub fn add_stream(&mut self, stream: ScopedStream) -> &mut Self {
        self.starting_stream_cuts
            .insert(stream, StreamCutVersioned::Unbounded);
        self
    }

    /// Build a ReaderGroupConfig object.
    /// This method panics for invalid configuration.
    pub fn build(&self) -> ReaderGroupConfig {
        assert!(
            !self.starting_stream_cuts.is_empty(),
            "Atleast 1 stream should be part of the reader group config"
        );
        ReaderGroupConfig {
            config: ReaderGroupConfigVersioned::V1(ReaderGroupConfigV1 {
                group_refresh_time_millis: self.group_refresh_time_millis,
                starting_stream_cuts: self.starting_stream_cuts.clone(),
                ending_stream_cuts: Default::default(), // This will be extended when bounded processing is enabled.
            }),
        }
    }
}

impl Default for ReaderGroupConfigBuilder {
    fn default() -> Self {
        Self {
            group_refresh_time_millis: 3000,
            starting_stream_cuts: Default::default(),
        }
    }
}

// ReaderGroupConfigVersioned enum contains all versions of Position struct
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) enum ReaderGroupConfigVersioned {
    V1(ReaderGroupConfigV1),
}

impl ReaderGroupConfigVersioned {
    fn to_bytes(&self) -> Result<Vec<u8>, SerdeError> {
        let encoded = to_vec(&self).context(Cbor {
            msg: "serialize ReaderGroupConfigVersioned".to_owned(),
        })?;
        Ok(encoded)
    }

    fn from_bytes(input: &[u8]) -> Result<ReaderGroupConfigVersioned, SerdeError> {
        let decoded: ReaderGroupConfigVersioned = from_slice(input).context(Cbor {
            msg: "serialize ReaderGroupConfigVersioned".to_owned(),
        })?;
        Ok(decoded)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) struct ReaderGroupConfigV1 {
    /// maximum delay by which the readers return the latest read offsets of their
    /// assigned segments.
    group_refresh_time_millis: u64,
    starting_stream_cuts: HashMap<ScopedStream, StreamCutVersioned>,
    ending_stream_cuts: HashMap<ScopedStream, StreamCutVersioned>,
}

impl Default for ReaderGroupConfigV1 {
    fn default() -> Self {
        Self::new()
    }
}

impl ReaderGroupConfigV1 {
    pub(crate) fn new() -> Self {
        ReaderGroupConfigV1 {
            group_refresh_time_millis: 3000,
            starting_stream_cuts: HashMap::new(),
            ending_stream_cuts: HashMap::new(),
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
                .insert(stream.clone(), StreamCutVersioned::Unbounded);
        }

        if let Some(cut) = ending_stream_cuts {
            self.ending_stream_cuts.insert(stream, cut);
        } else {
            self.ending_stream_cuts
                .insert(stream, StreamCutVersioned::Unbounded);
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
}

/// StreamCutVersioned enum contains all versions of StreamCut struct
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) enum StreamCutVersioned {
    V1(StreamCutV1),
    Unbounded,
}

impl StreamCutVersioned {
    fn to_bytes(&self) -> Result<Vec<u8>, SerdeError> {
        let encoded = to_vec(&self).context(Cbor {
            msg: "serialize StreamCutVersioned".to_owned(),
        })?;
        Ok(encoded)
    }

    fn from_bytes(input: &[u8]) -> Result<StreamCutVersioned, SerdeError> {
        let decoded: StreamCutVersioned = from_slice(input).context(Cbor {
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

#[derive(Debug, Snafu)]
pub enum SerdeError {
    #[snafu(display("Failed to {:?} due to {:?}", msg, source))]
    Cbor { msg: String, source: CborError },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::reader_group::ReaderGroupConfigBuilder;
    use crate::event::reader_group_state::ReaderGroupStateError;
    use crate::sync::synchronizer::SynchronizerError::SyncUpdateError;
    use pravega_client_config::ClientConfigBuilder;
    use pravega_client_config::MOCK_CONTROLLER_URI;
    use pravega_client_shared::{Segment, Stream};

    #[test]
    fn test_stream_cut_serde() {
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
            .get_runtime()
            .block_on(rg.create_reader("r1".to_string()));
    }

    #[test]
    fn test_reader_group_config_serde() {
        let scope = Scope::from("scope".to_owned());
        let stream = Stream::from("stream".to_owned());
        let scoped_stream = ScopedStream::new(scope.clone(), stream.clone());

        let mut v1 = ReaderGroupConfigV1::new();
        v1 = v1.stream(scoped_stream, None, None);

        let config = ReaderGroupConfigVersioned::V1(v1.clone());

        let encoded = config.to_bytes().expect("encode to byte array");
        let decoded = ReaderGroupConfigVersioned::from_bytes(&encoded).expect("decode from byte array");
        assert_eq!(ReaderGroupConfigVersioned::V1(v1), decoded);
    }

    #[test]
    fn test_reader_group_config_builder() {
        let rg_config = ReaderGroupConfigBuilder::default()
            .set_group_refresh_time(4000)
            .add_stream(ScopedStream::from("scope1/s1"))
            .add_stream(ScopedStream::from("scope2/s2"))
            .build();
        let ReaderGroupConfigVersioned::V1(v1) = rg_config.config;
        assert_eq!(v1.group_refresh_time_millis, 4000);
        //Validate both the streams are present.
        assert!(v1
            .starting_stream_cuts
            .contains_key(&ScopedStream::from("scope1/s1")));
        assert!(v1
            .starting_stream_cuts
            .contains_key(&ScopedStream::from("scope2/s2")));
        for val in v1.starting_stream_cuts.values() {
            assert_eq!(&StreamCutVersioned::Unbounded, val);
        }
    }

    #[test]
    fn test_reader_group_config_builder_default() {
        let rg_config = ReaderGroupConfigBuilder::default()
            .add_stream(ScopedStream::from("scope1/s1"))
            .build();
        let ReaderGroupConfigVersioned::V1(v1) = rg_config.config;
        // verify default
        assert_eq!(v1.group_refresh_time_millis, 3000);
        //Validate both the streams are present.
        assert!(v1
            .starting_stream_cuts
            .contains_key(&ScopedStream::from("scope1/s1")));
        for val in v1.starting_stream_cuts.values() {
            assert_eq!(&StreamCutVersioned::Unbounded, val);
        }
    }

    #[test]
    #[should_panic]
    fn test_reader_group_config_builder_invalid() {
        let _rg_config = ReaderGroupConfigBuilder::default().build();
    }
}
