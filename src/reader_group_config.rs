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
use crate::stream::stream_cut::StreamCutVersioned;
use pravega_client_shared::ScopedStream;
use serde::{Deserialize, Serialize};
use serde_cbor::from_slice;
use serde_cbor::to_vec;
use snafu::ResultExt;
use std::collections::HashMap;

///
/// Specifies the ReaderGroupConfig.
/// ReaderGroupConfig::default() ensures the group refresh interval is set to 3 seconds.
///
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ReaderGroupConfig {
    pub(crate) config: ReaderGroupConfigVersioned,
}

impl ReaderGroupConfig {
    ///
    /// Create a new ReaderGroupConfig by specifying the group refresh interval in millis.
    ///
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

    ///
    /// Method to serialize the ReaderGroupConfig into bytes.
    ///
    pub fn to_bytes(&self) -> Result<Vec<u8>, SerdeError> {
        self.config.to_bytes()
    }

    ///
    /// Method to de-serialize the ReaderGroupConfig object from bytes.
    ///
    pub fn from_bytes(input: &[u8]) -> Result<Self, SerdeError> {
        let decoded = ReaderGroupConfigVersioned::from_bytes(input);
        decoded.map(|config| ReaderGroupConfig { config })
    }

    ///
    /// Method to obtain the streams in a ReaderGroupConfig.
    ///
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

impl Default for ReaderGroupConfigBuilder {
    fn default() -> Self {
        Self {
            group_refresh_time_millis: 3000,
            starting_stream_cuts: Default::default(),
        }
    }
}

impl ReaderGroupConfigBuilder {
    ///
    /// Set reader group refresh time.
    ///
    pub fn set_group_refresh_time(&mut self, group_refresh_time_millis: u64) -> &mut Self {
        self.group_refresh_time_millis = group_refresh_time_millis;
        self
    }

    ///
    /// Add a Pravega Stream to the reader group.
    ///
    pub fn add_stream(&mut self, stream: ScopedStream) -> &mut Self {
        self.starting_stream_cuts
            .insert(stream, StreamCutVersioned::Unbounded);
        self
    }

    ///
    /// Build a ReaderGroupConfig object.
    /// This method panics for invalid configuration.
    ///
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

/// ReaderGroupConfigVersioned enum contains all versions of Position struct
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

#[cfg(test)]
mod tests {
    use super::*;
    use pravega_client_shared::{Scope, Stream};

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
