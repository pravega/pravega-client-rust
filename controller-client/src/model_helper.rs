/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
use super::PravegaNodeUri;
use crate::controller::*;
use pravega_rust_client_shared::*;

impl From<NodeUri> for PravegaNodeUri {
    fn from(value: NodeUri) -> PravegaNodeUri {
        let mut uri: String = value.endpoint;
        uri.push_str(":");
        uri.push_str(&value.port.to_string());
        PravegaNodeUri::new(uri)
    }
}

impl Into<SegmentId> for ScopedSegment {
    fn into(self) -> SegmentId {
        SegmentId {
            stream_info: Some(StreamInfo {
                scope: self.scope.name,
                stream: self.stream.name,
            }),
            segment_id: self.segment.number,
        }
    }
}

impl Into<StreamInfo> for ScopedStream {
    fn into(self) -> StreamInfo {
        StreamInfo {
            scope: self.scope.name,
            stream: self.stream.name,
        }
    }
}

impl<'a> From<&'a Scope> for ScopeInfo {
    fn from(value: &Scope) -> ScopeInfo {
        ScopeInfo {
            scope: value.name.to_owned(),
        }
    }
}

impl Into<ScopeInfo> for Scope {
    fn into(self) -> ScopeInfo {
        ScopeInfo {
            scope: self.to_string(),
        }
    }
}

impl Into<StreamConfig> for StreamConfiguration {
    fn into(self) -> StreamConfig {
        StreamConfig {
            stream_info: Some(self.scoped_stream.into()),
            scaling_policy: Some(ScalingPolicy {
                scale_type: self.scaling.scale_type as i32,
                target_rate: self.scaling.target_rate,
                scale_factor: self.scaling.scale_factor,
                min_num_segments: self.scaling.min_num_segments,
            }),
            retention_policy: Some(RetentionPolicy {
                retention_type: self.retention.retention_type as i32,
                retention_param: self.retention.retention_param,
            }),
        }
    }
}

impl Into<crate::controller::StreamCut> for pravega_rust_client_shared::StreamCut {
    fn into(self) -> crate::controller::StreamCut {
        crate::controller::StreamCut {
            stream_info: Some(self.scoped_stream.into()),
            cut: self.segment_offset_map.to_owned(), // create a clone
        }
    }
}
