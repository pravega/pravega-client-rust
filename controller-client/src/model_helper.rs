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
use ordered_float::OrderedFloat;
use pravega_rust_client_shared::*;
use std::collections::BTreeMap;
use uuid::Uuid;

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

impl<'a> From<&'a ScopedSegment> for SegmentId {
    fn from(value: &'a ScopedSegment) -> SegmentId {
        SegmentId {
            stream_info: Some(StreamInfo {
                scope: value.scope.name.to_owned(),
                stream: value.stream.name.to_owned(),
            }),
            segment_id: value.segment.number,
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

impl<'a> From<&'a ScopedStream> for StreamInfo {
    fn from(value: &'a ScopedStream) -> StreamInfo {
        StreamInfo {
            scope: value.scope.name.to_owned(),
            stream: value.stream.name.to_owned(),
        }
    }
}

impl<'a> From<&'a Scope> for ScopeInfo {
    fn from(value: &'a Scope) -> ScopeInfo {
        ScopeInfo {
            scope: value.name.to_owned(),
        }
    }
}

impl<'a> From<&'a StreamConfiguration> for StreamConfig {
    fn from(value: &'a StreamConfiguration) -> StreamConfig {
        StreamConfig {
            stream_info: Some(StreamInfo::from(&value.scoped_stream)),
            scaling_policy: Some(ScalingPolicy {
                scale_type: value.scaling.scale_type.to_owned() as i32,
                target_rate: value.scaling.target_rate,
                scale_factor: value.scaling.scale_factor,
                min_num_segments: value.scaling.min_num_segments,
            }),
            retention_policy: Some(RetentionPolicy {
                retention_type: value.retention.retention_type.to_owned() as i32,
                retention_param: value.retention.retention_param,
            }),
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

impl<'a> From<&'a pravega_rust_client_shared::StreamCut> for crate::controller::StreamCut {
    fn from(value: &'a pravega_rust_client_shared::StreamCut) -> crate::controller::StreamCut {
        crate::controller::StreamCut {
            stream_info: Some(StreamInfo::from(&value.scoped_stream)),
            cut: value.segment_offset_map.to_owned(),
        }
    }
}

impl From<SegmentId> for ScopedSegment {
    fn from(value: SegmentId) -> ScopedSegment {
        let stream_info: StreamInfo = value.stream_info.unwrap();
        ScopedSegment {
            scope: Scope::new(stream_info.scope),
            stream: Stream::new(stream_info.stream),
            segment: Segment::new(value.segment_id),
        }
    }
}

impl From<SegmentRanges> for StreamSegments {
    fn from(ranges: SegmentRanges) -> StreamSegments {
        let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
        for range in ranges.segment_ranges {
            segment_map.insert(
                OrderedFloat(range.max_key),
                SegmentWithRange::new(
                    ScopedSegment::from(range.segment_id.unwrap()),
                    OrderedFloat(range.min_key),
                    OrderedFloat(range.max_key),
                ),
            );
        }
        StreamSegments::new(segment_map)
    }
}
impl From<CreateTxnResponse> for TxnSegments {
    fn from(txn_response: CreateTxnResponse) -> TxnSegments {
        let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
        for range in txn_response.active_segments {
            segment_map.insert(
                OrderedFloat(range.max_key),
                SegmentWithRange::new(
                    ScopedSegment::from(range.segment_id.unwrap()),
                    OrderedFloat(range.min_key),
                    OrderedFloat(range.max_key),
                ),
            );
        }
        let txn_uuid: Uuid = match txn_response.txn_id {
            Some(x) => {
                let t: u128 = (x.high_bits as u128) << 64 | (x.low_bits as u128);
                Uuid::from_u128(t)
            }
            None => panic!("Incorrect response from Controller"),
        };
        TxnSegments::new(segment_map, txn_uuid)
    }
}
/*
pub struct CreateTxnResponse {
    #[prost(message, optional, tag = "1")]
    pub txn_id: ::std::option::Option<TxnId>,
    i64

    #[prost(message, repeated, tag = "2")]
    pub active_segments: ::std::vec::Vec<SegmentRange>,
    #[prost(string, tag = "3")]
    pub delegation_token: std::string::String,
}
pub struct TxnSegments {
    pub key_segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange>,
    pub uuid: Uuid,
}
*/
