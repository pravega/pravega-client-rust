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
use pravega_client_shared::*;
use std::collections::{BTreeMap, HashMap};
use stream_info::AccessOperation;

impl From<NodeUri> for PravegaNodeUri {
    fn from(value: NodeUri) -> PravegaNodeUri {
        let mut uri: String = value.endpoint;
        uri.push(':');
        uri.push_str(&value.port.to_string());
        PravegaNodeUri::from(uri)
    }
}

impl From<TxId> for TxnId {
    fn from(value: TxId) -> TxnId {
        TxnId {
            high_bits: (value.0 >> 64) as i64,
            low_bits: value.0 as i64,
        }
    }
}
impl From<ScopedSegment> for SegmentId {
    fn from(segment: ScopedSegment) -> SegmentId {
        SegmentId {
            stream_info: Some(StreamInfo {
                scope: segment.scope.name,
                stream: segment.stream.name,
                access_operation: AccessOperation::Unspecified as i32,
            }),
            segment_id: segment.segment.number,
        }
    }
}

impl<'a> From<&'a ScopedSegment> for SegmentId {
    fn from(value: &'a ScopedSegment) -> SegmentId {
        SegmentId {
            stream_info: Some(StreamInfo {
                scope: value.scope.name.to_owned(),
                stream: value.stream.name.to_owned(),
                access_operation: AccessOperation::Unspecified as i32,
            }),
            segment_id: value.segment.number,
        }
    }
}
impl From<ScopedStream> for StreamInfo {
    fn from(stream: ScopedStream) -> StreamInfo {
        StreamInfo {
            scope: stream.scope.name,
            stream: stream.stream.name,
            access_operation: AccessOperation::Unspecified as i32,
        }
    }
}

impl<'a> From<&'a ScopedStream> for StreamInfo {
    fn from(value: &'a ScopedStream) -> StreamInfo {
        StreamInfo {
            scope: value.scope.name.to_owned(),
            stream: value.stream.name.to_owned(),
            access_operation: AccessOperation::Unspecified as i32,
        }
    }
}

impl From<StreamInfo> for ScopedStream {
    fn from(value: StreamInfo) -> ScopedStream {
        ScopedStream {
            scope: Scope::from(value.scope),
            stream: Stream::from(value.stream),
        }
    }
}

impl<'a> From<&'a CToken> for ContinuationToken {
    fn from(t: &'a CToken) -> ContinuationToken {
        ContinuationToken {
            token: t.token.to_owned(),
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
                retention_max: i64::MAX,
            }),
            tags: value.tags.as_ref().map(|tags| Tags { tag: tags.to_owned() }),
        }
    }
}
impl From<StreamConfiguration> for StreamConfig {
    fn from(config: StreamConfiguration) -> StreamConfig {
        StreamConfig {
            stream_info: Some(config.scoped_stream.into()),
            scaling_policy: Some(ScalingPolicy {
                scale_type: config.scaling.scale_type as i32,
                target_rate: config.scaling.target_rate,
                scale_factor: config.scaling.scale_factor,
                min_num_segments: config.scaling.min_num_segments,
            }),
            retention_policy: Some(RetentionPolicy {
                retention_type: config.retention.retention_type as i32,
                retention_param: config.retention.retention_param,
                retention_max: i64::MAX,
            }),
            tags: config.tags.map(|tags| Tags { tag: tags }),
        }
    }
}
impl From<StreamConfig> for StreamConfiguration {
    fn from(config: StreamConfig) -> StreamConfiguration {
        // StreamInfo is mandatory, panic if not present.
        let info: StreamInfo = config.stream_info.unwrap();
        // Scaling policy is mandatory, panic if not present.
        let scaling_policy = config.scaling_policy.unwrap();

        StreamConfiguration {
            scoped_stream: ScopedStream::from(info),
            scaling: Scaling {
                scale_type: num::FromPrimitive::from_i32(scaling_policy.scale_type).unwrap(),
                target_rate: scaling_policy.target_rate,
                scale_factor: scaling_policy.scale_factor,
                min_num_segments: scaling_policy.min_num_segments,
            },
            retention: config
                .retention_policy
                .map(|ret| Retention {
                    retention_type: num::FromPrimitive::from_i32(ret.retention_type).unwrap(),
                    retention_param: ret.retention_param,
                })
                .unwrap_or_default(),
            tags: config.tags.map(|tags| tags.tag),
        }
    }
}
impl From<pravega_client_shared::StreamCut> for crate::controller::StreamCut {
    fn from(cut: pravega_client_shared::StreamCut) -> crate::controller::StreamCut {
        crate::controller::StreamCut {
            stream_info: Some(cut.scoped_stream.into()),
            cut: cut.segment_offset_map,
        }
    }
}

impl<'a> From<&'a pravega_client_shared::StreamCut> for crate::controller::StreamCut {
    fn from(value: &'a pravega_client_shared::StreamCut) -> crate::controller::StreamCut {
        crate::controller::StreamCut {
            stream_info: Some(StreamInfo::from(&value.scoped_stream)),
            cut: value.segment_offset_map.to_owned(),
        }
    }
}

impl<'a> From<&'a SegmentRange> for SegmentWithRange {
    fn from(value: &'a SegmentRange) -> SegmentWithRange {
        SegmentWithRange::new(
            ScopedSegment::from(value.segment_id.clone().unwrap()),
            OrderedFloat(value.min_key),
            OrderedFloat(value.max_key),
        )
    }
}

impl From<SegmentId> for ScopedSegment {
    fn from(value: SegmentId) -> ScopedSegment {
        let stream_info: StreamInfo = value.stream_info.unwrap();
        ScopedSegment {
            scope: Scope::from(stream_info.scope),
            stream: Stream::from(stream_info.stream),
            segment: Segment::from(value.segment_id),
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
        let txn_uuid: u128 = match txn_response.txn_id {
            Some(x) => (x.high_bits as u128) << 64 | (x.low_bits as u128),
            None => panic!("Incorrect response from Controller"),
        };
        TxnSegments::new(StreamSegments::new(segment_map), TxId::from(txn_uuid))
    }
}

impl From<SuccessorResponse> for StreamSegmentsWithPredecessors {
    fn from(successor_response: SuccessorResponse) -> StreamSegmentsWithPredecessors {
        let s: Vec<successor_response::SegmentEntry> = successor_response.segments;
        let mut successor_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
        for e in &s {
            let seg_range: SegmentWithRange = SegmentWithRange::from(&e.segment.clone().unwrap());
            let pred_segm: Vec<Segment> = e
                .value
                .iter()
                .map(|&x| Segment::from(x))
                .collect::<Vec<Segment>>();
            successor_map.insert(seg_range, pred_segm);
        }
        // convert std::collections::HashMap to im::hashmap::HashMap
        StreamSegmentsWithPredecessors::new(successor_map.into())
    }
}
