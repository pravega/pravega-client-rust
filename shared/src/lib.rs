/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
#![deny(
    clippy::all,
    clippy::cargo,
    clippy::else_if_without_else,
    clippy::empty_line_after_outer_attr,
    clippy::multiple_inherent_impl,
    clippy::mut_mut,
    clippy::path_buf_push_overwrite
)]
#![warn(
    clippy::cargo_common_metadata,
    clippy::mutex_integer,
    clippy::needless_borrow,
    clippy::similar_names
)]
#![allow(clippy::multiple_crate_versions)]

pub mod naming_utils;

use crate::naming_utils::NameUtils;
use derive_more::{Display, From};
use encoding_rs::mem;
use im::HashMap as ImHashMap;
use im::OrdMap;
use murmurhash3::murmurhash3_x64_128;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::cmp::{min, Reverse};
use std::collections::{BTreeMap, HashMap};
use std::convert::From;
use std::fmt;
use std::fmt::Write;
use std::fmt::{Display, Formatter};
use std::ops::Index;
use uuid::Uuid;

#[macro_use]
extern crate shrinkwraprs;

#[macro_use]
extern crate derive_new;

#[derive(From, Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct PravegaNodeUri(pub String);

#[derive(From, Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct DelegationToken(pub String);

#[derive(From, Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct Timestamp(pub u64);

#[derive(From, Shrinkwrap, Debug, Display, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    pub name: String,
}

#[derive(From, Shrinkwrap, Debug, Display, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stream {
    pub name: String,
}

#[derive(From, Shrinkwrap, Debug, Display, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reader {
    pub name: String,
}

#[derive(From, Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    pub number: i64,
    pub tx_id: Option<TxId>,
}

impl Segment {
    pub fn from(number: i64) -> Self {
        Segment { number, tx_id: None }
    }

    pub fn from_id_and_epoch(segment_id: i32, epoch: i32) -> Self {
        let epoch_i64 = (epoch as i64) << 32;
        let id_i64 = segment_id as i64;
        Segment {
            number: epoch_i64 + id_i64,
            tx_id: None,
        }
    }

    pub fn from_txn(number: i64, tx_id: TxId) -> Self {
        Segment {
            number,
            tx_id: Some(tx_id),
        }
    }

    pub fn is_transaction_segment(&self) -> bool {
        self.tx_id.is_some()
    }
}

#[derive(new, Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedStream {
    pub scope: Scope,
    pub stream: Stream,
}

#[derive(new, Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedSegment {
    pub scope: Scope,
    pub stream: Stream,
    pub segment: Segment,
}

impl ScopedSegment {
    pub fn get_scoped_stream(&self) -> ScopedStream {
        ScopedStream::new(self.scope.clone(), self.stream.clone())
    }
}

impl From<&str> for ScopedSegment {
    fn from(qualified_name: &str) -> Self {
        if NameUtils::is_transaction_segment(qualified_name) {
            let original_segment_name = NameUtils::get_parent_stream_segment_name(qualified_name);
            ScopedSegment::from(original_segment_name)
        } else {
            let mut tokens = NameUtils::extract_segment_tokens(qualified_name.to_owned());
            if tokens.len() == 2 {
                // scope not present
                let segment_id = tokens.pop().expect("get segment id from tokens");
                let stream_name = tokens.pop().expect("get stream name from tokens");
                ScopedSegment {
                    scope: Scope {
                        name: String::from(""),
                    },
                    stream: Stream { name: stream_name },
                    segment: Segment {
                        number: segment_id.parse::<i64>().expect("parse string to i64"),
                        tx_id: None,
                    },
                }
            } else {
                let segment_id = tokens.pop().expect("get segment id from tokens");
                let stream_name = tokens.pop().expect("get stream name from tokens");
                let scope = tokens.pop().expect("get scope from tokens");
                ScopedSegment {
                    scope: Scope { name: scope },
                    stream: Stream { name: stream_name },
                    segment: Segment {
                        number: segment_id.parse::<i64>().expect("parse string to i64"),
                        tx_id: None,
                    },
                }
            }
        }
    }
}

#[derive(From, Shrinkwrap, Copy, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxId(pub u128);

impl TxId {
    ///
    /// Obtain epoch from a given Transaction Id.
    ///
    pub fn get_epoch(&self) -> i32 {
        (self.0 >> 96) as i32
    }
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct WriterId(pub u64);

impl Display for TxId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(Uuid::from_u128(self.0).to_hyphenated().to_string().as_str())?;
        Ok(())
    }
}

impl fmt::Debug for TxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Uuid::from_u128(self.0).to_hyphenated().to_string().as_str())?;
        Ok(())
    }
}

impl Display for ScopedStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.scope.name)?;
        f.write_char('/')?;
        f.write_str(&self.stream.name)?;
        Ok(())
    }
}

impl Display for ScopedSegment {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&NameUtils::get_qualified_stream_segment_name(
            &self.scope.name,
            &self.stream.name,
            self.segment.number,
            self.segment.tx_id,
        ))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ScaleType {
    FixedNumSegments = 0,
    ByRateInKbytesPerSec = 1,
    ByRateInEventsPerSec = 2,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Scaling {
    pub scale_type: ScaleType,
    pub target_rate: i32,
    pub scale_factor: i32,
    pub min_num_segments: i32,
}

impl Default for Scaling {
    fn default() -> Self {
        Scaling {
            scale_type: ScaleType::FixedNumSegments,
            min_num_segments: 1,
            scale_factor: 1,
            target_rate: 1000,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum RetentionType {
    None = 0,
    Time = 1,
    Size = 2,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum PingStatus {
    Ok = 0,
    Committed = 1,
    Aborted = 2,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum TransactionStatus {
    Open = 0,
    Committing = 1,
    Committed = 2,
    Aborting = 3,
    Aborted = 4,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Retention {
    pub retention_type: RetentionType,
    pub retention_param: i64,
}

impl Default for Retention {
    fn default() -> Self {
        Retention {
            retention_type: RetentionType::None,
            retention_param: std::i64::MAX,
        }
    }
}

#[derive(new, Debug, Clone, Hash, PartialEq, Eq)]
pub struct StreamConfiguration {
    pub scoped_stream: ScopedStream,
    pub scaling: Scaling,
    pub retention: Retention,
}

#[derive(new, Debug, Clone)]
pub struct StreamCut {
    pub scoped_stream: ScopedStream,
    pub segment_offset_map: HashMap<i64, i64>,
}

const PREFIX_LENGTH: usize = 2;

#[derive(new, Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct SegmentWithRange {
    pub scoped_segment: ScopedSegment,
    pub min_key: OrderedFloat<f64>,
    pub max_key: OrderedFloat<f64>,
}

impl SegmentWithRange {
    pub fn get_segment(&self) -> Segment {
        self.scoped_segment.segment.clone()
    }
}

impl fmt::Display for SegmentWithRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let segment_str = self.scoped_segment.to_string();
        write!(
            f,
            "{:02}{}{}-{}",
            segment_str.len(),
            segment_str,
            self.min_key,
            self.max_key
        )
    }
}

impl From<&str> for SegmentWithRange {
    fn from(name: &str) -> Self {
        let segment_name_length: usize = name[..PREFIX_LENGTH].parse().expect("parse prefix length");

        let segment_str = &*name[PREFIX_LENGTH..PREFIX_LENGTH + segment_name_length]
            .parse::<String>()
            .expect("parse segment name");

        let scoped_segment: ScopedSegment = segment_str.into();

        let rest_string = name[PREFIX_LENGTH + segment_name_length..]
            .parse::<String>()
            .expect("parse segment name");

        let mut parts: Vec<&str> = rest_string.split('-').collect();
        let max_key = parts
            .pop()
            .expect("get max key")
            .parse::<OrderedFloat<f64>>()
            .expect("parse OrderedFloat from str");
        let min_key = parts
            .pop()
            .expect("get max key")
            .parse::<OrderedFloat<f64>>()
            .expect("parse OrderedFloat from str");

        SegmentWithRange {
            scoped_segment,
            min_key,
            max_key,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct StreamSegments {
    pub key_segment_map: OrdMap<OrderedFloat<f64>, SegmentWithRange>,
}

impl StreamSegments {
    const SEED: u64 = 1741865571; // This is the hashcode of String "EventRouter" in Java client

    pub fn new(map_key_segment: BTreeMap<OrderedFloat<f64>, SegmentWithRange>) -> StreamSegments {
        StreamSegments::is_valid(&map_key_segment).expect("Invalid key segment map");
        StreamSegments {
            key_segment_map: map_key_segment.into(),
        }
    }

    fn is_valid(map: &BTreeMap<OrderedFloat<f64>, SegmentWithRange>) -> Result<(), String> {
        if !map.is_empty() {
            let (min_key, _min_seg) = map.iter().next().expect("Error reading min key");
            let (max_key, _max_seg) = map.iter().next_back().expect("Error read max key");
            assert!(
                min_key.gt(&OrderedFloat(0.0)),
                "Min key is expected to be greater than 0.0"
            );
            assert!(max_key.ge(&OrderedFloat(1.0)), "Last Key is missing");
            assert!(
                max_key.lt(&OrderedFloat(1.0001)),
                "Segments should have values only up to 1.0"
            );
        }
        Ok(())
    }

    pub fn get_segment(&self, key: f64) -> ScopedSegment {
        assert!(OrderedFloat(key).ge(&OrderedFloat(0.0)), "Key should be >= 0.0");
        assert!(OrderedFloat(key).le(&OrderedFloat(1.0)), "Key should be <= 1.0");
        let r = self
            .key_segment_map
            .range(&OrderedFloat(key)..)
            .next()
            .expect("No matching segment found for the given key");
        r.1.scoped_segment.to_owned()
    }

    pub fn get_segment_for_string(&self, str: &str) -> ScopedSegment {
        let mut buffer_u16 = vec![0; str.len()];

        // convert uft-8 encoded Rust string to utf-16.
        mem::convert_str_to_utf16(str, &mut buffer_u16);

        // the utf-16 is stored as u16 array, convert it to u8 array
        let (prefix, buffer_u8, suffix) = unsafe { buffer_u16.align_to::<u8>() };
        assert!(prefix.is_empty());
        assert!(suffix.is_empty());

        let (upper, _lower) = murmurhash3_x64_128(buffer_u8, StreamSegments::SEED);

        // takes the first 64 bit as Java client uses asLong method.
        let key = u64_to_f64_fraction(upper);
        self.get_segment(key)
    }

    pub fn get_segments(&self) -> Vec<ScopedSegment> {
        self.key_segment_map
            .values()
            .map(|v| v.scoped_segment.to_owned())
            .collect::<Vec<ScopedSegment>>()
    }

    pub fn apply_replacement_range(
        &self,
        segment_replace: &Segment,
        replacement_ranges: &StreamSegmentsWithPredecessors,
    ) -> Result<StreamSegments, String> {
        let mut replaced_ranges = replacement_ranges
            .replacement_segments
            .get(segment_replace)
            .unwrap_or_else(|| panic!("Empty set of replacements"))
            .clone();

        replaced_ranges.sort_by_key(|k| Reverse(k.max_key));
        let replaced_ranges_ref = &replaced_ranges;
        StreamSegments::verify_continuous(replaced_ranges_ref)
            .expect("Replacement ranges are not continuous");

        let mut result: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
        for (key, seg) in self.key_segment_map.iter().rev() {
            if segment_replace.number == seg.scoped_segment.segment.number {
                // segment should be replaced.
                for new_segment in replaced_ranges_ref {
                    let lower_bound = self.key_segment_map.range(..key).next_back();
                    match lower_bound {
                        None => {
                            result.insert(min(new_segment.max_key, *key), new_segment.clone());
                        }
                        Some(lower_bound_value) => {
                            if new_segment.max_key.ge(&lower_bound_value.0) {
                                result.insert(min(new_segment.max_key, *key), new_segment.clone());
                            }
                        }
                    };
                }
            } else {
                result.insert(*key, seg.clone());
            }
        }

        Ok(StreamSegments::new(result))
    }

    fn verify_continuous(segment_replace_ranges: &[SegmentWithRange]) -> Result<(), String> {
        let mut previous = segment_replace_ranges.index(0).max_key;
        for x in segment_replace_ranges {
            if x.max_key.0.ne(&previous.0) {
                return Err("Replacement segments are not continuous".to_string());
            }
            previous = x.min_key;
        }
        Ok(())
    }
}

#[derive(new, Debug, Clone, Hash, PartialEq, Eq)]
pub struct TxnSegments {
    pub stream_segments: StreamSegments,
    pub tx_id: TxId,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct StreamSegmentsWithPredecessors {
    pub segment_with_predecessors: ImHashMap<SegmentWithRange, Vec<Segment>>,
    pub replacement_segments: ImHashMap<Segment, Vec<SegmentWithRange>>, // inverse lookup
}

impl StreamSegmentsWithPredecessors {
    pub fn new(
        segment_with_predecessor: ImHashMap<SegmentWithRange, Vec<Segment>>,
    ) -> StreamSegmentsWithPredecessors {
        let mut replacement_map: HashMap<Segment, Vec<SegmentWithRange>> = HashMap::new();
        for (segment, predecessor) in &segment_with_predecessor {
            for predecessor_segment in predecessor {
                let predecessor = predecessor_segment.clone();
                let mut replacement_segments = replacement_map
                    .get(&predecessor)
                    .get_or_insert(&Vec::new())
                    .clone();
                replacement_segments.push((*segment).clone());
                replacement_map.insert(predecessor, replacement_segments.to_vec());
            }
        }
        StreamSegmentsWithPredecessors {
            segment_with_predecessors: segment_with_predecessor,
            replacement_segments: replacement_map.into(), // convert to immutable map.
        }
    }
}

// convert u64 to 0.0 - 1.0 in f64
pub(crate) fn u64_to_f64_fraction(hash: u64) -> f64 {
    let shifted = (hash >> 12) & 0x000f_ffff_ffff_ffff_u64;
    f64::from_bits(0x3ff0_0000_0000_0000_u64 + shifted) - 1.0
}

#[derive(new, Debug, Clone, Hash, PartialEq, Eq)]
pub struct EventRead {
    pub event: Vec<u8>,
}

#[cfg(test)]
mod test {
    use super::*;
    use std::convert::From;

    #[test]
    fn test_hash() {
        let s = "hello";
        let mut buffer_u16 = vec![0; s.len()];

        mem::convert_str_to_utf16(s, &mut buffer_u16);

        let (prefix, buffer_u8, suffix) = unsafe { buffer_u16.align_to::<u8>() };
        assert!(prefix.is_empty());
        assert!(suffix.is_empty());

        let (upper, _lower) = murmurhash3_x64_128(&buffer_u8, StreamSegments::SEED);
        assert_eq!(u64_to_f64_fraction(upper), 0.658716230571337);
    }

    #[test]
    fn test_segment_with_range() {
        let segment = SegmentWithRange {
            scoped_segment: ScopedSegment {
                scope: Scope::from("scope".to_owned()),
                stream: Stream::from("stream".to_owned()),
                segment: Segment::from(0),
            },
            min_key: OrderedFloat::from(0.0),
            max_key: OrderedFloat::from(1.0),
        };

        let segment_string = &*segment.to_string();

        let segment_from_string: SegmentWithRange = segment_string.into();

        assert_eq!(segment_from_string, segment);
    }
}
