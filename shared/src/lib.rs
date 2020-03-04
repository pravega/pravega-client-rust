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
    clippy::option_unwrap_used,
    clippy::result_unwrap_used,
    clippy::similar_names
)]
#![allow(clippy::multiple_crate_versions)]

use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fmt::Write;
use std::fmt::{Display, Formatter};
use uuid::Uuid;

#[macro_use]
extern crate shrinkwraprs;

#[macro_use]
extern crate derive_new;

#[derive(new, Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct PravegaNodeUri(pub String);

#[derive(new, Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct DelegationToken(pub String);

#[derive(new, Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct Timestamp(pub u64);

#[derive(new, Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct Scope {
    pub name: String,
}

#[derive(new, Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct Stream {
    pub name: String,
}

#[derive(new, Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct Segment {
    pub number: i64,
}

#[derive(new, Debug, Clone, Hash, PartialEq, Eq)]
pub struct ScopedStream {
    pub scope: Scope,
    pub stream: Stream,
}

#[derive(new, Debug, Clone, Hash, PartialEq, Eq)]
pub struct ScopedSegment {
    pub scope: Scope,
    pub stream: Stream,
    pub segment: Segment,
}

#[derive(new, Shrinkwrap, Copy, Clone, Hash, PartialEq, Eq)]
pub struct TxId(pub u128);

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct WriterId(pub u64);

impl Display for Stream {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)?;
        Ok(())
    }
}

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

impl Display for Scope {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)?;
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
        f.write_str(&self.scope.name)?;
        f.write_char('/')?;
        f.write_str(&self.stream.name)?;
        f.write_char('/')?;
        f.write_fmt(format_args!("{}", self.segment.number))?;
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

#[derive(new, Debug, Clone, Hash, Eq, PartialEq)]
pub struct SegmentWithRange {
    pub scoped_segment: ScopedSegment,
    pub min_key: OrderedFloat<f64>,
    pub max_key: OrderedFloat<f64>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct StreamSegments {
    pub key_segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange>,
}

impl StreamSegments {
    pub fn new(map_key_segment: BTreeMap<OrderedFloat<f64>, SegmentWithRange>) -> StreamSegments {
        StreamSegments::is_valid(&map_key_segment).expect("Invalid key segment map");
        StreamSegments {
            key_segment_map: map_key_segment,
        }
    }

    fn is_valid(map: &BTreeMap<OrderedFloat<f64>, SegmentWithRange>) -> Result<(), String> {
        println!("Validation invoked");
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
                "Segments should have values only upto 1.0"
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
}

#[derive(new, Debug, Clone, Hash, PartialEq, Eq)]
pub struct TxnSegments {
    pub key_segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange>,
    pub tx_id: TxId,
}

#[cfg(test)]
mod test;
