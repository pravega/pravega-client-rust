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

use std::collections::HashMap;
use std::fmt;
use std::fmt::Write;
use std::fmt::{Display, Formatter};

#[macro_use]
extern crate shrinkwraprs;

#[macro_use]
extern crate derive_new;

#[derive(new, Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct PravegaNodeUri(String);

#[derive(new, Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct DelegationToken(String);

#[derive(new, Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct Timestamp(u64);

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

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct TxId(u128);

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct WriterId(u64);

impl Display for Stream {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)?;
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

pub struct StreamSegments {
    //TODO
}

pub struct TxnSegments {
    //TODO
}
