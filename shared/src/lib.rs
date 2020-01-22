use std::fmt;
use std::fmt::Write;
use std::fmt::{Display, Formatter};

#[macro_use]
extern crate shrinkwraprs;

#[derive(Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct PravegaNodeUri(String);

#[derive(Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct DelegationToken(String);

#[derive(Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct Timestamp(u64);

#[derive(Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct Scope {
    name: String,
}

#[derive(Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct Stream {
    name: String,
}

#[derive(Shrinkwrap, Debug, Clone, Hash, PartialEq, Eq)]
pub struct Segment {
    number: u64,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ScopedStream {
    scope: Scope,
    stream: Stream,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ScopedSegment {
    scope: Scope,
    stream: Stream,
    segment: Segment,
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

pub struct StreamConfiguration {
    //TODO
}

pub struct StreamCut {
    //TODO
}

pub struct StreamSegments {
    //TODO
}

pub struct TxnSegments {
    //TODO
}
