//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

//! The Byte API for writing and reading data to a segment in raw bytes.
//!
//! Byte streams are restricted to single-segment streams, but, unlike event streams, there is no event framing,
//! so a reader cannot distinguish separate writes for you, and, if needed, you must do so by
//! convention or by protocol in a layer above the reader.
//!
//! ## Note
//! * Sharing a single-segment stream between the byte API and the Event readers/writers will
//!   CORRUPT YOUR DATA in an unrecoverable way.
//! * Sharing a single-segment stream between multiple byte writers is possible but it might generate
//!   interleaved data.

//!
//! [`ByteWriter`]: ByteWriter
//! [`ByteReader`]: ByteReader
//!

pub mod writer;
#[doc(inline)]
pub use writer::ByteWriter;

pub mod reader;
#[doc(inline)]
pub use reader::ByteReader;
