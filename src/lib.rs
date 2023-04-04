//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//

#![allow(dead_code)]
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
// clippy::result_large_err will be fixed by https://github.com/pravega/pravega-client-rust/issues/413.
#![allow(
    clippy::multiple_crate_versions,
    clippy::result_large_err,
    clippy::needless_doctest_main
)]
#![allow(bare_trait_objects)]
#![recursion_limit = "1024"]
#![allow(clippy::redundant_allocation)]

//! A Rust client for [Pravega].
//!
//! [Pravega] is an open source storage system implementing Streams as first-class
//! primitive for storing/serving continuous and unbounded data. It has a number of exciting features
//! including Exactly Once Semantics, Auto Scaling, Unlimited Retention and etc. More details
//! at the [website].
//!
//! Pravega client in Rust provides a few APIs at high level:
//! * [Event] provides a way to write and read discrete item.
//! * [Byte] provides a way to write and read raw bytes.
//!
//! [Pravega]: https://www.pravega.io/
//! [website]: http://pravega.io/docs/latest/key-features/#pravega-key-features
//! [Event]: crate::event
//! [Byte]: crate::byte
//!
pub mod byte;
pub mod client_factory;
pub mod event;
pub mod index;
pub mod sync;

#[cfg(feature = "cli")]
pub mod cli;
pub(crate) mod segment;
#[cfg(feature = "integration-test")]
#[doc(hidden)]
pub mod test_utils;
#[doc(hidden)]
#[macro_use]
pub mod util;
#[macro_use]
extern crate derive_new;
pub mod error;
