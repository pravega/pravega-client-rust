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
#![allow(clippy::multiple_crate_versions, clippy::needless_doctest_main)]

use pcg_rand::Pcg32;
use rand::{Rng, SeedableRng};
use std::cell::RefCell;
use std::sync::atomic::{AtomicI64, Ordering};

pub mod byte_stream;
pub mod client_factory;
pub mod error;
pub mod event_stream_writer;
#[macro_use]
pub mod metrics;
pub mod raw_client;
pub mod reader_group;
pub mod segment_metadata;
mod segment;
pub mod segment_reader;
mod stream;
pub mod table_synchronizer;
pub mod tablemap;
pub mod trace;
pub mod transaction;

thread_local! {
    pub(crate) static RNG: RefCell<Pcg32> = RefCell::new(Pcg32::from_entropy());
}

pub(crate) static REQUEST_ID_GENERATOR: AtomicI64 = AtomicI64::new(0);

///
/// Function used to generate request ids for all the modules.
///
pub(crate) fn get_request_id() -> i64 {
    REQUEST_ID_GENERATOR.fetch_add(1, Ordering::SeqCst) + 1
}

/// Function used to generate random u64.
pub(crate) fn get_random_u64() -> u64 {
    RNG.with(|rng| rng.borrow_mut().gen())
}

/// Function used to generate random u128.
pub(crate) fn get_random_u128() -> u128 {
    RNG.with(|rng| rng.borrow_mut().gen())
}

/// Function used to generate random i64.
pub(crate) fn get_random_f64() -> f64 {
    RNG.with(|rng| rng.borrow_mut().gen())
}

#[macro_use]
extern crate derive_new;
