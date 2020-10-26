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

use crate::client_factory::ClientFactory;
use pcg_rand::Pcg32;
use pravega_rust_client_shared::{
    Retention, RetentionType, ScaleType, Scaling, Scope, ScopedStream, Stream, StreamConfiguration,
};
use rand::{Rng, SeedableRng};
use std::cell::RefCell;
use std::sync::atomic::{AtomicI64, Ordering};

pub mod byte_stream;
pub mod client_factory;
pub mod error;
pub mod event_reader;
pub mod event_stream_writer;
#[macro_use]
pub mod metric;
pub mod raw_client;
mod reactor;
pub mod reader_group;
pub mod segment_metadata;
pub mod segment_reader;
pub mod segment_slice;
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

// helper method
async fn create_stream(factory: &ClientFactory, scope: &str, stream: &str) {
    factory
        .get_controller_client()
        .create_scope(&Scope {
            name: scope.to_string(),
        })
        .await
        .unwrap();
    factory
        .get_controller_client()
        .create_stream(&StreamConfiguration {
            scoped_stream: ScopedStream {
                scope: Scope {
                    name: scope.to_string(),
                },
                stream: Stream {
                    name: stream.to_string(),
                },
            },
            scaling: Scaling {
                scale_type: ScaleType::FixedNumSegments,
                target_rate: 0,
                scale_factor: 0,
                min_num_segments: 1,
            },
            retention: Retention {
                retention_type: RetentionType::None,
                retention_param: 0,
            },
        })
        .await
        .unwrap();
}
