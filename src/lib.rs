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
    clippy::option_unwrap_used,
    clippy::result_unwrap_used,
    clippy::similar_names
)]
#![allow(clippy::multiple_crate_versions)]
#[macro_use]
extern crate derive_new;

pub mod byte_stream;
pub mod client_factory;
pub mod raw_client;
pub mod segment_reader;
