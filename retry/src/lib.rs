/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

//! Retry is a crate for retrying something that can fail with exponential backoff.
//! It is designed to have a declarative interface for ease of use.
//! It can be used as follows:
//! ```
//! # use pravega_client_retry::retry_policy::RetryWithBackoff;
//! # use pravega_client_retry::retry_result::RetryResult;
//! # use pravega_client_retry::retry_sync::retry_sync;
//! let retry_policy = RetryWithBackoff::default().max_tries(1);
//! let mut collection = vec![1, 2].into_iter();
//! let value = retry_sync(retry_policy, || match collection.next() {
//!     Some(n) if n == 2 => RetryResult::Success(n),
//!     Some(_) => RetryResult::Retry("not 2"),
//!     None => RetryResult::Fail("to the end"),
//! }).unwrap();
//!
//! assert_eq!(value, 2);
//!
//! ```
//! The above will retry the code  1 times if it throws Err(Retry::Retry).
//! If it throws a Err(Retry::Err) or returns successfully it will return immediately.
//! The delay following each of the filed attempts would be 1, 10,respectively.
//! If all retries fail, it will return Err(RetryErr) that has error message.
//!

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

pub mod retry_async;
pub mod retry_policy;
pub mod retry_result;
pub mod retry_sync;
