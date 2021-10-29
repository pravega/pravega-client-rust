//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use snafu::Snafu;
use std::fmt::Debug;
use std::time::Duration;

/// The RetryResult that the operation should return.
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug, Hash)]
pub enum RetryResult<T, E> {
    /// Contains the return value if the operation succeed.
    Success(T),
    /// Contains the error value if duration is exceeded.
    Retry(E),
    /// Contains an error value to return immediately.
    Fail(E),
}

/// An error that the Retry function would give.
#[derive(Debug, PartialEq, Eq, Snafu)]
#[snafu(display(
    "Failed after {} retries due to {} which took {:?}",
    tries,
    error,
    total_delay
))]
pub struct RetryError<E: std::fmt::Display> {
    /// The error returned by the operation on the last try.
    pub error: E,
    /// The duration spent waiting between retries of the operation.
    pub total_delay: Duration,
    /// The total number of times the operation was tried.
    pub tries: u64,
}

///
/// Trait which is used check if the Error is Retryable.
///
pub trait Retryable {
    fn can_retry(&self) -> bool;
}

///
/// `wrap_with_async_retry!` macro wraps any arbitrary async function with `pravega_rust_client_retry::retry_async::retry_async`
/// This macro takes two parameters. The first parameter is the Retry policy which implements `trait BackoffSchedule`.
/// The second parameter is the async function that needs to be wrapped within the retry logic.
/// The function invocation will be retried only error returned by the function returns `can_retry()` as true.
///
/// E.g: usage
///
/// ```ignore
/// use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
/// use pravega_rust_client_retry::retry_async::retry_async;
/// use pravega_rust_client_retry::wrap_with_async_retry;
/// //CustomError implements Retrayable trait
/// async fn function_a(param1: &str, param2:u8) -> Result<(), CustomError> {
///
/// }
/// let retry_policy = RetryWithBackoff::default().max_tries(10);
/// // the below invocation wraps function_a with the retry logic.
/// wrap_with_async_retry!(retry_policy, function_a("test", 1));
/// ```
///
#[macro_export]
macro_rules! wrap_with_async_retry {
    ($retrypolicy:expr, $expression:expr) => {
        retry_async($retrypolicy, || async {
            let r = $expression.await;
            match r {
                Ok(res) => RetryResult::Success(res),
                Err(e) => {
                    if e.can_retry() {
                        RetryResult::Retry(e)
                    } else {
                        RetryResult::Fail(e)
                    }
                }
            }
        })
        .await
    };
}

///
/// `wrap_with_sync_retry!` macro wraps any arbitrary synchronous function with `pravega_rust_client_retry::retry_sync::retry_sync`
/// This macro takes two parameters. The first parameter is the Retry policy which implements `trait BackoffSchedule`.
/// The second parameter is the synchrounous function that needs to be wrapped within the retry logic.
/// The function invocation will be retried only error returned by the function returns `can_retry()` as true.
///
/// E.g: usage
///
/// ```ignore
/// use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
/// use pravega_rust_client_retry::retry_sync::retry_sync;
/// use pravega_rust_client_retry::wrap_with_sync_retry;
/// // CustomError implements Retryable trait
/// fn function_a(param1: &str, param2:u8) -> Result<(), CustomError>{
///
/// }
/// let retry_policy = RetryWithBackoff::default().max_tries(5);
/// // the below invocation wraps function_a with the retry logic.
/// wrap_with_sync_retry!(retry_policy, function_a("test", 1));
/// ```
///
#[macro_export]
macro_rules! wrap_with_sync_retry {
    ($retrypolicy:expr, $expression:expr) => {
        retry_sync($retrypolicy, || {
            let r = $expression;
            match r {
                Ok(res) => RetryResult::Success(res),
                Err(e) => {
                    if e.can_retry() {
                        RetryResult::Retry(e)
                    } else {
                        RetryResult::Fail(e)
                    }
                }
            }
        })
        .await
    };
}
