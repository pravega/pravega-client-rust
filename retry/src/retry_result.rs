//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

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
#[derive(Debug, PartialEq, Eq)]
pub struct RetryError<E> {
    /// The error returned by the operation on the last try.
    pub error: E,
    /// The duration spent waiting between retries of the operation.
    pub total_delay: Duration,
    /// The total number of times the operation was tried.
    pub tries: u64,
}
