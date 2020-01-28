use std::time::Duration;

/// A result that represents either success, retryable error,or immediately-returning error.
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug, Hash)]
pub enum RetryResult<T, E> {
    /// Contains the success value.
    Success(T),
    /// Contains the error value if duration is exceeded.
    Retry(E),
    /// Contains an error value to return immediately.
    Err(E),
}


/// An error with a retryable operation.
#[derive(Debug, PartialEq, Eq)]
pub enum RetryError<E> {
    Operation {
        /// The error returned by the operation on the last try.
        error: E,
        /// The duration spent waiting between retries of the operation.
        total_delay: Duration,
        /// The total number of times the operation was tried.
        tries: u64,
    },
    /// Something went wrong in the internal logic.
    Internal(String),
}

