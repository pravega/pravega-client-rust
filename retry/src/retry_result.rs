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
