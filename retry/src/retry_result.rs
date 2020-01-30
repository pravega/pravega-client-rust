use futures::future::FutureResult;
use std::time::Duration;
use tokio_timer::Delay;

///
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug, Hash)]
pub enum Retry<E> {
    /// Contains the error value if duration is exceeded.
    Retry(E),
    /// Contains an error value to return immediately.
    Err(E),
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

/// Keep track of the state of the future
/// currently sleeping or executing the operation.
pub enum RetryState<T, E> {
    Running(FutureResult<T, E>),
    Sleeping(Delay),
}
