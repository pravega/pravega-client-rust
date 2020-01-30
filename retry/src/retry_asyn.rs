use std::time::{Duration, Instant};
use futures::{Async, IntoFuture, Future, Poll};
use futures::future::Either;
use tokio_timer::Delay;
use super::retry_result::RetryError;
use super::retry_result::RetryState;
use super::retry_result::Retry;

/// Future that drives multiple attempts at an operation.
pub struct RetryFuture <I, O, T, E> where I: IntoIterator<Item = Duration>, O: FnMut() -> Result<T, Retry<E>>{
    strategy: I::IntoIter,
    state: RetryState<T, Retry<E>>,
    operation: O,
    total_delay: Duration,
    tries: u64,
}

/// Retry the given operation asynchronously until it succeeds,
/// or until the given Duration iterator ends.
pub fn retry_asyn<I, O, T, E>(strategy: I, operation: O) -> RetryFuture<I, O, T, E>
    where
        I: IntoIterator<Item = Duration>,
        O: FnMut() -> Result<T, Retry<E>>
{
    RetryFuture::spawn(strategy, operation)
}


impl<I, O, T, E> RetryFuture<I, O, T, E>  where I: IntoIterator<Item = Duration>, O: FnMut() -> Result<T, Retry<E>> {
    fn spawn(iterable: I, mut operation: O)  -> RetryFuture<I, O, T, E> {

        RetryFuture{
            strategy: iterable.into_iter(),
            state: RetryState::Running(operation().into_future()),
            operation: operation,
            total_delay: Duration::default(),
            tries: 1,
        }
    }

    fn attempt(&mut self) -> Poll<T, RetryError<E>> {
        let future = (self.operation)().into_future();
        self.state = RetryState::Running(future);
        return self.poll();
    }

    fn retry(&mut self, err: E) -> Poll<T, RetryError<E>> {
        match self.strategy.next() {
            None => Err(RetryError{
                error: err,
                total_delay: self.total_delay,
                tries: self.tries
            }),
            Some(duration) => {
                let deadline = Instant::now() + duration;
                self.total_delay += duration;
                self.tries += 1;
                let future = Delay::new(deadline);
                self.state = RetryState::Sleeping(future);
                self.poll()
            }
        }
    }
}


impl<I, O, T, E> Future for RetryFuture<I, O, T, E> where I: IntoIterator<Item = Duration>,
                                                          O: FnMut() -> Result<T, Retry<E>> {
    type Item = T;
    type Error = RetryError<E>;

    /// Query this future to see if its value has become available.
    /// This function returns `Async::NotReady` if the future is not ready yet,
    /// `Err` if the future is finished but resolved to an error, or
    /// `Async::Ready` with the result of this future if it's finished
    /// successfully. Once a future has finished it is considered a contract
    /// error to continue polling the future.
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let result = match self.state {
            RetryState::Running(ref mut future) =>
                Either::A(future.poll()),
            RetryState::Sleeping(ref mut future) =>
                Either::B(future.poll())
        };

        match result {
            Either::A(poll_result) =>  match poll_result {
                Ok(ok) => Ok(ok),
                Err(err) => match err {
                    Retry::Retry(e) => {
                        self.retry(e)
                    }
                    Retry::Err(e) => {
                        Err(RetryError{
                            error: e,
                            total_delay: self.total_delay,
                            tries: self.tries
                        })
                    }
                }
            }
            Either::B(poll_result) =>  match poll_result {
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Ok(Async::Ready(_)) => self.attempt(),
                Err(_) => panic!()
            }
        }
    }
}