use super::retry_policy::BackoffSchedule;
use super::retry_result::RetryResult;
use super::retry_result::RetryError;
use tokio::time::delay_for;
use std::time::Duration;
use std::future::Future;

/// Retry the given operation asynchronously until it succeeds,
/// or until the given Duration iterator ends.
///
pub async fn retry_async<F, T, E>(retry_schedule: impl BackoffSchedule,
                                  mut operation: impl FnMut() -> F) -> Result<T, RetryError<E>>
    where
        F : Future<Output=RetryResult<T, E>>
{
    let mut iterator = retry_schedule.into_iter();
    let mut current_try = 1;
    let mut total_delay = Duration::default();
    loop {

        let result: RetryResult<T, E> = operation().await;

        match result {
            RetryResult::Success(value) => return Ok(value),
            RetryResult::Retry(error) => {
                if let Some(delay) = iterator.next() {
                    delay_for(delay).await;
                    current_try += 1;
                    total_delay += delay;
                } else {
                    return Err(RetryError {
                        error,
                        total_delay,
                        tries: current_try,
                    });
                }
            }
            RetryResult::Fail(error) => {
                return Err(RetryError {
                    error,
                    total_delay,
                    tries: current_try,
                });
            }
        }
    }
}

