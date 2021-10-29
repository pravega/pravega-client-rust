/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

use super::retry_policy::BackoffSchedule;
use super::retry_result::RetryError;
use super::retry_result::RetryResult;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

/// Retry the given operation asynchronously until it succeeds,
/// or until the given Duration iterator ends.
/// It can be used as follows:
/// let retry_policy = RetryWithBackoff::default();
/// let future = retry_async(retry_policy, || async {
///     let previous = 1;
///     match previous {
///         1 => RetryResult::Fail("not retry"),
///         2 => RetryResult::Success(previous),
///         _ => RetryResult::Retry("retry"),
///     }
/// });
pub async fn retry_async<F, T, E>(
    retry_schedule: impl BackoffSchedule,
    mut operation: impl FnMut() -> F,
) -> Result<T, RetryError<E>>
where
    F: Future<Output = RetryResult<T, E>>,
    E: std::fmt::Display
{
    let mut iterator = retry_schedule;
    let mut current_try = 1;
    let mut total_delay = Duration::default();
    loop {
        let result: RetryResult<T, E> = operation().await;

        match result {
            RetryResult::Success(value) => return Ok(value),
            RetryResult::Retry(error) => {
                if let Some(delay) = iterator.next() {
                    sleep(delay).await;
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

#[cfg(test)]
mod tests {
    use super::super::retry_policy::RetryWithBackoff;
    use super::retry_async;
    use super::RetryError;
    use super::RetryResult;
    use std::time::Duration;
    use tokio::runtime::Runtime;
    use snafu::Snafu;

    #[derive(Debug, PartialEq, Eq, Snafu)]
    pub enum SnafuError {
        #[snafu(display("Retryable error"))]
        Retryable,
        #[snafu(display("NonRetryable error"))]
        Nonretryable,
    }

    #[test]
    fn attempts_just_once() {
        let runtime = Runtime::new().unwrap();
        let retry_policy = RetryWithBackoff::default().max_tries(1);
        let future = retry_async(retry_policy, || async {
            let previous = 1;
            match previous {
                1 => RetryResult::Fail(SnafuError::Nonretryable),
                2 => RetryResult::Success(previous),
                _ => RetryResult::Retry(SnafuError::Retryable),
            }
        });
        let res = runtime.block_on(future);
        assert_eq!(
            res,
            Err(RetryError {
                error: SnafuError::Nonretryable,
                tries: 1,
                total_delay: Duration::from_millis(0),
            })
        );
    }

    #[test]
    fn attempts_until_max_retries_exceeded() {
        let runtime = Runtime::new().unwrap();
        let retry_policy = RetryWithBackoff::default().max_tries(3);
        let future = retry_async(retry_policy, || async {
            let previous = 3;
            match previous {
                1 => RetryResult::Fail(SnafuError::Nonretryable),
                2 => RetryResult::Success(previous),
                _ => RetryResult::Retry(SnafuError::Retryable),
            }
        });

        let res = runtime.block_on(future);
        assert_eq!(res.err().unwrap().tries, 4);
    }

    #[test]
    fn attempts_until_success() {
        let runtime = Runtime::new().unwrap();
        let retry_policy = RetryWithBackoff::default().max_tries(3);
        let mut counter = 0;

        let future = retry_async(retry_policy, || {
            let previous = counter;
            counter += 1;
            async move {
                if previous < 3 {
                    RetryResult::Retry(SnafuError::Retryable)
                } else {
                    RetryResult::Success(previous)
                }
            }
        });
        let res = runtime.block_on(future);
        assert_eq!(res, Ok(3));
        assert_eq!(counter, 4);
    }
}
