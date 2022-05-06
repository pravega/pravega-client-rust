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
use std::error::Error;
use std::thread::sleep;
use std::time::Duration;

/// Retry the given operation synchronously until it succeeds, or until the given `Duration` end.
/// retry_schedule: The retry policy that has max retry times and retry delay.
/// It can be used as follows:
/// let retry_policy = RetryWithBackoff::default();
/// let mut collection = vec![1, 2].into_iter();
/// let res = retry_sync(retry_policy, || match collection.next() {
///     Some(n) if n == 3 => RetryResult::Success(n),
///     Some(_) => RetryResult::Retry(SnafuError::Retryable),
///     None => RetryResult::Fail(SnafuError::Nonretryable),
/// });

pub fn retry_sync<O, T, E>(retry_schedule: impl BackoffSchedule, mut operation: O) -> Result<T, RetryError<E>>
where
    O: FnMut() -> RetryResult<T, E>,
    E: Error,
{
    retry_internal(retry_schedule, |_| operation())
}

pub fn retry_internal<O, T, E>(
    retry_schedule: impl BackoffSchedule,
    mut operation: O,
) -> Result<T, RetryError<E>>
where
    O: FnMut(u64) -> RetryResult<T, E>,
    E: Error,
{
    let mut iterator = retry_schedule;
    let mut current_try = 1;
    let mut total_delay = Duration::default();
    // Must use return(for early return).
    loop {
        match operation(current_try) {
            RetryResult::Success(value) => return Ok(value),
            RetryResult::Retry(error) => {
                if let Some(delay) = iterator.next() {
                    sleep(delay);
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
#[allow(deprecated)]
mod tests {
    use super::super::retry_policy::RetryWithBackoff;
    use super::retry_sync;
    use super::RetryError;
    use super::RetryResult;
    use snafu::Snafu;
    use std::time::Duration;

    #[derive(Debug, PartialEq, Eq, Snafu)]
    pub enum SnafuError {
        #[snafu(display("Retryable error"))]
        Retryable,
        #[snafu(display("NonRetryable error"))]
        Nonretryable,
    }

    #[test]
    fn test_succeeds_with_default_setting() {
        let retry_policy = RetryWithBackoff::default();
        let mut collection = vec![1, 2, 3, 4, 5].into_iter();
        let value = retry_sync(retry_policy, || match collection.next() {
            Some(n) if n == 5 => RetryResult::Success(n),
            Some(_) => RetryResult::Retry(SnafuError::Retryable),
            None => RetryResult::Fail(SnafuError::Nonretryable),
        })
        .unwrap();
        assert_eq!(value, 5);
    }

    #[test]
    fn test_succeeds_with_maximum_retries() {
        let retry_policy = RetryWithBackoff::default().max_tries(1);
        let mut collection = vec![1, 2].into_iter();
        let value = retry_sync(retry_policy, || match collection.next() {
            Some(n) if n == 2 => RetryResult::Success(n),
            Some(_) => RetryResult::Retry(SnafuError::Retryable),
            None => RetryResult::Fail(SnafuError::Nonretryable),
        })
        .unwrap();
        assert_eq!(value, 2);
    }

    #[test]
    fn test_fails_after_last_retry() {
        let retry_policy = RetryWithBackoff::default().max_tries(1);
        let mut collection = vec![1, 2].into_iter();
        let res = retry_sync(retry_policy, || match collection.next() {
            Some(n) if n == 3 => RetryResult::Success(n),
            Some(_) => RetryResult::Retry(SnafuError::Retryable),
            None => RetryResult::Fail(SnafuError::Nonretryable),
        });

        assert_eq!(
            res,
            Err(RetryError {
                error: SnafuError::Retryable,
                tries: 2,
                total_delay: Duration::from_millis(1),
            })
        );
    }

    #[test]
    fn test_fails_with_non_retryable_err() {
        let retry_policy = RetryWithBackoff::default().max_tries(1);
        let mut collection = vec![1].into_iter();
        let res = retry_sync(retry_policy, || match collection.next() {
            Some(n) if n == 3 => RetryResult::Success(n),
            Some(_) => RetryResult::Fail(SnafuError::Nonretryable),
            None => RetryResult::Fail(SnafuError::Nonretryable),
        });
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
    fn test_succeeds_with_snafu_error() {
        let retry_policy = RetryWithBackoff::default().max_tries(1);
        let mut collection = vec![1, 2].into_iter();
        let res = retry_sync(retry_policy, || match collection.next() {
            Some(n) if n == 3 => RetryResult::Success(n),
            Some(_) => RetryResult::Retry(SnafuError::Retryable),
            None => RetryResult::Fail(SnafuError::Nonretryable),
        });
        assert_eq!(
            res,
            Err(RetryError {
                error: SnafuError::Retryable,
                tries: 2,
                total_delay: Duration::from_millis(1),
            })
        );
    }
}
