/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

use num::checked_pow;
use std::iter::Iterator;
use std::time::{Duration, Instant};

/// The retry policy that can retry something with
/// backoff policy.
pub trait BackoffSchedule: Iterator<Item = Duration> {}

/// Any implementation which implements the Iterator trait would also implement BackoffSchedule.
impl<T> BackoffSchedule for T where T: Iterator<Item = Duration> {}

/// The retry policy that can retry something with
/// exp backoff policy.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct RetryWithBackoff {
    attempt: usize,

    initial_delay: Duration,
    backoff_coefficient: u32,
    max_attempt: Option<usize>,
    max_delay: Option<Duration>,
    expiration_time: Option<Instant>,
}

impl RetryWithBackoff {
    /// Constructs a new exponential back-off strategy,
    /// using default setting.
    pub fn default() -> RetryWithBackoff {
        RetryWithBackoff {
            attempt: 0,

            initial_delay: Duration::from_millis(1),
            backoff_coefficient: 10,
            max_delay: Some(Duration::from_millis(10000)),
            max_attempt: None,
            expiration_time: None,
        }
    }

    /// Apply a initial delay.
    pub fn initial_delay(mut self, initial_delay: Duration) -> RetryWithBackoff {
        self.initial_delay = initial_delay;
        self
    }

    /// Apply a backoff coefficient.
    pub fn backoff_coefficient(mut self, backoff_coefficient: u32) -> RetryWithBackoff {
        self.backoff_coefficient = backoff_coefficient;
        self
    }

    /// Apply a maximum attempt. No retry attempt will be larger than this `usize`.
    pub fn max_attempt(mut self, attempt: usize) -> RetryWithBackoff {
        self.max_attempt = Some(attempt);
        self
    }

    /// Apply a maximum delay. No retry delay will be longer than this `Duration`.
    pub fn max_delay(mut self, duration: Duration) -> RetryWithBackoff {
        self.max_delay = Some(duration);
        self
    }

    /// Apply a expiration time. No retry will be performed after this `Instant`.
    pub fn expiration_time(mut self, time: Instant) -> RetryWithBackoff {
        self.expiration_time = Some(time);
        self
    }

    #[deprecated(since = "0.4.0", note = "please use `initial_delay` instead")]
    /// Constructs a new exponential back-off strategy,
    /// given a base duration in milliseconds.
    pub fn from_millis(base: u64) -> RetryWithBackoff {
        RetryWithBackoff {
            attempt: 0,

            initial_delay: Duration::from_millis(base),
            backoff_coefficient: base as u32,
            max_delay: None,
            max_attempt: None,
            expiration_time: None,
        }
    }

    #[deprecated(since = "0.4.0", note = "please use `max_attempt` instead")]
    /// Apply a the max number of tries.
    pub fn max_tries(self, tries: i32) -> std::iter::Take<RetryWithBackoff> {
        self.take(tries as usize)
    }
}

impl Iterator for RetryWithBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        if let Some(expiration_time) = self.expiration_time {
            if expiration_time <= Instant::now() {
                return None;
            }
        }
        if let Some(max_attempt) = self.max_attempt {
            if self.attempt >= max_attempt {
                return None;
            }
        }

        self.attempt += 1;
        let coefficient = checked_pow(self.backoff_coefficient, self.attempt - 1);
        let delay = coefficient.and_then(|coefficient| self.initial_delay.checked_mul(coefficient));

        if delay.is_some() && self.max_delay.is_some() {
            if delay < self.max_delay {
                delay
            } else {
                self.max_delay
            }
        } else {
            delay.or(self.max_delay)
        }
    }
}

#[test]
fn test_uses_default_setting() {
    let mut s = RetryWithBackoff::default();

    assert_eq!(s.next(), Some(Duration::from_millis(1)));
    assert_eq!(s.next(), Some(Duration::from_millis(10)));
    assert_eq!(s.next(), Some(Duration::from_millis(100)));
    assert_eq!(s.next(), Some(Duration::from_millis(1000)));
}

#[test]
fn test_returns_some_exponential_base_10() {
    let mut s = RetryWithBackoff::from_millis(10);

    assert_eq!(s.next(), Some(Duration::from_millis(10)));
    assert_eq!(s.next(), Some(Duration::from_millis(100)));
    assert_eq!(s.next(), Some(Duration::from_millis(1000)));
}

#[test]
#[allow(deprecated)]
fn test_returns_with_finite_retries() {
    let mut s = RetryWithBackoff::from_millis(10).max_tries(5);
    assert_eq!(s.next(), Some(Duration::from_millis(10)));
    assert_eq!(s.next(), Some(Duration::from_millis(100)));
    assert_eq!(s.next(), Some(Duration::from_millis(1000)));
    assert_eq!(s.next(), Some(Duration::from_millis(10000)));
    assert_eq!(s.next(), Some(Duration::from_millis(100000)));
    assert_eq!(s.next(), None);
}
#[test]
fn test_returns_some_exponential_base_2() {
    let mut s = RetryWithBackoff::from_millis(2);

    assert_eq!(s.next(), Some(Duration::from_millis(2)));
    assert_eq!(s.next(), Some(Duration::from_millis(4)));
    assert_eq!(s.next(), Some(Duration::from_millis(8)));
}

#[test]
fn stops_increasing_at_max_delay() {
    let mut s = RetryWithBackoff::from_millis(2).max_delay(Duration::from_millis(4));

    assert_eq!(s.next(), Some(Duration::from_millis(2)));
    assert_eq!(s.next(), Some(Duration::from_millis(4)));
    assert_eq!(s.next(), Some(Duration::from_millis(4)));
}

#[test]
fn returns_max_when_max_less_than_base() {
    let mut s = RetryWithBackoff::from_millis(20).max_delay(Duration::from_millis(10));
    assert_eq!(s.next(), Some(Duration::from_millis(10)));
    assert_eq!(s.next(), Some(Duration::from_millis(10)));
}

#[cfg(test)]
mod tests {
    use std::{ops::Add, thread};

    use super::*;

    #[test]
    fn test_without_max_attempt_without_max_delay_without_expiration_time() {
        let mut s = RetryWithBackoff::default()
            .initial_delay(Duration::from_millis(1))
            .backoff_coefficient(2);

        assert_eq!(s.next(), Some(Duration::from_millis(1)));
        assert_eq!(s.next(), Some(Duration::from_millis(2)));
        assert_eq!(s.next(), Some(Duration::from_millis(4)));
        assert_eq!(s.next(), Some(Duration::from_millis(8)));
    }

    #[test]
    fn test_with_max_attempt_without_max_delay_without_expiration_time() {
        let mut s = RetryWithBackoff::default()
            .initial_delay(Duration::from_millis(1))
            .backoff_coefficient(2)
            .max_attempt(4);

        assert_eq!(s.next(), Some(Duration::from_millis(1)));
        assert_eq!(s.next(), Some(Duration::from_millis(2)));
        assert_eq!(s.next(), Some(Duration::from_millis(4)));
        assert_eq!(s.next(), Some(Duration::from_millis(8)));
        assert_eq!(s.next(), None);
    }

    #[test]
    fn test_without_max_attempt_without_max_delay_with_expiration_time() {
        let mut s = RetryWithBackoff::default()
            .initial_delay(Duration::from_millis(1))
            .backoff_coefficient(2)
            .max_delay(Duration::from_millis(12));

        assert_eq!(s.next(), Some(Duration::from_millis(1)));
        assert_eq!(s.next(), Some(Duration::from_millis(2)));
        assert_eq!(s.next(), Some(Duration::from_millis(4)));
        assert_eq!(s.next(), Some(Duration::from_millis(8)));
        assert_eq!(s.next(), Some(Duration::from_millis(12)));
        assert_eq!(s.next(), Some(Duration::from_millis(12)));
    }

    #[test]
    fn test_without_max_attempt_with_max_delay_without_expiration_time() {
        let sleep_duration = Duration::from_millis(10);
        let mut s = RetryWithBackoff::default()
            .initial_delay(Duration::from_millis(1))
            .backoff_coefficient(2)
            .expiration_time(Instant::now().add(sleep_duration));

        assert_eq!(s.next(), Some(Duration::from_millis(1)));
        assert_eq!(s.next(), Some(Duration::from_millis(2)));
        assert_eq!(s.next(), Some(Duration::from_millis(4)));
        assert_eq!(s.next(), Some(Duration::from_millis(8)));

        thread::sleep(sleep_duration);

        assert_eq!(s.next(), None);
    }

    #[test]
    fn test_with_max_attempt_with_max_delay_without_expiration_time() {
        let mut s = RetryWithBackoff::default()
            .initial_delay(Duration::from_millis(1))
            .backoff_coefficient(2)
            .max_attempt(8)
            .max_delay(Duration::from_millis(12));

        assert_eq!(s.next(), Some(Duration::from_millis(1)));
        assert_eq!(s.next(), Some(Duration::from_millis(2)));
        assert_eq!(s.next(), Some(Duration::from_millis(4)));
        assert_eq!(s.next(), Some(Duration::from_millis(8)));
        assert_eq!(s.next(), Some(Duration::from_millis(12)));
        assert_eq!(s.next(), Some(Duration::from_millis(12)));
        assert_eq!(s.next(), Some(Duration::from_millis(12)));
        assert_eq!(s.next(), Some(Duration::from_millis(12)));
        assert_eq!(s.next(), None);
    }
}
