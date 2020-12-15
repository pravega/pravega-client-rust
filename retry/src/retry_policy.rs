/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

use std::iter::Iterator;
use std::time::Duration;
use std::u64::MAX as U64_MAX;

/// The retry policy that can retry something with
/// backoff policy.
pub trait BackoffSchedule: Iterator<Item = Duration> {}

/// Any implementation which implements the Iterator trait would also implement BackoffSchedule.
impl<T> BackoffSchedule for T where T: Iterator<Item = Duration> {}

/// The retry policy that can retry something with
/// exp backoff policy.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct RetryWithBackoff {
    current: u64,
    base: u64,
    max_delay: Option<Duration>,
}

impl RetryWithBackoff {
    /// Constructs a new exponential back-off strategy,
    /// using default setting.
    pub fn default() -> RetryWithBackoff {
        let delay = Some(Duration::from_millis(10000));
        RetryWithBackoff {
            current: 1,
            base: 10,
            max_delay: delay,
        }
    }

    /// Constructs a new exponential back-off strategy,
    /// given a base duration in milliseconds.
    pub fn from_millis(base: u64) -> RetryWithBackoff {
        RetryWithBackoff {
            current: base,
            base,
            max_delay: None,
        }
    }

    /// Apply a maximum delay. No retry delay will be longer than this `Duration`.
    pub fn max_delay(mut self, duration: Duration) -> RetryWithBackoff {
        self.max_delay = Some(duration);
        self
    }

    /// Apply a the max number of tries.
    pub fn max_tries(self, tries: i32) -> std::iter::Take<RetryWithBackoff> {
        self.take(tries as usize)
    }
}

impl Iterator for RetryWithBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        // set delay duration by applying factor
        let duration = Duration::from_millis(self.current);

        // check if we reached max delay
        if let Some(ref max_delay) = self.max_delay {
            if duration > *max_delay {
                return Some(*max_delay);
            }
        }

        if let Some(next) = self.current.checked_mul(self.base) {
            self.current = next;
        } else {
            self.current = U64_MAX;
        }

        Some(duration)
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
fn test_saturates_at_maximum_value() {
    let mut s = RetryWithBackoff::from_millis(U64_MAX - 1);
    assert_eq!(s.next(), Some(Duration::from_millis(U64_MAX - 1)));
    assert_eq!(s.next(), Some(Duration::from_millis(U64_MAX)));
    assert_eq!(s.next(), Some(Duration::from_millis(U64_MAX)));
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
