use super::retry_async::retry_async;
use super::retry_policy::RetryWithBackoff;
use super::retry_result::RetryError;
use super::retry_result::RetryResult;
use std::time::Duration;
use tokio::runtime::Runtime;

#[test]
fn attempts_just_once() {
    let mut runtime = Runtime::new().unwrap();
    let retry_policy = RetryWithBackoff::default().max_tries(1);
    let future = retry_async(retry_policy, || {
        async {
            let previous = 1;
            match previous {
                1 => RetryResult::Fail("not retry"),
                2 => RetryResult::Success(previous),
                _ => RetryResult::Retry("retry"),
            }
        }
    });
    let res = runtime.block_on(future);
    assert_eq!(
        res,
        Err(RetryError {
            error: "not retry",
            tries: 1,
            total_delay: Duration::from_millis(0)
        })
    );
}

#[test]
fn attempts_until_max_retries_exceeded() {
    let mut runtime = Runtime::new().unwrap();
    let retry_policy = RetryWithBackoff::default().max_tries(3);
    let future = retry_async(retry_policy, || {
        async {
            let previous = 3;
            match previous {
                1 => RetryResult::Fail("not retry"),
                2 => RetryResult::Success(previous),
                _ => RetryResult::Retry("retry"),
            }
        }
    });

    let res = runtime.block_on(future);
    assert_eq!(
        res,
        Err(RetryError {
            error: "retry",
            tries: 4,
            total_delay: Duration::from_millis(111)
        })
    );
}

#[test]
fn attempts_until_success() {
    let mut runtime = Runtime::new().unwrap();
    let retry_policy = RetryWithBackoff::default().max_tries(3);
    let mut counter = 0;

    let future = retry_async(retry_policy, || {
        let previous = counter;
        counter += 1;
        async move {
            if previous < 3 {
                RetryResult::Retry("retry")
            } else {
                RetryResult::Success(previous)
            }
        }
    });
    let res = runtime.block_on(future);
    assert_eq!(res, Ok(3));
    assert_eq!(counter, 4);
}
