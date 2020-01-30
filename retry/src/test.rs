use super::retry_asyn::retry_asyn;
use super::retry_policy::RetryWithBackoff;
use super::retry_result::Retry;
use super::retry_result::RetryError;
use futures::sync::oneshot::spawn;
use futures::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

#[test]
fn attempts_just_once() {
    let runtime = Runtime::new().unwrap();
    let retry_policy = RetryWithBackoff::default().max_tries(1);
    let counter = Arc::new(AtomicUsize::new(0));
    let cloned_counter = counter.clone();
    let future = retry_asyn(retry_policy, move || {
        cloned_counter.fetch_add(1, Ordering::SeqCst);
        Err::<(), Retry<&str>>(Retry::Err("not retry"))
    });

    let res = spawn(future, &runtime.executor()).wait();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
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
    let runtime = Runtime::new().unwrap();
    let retry_policy = RetryWithBackoff::default().max_tries(3);
    let counter = Arc::new(AtomicUsize::new(0));
    let cloned_counter = counter.clone();
    let future = retry_asyn(retry_policy, move || {
        cloned_counter.fetch_add(1, Ordering::SeqCst);
        Err::<(), Retry<&str>>(Retry::Retry("retry"))
    });

    let res = spawn(future, &runtime.executor()).wait();
    assert_eq!(counter.load(Ordering::SeqCst), 4);
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
    let runtime = Runtime::new().unwrap();
    let retry_policy = RetryWithBackoff::default().max_tries(3);
    let counter = Arc::new(AtomicUsize::new(0));
    let cloned_counter = counter.clone();
    let future = retry_asyn(retry_policy, move || {
        let previous = cloned_counter.fetch_add(1, Ordering::SeqCst);
        if previous < 3 {
            Err::<i32, Retry<&str>>(Retry::Retry("retry"))
        } else {
            Ok::<i32, Retry<&str>>(previous as i32)
        }
    });
    let res = spawn(future, &runtime.executor()).wait();
    assert_eq!(res, Ok(3));
    assert_eq!(counter.load(Ordering::SeqCst), 4);
}
