use std::time::Duration;
use std::thread::sleep;
use super::retry_result::Retry;
use super::retry_result::RetryError;

/// Retry the given operation synchronously until it succeeds, or until the given `Duration`
/// iterator ends.
pub fn retry<I, O, T, E> (iterable: I, mut operation: O) -> Result<T, RetryError<E>>
where
    I: IntoIterator<Item = Duration>,
    O: FnMut() -> Result<T, Retry<E>>,
{
    retry_internal(iterable, |_| operation())
}

fn retry_internal <I, O, T, E> (iterable: I, mut operation: O) -> Result<T, RetryError<E>>
    where
        I: IntoIterator<Item = Duration>,
        O: FnMut(u64) -> Result<T, Retry<E>>,

{
    let mut iterator = iterable.into_iter();
    let mut current_try = 1;
    let mut total_delay = Duration::default();
    // Must use return(for early return).
    loop {
        match operation(current_try) {
            Ok(value) => return Ok(value),
            Err(Retry::Retry(error)) => {
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
            Err(Retry::Err(error)) => {
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
    use super::retry;
    use super::Retry;
    use super::RetryError;
    use std::time::Duration;
    use snafu::Snafu;

    #[derive(Debug,PartialEq, Eq, Snafu)]
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
        let value = retry(retry_policy, || match collection.next(){
            Some(n) if n == 5 => Ok(n),
            Some(_) => Err(Retry::Retry("not 5")),
            None => Err(Retry::Err("to the end")),
        }).unwrap();
        assert_eq!(value, 5);
    }

    #[test]
    fn test_succeeds_with_maximum_retries() {
        let retry_policy = RetryWithBackoff::default().max_tries(1);
        let mut collection = vec![1, 2].into_iter();
        let value = retry(retry_policy, || match collection.next(){
            Some(n) if n == 2 => Ok(n),
            Some(_) => Err(Retry::Retry("not 2")),
            None => Err(Retry::Err("to the end")),
        }).unwrap();
        assert_eq!(value, 2);
    }

    #[test]
    fn test_fails_after_last_retry() {
        let retry_policy = RetryWithBackoff::default().max_tries(1);
        let mut collection = vec![1, 2].into_iter();
        let res = retry(retry_policy, || match collection.next(){
            Some(n) if n == 3 => Ok(n),
            Some(_) => Err(Retry::Retry("retry")),
            None => Err(Retry::Err("to the end")),
        });

        assert_eq!(res, Err(RetryError{
            error: "retry",
            tries: 2,
            total_delay: Duration::from_millis(1),
        }));
    }

    #[test]
    fn test_fails_with_non_retryable_err() {
        let retry_policy = RetryWithBackoff::default().max_tries(1);
        let mut collection = vec![1].into_iter();
        let res = retry(retry_policy, || match collection.next(){
            Some(n) if n == 3 => Ok(n),
            Some(_) => Err(Retry::Err("non-retry")),
            None => Err(Retry::Err("to the end")),
        });
        assert_eq!(res, Err(RetryError{
            error: "non-retry",
            tries: 1,
            total_delay: Duration::from_millis(0),
        }));
    }

    #[test]
    fn test_succeeds_with_snafu_error() {
        let retry_policy = RetryWithBackoff::default().max_tries(1);
        let mut collection = vec![1, 2].into_iter();
        let res = retry(retry_policy, || match collection.next(){
            Some(n) if n == 3 => Ok(n),
            Some(_) => Err(Retry::Retry(SnafuError::Retryable)),
            None => Err(Retry::Err(SnafuError::Nonretryable)),
        });
        assert_eq!(res, Err(RetryError{
            error: SnafuError::Retryable,
            tries: 2,
            total_delay: Duration::from_millis(1),
        }));
    }
}