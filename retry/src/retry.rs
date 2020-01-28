use std::time::Duration;
use std::thread::sleep;
use super::retry_result::RetryResult;
use super::retry_result::RetryError;
/// Retry the given operation synchronously until it succeeds, or until the given `Duration`
/// iterator ends.
pub fn retry<I, O, T, E> (iterable: I, mut operation: O) -> Result<T, RetryError<E>>
where
    I: IntoIterator<Item = Duration>,
    O: FnMut() -> RetryResult<T, E>,
{
    retry_internal(iterable, |_| operation())
}

fn retry_internal <I, O, T, E> (iterable: I, mut operation: O) -> Result<T, RetryError<E>>
    where
        I: IntoIterator<Item = Duration>,
        O: FnMut(u64) -> RetryResult<T, E>,

{
    let mut iterator = iterable.into_iter();
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
                    return Err(RetryError::Operation {
                        error,
                        total_delay,
                        tries: current_try,
                    });
                }
            }
            RetryResult::Err(error) => {
               return Err(RetryError::Operation {
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
    use super::super::retry_result::RetryResult;
    use super::super::retry_result::RetryError;
    use std::time::Duration;
    use snafu::s
    /*
    use snafu::{Snafu};

    #[derive(Debug, Snafu)]
    pub enum SnafuError {
        Retryable,
        Nonretryable,
    }
    */

    #[test]
    fn test_succeeds_with_default_setting() {
        let retry_policy = RetryWithBackoff::default();
        let mut collection = vec![1, 2, 3, 4, 5].into_iter();
        let value = retry(retry_policy, || match collection.next(){
            Some(n) if n == 5 => RetryResult::Success(n),
            Some(_) => RetryResult::Retry("not 5"),
            None => RetryResult::Err("to the end"),
        }).unwrap();
        assert_eq!(value, 5);
    }

    #[test]
    fn test_succeeds_with_maximum_retries() {
        let retry_policy = RetryWithBackoff::default().max_tries(1);
        let mut collection = vec![1, 2].into_iter();
        let value = retry(retry_policy, || match collection.next(){
            Some(n) if n == 2 => RetryResult::Success(n),
            Some(_) => RetryResult::Retry("not 2"),
            None => RetryResult::Err("to the end"),
        }).unwrap();
        assert_eq!(value, 2);
    }

    #[test]
    fn test_fails_after_last_retry() {
        let retry_policy = RetryWithBackoff::default().max_tries(1);
        let mut collection = vec![1, 2].into_iter();
        let res = retry(retry_policy, || match collection.next(){
            Some(n) if n == 3 => RetryResult::Success(n),
            Some(_) => RetryResult::Retry("retry"),
            None => RetryResult::Err("to the end"),
        });

        assert_eq!(res, Err(RetryError::Operation{
            error: "retry",
            tries: 2,
            total_delay: Duration::from_millis(1),
        }));
    }

    #[test]
    fn test_fails_with_nonretriable_err() {
        let retry_policy = RetryWithBackoff::default().max_tries(1);
        let mut collection = vec![1].into_iter();
        let res = retry(retry_policy, || match collection.next(){
            Some(n) if n == 3 => RetryResult::Success(n),
            Some(_) => RetryResult::Err("non-retry"),
            None => RetryResult::Err("to the end"),
        });
        assert_eq!(res, Err(RetryError::Operation{
            error: "non-retry",
            tries: 1,
            total_delay: Duration::from_millis(0),
        }));
    }

    #[test]
    fn test_succeeds_with_snafu_error() {

    }
}