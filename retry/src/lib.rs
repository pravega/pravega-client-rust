//! Retry is a crate for retrying something that can fail with exponential backoff.
//! It is designed to have a declarative interface for ease of use.
//! It can be used as follows:
//!
//!
//!
//!
//!
//! The above will retry the code in the block up to 5 times if it throws Error.
//! If it throws a RuntimeException or returns successfully it will throw or return immediately.
//! The delay following each of the filed attempts would be 1, 10, 100, 1000, and 10000ms respectively.
//! If all retries fail {@link RetriesExhaustedException} will be thrown.
//!



mod retry;
mod retry_result;
mod retry_policy;
