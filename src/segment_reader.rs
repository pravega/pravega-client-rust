//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::raw_client::RawClient;
use async_trait::async_trait;
use pravega_rust_client_shared::{EventRead, ScopedSegment};
use snafu::Snafu;
use std::result::Result as StdResult;

#[derive(Debug, Snafu)]
pub enum ClientError {
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    OperationError {
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Could not connect to controller {}", endpoint))]
    ConnectionError {
        can_retry: bool,
        endpoint: String,
        error_msg: String,
    },
}

#[async_trait]
pub trait AsyncSegmentReader {
    async fn read(offset: i64) -> StdResult<EventRead, ClientError>;
}

#[derive(new)]
struct AsyncSegmentReaderImpl<'a> {
    segment: &'a ScopedSegment,
    raw_client: &'a dyn RawClient<'a>,
}

#[async_trait]
impl<'a> AsyncSegmentReader for AsyncSegmentReaderImpl<'a> {
    async fn read(offset: i64) -> StdResult<EventRead, ClientError> {
        Ok(EventRead::new(Vec::new()))
    }
}
