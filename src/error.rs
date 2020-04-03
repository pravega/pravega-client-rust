//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_controller_client::ControllerError;
use pravega_rust_client_retry::retry_result::RetryError;
use pravega_wire_protocol::error::*;
use pravega_wire_protocol::wire_commands::Replies;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum RawClientError {
    #[snafu(display("Failed to get connection from connection pool: {}", source))]
    GetConnectionFromPool { source: ConnectionPoolError },

    #[snafu(display("Failed to write request: {}", source))]
    WriteRequest { source: ClientConnectionError },

    #[snafu(display("Failed to read reply: {}", source))]
    ReadReply { source: ClientConnectionError },

    #[snafu(display("Reply incompatible wirecommand version: low {}, high {}", low, high))]
    IncompatibleVersion { low: i32, high: i32 },
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum EventStreamWriterError {
    #[snafu(display("Failed to send request to the processor"))]
    SendToProcessor {},

    #[snafu(display("The size limit is {} while actual size is {}", limit, size))]
    EventSizeTooLarge { limit: i32, size: i32 },

    #[snafu(display("Failed to parse to an Event Command: {}", source))]
    ParseToEventCommand { source: CommandError },

    #[snafu(display("Failed to send request to segmentstore due to: {:?}", source))]
    SegmentWriting { source: ClientConnectionError },

    #[snafu(display("Retry failed due to error: {:?}", err))]
    RetryControllerWriting { err: RetryError<ControllerError> },

    #[snafu(display("Retry connection pool failed due to error {:?}", err))]
    RetryConnectionPool { err: RetryError<ConnectionPoolError> },

    #[snafu(display("Retry raw client failed due to error {:?}", err))]
    RetryRawClient { err: RetryError<RawClientError> },

    #[snafu(display("Wrong reply, expected {:?} but get {:?}", expected, actual))]
    WrongReply { expected: String, actual: Replies },
}
