//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::tablemap::TableError;
use pravega_client_retry::retry_result::RetryError;
use pravega_client_shared::{TransactionStatus, TxId};
use pravega_connection_pool::connection_pool::ConnectionPoolError;
use pravega_controller_client::ControllerError;
use pravega_wire_protocol::error::*;
use pravega_wire_protocol::wire_commands::Replies;
use serde_cbor::Error as CborError;
use snafu::Snafu;
use std::fmt::Debug;
use tokio::time::error::Elapsed;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum RawClientError {
    #[snafu(display("Auth token has expired, refresh to try again: {}", reply))]
    AuthTokenExpired { reply: Replies },

    #[snafu(display("Failed to get connection from connection pool: {}", source))]
    GetConnectionFromPool { source: ConnectionPoolError },

    #[snafu(display("Failed to write request: {}", source))]
    WriteRequest { source: ClientConnectionError },

    #[snafu(display("Failed to read reply: {}", source))]
    ReadReply { source: ClientConnectionError },

    #[snafu(display("Reply incompatible wirecommand version: low {}, high {}", low, high))]
    IncompatibleVersion { low: i32, high: i32 },

    #[snafu(display("Request has timed out: {:?}", source))]
    RequestTimeout { source: Elapsed },
}

impl RawClientError {
    pub fn is_token_expired(&self) -> bool {
        matches!(self, RawClientError::AuthTokenExpired { .. })
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum SegmentWriterError {
    #[snafu(display("Failed to send request to the processor"))]
    SendToProcessor {},

    #[snafu(display("The size limit is {} while actual size is {}", limit, size))]
    EventSizeTooLarge { limit: usize, size: usize },

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

    #[snafu(display("Wrong host: {:?}", error_msg))]
    WrongHost { error_msg: String },

    #[snafu(display("Reactor is closed due to: {:?}", msg))]
    ReactorClosed { msg: String },

    #[snafu(display("Conditional append has failed"))]
    ConditionalCheckFailed {},
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum TransactionalEventStreamWriterError {
    #[snafu(display("Pinger failed to {:?}", msg))]
    PingerError { msg: String },

    #[snafu(display("Controller client failed with error {:?}", source))]
    TxnStreamControllerError { source: ControllerError },
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum TransactionError {
    #[snafu(display("Transactional failed to write due to {:?}", error_msg))]
    TxnSegmentWriterError { error_msg: String },

    #[snafu(display("Transactional stream writer failed due to {:?}", source))]
    TxnStreamWriterError {
        source: TransactionalEventStreamWriterError,
    },

    #[snafu(display("Transaction {:?} already closed", id))]
    TxnClosed { id: TxId },

    #[snafu(display("Transaction failed due to controller error: {:?}", source))]
    TxnControllerError { source: ControllerError },

    #[snafu(display("Commit Transaction {:?} error due to Transaction {:?}", id, status))]
    TxnCommitError { id: TxId, status: TransactionStatus },

    #[snafu(display("Abort Transaction {:?} error due to Transaction {:?}", id, status))]
    TxnAbortError { id: TxId, status: TransactionStatus },
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum SerdeError {
    #[snafu(display("Failed to {:?} due to {:?}", msg, source))]
    Cbor { msg: String, source: CborError },
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum SynchronizerError {
    #[snafu(display(
        "Table Synchronizer failed while performing {:?} with table error: {:?}",
        operation,
        source
    ))]
    SyncTableError { operation: String, source: TableError },

    #[snafu(display("Failed to run update function in table synchronizer due to: {:?}", error_msg))]
    SyncUpdateError { error_msg: String },

    #[snafu(display("Failed insert tombestone in table synchronizer due to: {:?}", error_msg))]
    SyncTombstoneError { error_msg: String },
}
