//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use bincode2::Error as BincodeError;
use pravega_connection_pool::connection_pool::ConnectionPoolError;
use pravega_controller_client::ControllerError;
use pravega_rust_client_retry::retry_result::RetryError;
use pravega_rust_client_shared::{ScopedSegment, TransactionStatus, TxId};
use pravega_wire_protocol::error::*;
use pravega_wire_protocol::wire_commands::Replies;
use snafu::Snafu;
use std::fmt::Debug;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::oneshot;

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
pub enum SegmentWriterError {
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

    #[snafu(display("Segment {:?} is sealed", segment))]
    SegmentIsSealed { segment: ScopedSegment },

    #[snafu(display("No such segment {:?}", segment))]
    NoSuchSegment { segment: ScopedSegment },

    #[snafu(display("Unexpected error from {:?}", segment))]
    Unexpected { segment: ScopedSegment },
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
pub enum TransactionalEventSegmentWriterError {
    #[snafu(display("Mpsc failed with error {:?}", source))]
    MpscError { source: TryRecvError },

    #[snafu(display("Mpsc closed with sender dropped"))]
    MpscSenderDropped {},

    #[snafu(display("Oneshot failed with error {:?}", source))]
    OneshotError { source: oneshot::error::TryRecvError },

    #[snafu(display("EventSegmentWriter failed due to {:?}", source))]
    WriterError { source: SegmentWriterError },

    #[snafu(display("Unexpected reply from segmentstore {:?}", error))]
    UnexpectedReply { error: Replies },
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub")]
pub enum TransactionError {
    #[snafu(display("Transactional segment writer failed due to {:?}", source))]
    TxnSegmentWriterError {
        source: TransactionalEventSegmentWriterError,
    },

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
    Serde { msg: String, source: BincodeError },
}
