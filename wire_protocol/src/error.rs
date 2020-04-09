//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use super::connection_factory::ConnectionType;
use crate::wire_commands::Replies;
use bincode2::Error as BincodeError;
use snafu::{Backtrace, Snafu};
use std::io::Error as IoError;
use std::net::SocketAddr;

/// This kind of error that can be produced during Pravega client connecting to server.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum ConnectionError {
    #[snafu(display("Could not send data to {} asynchronously: {}", endpoint, source))]
    SendData {
        endpoint: SocketAddr,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Could not read data from {} asynchronously: {}", endpoint, source))]
    ReadData {
        endpoint: SocketAddr,
        source: std::io::Error,
        backtrace: Backtrace,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum ConnectionFactoryError {
    #[snafu(display(
        "Could not connect to endpoint {} using connection type {}: {}",
        endpoint,
        connection_type,
        source
    ))]
    Connect {
        connection_type: ConnectionType,
        endpoint: SocketAddr,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to verify the connection: {}", source))]
    Verify { source: ClientConnectionError },
}

/// This kind of error that can be produced during Pravega serialize and deserialize the wire commands.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum CommandError {
    #[snafu(display(
        "Could not serialize/deserialize command {} because of: {}",
        command_type,
        source
    ))]
    InvalidData {
        command_type: i32,
        source: BincodeError,
        backtrace: Backtrace,
    },
    #[snafu(display(
        "Could not serialize/deserialize command {} because of: {}",
        command_type,
        source
    ))]
    Io {
        command_type: i32,
        source: IoError,
        backtrace: Backtrace,
    },
    #[snafu(display(
        "Could not serialize/deserialize command {} because of: Unknown Command",
        command_type
    ))]
    InvalidType { command_type: i32, backtrace: Backtrace },
}

/// This kind of error that can be produced during Pravega client connection.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum ClientConnectionError {
    #[snafu(display("Failed to read wirecommand {} : {}", part, source))]
    Read {
        part: String,
        source: ConnectionError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to write wirecommand: {}", source))]
    Write {
        source: ConnectionError,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "The payload size {} exceeds the max wirecommand size {}",
        payload_size,
        max_wirecommand_size
    ))]
    PayloadLengthTooLong {
        payload_size: u32,
        max_wirecommand_size: u32,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to encode wirecommand: {}", source))]
    EncodeCommand {
        source: CommandError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode wirecommand: {}", source))]
    DecodeCommand {
        source: CommandError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to write/read since connection is split"))]
    ConnectionIsSplit {},

    #[snafu(display("Wrong Hello Wirecommand reply: current wire version {} and oldest compatible version {}, but get {} and {}", wire_version, oldest_compatible, wire_version_received, oldest_compatible_received))]
    WrongHelloVersion {
        wire_version: i32,
        oldest_compatible: i32,
        wire_version_received: i32,
        oldest_compatible_received: i32,
    },

    #[snafu(display("Expect to receive Hello Wirecommand but get {}", reply))]
    WrongReply { reply: Replies },
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum ConnectionPoolError {
    #[snafu(display("Could not establish connection to endpoint: {}", endpoint))]
    EstablishConnection { endpoint: String, error_msg: String },

    #[snafu(display("No available connection in the internal pool"))]
    NoAvailableConnection {},
}
