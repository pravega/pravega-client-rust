use super::connection_factory::ConnectionType;
use bincode2::Error as BincodeError;
use snafu::{Backtrace, Snafu};
use std::io::Error as IoError;
use std::net::SocketAddr;

/// This kind of error that can be produced during Pravega client connecting to server.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum ConnectionError {
    #[snafu(display(
        "Could not connect to endpoint {} using connection type {}",
        endpoint,
        connection_type
    ))]
    Connect {
        connection_type: ConnectionType,
        endpoint: SocketAddr,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Could not send data to {} asynchronously", endpoint))]
    SendData {
        endpoint: SocketAddr,
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Could not read data from {} asynchronously", endpoint))]
    ReadData {
        endpoint: SocketAddr,
        source: std::io::Error,
        backtrace: Backtrace,
    },
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

/// This kind of error that can be produced during Pravega read Wire Commands.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum ReaderError {
    #[snafu(display("Failed to read wirecommand {}", part))]
    ReadWirecommand {
        part: String,
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
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum ConnectionPoolError {
    //    #[snafu(display("Could not connect to endpoint"))]
    //    Connect { source: ConnectionError },
    #[snafu(display("Could not get connection from internal pool: {}", message))]
    GetConnection { message: String },
}
