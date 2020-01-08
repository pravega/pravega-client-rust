use std::net::SocketAddr;
use bincode::Error as BincodeError;
use std::io::Error as IoError;
use snafu::{Snafu};
use super::connection_factory::ConnectionType;


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
    },
    #[snafu(display("Could not send data to {} asynchronously", endpoint))]
    SendData {
        endpoint: SocketAddr,
        source: std::io::Error,
    },
    #[snafu(display("Could not read data from {} asynchronously", endpoint))]
    ReadData {
        endpoint: SocketAddr,
        source: std::io::Error,
    }
}

/// This kind of error that can be produced during Pravega serialize the wire commands.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum SerializeError {
    #[snafu(display("Could not serialize command {} because of: {}", command_type, source))]
    InvalidData {
        command_type: i32,
        source: BincodeError,
    },
    #[snafu(display("Could not serialize command {} because of: {}", command_type, source))]
    Io {
        command_type: i32,
        source: IoError,
    }
}

