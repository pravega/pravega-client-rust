use std::net::SocketAddr;
use bincode::Error as bincodeError;
use std::io::Error as IOError;
use snafu::{Snafu};
use crate::commands;
use crate::connection_factory::ConnectionType;


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
pub enum SerializeError {
    #[snafu(display("Could not serialize command {} because of: {}", commandType, source))]
    InvalidData {
        command_type: u8,
        source: bincodeError
    },
    #[snafu(display("Could not serialize command {} because of: {}", commandType, source))]
    Io {
        command_type: u8,
        source: IOError
    }
}

