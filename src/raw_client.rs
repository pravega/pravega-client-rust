//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::error::*;
use async_trait::async_trait;
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::connection_factory::SegmentConnectionManager;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use snafu::ResultExt;
use std::fmt;
use std::net::SocketAddr;
use tracing::{span, Level};

/// RawClient is on top of the ClientConnection. It provides methods that take
/// Request enums and return Reply enums asynchronously. It has logic to process some of the replies from
/// server and return the processed result to caller.
#[async_trait]
pub trait RawClient<'a>: Send + Sync {
    /// Asynchronously send a request to the server and receive a response.
    async fn send_request(&self, request: &Requests) -> Result<Replies, RawClientError>
    where
        'a: 'async_trait;

    /// Asynchronously send a request to the server and receive a response and return the connection to the caller.
    async fn send_setup_request(
        &self,
        request: &Requests,
    ) -> Result<(Replies, Box<dyn ClientConnection + 'a>), RawClientError>
    where
        'a: 'async_trait;
}

pub struct RawClientImpl<'a> {
    pool: &'a ConnectionPool<SegmentConnectionManager>,
    endpoint: SocketAddr,
}

impl<'a> fmt::Debug for RawClientImpl<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RawClient endpoint: {:?}", self.endpoint)
    }
}

impl<'a> RawClientImpl<'a> {
    pub fn new(
        pool: &'a ConnectionPool<SegmentConnectionManager>,
        endpoint: SocketAddr,
    ) -> RawClientImpl<'a> {
        RawClientImpl { pool, endpoint }
    }
}

#[allow(clippy::needless_lifetimes)]
#[async_trait]
impl<'a> RawClient<'a> for RawClientImpl<'a> {
    async fn send_request(&self, request: &Requests) -> Result<Replies, RawClientError> {
        let connection = self
            .pool
            .get_connection(self.endpoint)
            .await
            .context(GetConnectionFromPool {})?;
        let mut client_connection = ClientConnectionImpl::new(connection);
        client_connection.write(request).await.context(WriteRequest {})?;
        let reply = client_connection.read().await.context(ReadReply {})?;
        Ok(reply)
    }

    async fn send_setup_request(
        &self,
        request: &Requests,
    ) -> Result<(Replies, Box<dyn ClientConnection + 'a>), RawClientError> {
        let span = span!(Level::DEBUG, "send_setup_request");
        let _guard = span.enter();
        let connection = self
            .pool
            .get_connection(self.endpoint)
            .await
            .context(GetConnectionFromPool {})?;
        let mut client_connection = ClientConnectionImpl::new(connection);
        client_connection.write(request).await.context(WriteRequest {})?;

        let reply = client_connection.read().await.context(ReadReply {})?;

        Ok((reply, Box::new(client_connection) as Box<dyn ClientConnection>))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pravega_wire_protocol::commands::HelloCommand;
    use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionType};
    use pravega_wire_protocol::wire_commands::Encode;
    use std::io::Write;
    use std::net::{SocketAddr, TcpListener};
    use std::thread;
    use tokio::runtime::Runtime;

    struct Common {
        rt: Runtime,
        pool: ConnectionPool<SegmentConnectionManager>,
    }

    impl Common {
        fn new() -> Self {
            let rt = Runtime::new().expect("create tokio Runtime");
            let connection_factory = ConnectionFactory::create(ConnectionType::Tokio);
            let manager = SegmentConnectionManager::new(connection_factory, 2);
            let pool = ConnectionPool::new(manager);
            Common { rt, pool }
        }
    }

    struct Server {
        address: SocketAddr,
        listener: TcpListener,
    }

    impl Server {
        pub fn new() -> Server {
            let listener = TcpListener::bind("127.0.0.1:0").expect("local server");
            let address = listener.local_addr().unwrap();
            Server { address, listener }
        }

        pub fn send_hello(&mut self) {
            let reply = Replies::Hello(HelloCommand {
                high_version: 9,
                low_version: 5,
            })
            .write_fields()
            .expect("serialize hello wirecommand");

            for stream in self.listener.incoming() {
                let mut stream = stream.expect("get tcp stream");
                stream.write_all(&reply).expect("reply with hello wirecommand");
                break;
            }
        }

        pub fn send_hello_wrong_version(&mut self) {
            let reply = Replies::Hello(HelloCommand {
                high_version: 10,
                low_version: 10,
            })
            .write_fields()
            .expect("serialize hello wirecommand");

            for stream in self.listener.incoming() {
                let mut stream = stream.expect("get tcp stream");
                stream.write_all(&reply).expect("reply with hello wirecommand");
                break;
            }
        }
    }

    #[test]
    #[should_panic] // since connection verify will panic
    fn test_hello() {
        let mut common = Common::new();
        let mut server = Server::new();

        let raw_client = RawClientImpl::new(&common.pool, server.address);
        let h = thread::spawn(move || {
            server.send_hello();
        });

        let request = Requests::Hello(HelloCommand {
            low_version: 5,
            high_version: 9,
        });

        let reply = common
            .rt
            .block_on(raw_client.send_request(&request))
            .expect("get reply");

        assert_eq!(
            reply,
            Replies::Hello(HelloCommand {
                high_version: 9,
                low_version: 5,
            })
        );
        h.join().expect("thread finished");
    }
}
