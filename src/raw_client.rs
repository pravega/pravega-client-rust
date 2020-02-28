//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use async_trait::async_trait;
use log::info;
use pravega_wire_protocol::client_connection::*;
use pravega_wire_protocol::commands::{OLDEST_COMPATIBLE_VERSION, WIRE_VERSION};
use pravega_wire_protocol::connection_pool::*;
use pravega_wire_protocol::error::*;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use snafu::ResultExt;
use std::fmt;
use std::net::SocketAddr;
use tracing::{event, span, Level};

/// RawClient is on top of the ClientConnection. It provides methods that take
/// Request enums and return Reply enums asynchronously. It has logic to process some of the replies from
/// server and return the processed result to caller.
#[async_trait]
trait RawClient<'a> {
    /// Asynchronously send a request to the server and receive a response.
    async fn send_request(&self, request: Requests) -> Result<Replies, RawClientError>;

    /// Asynchronously send a request to the server and receive a response and return the connection to the caller.
    async fn send_setup_request(
        &self,
        request: Requests,
    ) -> Result<(Replies, Box<dyn ClientConnection + 'a>), RawClientError>;
}

pub struct RawClientImpl<'a> {
    pool: &'a dyn ConnectionPool,
    endpoint: SocketAddr,
}

impl<'a> fmt::Debug for RawClientImpl<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RawClient endpoint: {:?}", self.endpoint)
    }
}

impl<'a> RawClientImpl<'a> {
    #[allow(clippy::new_ret_no_self)]
    async fn new(pool: &'a dyn ConnectionPool, endpoint: SocketAddr) -> Box<dyn RawClient<'a> + 'a> {
        Box::new(RawClientImpl { pool, endpoint })
    }

    fn process(&self, reply: Replies) -> Result<Replies, RawClientError> {
        match reply {
            Replies::Hello(hello) => {
                info!("Received hello: {:?}", hello);
                if hello.low_version > WIRE_VERSION || hello.high_version < OLDEST_COMPATIBLE_VERSION {
                    Err(RawClientError::IncompatibleVersion {
                        low: hello.low_version,
                        high: hello.high_version,
                    })
                } else {
                    Ok(Replies::Hello(hello))
                }
            }
            _ => Ok(reply),
        }
    }
}
#[allow(clippy::needless_lifetimes)]
#[async_trait]
impl<'a> RawClient<'a> for RawClientImpl<'a> {
    async fn send_request(&self, request: Requests) -> Result<Replies, RawClientError> {
        let connection = self
            .pool
            .get_connection(self.endpoint)
            .await
            .context(GetConnectionFromPool {})?;
        let mut client_connection = ClientConnectionImpl::new(connection);
        client_connection.write(&request).await.context(WriteRequest {})?;
        let reply = client_connection.read().await.context(ReadReply {})?;

        self.process(reply)
    }

    async fn send_setup_request(
        &self,
        request: Requests,
    ) -> Result<(Replies, Box<dyn ClientConnection + 'a>), RawClientError> {
        let span = span!(Level::DEBUG, "send_setup_request");
        let _guard = span.enter();
        let connection = self
            .pool
            .get_connection(self.endpoint)
            .await
            .context(GetConnectionFromPool {})?;
        let mut client_connection = ClientConnectionImpl::new(connection);
        client_connection.write(&request).await.context(WriteRequest {})?;

        let reply = client_connection.read().await.context(ReadReply {})?;

        self.process(reply).map_or_else(
            |e| {
                event!(Level::ERROR, "error when processing reply");
                Err(e)
            },
            |r| Ok((r, Box::new(client_connection) as Box<dyn ClientConnection>)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::info;
    use pravega_wire_protocol::client_config::ClientConfigBuilder;
    use pravega_wire_protocol::commands::HelloCommand;
    use pravega_wire_protocol::connection_factory::ConnectionFactoryImpl;
    use pravega_wire_protocol::wire_commands::Encode;
    use std::io::Write;
    use std::net::{SocketAddr, TcpListener};
    use std::thread;
    use tokio::runtime::Runtime;

    struct Common {
        rt: Runtime,
        pool: Box<dyn ConnectionPool>,
    }

    impl Common {
        fn new() -> Self {
            let rt = Runtime::new().expect("create tokio Runtime");
            let connection_factory = ConnectionFactoryImpl {};
            let pool = Box::new(ConnectionPoolImpl::new(
                Box::new(connection_factory),
                ClientConfigBuilder::default()
                    .build()
                    .expect("build client config"),
            ));
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
            info!("server created");
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
    fn test_hello() {
        let mut common = Common::new();
        let mut server = Server::new();

        let raw_client_fut = RawClientImpl::new(&*common.pool, server.address);
        let raw_client = common.rt.block_on(raw_client_fut);
        let h = thread::spawn(move || {
            server.send_hello();
        });

        let request = Requests::Hello(HelloCommand {
            low_version: 5,
            high_version: 9,
        });

        let reply = common
            .rt
            .block_on(raw_client.send_request(request))
            .expect("get reply");

        assert_eq!(
            reply,
            Replies::Hello(HelloCommand {
                high_version: 9,
                low_version: 5,
            })
        );
        h.join().expect("thread finished");
        info!("Testing raw client hello passed");
    }

    #[test]
    fn test_hello_failed() {
        let mut common = Common::new();
        let mut server = Server::new();

        let raw_client_fut = RawClientImpl::new(&*common.pool, server.address);
        let raw_client = common.rt.block_on(raw_client_fut);
        let h = thread::spawn(move || {
            server.send_hello_wrong_version();
        });

        let request = Requests::Hello(HelloCommand {
            low_version: 5,
            high_version: 9,
        });

        let reply = common.rt.block_on(raw_client.send_request(request));
        assert!(reply.is_err(), "wrong wirecommand version");

        h.join().expect("thread finished");
        info!("Testing raw client hello passed");
    }
}
