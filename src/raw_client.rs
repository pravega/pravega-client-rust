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
use log::{info, warn};
use pravega_wire_protocol::client_connection::*;
use pravega_wire_protocol::commands::{OLDEST_COMPATIBLE_VERSION, WIRE_VERSION};
use pravega_wire_protocol::connection_pool::*;
use pravega_wire_protocol::error::ReplyError;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::net::SocketAddr;

#[async_trait]
trait RawClient<'a> {
    ///Asynchronously send a request to the server and receive a response.
    async fn send_request(&self, request: Requests) -> Result<Replies, ReplyError>;

    async fn send_setup_request(
        &self,
        request: Requests,
    ) -> Result<(Replies, Box<dyn ClientConnection + 'a>), ReplyError>;
}

pub struct RawClientImpl<'a> {
    pool: &'a dyn ConnectionPool,
    endpoint: SocketAddr,
}

impl<'a> RawClientImpl<'a> {
    #[allow(clippy::new_ret_no_self)]
    async fn new(pool: &'a dyn ConnectionPool, endpoint: SocketAddr) -> Box<dyn RawClient<'a> + 'a> {
        Box::new(RawClientImpl { pool, endpoint })
    }

    fn process(&self, reply: Replies) -> Result<Replies, ReplyError> {
        match reply {
            Replies::Hello(hello) => {
                info!("Received hello: {:?}", hello);
                if hello.low_version > WIRE_VERSION || hello.high_version < OLDEST_COMPATIBLE_VERSION {
                    Err(ReplyError::IncompatibleVersion {
                        low: hello.low_version,
                        high: hello.high_version,
                    })
                } else {
                    Ok(Replies::Hello(hello))
                }
            }
            Replies::WrongHost(wrong_host) => Err(ReplyError::ConnectionFailed {
                state: Replies::WrongHost(wrong_host),
            }),
            _ => Ok(reply),
        }
    }
}
#[allow(clippy::needless_lifetimes)]
#[async_trait]
impl<'a> RawClient<'a> for RawClientImpl<'a> {
    async fn send_request(&self, request: Requests) -> Result<Replies, ReplyError> {
        let connection = self
            .pool
            .get_connection(self.endpoint)
            .await
            .expect("get connection");
        let mut client_connection = ClientConnectionImpl::new(connection);
        client_connection
            .write(&request)
            .await
            .expect("write successfully");
        let reply = client_connection.read().await.expect("read reply wirecommand");

        self.process(reply)
    }

    async fn send_setup_request(
        &self,
        request: Requests,
    ) -> Result<(Replies, Box<dyn ClientConnection + 'a>), ReplyError> {
        let connection = self
            .pool
            .get_connection(self.endpoint)
            .await
            .expect("get connection");
        let mut client_connection = ClientConnectionImpl::new(connection);
        client_connection
            .write(&request)
            .await
            .expect("write successfully");

        let reply = client_connection.read().await.expect("read reply wirecommand");

        self.process(reply).map_or_else(
            |e| {
                warn!("error when procssing reply");
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
    use pravega_wire_protocol::wire_commands::{Decode, Encode};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::sync::Arc;
    use std::{io, thread};
    use tokio::runtime::Runtime;

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

        pub fn receive(&mut self) {
            let reply = Replies::Hello(HelloCommand {
                high_version: 9,
                low_version: 5,
            })
            .write_fields()
            .unwrap();

            for stream in self.listener.incoming() {
                let mut stream = stream.unwrap();
                stream.write_all(&reply).unwrap();
            }
        }
    }

    #[test]
    fn test_hello() {
        let mut rt = Runtime::new().unwrap();

        let mut server = Server::new();

        let connection_factory = ConnectionFactoryImpl {};
        let pool = ConnectionPoolImpl::new(
            Box::new(connection_factory),
            ClientConfigBuilder::default().build().unwrap(),
        );
        let raw_client_fut = RawClientImpl::new(&pool, server.address);
        let raw_client = rt.block_on(raw_client_fut);
        let h = thread::spawn(move || {
            server.receive();
        });

        let request = Requests::Hello(HelloCommand {
            low_version: 5,
            high_version: 9,
        });

        let reply = rt.block_on(raw_client.send_request(request)).expect("get reply");

        assert_eq!(
            reply,
            Replies::Hello(HelloCommand {
                high_version: 9,
                low_version: 5,
            })
        );
        info!("Testing raw client hello passed");
    }
}
