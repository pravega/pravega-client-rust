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
use dashmap::DashMap;
use log::{debug, info, warn};
use pravega_wire_protocol::client_connection::*;
use pravega_wire_protocol::commands::{Reply, Request, OLDEST_COMPATIBLE_VERSION, WIRE_VERSION};
use pravega_wire_protocol::connection_pool::*;
use pravega_wire_protocol::error::*;
use pravega_wire_protocol::reply_processor::FailingReplyProcessor;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::oneshot;

#[async_trait]
trait RawClient: Send + Sync {
    ///Asynchronously send a request to the server and receive a response.
    async fn send_request(&mut self, request: Requests) -> Result<Replies, ReplyError>;

    ///True if this client currently has an open connection to the server.
    fn is_connected(&self) -> bool;
}

pub struct RawClientImpl<'a> {
    connection: Box<dyn ClientConnection + 'a>,
    map: DashMap<i64, oneshot::Sender<Result<Replies, ReplyError>>>,
    is_closed: AtomicBool,
}

impl<'a> RawClientImpl<'a> {
    async fn new(pool: &'a dyn ConnectionPool, endpoint: SocketAddr) -> Box<dyn RawClient + 'a> {
        let connection = pool
            .get_connection(endpoint)
            .await
            .expect("get connection from pool");
        let map = DashMap::new();
        let client_connection = Box::new(ClientConnectionImpl::new(connection));
        Box::new(RawClientImpl {
            connection: client_connection,
            map,
            is_closed: AtomicBool::new(false),
        })
    }

    fn close_connection(&mut self, error: ReplyError) {
        match error.clone() {
            ReplyError::ConnectionClosed { state: state } => debug!("Closing connection as requested"),
            _ => warn!("Closing connection to segment with error: {}", error.clone()),
        }
        self.is_closed.compare_and_swap(false, true, Ordering::Relaxed);

        let mut to_remove = Vec::new();
        self.map.iter().for_each(|r| to_remove.push(r.key().clone()));

        for key in to_remove.iter() {
            let (key, value) = self.map.remove(key).expect("remove sender");
            value.send(Err(error.clone()));
        }
    }

    fn reply(&self, reply: Replies) {
        let id = reply.get_request_id();
        let (key, value) = self.map.remove(&id).expect("get sender using request id");
        value.send(Ok(reply));
        self.map.remove(&id);
    }
}

#[async_trait]
impl RawClient for RawClientImpl<'_> {
    async fn send_request(&mut self, request: Requests) -> Result<Replies, ReplyError> {
        self.connection.write(&request).await;
        let (tx, rx) = oneshot::channel();
        self.map.insert(request.get_request_id(), tx);

        let reply = self.connection.read().await.expect("read reply wirecommand");

        self.process(reply);
        match rx.await {
            Ok(result) => match result {
                Ok(reply) => Ok(reply),
                Err(e) => Err(e),
            },
            // TODO
            Err(e) => panic!("the sender dropped"),
        }
    }

    fn is_connected(&self) -> bool {
        self.is_closed.load(Ordering::Relaxed)
    }
}

impl FailingReplyProcessor for RawClientImpl<'_> {
    fn process(&mut self, reply: Replies) {
        match reply {
            Replies::Hello(hello) => {
                info!("Received hello: {:?}", hello);
                if hello.low_version > WIRE_VERSION || hello.high_version < OLDEST_COMPATIBLE_VERSION {
                    self.close_connection(ReplyError::IncompatibleVersion {
                        low: hello.low_version,
                        high: hello.high_version,
                    });
                } else {
                    self.reply(Replies::Hello(hello));
                }
            }
            Replies::WrongHost(wrong_host) => {
                self.close_connection(ReplyError::ConnectionFailed {
                    state: Replies::WrongHost(wrong_host),
                });
            }
            _ => {
                self.reply(reply);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::info;
    use pravega_wire_protocol::client_config::ClientConfigBuilder;
    use pravega_wire_protocol::wire_commands::{Decode, Encode};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::ops::DerefMut;
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
            listener.set_nonblocking(true).expect("Cannot set non-blocking");
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
                stream.write(&reply).unwrap();
                break;
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
        let mut raw_client = rt.block_on(raw_client_fut);
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
        h.join().unwrap();
        info!("Testing raw client hello passed");
    }
}
