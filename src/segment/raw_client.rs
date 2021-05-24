//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_client_shared::PravegaNodeUri;
use pravega_connection_pool::connection_pool::{ConnectionPool, ConnectionPoolError};
use pravega_wire_protocol::client_connection::{ClientConnection, ClientConnectionImpl};
use pravega_wire_protocol::commands::{Reply, Request};
use pravega_wire_protocol::connection_factory::SegmentConnectionManager;
use pravega_wire_protocol::error::ClientConnectionError;
use pravega_wire_protocol::wire_commands::{Replies, Requests};

use async_trait::async_trait;
use snafu::ResultExt;
use snafu::Snafu;
use std::fmt;
use std::fmt::Debug;
use tokio::time::error::Elapsed;
use tokio::time::{timeout, Duration};

#[derive(Debug, Snafu)]
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

    #[snafu(display("Wrong reply id {:?} for request {:?}", reply_id, request_id))]
    WrongReplyId { reply_id: i64, request_id: i64 },
}

impl RawClientError {
    pub fn is_token_expired(&self) -> bool {
        matches!(self, RawClientError::AuthTokenExpired { .. })
    }
}

// RawClient is on top of the ClientConnection. It provides methods that take
// Request enums and return Reply enums asynchronously. It has logic to process some of the replies from
// server and return the processed result to caller.
#[async_trait]
pub(crate) trait RawClient<'a>: Send + Sync {
    // Asynchronously send a request to the server and receive a response.
    async fn send_request(&self, request: &Requests) -> Result<Replies, RawClientError>
    where
        'a: 'async_trait;

    // Asynchronously send a request to the server and receive a response and return the connection to the caller.
    async fn send_setup_request(
        &self,
        request: &Requests,
    ) -> Result<(Replies, Box<dyn ClientConnection + 'a>), RawClientError>
    where
        'a: 'async_trait;
}

pub(crate) struct RawClientImpl<'a> {
    pool: &'a ConnectionPool<SegmentConnectionManager>,
    endpoint: PravegaNodeUri,
    timeout: Duration,
}

impl<'a> fmt::Debug for RawClientImpl<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RawClient endpoint: {:?}", self.endpoint)
    }
}

impl<'a> RawClientImpl<'a> {
    pub(crate) fn new(
        pool: &'a ConnectionPool<SegmentConnectionManager>,
        endpoint: PravegaNodeUri,
        timeout: Duration,
    ) -> RawClientImpl<'a> {
        RawClientImpl {
            pool,
            endpoint,
            timeout,
        }
    }
}

#[allow(clippy::needless_lifetimes)]
#[async_trait]
impl<'a> RawClient<'a> for RawClientImpl<'a> {
    async fn send_request(&self, request: &Requests) -> Result<Replies, RawClientError> {
        let connection = self
            .pool
            .get_connection(self.endpoint.clone())
            .await
            .context(GetConnectionFromPool {})?;
        let mut client_connection = ClientConnectionImpl::new(connection);
        client_connection.write(request).await.context(WriteRequest {})?;
        let read_future = client_connection.read();
        let result = timeout(self.timeout, read_future)
            .await
            .context(RequestTimeout {})?;
        let reply = result.context(ReadReply {})?;
        if reply.get_request_id() != request.get_request_id() {
            client_connection.connection.invalidate();
            return Err(RawClientError::WrongReplyId {
                reply_id: reply.get_request_id(),
                request_id: request.get_request_id(),
            });
        }
        check_auth_token_expired(&reply)?;
        Ok(reply)
    }

    async fn send_setup_request(
        &self,
        request: &Requests,
    ) -> Result<(Replies, Box<dyn ClientConnection + 'a>), RawClientError> {
        let connection = self
            .pool
            .get_connection(self.endpoint.clone())
            .await
            .context(GetConnectionFromPool {})?;
        let mut client_connection = ClientConnectionImpl::new(connection);
        client_connection.write(request).await.context(WriteRequest {})?;
        let read_future = client_connection.read();
        let result = timeout(self.timeout, read_future)
            .await
            .context(RequestTimeout {})?;
        let reply = result.context(ReadReply {})?;
        if reply.get_request_id() != request.get_request_id() {
            client_connection.connection.invalidate();
            return Err(RawClientError::WrongReplyId {
                reply_id: reply.get_request_id(),
                request_id: request.get_request_id(),
            });
        }
        check_auth_token_expired(&reply)?;
        Ok((reply, Box::new(client_connection) as Box<dyn ClientConnection>))
    }
}

fn check_auth_token_expired(reply: &Replies) -> Result<(), RawClientError> {
    if let Replies::AuthTokenCheckFailed(ref cmd) = reply {
        if cmd.is_token_expired() {
            return Err(RawClientError::AuthTokenExpired { reply: reply.clone() });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pravega_client_config::connection_type::ConnectionType;
    use pravega_wire_protocol::commands::{HelloCommand, ReadSegmentCommand, SegmentReadCommand};
    use pravega_wire_protocol::connection_factory::{ConnectionFactory, ConnectionFactoryConfig};
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
            let config = ConnectionFactoryConfig::new(ConnectionType::Tokio);
            let connection_factory = ConnectionFactory::create(config);
            let manager = SegmentConnectionManager::new(connection_factory, 1);
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
        let common = Common::new();
        let mut server = Server::new();

        let raw_client = RawClientImpl::new(
            &common.pool,
            PravegaNodeUri::from(server.address),
            Duration::from_secs(3600),
        );
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

    #[test]
    fn test_invalid_connection() {
        let common = Common::new();
        let server = Server::new();

        let raw_client = RawClientImpl::new(
            &common.pool,
            PravegaNodeUri::from(server.address),
            Duration::from_secs(30),
        );
        let h = thread::spawn(move || {
            let mut cnt = 0;
            for stream in server.listener.incoming() {
                cnt += 1;
                match stream {
                    Ok(mut stream) => {
                        if cnt == 1 {
                            let reply = Replies::Hello(HelloCommand {
                                high_version: 11,
                                low_version: 5,
                            });
                            let bytes = reply.write_fields().expect("serialize reply");
                            stream.write_all(&bytes).expect("send valid payload");
                        } else if cnt == 2 {
                            let buf = vec![0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0, 255, 255, 255, 255];
                            stream.write_all(&buf).expect("send invalid payload");
                        } else if cnt == 3 {
                            let reply = Replies::Hello(HelloCommand {
                                high_version: 11,
                                low_version: 5,
                            });
                            let bytes = reply.write_fields().expect("serialize reply");
                            stream.write_all(&bytes).expect("send valid payload");
                        } else if cnt == 4 {
                            let reply = Replies::SegmentRead(SegmentReadCommand {
                                segment: "foo".to_string(),
                                offset: 0,
                                at_tail: false,
                                end_of_segment: false,
                                data: vec![0, 0, 0, 0],
                                request_id: 0,
                            });
                            let bytes = reply.write_fields().expect("serialize reply");
                            stream.write_all(&bytes).expect("send valid payload");
                            break;
                        }
                    }
                    Err(_e) => {
                        panic!("connection failed");
                    }
                }
            }
        });
        let request = Requests::ReadSegment(ReadSegmentCommand {
            segment: "foo".to_string(),
            offset: 0,
            suggested_length: 0,
            delegation_token: "".to_string(),
            request_id: 0,
        });

        let res = common.rt.block_on(raw_client.send_request(&request));
        // payload length too long
        assert!(res.is_err());

        // send again for retry
        let reply = common
            .rt
            .block_on(raw_client.send_request(&request))
            .expect("get reply");
        assert_eq!(
            reply,
            Replies::SegmentRead(SegmentReadCommand {
                segment: "foo".to_string(),
                offset: 0,
                at_tail: false,
                end_of_segment: false,
                data: vec![0, 0, 0, 0],
                request_id: 0
            })
        );
        h.join().expect("thread finished");
    }
}
