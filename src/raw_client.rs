use async_trait::async_trait;
use pravega_wire_protocol::commands::{HelloCommand, WIRE_VERSION, OLDEST_COMPATIBLE_VERSION};
use pravega_wire_protocol::wire_commands::{Requests, Replies};
use pravega_wire_protocol::client_connection::*;
use pravega_wire_protocol::connection_factory::*;
use pravega_wire_protocol::error::*;
use std::net::SocketAddr;
use pravega_wire_protocol::reply_processor::FailingReplyProcessor;
use

#[async_trait]
trait RawClient {
    ///Given a connection factory create a raw client.
    async fn new(factory: &dyn ConnectionFactory, endpoint: SocketAddr) -> Self;

    ///Asynchronously send a request to the server and receive a response.
    async fn send_request(&self, request: Requests) -> Result<Replies, ConnectionError>;

    ///True if this client currently has an open connection to the server.
    fn is_connected(&self) -> bool;

    fn close_connection(&self, error: ReplyError);

    fn reply(&self);
}

pub struct RawClientImpl {
    connection: Box<dyn ClientConnection>
    map: DashMap;
}

impl RawClientImpl {}

#[async_trait]
impl RawClient for RawClientImpl {
    async fn new(factory: &dyn ConnectionFactory, endpoint: SocketAddr) -> Self {
        let connection = factory.establish_connection(endpoint).await.expect("establish connection");
        let client_connection = ClientConnection::new(factory, connection);
        RawClientImpl{connection}
    }

    async fn send_request(&mut self, request: Requests) -> Result<Replies, ConnectionError> {
        self.connection.write(request).await;

        let reply = self.connection.read().await.expect("read reply wirecommand");
        Ok(reply)
    }

    fn is_connected(&self) -> bool {

    }

    fn close_connection(&self, error: ReplyError) {

    }

    fn reply(&self) {

    }
}

struct ReplyProcessor<'a> {
    raw_client: &'a dyn RawClient,
}

impl FailingReplyProcessor for ReplyProcessor {
    fn process(&self, reply: Replies) {
        match reply {
            Replies::Hello(hello) => {
                info!("Received hello: {}", hello);
                if hello.low_version > WIRE_VERSION || hello.high_version < OLDEST_COMPATIBLE_VERSION {
                    self.raw_client.close_connection(ReplyError::IncompatibleVersion { low: hello.low_version, high: hello.high_version });
                }
            },
            Replies::WrongHost(wrong_host) => {
                self.raw_client.close_connection(ReplyError::ConnectionFailed {state: Replies::WrongHost(wrong_host)});
            },
            _ => {
                self.raw_client.reply(reply);
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_config::ClientConfigBuilder;
    use log::info;
    use std::io::Read;
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

        pub fn receive(&mut self) -> u32 {
            let mut connections: u32 = 0;

            for stream in self.listener.incoming() {
                match stream {
                    Ok(mut stream) => {
                        let mut buf = [0; 1024];
                        match stream.read(&mut buf) {
                            Ok(_) => {
                                info!("received data");
                            }
                            Err(e) => panic!("encountered IO error: {}", e),
                        }
                        connections += 1;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        break;
                    }
                    Err(e) => panic!("encountered IO error: {}", e),
                }
            }
            connections
        }
    }

    #[test]
    fn test_raw_client() {
    }
}