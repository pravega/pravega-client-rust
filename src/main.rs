/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
mod connection_factory;

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_factory::ConnectionFactory;
    use log::info;
    use std::io::Write;
    use std::net::{SocketAddr, TcpListener};
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

        pub fn echo(&mut self) {
            for stream in self.listener.incoming() {
                let mut stream = stream.unwrap();
                stream.write(b"Hello World\r\n").unwrap();
                break;
            }
            info!("echo back");
        }
    }

    #[test]
    fn test_connection() {
        let rt = Runtime::new().unwrap();

        let mut server = Server::new();

        let connection_factory = connection_factory::ConnectionFactoryImpl {};
        let connection_future = connection_factory
            .establish_connection(connection_factory::ConnectionType::Tokio, server.address);
        let mut connection = rt.block_on(connection_future).unwrap();
        info!("connection established");

        let mut payload: Vec<u8> = Vec::new();
        payload.push(12);
        let fut = connection.send_async(&payload);

        rt.block_on(fut);
        info!("payload sent");

        server.echo();
        let mut buf = [0; 13];

        let fut = connection.read_async(&mut buf);
        let res = rt.block_on(fut).unwrap();

        let echo = "Hello World\r\n".as_bytes();
        assert_eq!(buf, &echo[..]);
        info!("Testing connection passed");
    }
}
