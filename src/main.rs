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
    use tokio::runtime::Runtime;
    use crate::connection_factory::ConnectionFactory;
    use std::net::{TcpListener, TcpStream, SocketAddr};
    use std::thread;
    use std::io::Read;
    use std::io::Write;

    struct server {
        address: SocketAddr,
        listener: TcpListener,
    }

    impl server {
        pub fn new() -> server {
            let listener = TcpListener::bind("127.0.0.1:0").expect("local server");
            let address = listener.local_addr().unwrap();
            println!("server created");
            server { address, listener }
        }

        pub fn echo(&mut self) {
            for stream in self.listener.incoming() {
                let mut stream = stream.unwrap();
                stream.write(b"Hello World\r\n").unwrap();
                break;
            }
            println!("echo back");
        }
    }

    #[test]
    fn test_connection() {
        let rt = Runtime::new().unwrap();

        let mut server = server::new();

        let connection_factory = connection_factory::ConnectionFactoryImpl {};
        let connection_future = connection_factory.establish_connection(connection_factory::ConnectionType::Tokio, server.address);
        let mut connection = rt.block_on(connection_future).unwrap();
        println!("connection established");

        let mut data: Vec<u8> = Vec::new();
        data.push(12);
        let fut = connection.send_async(&data);

        rt.block_on(fut);
        println!("payload sent");

        server.echo();
        let mut buf: Vec<u8> = Vec::new();
        let fut = connection.read_async(&mut buf);

        rt.block_on(fut);
        let echo = "Hello World\r\n".as_bytes();
        assert_eq!(buf, &echo[..]);
    }
}
