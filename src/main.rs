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

    #[test]
    fn test_send_async() {
        let rt = Runtime::new().unwrap();

        let connection_factory = connection_factory::ConnectionFactoryImpl {};
        let connection = connection_factory.establish_connection("127.0.0.1:0").unwrap();
        let mut data: Vec<u8> = Vec::new();
        data.push(12);
        let fut = connection.send_async(&data);

        rt.block_on(fut);
    }
}
