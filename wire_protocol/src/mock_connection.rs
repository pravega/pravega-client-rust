//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

extern crate byteorder;
use crate::connection::{Connection, ReadingConnection, WritingConnection};
use crate::error::*;
use crate::wire_commands::{Decode, Encode, Replies, Requests};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

pub struct MockConnection {
    id: Uuid,
    endpoint: SocketAddr,
    replies: Arc<Mutex<VecDeque<Replies>>>,
    buffer: VecDeque<u8>,
}

impl MockConnection {
    pub fn new(endpoint: SocketAddr) -> Self {
        MockConnection {
            id: Uuid::new_v4(),
            endpoint,
            replies: Arc::new(Mutex::new(VecDeque::new())),
            buffer: VecDeque::new(),
        }
    }
}

#[async_trait]
impl Connection for MockConnection {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        send(self.replies.lock().await, payload).await
    }

    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError> {
        if self.buffer.is_empty() {
            let reply: Replies = self
                .replies
                .lock()
                .await
                .pop_front()
                .expect("pop reply from mock connection");
            self.buffer = VecDeque::from(reply.write_fields().expect("serialize reply"))
        }
        read(&mut self.buffer, buf).await
    }

    fn split(&mut self) -> (Box<dyn ReadingConnection>, Box<dyn WritingConnection>) {
        let reader = Box::new(MockReadingConnection {
            id: self.id,
            replies: self.replies.clone(),
            buffer: VecDeque::new(),
        }) as Box<dyn ReadingConnection>;
        let writer = Box::new(MockWritingConnection {
            id: self.id,
            replies: self.replies.clone(),
        }) as Box<dyn WritingConnection>;
        (reader, writer)
    }

    fn get_endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    fn get_uuid(&self) -> Uuid {
        self.id
    }

    fn is_valid(&self) -> bool {
        true
    }
}

pub struct MockReadingConnection {
    id: Uuid,
    replies: Arc<Mutex<VecDeque<Replies>>>,
    buffer: VecDeque<u8>,
}

pub struct MockWritingConnection {
    id: Uuid,
    replies: Arc<Mutex<VecDeque<Replies>>>,
}

#[async_trait]
impl ReadingConnection for MockReadingConnection {
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError> {
        if self.buffer.is_empty() {
            let reply: Replies = self
                .replies
                .lock()
                .await
                .pop_front()
                .expect("pop reply from mock connection");
            self.buffer = VecDeque::from(reply.write_fields().expect("serialize reply"))
        }
        read(&mut self.buffer, buf).await
    }

    fn get_id(&self) -> Uuid {
        self.id
    }
}

#[async_trait]
impl WritingConnection for MockWritingConnection {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        send(self.replies.lock().await, payload).await
    }

    fn get_id(&self) -> Uuid {
        self.id
    }
}

async fn send(mut replies: MutexGuard<'_, VecDeque<Replies>>, payload: &[u8]) -> Result<(), ConnectionError> {
    let request: Requests = Requests::read_from(payload).expect("mock connection decode request");
    match request {
        Requests::Hello(cmd) => {
            let reply = Replies::Hello(cmd);
            replies.push_back(reply);
        }
        _ => {}
    }
    Ok(())
}

async fn read(connection_buf: &mut VecDeque<u8>, buf: &mut [u8]) -> Result<(), ConnectionError> {
    for i in 0..buf.len() {
        buf[i] = connection_buf.pop_front().expect("buffer should not be empty");
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::commands::{Command, HelloCommand};
    use log::info;

    #[test]
    fn test_simple_write_and_read() {
        info!("mock client connection test");
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let mut mock_connection = MockConnection::new("127.1.1.1:9090".parse::<SocketAddr>().unwrap());
        let request = Requests::Hello(HelloCommand {
            high_version: 9,
            low_version: 5,
        })
        .write_fields()
        .unwrap();
        let len = request.len();
        rt.block_on(mock_connection.send_async(&request))
            .expect("write to mock connection");
        let mut buf = vec![0; len];
        rt.block_on(mock_connection.read_async(&mut buf))
            .expect("read from mock connection");
        let reply = Replies::read_from(&buf).unwrap();
        let expected = Replies::Hello(HelloCommand {
            high_version: 9,
            low_version: 5,
        });
        assert_eq!(reply, expected);
        info!("mock connection test passed");
    }
}
