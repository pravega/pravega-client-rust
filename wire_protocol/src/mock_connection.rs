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
use crate::commands::{AppendSetupCommand, DataAppendedCommand};
use crate::connection::{Connection, ReadingConnection, WritingConnection};
use crate::error::*;
use crate::wire_commands::{Decode, Encode, Replies, Requests};
use async_trait::async_trait;
use std::net::SocketAddr;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

pub struct MockConnection {
    id: Uuid,
    endpoint: SocketAddr,
    sender: Option<UnboundedSender<Replies>>,
    receiver: Option<UnboundedReceiver<Replies>>,
    buffer: Vec<u8>,
    index: usize,
}

impl MockConnection {
    pub fn new(endpoint: SocketAddr) -> Self {
        let (tx, rx) = unbounded_channel();
        MockConnection {
            id: Uuid::new_v4(),
            endpoint,
            sender: Some(tx),
            receiver: Some(rx),
            buffer: vec![],
            index: 0,
        }
    }
}

#[async_trait]
impl Connection for MockConnection {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        send(self.sender.as_mut().expect("get sender"), payload).await
    }

    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError> {
        if self.index == self.buffer.len() {
            let reply: Replies = self
                .receiver
                .as_mut()
                .expect("get receiver")
                .recv()
                .await
                .expect("read");
            self.buffer = reply.write_fields().expect("serialize reply");
            self.index = 0;
        }
        buf.copy_from_slice(&self.buffer[self.index..self.index + buf.len()]);
        self.index += buf.len();
        assert!(self.index <= self.buffer.len());
        Ok(())
    }

    fn split(&mut self) -> (Box<dyn ReadingConnection>, Box<dyn WritingConnection>) {
        let reader = Box::new(MockReadingConnection {
            id: self.id,
            receiver: self
                .receiver
                .take()
                .expect("split mock connection and get receiver"),
            buffer: vec![],
            index: 0,
        }) as Box<dyn ReadingConnection>;
        let writer = Box::new(MockWritingConnection {
            id: self.id,
            sender: self.sender.take().expect("split mock connection and get sender"),
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
    receiver: UnboundedReceiver<Replies>,
    buffer: Vec<u8>,
    index: usize,
}

pub struct MockWritingConnection {
    id: Uuid,
    sender: UnboundedSender<Replies>,
}

#[async_trait]
impl ReadingConnection for MockReadingConnection {
    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError> {
        if self.index == self.buffer.len() {
            let reply: Replies = self.receiver.recv().await.expect("read");
            self.buffer = reply.write_fields().expect("serialize reply");
            self.index = 0;
        }
        buf.copy_from_slice(&self.buffer[self.index..self.index + buf.len()]);
        self.index += buf.len();
        assert!(self.index <= self.buffer.len());
        Ok(())
    }

    fn get_id(&self) -> Uuid {
        self.id
    }
}

#[async_trait]
impl WritingConnection for MockWritingConnection {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        send(&mut self.sender, payload).await
    }

    fn get_id(&self) -> Uuid {
        self.id
    }
}

async fn send(sender: &mut UnboundedSender<Replies>, payload: &[u8]) -> Result<(), ConnectionError> {
    let request: Requests = Requests::read_from(payload).expect("mock connection decode request");
    match request {
        Requests::Hello(cmd) => {
            let reply = Replies::Hello(cmd);
            sender.send(reply).expect("send reply");
        }
        Requests::SetupAppend(cmd) => {
            let reply = Replies::AppendSetup(AppendSetupCommand {
                request_id: cmd.request_id,
                segment: cmd.segment,
                writer_id: cmd.writer_id,
                last_event_number: -9_223_372_036_854_775_808, // when there is no previous event in this segment
            });
            sender.send(reply).expect("send reply");
        }
        Requests::AppendBlockEnd(cmd) => {
            let reply = Replies::DataAppended(DataAppendedCommand {
                writer_id: cmd.writer_id,
                event_number: cmd.last_event_number,
                previous_event_number: 0, //not used in event stream writer
                request_id: cmd.request_id,
                current_segment_write_offset: 0, //not used in event stream writer
            });
            sender.send(reply).expect("send reply");
        }
        _ => {
            panic!("unsupported request {:?}", request);
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::commands::HelloCommand;
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
