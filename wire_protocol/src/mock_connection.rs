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
use crate::commands::{AppendSetupCommand, DataAppendedCommand, SegmentSealedCommand, WrongHostCommand};
use crate::connection::{Connection, ConnectionReadHalf, ConnectionWriteHalf};
use crate::error::*;
use crate::wire_commands::{Decode, Encode, Replies, Requests};
use async_trait::async_trait;
use downcast_rs::__std::fmt::Formatter;
use pravega_rust_client_config::connection_type::MockType;
use pravega_rust_client_shared::PravegaNodeUri;
use std::fmt;
use std::fmt::Debug;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

pub struct MockConnection {
    id: Uuid,
    mock_type: MockType,
    endpoint: PravegaNodeUri,
    sender: Option<UnboundedSender<Replies>>,
    receiver: Option<UnboundedReceiver<Replies>>,
    buffer: Vec<u8>,
    index: usize,
}

impl MockConnection {
    pub fn new(endpoint: PravegaNodeUri, mock_type: MockType) -> Self {
        let (tx, rx) = unbounded_channel();
        MockConnection {
            id: Uuid::new_v4(),
            mock_type,
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
        match self.mock_type {
            MockType::Happy => send_happy(self.sender.as_mut().expect("get sender"), payload).await,
            MockType::SegmentSealed => send_sealed(self.sender.as_mut().expect("get sender"), payload).await,
            MockType::SegmentTruncated => {
                send_truncated(self.sender.as_mut().expect("get sender"), payload).await
            }
            MockType::WrongHost => send_wrong_host(self.sender.as_mut().expect("get sender"), payload).await,
        }
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

    fn split(&mut self) -> (Box<dyn ConnectionReadHalf>, Box<dyn ConnectionWriteHalf>) {
        let reader = Box::new(MockReadingConnection {
            id: self.id,
            endpoint: self.endpoint.clone(),
            mock_type: self.mock_type,
            receiver: self
                .receiver
                .take()
                .expect("split mock connection and get receiver"),
            buffer: vec![],
            index: 0,
        }) as Box<dyn ConnectionReadHalf>;
        let writer = Box::new(MockWritingConnection {
            id: self.id,
            mock_type: self.mock_type,
            sender: self.sender.take().expect("split mock connection and get sender"),
        }) as Box<dyn ConnectionWriteHalf>;
        (reader, writer)
    }

    fn get_endpoint(&self) -> PravegaNodeUri {
        self.endpoint.clone()
    }

    fn get_uuid(&self) -> Uuid {
        self.id
    }

    fn is_valid(&self) -> bool {
        true
    }
}

impl Debug for MockConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConnection")
            .field("connection id", &self.id)
            .field("pravega endpoint", &self.endpoint)
            .finish()
    }
}

pub struct MockReadingConnection {
    id: Uuid,
    endpoint: PravegaNodeUri,
    mock_type: MockType,
    receiver: UnboundedReceiver<Replies>,
    buffer: Vec<u8>,
    index: usize,
}

#[derive(Debug)]
pub struct MockWritingConnection {
    id: Uuid,
    mock_type: MockType,
    sender: UnboundedSender<Replies>,
}

#[async_trait]
impl ConnectionReadHalf for MockReadingConnection {
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

    fn unsplit(&mut self, _write_half: Box<dyn ConnectionWriteHalf>) -> Box<dyn Connection> {
        let (tx, rx) = unbounded_channel();
        Box::new(MockConnection {
            id: self.id,
            endpoint: self.endpoint.clone(),
            mock_type: self.mock_type,
            sender: Some(tx),
            receiver: Some(rx),
            buffer: self.buffer.clone(),
            index: self.index,
        }) as Box<dyn Connection>
    }
}

#[async_trait]
impl ConnectionWriteHalf for MockWritingConnection {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        match self.mock_type {
            MockType::Happy => send_happy(&mut self.sender, payload).await,
            MockType::SegmentSealed => send_sealed(&mut self.sender, payload).await,
            MockType::SegmentTruncated => send_truncated(&mut self.sender, payload).await,
            MockType::WrongHost => send_wrong_host(&mut self.sender, payload).await,
        }
    }

    fn get_id(&self) -> Uuid {
        self.id
    }
}

async fn send_happy(sender: &mut UnboundedSender<Replies>, payload: &[u8]) -> Result<(), ConnectionError> {
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

async fn send_sealed(sender: &mut UnboundedSender<Replies>, payload: &[u8]) -> Result<(), ConnectionError> {
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
            let reply = Replies::SegmentSealed(SegmentSealedCommand {
                request_id: cmd.request_id,
                segment: "".to_string(),
            });
            sender.send(reply).expect("send reply");
        }
        _ => {
            panic!("unsupported request {:?}", request);
        }
    }
    Ok(())
}

async fn send_truncated(
    sender: &mut UnboundedSender<Replies>,
    payload: &[u8],
) -> Result<(), ConnectionError> {
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
            let reply = Replies::SegmentSealed(SegmentSealedCommand {
                request_id: cmd.request_id,
                segment: "".to_string(),
            });
            sender.send(reply).expect("send reply");
        }
        _ => {
            panic!("unsupported request {:?}", request);
        }
    }
    Ok(())
}

async fn send_wrong_host(
    sender: &mut UnboundedSender<Replies>,
    payload: &[u8],
) -> Result<(), ConnectionError> {
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
            let reply = Replies::WrongHost(WrongHostCommand {
                request_id: cmd.request_id,
                segment: "".to_string(),
                correct_host: "".to_string(),
                server_stack_trace: "".to_string(),
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
        let mut mock_connection = MockConnection::new(PravegaNodeUri::from("127.1.1.1:9090".to_string()));
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
