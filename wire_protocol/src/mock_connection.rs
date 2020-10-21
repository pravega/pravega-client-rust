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
use crate::commands::{
    AppendSetupCommand, DataAppendedCommand, SegmentIsSealedCommand, SegmentIsTruncatedCommand,
    SegmentReadCommand, SegmentTruncatedCommand, StreamSegmentInfoCommand, WrongHostCommand,
};
use crate::connection::{Connection, ConnectionReadHalf, ConnectionWriteHalf};
use crate::error::*;
use crate::wire_commands::{Decode, Encode, Replies, Requests};
use async_trait::async_trait;
use pravega_rust_client_config::connection_type::MockType;
use pravega_rust_client_shared::{PravegaNodeUri, ScopedSegment, SegmentInfo};
use std::cmp;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct MockConnection {
    id: Uuid,
    mock_type: MockType,
    endpoint: PravegaNodeUri,
    sender: Option<UnboundedSender<Replies>>,
    receiver: Option<UnboundedReceiver<Replies>>,
    buffer: Vec<u8>,
    buffer_offset: usize,
    // maps from segment to segment info
    segments: Arc<Mutex<HashMap<String, SegmentInfo>>>,
    // maps from writerId to segment
    writers: Arc<Mutex<HashMap<u128, String>>>,
}

impl MockConnection {
    pub fn new(
        endpoint: PravegaNodeUri,
        segments: Arc<Mutex<HashMap<String, SegmentInfo>>>,
        writers: Arc<Mutex<HashMap<u128, String>>>,
        mock_type: MockType,
    ) -> Self {
        let (tx, rx) = unbounded_channel();
        MockConnection {
            id: Uuid::new_v4(),
            mock_type,
            endpoint,
            sender: Some(tx),
            receiver: Some(rx),
            buffer: vec![],
            buffer_offset: 0,
            segments,
            writers,
        }
    }
}

#[async_trait]
impl Connection for MockConnection {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        let mut segments_guard = self.segments.lock().await;
        let mut writers_guard = self.writers.lock().await;
        match self.mock_type {
            MockType::Happy => {
                send_happy(
                    self.sender.as_mut().expect("get sender"),
                    payload,
                    &mut *segments_guard,
                    &mut *writers_guard,
                )
                .await
            }
            MockType::SegmentIsSealed => {
                send_sealed(self.sender.as_mut().expect("get sender"), payload).await
            }
            MockType::SegmentIsTruncated => {
                send_truncated(self.sender.as_mut().expect("get sender"), payload).await
            }
            MockType::WrongHost => send_wrong_host(self.sender.as_mut().expect("get sender"), payload).await,
        }
    }

    async fn read_async(&mut self, buf: &mut [u8]) -> Result<(), ConnectionError> {
        if self.buffer_offset == self.buffer.len() {
            let reply: Replies = self
                .receiver
                .as_mut()
                .expect("get receiver")
                .recv()
                .await
                .expect("read");
            self.buffer = reply.write_fields().expect("serialize reply");
            self.buffer_offset = 0;
        }
        buf.copy_from_slice(&self.buffer[self.buffer_offset..self.buffer_offset + buf.len()]);
        self.buffer_offset += buf.len();
        assert!(self.buffer_offset <= self.buffer.len());
        Ok(())
    }

    fn split(&mut self) -> (Box<dyn ConnectionReadHalf>, Box<dyn ConnectionWriteHalf>) {
        let reader = Box::new(MockReadingConnection {
            id: self.id,
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
            segments: self.segments.clone(),
            writers: self.writers.clone(),
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
    receiver: UnboundedReceiver<Replies>,
    buffer: Vec<u8>,
    index: usize,
}

#[derive(Debug)]
pub struct MockWritingConnection {
    id: Uuid,
    mock_type: MockType,
    sender: UnboundedSender<Replies>,
    // maps from segment to segment info
    segments: Arc<Mutex<HashMap<String, SegmentInfo>>>,
    // maps from writerId to segment
    writers: Arc<Mutex<HashMap<u128, String>>>,
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
}

#[async_trait]
impl ConnectionWriteHalf for MockWritingConnection {
    async fn send_async(&mut self, payload: &[u8]) -> Result<(), ConnectionError> {
        let mut segments_guard = self.segments.lock().await;
        let mut writers_guard = self.writers.lock().await;
        match self.mock_type {
            MockType::Happy => {
                send_happy(
                    &mut self.sender,
                    payload,
                    &mut *segments_guard,
                    &mut *writers_guard,
                )
                .await
            }
            MockType::SegmentIsSealed => send_sealed(&mut self.sender, payload).await,
            MockType::SegmentIsTruncated => send_truncated(&mut self.sender, payload).await,
            MockType::WrongHost => send_wrong_host(&mut self.sender, payload).await,
        }
    }

    fn get_id(&self) -> Uuid {
        self.id
    }
}

async fn send_happy(
    sender: &mut UnboundedSender<Replies>,
    payload: &[u8],
    segments: &mut HashMap<String, SegmentInfo>,
    writers: &mut HashMap<u128, String>,
) -> Result<(), ConnectionError> {
    let request: Requests = Requests::read_from(payload).expect("mock connection decode request");
    match request {
        Requests::Hello(cmd) => {
            let reply = Replies::Hello(cmd);
            sender.send(reply).expect("send reply");
        }
        Requests::SetupAppend(cmd) => {
            // initialize a new segment
            segments.entry(cmd.segment.to_string()).or_insert(SegmentInfo {
                segment: ScopedSegment::from(&*cmd.segment),
                starting_offset: 0,
                write_offset: 0,
                is_sealed: false,
                last_modified_time: 0,
            });
            writers.insert(cmd.writer_id, cmd.segment.to_string());
            let reply = Replies::AppendSetup(AppendSetupCommand {
                request_id: cmd.request_id,
                segment: cmd.segment,
                writer_id: cmd.writer_id,
                last_event_number: -9_223_372_036_854_775_808, // when there is no previous event in this segment
            });
            sender.send(reply).expect("send reply");
        }
        Requests::AppendBlockEnd(cmd) => {
            let segment = writers.get(&cmd.writer_id).expect("writer hasn't been set up");
            let segment_info = segments.get_mut(segment).expect("segment is not created");
            segment_info.write_offset += cmd.data.len() as i64;

            let reply = Replies::DataAppended(DataAppendedCommand {
                writer_id: cmd.writer_id,
                event_number: cmd.last_event_number,
                previous_event_number: 0, //not used in event stream writer
                request_id: cmd.request_id,
                current_segment_write_offset: 0, //not used in event stream writer
            });
            sender.send(reply).expect("send reply");
        }
        Requests::TruncateSegment(cmd) => {
            let segment_info = segments.get_mut(&cmd.segment).expect("segment is not created");
            segment_info.starting_offset = cmd.truncation_offset;

            let reply = Replies::SegmentTruncated(SegmentTruncatedCommand {
                request_id: cmd.request_id,
                segment: cmd.segment,
            });
            sender.send(reply).expect("send reply");
        }
        Requests::GetStreamSegmentInfo(cmd) => {
            let segment_info = segments
                .get_mut(&cmd.segment_name)
                .expect("segment is not created");

            let reply = Replies::StreamSegmentInfo(StreamSegmentInfoCommand {
                request_id: cmd.request_id,
                segment_name: cmd.segment_name.to_string(),
                exists: true,
                is_sealed: false,
                is_deleted: false,
                last_modified: 0,
                write_offset: segment_info.write_offset,
                start_offset: segment_info.starting_offset,
            });
            sender.send(reply).expect("send reply");
        }
        Requests::ReadSegment(cmd) => {
            let segment_info = segments.get(&cmd.segment).expect("segment is not created");
            assert!(segment_info.write_offset >= cmd.offset);

            let reply = if cmd.offset < segment_info.starting_offset {
                Replies::SegmentIsTruncated(SegmentIsTruncatedCommand {
                    request_id: cmd.request_id,
                    segment: cmd.segment.to_string(),
                    start_offset: segment_info.starting_offset,
                    server_stack_trace: "".to_string(),
                    offset: cmd.offset,
                })
            } else {
                let read_length = cmp::min(
                    segment_info.write_offset - cmd.offset,
                    cmd.suggested_length as i64,
                );
                Replies::SegmentRead(SegmentReadCommand {
                    segment: cmd.segment.to_string(),
                    offset: cmd.offset,
                    at_tail: false,
                    end_of_segment: false,
                    data: vec![1; read_length as usize],
                    request_id: cmd.request_id,
                })
            };
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
            let reply = Replies::SegmentIsSealed(SegmentIsSealedCommand {
                request_id: cmd.request_id,
                segment: "scope/stream/0".to_string(),
                server_stack_trace: "".to_string(),
                offset: 0,
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
            let reply = Replies::SegmentIsTruncated(SegmentIsTruncatedCommand {
                request_id: cmd.request_id,
                segment: "".to_string(),
                start_offset: 0,
                server_stack_trace: "".to_string(),
                offset: 0,
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
    use tracing::info;

    #[test]
    fn test_simple_write_and_read() {
        info!("mock client connection test");
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let mut mock_connection = MockConnection::new(
            PravegaNodeUri::from("127.1.1.1:9090"),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
            MockType::Happy,
        );
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
