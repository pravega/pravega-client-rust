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
use pravega_client_auth::DelegationTokenProvider;
use pravega_client_shared::{PravegaNodeUri, ScopedSegment};
use pravega_wire_protocol::commands::{ReadSegmentCommand, SegmentReadCommand};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use snafu::Snafu;
use std::result::Result as StdResult;

use crate::client_factory::ClientFactory;
use crate::error::RawClientError;
use crate::get_request_id;
use crate::raw_client::RawClient;
use pravega_client_retry::retry_async::retry_async;
use pravega_client_retry::retry_result::RetryResult;
use pravega_client_retry::retry_result::Retryable;
use std::cmp;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{oneshot, Mutex};
use tracing::warn;

#[derive(Debug, Snafu)]
pub enum ReaderError {
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    SegmentIsTruncated {
        segment: String,
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    SegmentSealed {
        segment: String,
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    OperationError {
        segment: String,
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Could not connect due to {}", error_msg))]
    ConnectionError {
        segment: String,
        can_retry: bool,
        source: RawClientError,
        error_msg: String,
    },
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    AuthTokenCheckFailed {
        segment: String,
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
    #[snafu(display("Could not connect due to {}", error_msg))]
    AuthTokenExpired {
        segment: String,
        can_retry: bool,
        source: RawClientError,
        error_msg: String,
    },
    #[snafu(display("Could not connect due to {}", error_msg))]
    WrongHost {
        segment: String,
        can_retry: bool,
        operation: String,
        error_msg: String,
    },
}

///
/// Fetch the segment from a given ReaderError
///
impl ReaderError {
    pub(crate) fn get_segment(&self) -> String {
        use ReaderError::*;
        match self {
            SegmentIsTruncated {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => segment.clone(),
            SegmentSealed {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => segment.clone(),
            OperationError {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => segment.clone(),
            ConnectionError {
                segment,
                can_retry: _,
                source: _,
                error_msg: _,
            } => segment.clone(),
            AuthTokenCheckFailed {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => segment.clone(),
            AuthTokenExpired {
                segment,
                can_retry: _,
                source: _,
                error_msg: _,
            } => segment.clone(),
            WrongHost {
                segment,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => segment.clone(),
        }
    }

    fn refresh_token(&self) -> bool {
        matches!(self, ReaderError::AuthTokenExpired { .. })
    }
}
// Implementation of Retryable trait for the error thrown by the Controller.
// this ensures we can use the wrap_with_async_retry macros.
impl Retryable for ReaderError {
    fn can_retry(&self) -> bool {
        use ReaderError::*;
        match self {
            SegmentIsTruncated {
                segment: _,
                can_retry,
                operation: _,
                error_msg: _,
            } => *can_retry,
            SegmentSealed {
                segment: _,
                can_retry,
                operation: _,
                error_msg: _,
            } => *can_retry,
            OperationError {
                segment: _,
                can_retry,
                operation: _,
                error_msg: _,
            } => *can_retry,
            ConnectionError {
                segment: _,
                can_retry,
                source: _,
                error_msg: _,
            } => *can_retry,
            AuthTokenCheckFailed {
                segment: _,
                can_retry,
                operation: _,
                error_msg: _,
            } => *can_retry,
            AuthTokenExpired {
                segment: _,
                can_retry,
                source: _,
                error_msg: _,
            } => *can_retry,
            WrongHost {
                segment: _,
                can_retry,
                operation: _,
                error_msg: _,
            } => *can_retry,
        }
    }
}

///
/// AsyncSegmentReader is used to read from a given segment given the connection pool and the Controller URI
/// The reads given the offset and the length are processed asynchronously.
/// e.g: usage pattern is
/// AsyncSegmentReaderImpl::new(&segment_name, connection_pool, "http://controller uri").await
///
#[async_trait]
pub trait AsyncSegmentReader: Send + Sync {
    async fn read(&self, offset: i64, length: i32) -> StdResult<SegmentReadCommand, ReaderError>;
}

#[derive(new)]
pub struct AsyncSegmentReaderImpl {
    segment: ScopedSegment,
    endpoint: Mutex<PravegaNodeUri>,
    factory: ClientFactory,
    delegation_token_provider: DelegationTokenProvider,
}

#[async_trait]
impl AsyncSegmentReader for AsyncSegmentReaderImpl {
    async fn read(&self, offset: i64, length: i32) -> StdResult<SegmentReadCommand, ReaderError> {
        retry_async(self.factory.get_config().retry_policy, || async {
            let raw_client = self
                .factory
                .create_raw_client_for_endpoint(self.endpoint.lock().await.clone());
            match self.read_inner(offset, length, &raw_client).await {
                Ok(cmd) => RetryResult::Success(cmd),
                Err(e) => {
                    if e.can_retry() {
                        let controller = self.factory.get_controller_client();
                        let endpoint = controller
                            .get_endpoint_for_segment(&self.segment)
                            .await
                            .expect("get endpoint for async semgnet reader");
                        let mut guard = self.endpoint.lock().await;
                        *guard = endpoint;
                        if e.refresh_token() {
                            self.delegation_token_provider.signal_token_expiry();
                        }
                        RetryResult::Retry(e)
                    } else {
                        RetryResult::Fail(e)
                    }
                }
            }
        })
        .await
        .map_err(|e| e.error)
    }
}

impl AsyncSegmentReaderImpl {
    pub async fn init(
        segment: ScopedSegment,
        factory: ClientFactory,
        delegation_token_provider: DelegationTokenProvider,
    ) -> AsyncSegmentReaderImpl {
        let endpoint = factory
            .get_controller_client()
            .get_endpoint_for_segment(&segment)
            .await
            .expect("get endpoint for segment");

        AsyncSegmentReaderImpl {
            segment,
            endpoint: Mutex::new(endpoint),
            factory: factory.clone(),
            delegation_token_provider,
        }
    }

    async fn read_inner(
        &self,
        offset: i64,
        length: i32,
        raw_client: &dyn RawClient<'_>,
    ) -> StdResult<SegmentReadCommand, ReaderError> {
        let request = Requests::ReadSegment(ReadSegmentCommand {
            segment: self.segment.to_string(),
            offset,
            suggested_length: length,
            delegation_token: self
                .delegation_token_provider
                .retrieve_token(self.factory.get_controller_client())
                .await,
            request_id: get_request_id(),
        });

        let reply = raw_client.send_request(&request).await;
        match reply {
            Ok(reply) => match reply {
                Replies::SegmentRead(cmd) => {
                    assert_eq!(
                        cmd.offset, offset,
                        "Offset of SegmentRead response different from the request"
                    );
                    Ok(cmd)
                }
                Replies::AuthTokenCheckFailed(_cmd) => Err(ReaderError::AuthTokenCheckFailed {
                    segment: self.segment.to_string(),
                    can_retry: false,
                    operation: "Read segment".to_string(),
                    error_msg: "Auth token expired".to_string(),
                }),
                Replies::NoSuchSegment(_cmd) => Err(ReaderError::SegmentIsTruncated {
                    segment: self.segment.to_string(),
                    can_retry: false,
                    operation: "Read segment".to_string(),
                    error_msg: "No Such Segment".to_string(),
                }),
                Replies::SegmentIsTruncated(_cmd) => Err(ReaderError::SegmentIsTruncated {
                    segment: self.segment.to_string(),
                    can_retry: false,
                    operation: "Read segment".to_string(),
                    error_msg: "Segment truncated".into(),
                }),
                Replies::WrongHost(_cmd) => Err(ReaderError::WrongHost {
                    segment: self.segment.to_string(),
                    can_retry: true,
                    operation: "Read segment".to_string(),
                    error_msg: "Wrong host".to_string(),
                }),
                Replies::SegmentIsSealed(cmd) => Ok(SegmentReadCommand {
                    segment: self.segment.to_string(),
                    offset,
                    at_tail: true,
                    end_of_segment: true,
                    data: vec![],
                    request_id: cmd.request_id,
                }),
                _ => Err(ReaderError::OperationError {
                    segment: self.segment.to_string(),
                    can_retry: false,
                    operation: "Read segment".to_string(),
                    error_msg: "".to_string(),
                }),
            },
            Err(error) => match error {
                RawClientError::AuthTokenExpired { .. } => Err(ReaderError::AuthTokenExpired {
                    segment: self.segment.to_string(),
                    can_retry: true,
                    source: error,
                    error_msg: "Auth token expired".to_string(),
                }),
                _ => Err(ReaderError::ConnectionError {
                    segment: self.segment.to_string(),
                    can_retry: true,
                    source: error,
                    error_msg: "RawClient error".to_string(),
                }),
            },
        }
    }
}

/// Prefetches data using AsyncSegmentReader.
///
/// It maintains a buffer and prefetches data to fill that buffer in the background so that
/// every read call can read from the local buffer instead of waiting for server response.
pub(crate) struct PrefetchingAsyncSegmentReader {
    buffer: VecDeque<SegmentReadCommand>,
    buffer_offset: usize,
    reader: Arc<Box<dyn AsyncSegmentReader>>,
    read_size: usize,
    pub(crate) offset: i64,
    end_of_segment: bool,
    receiver: Option<oneshot::Receiver<Result<SegmentReadCommand, ReaderError>>>,
    handle: Handle,
    cnt: i32,
}

// maximum number of buffered reply
const BUFFERED_REPLY_NUMBER: usize = 2;

impl PrefetchingAsyncSegmentReader {
    pub(crate) fn new(
        handle: Handle,
        reader: Arc<Box<dyn AsyncSegmentReader>>,
        offset: i64,
        buffer_size: usize,
    ) -> Self {
        let mut wrapper = PrefetchingAsyncSegmentReader {
            buffer: VecDeque::with_capacity(BUFFERED_REPLY_NUMBER),
            buffer_offset: 0,
            reader,
            read_size: buffer_size / BUFFERED_REPLY_NUMBER,
            offset,
            handle,
            end_of_segment: false,
            receiver: None,
            cnt: 0,
        };
        wrapper.issue_request_if_needed();
        wrapper
    }

    /// Reads from buffered data.
    ///
    /// Note: bytes returned could be less than requested.
    pub(crate) async fn read(&mut self, buf: &mut [u8]) -> StdResult<usize, ReaderError> {
        self.fill_buffer_if_available()?;
        self.issue_request_if_needed();

        while self.buffer.is_empty() {
            if self.end_of_segment {
                // no more bytes could be read from the current offset
                return Ok(0);
            }
            self.fill_buffer().await?;
            self.issue_request_if_needed();
        }
        let mut need_to_read = buf.len();
        let mut copy_offset = 0;
        while let Some(cmd) = self.buffer.front() {
            self.end_of_segment = cmd.end_of_segment;
            let read_size = cmp::min(need_to_read, cmd.data.len() - self.buffer_offset);
            buf[copy_offset..copy_offset + read_size]
                .copy_from_slice(&cmd.data[self.buffer_offset..self.buffer_offset + read_size]);
            self.offset += read_size as i64;
            self.buffer_offset += read_size;
            need_to_read -= read_size;
            copy_offset += read_size;

            if cmd.data.len() == self.buffer_offset {
                // cmd has been read entirely
                self.buffer.pop_front();
                self.buffer_offset = 0;
            }

            if need_to_read == 0 {
                //has read up to requested size
                break;
            }
        }

        Ok(buf.len() - need_to_read)
    }

    /// Returns the underlying reader and drops the prefetching reader.
    pub(crate) fn extract_reader(self) -> Arc<Box<dyn AsyncSegmentReader>> {
        self.reader
    }

    /// Returns the size of data availble in buffer
    pub(crate) fn available(&self) -> usize {
        let mut size = 0;
        for (i, cmd) in self.buffer.iter().enumerate() {
            if i == 0 {
                size += cmd.data.len() - self.buffer_offset;
            } else {
                size += cmd.data.len();
            }
        }
        size
    }

    fn issue_request_if_needed(&mut self) {
        if !self.end_of_segment && self.receiver.is_none() {
            self.cnt += 1;
            let (sender, receiver) = oneshot::channel();
            self.handle.spawn(PrefetchingAsyncSegmentReader::read_async(
                self.reader.clone(),
                sender,
                self.offset + self.available() as i64,
                self.read_size as i32,
            ));
            self.receiver = Some(receiver);
        }
    }

    // issues the read in the background
    async fn read_async(
        reader: Arc<Box<dyn AsyncSegmentReader>>,
        sender: oneshot::Sender<Result<SegmentReadCommand, ReaderError>>,
        offset: i64,
        length: i32,
    ) {
        let result = reader.read(offset, length).await;
        sender
            .send(result)
            .map_err(|e| warn!("failed to send reply back: {:?}", e))
            .expect("send reply back");
    }

    fn fill_buffer_if_available(&mut self) -> Result<(), ReaderError> {
        if self.buffer.len() >= BUFFERED_REPLY_NUMBER {
            return Ok(());
        }
        if let Some(mut recv) = self.receiver.take() {
            match recv.try_recv() {
                Err(TryRecvError::Empty) => {
                    self.receiver = Some(recv);
                    return Ok(());
                }
                Err(e) => warn!("failed to receive reply from background read: {}", e),
                Ok(res) => match res {
                    Ok(cmd) => {
                        if !cmd.end_of_segment && cmd.data.is_empty() {
                            return Ok(());
                        }
                        self.buffer.push_back(cmd);
                    }
                    Err(e) => return Err(e),
                },
            }
        }
        Ok(())
    }
    async fn fill_buffer(&mut self) -> Result<(), ReaderError> {
        if let Some(receiver) = self.receiver.take() {
            match receiver.await {
                Ok(res) => match res {
                    Ok(cmd) => {
                        self.buffer.push_back(cmd);
                    }
                    Err(e) => return Err(e),
                },
                Err(e) => {
                    warn!("failed to receive reply from background read: {}", e);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mockall::predicate::*;
    use mockall::*;
    use tokio::time::delay_for;

    use pravega_client_shared::*;
    use pravega_wire_protocol::client_connection::ClientConnection;
    use pravega_wire_protocol::commands::{
        Command, EventCommand, NoSuchSegmentCommand, SegmentIsSealedCommand, SegmentIsTruncatedCommand,
    };

    use super::*;
    use crate::client_factory::ClientFactory;
    use pravega_client_config::ClientConfigBuilder;
    use tokio::runtime::Runtime;

    // Setup mock.
    mock! {
        pub RawClientImpl {
            fn send_request(&self, request: &Requests) -> Result<Replies, RawClientError>{
            }
        }
    }

    #[async_trait]
    impl RawClient<'static> for MockRawClientImpl {
        async fn send_request(&self, request: &Requests) -> Result<Replies, RawClientError> {
            delay_for(Duration::from_nanos(1)).await;
            self.send_request(request)
        }

        async fn send_setup_request(
            &self,
            _request: &Requests,
        ) -> Result<(Replies, Box<dyn ClientConnection + 'static>), RawClientError> {
            unimplemented!() // Not required for this test.
        }
    }

    #[test]
    fn test_read_happy_path() {
        let config = ClientConfigBuilder::default()
            .controller_uri(pravega_client_config::MOCK_CONTROLLER_URI)
            .is_auth_enabled(false)
            .mock(true)
            .build()
            .expect("creating config");
        let factory = ClientFactory::new(config);
        let runtime = factory.get_runtime_handle();

        let scope_name = Scope::from("examples".to_owned());
        let stream_name = Stream::from("someStream".to_owned());

        let segment_name = ScopedSegment {
            scope: scope_name,
            stream: stream_name,
            segment: Segment {
                number: 0,
                tx_id: None,
            },
        };

        let segment_name_copy = segment_name.clone();
        let mut raw_client = MockRawClientImpl::new();
        let mut request_cnt = 1;
        raw_client
            .expect_send_request()
            .returning(move |req: &Requests| match req {
                Requests::ReadSegment(_cmd) => {
                    if request_cnt == 1 {
                        request_cnt += 1;
                        Ok(Replies::SegmentRead(SegmentReadCommand {
                            segment: segment_name_copy.to_string(),
                            offset: 0,
                            at_tail: false,
                            end_of_segment: false,
                            data: vec![0, 0, 0, 0, 0, 0, 0, 3, 97, 98, 99],
                            request_id: 1,
                        }))
                    } else if request_cnt == 2 {
                        request_cnt += 1;
                        Ok(Replies::NoSuchSegment(NoSuchSegmentCommand {
                            segment: segment_name_copy.to_string(),
                            server_stack_trace: "".to_string(),
                            offset: 0,
                            request_id: 2,
                        }))
                    } else if request_cnt == 3 {
                        request_cnt += 1;
                        Ok(Replies::SegmentIsTruncated(SegmentIsTruncatedCommand {
                            request_id: 3,
                            segment: segment_name_copy.to_string(),
                            start_offset: 0,
                            server_stack_trace: "".to_string(),
                            offset: 0,
                        }))
                    } else {
                        Ok(Replies::SegmentIsSealed(SegmentIsSealedCommand {
                            request_id: 4,
                            segment: segment_name_copy.to_string(),
                            server_stack_trace: "".to_string(),
                            offset: 0,
                        }))
                    }
                }
                _ => Ok(Replies::NoSuchSegment(NoSuchSegmentCommand {
                    segment: segment_name_copy.to_string(),
                    server_stack_trace: "".to_string(),
                    offset: 0,
                    request_id: 1,
                })),
            });
        let async_segment_reader = runtime.block_on(factory.create_async_event_reader(segment_name));
        let data = runtime.block_on(async_segment_reader.read_inner(0, 11, &raw_client));
        let segment_read_result: SegmentReadCommand = data.unwrap();
        assert_eq!(
            segment_read_result.segment,
            "examples/someStream/0.#epoch.0".to_string()
        );
        assert_eq!(segment_read_result.offset, 0);
        assert_eq!(segment_read_result.at_tail, false);
        assert_eq!(segment_read_result.end_of_segment, false);
        let event_data = EventCommand::read_from(segment_read_result.data.as_slice()).unwrap();
        let data = std::str::from_utf8(event_data.data.as_slice()).unwrap();
        assert_eq!("abc", data);

        // simulate NoSuchSegment
        let data = runtime.block_on(async_segment_reader.read_inner(11, 1024, &raw_client));
        let segment_read_result: ReaderError = data.err().unwrap();
        match segment_read_result {
            ReaderError::SegmentIsTruncated {
                segment: _,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => assert_eq!(segment_read_result.can_retry(), false),
            _ => assert!(false, "Segment is truncated expected"),
        }

        // simulate SegmentTruncated
        let data = runtime.block_on(async_segment_reader.read_inner(12, 1024, &raw_client));
        let segment_read_result: ReaderError = data.err().unwrap();
        match segment_read_result {
            ReaderError::SegmentIsTruncated {
                segment: _,
                can_retry: _,
                operation: _,
                error_msg: _,
            } => assert_eq!(segment_read_result.can_retry(), false),
            _ => assert!(false, "Segment is truncated expected"),
        }

        // simulate SealedSegment
        let data = runtime.block_on(async_segment_reader.read_inner(13, 1024, &raw_client));
        let segment_read_result: SegmentReadCommand = data.unwrap();
        assert_eq!(
            segment_read_result.segment,
            "examples/someStream/0.#epoch.0".to_string()
        );
        assert_eq!(segment_read_result.offset, 13);
        assert_eq!(segment_read_result.at_tail, true);
        assert_eq!(segment_read_result.end_of_segment, true);
        assert_eq!(segment_read_result.data.len(), 0);
    }

    struct MockSegmentReader {}

    #[async_trait]
    impl AsyncSegmentReader for MockSegmentReader {
        async fn read(&self, offset: i64, length: i32) -> Result<SegmentReadCommand, ReaderError> {
            Ok(SegmentReadCommand {
                segment: "segment".to_string(),
                offset,
                at_tail: false,
                end_of_segment: false,
                data: vec![1; length as usize],
                request_id: 0,
            })
        }
    }

    #[test]
    fn test_prefetch_async_segment_reader() {
        let mock = MockSegmentReader {};
        let runtime = Runtime::new().unwrap();
        let handle = runtime.handle();
        let mut prefetch_reader = PrefetchingAsyncSegmentReader::new(
            runtime.handle().clone(),
            Arc::new(Box::new(mock)),
            0,
            1024 * 4,
        );

        // one buffered reply should contains 1024 * 2 size of data,
        // first read should read half of the buffered reply
        let mut buf = vec![0; 1024];
        let read = handle
            .block_on(prefetch_reader.read(&mut buf))
            .expect("read from wrapper");
        assert_eq!(read, 1024);
        assert_eq!(prefetch_reader.buffer.len(), 1);
        assert_eq!(prefetch_reader.buffer_offset, 1024);
        assert_eq!(prefetch_reader.offset, 1024);

        // reads next 1024 bytes and finishes the first buffered reply.
        let mut buf = vec![0; 1024];
        let read = handle
            .block_on(prefetch_reader.read(&mut buf))
            .expect("read from wrapper");
        assert_eq!(read, 1024);
        assert_eq!(prefetch_reader.offset, 1024 * 2);
        assert_eq!(prefetch_reader.buffer_offset, 0);
    }
}
