//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::trace;
use crate::{get_random_u128, get_request_id};
use snafu::ResultExt;
use std::collections::VecDeque;
use tracing::{debug, error, field, info, info_span, trace, warn};

use pravega_rust_client_retry::retry_async::retry_async;
use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
use pravega_rust_client_retry::retry_result::RetryResult;
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_connection::*;
use pravega_wire_protocol::commands::{AppendBlockEndCommand, SetupAppendCommand};
use pravega_wire_protocol::wire_commands::{Replies, Requests};

use crate::client_factory::ClientFactory;
use crate::error::*;
use crate::metric::ClientMetrics;
use crate::raw_client::RawClient;
use crate::reactor::event::{Incoming, PendingEvent, ServerReply, WriterInfo};
use pravega_rust_client_auth::DelegationTokenProvider;
use pravega_rust_client_channel::{CapacityGuard, ChannelSender};
use std::fmt;
use std::sync::Arc;
use tokio::select;
use tokio::sync::oneshot;
use tracing_futures::Instrument;

pub(crate) struct SegmentWriter {
    /// Unique id for each EventSegmentWriter.
    pub(crate) id: WriterId,

    /// The segment that this writer is writing to, it does not change for a EventSegmentWriter instance.
    pub(crate) segment: ScopedSegment,

    /// Client connection that writes to the segmentstore.
    pub(crate) connection: Option<ClientConnectionWriteHalf>,

    /// Closes listener task before setting up new connection.
    connection_listener_handle: Option<oneshot::Sender<bool>>,

    /// Events that are sent but unacknowledged.
    inflight: VecDeque<Append>,

    /// Events that are waiting to be sent.
    pending: VecDeque<Append>,

    /// Incremental event id.
    event_num: i64,

    /// The sender that sends back reply to reactor for processing.
    sender: ChannelSender<Incoming>,

    /// The client retry policy.
    retry_policy: RetryWithBackoff,

    /// Delegation token provider used to authenticate client when communicating with segmentstore.
    delegation_token_provider: Arc<DelegationTokenProvider>,
}

impl SegmentWriter {
    /// Maximum data size in one append block.
    const MAX_WRITE_SIZE: i32 = 8 * 1024 * 1024 + 8;
    /// Maximum event number in one append block.
    const MAX_EVENTS: i32 = 500;

    pub(crate) fn new(
        segment: ScopedSegment,
        sender: ChannelSender<Incoming>,
        retry_policy: RetryWithBackoff,
        delegation_token_provider: Arc<DelegationTokenProvider>,
    ) -> Self {
        SegmentWriter {
            id: WriterId::from(get_random_u128()),
            connection: None,
            segment,
            inflight: VecDeque::new(),
            pending: VecDeque::new(),
            event_num: 0,
            sender,
            retry_policy,
            delegation_token_provider,
            connection_listener_handle: None,
        }
    }

    pub(crate) fn pending_append_num(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn inflight_append_num(&self) -> usize {
        self.inflight.len()
    }

    /// Sends a SetupAppend wirecommand to segmentstore. Upon completing setup, it
    /// spins up a listener task that listens to the reply from segmentstore and sends that reply
    /// to the reactor for processing.
    pub(crate) async fn setup_connection(
        &mut self,
        factory: &ClientFactory,
    ) -> Result<(), SegmentWriterError> {
        let span = info_span!("setup connection", segment_writer_id= %self.id, segment= %self.segment, host = field::Empty);
        // span.enter doesn't work for async code https://docs.rs/tracing/0.1.17/tracing/span/struct.Span.html#in-asynchronous-code
        async {
            info!("setting up connection for segment writer");
            // close current listener task by dropping the sender, receiver will automatically closes
            let (oneshot_tx, mut oneshot_rx) = oneshot::channel();
            self.connection_listener_handle = Some(oneshot_tx);

            // get endpoint
            let uri = match factory
                .get_controller_client()
                .get_endpoint_for_segment(&self.segment) // retries are internal to the controller client.
                .await
            {
                Ok(uri) => uri,
                Err(e) => return Err(SegmentWriterError::RetryControllerWriting { err: e }),
            };
            trace::current_span().record("host", &field::debug(&uri));

            let request = Requests::SetupAppend(SetupAppendCommand {
                request_id: get_request_id(),
                writer_id: self.id.0,
                segment: self.segment.to_string(),
                delegation_token: self
                    .delegation_token_provider
                    .retrieve_token(factory.get_controller_client())
                    .await,
            });

            let raw_client = factory.create_raw_client_for_endpoint(uri);
            let result = retry_async(self.retry_policy, || async {
                debug!(
                    "setting up append for writer:{:?}/segment:{:?}",
                    self.id, self.segment
                );
                match raw_client.send_setup_request(&request).await {
                    Ok((reply, connection)) => RetryResult::Success((reply, connection)),
                    Err(e) => {
                        warn!("failed to setup append using rawclient due to {:?}", e);
                        RetryResult::Retry(e)
                    }
                }
            })
            .await;

            let mut connection = match result {
                Ok((reply, connection)) => match reply {
                    Replies::AppendSetup(cmd) => {
                        debug!(
                            "append setup completed for writer:{:?}/segment:{:?}",
                            self.id, self.segment
                        );
                        self.ack(cmd.last_event_number);
                        connection
                    }
                    _ => {
                        warn!("append setup failed due to {:?}", reply);
                        return Err(SegmentWriterError::WrongReply {
                            expected: String::from("AppendSetup"),
                            actual: reply,
                        });
                    }
                },
                Err(e) => return Err(SegmentWriterError::RetryRawClient { err: e }),
            };

            let (mut r, w) = connection.split();
            self.connection = Some(w);

            let segment = self.segment.clone();
            let sender = self.sender.clone();
            // spins up a connection listener that keeps listening on the connection
            let listener_id = r.get_id();
            let writer_id = self.id;
            tokio::spawn(async move {
                loop {
                    select! {
                        _ = &mut oneshot_rx => {
                            debug!("shut down connection listener {:?}", listener_id);
                            break;
                        }
                        result = r.read() => {
                            let reply = match result {
                                Ok(reply) => reply,
                                Err(e) => {
                                    warn!("connection failed to read data back from segmentstore due to {:?}, closing listener task {:?}", e, listener_id);
                                    let result = sender
                                        .send((Incoming::Reconnect(WriterInfo {
                                            segment: segment.clone(),
                                            connection_id: r.get_id(),
                                            writer_id,
                                        }), 0)).await;
                                    if let Err(e) = result {
                                        error!("failed to send connectionFailure signal to reactor {:?}", e);
                                    }
                                    break;
                                }
                            };
                            let res = sender
                                .send((Incoming::ServerReply(ServerReply {
                                    segment: segment.clone(),
                                    reply,
                                }), 0))
                                .await;

                            if let Err(e) = res {
                                error!("connection read data from segmentstore but failed to send reply back to reactor due to {:?}",e);
                                break;
                            }
                        }
                    }
                }
                info!("listener task shut down {:?}", listener_id);
            });
            info!("finished setting up connection");
            Ok(())
        }.instrument(span).await
    }

    /// Adds the event to the pending list
    /// then writes the pending list if the inflight list is empty.
    pub(crate) async fn write(
        &mut self,
        event: PendingEvent,
        cap_guard: CapacityGuard,
    ) -> Result<(), SegmentWriterError> {
        self.add_pending(event, cap_guard);
        self.write_pending_events().await
    }

    /// Adds the event to the pending list
    pub(crate) fn add_pending(&mut self, event: PendingEvent, cap_guard: CapacityGuard) {
        self.event_num += 1;
        self.pending.push_back(Append {
            event_id: self.event_num,
            event,
            cap_guard,
        });
    }

    /// Writes the pending events to the server. It will grab at most MAX_WRITE_SIZE of data
    /// from the pending list and send them to the server. Those events will be moved to inflight list waiting to be acked.
    pub(crate) async fn write_pending_events(&mut self) -> Result<(), SegmentWriterError> {
        if !self.inflight.is_empty() || self.pending.is_empty() {
            return Ok(());
        }

        let mut total_size = 0;
        let mut to_send = vec![];
        let mut event_count = 0;

        while let Some(append) = self.pending.pop_front() {
            assert!(
                append.event.data.len() <= SegmentWriter::MAX_WRITE_SIZE as usize,
                "event size {} must be under {}",
                append.event.data.len(),
                SegmentWriter::MAX_WRITE_SIZE
            );
            if append.event.data.len() + to_send.len() <= SegmentWriter::MAX_WRITE_SIZE as usize
                && event_count < SegmentWriter::MAX_EVENTS as usize
            {
                event_count += 1;
                total_size += append.event.data.len();
                to_send.extend(append.event.data.clone());
                self.inflight.push_back(append);
            } else {
                self.pending.push_front(append);
                break;
            }
        }

        debug!(
            "flushing {} events of total size {} to segment {:?}; event segment writer id {:?}/connection id: {:?}",
            event_count,
            to_send.len(),
            self.segment.to_string(),
            self.id,
            self.connection.as_ref().expect("must have connection").get_id(),
        );
        let request = Requests::AppendBlockEnd(AppendBlockEndCommand {
            writer_id: self.id.0,
            size_of_whole_events: total_size as i32,
            data: to_send,
            num_event: self.inflight.len() as i32,
            last_event_number: self.inflight.back().expect("last event").event_id,
            request_id: get_request_id(),
        });

        let writer = self.connection.as_mut().expect("must have connection");
        writer.write(&request).await.context(SegmentWriting {})?;

        update!(ClientMetrics::ClientAppendBlockSize, total_size as u64, "Segment Writer Id" => self.id.to_string());
        update!(
            ClientMetrics::ClientOutstandingAppendCount,
            self.pending.len() as u64,
            "Segment Writer Id" => self.id.to_string()
        );
        Ok(())
    }

    /// Acks inflight events. It will send the reply from server back to the caller using oneshot.
    pub(crate) fn ack(&mut self, event_id: i64) {
        // no events need to ack
        if self.inflight.is_empty() {
            return;
        }

        // event id is i64::MIN if no event acked for appendsetup
        if event_id < 0 {
            return;
        }

        loop {
            let acked = self.inflight.front().expect("must not be empty");

            // event has been acked before, it must be caused by reconnection
            if event_id < acked.event_id {
                return;
            }

            let acked = self.inflight.pop_front().expect("must have");
            if acked.event.oneshot_sender.send(Result::Ok(())).is_err() {
                trace!(
                    "failed to send ack back to caller using oneshot due to Receiver dropped: event id {:?}",
                    acked.event_id
                );
            }

            // ack up to event id
            if acked.event_id == event_id {
                break;
            }
        }
    }

    /// Gets the unacked events. Notice that it will pass the ownership
    /// of the unacked events to the caller, which means this method can only be called once.
    pub(crate) fn get_unacked_events(&mut self) -> Vec<Append> {
        let mut ret = vec![];
        while let Some(append) = self.inflight.pop_front() {
            ret.push(append);
        }
        while let Some(append) = self.pending.pop_front() {
            ret.push(append);
        }
        ret
    }

    /// Reconnect will not exist until this writer has been successfully connected to the right host.
    ///
    /// It does the following steps:
    /// 1. sets up a new connection
    /// 2. puts inflight events back to the pending list
    /// 3. writes pending data to the server
    ///
    /// If error occurs during any one of the steps above, redo the reconnect from step 1.
    pub(crate) async fn reconnect(&mut self, factory: &ClientFactory) {
        debug!("Reconnecting segment writer {:?}", self.id);
        let connection_id = if let Some(ref write_half) = self.connection {
            write_half.get_id()
        } else {
            panic!("should always have connection here");
        };

        // setup the connection
        let setup_res = self.setup_connection(factory).await;
        if setup_res.is_err() {
            self.sender
                .send((
                    Incoming::Reconnect(WriterInfo {
                        segment: self.segment.clone(),
                        connection_id,
                        writer_id: self.id,
                    }),
                    0,
                ))
                .await
                .expect("send reconnect signal to reactor");
            return;
        }

        while self.inflight.back().is_some() {
            self.pending
                .push_front(self.inflight.pop_back().expect("must have event"));
        }

        // flush any pending events
        let flush_res = self.write_pending_events().await;
        if flush_res.is_err() {
            self.sender
                .send((
                    Incoming::Reconnect(WriterInfo {
                        segment: self.segment.clone(),
                        connection_id,
                        writer_id: self.id,
                    }),
                    0,
                ))
                .await
                .expect("send reconnect signal to reactor");
        }
    }

    pub(crate) fn signal_delegation_token_expiry(&self) {
        self.delegation_token_provider.signal_token_expiry()
    }
}

impl fmt::Debug for SegmentWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentWriter")
            .field("segment writer id", &self.id)
            .field("segment", &self.segment)
            .field(
                "connection",
                &match &self.connection {
                    Some(w) => format!("WritingClientConnection is {:?}", w),
                    None => "doesn't have connection".to_owned(),
                },
            )
            .field(
                "inflight",
                &format!("number of inflight events is {}", &self.inflight.len()),
            )
            .field(
                "pending",
                &format!("number of pending events is {}", &self.pending.len()),
            )
            .field("current event_number", &self.event_num)
            .finish()
    }
}

pub(crate) struct Append {
    pub(crate) event_id: i64,
    pub(crate) event: PendingEvent,
    pub(crate) cap_guard: CapacityGuard,
}

#[derive(Debug)]
struct WriteHalfConnectionWrapper {
    write_half: ClientConnectionWriteHalf,
    // whether to close the reactor after merging the write half and read half
    close_reactor: bool,
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use pravega_rust_client_channel::{create_channel, ChannelReceiver};
    use pravega_rust_client_config::connection_type::{ConnectionType, MockType};
    use pravega_rust_client_config::ClientConfigBuilder;
    use tokio::sync::oneshot;

    type EventHandle = oneshot::Receiver<Result<(), SegmentWriterError>>;

    #[test]
    fn test_segment_writer_happy_write() {
        // set up segment writer
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let (mut segment_writer, mut sender, mut receiver, factory) = create_segment_writer(MockType::Happy);

        // test set up connection
        let result = rt.block_on(segment_writer.setup_connection(&factory));
        assert!(result.is_ok());

        // write data using mock connection
        let (event, guard, event_handle1) = rt.block_on(create_event(512, &mut sender, &mut receiver));
        let (reply, cap_guard) = rt
            .block_on(async {
                segment_writer.write(event, guard).await.expect("write data");
                return receiver.recv().await;
            })
            .expect("receive DataAppend from segment writer");

        assert_eq!(cap_guard.size, 0);
        assert_eq!(segment_writer.event_num, 1);
        assert_eq!(segment_writer.inflight.len(), 1);
        assert!(segment_writer.pending.is_empty());

        let (event, guard, event_handle2) = rt.block_on(create_event(512, &mut sender, &mut receiver));
        rt.block_on(segment_writer.write(event, guard))
            .expect("write data");

        assert_eq!(segment_writer.event_num, 2);
        assert_eq!(segment_writer.inflight.len(), 1);
        assert_eq!(segment_writer.pending.len(), 1);
        ack_server_reply(reply, &mut segment_writer);
        rt.block_on(segment_writer.write_pending_events())
            .expect("write data");
        let (reply, cap_guard) = rt
            .block_on(receiver.recv())
            .expect("receive DataAppend from segment writer");
        ack_server_reply(reply, &mut segment_writer);

        assert_eq!(cap_guard.size, 0);
        assert!(segment_writer.inflight.is_empty());
        assert!(segment_writer.pending.is_empty());

        let caller_reply = rt.block_on(event_handle1).expect("caller receive reply");
        assert!(caller_reply.is_ok());
        let caller_reply = rt.block_on(event_handle2).expect("caller receive reply");
        assert!(caller_reply.is_ok());
    }

    #[test]
    fn test_segment_writer_reply_error() {
        // set up segment writer
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let (mut segment_writer, mut sender, mut receiver, factory) =
            create_segment_writer(MockType::SegmentIsSealed);

        // test set up connection
        let result = rt.block_on(segment_writer.setup_connection(&factory));
        assert!(result.is_ok());

        // write data using mock connection to a sealed segment
        let (event, guard, _event_handle) = rt.block_on(create_event(512, &mut sender, &mut receiver));
        let (reply, cap_guard) = rt
            .block_on(async {
                segment_writer.write(event, guard).await.expect("write data");
                receiver.recv().await
            })
            .expect("receive DataAppend from segment writer");
        assert_eq!(cap_guard.size, 0);

        let result = if let Incoming::ServerReply(server) = reply {
            if let Replies::SegmentIsSealed(_cmd) = server.reply {
                true
            } else {
                false
            }
        } else {
            false
        };
        assert!(result);
    }

    // helper function section
    pub(crate) fn create_segment_writer(
        mock: MockType,
    ) -> (
        SegmentWriter,
        ChannelSender<Incoming>,
        ChannelReceiver<Incoming>,
        ClientFactory,
    ) {
        let segment = ScopedSegment::from("testScope/testStream/0");
        let config = ClientConfigBuilder::default()
            .connection_type(ConnectionType::Mock(mock))
            .controller_uri(PravegaNodeUri::from("127.0.0.1:9091".to_string()))
            .mock(true)
            .build()
            .unwrap();
        let factory = ClientFactory::new(config);
        let (sender, receiver) = create_channel(1024);
        let delegation_token_provider = DelegationTokenProvider::new(ScopedStream::from(&segment));
        (
            SegmentWriter::new(
                segment,
                sender.clone(),
                factory.get_config().retry_policy,
                Arc::new(delegation_token_provider),
            ),
            sender,
            receiver,
            factory,
        )
    }

    async fn create_event(
        size: usize,
        sender: &mut ChannelSender<Incoming>,
        receiver: &mut ChannelReceiver<Incoming>,
    ) -> (PendingEvent, CapacityGuard, EventHandle) {
        let (oneshot_sender, oneshot_receiver) = tokio::sync::oneshot::channel();
        let event = PendingEvent::new(Some("routing_key".into()), vec![1; size], oneshot_sender)
            .expect("create pending event");
        sender.send((Incoming::AppendEvent(event), 512)).await.unwrap();
        let (event, guard) = receiver.recv().await.unwrap();
        if let Incoming::AppendEvent(e) = event {
            (e, guard, oneshot_receiver)
        } else {
            panic!("wrong type");
        }
    }

    fn ack_server_reply(reply: Incoming, writer: &mut SegmentWriter) {
        let result = if let Incoming::ServerReply(server) = reply {
            if let Replies::DataAppended(cmd) = server.reply {
                writer.ack(cmd.event_number);
                true
            } else {
                false
            }
        } else {
            false
        };
        assert!(result);
    }
}
