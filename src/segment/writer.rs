//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactoryAsync;
use crate::error::Error;
use crate::segment::event::{Incoming, PendingEvent, ServerReply, WriterInfo};
use crate::segment::raw_client::{RawClient, RawClientError};
use crate::update;
use crate::util::metric::ClientMetrics;
use crate::util::{current_span, get_random_u128, get_request_id};

use pravega_client_auth::DelegationTokenProvider;
use pravega_client_channel::{CapacityGuard, ChannelSender};
use pravega_client_retry::retry_async::retry_async;
use pravega_client_retry::retry_policy::RetryWithBackoff;
use pravega_client_retry::retry_result::{RetryError, RetryResult};
use pravega_client_shared::*;
use pravega_connection_pool::connection_pool::ConnectionPoolError;
use pravega_controller_client::ControllerError;
use pravega_wire_protocol::client_connection::*;
use pravega_wire_protocol::commands::{
    AppendBlockEndCommand, ConditionalBlockEndCommand, SetupAppendCommand,
};
use pravega_wire_protocol::error::ClientConnectionError;
use pravega_wire_protocol::wire_commands::{Replies, Requests};

use snafu::{ResultExt, Snafu};
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use tokio::select;
use tokio::sync::oneshot;
use tracing::{debug, error, field, info, info_span, trace};
use tracing_futures::Instrument;

pub(crate) struct SegmentWriter {
    /// Unique id for each SegmentWriter.
    pub(crate) id: WriterId,

    /// The segment that this writer writes to.
    pub(crate) segment: ScopedSegment,

    /// Client connection that writes to the segmentstore.
    pub(crate) connection: Option<ClientConnectionWriteHalf>,

    // Closes listener task before setting up new connection.
    connection_listener_handle: Option<oneshot::Sender<bool>>,

    // Events that are sent but unacknowledged.
    inflight: VecDeque<Append>,

    // Events that are waiting to be sent.
    pending: VecDeque<Append>,

    // Incremental event id.
    event_num: i64,

    // The sender that sends back reply to reactor for processing.
    sender: ChannelSender<Incoming>,

    // The client retry policy.
    retry_policy: RetryWithBackoff,

    // Delegation token provider used to authenticate client when communicating with segmentstore.
    delegation_token_provider: Arc<DelegationTokenProvider>,

    // When ConditionalCheckFailure happens, there could be a race condition that the caller
    // sends out an append before it receives the ConditionalCheckFailure. This append will succeed
    // if it happens to have the correct offset, which breaks the ByteWriter contract.
    // So it's necessary to let caller explicitly send a reset signal to the reactor indicating
    // that the caller is fully aware of the ConditionalCheckFailure so any subsequent appends can be processed.
    // Before receiving such reset signal, reactor will reject any append.
    pub(crate) need_reset: bool,
}

impl SegmentWriter {
    // Maximum data size in one append block.
    const MAX_WRITE_SIZE: i32 = 8 * 1024 * 1024 + 8;
    // Maximum event number in one append block.
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
            need_reset: false,
        }
    }

    /// Sends a SetupAppend wirecommand to segmentstore. Upon completing setup, it
    /// spins up a listener task that listens to the reply from segmentstore and sends that reply
    /// to the reactor for processing.
    pub(crate) async fn setup_connection(
        &mut self,
        factory: &ClientFactoryAsync,
    ) -> Result<(), SegmentWriterError> {
        let span = info_span!("setup connection", segment_writer_id= %self.id, segment= %self.segment, host = field::Empty);
        // span.enter doesn't work for async code https://docs.rs/tracing/0.1.17/tracing/span/struct.Span.html#in-asynchronous-code
        async {
            info!("setting up connection for segment writer");
            // close current listener task by dropping the sender, receiver will automatically closes
            let (oneshot_tx, oneshot_rx) = oneshot::channel();
            self.connection_listener_handle = Some(oneshot_tx);

            // get endpoint
            let uri = match factory
                .controller_client()
                .get_endpoint_for_segment(&self.segment) // retries are internal to the controller client.
                .await
            {
                Ok(uri) => uri,
                Err(e) => return Err(SegmentWriterError::RetryControllerWriting { err: e }),
            };
            current_span().record("host", &field::debug(&uri));

            let request = Requests::SetupAppend(SetupAppendCommand {
                request_id: get_request_id(),
                writer_id: self.id.0,
                segment: self.segment.to_string(),
                delegation_token: self
                    .delegation_token_provider
                    .retrieve_token(factory.controller_client())
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
                        info!("failed to setup append using rawclient due to {:?}", e);
                        RetryResult::Retry(e)
                    }
                }
            })
            .await;

            let mut connection = match result {
                Ok((reply, connection)) => match reply {
                    Replies::AppendSetup(cmd) => {
                        debug!(
                            "append setup completed for writer:{:?}/segment:{:?} with latest event number {}",
                            self.id, self.segment, cmd.last_event_number
                        );
                        self.ack(cmd.last_event_number);
                        connection
                    }
                    _ => {
                        info!("append setup failed due to {:?}", reply);
                        return Err(SegmentWriterError::WrongReply {
                            expected: String::from("AppendSetup"),
                            actual: reply,
                        });
                    }
                },
                Err(e) => return Err(SegmentWriterError::RetryRawClient { err: e }),
            };
            let (r, w) = connection.split();
            self.connection = Some(w);

            // spins up a connection listener that keeps listening on the connection
            self.spawn_listener_task(r, oneshot_rx);
            info!("finished setting up connection");
            Ok(())
        }
        .instrument(span)
        .await
    }

    fn spawn_listener_task(&self, mut r: ClientConnectionReadHalf, mut oneshot_rx: oneshot::Receiver<bool>) {
        let segment = self.segment.clone();
        let sender = self.sender.clone();
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
                                info!("connection failed to read data back from segmentstore due to {:?}, closing listener task {:?}", e, listener_id);
                                let result = sender
                                    .send((Incoming::Reconnect(WriterInfo {
                                        segment: segment.clone(),
                                        connection_id: Some(r.get_id()),
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
        // conditional append
        let conditional = self.pending.front().unwrap().event.conditional_offset.is_some();
        let mut offset: i64 = -1;

        while let Some(append) = self.pending.pop_front() {
            assert!(
                append.event.data.len() <= SegmentWriter::MAX_WRITE_SIZE as usize,
                "event size {} must be under {}",
                append.event.data.len(),
                SegmentWriter::MAX_WRITE_SIZE
            );
            if append.event.data.len() + to_send.len() <= SegmentWriter::MAX_WRITE_SIZE as usize
                && event_count < SegmentWriter::MAX_EVENTS as usize
                && conditional == append.event.conditional_offset.is_some()
            {
                if conditional {
                    let event_offset = append.event.conditional_offset.as_ref().unwrap().to_owned();
                    if offset == -1 {
                        // first conditional append
                        offset = event_offset + append.event.data.len() as i64;
                    } else if offset != event_offset {
                        // next conditional append does not depend on the previous one to succeed,
                        // do not send them in one event.
                        self.pending.push_front(append);
                        break;
                    } else {
                        offset += append.event.data.len() as i64;
                    }
                }
                event_count += 1;
                total_size += append.event.data.len();
                to_send.extend(&append.event.data);
                self.inflight.push_back(append);
            } else {
                self.pending.push_front(append);
                break;
            }
        }

        debug!(
            "flushing {} events of total size {} to segment {:?} based on offset {:?}; event segment writer id {:?}/connection id: {:?}",
            event_count,
            to_send.len(),
            self.segment.to_string(),
            self.inflight.front().as_ref().unwrap().event.conditional_offset,
            self.id,
            self.connection.as_ref().expect("must have connection").get_id(),
        );

        let request = if let Some(offset) = self.inflight.front().unwrap().event.conditional_offset {
            Requests::ConditionalBlockEnd(ConditionalBlockEndCommand {
                writer_id: self.id.0,
                event_number: self.inflight.back().expect("last event").event_id,
                expected_offset: offset,
                data: to_send,
                request_id: get_request_id(),
            })
        } else {
            Requests::AppendBlockEnd(AppendBlockEndCommand {
                writer_id: self.id.0,
                size_of_whole_events: total_size as i32,
                data: to_send,
                num_event: self.inflight.len() as i32,
                last_event_number: self.inflight.back().expect("last event").event_id,
                request_id: get_request_id(),
            })
        };

        let writer = self.connection.as_mut().expect("must have connection");
        writer.write(&request).await.context(SegmentWriting {})?;

        update!(ClientMetrics::AppendBlockSize, total_size as u64, "Segment Writer Id" => self.id.to_string());
        update!(
            ClientMetrics::OutstandingAppendCount,
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
            if let Some(flush_sender) = acked.event.flush_oneshot_sender {
                if flush_sender.send(Result::Ok(())).is_err() {
                    info!(
                        "failed to send ack back to caller using oneshot due to Receiver dropped: event id {:?}",
                        acked.event_id
                    );
                }
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

    /// Reconnection will not exit until this writer has been successfully connected to the right host.
    ///
    /// It does the following steps:
    /// 1. sets up a new connection
    /// 2. puts inflight events back to the pending list
    /// 3. writes pending data to the server
    ///
    /// If error occurs during any one of the steps above, redo the reconnection from step 1.
    pub(crate) async fn reconnect(&mut self, factory: &ClientFactoryAsync) {
        debug!("Reconnecting segment writer {:?}", self.id);
        let connection_id = self.connection.as_ref().map(|c| c.get_id());

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

    /// Force delegation token provider to refresh.
    pub(crate) fn signal_delegation_token_expiry(&self) {
        self.delegation_token_provider.signal_token_expiry()
    }

    /// Fails event that has id bigger than or equal to the given event id.
    pub(crate) fn fail_events_upon_conditional_check_failure(&mut self, event_id: i64) {
        // remove failed append from inflight list
        while let Some(append) = self.inflight.pop_back() {
            if append.event_id >= event_id {
                let _res = append
                    .event
                    .oneshot_sender
                    .send(Result::Err(Error::ConditionalCheckFailure {
                        msg: format!("Conditional check failed for event {}", append.event_id),
                    }));
            } else {
                self.inflight.push_back(append);
                break;
            }
        }

        // clear pending list
        while let Some(append) = self.pending.pop_back() {
            let _res = append
                .event
                .oneshot_sender
                .send(Result::Err(Error::ConditionalCheckFailure {
                    msg: format!(
                        "Conditional check failed in previous appends. Event id {}",
                        append.event_id
                    ),
                }));
        }
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

#[derive(Debug, Snafu)]
pub enum SegmentWriterError {
    #[snafu(display("Failed to send request to segmentstore due to: {:?}", source))]
    SegmentWriting { source: ClientConnectionError },

    #[snafu(display("Retry failed due to error: {:?}", err))]
    RetryControllerWriting { err: RetryError<ControllerError> },

    #[snafu(display("Retry connection pool failed due to error {:?}", err))]
    RetryConnectionPool { err: RetryError<ConnectionPoolError> },

    #[snafu(display("Retry raw client failed due to error {:?}", err))]
    RetryRawClient { err: RetryError<RawClientError> },

    #[snafu(display("Wrong reply, expected {:?} but get {:?}", expected, actual))]
    WrongReply { expected: String, actual: Replies },

    #[snafu(display("Wrong host: {:?}", error_msg))]
    WrongHost { error_msg: String },

    #[snafu(display("Reactor is closed due to: {:?}", msg))]
    ReactorClosed { msg: String },

    #[snafu(display("Conditional check failed: {}", msg))]
    ConditionalCheckFailure { msg: String },
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::client_factory::ClientFactory;
    use crate::segment::event::RoutingInfo;
    use pravega_client_channel::{create_channel, ChannelReceiver};
    use pravega_client_config::connection_type::{ConnectionType, MockType};
    use pravega_client_config::ClientConfigBuilder;
    use tokio::sync::oneshot;

    type EventHandle = oneshot::Receiver<Result<(), Error>>;

    #[test]
    fn test_segment_writer_happy_write() {
        // set up segment writer
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (mut segment_writer, mut sender, mut receiver, factory) = create_segment_writer(MockType::Happy);

        // test set up connection
        let result = rt.block_on(segment_writer.setup_connection(&factory.to_async()));
        assert!(result.is_ok());

        // write data using mock connection
        let (event, guard, event_handle1) = rt.block_on(create_event(512, &mut sender, &mut receiver, None));
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

        let (event, guard, event_handle2) = rt.block_on(create_event(512, &mut sender, &mut receiver, None));
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
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (mut segment_writer, mut sender, mut receiver, factory) =
            create_segment_writer(MockType::SegmentIsSealed);

        // test set up connection
        let result = rt.block_on(segment_writer.setup_connection(&factory.to_async()));
        assert!(result.is_ok());

        // write data using mock connection to a sealed segment
        let (event, guard, _event_handle) = rt.block_on(create_event(512, &mut sender, &mut receiver, None));
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

    #[test]
    fn test_segment_writer_mixed_writes() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (mut segment_writer, mut sender, mut receiver, factory) = create_segment_writer(MockType::Happy);
        // test set up connection
        let result = rt.block_on(segment_writer.setup_connection(&factory.to_async()));
        assert!(result.is_ok());

        // conditional and unconditional events should not be mixed
        let (event0, guard0, _event_handle) =
            rt.block_on(create_event(128, &mut sender, &mut receiver, Some(0)));
        let (event1, guard1, _event_handle) =
            rt.block_on(create_event(128, &mut sender, &mut receiver, None));

        segment_writer.add_pending(event0, guard0);
        segment_writer.add_pending(event1, guard1);

        rt.block_on(segment_writer.write_pending_events()).expect("write");
        assert_eq!(segment_writer.pending.len(), 1);
        assert_eq!(segment_writer.inflight.len(), 1);

        segment_writer.inflight.clear();
        segment_writer.pending.clear();

        // conditional events with aligned offsets
        let (event0, guard0, _event_handle) =
            rt.block_on(create_event(128, &mut sender, &mut receiver, Some(0)));
        let (event1, guard1, _event_handle) =
            rt.block_on(create_event(128, &mut sender, &mut receiver, Some(128)));

        segment_writer.add_pending(event0, guard0);
        segment_writer.add_pending(event1, guard1);

        rt.block_on(segment_writer.write_pending_events()).expect("write");
        assert_eq!(segment_writer.pending.len(), 0);
        assert_eq!(segment_writer.inflight.len(), 2);

        segment_writer.inflight.clear();
        segment_writer.pending.clear();

        // conditional events with unaligned offsets
        let (event0, guard0, _event_handle) =
            rt.block_on(create_event(128, &mut sender, &mut receiver, Some(0)));
        let (event1, guard1, _event_handle) =
            rt.block_on(create_event(128, &mut sender, &mut receiver, Some(256)));

        segment_writer.add_pending(event0, guard0);
        segment_writer.add_pending(event1, guard1);

        rt.block_on(segment_writer.write_pending_events()).expect("write");
        assert_eq!(segment_writer.pending.len(), 1);
        assert_eq!(segment_writer.inflight.len(), 1);
    }

    #[test]
    fn test_segment_writer_conditional_append_failure() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (mut segment_writer, mut sender, mut receiver, factory) = create_segment_writer(MockType::Happy);
        // test set up connection
        let result = rt.block_on(segment_writer.setup_connection(&factory.to_async()));
        assert!(result.is_ok());

        // wrong offset
        let (event0, guard0, _event_handle) =
            rt.block_on(create_event(128, &mut sender, &mut receiver, Some(1)));
        let (event1, guard1, _event_handle) =
            rt.block_on(create_event(128, &mut sender, &mut receiver, Some(129)));
        let (event2, guard2, _event_handle) =
            rt.block_on(create_event(128, &mut sender, &mut receiver, Some(257)));

        segment_writer.add_pending(event0, guard0);
        segment_writer.add_pending(event1, guard1);
        segment_writer.add_pending(event2, guard2);

        rt.block_on(segment_writer.write_pending_events()).expect("write");
        assert_eq!(segment_writer.pending.len(), 0);
        assert_eq!(segment_writer.inflight.len(), 3);

        segment_writer.fail_events_upon_conditional_check_failure(1);
        assert_eq!(segment_writer.pending.len(), 0);
        assert_eq!(segment_writer.inflight.len(), 0);

        let (event0, guard0, _event_handle) =
            rt.block_on(create_event(128, &mut sender, &mut receiver, Some(1)));
        let (event1, guard1, _event_handle) =
            rt.block_on(create_event(128, &mut sender, &mut receiver, Some(129)));
        let (event2, guard2, _event_handle) =
            rt.block_on(create_event(128, &mut sender, &mut receiver, Some(257)));

        segment_writer.add_pending(event0, guard0);
        segment_writer.add_pending(event1, guard1);
        rt.block_on(segment_writer.write_pending_events()).expect("write");
        segment_writer.add_pending(event2, guard2);

        assert_eq!(segment_writer.pending.len(), 1);
        assert_eq!(segment_writer.inflight.len(), 2);

        segment_writer.fail_events_upon_conditional_check_failure(1);
        assert_eq!(segment_writer.pending.len(), 0);
        assert_eq!(segment_writer.inflight.len(), 0);
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
                factory.config().retry_policy,
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
        offset: Option<i64>,
    ) -> (PendingEvent, CapacityGuard, EventHandle) {
        let (oneshot_sender, oneshot_receiver) = tokio::sync::oneshot::channel();
        let event = PendingEvent::new(
            RoutingInfo::RoutingKey(Some("routing_key".into())),
            vec![1; size],
            offset,
            oneshot_sender,
            None,
        )
        .expect("create pending event");
        sender.send((Incoming::AppendEvent(event), size)).await.unwrap();
        loop {
            let (event, guard) = receiver.recv().await.unwrap();
            if let Incoming::AppendEvent(e) = event {
                return (e, guard, oneshot_receiver);
            }
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
