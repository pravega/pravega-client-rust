//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hasher;
use std::net::SocketAddr;
use std::sync::Arc;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use snafu::ResultExt;

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{debug, error, warn};
use uuid::Uuid;

use pravega_rust_client_retry::retry_async::retry_async;
use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
use pravega_rust_client_retry::retry_result::RetryResult;
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::ClientConfig;
use pravega_wire_protocol::client_connection::*;
use pravega_wire_protocol::commands::{AppendBlockEndCommand, Command, EventCommand, SetupAppendCommand};
use pravega_wire_protocol::wire_commands::{Replies, Requests};

use crate::client_factory::ClientFactoryInternal;
use crate::error::*;
use crate::raw_client::RawClient;

/// EventStreamWriter contains a writer id and a mpsc sender which is used to send Event
/// to the Processor
pub struct EventStreamWriter {
    writer_id: Uuid,
    sender: Sender<Incoming>,
}

impl EventStreamWriter {
    const CHANNEL_CAPACITY: usize = 100;

    pub(crate) fn new(
        stream: ScopedStream,
        config: ClientConfig,
        factory: Arc<ClientFactoryInternal>,
    ) -> Self {
        let (tx, rx) = channel(EventStreamWriter::CHANNEL_CAPACITY);
        let selector = SegmentSelector::new(stream, tx.clone(), config, factory.clone());
        let processor = Processor {
            receiver: rx,
            selector,
            factory,
        };
        tokio::spawn(Processor::run(processor));
        EventStreamWriter {
            writer_id: Uuid::new_v4(),
            sender: tx,
        }
    }

    pub async fn write_event(
        &mut self,
        event: Vec<u8>,
    ) -> oneshot::Receiver<Result<(), EventStreamWriterError>> {
        let (tx, rx) = oneshot::channel();
        let append_event = Incoming::AppendEvent(AppendEvent {
            inner: event,
            routing_key: Option::None,
            oneshot_sender: tx,
        });
        if let Err(_e) = self.sender.send(append_event).await {
            let (tx_error, rx_error) = oneshot::channel();
            tx_error
                .send(Err(EventStreamWriterError::SendToProcessor {}))
                .expect("send error");
            rx_error
        } else {
            rx
        }
    }

    pub async fn write_event_by_routing_key(
        &mut self,
        routing_key: String,
        event: Vec<u8>,
    ) -> oneshot::Receiver<Result<(), EventStreamWriterError>> {
        let (tx, rx) = oneshot::channel();
        let append_event = Incoming::AppendEvent(AppendEvent {
            inner: event,
            routing_key: Option::Some(routing_key),
            oneshot_sender: tx,
        });
        if let Err(_e) = self.sender.send(append_event).await {
            let (tx_error, rx_error) = oneshot::channel();
            tx_error
                .send(Err(EventStreamWriterError::SendToProcessor {}))
                .expect("send error");
            rx_error
        } else {
            rx
        }
    }
}

pub struct Processor {
    receiver: Receiver<Incoming>,
    selector: SegmentSelector,
    factory: Arc<ClientFactoryInternal>,
}

impl Processor {
    #[allow(clippy::cognitive_complexity)]
    pub async fn run(mut self) {
        // get the current segments and create corresponding event segment writers
        self.selector.initialize().await;

        loop {
            let event = self.receiver.recv().await.expect("sender closed, processor exit");
            match event {
                Incoming::AppendEvent(event) => {
                    let segment = self.selector.get_segment_for_event(&event.routing_key);
                    let event_segment_writer =
                        self.selector.writers.get_mut(&segment).expect("must have writer");

                    if let Some(event) =
                        PendingEvent::with_header(event.routing_key, event.inner, event.oneshot_sender)
                    {
                        if let Err(e) = event_segment_writer.write(event).await {
                            warn!("failed to write append to segment due to {:?}, reconnecting", e);
                            event_segment_writer.reconnect(&self.factory).await;
                        }
                    }
                }
                Incoming::ServerReply(server_reply) => {
                    // it should always have writer because writer will
                    // not be removed until it receives SegmentSealed reply
                    let writer = self
                        .selector
                        .writers
                        .get_mut(&server_reply.segment)
                        .expect("should always be able to get event segment writer");

                    match server_reply.reply {
                        Replies::DataAppended(cmd) => {
                            debug!(
                                "data appended for writer {:?}, latest event id is: {:?}",
                                writer.id, cmd.event_number
                            );
                            writer.ack(cmd.event_number);
                            match writer.flush().await {
                                Ok(()) => {
                                    continue;
                                }
                                Err(e) => {
                                    warn!("writer {:?} failed to flush data to segment {:?} due to {:?}, reconnecting", writer.id ,writer.segment, e);
                                    writer.reconnect(&self.factory).await;
                                }
                            }
                        }

                        Replies::SegmentIsSealed(cmd) => {
                            debug!("segment {:?} sealed", cmd.segment);
                            let segment = ScopedSegment::from(cmd.segment);
                            let inflight = self
                                .selector
                                .refresh_segment_event_writers_upon_sealed(&segment)
                                .await;
                            self.selector.resend(inflight).await;
                            self.selector.remove_segment_event_writer(&segment);
                        }

                        // same handling logic as segment sealed reply
                        Replies::NoSuchSegment(cmd) => {
                            debug!(
                                "no such segment {:?} due to segment truncation: stack trace {}",
                                cmd.segment, cmd.server_stack_trace
                            );
                            let segment = ScopedSegment::from(cmd.segment);
                            let inflight = self
                                .selector
                                .refresh_segment_event_writers_upon_sealed(&segment)
                                .await;
                            self.selector.resend(inflight).await;
                            self.selector.remove_segment_event_writer(&segment);
                        }

                        _ => {
                            error!(
                                "receive unexpected reply {:?}, closing event stream writer",
                                server_reply.reply
                            );
                            self.receiver.close();
                            panic!("{:?}", server_reply.reply);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
enum Incoming {
    AppendEvent(AppendEvent),
    ServerReply(ServerReply),
}

#[derive(Debug)]
struct AppendEvent {
    inner: Vec<u8>,
    routing_key: Option<String>,
    oneshot_sender: oneshot::Sender<Result<(), EventStreamWriterError>>,
}

#[derive(Debug)]
struct ServerReply {
    segment: ScopedSegment,
    reply: Replies,
}

struct EventSegmentWriter {
    /// unique id for each EventSegmentWriter
    id: Uuid,

    /// the segment that this writer is writing to, it does not change for a EventSegmentWriter instance
    segment: ScopedSegment,

    /// the endpoint for segment, it might change if the segment is moved to another host
    endpoint: SocketAddr,

    /// client connection that writes to the segmentstore
    writer: Option<WritingClientConnection>,

    /// events that are sent but yet acknowledged
    inflight: VecDeque<Append>,

    /// events that are waiting to be sent
    pending: VecDeque<Append>,

    /// incremental event id
    event_num: i64,

    /// the random generator for request id
    rng: SmallRng,

    /// the sender that sends back reply to processor
    sender: Sender<Incoming>,

    /// The client retry policy
    retry_policy: RetryWithBackoff,
}

impl EventSegmentWriter {
    /// maximum data size in one append block
    const MAX_WRITE_SIZE: i32 = 8 * 1024 * 1024 + 8;
    const MAX_EVENTS: i32 = 500;

    fn new(segment: ScopedSegment, sender: Sender<Incoming>, retry_policy: RetryWithBackoff) -> Self {
        EventSegmentWriter {
            endpoint: "127.0.0.1:0".parse::<SocketAddr>().expect("get socketaddr"), //Dummy address
            id: Uuid::new_v4(),
            writer: None,
            segment,
            inflight: VecDeque::new(),
            pending: VecDeque::new(),
            event_num: 0,
            rng: SmallRng::from_entropy(),
            sender,
            retry_policy,
        }
    }

    pub fn get_segment_name(&self) -> String {
        self.segment.to_string()
    }

    pub async fn setup_connection(
        &mut self,
        factory: &ClientFactoryInternal,
    ) -> Result<(), EventStreamWriterError> {
        // retry to get latest endpoint
        let uri = match retry_async(self.retry_policy, || async {
            match factory
                .get_controller_client()
                .get_endpoint_for_segment(&self.segment)
                .await
            {
                Ok(uri) => RetryResult::Success(uri),
                Err(e) => {
                    warn!("retry controller due to error {:?}", e);
                    RetryResult::Retry(e)
                }
            }
        })
        .await
        {
            Ok(uri) => uri,
            Err(e) => return Err(EventStreamWriterError::RetryControllerWriting { err: e }),
        };

        self.endpoint = uri.0.parse::<SocketAddr>().expect("should parse to socketaddr");

        let request = Requests::SetupAppend(SetupAppendCommand {
            request_id: self.rng.gen::<i64>(),
            writer_id: self.id.as_u128(),
            segment: self.segment.to_string(),
            delegation_token: "".to_string(),
        });

        let raw_client = factory.create_raw_client(self.endpoint);
        let mut connection = match retry_async(self.retry_policy, || async {
            debug!(
                "setting up append for writer:{:?}/segment:{}",
                self.id,
                self.segment.to_string()
            );
            match raw_client.send_setup_request(&request).await {
                Ok((reply, connection)) => RetryResult::Success((reply, connection)),
                Err(e) => {
                    warn!("failed to setup append using rawclient due to {:?}", e);
                    RetryResult::Retry(e)
                }
            }
        })
        .await
        {
            Ok((reply, connection)) => match reply {
                Replies::AppendSetup(cmd) => {
                    debug!(
                        "append setup completed for writer:{:?}/segment:{:?}",
                        cmd.writer_id, cmd.segment
                    );
                    self.ack(cmd.last_event_number);
                    connection
                }
                _ => {
                    warn!(
                        "append setup failed for writer:{:?}/segment:{:?} due to {:?}",
                        self.id, self.segment, reply
                    );
                    return Err(EventStreamWriterError::WrongReply {
                        expected: String::from("AppendSetup"),
                        actual: reply,
                    });
                }
            },
            Err(e) => return Err(EventStreamWriterError::RetryRawClient { err: e }),
        };

        let (mut r, w) = connection.split();
        self.writer = Some(w);

        let segment = self.segment.clone();
        let mut sender = self.sender.clone();

        // spin up connection listener that keeps listening on the connection
        tokio::spawn(async move {
            loop {
                // listen to the receiver channel
                match r.read().await {
                    Ok(reply) => {
                        if let Err(e) = sender
                            .send(Incoming::ServerReply(ServerReply {
                                segment: segment.clone(),
                                reply,
                            }))
                            .await
                        {
                            error!("connection {:?} read data from segmentstore but failed to send reply back to processor due to {:?}", r.get_id() ,e);
                            return;
                        }
                    }
                    Err(e) => {
                        warn!("connection {:?} failed to read data back from segmentstore due to {:?}, closing the listener task", r.get_id(), e);
                        return;
                    }
                };
            }
        });
        Ok(())
    }

    /// first add the event to the pending list
    /// then flush the pending list if the inflight list is empty
    pub async fn write(&mut self, event: PendingEvent) -> Result<(), EventStreamWriterError> {
        self.add_pending(event);
        self.flush().await
    }

    /// add the event to the pending list
    pub fn add_pending(&mut self, event: PendingEvent) {
        self.event_num += 1;
        self.pending.push_back(Append {
            event_id: self.event_num,
            event,
        });
    }

    /// flush the pending events. It will grab at most MAX_WRITE_SIZE of data
    /// from the pending list and send them to the server. Those events will be moved to inflight list waiting to be acked.
    pub async fn flush(&mut self) -> Result<(), EventStreamWriterError> {
        if !self.inflight.is_empty() || self.pending.is_empty() {
            return Ok(());
        }

        let mut total_size = 0;
        let mut to_send = vec![];
        let mut event_count = 0;

        while let Some(append) = self.pending.pop_front() {
            assert!(
                append.event.data.len() <= EventSegmentWriter::MAX_WRITE_SIZE as usize,
                "event size {} must be under {}",
                append.event.data.len(),
                EventSegmentWriter::MAX_WRITE_SIZE
            );
            if append.event.data.len() + to_send.len() <= EventSegmentWriter::MAX_WRITE_SIZE as usize
                || event_count == EventSegmentWriter::MAX_EVENTS as usize
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
            self.writer.as_ref().expect("must have writer").get_id(),

        );

        let request = Requests::AppendBlockEnd(AppendBlockEndCommand {
            writer_id: self.id.as_u128(),
            size_of_whole_events: total_size as i32,
            data: to_send,
            num_event: self.inflight.len() as i32,
            last_event_number: self.inflight.back().expect("last event").event_id,
            request_id: self.rng.gen::<i64>(),
        });

        let writer = self.writer.as_mut().expect("must have writer");
        writer.write(&request).await.context(SegmentWriting {})
    }

    /// ack inflight events. It will send the reply from server back to the caller using oneshot.
    pub fn ack(&mut self, event_id: i64) {
        // no events need to ack
        if self.inflight.is_empty() {
            return;
        }

        // event id is -9223372036854775808 if no event acked for appendsetup
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
            if let Err(e) = acked.event.oneshot_sender.send(Result::Ok(())) {
                error!(
                    "failed to send ack back to caller using oneshot due to {:?}: event id {:?}",
                    e, acked.event_id
                );
            }

            // ack up to event id
            if acked.event_id == event_id {
                break;
            }
        }
    }

    /// get the unacked events. Notice that it will pass the ownership
    /// of the unacked events to the caller, which means this method can only be called once.
    pub fn get_unacked_events(&mut self) -> Vec<PendingEvent> {
        let mut ret = vec![];
        while let Some(append) = self.inflight.pop_front() {
            ret.push(append.event);
        }
        while let Some(append) = self.pending.pop_front() {
            ret.push(append.event);
        }
        ret
    }

    pub async fn reconnect(&mut self, factory: &ClientFactoryInternal) {
        loop {
            debug!("Reconnecting event segment writer {:?}", self.id);
            // setup the connection
            let setup_res = self.setup_connection(factory).await;
            if setup_res.is_err() {
                continue;
            }

            while self.inflight.back().is_some() {
                self.pending
                    .push_front(self.inflight.pop_back().expect("must have event"));
            }

            // flush any pending events
            let flush_res = self.flush().await;
            if flush_res.is_err() {
                continue;
            }

            return;
        }
    }
}

pub struct SegmentSelector {
    /// Stream that this SegmentSelector is on
    stream: ScopedStream,

    /// mapping each segment in this stream to it's EventSegmentWriter
    writers: HashMap<ScopedSegment, EventSegmentWriter>,

    /// the current segments in this stream
    current_segments: StreamSegments,

    /// the sender that sends reply back to Processor
    sender: Sender<Incoming>,

    /// client config that contains the retry policy
    config: ClientConfig,

    //Used to gain access to the controller and connection pool
    factory: Arc<ClientFactoryInternal>,
}

impl SegmentSelector {
    fn new(
        stream: ScopedStream,
        sender: Sender<Incoming>,
        config: ClientConfig,
        factory: Arc<ClientFactoryInternal>,
    ) -> Self {
        SegmentSelector {
            stream,
            writers: HashMap::new(),
            current_segments: StreamSegments::new(BTreeMap::new()),
            sender,
            config,
            factory,
        }
    }

    async fn initialize(&mut self) {
        self.current_segments = retry_async(self.config.retry_policy, || async {
            match self
                .factory
                .get_controller_client()
                .get_current_segments(&self.stream)
                .await
            {
                Ok(ss) => RetryResult::Success(ss),
                Err(_e) => RetryResult::Retry("retry controller command due to error"),
            }
        })
        .await
        .expect("retry failed");

        self.create_missing_writers().await;
    }

    /// get the Segment by passing a routing key
    fn get_segment_for_event(&self, routing_key: &Option<String>) -> ScopedSegment {
        let mut small_rng = SmallRng::from_entropy();
        if let Some(key) = routing_key {
            self.current_segments.get_segment(hash_string_to_f64(key))
        } else {
            self.current_segments.get_segment(small_rng.gen::<f64>())
        }
    }

    /// refresh segment event writer when a segment is sealed
    /// return the inflight events of that sealed segment
    async fn refresh_segment_event_writers_upon_sealed(
        &mut self,
        sealed_segment: &ScopedSegment,
    ) -> Vec<PendingEvent> {
        let stream_segments_with_predecessors = retry_async(self.config.retry_policy, || async {
            match self
                .factory
                .get_controller_client()
                .get_successors(sealed_segment)
                .await
            {
                Ok(ss) => {
                    if !ss.replacement_segments.contains_key(&sealed_segment.segment) {
                        RetryResult::Retry("retry get successors due to empty successors")
                    } else {
                        RetryResult::Success(ss)
                    }
                }
                Err(_e) => RetryResult::Retry("retry controller command due to error"),
            }
        })
        .await
        .expect("retry failed");
        self.update_segments_upon_sealed(stream_segments_with_predecessors, sealed_segment)
            .await
    }

    /// create event segment writer for the successor segment of the sealed segment and return the inflight event
    async fn update_segments_upon_sealed(
        &mut self,
        successors: StreamSegmentsWithPredecessors,
        sealed_segment: &ScopedSegment,
    ) -> Vec<PendingEvent> {
        self.current_segments = self
            .current_segments
            .apply_replacement_range(&sealed_segment.segment, &successors)
            .expect("apply replacement range");
        self.create_missing_writers().await;
        self.writers
            .get_mut(sealed_segment)
            .expect("get writer")
            .get_unacked_events()
    }

    /// create missing EventSegmentWriter and set up the connections for ready to use
    #[allow(clippy::map_entry)] // clippy warns about using entry, but async closure is not stable
    async fn create_missing_writers(&mut self) {
        for scoped_segment in self.current_segments.get_segments() {
            if !self.writers.contains_key(&scoped_segment) {
                let mut writer = EventSegmentWriter::new(
                    scoped_segment.clone(),
                    self.sender.clone(),
                    self.config.retry_policy,
                );

                debug!(
                    "writer {:?} created for segment {:?}",
                    writer.id,
                    scoped_segment.to_string()
                );
                match writer.setup_connection(&self.factory).await {
                    Ok(()) => {}
                    Err(_) => {
                        writer.reconnect(&self.factory).await;
                    }
                }
                self.writers.insert(scoped_segment, writer);
            }
        }
    }

    /// resend events
    async fn resend(&mut self, to_resend: Vec<PendingEvent>) {
        for event in to_resend {
            let segment = self.get_segment_for_event(&event.routing_key);
            let segment_writer = self.writers.get_mut(&segment).expect("must have writer");
            if let Err(e) = segment_writer.write(event).await {
                warn!(
                    "failed to resend an event due to: {:?}, reconnecting the event segment writer",
                    e
                );
                segment_writer.reconnect(&self.factory).await;
            }
        }
    }

    fn remove_segment_event_writer(&mut self, segment: &ScopedSegment) {
        self.writers.remove(segment);
    }
}

struct Append {
    event_id: i64,
    event: PendingEvent,
}

struct PendingEvent {
    routing_key: Option<String>,
    data: Vec<u8>,
    oneshot_sender: oneshot::Sender<Result<(), EventStreamWriterError>>,
}

impl PendingEvent {
    const MAX_WRITE_SIZE: i32 = 8 * 1024 * 1024 + 8;

    fn new(
        routing_key: Option<String>,
        data: Vec<u8>,
        oneshot_sender: oneshot::Sender<Result<(), EventStreamWriterError>>,
    ) -> Option<Self> {
        if data.len() as i32 > PendingEvent::MAX_WRITE_SIZE {
            warn!(
                "event size {:?} exceeds limit {:?}",
                data.len(),
                PendingEvent::MAX_WRITE_SIZE
            );
            oneshot_sender
                .send(Err(EventStreamWriterError::EventSizeTooLarge {
                    limit: PendingEvent::MAX_WRITE_SIZE,
                    size: data.len() as i32,
                }))
                .expect("send error to caller");
            None
        } else {
            Some(PendingEvent {
                routing_key,
                data,
                oneshot_sender,
            })
        }
    }

    fn with_header(
        routing_key: Option<String>,
        data: Vec<u8>,
        oneshot_sender: oneshot::Sender<Result<(), EventStreamWriterError>>,
    ) -> Option<PendingEvent> {
        let cmd = EventCommand { data };
        match cmd.write_fields() {
            Ok(data) => PendingEvent::new(routing_key, data, oneshot_sender),
            Err(e) => {
                warn!("failed to serialize event to event command, sending this error back to caller");
                oneshot_sender
                    .send(Err(EventStreamWriterError::ParseToEventCommand { source: e }))
                    .expect("send error to caller");
                None
            }
        }
    }

    fn without_header(
        routing_key: Option<String>,
        data: Vec<u8>,
        oneshot_sender: oneshot::Sender<Result<(), EventStreamWriterError>>,
    ) -> Option<PendingEvent> {
        PendingEvent::new(routing_key, data, oneshot_sender)
    }
}

// hash string to 0.0 - 1.0 in f64
fn hash_string_to_f64(s: &str) -> f64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(s.as_bytes());
    let hash_u64 = hasher.finish();
    let shifted = (hash_u64 >> 12) & 0x000f_ffff_ffff_ffff_u64;
    f64::from_bits(0x3ff0_0000_0000_0000_u64 + shifted) - 1.0
}

#[cfg(test)]
mod tests {
    use tokio::runtime::Runtime;

    use super::*;

    #[test]
    fn test_hash_string() {
        let hashed = hash_string_to_f64("hello");
        assert!(hashed >= 0.0);
        assert!(hashed <= 1.0);
    }

    #[test]
    fn test_pending_event() {
        // test with legal event size
        let (tx, _rx) = oneshot::channel();
        let data = vec![];
        let routing_key = None;

        let event = PendingEvent::without_header(routing_key, data, tx).expect("create pending event");
        assert_eq!(event.data.len(), 0);

        // test with illegal event size
        let (tx, rx) = oneshot::channel();
        let data = vec![0; (PendingEvent::MAX_WRITE_SIZE + 1) as usize];
        let routing_key = None;

        let event = PendingEvent::without_header(routing_key, data, tx);
        assert!(event.is_none());

        let mut rt = Runtime::new().expect("get runtime");
        let reply = rt.block_on(rx).expect("get reply");
        assert!(reply.is_err());
    }
}
