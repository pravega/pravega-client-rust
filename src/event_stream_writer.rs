//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::error::*;
use crate::setup_logger;
use lazy_static::*;
use pravega_controller_client::ControllerClient;
use pravega_rust_client_retry::retry_async::retry_async;
use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
use pravega_rust_client_retry::retry_result::RetryResult;
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_connection::*;
use pravega_wire_protocol::commands::{AppendBlockEndCommand, Command, EventCommand, SetupAppendCommand};
use pravega_wire_protocol::connection_pool::*;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hasher;
use std::net::SocketAddr;
use tokio;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{debug, warn};
use uuid::Uuid;

lazy_static! {
    static ref RETRY_POLICY: RetryWithBackoff = RetryWithBackoff::default();
}

/// EventStreamWriter contains a writer id and a mpsc sender which is used to send Event
/// to the Processor
pub struct EventStreamWriter {
    writer_id: Uuid,
    sender: Sender<Incoming>,
}

impl EventStreamWriter {
    const CHANNEL_CAPACITY: usize = 100;

    pub async fn new(stream: ScopedStream) -> (Self, Processor) {
        setup_logger();
        let (tx, rx) = channel(EventStreamWriter::CHANNEL_CAPACITY);
        let selector = SegmentSelector::new(stream, tx.clone()).await;
        let processor = Processor {
            receiver: rx,
            selector,
        };
        (
            EventStreamWriter {
                writer_id: Uuid::new_v4(),
                sender: tx,
            },
            processor,
        )
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
}

impl Processor {
    pub async fn run(
        mut processor: Processor,
        controller: Box<dyn ControllerClient>,
        connection_pool: ConnectionPool<SegmentConnectionManager>,
    ) {
        // get the current segments and create corresponding event segment writers
        processor.selector.initialize(&connection_pool, &controller).await;

        loop {
            let event = processor
                .receiver
                .recv()
                .await
                .expect("sender closed, processor exit");
            match event {
                Incoming::AppendEvent(event) => {
                    let event_segment_writer = processor
                        .selector
                        .get_segment_writer_for_key(event.routing_key.clone());

                    if let Some(event) =
                        PendingEvent::with_header(event.routing_key, event.inner, event.oneshot_sender)
                    {
                        if event_segment_writer.write(event).await.is_err() {
                            event_segment_writer
                                .reconnect(&connection_pool, &controller)
                                .await;
                        }
                    }
                }
                Incoming::ServerReply(server_reply) => {
                    // it should always have writer because writer will
                    // not be removed until it receives SegmentSealed reply
                    let writer = processor
                        .selector
                        .writers
                        .get_mut(&server_reply.segment)
                        .expect("should always be able to get event segment writer");

                    match server_reply.reply {
                        Replies::AppendSetup(cmd) => {
                            debug!(
                                "append setup for writer:{:?} and segment:{:?}",
                                cmd.writer_id, cmd.segment
                            );
                            writer.ack(cmd.last_event_number);
                            match writer.flush().await {
                                Ok(()) => {
                                    continue;
                                }
                                Err(_) => {
                                    writer.reconnect(&connection_pool, &controller).await;
                                }
                            }
                        }

                        Replies::DataAppended(cmd) => {
                            debug!("date appended: {:?}", cmd.event_number);
                            writer.ack(cmd.event_number);
                            match writer.flush().await {
                                Ok(()) => {
                                    continue;
                                }
                                Err(_) => {
                                    writer.reconnect(&connection_pool, &controller).await;
                                }
                            }
                        }

                        Replies::SegmentIsSealed(cmd) => {
                            debug!("segment sealed: {:?}", cmd.segment);
                            let segment = ScopedSegment::from(cmd.segment);
                            let inflight = processor
                                .selector
                                .refresh_segment_event_writers_upon_sealed(
                                    &segment,
                                    &connection_pool,
                                    &controller,
                                )
                                .await;
                            processor
                                .selector
                                .resend(inflight, &connection_pool, &controller)
                                .await;
                            processor.selector.remove_segment_event_writer(&segment);
                        }
                        _ => {
                            // TODO, add other replies
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
}

impl EventSegmentWriter {
    /// maximum data size in one append block
    const MAX_DATA_SIZE: i32 = 1024 * 1024;
    const MAX_EVENTS: i32 = 500;

    fn new(segment: ScopedSegment, sender: Sender<Incoming>) -> Self {
        EventSegmentWriter {
            endpoint: "127.0.0.1:9090".parse::<SocketAddr>().expect("get socketaddr"),
            id: Uuid::new_v4(),
            writer: None,
            segment,
            inflight: VecDeque::new(),
            pending: VecDeque::new(),
            event_num: 0,
            rng: SmallRng::from_entropy(),
            sender,
        }
    }

    pub fn get_segment_name(&self) -> String {
        self.segment.to_string()
    }

    pub async fn setup_connection(
        &mut self,
        pool: &ConnectionPool<SegmentConnectionManager>,
        controller: &Box<dyn ControllerClient>,
    ) -> Result<(), EventStreamWriterError> {
        // retry to get latest endpoint
        let uri = match retry_async(RETRY_POLICY.clone(), || async {
            match controller.get_endpoint_for_segment(&self.segment).await {
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

        // retry to get connection from pool
        let connection = match retry_async(RETRY_POLICY.clone(), || async {
            match pool.get_connection(self.endpoint).await {
                Ok(connection) => RetryResult::Success(connection),
                Err(e) => {
                    warn!("failed to get connection from pool");
                    RetryResult::Retry(e)
                }
            }
        })
        .await
        {
            Ok(connection) => connection,
            Err(e) => return Err(EventStreamWriterError::RetryConnectionPool { err: e }),
        };

        let mut client_connection = ClientConnectionImpl { connection };
        let (mut r, w) = client_connection.split();
        self.writer = Some(w);

        let segment = self.segment.clone();
        let mut sender = self.sender.clone();

        // spin up connection listener that keeps listening on the connection
        tokio::spawn(async move {
            loop {
                // listen to the receiver channel
                let reply = r.read().await.expect("sender closed, listener exit");
                sender
                    .send(Incoming::ServerReply(ServerReply {
                        segment: segment.clone(),
                        reply,
                    }))
                    .await
                    .expect("send reply to processor");
            }
        });
        self.setup_append().await
    }

    /// send setup_append command to the server.
    async fn setup_append(&mut self) -> Result<(), EventStreamWriterError> {
        assert!(self.writer.is_some(), "should have event segment writer");

        // TODO: implement token related feature
        let request = Requests::SetupAppend(SetupAppendCommand {
            request_id: self.rng.gen::<i64>(),
            writer_id: self.id.as_u128(),
            segment: self.segment.to_string(),
            delegation_token: "".to_string(),
        });

        let writer = self.writer.take().expect("must have writer");
        match segment_write_with_retry(writer, &request).await {
            Ok(writer) => {
                self.writer = Some(writer);
                Ok(())
            }
            Err(e) => {
                warn!("event segment writer failed to send setup append");
                Err(e)
            }
        }
    }

    /// first add the event to the pending list
    /// then flush the pending list is the inflight list is empty
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

    /// flush the pending events. It will grab at most MAX_DATA_SIZE of data
    /// from the pending list and send them to the server. Those events will be moved to inflight list waiting to be acked.
    pub async fn flush(&mut self) -> Result<(), EventStreamWriterError> {
        if !self.inflight.is_empty() || self.pending.is_empty() {
            return Ok(());
        }

        let mut total_size = 0;
        let mut to_send = vec![];

        while let Some(append) = self.pending.pop_front() {
            // TODO what if a single event size is larger than MAX_DATA_SIZE
            if append.event.data.len() + total_size <= EventSegmentWriter::MAX_DATA_SIZE as usize
                || to_send.len() > EventSegmentWriter::MAX_EVENTS as usize
            {
                total_size += append.event.data.len();
                to_send.extend(append.event.data.clone());
                self.inflight.push_back(append);
            } else {
                self.pending.push_front(append);
                break;
            }
        }

        debug!(
            "flushing {} events of total size {} using connection with id: {:?}",
            to_send.len(),
            total_size,
            self.writer.as_ref().expect("must have writer").get_id()
        );
        let request = Requests::AppendBlockEnd(AppendBlockEndCommand {
            writer_id: self.id.as_u128(),
            size_of_whole_events: total_size as i32,
            data: to_send,
            num_event: self.inflight.len() as i32,
            last_event_number: self.inflight.back().expect("last event").event_id,
            request_id: self.rng.gen::<i64>(),
        });

        let writer = self.writer.take().expect("must take writer");
        match segment_write_with_retry(writer, &request).await {
            Ok(writer) => {
                self.writer = Some(writer);
                Ok(())
            }
            Err(e) => {
                warn!("event segment writer failed to flush events to segmentstore");
                Err(e)
            }
        }
    }

    /// ack inflight events. It will send the reply from server back to the caller using oneshot.
    pub fn ack(&mut self, event_id: i64) {
        // no events need to ack
        if self.inflight.len() == 0 {
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
            acked
                .event
                .oneshot_sender
                .send(Result::Ok(()))
                .expect("send ack to caller");

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

    pub async fn reconnect(
        &mut self,
        pool: &ConnectionPool<SegmentConnectionManager>,
        controller: &Box<dyn ControllerClient>,
    ) {
        loop {
            // setup the connection
            let setup_res = self.setup_connection(pool, controller).await;
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

async fn segment_write_with_retry(
    writer: WritingClientConnection,
    req: &Requests,
) -> Result<WritingClientConnection, EventStreamWriterError> {
    let writer_cell = RefCell::new(writer);

    retry_async(RETRY_POLICY.clone(), || async {
        let mut writer_mut = writer_cell.borrow_mut();
        match writer_mut.write(&req).await {
            Ok(()) => RetryResult::Success(()),
            Err(e) => {
                warn!("retry write to segment due to error {:?}", e);
                RetryResult::Retry(e)
            }
        }
    })
    .await
    .map_or_else(
        |e| Err(EventStreamWriterError::RetrySegmentWriting { err: e }),
        |_| Ok(writer_cell.into_inner()),
    )
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
}

impl SegmentSelector {
    async fn new(stream: ScopedStream, sender: Sender<Incoming>) -> Self {
        SegmentSelector {
            stream,
            writers: HashMap::new(),
            current_segments: StreamSegments::new(BTreeMap::new()),
            sender,
        }
    }

    async fn initialize(
        &mut self,
        connection_pool: &ConnectionPool<SegmentConnectionManager>,
        controller: &Box<dyn ControllerClient>,
    ) {
        self.current_segments = retry_async(RETRY_POLICY.clone(), || async {
            match controller.get_current_segments(&self.stream).await {
                Ok(ss) => RetryResult::Success(ss),
                Err(_e) => RetryResult::Retry("retry controller command due to error"),
            }
        })
        .await
        .expect("retry failed");

        self.create_missing_writers(&connection_pool, &controller).await;
    }

    /// get the segment writer by passing a routing key if there is one
    fn get_segment_writer_for_key(&mut self, routing_key: Option<String>) -> &mut EventSegmentWriter {
        let segment = &self.get_segment_for_event(routing_key);
        self.writers.get_mut(segment).expect("must have writer")
    }

    /// get the Segment by passing a routing key
    fn get_segment_for_event(&self, routing_key: Option<String>) -> ScopedSegment {
        let mut small_rng = SmallRng::from_entropy();
        if routing_key.is_none() {
            self.current_segments.get_segment(small_rng.gen::<f64>())
        } else {
            self.current_segments
                .get_segment(hash_string_to_f64(routing_key.expect("routing key")))
        }
    }

    /// refresh segment event writer when a segment is sealed
    /// return the inflight events of that sealed segment
    async fn refresh_segment_event_writers_upon_sealed(
        &mut self,
        sealed_segment: &ScopedSegment,
        connection_pool: &ConnectionPool<SegmentConnectionManager>,
        controller: &Box<dyn ControllerClient>,
    ) -> Vec<PendingEvent> {
        let stream_segments_with_predecessors = retry_async(RETRY_POLICY.clone(), || async {
            match controller.get_successors(&sealed_segment).await {
                Ok(ss) => RetryResult::Success(ss),
                Err(_e) => RetryResult::Retry("retry controller command due to error"),
            }
        })
        .await
        .expect("retry failed");
        self.update_segments_upon_sealed(
            stream_segments_with_predecessors,
            sealed_segment,
            connection_pool,
            controller,
        )
        .await
    }

    /// create event segment writer for the successor segment of the sealed segment and return the inflight event
    async fn update_segments_upon_sealed(
        &mut self,
        successors: StreamSegmentsWithPredecessors,
        sealed_segment: &ScopedSegment,
        connection_pool: &ConnectionPool<SegmentConnectionManager>,
        controller: &Box<dyn ControllerClient>,
    ) -> Vec<PendingEvent> {
        self.current_segments = self
            .current_segments
            .apply_replacement_range(&sealed_segment.segment, &successors)
            .expect("apply replacement range");
        self.create_missing_writers(connection_pool, controller).await;
        self.writers
            .get_mut(&sealed_segment)
            .expect("get writer")
            .get_unacked_events()
    }

    /// create missing EventSegmentWriter and set up the connections for ready to use
    async fn create_missing_writers(
        &mut self,
        connection_pool: &ConnectionPool<SegmentConnectionManager>,
        controller: &Box<dyn ControllerClient>,
    ) {
        for scoped_segment in self.current_segments.get_segments() {
            if !self.writers.contains_key(&scoped_segment) {
                let mut writer = EventSegmentWriter::new(scoped_segment.clone(), self.sender.clone());
                match writer.setup_connection(&connection_pool, &controller).await {
                    Ok(()) => {}
                    Err(_) => {
                        writer.reconnect(&connection_pool, &controller).await;
                    }
                }
                self.writers.insert(scoped_segment, writer);
            }
        }
    }

    /// resend events
    async fn resend(
        &mut self,
        to_resend: Vec<PendingEvent>,
        pool: &ConnectionPool<SegmentConnectionManager>,
        controller: &Box<dyn ControllerClient>,
    ) {
        for event in to_resend {
            let segment_writer = self.get_segment_writer_for_key(event.routing_key.clone());
            if let Err(e) = segment_writer.write(event).await {
                segment_writer.reconnect(pool, controller).await;
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
fn hash_string_to_f64(s: String) -> f64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(s.as_bytes());
    let hash_u64 = hasher.finish();
    let shifted = (hash_u64 >> 12) & 0x000f_ffff_ffff_ffff_u64;
    f64::from_bits(0x3ff0_0000_0000_0000_u64 + shifted) - 1.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_hash_string() {
        let hashed = hash_string_to_f64(String::from("hello"));
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
