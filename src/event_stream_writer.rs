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
use pravega_controller_client::ControllerClient;
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_connection::*;
use pravega_wire_protocol::commands::{AppendBlockEndCommand, Command, EventCommand, SetupAppendCommand};
use pravega_wire_protocol::connection_pool::*;
use pravega_wire_protocol::error::*;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use tokio;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
extern crate rand;
use self::rand::thread_rng;
use rand::Rng;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use uuid::Uuid;

/// EventStreamWriter will handle the write to a given Stream.
#[async_trait]
pub trait EventStreamWriter<T: std::convert::Into<Vec<u8>> + Send + Sized + Send + Sized + 'static> {
    /// Write event with out routing key, this event will be sent to a random segment of that stream.
    /// It returns a oneshot Receiver immediately, caller will listen on this Receiver and see if it has been successfully written.
    async fn write_event(
        &mut self,
        event: T,
    ) -> Result<oneshot::Receiver<Result<(), EventStreamWriterError>>, EventStreamWriterError>;

    /// Write event with a routing key.
    async fn write_event_by_routing_key(
        &mut self,
        routing_key: String,
        event: T,
    ) -> Result<oneshot::Receiver<Result<(), EventStreamWriterError>>, EventStreamWriterError>;
}

/// the impl of EventStreamWriter. It contains a writer id and a mpsc sender which is used to send Event
/// to the Processor
pub struct EventStreamWriterImpl<T: std::convert::Into<Vec<u8>> + Send + Sized + Send + Sized + 'static> {
    writer_id: Uuid,
    tx: Sender<Event<T>>,
}

impl<T: std::convert::Into<Vec<u8>> + Send + Sized + 'static> EventStreamWriterImpl<T> {
    const CHANNEL_CAPACITY: usize = 100;

    pub async fn new(
        stream: ScopedStream,
        controller: Box<dyn ControllerClient>,
        connection_pool: Box<dyn ConnectionPool>,
    ) -> (Self, Processor<T>) {
        let (tx, rx) = channel(EventStreamWriterImpl::<T>::CHANNEL_CAPACITY);
        let selector = SegmentSelector::new(controller, stream).await;
        let processor = Processor {
            tx: tx.clone(),
            rx,
            selector,
            connection_pool,
        };
        (
            EventStreamWriterImpl {
                writer_id: Uuid::new_v4(),
                tx,
            },
            processor,
        )
    }
}

#[async_trait]
impl<T: std::convert::Into<Vec<u8>> + Send + Sized + 'static> EventStreamWriter<T>
    for EventStreamWriterImpl<T>
{
    async fn write_event(
        &mut self,
        event: T,
    ) -> Result<oneshot::Receiver<Result<(), EventStreamWriterError>>, EventStreamWriterError> {
        let (tx, rx) = oneshot::channel();
        let append_event = Event::AppendEvent(AppendEvent {
            inner: event,
            routing_key: Option::None,
            tx_oneshot: tx,
        });
        if let Err(_e) = self.tx.send(append_event).await {
            Err(EventStreamWriterError::SendToProcessor {})
        } else {
            Ok(rx)
        }
    }

    async fn write_event_by_routing_key(
        &mut self,
        routing_key: String,
        event: T,
    ) -> Result<oneshot::Receiver<Result<(), EventStreamWriterError>>, EventStreamWriterError> {
        let (tx, rx) = oneshot::channel();
        let append_event = Event::AppendEvent(AppendEvent {
            inner: event,
            routing_key: Option::Some(routing_key),
            tx_oneshot: tx,
        });
        self.tx
            .send(append_event)
            .await
            .map_or_else(|_| Err(EventStreamWriterError::SendToProcessor {}), |_| Ok(rx))
    }
}

pub struct Processor<T: std::convert::Into<Vec<u8>> + Send + Sized + 'static> {
    tx: Sender<Event<T>>,
    rx: Receiver<Event<T>>,
    selector: SegmentSelector,
    connection_pool: Box<dyn ConnectionPool>,
}

impl<T: std::convert::Into<Vec<u8>> + Send + Sized + 'static> Processor<T> {
    pub async fn run(mut processor: Processor<T>) {
        loop {
            let event = processor.rx.recv().await.expect("sender closed, processor exit");
            match event {
                Event::AppendEvent(event) => {
                    let mut option = processor
                        .selector
                        .get_segment_writer_for_key(event.routing_key.clone());
                    while option.is_none() {
                        processor.selector.refresh_segment_event_writers().await;
                        option = processor
                            .selector
                            .get_segment_writer_for_key(event.routing_key.clone());
                    }

                    let event_segment_writer = option.expect("get writer");
                    if !event_segment_writer.connection_setup {
                        let connection = processor
                            .connection_pool
                            .get_connection(event_segment_writer.endpoint)
                            .await
                            .expect("get connection");
                        let mut client_connection = ClientConnectionImpl { connection };
                        let (mut r, w) = client_connection.split();

                        let mut tx_clone = processor.tx.clone();
                        event_segment_writer.writer = Some(w);
                        let segment = event_segment_writer.segment.clone();

                        // spin up connection listener that keeps listening on the connection
                        tokio::spawn(async move {
                            loop {
                                // listen to the receiver channel
                                let reply = r.read().await.expect("sender closed, listener exit");
                                tx_clone
                                    .send(Event::ServerReply(ServerReply {
                                        segment: segment.clone(),
                                        reply,
                                    }))
                                    .await;
                            }
                        });
                        event_segment_writer.setup_append().await;
                    }

                    if let Some(event) =
                        PendingEvent::with_header(event.routing_key, event.inner.into(), event.tx_oneshot)
                    {
                        event_segment_writer.add_pending(event)
                    }
                }
                Event::ServerReply(server_reply) => {
                    let writer = processor
                        .selector
                        .writers
                        .get_mut(&server_reply.segment)
                        .expect("should always have writer");
                    match server_reply.reply {
                        Replies::AppendSetup(_cmd) => {
                            writer.connection_setup = true;
                            writer.flush().await;
                        }
                        Replies::DataAppended(cmd) => {
                            writer.ack(cmd.event_number).await;
                        }
                        Replies::SegmentIsSealed(cmd) => {
                            let segment = ScopedSegment::from(cmd.segment);
                            let inflight = processor
                                .selector
                                .refresh_segment_event_writers_upon_sealed(&segment)
                                .await;
                            processor.selector.resend(inflight).await;
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

enum Event<T: Send + Sized + 'static> {
    AppendEvent(AppendEvent<T>),
    ServerReply(ServerReply),
}

struct AppendEvent<T: Send + Sized + 'static> {
    inner: T,
    routing_key: Option<String>,
    tx_oneshot: oneshot::Sender<Result<(), EventStreamWriterError>>,
}

struct ServerReply {
    segment: ScopedSegment,
    reply: Replies,
}

struct EventSegmentWriter {
    /// the endpoint for segment
    endpoint: SocketAddr,

    /// unique id for each EventSegmentWriter
    writer_id: Uuid,

    /// writer that writes to the segmentstore
    writer: Option<Box<dyn WritingClientConnection>>,

    /// the segment that this writer is writing to
    segment: ScopedSegment,

    /// indicates that the client connection has been setup and ready to use
    connection_setup: bool,

    /// events that are sent but yet acknowledged
    inflight: VecDeque<Append>,

    /// events that are waiting to be sent
    pending: VecDeque<Append>,

    /// Incremental event id
    event_num: i64,
}

impl EventSegmentWriter {
    /// maximum data size in one append block
    const MAX_DATA_SIZE: i32 = 1024 * 1024;

    fn new(endpoint: SocketAddr, segment: ScopedSegment) -> Self {
        EventSegmentWriter {
            endpoint,
            writer_id: Uuid::new_v4(),
            writer: None,
            segment,
            connection_setup: false,
            inflight: VecDeque::new(),
            pending: VecDeque::new(),
            event_num: 0,
        }
    }

    pub fn get_segment_name(&self) -> String {
        self.segment.to_string()
    }

    /// send setup_append command to the server.
    async fn setup_append(&mut self) {
        assert!(self.writer.is_some(), "should have client connection");

        // TODO: implement token related feature
        let cmd = Requests::SetupAppend(SetupAppendCommand {
            request_id: 1,
            writer_id: self.writer_id.as_u128(),
            segment: self.segment.to_string(),
            delegation_token: "".to_string(),
        });

        //TODO: add retry
        self.writer.as_mut().unwrap().write(&cmd).await.expect("TODO");
    }

    /// first add the event to the pending list
    /// then flush the pending list is the inflight list is empty
    pub async fn write(&mut self, event: PendingEvent) {
        self.add_pending(event);
        if self.inflight.is_empty() {
            self.flush().await;
        }
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
    pub async fn flush(&mut self) {
        if self.pending.is_empty() {
            return;
        }

        let mut total_size = 0;
        let mut to_send = vec![];

        while let Some(append) = self.pending.pop_front() {
            // TODO what if a single event size is larger than MAX_DATA_SIZE
            if append.event.data.len() + total_size <= EventSegmentWriter::MAX_DATA_SIZE as usize {
                total_size += append.event.data.len();
                to_send.extend(append.event.data.clone());
                self.inflight.push_back(append);
            } else {
                self.pending.push_front(append);
                break;
            }
        }

        let request = Requests::AppendBlockEnd(AppendBlockEndCommand {
            writer_id: self.writer_id.as_u128(),
            size_of_whole_events: total_size as i32,
            data: to_send,
            num_event: self.inflight.len() as i32,
            last_event_number: self.inflight.back().expect("last event").event_id,
            request_id: 2,
        });

        //TODO: add retry
        self.writer
            .as_mut()
            .expect("get writer")
            .write(&request)
            .await
            .unwrap();
    }

    /// ack inflight events. It will send the reply from server back to the caller using oneshot.
    pub async fn ack(&mut self, event_id: i64) {
        loop {
            let acked = self
                .inflight
                .pop_front()
                .expect("given event id doesn't exist in inflight list");
            assert!(
                event_id >= acked.event_id,
                "given event id is illegal or has been acked"
            );

            acked.event.tx.send(Result::Ok(()));
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
        ret
    }
}

pub struct SegmentSelector {
    /// Stream that this SegmentSelector is on
    stream: ScopedStream,

    /// mapping each segment in this stream to it's EventSegmentWriter
    writers: HashMap<ScopedSegment, EventSegmentWriter>,

    /// the current segments in this stream
    current_segments: StreamSegments,

    /// the controller instance that is used to get updated segment information from controller
    controller: Box<dyn ControllerClient>,
}

impl SegmentSelector {
    async fn new(mut controller: Box<dyn ControllerClient>, stream: ScopedStream) -> Self {
        let writers = HashMap::new();
        let current_segments = controller.get_current_segments(&stream).await.expect("TODO");
        SegmentSelector {
            stream,
            writers,
            current_segments,
            controller,
        }
    }

    /// get the segment writer by passing a routing key if there is one
    fn get_segment_writer_for_key(&mut self, routing_key: Option<String>) -> Option<&mut EventSegmentWriter> {
        self.writers.get_mut(&self.get_segment_for_event(routing_key))
    }

    /// get the Segment by passing a routing key
    fn get_segment_for_event(&self, routing_key: Option<String>) -> ScopedSegment {
        let mut rng = thread_rng();
        if routing_key.is_none() {
            self.current_segments.get_segment(rng.gen::<f64>())
        } else {
            self.current_segments
                .get_segment(hash_string_to_f64(routing_key.expect("routing key")))
        }
    }

    /// refresh the mapping from Segment to EventSegmentWriter. It will create writers for newly added segments
    /// and delete outdated segment writer pairs.
    async fn refresh_segment_event_writers(&mut self) -> Vec<PendingEvent> {
        self.current_segments = self
            .controller
            .get_current_segments(&self.stream)
            .await
            .expect("get current segments");
        self.create_missing_writers().await;

        let segments = self.current_segments.get_segments();

        let mut to_resend = vec![];
        let mut to_remove = vec![];

        for (key, value) in &mut self.writers {
            if !segments.contains(key) {
                let mut unacked = value.get_unacked_events();
                to_resend.append(&mut unacked);
                to_remove.push(key.to_owned());
            }
        }

        for k in to_remove {
            self.writers.remove(&k);
        }

        to_resend
    }

    /// refresh segment event writer when a segment is sealed
    /// return the inflight events of that sealed segment
    async fn refresh_segment_event_writers_upon_sealed(
        &mut self,
        sealed_segment: &ScopedSegment,
    ) -> Vec<PendingEvent> {
        let result = self.controller.get_successors(&sealed_segment).await;
        match result {
            Ok(successors) => self.update_segments_upon_sealed(successors, sealed_segment).await,
            Err(_e) => {
                //TODO error handling
                vec![]
            }
        }
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
            .get_mut(&sealed_segment)
            .expect("get writer")
            .get_unacked_events()
    }

    /// create missing EventSegmentWriter and set up the connections for ready to use
    async fn create_missing_writers(&mut self) {
        for scoped_segment in self.current_segments.get_segments() {
            if !self.writers.contains_key(&scoped_segment) {
                // TODO: how to handle failure, do we retry?
                let uri = self
                    .controller
                    .get_endpoint_for_segment(&scoped_segment)
                    .await
                    .expect("TODO");
                let writer = EventSegmentWriter::new(uri.0.parse().expect("TODO"), scoped_segment.clone());
                self.writers.insert(scoped_segment, writer);
            }
        }
    }

    /// resend events
    async fn resend(&mut self, mut to_resend: Vec<PendingEvent>) {
        while !to_resend.is_empty() {
            let mut unsent = vec![];
            let mut send_failed = false;
            for event in to_resend {
                if send_failed {
                    unsent.push(event);
                } else {
                    let segment_writer = self.get_segment_writer_for_key(event.routing_key.clone());
                    if segment_writer.is_none() {
                        unsent.extend(self.refresh_segment_event_writers().await);
                        send_failed = true;
                    } else {
                        segment_writer.unwrap().write(event);
                    }
                }
            }
            to_resend = unsent;
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
    tx: oneshot::Sender<Result<(), EventStreamWriterError>>,
}

impl PendingEvent {
    const MAX_WRITE_SIZE: i32 = 8 * 1024 * 1024 + 8;

    fn new(
        routing_key: Option<String>,
        data: Vec<u8>,
        tx: oneshot::Sender<Result<(), EventStreamWriterError>>,
    ) -> Option<Self> {
        if data.len() as i32 > PendingEvent::MAX_WRITE_SIZE {
            tx.send(Err(EventStreamWriterError::EventSizeTooLarge {
                limit: PendingEvent::MAX_WRITE_SIZE,
                size: data.len() as i32,
            }));
            None
        } else {
            Some(PendingEvent {
                routing_key,
                data,
                tx,
            })
        }
    }

    fn with_header(
        routing_key: Option<String>,
        data: Vec<u8>,
        tx: oneshot::Sender<Result<(), EventStreamWriterError>>,
    ) -> Option<PendingEvent> {
        let cmd = EventCommand { data };
        match cmd.write_fields() {
            Ok(data) => PendingEvent::new(routing_key, data, tx),
            Err(e) => {
                tx.send(Err(EventStreamWriterError::ParseToEventCommand { source: e }));
                None
            }
        }
    }

    fn without_header(
        routing_key: Option<String>,
        data: Vec<u8>,
        tx: oneshot::Sender<Result<(), EventStreamWriterError>>,
    ) -> Option<PendingEvent> {
        PendingEvent::new(routing_key, data, tx)
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

    #[test]
    fn test() {}
}
