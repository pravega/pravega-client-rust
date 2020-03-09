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
use pravega_wire_protocol::client_connection::*;
use pravega_wire_protocol::connection_pool::*;
use pravega_wire_protocol::error::*;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use pravega_wire_protocol::commands::{EventCommand, Command};
use snafu::ResultExt;
use std::fmt;
use std::net::SocketAddr;
use tracing::{span, Level};
use std::sync::mpsc::{channel, Receiver, Sender};
use tokio;
use pravega_controller_client::{create_connection, ControllerClient, ControllerClientImpl};
use pravega_rust_client_shared::*;
use std::collections::HashMap;
extern crate rand;
use rand::Rng;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

trait EventStreamWriter<T> {
    fn write_event(&self, event: T );

    fn write_event_by_routing_key(&self, routing_key: String, event: T);
}

pub struct EventStreamWriterImpl<T> {
    stream: Stream,
    writer_id: String,
    tx: Sender<Event<T>>,
}

impl<T> EventStreamWriterImpl<T> {
    pub async fn new(stream: Stream, writer_id: String, controller: Box<dyn ControllerClient>) -> Self {
        let (tx, rx) = channel();
        let selector = SegmentSelector::new(controller).await;
        let processor = Processor{};
        processor.run(tx.clone(), rx, selector).await;
        EventStreamWriterImpl{stream, writer_id, tx}
    }
}

impl<T> EventStreamWriter<T> for EventStreamWriterImpl<T> {
    fn write_event(&self, event: T) {
        let append_event = Event::AppendEvent(AppendEvent{ inner: event, routing_key: Option::None});
        self.tx.send(append_event).expect("send success TODO");
    }

    fn write_event_by_routing_key(&self, routing_key: String, event: T) {
        let append_event = Event::AppendEvent(AppendEvent{inner: event, routing_key: Option::Some(routing_key)});
        self.tx.send(append_event).expect("send success TODO");
    }
}


struct Processor {}

#[async_trait]
impl Processor {
    async fn run<T>(tx: Sender<Event<T>>, rx: Receiver<Event<T>>, mut selector: SegmentSelector) {
        tokio::spawn(async move {
            loop {
                // listen to the receiver channel
                let event = rx.recv().expect("sender closed, processor exit?");
                match event {
                    Event::AppendEvent(event) => {
                        let event_segment_writer = selector.get_segment_writer_for_key(event.routing_key.clone());
                        // TODO how to serialize the event
                        event_segment_writer.write(event).await;
                    },
                    Event::ServerReply(reply) => {

                    }
                }
            }
        })
    }
}

enum Event<T> {
    AppendEvent(AppendEvent<T>),
    ServerReply(ServerReply),
}

struct AppendEvent<T> {
    inner: Option<T>,
    routing_key: Option<String>,
}

struct ServerReply {

}

struct EventSegmentWriter {
    client_connection: Option<Box<dyn ClientConnection>>,
    inflight: Vec<PendingEvent>,
    waiting: Vec<PendingEvent>,
}

#[async_trait]
impl EventSegmentWriter {

    fn new() -> Self {

    }

    pub fn get_segment_name(&self) -> String{}

    pub async fn write(&mut self, event: PendingEvent) {
        self.waiting.push(event);

    }

    pub fn close(&self){}

    pub fn flush(&self){}

    pub fn get_unacked_events_on_seal(&self) -> Vec<PendingEvent>{}

    pub fn get_last_observed_write_offset(&self){}
}

struct ConnectionListener {

}

#[async_trait]
impl ConnectionListener {
    fn run(tx: Sender<Event<T>>) {

    }
}

struct SegmentSelector {
    /// mapping each segment in this stream to it's EventSegmentWriter
    writers: HashMap<Segment, EventSegmentWriter>,

    /// the current segments in this stream
    current_segments: StreamSegments,

    /// the controller instance that is used to get updated segment information from controller
    controller: Box<dyn ControllerClient>,
}

#[async_trait]
impl SegmentSelector {
    async fn new(mut controller: Box<dyn ControllerClient>) -> Self {
        let writers = HashMap::new();
        let current_segments = controller.get_current_segments().await.expect("TODO");
        SegmentSelector{writers, current_segments, controller}
    }

    /// get the segment writer by passing a routing key
    async fn get_segment_writer_for_key(&mut self, routing_key: Option<String>) -> &EventSegmentWriter {
        let event_segment_writer = self.writers.get(&self.get_segment_for_event(routing_key));

        // update the segment information
        while event_segment_writer.is_none() {
            self.refresh_segment_event_writers(&mut self.controller).await;
            let event_segment_writer = self.writers.get(&self.get_segment_for_event(routing_key));
        }
        event_segment_writer.expect("Has to have")
    }

    /// get the Segment by passing a routing key
    fn get_segment_for_event(&self, routing_key: Option<String>) -> Segment {
        if routing_key.is_none() {
            self.current_segments.get_segment_for_key(rng.gen::<f64>())
        } else {
            self.current_segments.get_segment_for_key(hash_string_to_f64(routing_key.expect("routing key")))
        }
    }

    /// refresh the mapping from Segment to EventSegmentWriter. It will create writers for newly added segments
    /// and delete outdated segment writer pairs.
    async fn refresh_segment_event_writers(&mut self, controller: &mut dyn ControllerClient) -> Vec<PendingEvent> {
        self.current_segments = controller.get_current_segments().await.expect("TODO");
        self.create_missing_writers();

        let to_resend = vec!{};
        self.writers = self.writers.into_iter().filter(|&(segment, writer)|
           if !self.current_segments.get_segments().contains(segment) {
               writer.close();
               let unacked = writer.getUnackedEventsOnSeal();
               let to_resend = [&to_resend[..], &unacked[..]].concat();
               false
           } else {
               true
           }
        ).collect();
        to_resend
    }

    /// create missing EventSegmentWriter and set up the connections for ready to use
    fn create_missing_writers(&self) {
        for segment in self.current_segments.getSegments() {
            if !self.writers.containsKey(segment) {
                // TODO: wait for client connection
                // set up connection

                writers.put(segment, writer);
            }
        }
    }
}

struct PendingEvent {
    routing_key:  Option<String>,
    data: Vec<u8>,
}

impl PendingEvent {
    const MAX_WRITE_SIZE: i32 = 8 * 1024 * 1024 + 8;

    fn new(routing_key: Option<String>, data: vec<u8>) -> Result<Self, Error> {
        if data.len()> MAX_WRITE_SIZE{
            // TODO
            Err("Write size too large")
        } else {
            PendingEvent{routing_key, data}
        }
    }

    fn with_header(routing_key: Option<String>, data: Vec<u8>) -> Result<PendingEvent, Error> {
        let data = EventCommand{data}.write_fields().expect("TODO");
        PendingEvent::new(routing_key, data)
    }

    fn without_header(routing_key: Option<String>, data: Vec<u8>) -> Result<PendingEvent, Error> {
        PendingEvent::new(routing_key, data)
    }
}


// hash string to 0.0 - 1.0 in f64
fn hash_string_to_f64(s: String) -> f64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(key.as_bytes());
    let hash_u64 = hasher.finish();
    let shifted = (hash_u64 >> 12) & 0x000fffffffffffffu64;
    f64::from_bits(0x3ff0000000000000u64 + shifted) - 1.0
}