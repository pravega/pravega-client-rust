//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::get_random_f64;
use tokio::sync::mpsc::Sender;
use tracing::{debug, warn};

use pravega_rust_client_retry::retry_async::retry_async;
use pravega_rust_client_retry::retry_result::RetryResult;
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_config::ClientConfig;

use crate::client_factory::ClientFactoryInternal;
use crate::reactor::event::{Incoming, PendingEvent};
use crate::reactor::segment_writer::SegmentWriter;

pub(crate) struct SegmentSelector {
    /// Stream that this SegmentSelector is on
    pub(crate) stream: ScopedStream,

    /// mapping each segment in this stream to it's EventSegmentWriter
    pub(crate) writers: HashMap<ScopedSegment, SegmentWriter>,

    /// the current segments in this stream
    pub(crate) current_segments: StreamSegments,

    /// the sender that sends reply back to Processor
    pub(crate) sender: Sender<Incoming>,

    /// client config that contains the retry policy
    pub(crate) config: ClientConfig,

    /// Used to gain access to the controller and connection pool
    pub(crate) factory: Arc<ClientFactoryInternal>,
}

impl SegmentSelector {
    pub(crate) fn new(
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

    pub(crate) async fn initialize(&mut self) {
        self.current_segments = self
            .factory
            .get_controller_client()
            .get_current_segments(&self.stream)
            .await
            .expect("retry failed");
        self.create_missing_writers().await;
    }

    /// get the Segment by passing a routing key
    pub(crate) fn get_segment_for_event(&mut self, routing_key: &Option<String>) -> ScopedSegment {
        if let Some(key) = routing_key {
            self.current_segments.get_segment_for_string(key)
        } else {
            self.current_segments.get_segment(get_random_f64())
        }
    }

    /// refresh segment event writer when a segment is sealed
    /// return the inflight events of that sealed segment
    pub(crate) async fn refresh_segment_event_writers_upon_sealed(
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
    pub(crate) async fn update_segments_upon_sealed(
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
    pub(crate) async fn create_missing_writers(&mut self) {
        for scoped_segment in self.current_segments.get_segments() {
            if !self.writers.contains_key(&scoped_segment) {
                let mut writer = SegmentWriter::new(
                    scoped_segment.clone(),
                    self.sender.clone(),
                    self.config.retry_policy,
                );

                debug!(
                    "writer {:?} created for segment {:?}",
                    writer.id,
                    scoped_segment.to_string()
                );
                if let Err(_e) = writer.setup_connection(&self.factory).await {
                    writer.reconnect(&self.factory).await;
                }
                self.writers.insert(scoped_segment, writer);
            }
        }
    }

    /// resend events
    pub(crate) async fn resend(&mut self, to_resend: Vec<PendingEvent>) {
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

    pub(crate) fn remove_segment_event_writer(&mut self, segment: &ScopedSegment) {
        self.writers.remove(segment);
    }
}
