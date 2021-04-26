//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactory;
use crate::util::get_random_f64;
use crate::segment::event::Incoming;
use crate::segment::writer::{Append, SegmentWriter};

use pravega_client_channel::ChannelSender;
use pravega_client_auth::DelegationTokenProvider;
use pravega_client_shared::{ScopedStream, ScopedSegment, StreamSegments, StreamSegmentsWithPredecessors};

use ahash::RandomState;
use std::collections::{BTreeMap, HashMap};
use tracing::{debug, warn};
use std::sync::Arc;

/// Maintains mapping from segments to segment writers.
pub(crate) struct SegmentSelector {
    /// The stream of this SegmentSelector.
    pub(crate) stream: ScopedStream,

    /// Maps segment to SegmentWriter.
    pub(crate) writers: HashMap<ScopedSegment, SegmentWriter, RandomState>,

    /// The current segments in this stream.
    pub(crate) current_segments: StreamSegments,

    /// The sender that sends reply back to Reactor.
    pub(crate) sender: ChannelSender<Incoming>,

    /// Access the controller and connection pool.
    pub(crate) factory: ClientFactory,

    /// Delegation token for authentication.
    pub(crate) delegation_token_provider: Arc<DelegationTokenProvider>,
}

impl SegmentSelector {
    pub(crate) async fn new(
        stream: ScopedStream,
        sender: ChannelSender<Incoming>,
        factory: ClientFactory,
    ) -> Self {
        let delegation_token_provider = factory.create_delegation_token_provider(stream.clone()).await;
        SegmentSelector {
            stream,
            writers: HashMap::default(),
            current_segments: StreamSegments::new(BTreeMap::new()),
            sender,
            factory,
            delegation_token_provider: Arc::new(delegation_token_provider),
        }
    }

    /// Initializes segment writers by setting up connections so that segment
    /// writers are ready to use after initialization.
    pub(crate) async fn initialize(&mut self, stream_segments: Option<StreamSegments>) {
        if let Some(ss) = stream_segments {
            self.current_segments = ss;
        } else {
            self.current_segments = self
                .factory
                .get_controller_client()
                .get_current_segments(&self.stream)
                .await                .expect("retry failed");
        }
        self.create_missing_writers().await;
    }

    /// Gets a segment writer by providing an optional routing key. The stream at least owns one
    /// segment so this method should always has writer to return.
    pub(crate) fn get_segment_writer(&mut self, routing_key: &Option<String>) -> &mut SegmentWriter {
        let segment = self
            .current_segments
            .get_segment_for_routing_key(routing_key, get_random_f64);
        self.writers
            .get_mut(segment)
            .expect("must have corresponding writer")
    }

    /// Maintains an internal segment-writer mapping. Fetches the successor segments from controller
    /// when a segment is sealed and creates segment writer for the new segments. Returns any inflight
    /// events of the sealed segment so that those could be resend to their successors.
    pub(crate) async fn refresh_segment_event_writers_upon_sealed(
        &mut self,
        sealed_segment: &ScopedSegment,
    ) -> Option<Vec<Append>> {
        let stream_segments_with_predecessors = self
            .factory
            .get_controller_client()
            .get_successors(sealed_segment)
            .await
            .expect("get successors for sealed segment");

        if stream_segments_with_predecessors.is_stream_sealed() {
            None
        } else {
            Some(
                self.update_segments_upon_sealed(stream_segments_with_predecessors, sealed_segment)
                    .await,
            )
        }
    }

    /// Creates event segment writer for the successor segment of the sealed segment and returns
    /// any inflight events.
    pub(crate) async fn update_segments_upon_sealed(
        &mut self,
        successors: StreamSegmentsWithPredecessors,
        sealed_segment: &ScopedSegment,
    ) -> Vec<Append> {
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

    /// Creates any missing segment writers and sets up connections for them.
    #[allow(clippy::map_entry)] // clippy warns about using entry, but async closure is not stable
    pub(crate) async fn create_missing_writers(&mut self) {
        for scoped_segment in self.current_segments.get_segments() {
            if !self.writers.contains_key(&scoped_segment) {
                let mut writer = SegmentWriter::new(
                    scoped_segment.clone(),
                    self.sender.clone(),
                    self.factory.get_config().retry_policy,
                    self.delegation_token_provider.clone(),
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

    /// Resends a list of events.
    pub(crate) async fn resend(&mut self, to_resend: Vec<Append>) {
        for append in to_resend {
            let segment = self
                .current_segments
                .get_segment_for_routing_key(&append.event.routing_key, get_random_f64);
            let segment_writer = self.writers.get_mut(segment).expect("must have writer");
            segment_writer.add_pending(append.event, append.cap_guard);
            if let Err(e) = segment_writer.write_pending_events().await {
                warn!(
                    "failed to resend an event due to: {:?}, reconnecting the event segment writer",
                    e
                );
                segment_writer.reconnect(&self.factory).await;
            }
        }
    }

    /// Removes segment writer from the internal map.
    pub(crate) fn remove_segment_writer(&mut self, segment: &ScopedSegment) -> Option<SegmentWriter> {
        self.writers.remove(segment)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use im::HashMap as ImHashMap;
    use ordered_float::OrderedFloat;
    use pravega_client_channel::{create_channel, ChannelReceiver};
    use pravega_client_config::connection_type::{ConnectionType, MockType};
    use pravega_client_config::ClientConfigBuilder;
    use tokio::runtime::Runtime;
    use pravega_client_shared::{PravegaNodeUri, SegmentWithRange, Segment, Scope, StreamConfiguration, Scaling, ScaleType, Retention, RetentionType};

    #[test]
    fn test_segment_selector() {
        let rt = Runtime::new().unwrap();
        let (mut selector, _sender, _receiver, _factory) =
            rt.block_on(create_segment_selector(MockType::Happy));
        assert_eq!(selector.writers.len(), 2);

        // update successors for sealed segment
        let mut sp = ImHashMap::new();
        let successor1 = SegmentWithRange {
            scoped_segment: ScopedSegment::from("testScope/testStream/2"),
            min_key: OrderedFloat::from(0.0),
            max_key: OrderedFloat::from(0.25),
        };
        let successor2 = SegmentWithRange {
            scoped_segment: ScopedSegment::from("testScope/testStream/3"),
            min_key: OrderedFloat::from(0.25),
            max_key: OrderedFloat::from(0.5),
        };
        let pred = vec![Segment::from(0)];
        sp.insert(successor1.clone(), pred.clone());
        sp.insert(successor2.clone(), pred);

        let replace = vec![successor1, successor2];
        let mut rs = ImHashMap::new();
        rs.insert(Segment::from(0), replace);
        let ssp = StreamSegmentsWithPredecessors {
            segment_with_predecessors: sp,
            replacement_segments: rs,
        };

        let sealed_segment = ScopedSegment::from("testScope/testStream/0");

        let events = rt.block_on(selector.update_segments_upon_sealed(ssp, &sealed_segment));
        assert!(events.is_empty());
        assert_eq!(selector.writers.len(), 4);
    }

    // helper function section
    pub(crate) async fn create_segment_selector(
        mock: MockType,
    ) -> (
        SegmentSelector,
        ChannelSender<Incoming>,
        ChannelReceiver<Incoming>,
        ClientFactory,
    ) {
        let stream = ScopedStream::from("testScope/testStream");
        let config = ClientConfigBuilder::default()
            .connection_type(ConnectionType::Mock(mock))
            .controller_uri(PravegaNodeUri::from("127.0.0.1:9091".to_string()))
            .mock(true)
            .build()
            .unwrap();
        let factory = ClientFactory::new(config);
        factory
            .get_controller_client()
            .create_scope(&Scope {
                name: "testScope".to_string(),
            })
            .await
            .unwrap();
        factory
            .get_controller_client()
            .create_stream(&StreamConfiguration {
                scoped_stream: stream.clone(),
                scaling: Scaling {
                    scale_type: ScaleType::FixedNumSegments,
                    target_rate: 1,
                    scale_factor: 1,
                    min_num_segments: 2,
                },
                retention: Retention {
                    retention_type: RetentionType::None,
                    retention_param: 0,
                },
            })
            .await
            .unwrap();
        let (sender, receiver) = create_channel(1024);
        let mut selector = SegmentSelector::new(stream.clone(), sender.clone(), factory.clone()).await;
        let stream_segments = factory
            .get_controller_client()
            .get_current_segments(&stream)
            .await
            .unwrap();
        selector.initialize(Some(stream_segments)).await;
        (selector, sender, receiver, factory)
    }
}
