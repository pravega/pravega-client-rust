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
use im::HashMap;
use pravega_client_shared::{ScopedSegment, ScopedStream, Segment, SegmentInfo};
use pravega_controller_client::ControllerError;
use snafu::Snafu;
use tracing::{debug, error};

///
/// A client to fetch meta data of a Stream.
///
#[derive(new)]
pub struct MetaClient {
    scoped_stream: ScopedStream,
    factory: ClientFactoryAsync,
}

#[derive(Debug, Snafu)]
pub enum MetaClientError {
    #[snafu(display("Reader failed to perform reads {} due to {}", operation, error_msg,))]
    StreamSealed {
        stream: String,
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
    #[snafu(display("Could not segment info after configured retries to {}", error_msg))]
    SegmentMetaError {
        segment: String,
        can_retry: bool,
        error_msg: String,
    },
    #[snafu(display("Could not connect due to {}", error_msg))]
    ControllerConnectionError {
        stream: String,
        can_retry: bool,
        source: ControllerError,
        error_msg: String,
    },
}

impl MetaClient {
    ///
    /// Fetch the current Head Segments and the corresponding offsets for the given Stream.
    ///
    pub async fn fetch_current_head_segments(&self) -> Result<HashMap<Segment, i64>, MetaClientError> {
        let segments = self
            .factory
            .controller_client()
            .get_head_segments(&self.scoped_stream)
            .await;
        segments.map_err(|e| {
            MetaClientError::ControllerConnectionError {
                stream: self.scoped_stream.to_string(),
                can_retry: false, // the controller client has retried internally
                source: e.error,
                error_msg: "Failed to fetch Stream's Head segments from controller".to_string(),
            }
        })
    }

    ///
    /// Fetch the Current Tail Segments of a given Stream.
    ///
    pub async fn fetch_current_tail_segments(&self) -> Result<HashMap<Segment, i64>, MetaClientError> {
        match self
            .factory
            .controller_client()
            .get_current_segments(&self.scoped_stream)
            .await
        {
            Ok(segments) => {
                let key_map = segments.key_segment_map;
                if key_map.is_empty() {
                    Err(MetaClientError::StreamSealed {
                        stream: self.scoped_stream.to_string(),
                        can_retry: false,
                        operation: "Get current segments for a stream".to_string(),
                        error_msg: "Zero current segments for the stream".to_string(),
                    })
                } else {
                    let mut map = HashMap::new();
                    for (_, segment_range) in key_map {
                        let info = self.fetch_segment_info(&segment_range.scoped_segment).await;
                        match info {
                            Ok(segment_info) => {
                                debug!("Received SegmentInfo {:?}", segment_info);
                                map.insert(segment_range.get_segment(), segment_info.write_offset);
                            }
                            Err(e) => {
                                error!(
                                    "Error while fetching segment info for segment {:?} after retries. {:?}",
                                    segment_range.scoped_segment, e
                                );
                                return Err(e);
                            }
                        }
                    }
                    Ok(map)
                }
            }
            Err(e) => {
                Err(MetaClientError::ControllerConnectionError {
                    stream: self.scoped_stream.to_string(),
                    can_retry: false, // Controller client internally retries
                    source: e.error,
                    error_msg: "Failed to fetch Stream's current segments from controller".to_string(),
                })
            }
        }
    }

    ///
    /// Check if the stream is sealed.
    ///
    pub async fn is_stream_sealed(&self) -> bool {
        match self.fetch_current_tail_segments().await {
            Ok(_) => false,
            Err(e) => {
                matches!(e, MetaClientError::StreamSealed { .. })
            }
        }
    }

    // Helper method to fetch Segment information for a given Segment.
    // This ensures we retry with the provided retry configuration incase of errors.
    async fn fetch_segment_info(
        &self,
        scoped_segment: &ScopedSegment,
    ) -> Result<SegmentInfo, MetaClientError> {
        // Create a segment meta client for the specified segment.
        let segment_meta_client = self
            .factory
            .create_segment_metadata_client(scoped_segment.clone())
            .await;
        segment_meta_client.get_segment_info().await.map_err(|e| {
            error!(
                "Failed to fetch Segment info for segment {:?}. Error {:?}",
                scoped_segment, e
            );
            MetaClientError::SegmentMetaError {
                segment: scoped_segment.to_string(),
                can_retry: false,
                error_msg: "Failed to fetch segment info for segment".to_string(),
            }
        })
    }
}
