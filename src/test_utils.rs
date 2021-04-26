//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//

use crate::segment::raw_client::{RawClientImpl, RawClient, RawClientError};
use crate::segment::metadata::SegmentMetadataClient;
use crate::client_factory::ClientFactory;
use crate::segment::reader::{AsyncSegmentReaderImpl, AsyncSegmentReader};

use pravega_client_shared::{PravegaNodeUri, ScopedSegment, SegmentInfo};
use pravega_connection_pool::connection_pool::ConnectionPool;
use pravega_wire_protocol::client_connection::ClientConnection;
use pravega_wire_protocol::connection_factory::SegmentConnectionManager;
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use pravega_wire_protocol::commands::SegmentReadCommand;

use tokio::time::Duration;
use pravega_client_auth::DelegationTokenProvider;

// This wrapper is used for integration test.
// It is not supposed to be used by applications.
#[doc(hidden)]
pub struct RawClientWrapper<'a> {
    inner: RawClientImpl<'a>
}

impl<'a> RawClientWrapper<'a> {
    pub fn new(
        pool: &'a ConnectionPool<SegmentConnectionManager>,
        endpoint: PravegaNodeUri,
        timeout: Duration,
    ) -> RawClientWrapper<'a> {
        let inner = RawClientImpl::new(pool,endpoint,timeout);
        RawClientWrapper{inner}
    }

    pub async fn send_request(&self, request: &Requests) -> Result<Replies, RawClientError> {
        self.inner.send_request(request).await
    }

    pub async fn send_setup_request(
        &self,
        request: &Requests,
    ) -> Result<(Replies, Box<dyn ClientConnection + 'a>), RawClientError> {
        self.inner.send_setup_request(request).await
    }
}

// This wrapper is used for integration test.
// It is not supposed to be used by applications.
#[doc(hidden)]
pub struct MetadataWrapper {
    inner: SegmentMetadataClient
}

impl MetadataWrapper {
    pub async fn new(segment: ScopedSegment, factory: ClientFactory) -> Self {
        let inner = SegmentMetadataClient::new(segment, factory).await;
        MetadataWrapper{inner}
    }

    pub async fn get_segment_info(&self) -> Result<SegmentInfo, String> {
        self.inner.get_segment_info().await.map_err(|e| format!("{:?}", e))
    }

    pub async fn fetch_current_segment_length(&self) -> Result<i64, String> {
        self.inner.fetch_current_segment_length().await.map_err(|e| format!("{:?}", e))
    }

    pub async fn fetch_current_starting_head(&self) -> Result<i64, String> {
        self.inner.fetch_current_starting_head().await.map_err(|e| format!("{:?}", e))
    }

    pub async fn truncate_segment(&self, offset: i64) -> Result<(), String> {
        self.inner.truncate_segment(offset).await.map_err(|e| format!("{:?}", e))
    }

    pub async fn seal_segment(&self) -> Result<(), String> {
        self.inner.seal_segment().await.map_err(|e| format!("{:?}", e))
    }
}

// This wrapper is used for integration test.
// It is not supposed to be used by applications.
#[doc(hidden)]
pub struct SegmentReaderWrapper {
    inner: AsyncSegmentReaderImpl
}

impl SegmentReaderWrapper {
    pub async fn new(
        segment: ScopedSegment,
        factory: ClientFactory,
        delegation_token_provider: DelegationTokenProvider,
    ) -> SegmentReaderWrapper {
        let inner = AsyncSegmentReaderImpl::new(segment, factory, delegation_token_provider).await;
        SegmentReaderWrapper{inner}
    }
    pub async fn read(&self, offset: i64, length: i32) -> Result<SegmentReadCommand, String> {
        self.inner.read(offset, length).await.map_err(|e| format!("{:?}", e))
    }
}