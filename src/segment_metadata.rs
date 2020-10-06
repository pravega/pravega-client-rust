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
use crate::get_request_id;
use crate::raw_client::RawClient;
use pravega_rust_client_auth::DelegationTokenProvider;
use pravega_rust_client_retry::retry_async::retry_async;
use pravega_rust_client_retry::retry_result::RetryResult;
use pravega_rust_client_shared::{ScopedSegment, SegmentInfo};
use pravega_wire_protocol::commands::{
    GetStreamSegmentInfoCommand, SealSegmentCommand, TruncateSegmentCommand,
};
use pravega_wire_protocol::wire_commands::{Replies, Requests};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum SegmentMetadataClientError {
    #[snafu(display(
        "SegmentMetadataClient for segment {} failed to {} due to {}",
        segment,
        operation,
        error_msg
    ))]
    NoSuchSegment {
        segment: String,
        operation: String,
        error_msg: String,
    },
}

/// A client for looking at and editing the metadata related to a specific segment.
#[derive(new)]
pub struct SegmentMetadataClient {
    segment: ScopedSegment,
    factory: ClientFactory,
    delegation_token_provider: DelegationTokenProvider,
}

impl SegmentMetadataClient {
    /// Returns info for the current segment.
    pub async fn get_segment_info(&self) -> Result<SegmentInfo, SegmentMetadataClientError> {
        let controller = self.factory.get_controller_client();

        retry_async(self.factory.get_config().retry_policy, || async {
            let raw_client = self.factory.create_raw_client(&self.segment).await;
            let result = raw_client
                .send_request(&Requests::GetStreamSegmentInfo(GetStreamSegmentInfoCommand {
                    request_id: get_request_id(),
                    segment_name: self.segment.to_string(),
                    delegation_token: self.delegation_token_provider.retrieve_token(controller).await,
                }))
                .await;

            match result {
                Ok(reply) => match reply {
                    Replies::StreamSegmentInfo(cmd) => RetryResult::Success(SegmentInfo {
                        segment: self.segment.clone(),
                        starting_offset: cmd.start_offset,
                        write_offset: cmd.write_offset,
                        is_sealed: cmd.is_sealed,
                        last_modified_time: cmd.last_modified,
                    }),
                    Replies::WrongHost(_cmd) => RetryResult::Retry("wrong host".to_string()),
                    Replies::NoSuchSegment(_cmd) => RetryResult::Fail("no such segment".to_string()),
                    _ => RetryResult::Fail("unexpected reply".to_string()),
                },
                Err(e) => {
                    if e.is_token_expired() {
                        self.delegation_token_provider.signal_token_expiry();
                        RetryResult::Retry(e.to_string())
                    } else {
                        RetryResult::Retry(e.to_string())
                    }
                }
            }
        })
        .await
        .map_err(|e| SegmentMetadataClientError::NoSuchSegment {
            segment: self.segment.to_string(),
            operation: "get segment info".to_string(),
            error_msg: e.error,
        })
    }

    /// Returns the length of the current segment. i.e. the total length of all data written to the segment.
    pub async fn fetch_current_segment_length(&self) -> Result<i64, SegmentMetadataClientError> {
        self.get_segment_info().await.map(|cmd| cmd.write_offset)
    }

    /// Returns the current head of segment. Call this method to get the latest head after
    /// segment truncation.
    pub async fn fetch_current_starting_head(&self) -> Result<i64, SegmentMetadataClientError> {
        self.get_segment_info().await.map(|cmd| cmd.starting_offset)
    }

    /// Deletes all data before the offset of the provided segment.
    /// This data will no longer be readable. Existing offsets are not affected by this operations.
    /// The new startingOffset will be reflected in latest SegmentInfo.
    pub async fn truncate_segment(&self, offset: i64) -> Result<(), SegmentMetadataClientError> {
        let controller = self.factory.get_controller_client();

        retry_async(self.factory.get_config().retry_policy, || async {
            let raw_client = self.factory.create_raw_client(&self.segment).await;
            let result = raw_client
                .send_request(&Requests::TruncateSegment(TruncateSegmentCommand {
                    request_id: get_request_id(),
                    segment: self.segment.to_string(),
                    truncation_offset: offset,
                    delegation_token: self.delegation_token_provider.retrieve_token(controller).await,
                }))
                .await;

            match result {
                Ok(reply) => match reply {
                    Replies::SegmentTruncated(_cmd) => RetryResult::Success(()),
                    Replies::WrongHost(_cmd) => RetryResult::Retry("wrong host".to_string()),
                    Replies::NoSuchSegment(_cmd) => RetryResult::Fail("no such segment".to_string()),
                    _ => RetryResult::Fail("unexpected reply".to_string()),
                },
                Err(e) => {
                    if e.is_token_expired() {
                        self.delegation_token_provider.signal_token_expiry();
                        RetryResult::Retry(e.to_string())
                    } else {
                        RetryResult::Retry(e.to_string())
                    }
                }
            }
        })
        .await
        .map_err(|e| SegmentMetadataClientError::NoSuchSegment {
            segment: self.segment.to_string(),
            operation: "truncate segment".to_string(),
            error_msg: e.error,
        })
    }

    /// Seals the segment so that no more writes can go to it.
    pub async fn seal_segment(&self) -> Result<(), SegmentMetadataClientError> {
        let controller = self.factory.get_controller_client();

        retry_async(self.factory.get_config().retry_policy, || async {
            let raw_client = self.factory.create_raw_client(&self.segment).await;
            let result = raw_client
                .send_request(&Requests::SealSegment(SealSegmentCommand {
                    request_id: get_request_id(),
                    segment: self.segment.to_string(),
                    delegation_token: self.delegation_token_provider.retrieve_token(controller).await,
                }))
                .await;

            match result {
                Ok(reply) => match reply {
                    Replies::SegmentSealed(_cmd) => RetryResult::Success(()),
                    Replies::WrongHost(_cmd) => RetryResult::Retry("wrong host".to_string()),
                    Replies::NoSuchSegment(_cmd) => RetryResult::Fail("no such segment".to_string()),
                    _ => RetryResult::Fail("unexpected reply".to_string()),
                },
                Err(e) => {
                    if e.is_token_expired() {
                        self.delegation_token_provider.signal_token_expiry();
                        RetryResult::Retry(e.to_string())
                    } else {
                        RetryResult::Retry(e.to_string())
                    }
                }
            }
        })
        .await
        .map_err(|e| SegmentMetadataClientError::NoSuchSegment {
            segment: self.segment.to_string(),
            operation: "seal segment".to_string(),
            error_msg: e.error,
        })
    }
}
