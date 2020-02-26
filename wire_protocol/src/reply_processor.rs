//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::commands::*;
use crate::error::ReplyError;
use crate::wire_commands::*;

pub trait FailingReplyProcessor: Send + Sync {
    fn process(&mut self, reply: Replies);

    fn hello(&self, hello: HelloCommand) -> Result<(), ReplyError> {
        if hello.low_version > WIRE_VERSION || hello.high_version < OLDEST_COMPATIBLE_VERSION {
            Err(ReplyError::IncompatibleVersion {
                low: hello.low_version,
                high: hello.high_version,
            })
        } else {
            Ok(())
        }
    }

    fn wrong_host(&self, wrong_host: WrongHostCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::WrongHost(wrong_host),
        })
    }

    fn segment_already_exists(
        &self,
        segment_already_exists: SegmentAlreadyExistsCommand,
    ) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::SegmentAlreadyExists(segment_already_exists),
        })
    }

    fn segment_is_sealed(&self, segment_is_sealed: SegmentIsSealedCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::SegmentIsSealed(segment_is_sealed),
        })
    }

    fn segment_is_truncated(
        &self,
        segment_is_truncated: SegmentIsTruncatedCommand,
    ) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::SegmentIsTruncated(segment_is_truncated),
        })
    }

    fn no_such_segment(&self, no_such_segment: NoSuchSegmentCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::NoSuchSegment(no_such_segment),
        })
    }

    fn table_segment_not_empty(
        &self,
        table_segment_not_empty: TableSegmentNotEmptyCommand,
    ) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::TableSegmentNotEmpty(table_segment_not_empty),
        })
    }

    fn invalid_event_number(
        &self,
        invalid_event_number: InvalidEventNumberCommand,
    ) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::InvalidEventNumber(invalid_event_number),
        })
    }

    fn append_setup(&self, append_setup: AppendSetupCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::AppendSetup(append_setup),
        })
    }

    fn data_appended(&self, data_appended: DataAppendedCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::DataAppended(data_appended),
        })
    }

    fn conditional_check_failed(
        &self,
        data_not_appended: ConditionalCheckFailedCommand,
    ) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::ConditionalCheckFailed(data_not_appended),
        })
    }

    fn segment_read(&self, segment_read: SegmentReadCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::SegmentRead(segment_read),
        })
    }

    fn segment_attribute_updated(
        &self,
        segment_attribute_updated: SegmentAttributeUpdatedCommand,
    ) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::SegmentAttributeUpdated(segment_attribute_updated),
        })
    }

    fn segment_attribute(&self, segment_attribute: SegmentAttributeCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::SegmentAttribute(segment_attribute),
        })
    }

    fn stream_segment_info(&self, stream_info: StreamSegmentInfoCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::StreamSegmentInfo(stream_info),
        })
    }

    fn segment_created(&self, segment_created: SegmentCreatedCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::SegmentCreated(segment_created),
        })
    }

    fn segments_merged(&self, segments_merged: SegmentsMergedCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::SegmentsMerged(segments_merged),
        })
    }

    fn segment_sealed(&self, segment_sealed: SegmentSealedCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::SegmentSealed(segment_sealed),
        })
    }

    fn segment_truncated(&self, segment_truncated: SegmentTruncatedCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::SegmentTruncated(segment_truncated),
        })
    }

    fn segment_deleted(&self, segment_deleted: SegmentDeletedCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::SegmentDeleted(segment_deleted),
        })
    }

    fn operation_unsupported(
        &self,
        operation_unsupported: OperationUnsupportedCommand,
    ) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::OperationUnsupported(operation_unsupported),
        })
    }

    fn keep_alive(&self, keep_alive: KeepAliveCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::KeepAlive(keep_alive),
        })
    }

    fn segment_policy_updated(
        &self,
        segment_policy_updated: SegmentPolicyUpdatedCommand,
    ) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::SegmentPolicyUpdated(segment_policy_updated),
        })
    }

    fn auth_token_check_failed(
        &self,
        auth_token_check_failed: AuthTokenCheckFailedCommand,
    ) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::AuthTokenCheckFailed(auth_token_check_failed),
        })
    }

    fn table_entries_updated(
        &self,
        table_entries_updated: TableEntriesUpdatedCommand,
    ) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::TableEntriesUpdated(table_entries_updated),
        })
    }

    fn table_keys_removed(&self, table_keys_removed: TableKeysRemovedCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::TableKeysRemoved(table_keys_removed),
        })
    }

    fn table_read(&self, table_read: TableReadCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::TableRead(table_read),
        })
    }

    fn table_key_does_not_exist(
        &self,
        table_key_does_not_exist: TableKeyDoesNotExistCommand,
    ) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::TableKeyDoesNotExist(table_key_does_not_exist),
        })
    }

    fn table_key_bad_version(
        &self,
        table_key_bad_version: TableKeyBadVersionCommand,
    ) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::TableKeyBadVersion(table_key_bad_version),
        })
    }

    fn table_keys_read(&self, table_keys_read: TableReadCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::TableRead(table_keys_read),
        })
    }

    fn table_entries_read(&self, table_entries_read: TableEntriesReadCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {
            state: Replies::TableEntriesRead(table_entries_read),
        })
    }
}

pub struct FailingReplyProcessorImpl {}

impl FailingReplyProcessor for FailingReplyProcessorImpl {
    fn process(&mut self, _reply: Replies) {}
}
