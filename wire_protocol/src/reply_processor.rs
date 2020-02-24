//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
use crate::wire_commands::*;
use crate::error::ReplyError;
use crate::commands::*;

pub trait FailingReplyProcessor {
    fn process(&self, reply: Replies);

    fn hello(hello: HelloCommand) -> Result<(), ReplyError> {
        if hello.low_version > WIRE_VERSION || hello.high_version < OLDEST_COMPATIBLE_VERSION {
            Err(ReplyError::IncompatibleVersion { low: hello.low_version, high: hello.high_version })
        } else {
            Ok(())
        }
    }

    fn wrong_host(wrong_host: WrongHostCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState { state: Replies::WrongHost(wrong_host) })
    }

    fn segment_already_exists(segment_already_exists: SegmentAlreadyExistsCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::SegmentAlreadyExists(segment_already_exists)})
    }

    fn segment_is_sealed(segment_is_sealed: SegmentIsSealedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::SegmentIsSealed(segment_is_sealed)})
    }

    fn segment_is_truncated(segment_is_truncated: SegmentIsTruncatedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::SegmentIsTruncated(segment_is_truncated)})
    }

    fn no_such_segment(no_such_segment: NoSuchSegmentCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::NoSuchSegment(no_such_segment)})
    }

    fn table_segment_not_empty(table_segment_not_empty: TableSegmentNotEmptyCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::TableSegmentNotEmpty(table_segment_not_empty)})
    }

    fn invalid_event_number(invalid_event_number: InvalidEventNumberCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::InvalidEventNumber(invalid_event_number)})
    }

    fn append_setup(append_setup: AppendSetupCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::AppendSetup(append_setup)})
    }

    fn data_appended(data_appended: DataAppendedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::DataAppended(data_appended)})
    }

    fn conditional_check_failed(data_not_appended: ConditionalCheckFailedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::ConditionalCheckFailed(data_not_appended)})
    }

    fn segment_read(segment_read: SegmentReadCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::SegmentRead(segment_read)})
    }

    fn segment_attribute_updated(segment_attribute_updated: SegmentAttributeUpdatedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::SegmentAttributeUpdated(segment_attribute_updated)})
    }

    fn segment_attribute(segment_attribute: SegmentAttributeCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::SegmentAttribute(segment_attribute)})
    }

    fn stream_segment_info(stream_info: StreamSegmentInfoCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::StreamSegmentInfo(stream_info)})
    }

    fn segment_created(segment_created: SegmentCreatedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::SegmentCreated(segment_created)})
    }

    fn segments_merged(segments_merged: SegmentsMergedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::SegmentsMerged(segments_merged)})
    }

    fn segment_sealed(segment_sealed: SegmentSealedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::SegmentSealed(segment_sealed)})
    }

    fn segment_truncated(segment_truncated: SegmentTruncatedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::SegmentTruncated(segment_truncated)})
    }

    fn segment_deleted(segment_deleted: SegmentDeletedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::SegmentDeleted(segment_deleted)})
    }

    fn operation_unsupported(operation_unsupported: OperationUnsupportedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::OperationUnsupported(operation_unsupported)})
    }

    fn keep_alive(keep_alive: KeepAliveCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::KeepAlive(keep_alive)})
    }

    fn segment_policy_updated(segment_policy_updated: SegmentPolicyUpdatedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::SegmentPolicyUpdated(segment_policy_updated)})
    }

    fn auth_token_check_failed(auth_token_check_failed: AuthTokenCheckFailedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::AuthTokenCheckFailed(auth_token_check_failed)})
    }

    fn table_entries_updated(table_entries_updated: TableEntriesUpdatedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::TableEntriesUpdated(table_entries_updated)})
    }

    fn table_keys_removed(table_keys_removed: TableKeysRemovedCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::TableKeysRemoved(table_keys_removed)})
    }

    fn table_read(table_read: TableReadCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::TableRead(table_read)})
    }

    fn table_key_does_not_exist(table_key_does_not_exist: TableKeyDoesNotExistCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::TableKeyDoesNotExist(table_key_does_not_exist)})
    }

    fn table_key_bad_version(table_key_bad_version: TableKeyBadVersionCommand)-> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::TableKeyBadVersion(table_key_bad_version)})
    }

    fn table_keys_read(table_keys_read: TableReadCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::TableRead(table_keys_read)})
    }

    fn table_entries_read(table_entries_read: TableEntriesReadCommand) -> Result<(), ReplyError> {
        Err(ReplyError::IllegalState {state: Replies::TableEntriesRead(table_entries_read)})
    }
}