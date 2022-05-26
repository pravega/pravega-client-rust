//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use super::commands::*;
use super::error::CommandError;
use crate::error::InvalidType;
use byteorder::{BigEndian, ByteOrder};
use std::fmt;

#[derive(PartialEq, Debug)]
pub enum WireCommands {
    Requests(Requests),
    Replies(Replies),
}

#[derive(PartialEq, Debug)]
pub enum Requests {
    AppendBlock(AppendBlockCommand),
    AppendBlockEnd(AppendBlockEndCommand),
    Padding(PaddingCommand),
    PartialEvent(PartialEventCommand),
    Event(EventCommand),

    Hello(HelloCommand),
    SetupAppend(SetupAppendCommand),
    ConditionalAppend(ConditionalAppendCommand),
    ReadSegment(ReadSegmentCommand),
    GetSegmentAttribute(GetSegmentAttributeCommand),
    UpdateSegmentAttribute(UpdateSegmentAttributeCommand),
    GetStreamSegmentInfo(GetStreamSegmentInfoCommand),
    CreateSegment(CreateSegmentCommand),
    CreateTableSegment(CreateTableSegmentCommand),
    UpdateSegmentPolicy(UpdateSegmentPolicyCommand),
    MergeSegments(MergeSegmentsCommand),
    MergeTableSegments(MergeTableSegmentsCommand),
    SealSegment(SealSegmentCommand),
    SealTableSegment(SealTableSegmentCommand),
    TruncateSegment(TruncateSegmentCommand),
    DeleteSegment(DeleteSegmentCommand),
    DeleteTableSegment(DeleteTableSegmentCommand),
    KeepAlive(KeepAliveCommand),
    UpdateTableEntries(UpdateTableEntriesCommand),
    RemoveTableKeys(RemoveTableKeysCommand),
    ReadTable(ReadTableCommand),
    ReadTableKeys(ReadTableKeysCommand),
    ReadTableEntries(ReadTableEntriesCommand),
    ReadTableEntriesDelta(ReadTableEntriesDeltaCommand),
    ConditionalBlockEnd(ConditionalBlockEndCommand),
}

#[derive(PartialEq, Debug, Clone)]
pub enum Replies {
    Hello(HelloCommand),
    WrongHost(WrongHostCommand),
    SegmentIsSealed(SegmentIsSealedCommand),
    SegmentAlreadyExists(SegmentAlreadyExistsCommand),
    SegmentIsTruncated(SegmentIsTruncatedCommand),
    NoSuchSegment(NoSuchSegmentCommand),
    TableSegmentNotEmpty(TableSegmentNotEmptyCommand),
    InvalidEventNumber(InvalidEventNumberCommand),
    OperationUnsupported(OperationUnsupportedCommand),
    AppendSetup(AppendSetupCommand),
    DataAppended(DataAppendedCommand),
    ConditionalCheckFailed(ConditionalCheckFailedCommand),
    SegmentRead(SegmentReadCommand),
    SegmentAttribute(SegmentAttributeCommand),
    SegmentAttributeUpdated(SegmentAttributeUpdatedCommand),
    StreamSegmentInfo(StreamSegmentInfoCommand),
    SegmentCreated(SegmentCreatedCommand),
    SegmentPolicyUpdated(SegmentPolicyUpdatedCommand),
    SegmentsMerged(SegmentsMergedCommand),
    SegmentSealed(SegmentSealedCommand),
    SegmentTruncated(SegmentTruncatedCommand),
    SegmentDeleted(SegmentDeletedCommand),
    KeepAlive(KeepAliveCommand),
    AuthTokenCheckFailed(AuthTokenCheckFailedCommand),
    TableEntriesUpdated(TableEntriesUpdatedCommand),
    TableKeysRemoved(TableKeysRemovedCommand),
    TableRead(TableReadCommand),
    TableKeysRead(TableKeysReadCommand),
    TableEntriesRead(TableEntriesReadCommand),
    TableKeyDoesNotExist(TableKeyDoesNotExistCommand),
    TableKeyBadVersion(TableKeyBadVersionCommand),
    TableEntriesDeltaRead(TableEntriesDeltaReadCommand),
}

impl fmt::Display for Replies {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Request for Requests {
    fn get_request_id(&self) -> i64 {
        match self {
            Requests::Hello(hello_cmd) => Request::get_request_id(hello_cmd),
            Requests::SetupAppend(setup_append_cmd) => setup_append_cmd.get_request_id(),
            Requests::ConditionalAppend(conditional_append_cmd) => conditional_append_cmd.get_request_id(),
            Requests::ReadSegment(read_segment_cmd) => read_segment_cmd.get_request_id(),
            Requests::GetSegmentAttribute(get_segment_attribute_cmd) => {
                get_segment_attribute_cmd.get_request_id()
            }
            Requests::UpdateSegmentAttribute(update_segment_attribute_cmd) => {
                update_segment_attribute_cmd.get_request_id()
            }
            Requests::GetStreamSegmentInfo(get_stream_segment_info_cmd) => {
                get_stream_segment_info_cmd.get_request_id()
            }
            Requests::CreateSegment(create_segment_cmd) => create_segment_cmd.get_request_id(),
            Requests::CreateTableSegment(create_table_segment_command) => {
                create_table_segment_command.get_request_id()
            }
            Requests::UpdateSegmentPolicy(update_segment_policy_cmd) => {
                update_segment_policy_cmd.get_request_id()
            }
            Requests::MergeSegments(merge_segments_cmd) => merge_segments_cmd.get_request_id(),
            Requests::MergeTableSegments(merge_table_segments_cmd) => {
                merge_table_segments_cmd.get_request_id()
            }
            Requests::SealSegment(seal_segment_cmd) => seal_segment_cmd.get_request_id(),
            Requests::SealTableSegment(seal_table_segment_cmd) => seal_table_segment_cmd.get_request_id(),
            Requests::TruncateSegment(truncate_segment_cmd) => truncate_segment_cmd.get_request_id(),
            Requests::DeleteSegment(delete_segment_cmd) => delete_segment_cmd.get_request_id(),
            Requests::DeleteTableSegment(delete_table_segment_cmd) => {
                delete_table_segment_cmd.get_request_id()
            }
            Requests::KeepAlive(keep_alive_cmd) => Request::get_request_id(keep_alive_cmd),
            Requests::UpdateTableEntries(update_table_entries_cmd) => {
                update_table_entries_cmd.get_request_id()
            }
            Requests::RemoveTableKeys(remove_table_keys_cmd) => remove_table_keys_cmd.get_request_id(),
            Requests::ReadTable(read_table_cmd) => read_table_cmd.get_request_id(),
            Requests::ReadTableKeys(read_table_keys_cmd) => read_table_keys_cmd.get_request_id(),
            Requests::ReadTableEntries(read_table_entries_cmd) => read_table_entries_cmd.get_request_id(),
            Requests::ReadTableEntriesDelta(read_table_entries_delta_cmd) => {
                read_table_entries_delta_cmd.get_request_id()
            }
            Requests::ConditionalBlockEnd(conditional_block_end_cmd) => {
                conditional_block_end_cmd.get_request_id()
            }
            _ => -1,
        }
    }
}

impl Reply for Replies {
    fn get_request_id(&self) -> i64 {
        match self {
            Replies::Hello(hello_cmd) => Reply::get_request_id(hello_cmd),
            Replies::WrongHost(wrong_host_cmd) => wrong_host_cmd.get_request_id(),
            Replies::SegmentIsSealed(seg_is_sealed_cmd) => seg_is_sealed_cmd.get_request_id(),
            Replies::SegmentAlreadyExists(seg_already_exists_cmd) => seg_already_exists_cmd.get_request_id(),
            Replies::SegmentIsTruncated(seg_is_truncated_cmd) => seg_is_truncated_cmd.get_request_id(),
            Replies::NoSuchSegment(no_such_seg_cmd) => no_such_seg_cmd.get_request_id(),
            Replies::TableSegmentNotEmpty(table_seg_not_empty_cmd) => {
                table_seg_not_empty_cmd.get_request_id()
            }
            Replies::InvalidEventNumber(invalid_event_num_cmd) => invalid_event_num_cmd.get_request_id(),
            Replies::OperationUnsupported(operation_unsupported_cmd) => {
                operation_unsupported_cmd.get_request_id()
            }
            Replies::AppendSetup(append_setup_cmd) => append_setup_cmd.get_request_id(),
            Replies::DataAppended(data_appended_cmd) => data_appended_cmd.get_request_id(),
            Replies::ConditionalCheckFailed(conditional_check_failed_cmd) => {
                conditional_check_failed_cmd.get_request_id()
            }
            Replies::SegmentRead(segment_read_cmd) => segment_read_cmd.get_request_id(),
            Replies::SegmentAttribute(segment_attribute_cmd) => segment_attribute_cmd.get_request_id(),
            Replies::SegmentAttributeUpdated(segment_attribute_updated_cmd) => {
                segment_attribute_updated_cmd.get_request_id()
            }
            Replies::StreamSegmentInfo(stream_segment_info_cmd) => stream_segment_info_cmd.get_request_id(),
            Replies::SegmentCreated(segment_created_cmd) => segment_created_cmd.get_request_id(),
            Replies::SegmentPolicyUpdated(segment_policy_updated_cmd) => {
                segment_policy_updated_cmd.get_request_id()
            }
            Replies::SegmentsMerged(segments_merged_cmd) => segments_merged_cmd.get_request_id(),
            Replies::SegmentSealed(segment_sealed_cmd) => segment_sealed_cmd.get_request_id(),
            Replies::SegmentTruncated(segment_truncated_cmd) => segment_truncated_cmd.get_request_id(),
            Replies::SegmentDeleted(segment_deleted_cmd) => segment_deleted_cmd.get_request_id(),
            Replies::KeepAlive(keep_alive_cmd) => Reply::get_request_id(keep_alive_cmd),
            Replies::AuthTokenCheckFailed(auth_token_check_failed_cmd) => {
                auth_token_check_failed_cmd.get_request_id()
            }
            Replies::TableEntriesUpdated(table_entries_updated_cmd) => {
                table_entries_updated_cmd.get_request_id()
            }
            Replies::TableKeysRemoved(table_key_removed_cmd) => table_key_removed_cmd.get_request_id(),
            Replies::TableRead(table_read_cmd) => table_read_cmd.get_request_id(),
            Replies::TableKeysRead(table_keys_read_cmd) => table_keys_read_cmd.get_request_id(),
            Replies::TableEntriesRead(table_entries_read_cmd) => table_entries_read_cmd.get_request_id(),
            Replies::TableKeyDoesNotExist(table_key_does_not_exist_cmd) => {
                table_key_does_not_exist_cmd.get_request_id()
            }
            Replies::TableKeyBadVersion(table_key_bad_version_cmd) => {
                table_key_bad_version_cmd.get_request_id()
            }
            Replies::TableEntriesDeltaRead(table_entries_delta_read_cmd) => {
                table_entries_delta_read_cmd.get_request_id()
            }
        }
    }
}

pub trait Encode {
    fn write_fields(&self) -> Result<Vec<u8>, CommandError>;
}

pub trait Decode {
    type Item;
    fn read_from(raw_input: &[u8]) -> Result<Self::Item, CommandError>;
}

impl Encode for Requests {
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let mut res = Vec::new();
        match self {
            Requests::Padding(padding_command) => {
                res.extend_from_slice(&PaddingCommand::TYPE_CODE.to_be_bytes());
                let se = padding_command.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::PartialEvent(partial_event_cmd) => {
                res.extend_from_slice(&PartialEventCommand::TYPE_CODE.to_be_bytes());
                let se = partial_event_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::Event(event_cmd) => {
                res.extend_from_slice(&EventCommand::TYPE_CODE.to_be_bytes());
                let se = event_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::AppendBlock(append_block_cmd) => {
                res.extend_from_slice(&AppendBlockCommand::TYPE_CODE.to_be_bytes());
                let se = append_block_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::AppendBlockEnd(append_block_end_cmd) => {
                res.extend_from_slice(&AppendBlockEndCommand::TYPE_CODE.to_be_bytes());
                let se = append_block_end_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }

            Requests::Hello(hello_cmd) => {
                res.extend_from_slice(&HelloCommand::TYPE_CODE.to_be_bytes());
                let se = hello_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::SetupAppend(setup_append_cmd) => {
                res.extend_from_slice(&SetupAppendCommand::TYPE_CODE.to_be_bytes());
                let se = setup_append_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::ConditionalAppend(conditional_append_cmd) => {
                res.extend_from_slice(&ConditionalAppendCommand::TYPE_CODE.to_be_bytes());
                let se = conditional_append_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::ReadSegment(read_segment_cmd) => {
                res.extend_from_slice(&ReadSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = read_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::GetSegmentAttribute(get_segment_attribute_cmd) => {
                res.extend_from_slice(&GetSegmentAttributeCommand::TYPE_CODE.to_be_bytes());
                let se = get_segment_attribute_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::UpdateSegmentAttribute(update_segment_attribute_cmd) => {
                res.extend_from_slice(&UpdateSegmentAttributeCommand::TYPE_CODE.to_be_bytes());
                let se = update_segment_attribute_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::GetStreamSegmentInfo(get_stream_segment_info_cmd) => {
                res.extend_from_slice(&GetStreamSegmentInfoCommand::TYPE_CODE.to_be_bytes());
                let se = get_stream_segment_info_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::CreateSegment(create_segment_cmd) => {
                res.extend_from_slice(&CreateSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = create_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::CreateTableSegment(create_table_segment_command) => {
                res.extend_from_slice(&CreateTableSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = create_table_segment_command.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::UpdateSegmentPolicy(update_segment_policy_cmd) => {
                res.extend_from_slice(&UpdateSegmentPolicyCommand::TYPE_CODE.to_be_bytes());
                let se = update_segment_policy_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::MergeSegments(merge_segments_cmd) => {
                res.extend_from_slice(&MergeSegmentsCommand::TYPE_CODE.to_be_bytes());
                let se = merge_segments_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::MergeTableSegments(merge_table_segments_cmd) => {
                res.extend_from_slice(&MergeTableSegmentsCommand::TYPE_CODE.to_be_bytes());
                let se = merge_table_segments_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::SealSegment(seal_segment_cmd) => {
                res.extend_from_slice(&SealSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = seal_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::SealTableSegment(seal_table_segment_cmd) => {
                res.extend_from_slice(&SealTableSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = seal_table_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::TruncateSegment(truncate_segment_cmd) => {
                res.extend_from_slice(&TruncateSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = truncate_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::DeleteSegment(delete_segment_cmd) => {
                res.extend_from_slice(&DeleteSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = delete_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::DeleteTableSegment(delete_table_segment_cmd) => {
                res.extend_from_slice(&DeleteTableSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = delete_table_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::KeepAlive(keep_alive_cmd) => {
                res.extend_from_slice(&KeepAliveCommand::TYPE_CODE.to_be_bytes());
                let se = keep_alive_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::UpdateTableEntries(update_table_entries_cmd) => {
                res.extend_from_slice(&UpdateTableEntriesCommand::TYPE_CODE.to_be_bytes());
                let se = update_table_entries_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::RemoveTableKeys(remove_table_keys_cmd) => {
                res.extend_from_slice(&RemoveTableKeysCommand::TYPE_CODE.to_be_bytes());
                let se = remove_table_keys_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::ReadTable(read_table_cmd) => {
                res.extend_from_slice(&ReadTableCommand::TYPE_CODE.to_be_bytes());
                let se = read_table_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::ReadTableKeys(read_table_keys_cmd) => {
                res.extend_from_slice(&ReadTableKeysCommand::TYPE_CODE.to_be_bytes());
                let se = read_table_keys_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::ReadTableEntries(read_table_entries_cmd) => {
                res.extend_from_slice(&ReadTableEntriesCommand::TYPE_CODE.to_be_bytes());
                let se = read_table_entries_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::ReadTableEntriesDelta(read_table_entries_delta_cmd) => {
                res.extend_from_slice(&ReadTableEntriesDeltaCommand::TYPE_CODE.to_be_bytes());
                let se = read_table_entries_delta_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Requests::ConditionalBlockEnd(conditional_block_end_cmd) => {
                res.extend_from_slice(&ConditionalBlockEndCommand::TYPE_CODE.to_be_bytes());
                let se = conditional_block_end_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
        }
        Ok(res)
    }
}

impl Encode for Replies {
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let mut res = Vec::new();
        match self {
            Replies::Hello(hello_cmd) => {
                res.extend_from_slice(&HelloCommand::TYPE_CODE.to_be_bytes());
                let se = hello_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::WrongHost(wrong_host_cmd) => {
                res.extend_from_slice(&WrongHostCommand::TYPE_CODE.to_be_bytes());
                let se = wrong_host_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::SegmentIsSealed(seg_is_sealed_cmd) => {
                res.extend_from_slice(&SegmentIsSealedCommand::TYPE_CODE.to_be_bytes());
                let se = seg_is_sealed_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::SegmentAlreadyExists(seg_already_exists_cmd) => {
                res.extend_from_slice(&SegmentAlreadyExistsCommand::TYPE_CODE.to_be_bytes());
                let se = seg_already_exists_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::SegmentIsTruncated(seg_is_truncated_cmd) => {
                res.extend_from_slice(&SegmentIsTruncatedCommand::TYPE_CODE.to_be_bytes());
                let se = seg_is_truncated_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::NoSuchSegment(no_such_seg_cmd) => {
                res.extend_from_slice(&NoSuchSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = no_such_seg_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::TableSegmentNotEmpty(table_seg_not_empty_cmd) => {
                res.extend_from_slice(&TableSegmentNotEmptyCommand::TYPE_CODE.to_be_bytes());
                let se = table_seg_not_empty_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::InvalidEventNumber(invalid_event_num_cmd) => {
                res.extend_from_slice(&InvalidEventNumberCommand::TYPE_CODE.to_be_bytes());
                let se = invalid_event_num_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::OperationUnsupported(operation_unsupported_cmd) => {
                res.extend_from_slice(&OperationUnsupportedCommand::TYPE_CODE.to_be_bytes());
                let se = operation_unsupported_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::AppendSetup(append_setup_cmd) => {
                res.extend_from_slice(&AppendSetupCommand::TYPE_CODE.to_be_bytes());
                let se = append_setup_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::DataAppended(data_appended_cmd) => {
                res.extend_from_slice(&DataAppendedCommand::TYPE_CODE.to_be_bytes());
                let se = data_appended_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::ConditionalCheckFailed(conditional_check_failed_cmd) => {
                res.extend_from_slice(&ConditionalCheckFailedCommand::TYPE_CODE.to_be_bytes());
                let se = conditional_check_failed_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::SegmentRead(segment_read_cmd) => {
                res.extend_from_slice(&SegmentReadCommand::TYPE_CODE.to_be_bytes());
                let se = segment_read_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::SegmentAttribute(segment_attribute_cmd) => {
                res.extend_from_slice(&SegmentAttributeCommand::TYPE_CODE.to_be_bytes());
                let se = segment_attribute_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::SegmentAttributeUpdated(segment_attribute_updated_cmd) => {
                res.extend_from_slice(&SegmentAttributeUpdatedCommand::TYPE_CODE.to_be_bytes());
                let se = segment_attribute_updated_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::StreamSegmentInfo(stream_segment_info_cmd) => {
                res.extend_from_slice(&StreamSegmentInfoCommand::TYPE_CODE.to_be_bytes());
                let se = stream_segment_info_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::SegmentCreated(segment_created_cmd) => {
                res.extend_from_slice(&SegmentCreatedCommand::TYPE_CODE.to_be_bytes());
                let se = segment_created_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::SegmentPolicyUpdated(segment_policy_updated_cmd) => {
                res.extend_from_slice(&SegmentPolicyUpdatedCommand::TYPE_CODE.to_be_bytes());
                let se = segment_policy_updated_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se)
            }
            Replies::SegmentsMerged(segments_merged_cmd) => {
                res.extend_from_slice(&SegmentsMergedCommand::TYPE_CODE.to_be_bytes());
                let se = segments_merged_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::SegmentSealed(segment_sealed_cmd) => {
                res.extend_from_slice(&SegmentSealedCommand::TYPE_CODE.to_be_bytes());
                let se = segment_sealed_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::SegmentTruncated(segment_truncated_cmd) => {
                res.extend_from_slice(&SegmentTruncatedCommand::TYPE_CODE.to_be_bytes());
                let se = segment_truncated_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::SegmentDeleted(segment_deleted_cmd) => {
                res.extend_from_slice(&SegmentDeletedCommand::TYPE_CODE.to_be_bytes());
                let se = segment_deleted_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::KeepAlive(keep_alive_cmd) => {
                res.extend_from_slice(&KeepAliveCommand::TYPE_CODE.to_be_bytes());
                let se = keep_alive_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::AuthTokenCheckFailed(auth_token_check_failed_cmd) => {
                res.extend_from_slice(&AuthTokenCheckFailedCommand::TYPE_CODE.to_be_bytes());
                let se = auth_token_check_failed_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::TableEntriesUpdated(table_entries_updated_cmd) => {
                res.extend_from_slice(&TableEntriesUpdatedCommand::TYPE_CODE.to_be_bytes());
                let se = table_entries_updated_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::TableKeysRemoved(table_key_removed_cmd) => {
                res.extend_from_slice(&TableKeysRemovedCommand::TYPE_CODE.to_be_bytes());
                let se = table_key_removed_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::TableRead(table_read_cmd) => {
                res.extend_from_slice(&TableReadCommand::TYPE_CODE.to_be_bytes());
                let se = table_read_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::TableKeysRead(table_keys_read_cmd) => {
                res.extend_from_slice(&TableKeysReadCommand::TYPE_CODE.to_be_bytes());
                let se = table_keys_read_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::TableEntriesRead(table_entries_read_cmd) => {
                res.extend_from_slice(&TableEntriesReadCommand::TYPE_CODE.to_be_bytes());
                let se = table_entries_read_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::TableKeyDoesNotExist(table_key_does_not_exist_cmd) => {
                res.extend_from_slice(&TableKeyDoesNotExistCommand::TYPE_CODE.to_be_bytes());
                let se = table_key_does_not_exist_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::TableKeyBadVersion(table_key_bad_version_cmd) => {
                res.extend_from_slice(&TableKeyBadVersionCommand::TYPE_CODE.to_be_bytes());
                let se = table_key_bad_version_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            Replies::TableEntriesDeltaRead(table_entries_delta_read_cmd) => {
                res.extend_from_slice(&TableEntriesDeltaReadCommand::TYPE_CODE.to_be_bytes());
                let se = table_entries_delta_read_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
        }
        Ok(res)
    }
}

impl Encode for WireCommands {
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let res = match self {
            WireCommands::Requests(request) => request.write_fields()?,
            WireCommands::Replies(reply) => reply.write_fields()?,
        };
        Ok(res)
    }
}

impl Decode for Requests {
    type Item = Requests;
    fn read_from(raw_input: &[u8]) -> Result<Self::Item, CommandError> {
        let type_code = BigEndian::read_i32(raw_input);
        let _length = BigEndian::read_i32(&raw_input[4..]);
        let input = &raw_input[8..];
        match type_code {
            HelloCommand::TYPE_CODE => Ok(Requests::Hello(HelloCommand::read_from(input)?)),
            SetupAppendCommand::TYPE_CODE => Ok(Requests::SetupAppend(SetupAppendCommand::read_from(input)?)),
            ConditionalAppendCommand::TYPE_CODE => Ok(Requests::ConditionalAppend(
                ConditionalAppendCommand::read_from(input)?,
            )),
            ReadSegmentCommand::TYPE_CODE => Ok(Requests::ReadSegment(ReadSegmentCommand::read_from(input)?)),
            GetSegmentAttributeCommand::TYPE_CODE => Ok(Requests::GetSegmentAttribute(
                GetSegmentAttributeCommand::read_from(input)?,
            )),
            UpdateSegmentAttributeCommand::TYPE_CODE => Ok(Requests::UpdateSegmentAttribute(
                UpdateSegmentAttributeCommand::read_from(input)?,
            )),
            GetStreamSegmentInfoCommand::TYPE_CODE => Ok(Requests::GetStreamSegmentInfo(
                GetStreamSegmentInfoCommand::read_from(input)?,
            )),
            CreateSegmentCommand::TYPE_CODE => {
                Ok(Requests::CreateSegment(CreateSegmentCommand::read_from(input)?))
            }
            CreateTableSegmentCommand::TYPE_CODE => Ok(Requests::CreateTableSegment(
                CreateTableSegmentCommand::read_from(input)?,
            )),
            UpdateSegmentPolicyCommand::TYPE_CODE => Ok(Requests::UpdateSegmentPolicy(
                UpdateSegmentPolicyCommand::read_from(input)?,
            )),
            MergeSegmentsCommand::TYPE_CODE => {
                Ok(Requests::MergeSegments(MergeSegmentsCommand::read_from(input)?))
            }
            MergeTableSegmentsCommand::TYPE_CODE => Ok(Requests::MergeTableSegments(
                MergeTableSegmentsCommand::read_from(input)?,
            )),
            SealSegmentCommand::TYPE_CODE => Ok(Requests::SealSegment(SealSegmentCommand::read_from(input)?)),
            SealTableSegmentCommand::TYPE_CODE => Ok(Requests::SealTableSegment(
                SealTableSegmentCommand::read_from(input)?,
            )),
            TruncateSegmentCommand::TYPE_CODE => Ok(Requests::TruncateSegment(
                TruncateSegmentCommand::read_from(input)?,
            )),
            DeleteSegmentCommand::TYPE_CODE => {
                Ok(Requests::DeleteSegment(DeleteSegmentCommand::read_from(input)?))
            }
            DeleteTableSegmentCommand::TYPE_CODE => Ok(Requests::DeleteTableSegment(
                DeleteTableSegmentCommand::read_from(input)?,
            )),
            KeepAliveCommand::TYPE_CODE => Ok(Requests::KeepAlive(KeepAliveCommand::read_from(input)?)),
            UpdateTableEntriesCommand::TYPE_CODE => Ok(Requests::UpdateTableEntries(
                UpdateTableEntriesCommand::read_from(input)?,
            )),
            RemoveTableKeysCommand::TYPE_CODE => Ok(Requests::RemoveTableKeys(
                RemoveTableKeysCommand::read_from(input)?,
            )),
            ReadTableCommand::TYPE_CODE => Ok(Requests::ReadTable(ReadTableCommand::read_from(input)?)),
            ReadTableKeysCommand::TYPE_CODE => {
                Ok(Requests::ReadTableKeys(ReadTableKeysCommand::read_from(input)?))
            }
            ReadTableEntriesCommand::TYPE_CODE => Ok(Requests::ReadTableEntries(
                ReadTableEntriesCommand::read_from(input)?,
            )),
            ReadTableEntriesDeltaCommand::TYPE_CODE => Ok(Requests::ReadTableEntriesDelta(
                ReadTableEntriesDeltaCommand::read_from(input)?,
            )),

            AppendBlockCommand::TYPE_CODE => Ok(Requests::AppendBlock(AppendBlockCommand::read_from(input)?)),

            AppendBlockEndCommand::TYPE_CODE => {
                Ok(Requests::AppendBlockEnd(AppendBlockEndCommand::read_from(input)?))
            }

            PaddingCommand::TYPE_CODE => Ok(Requests::Padding(PaddingCommand::read_from(input)?)),

            PartialEventCommand::TYPE_CODE => {
                Ok(Requests::PartialEvent(PartialEventCommand::read_from(input)?))
            }

            EventCommand::TYPE_CODE => Ok(Requests::Event(EventCommand::read_from(input)?)),

            ConditionalBlockEndCommand::TYPE_CODE => Ok(Requests::ConditionalBlockEnd(
                ConditionalBlockEndCommand::read_from(input)?,
            )),

            _ => InvalidType {
                command_type: type_code,
            }
            .fail(),
        }
    }
}

impl Decode for Replies {
    type Item = Replies;
    fn read_from(raw_input: &[u8]) -> Result<Self::Item, CommandError> {
        let type_code = BigEndian::read_i32(raw_input);
        let _length = BigEndian::read_i32(&raw_input[4..]);
        let input = &raw_input[8..];
        match type_code {
            HelloCommand::TYPE_CODE => Ok(Replies::Hello(HelloCommand::read_from(input)?)),
            WrongHostCommand::TYPE_CODE => Ok(Replies::WrongHost(WrongHostCommand::read_from(input)?)),
            SegmentIsSealedCommand::TYPE_CODE => Ok(Replies::SegmentIsSealed(
                SegmentIsSealedCommand::read_from(input)?,
            )),
            SegmentAlreadyExistsCommand::TYPE_CODE => Ok(Replies::SegmentAlreadyExists(
                SegmentAlreadyExistsCommand::read_from(input)?,
            )),
            SegmentIsTruncatedCommand::TYPE_CODE => Ok(Replies::SegmentIsTruncated(
                SegmentIsTruncatedCommand::read_from(input)?,
            )),
            NoSuchSegmentCommand::TYPE_CODE => {
                Ok(Replies::NoSuchSegment(NoSuchSegmentCommand::read_from(input)?))
            }
            TableSegmentNotEmptyCommand::TYPE_CODE => Ok(Replies::TableSegmentNotEmpty(
                TableSegmentNotEmptyCommand::read_from(input)?,
            )),
            InvalidEventNumberCommand::TYPE_CODE => Ok(Replies::InvalidEventNumber(
                InvalidEventNumberCommand::read_from(input)?,
            )),
            OperationUnsupportedCommand::TYPE_CODE => Ok(Replies::OperationUnsupported(
                OperationUnsupportedCommand::read_from(input)?,
            )),
            AppendSetupCommand::TYPE_CODE => Ok(Replies::AppendSetup(AppendSetupCommand::read_from(input)?)),
            DataAppendedCommand::TYPE_CODE => {
                Ok(Replies::DataAppended(DataAppendedCommand::read_from(input)?))
            }
            ConditionalCheckFailedCommand::TYPE_CODE => Ok(Replies::ConditionalCheckFailed(
                ConditionalCheckFailedCommand::read_from(input)?,
            )),
            SegmentReadCommand::TYPE_CODE => Ok(Replies::SegmentRead(SegmentReadCommand::read_from(input)?)),
            SegmentAttributeCommand::TYPE_CODE => Ok(Replies::SegmentAttribute(
                SegmentAttributeCommand::read_from(input)?,
            )),
            SegmentAttributeUpdatedCommand::TYPE_CODE => Ok(Replies::SegmentAttributeUpdated(
                SegmentAttributeUpdatedCommand::read_from(input)?,
            )),
            StreamSegmentInfoCommand::TYPE_CODE => Ok(Replies::StreamSegmentInfo(
                StreamSegmentInfoCommand::read_from(input)?,
            )),
            SegmentCreatedCommand::TYPE_CODE => {
                Ok(Replies::SegmentCreated(SegmentCreatedCommand::read_from(input)?))
            }
            SegmentPolicyUpdatedCommand::TYPE_CODE => Ok(Replies::SegmentPolicyUpdated(
                SegmentPolicyUpdatedCommand::read_from(input)?,
            )),
            SegmentsMergedCommand::TYPE_CODE => {
                Ok(Replies::SegmentsMerged(SegmentsMergedCommand::read_from(input)?))
            }
            SegmentSealedCommand::TYPE_CODE => {
                Ok(Replies::SegmentSealed(SegmentSealedCommand::read_from(input)?))
            }
            SegmentTruncatedCommand::TYPE_CODE => Ok(Replies::SegmentTruncated(
                SegmentTruncatedCommand::read_from(input)?,
            )),
            SegmentDeletedCommand::TYPE_CODE => {
                Ok(Replies::SegmentDeleted(SegmentDeletedCommand::read_from(input)?))
            }
            KeepAliveCommand::TYPE_CODE => Ok(Replies::KeepAlive(KeepAliveCommand::read_from(input)?)),
            AuthTokenCheckFailedCommand::TYPE_CODE => Ok(Replies::AuthTokenCheckFailed(
                AuthTokenCheckFailedCommand::read_from(input)?,
            )),
            TableEntriesUpdatedCommand::TYPE_CODE => Ok(Replies::TableEntriesUpdated(
                TableEntriesUpdatedCommand::read_from(input)?,
            )),
            TableKeysRemovedCommand::TYPE_CODE => Ok(Replies::TableKeysRemoved(
                TableKeysRemovedCommand::read_from(input)?,
            )),
            TableReadCommand::TYPE_CODE => Ok(Replies::TableRead(TableReadCommand::read_from(input)?)),
            TableKeysReadCommand::TYPE_CODE => {
                Ok(Replies::TableKeysRead(TableKeysReadCommand::read_from(input)?))
            }
            TableEntriesReadCommand::TYPE_CODE => Ok(Replies::TableEntriesRead(
                TableEntriesReadCommand::read_from(input)?,
            )),
            TableKeyDoesNotExistCommand::TYPE_CODE => Ok(Replies::TableKeyDoesNotExist(
                TableKeyDoesNotExistCommand::read_from(input)?,
            )),
            TableKeyBadVersionCommand::TYPE_CODE => Ok(Replies::TableKeyBadVersion(
                TableKeyBadVersionCommand::read_from(input)?,
            )),
            TableEntriesDeltaReadCommand::TYPE_CODE => Ok(Replies::TableEntriesDeltaRead(
                TableEntriesDeltaReadCommand::read_from(input)?,
            )),
            _ => InvalidType {
                command_type: type_code,
            }
            .fail(),
        }
    }
}

impl Decode for WireCommands {
    type Item = WireCommands;
    fn read_from(raw_input: &[u8]) -> Result<Self::Item, CommandError> {
        let type_code = BigEndian::read_i32(raw_input);
        if let Ok(r) = Replies::read_from(raw_input) {
            Ok(WireCommands::Replies(r))
        } else if let Ok(r) = Requests::read_from(raw_input) {
            Ok(WireCommands::Requests(r))
        } else {
            InvalidType {
                command_type: type_code,
            }
            .fail()
        }
    }
}
