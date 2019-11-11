use crate::commands::*;
use byteorder::{ByteOrder, BigEndian, WriteBytesExt};

#[derive(PartialEq, Debug)]
pub enum  WireCommands {
    Hello(HelloCommand),
    WrongHost(WrongHostCommand),
    SegmentIsSealed(SegmentIsSealedCommand),
    SegmentAlreadyExists(SegmentAlreadyExistsCommand),
    SegmentIsTruncated(SegmentIsTruncatedCommand),
    NoSuchSegment(NoSuchSegmentCommand),
    TableSegmentNotEmpty(TableSegmentNotEmptyCommand),
    InvalidEventNumber(InvalidEventNumberCommand),
    OperationUnsupported(OperationUnsupportedCommand),
    Padding(PaddingCommand),
    PartialEvent(PartialEventCommand),
    Event(EventCommand),
    SetupAppend(SetupAppendCommand),
    AppendBlock(AppendBlockCommand),
    AppendBlockEnd(AppendBlockEndCommand),
    ConditionalAppend(ConditionalAppendCommand),
    AppendSetup(AppendSetupCommand),
    DataAppended(DataAppendedCommand),
    ConditionalCheckFailed(ConditionalCheckFailedCommand),
    ReadSegment(ReadSegmentCommand),
    SegmentRead(SegmentReadCommand),
    GetSegmentAttribute(GetSegmentAttributeCommand),
    SegmentAttribute(SegmentAttributeCommand),
    UpdateSegmentAttribute(UpdateSegmentAttributeCommand),
    SegmentAttributeUpdated(SegmentAttributeUpdatedCommand),
    GetStreamSegmentInfo(GetStreamSegmentInfoCommand),
    StreamSegmentInfo(StreamSegmentInfoCommand),
    CreateSegment(CreateSegmentCommand),
    CreateTableSegment(CreateTableSegmentCommand),
    SegmentCreated(SegmentCreatedCommand),
    UpdateSegmentPolicy(UpdateSegmentPolicyCommand),
    SegmentPolicyUpdated(SegmentPolicyUpdatedCommand),
    MergeSegments(MergeSegmentsCommand),
    MergeTableSegments(MergeTableSegmentsCommand),
    SegmentsMerged(SegmentsMergedCommand),
    SealSegment(SealSegmentCommand),
    SealTableSegment(SealTableSegmentCommand),
    SegmentSealed(SegmentSealedCommand),
    TruncateSegment(TruncateSegmentCommand),
    SegmentTruncated(SegmentTruncatedCommand),
    DeleteSegment(DeleteSegmentCommand),
    DeleteTableSegment(DeleteTableSegmentCommand),
    SegmentDeleted(SegmentDeletedCommand),
    KeepAlive(KeepAliveCommand),
    AuthTokenCheckFailed(AuthTokenCheckFailedCommand),
    UpdateTableEntries(UpdateTableEntriesCommand),
    TableEntriesUpdated(TableEntriesUpdatedCommand),
    RemoveTableKeys(RemoveTableKeysCommand),
    TableKeysRemoved(TableKeysRemovedCommand),
    ReadTable(ReadTableCommand),
    TableRead(TableReadCommand),
    ReadTableKeys(ReadTableKeysCommand),
    TableKeysRead(TableKeysReadCommand),
    ReadTableEntries(ReadTableEntriesCommand),
    TableEntriesRead(TableEntriesReadCommand),
    TableKeyDoesNotExist(TableKeyDoesNotExistCommand),
    TableKeyBadVersion(TableKeyBadVersionCommand),
}

pub trait Encode {
    fn write_fields(&self) -> Vec<u8>;
}

pub trait Decode {
    fn read_from(raw_input: &Vec<u8>) -> WireCommands;
}


impl Encode for WireCommands {
    fn write_fields(&self) -> Vec<u8> {
        let mut res = Vec::new();
        match self {
            WireCommands::Hello(hello_cmd) => {
                res.write_i32::<BigEndian>(HelloCommand::TYPE_CODE).unwrap();
                res.extend(hello_cmd.write_fields());
            },
            WireCommands::WrongHost(wrong_host_cmd) => {
                res.write_i32::<BigEndian>(WrongHostCommand::TYPE_CODE).unwrap();
                res.extend(wrong_host_cmd.write_fields());
            },
            WireCommands::SegmentIsSealed(seg_is_sealed_cmd) => {
                res.write_i32::<BigEndian>(SegmentIsSealedCommand::TYPE_CODE).unwrap();
                res.extend(seg_is_sealed_cmd.write_fields());
            },
            WireCommands::SegmentAlreadyExists(seg_already_exists_cmd) => {
                res.write_i32::<BigEndian>(SegmentAlreadyExistsCommand::TYPE_CODE).unwrap();
                res.extend(seg_already_exists_cmd.write_fields());
            },
            WireCommands::SegmentIsTruncated(seg_is_truncated_cmd) => {
                res.write_i32::<BigEndian>(SegmentIsTruncatedCommand::TYPE_CODE).unwrap();
                res.extend(seg_is_truncated_cmd.write_fields());
            },
            WireCommands::NoSuchSegment(no_such_seg_cmd) =>{
                res.write_i32::<BigEndian>(NoSuchSegmentCommand::TYPE_CODE).unwrap();
                res.extend(no_such_seg_cmd.write_fields());
            },
            WireCommands::TableSegmentNotEmpty(table_seg_not_empty_cmd) => {
                res.write_i32::<BigEndian>(TableSegmentNotEmptyCommand::TYPE_CODE).unwrap();
                res.extend(table_seg_not_empty_cmd.write_fields());
            },
            WireCommands::InvalidEventNumber(invalid_event_num_cmd) => {
                res.write_i32::<BigEndian>(InvalidEventNumberCommand::TYPE_CODE).unwrap();
                res.extend(invalid_event_num_cmd.write_fields());
            },
            WireCommands::OperationUnsupported(operation_unsupported_cmd) => {
                res.write_i32::<BigEndian>(OperationUnsupportedCommand::TYPE_CODE).unwrap();
                res.extend(operation_unsupported_cmd.write_fields());
            },
            WireCommands::Padding(paddding_command) => {
                res.write_i32::<BigEndian>(PaddingCommand::TYPE_CODE).unwrap();
                res.extend(paddding_command.write_fields());
            },
            WireCommands::PartialEvent(partial_event_cmd) => {
                res.write_i32::<BigEndian>(PartialEventCommand::TYPE_CODE).unwrap();
                res.extend(partial_event_cmd.write_fields());
            },
            WireCommands::Event(event_cmd) => {
                res.write_i32::<BigEndian>(EventCommand::TYPE_CODE).unwrap();
                res.extend(event_cmd.write_fields());
            },
            WireCommands::SetupAppend(setup_append_cmd) => {
                res.write_i32::<BigEndian>(SetupAppendCommand::TYPE_CODE).unwrap();
                res.extend(setup_append_cmd.write_fields());
            },
            WireCommands::AppendBlock(append_block_cmd) => {
                res.write_i32::<BigEndian>(AppendBlockCommand::TYPE_CODE).unwrap();
                res.extend(append_block_cmd.write_fields());
            },
            WireCommands::AppendBlockEnd(append_block_end_cmd) => {
                res.write_i32::<BigEndian>(AppendBlockCommand::TYPE_CODE).unwrap();
                res.extend(append_block_end_cmd.write_fields());
            },
            WireCommands::ConditionalAppend(conditional_append_cmd) => {
                res.write_i32::<BigEndian>(ConditionalAppendCommand::TYPE_CODE).unwrap();
                res.extend(conditional_append_cmd.write_fields());
            },
            WireCommands::AppendSetup(append_setup_cmd) => {
                res.write_i32::<BigEndian>(AppendSetupCommand::TYPE_CODE).unwrap();
                res.extend(append_setup_cmd.write_fields());
            },
            WireCommands::DataAppended(data_appended_cmd) => {
                res.write_i32::<BigEndian>(DataAppendedCommand::TYPE_CODE).unwrap();
                res.extend(data_appended_cmd.write_fields());
            },
            WireCommands::ConditionalCheckFailed(conditional_check_failed_cmd) => {
                res.write_i32::<BigEndian>(ConditionalCheckFailedCommand::TYPE_CODE).unwrap();
                res.extend(conditional_check_failed_cmd.write_fields());
            },
            WireCommands::ReadSegment(read_segment_cmd) => {
                res.write_i32::<BigEndian>(ReadSegmentCommand::TYPE_CODE).unwrap();
                res.extend(read_segment_cmd.write_fields());
            },
            WireCommands::SegmentRead(segment_read_cmd) => {
                res.write_i32::<BigEndian>(SegmentReadCommand::TYPE_CODE).unwrap();
                res.extend(segment_read_cmd.write_fields());
            },
            WireCommands::GetSegmentAttribute(get_segment_attribute_cmd) => {
                res.write_i32::<BigEndian>(GetSegmentAttributeCommand::TYPE_CODE).unwrap();
                res.extend(get_segment_attribute_cmd.write_fields());
            },
            WireCommands::SegmentAttribute(segment_attribute_cmd) => {
                res.write_i32::<BigEndian>(SegmentAttributeCommand::TYPE_CODE).unwrap();
                res.extend(segment_attribute_cmd.write_fields());
            },
            WireCommands::UpdateSegmentAttribute(update_segment_attribute_cmd) => {
                res.write_i32::<BigEndian>(UpdateSegmentAttributeCommand::TYPE_CODE).unwrap();
                res.extend(update_segment_attribute_cmd.write_fields());
            },
            WireCommands::SegmentAttributeUpdated(segment_attribute_updated_cmd) => {
                res.write_i32::<BigEndian>(SegmentAttributeUpdatedCommand::TYPE_CODE).unwrap();
                res.extend(segment_attribute_updated_cmd.write_fields());
            },
            WireCommands::GetStreamSegmentInfo(get_stream_segment_info_cmd) => {
                res.write_i32::<BigEndian>(GetStreamSegmentInfoCommand::TYPE_CODE).unwrap();
                res.extend(get_stream_segment_info_cmd.write_fields());
            },
            WireCommands::StreamSegmentInfo(stream_segment_info_cmd) => {
                res.write_i32::<BigEndian>(StreamSegmentInfoCommand::TYPE_CODE).unwrap();
                res.extend(stream_segment_info_cmd.write_fields());
            },
            WireCommands::CreateSegment(create_segment_cmd) => {
                res.write_i32::<BigEndian>(CreateSegmentCommand::TYPE_CODE).unwrap();
                res.extend(create_segment_cmd.write_fields());
            },
            WireCommands::CreateTableSegment(create_table_segment_command) => {
                res.write_i32::<BigEndian>(CreateTableSegmentCommand::TYPE_CODE).unwrap();
                res.extend(create_table_segment_command.write_fields());
            },
            WireCommands::SegmentCreated(segment_created_cmd) => {
                res.write_i32::<BigEndian>(SegmentCreatedCommand::TYPE_CODE).unwrap();
                res.extend(segment_created_cmd.write_fields());
            },
            WireCommands::UpdateSegmentPolicy(update_segment_policy_cmd) => {
                res.write_i32::<BigEndian>(UpdateSegmentPolicyCommand::TYPE_CODE).unwrap();
                res.extend(update_segment_policy_cmd.write_fields());
            },
            WireCommands::SegmentPolicyUpdated(segment_policy_updated_cmd) => {
                res.write_i32::<BigEndian>(SegmentPolicyUpdatedCommand::TYPE_CODE).unwrap();
                res.extend(segment_policy_updated_cmd.write_fields());
            },
            WireCommands::MergeSegments(merge_segments_cmd) => {
                res.write_i32::<BigEndian>(MergeSegmentsCommand::TYPE_CODE).unwrap();
                res.extend(merge_segments_cmd.write_fields());
            },
            WireCommands::MergeTableSegments(merge_table_segments_cmd) => {
                res.write_i32::<BigEndian>(MergeTableSegmentsCommand::TYPE_CODE).unwrap();
                res.extend(merge_table_segments_cmd.write_fields());
            },
            WireCommands::SegmentsMerged(segments_merged_cmd) => {
                res.write_i32::<BigEndian>(SegmentsMergedCommand::TYPE_CODE).unwrap();
                res.extend(segments_merged_cmd.write_fields());
            },
            WireCommands::SealSegment(seal_segment_cmd) => {
                res.write_i32::<BigEndian>(SealSegmentCommand::TYPE_CODE).unwrap();
                res.extend(seal_segment_cmd.write_fields());
            },
            WireCommands::SealTableSegment(seal_table_segment_cmd) => {
                res.write_i32::<BigEndian>(SealTableSegmentCommand::TYPE_CODE).unwrap();
                res.extend(seal_table_segment_cmd.write_fields());
            },
            WireCommands::SegmentSealed(segment_sealed_cmd) => {
                res.write_i32::<BigEndian>(SegmentSealedCommand::TYPE_CODE).unwrap();
                res.extend(segment_sealed_cmd.write_fields());
            },
            WireCommands::TruncateSegment(truncate_segment_cmd) => {
                res.write_i32::<BigEndian>(TruncateSegmentCommand::TYPE_CODE).unwrap();
                res.extend(truncate_segment_cmd.write_fields());
            },
            WireCommands::SegmentTruncated(segment_truncated_cmd) => {
                res.write_i32::<BigEndian>(SegmentTruncatedCommand::TYPE_CODE).unwrap();
                res.extend(segment_truncated_cmd.write_fields());
            },
            WireCommands::DeleteSegment(delete_segment_cmd) => {
                res.write_i32::<BigEndian>(DeleteSegmentCommand::TYPE_CODE).unwrap();
                res.extend(delete_segment_cmd.write_fields());
            },
            WireCommands::DeleteTableSegment(delete_table_segment_cmd) => {
                res.write_i32::<BigEndian>(DeleteTableSegmentCommand::TYPE_CODE).unwrap();
                res.extend(delete_table_segment_cmd.write_fields());
            },
            WireCommands::SegmentDeleted(segment_deleted_cmd) => {
                res.write_i32::<BigEndian>(SegmentDeletedCommand::TYPE_CODE).unwrap();
                res.extend(segment_deleted_cmd.write_fields());
            },
            WireCommands::KeepAlive(keep_alive_cmd) => {
                res.write_i32::<BigEndian>(KeepAliveCommand::TYPE_CODE).unwrap();
                res.extend(keep_alive_cmd.write_fields());
            },
            WireCommands::AuthTokenCheckFailed(auth_token_check_failed_cmd) => {
                res.write_i32::<BigEndian>(AuthTokenCheckFailedCommand::TYPE_CODE).unwrap();
                res.extend(auth_token_check_failed_cmd.write_fields());
            },
            WireCommands::UpdateTableEntries(update_table_entries_cmd) => {
                res.write_i32::<BigEndian>(UpdateTableEntriesCommand::TYPE_CODE).unwrap();
                res.extend(update_table_entries_cmd.write_fields());
            },
            WireCommands::TableEntriesUpdated(table_entries_updated_cmd) => {
                res.write_i32::<BigEndian>(TableEntriesUpdatedCommand::TYPE_CODE).unwrap();
                res.extend(table_entries_updated_cmd.write_fields());
            },
            WireCommands::RemoveTableKeys(remove_table_keys_cmd) => {
                res.write_i32::<BigEndian>(RemoveTableKeysCommand::TYPE_CODE).unwrap();
                res.extend(remove_table_keys_cmd.write_fields());
            },
            WireCommands::TableKeysRemoved(table_key_removed_cmd) => {
                res.write_i32::<BigEndian>(AppendBlockCommand::TYPE_CODE).unwrap();
                res.extend(table_key_removed_cmd.write_fields());
            },
            WireCommands::ReadTable(read_table_cmd) => {
                res.write_i32::<BigEndian>(ReadTableCommand::TYPE_CODE).unwrap();
                res.extend(read_table_cmd.write_fields());
            },
            WireCommands::TableRead(table_read_cmd) => {
                res.write_i32::<BigEndian>(TableReadCommand::TYPE_CODE).unwrap();
                res.extend(table_read_cmd.write_fields());
            },
            WireCommands::ReadTableKeys(read_table_keys_cmd) => {
                res.write_i32::<BigEndian>(ReadTableKeysCommand::TYPE_CODE).unwrap();
                res.extend(read_table_keys_cmd.write_fields());
            },
            WireCommands::TableKeysRead(table_keys_read_cmd) => {
                res.write_i32::<BigEndian>(TableKeysReadCommand::TYPE_CODE).unwrap();
                res.extend(table_keys_read_cmd.write_fields());
            },
            WireCommands::ReadTableEntries(read_table_entries_cmd) => {
                res.write_i32::<BigEndian>(AppendBlockCommand::TYPE_CODE).unwrap();
                res.extend(read_table_entries_cmd.write_fields());
            },
            WireCommands::TableEntriesRead(table_entries_read_cmd) => {
                res.write_i32::<BigEndian>(TableEntriesReadCommand::TYPE_CODE).unwrap();
                res.extend(table_entries_read_cmd.write_fields());
            },
            WireCommands::TableKeyDoesNotExist(table_key_does_not_exist_cmd) => {
                res.write_i32::<BigEndian>(TableKeyDoesNotExistCommand::TYPE_CODE).unwrap();
                res.extend(table_key_does_not_exist_cmd.write_fields());
            },
            WireCommands::TableKeyBadVersion(table_key_bad_version_cmd) => {
                res.write_i32::<BigEndian>(TableKeyBadVersionCommand::TYPE_CODE).unwrap();
                res.extend(table_key_bad_version_cmd.write_fields());
            },
        }
        res
    }
}

impl Decode for WireCommands {
    fn read_from(raw_input: &Vec<u8>) -> WireCommands {
        let type_code = BigEndian::read_i32(raw_input);
        let input =  &raw_input[4..];
        match type_code {
            HelloCommand::TYPE_CODE => WireCommands::Hello(HelloCommand::read_from(input)),
            WrongHostCommand::TYPE_CODE => WireCommands::WrongHost(WrongHostCommand::read_from(input)),
            SegmentIsSealedCommand::TYPE_CODE => WireCommands::SegmentIsSealed(SegmentIsSealedCommand::read_from(input)),
            SegmentAlreadyExistsCommand::TYPE_CODE => WireCommands::SegmentAlreadyExists(SegmentAlreadyExistsCommand::read_from(input)),
            SegmentIsTruncatedCommand::TYPE_CODE => WireCommands::SegmentIsTruncated(SegmentIsTruncatedCommand::read_from(input)),
            NoSuchSegmentCommand::TYPE_CODE => WireCommands::NoSuchSegment(NoSuchSegmentCommand::read_from(input)),
            TableSegmentNotEmptyCommand::TYPE_CODE => WireCommands::TableSegmentNotEmpty(TableSegmentNotEmptyCommand::read_from(input)),
            InvalidEventNumberCommand::TYPE_CODE => WireCommands::InvalidEventNumber(InvalidEventNumberCommand::read_from(input)),
            OperationUnsupportedCommand::TYPE_CODE => WireCommands::OperationUnsupported(OperationUnsupportedCommand::read_from(input)),
            PaddingCommand::TYPE_CODE => WireCommands::Padding(PaddingCommand::read_from(input)),
            PartialEventCommand::TYPE_CODE => WireCommands::PartialEvent(PartialEventCommand::read_from(input)),
            EventCommand::TYPE_CODE => WireCommands::Event(EventCommand::read_from(input)),
            SetupAppendCommand::TYPE_CODE => WireCommands::SetupAppend(SetupAppendCommand::read_from(input)),
            _ => panic!("Wrong input")
        }
    }
}