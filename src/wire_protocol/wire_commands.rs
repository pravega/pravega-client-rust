use super::commands::*;
use super::error::CommandError;
use byteorder::{BigEndian, ByteOrder};

#[derive(PartialEq, Debug)]
pub enum WireCommands {
    UnknownCommand,
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
    fn write_fields(&self) -> Result<Vec<u8>, CommandError>;
}

pub trait Decode {
    fn read_from(raw_input: &Vec<u8>) -> Result<WireCommands, CommandError>;
}

impl Encode for WireCommands {
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let mut res = Vec::new();
        match self {
            WireCommands::Hello(hello_cmd) => {
                res.extend_from_slice(&HelloCommand::TYPE_CODE.to_be_bytes());
                let se = hello_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::WrongHost(wrong_host_cmd) => {
                res.extend_from_slice(&WrongHostCommand::TYPE_CODE.to_be_bytes());
                let se = wrong_host_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SegmentIsSealed(seg_is_sealed_cmd) => {
                res.extend_from_slice(&SegmentIsSealedCommand::TYPE_CODE.to_be_bytes());
                let se = seg_is_sealed_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SegmentAlreadyExists(seg_already_exists_cmd) => {
                res.extend_from_slice(&SegmentAlreadyExistsCommand::TYPE_CODE.to_be_bytes());
                let se = seg_already_exists_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SegmentIsTruncated(seg_is_truncated_cmd) => {
                res.extend_from_slice(&SegmentIsTruncatedCommand::TYPE_CODE.to_be_bytes());
                let se = seg_is_truncated_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::NoSuchSegment(no_such_seg_cmd) => {
                res.extend_from_slice(&NoSuchSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = no_such_seg_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::TableSegmentNotEmpty(table_seg_not_empty_cmd) => {
                res.extend_from_slice(&TableSegmentNotEmptyCommand::TYPE_CODE.to_be_bytes());
                let se = table_seg_not_empty_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::InvalidEventNumber(invalid_event_num_cmd) => {
                res.extend_from_slice(&InvalidEventNumberCommand::TYPE_CODE.to_be_bytes());
                let se = invalid_event_num_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::OperationUnsupported(operation_unsupported_cmd) => {
                res.extend_from_slice(&OperationUnsupportedCommand::TYPE_CODE.to_be_bytes());
                let se = operation_unsupported_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::Padding(padding_command) => {
                res.extend_from_slice(&PaddingCommand::TYPE_CODE.to_be_bytes());
                let se = padding_command.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::PartialEvent(partial_event_cmd) => {
                res.extend_from_slice(&PartialEventCommand::TYPE_CODE.to_be_bytes());
                let se = partial_event_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::Event(event_cmd) => {
                res.extend_from_slice(&EventCommand::TYPE_CODE.to_be_bytes());
                let se = event_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SetupAppend(setup_append_cmd) => {
                res.extend_from_slice(&SetupAppendCommand::TYPE_CODE.to_be_bytes());
                let se = setup_append_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::AppendBlock(append_block_cmd) => {
                res.extend_from_slice(&AppendBlockCommand::TYPE_CODE.to_be_bytes());
                let se = append_block_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::AppendBlockEnd(append_block_end_cmd) => {
                res.extend_from_slice(&AppendBlockEndCommand::TYPE_CODE.to_be_bytes());
                let se = append_block_end_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::ConditionalAppend(conditional_append_cmd) => {
                res.extend_from_slice(&ConditionalAppendCommand::TYPE_CODE.to_be_bytes());
                let se = conditional_append_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::AppendSetup(append_setup_cmd) => {
                res.extend_from_slice(&AppendSetupCommand::TYPE_CODE.to_be_bytes());
                let se = append_setup_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::DataAppended(data_appended_cmd) => {
                res.extend_from_slice(&DataAppendedCommand::TYPE_CODE.to_be_bytes());
                let se = data_appended_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::ConditionalCheckFailed(conditional_check_failed_cmd) => {
                res.extend_from_slice(&ConditionalCheckFailedCommand::TYPE_CODE.to_be_bytes());
                let se = conditional_check_failed_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::ReadSegment(read_segment_cmd) => {
                res.extend_from_slice(&ReadSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = read_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SegmentRead(segment_read_cmd) => {
                res.extend_from_slice(&SegmentReadCommand::TYPE_CODE.to_be_bytes());
                let se = segment_read_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::GetSegmentAttribute(get_segment_attribute_cmd) => {
                res.extend_from_slice(&GetSegmentAttributeCommand::TYPE_CODE.to_be_bytes());
                let se = get_segment_attribute_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SegmentAttribute(segment_attribute_cmd) => {
                res.extend_from_slice(&SegmentAttributeCommand::TYPE_CODE.to_be_bytes());
                let se = segment_attribute_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::UpdateSegmentAttribute(update_segment_attribute_cmd) => {
                res.extend_from_slice(&UpdateSegmentAttributeCommand::TYPE_CODE.to_be_bytes());
                let se = update_segment_attribute_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SegmentAttributeUpdated(segment_attribute_updated_cmd) => {
                res.extend_from_slice(&SegmentAttributeUpdatedCommand::TYPE_CODE.to_be_bytes());
                let se = segment_attribute_updated_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::GetStreamSegmentInfo(get_stream_segment_info_cmd) => {
                res.extend_from_slice(&GetStreamSegmentInfoCommand::TYPE_CODE.to_be_bytes());
                let se = get_stream_segment_info_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::StreamSegmentInfo(stream_segment_info_cmd) => {
                res.extend_from_slice(&StreamSegmentInfoCommand::TYPE_CODE.to_be_bytes());
                let se = stream_segment_info_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::CreateSegment(create_segment_cmd) => {
                res.extend_from_slice(&CreateSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = create_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::CreateTableSegment(create_table_segment_command) => {
                res.extend_from_slice(&CreateTableSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = create_table_segment_command.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SegmentCreated(segment_created_cmd) => {
                res.extend_from_slice(&SegmentCreatedCommand::TYPE_CODE.to_be_bytes());
                let se = segment_created_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::UpdateSegmentPolicy(update_segment_policy_cmd) => {
                res.extend_from_slice(&UpdateSegmentPolicyCommand::TYPE_CODE.to_be_bytes());
                let se = update_segment_policy_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SegmentPolicyUpdated(segment_policy_updated_cmd) => {
                res.extend_from_slice(&SegmentPolicyUpdatedCommand::TYPE_CODE.to_be_bytes());
                let se = segment_policy_updated_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se)
            }
            WireCommands::MergeSegments(merge_segments_cmd) => {
                res.extend_from_slice(&MergeSegmentsCommand::TYPE_CODE.to_be_bytes());
                let se = merge_segments_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::MergeTableSegments(merge_table_segments_cmd) => {
                res.extend_from_slice(&MergeTableSegmentsCommand::TYPE_CODE.to_be_bytes());
                let se = merge_table_segments_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SegmentsMerged(segments_merged_cmd) => {
                res.extend_from_slice(&SegmentsMergedCommand::TYPE_CODE.to_be_bytes());
                let se = segments_merged_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SealSegment(seal_segment_cmd) => {
                res.extend_from_slice(&SealSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = seal_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SealTableSegment(seal_table_segment_cmd) => {
                res.extend_from_slice(&SealTableSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = seal_table_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SegmentSealed(segment_sealed_cmd) => {
                res.extend_from_slice(&SegmentSealedCommand::TYPE_CODE.to_be_bytes());
                let se = segment_sealed_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::TruncateSegment(truncate_segment_cmd) => {
                res.extend_from_slice(&TruncateSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = truncate_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SegmentTruncated(segment_truncated_cmd) => {
                res.extend_from_slice(&SegmentTruncatedCommand::TYPE_CODE.to_be_bytes());
                let se = segment_truncated_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::DeleteSegment(delete_segment_cmd) => {
                res.extend_from_slice(&DeleteSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = delete_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::DeleteTableSegment(delete_table_segment_cmd) => {
                res.extend_from_slice(&DeleteTableSegmentCommand::TYPE_CODE.to_be_bytes());
                let se = delete_table_segment_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::SegmentDeleted(segment_deleted_cmd) => {
                res.extend_from_slice(&SegmentDeletedCommand::TYPE_CODE.to_be_bytes());
                let se = segment_deleted_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::KeepAlive(keep_alive_cmd) => {
                res.extend_from_slice(&KeepAliveCommand::TYPE_CODE.to_be_bytes());
                let se = keep_alive_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::AuthTokenCheckFailed(auth_token_check_failed_cmd) => {
                res.extend_from_slice(&AuthTokenCheckFailedCommand::TYPE_CODE.to_be_bytes());
                let se = auth_token_check_failed_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::UpdateTableEntries(update_table_entries_cmd) => {
                res.extend_from_slice(&UpdateTableEntriesCommand::TYPE_CODE.to_be_bytes());
                let se = update_table_entries_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::TableEntriesUpdated(table_entries_updated_cmd) => {
                res.extend_from_slice(&TableEntriesUpdatedCommand::TYPE_CODE.to_be_bytes());
                let se = table_entries_updated_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::RemoveTableKeys(remove_table_keys_cmd) => {
                res.extend_from_slice(&RemoveTableKeysCommand::TYPE_CODE.to_be_bytes());
                let se = remove_table_keys_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::TableKeysRemoved(table_key_removed_cmd) => {
                res.extend_from_slice(&TableKeysRemovedCommand::TYPE_CODE.to_be_bytes());
                let se = table_key_removed_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::ReadTable(read_table_cmd) => {
                res.extend_from_slice(&ReadTableCommand::TYPE_CODE.to_be_bytes());
                let se = read_table_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::TableRead(table_read_cmd) => {
                res.extend_from_slice(&TableReadCommand::TYPE_CODE.to_be_bytes());
                let se = table_read_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::ReadTableKeys(read_table_keys_cmd) => {
                res.extend_from_slice(&ReadTableKeysCommand::TYPE_CODE.to_be_bytes());
                let se = read_table_keys_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::TableKeysRead(table_keys_read_cmd) => {
                res.extend_from_slice(&TableKeysReadCommand::TYPE_CODE.to_be_bytes());
                let se = table_keys_read_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::ReadTableEntries(read_table_entries_cmd) => {
                res.extend_from_slice(&ReadTableEntriesCommand::TYPE_CODE.to_be_bytes());
                let se = read_table_entries_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::TableEntriesRead(table_entries_read_cmd) => {
                res.extend_from_slice(&TableEntriesReadCommand::TYPE_CODE.to_be_bytes());
                let se = table_entries_read_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::TableKeyDoesNotExist(table_key_does_not_exist_cmd) => {
                res.extend_from_slice(&TableKeyDoesNotExistCommand::TYPE_CODE.to_be_bytes());
                let se = table_key_does_not_exist_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            WireCommands::TableKeyBadVersion(table_key_bad_version_cmd) => {
                res.extend_from_slice(&TableKeyBadVersionCommand::TYPE_CODE.to_be_bytes());
                let se = table_key_bad_version_cmd.write_fields()?;
                res.extend_from_slice(&(se.len() as i32).to_be_bytes());
                res.extend(se);
            }
            _ => panic!("Unknown WireCommands"),
        }
        Ok(res)
    }
}

impl Decode for WireCommands {
    fn read_from(raw_input: &Vec<u8>) -> Result<WireCommands, CommandError> {
        let type_code = BigEndian::read_i32(raw_input);
        let _length = BigEndian::read_i32(&raw_input[4..]);
        let input = &raw_input[8..];
        match type_code {
            HelloCommand::TYPE_CODE => Ok(WireCommands::Hello(HelloCommand::read_from(input)?)),
            WrongHostCommand::TYPE_CODE => {
                Ok(WireCommands::WrongHost(WrongHostCommand::read_from(input)?))
            }
            SegmentIsSealedCommand::TYPE_CODE => Ok(WireCommands::SegmentIsSealed(
                SegmentIsSealedCommand::read_from(input)?,
            )),
            SegmentAlreadyExistsCommand::TYPE_CODE => Ok(WireCommands::SegmentAlreadyExists(
                SegmentAlreadyExistsCommand::read_from(input)?,
            )),
            SegmentIsTruncatedCommand::TYPE_CODE => Ok(WireCommands::SegmentIsTruncated(
                SegmentIsTruncatedCommand::read_from(input)?,
            )),
            NoSuchSegmentCommand::TYPE_CODE => Ok(WireCommands::NoSuchSegment(
                NoSuchSegmentCommand::read_from(input)?,
            )),
            TableSegmentNotEmptyCommand::TYPE_CODE => Ok(WireCommands::TableSegmentNotEmpty(
                TableSegmentNotEmptyCommand::read_from(input)?,
            )),
            InvalidEventNumberCommand::TYPE_CODE => Ok(WireCommands::InvalidEventNumber(
                InvalidEventNumberCommand::read_from(input)?,
            )),
            OperationUnsupportedCommand::TYPE_CODE => Ok(WireCommands::OperationUnsupported(
                OperationUnsupportedCommand::read_from(input)?,
            )),
            PaddingCommand::TYPE_CODE => {
                Ok(WireCommands::Padding(PaddingCommand::read_from(input)?))
            }

            PartialEventCommand::TYPE_CODE => Ok(WireCommands::PartialEvent(
                PartialEventCommand::read_from(input)?,
            )),

            EventCommand::TYPE_CODE => Ok(WireCommands::Event(EventCommand::read_from(input)?)),

            SetupAppendCommand::TYPE_CODE => Ok(WireCommands::SetupAppend(
                SetupAppendCommand::read_from(input)?,
            )),
            AppendBlockCommand::TYPE_CODE => Ok(WireCommands::AppendBlock(
                AppendBlockCommand::read_from(input)?,
            )),
            AppendBlockEndCommand::TYPE_CODE => Ok(WireCommands::AppendBlockEnd(
                AppendBlockEndCommand::read_from(input)?,
            )),
            ConditionalAppendCommand::TYPE_CODE => Ok(WireCommands::ConditionalAppend(
                ConditionalAppendCommand::read_from(input)?,
            )),
            AppendSetupCommand::TYPE_CODE => Ok(WireCommands::AppendSetup(
                AppendSetupCommand::read_from(input)?,
            )),
            DataAppendedCommand::TYPE_CODE => Ok(WireCommands::DataAppended(
                DataAppendedCommand::read_from(input)?,
            )),
            ConditionalCheckFailedCommand::TYPE_CODE => Ok(WireCommands::ConditionalCheckFailed(
                ConditionalCheckFailedCommand::read_from(input)?,
            )),
            ReadSegmentCommand::TYPE_CODE => Ok(WireCommands::ReadSegment(
                ReadSegmentCommand::read_from(input)?,
            )),
            SegmentReadCommand::TYPE_CODE => Ok(WireCommands::SegmentRead(
                SegmentReadCommand::read_from(input)?,
            )),
            GetSegmentAttributeCommand::TYPE_CODE => Ok(WireCommands::GetSegmentAttribute(
                GetSegmentAttributeCommand::read_from(input)?,
            )),
            SegmentAttributeCommand::TYPE_CODE => Ok(WireCommands::SegmentAttribute(
                SegmentAttributeCommand::read_from(input)?,
            )),
            UpdateSegmentAttributeCommand::TYPE_CODE => Ok(WireCommands::UpdateSegmentAttribute(
                UpdateSegmentAttributeCommand::read_from(input)?,
            )),
            SegmentAttributeUpdatedCommand::TYPE_CODE => Ok(WireCommands::SegmentAttributeUpdated(
                SegmentAttributeUpdatedCommand::read_from(input)?,
            )),
            GetStreamSegmentInfoCommand::TYPE_CODE => Ok(WireCommands::GetStreamSegmentInfo(
                GetStreamSegmentInfoCommand::read_from(input)?,
            )),
            StreamSegmentInfoCommand::TYPE_CODE => Ok(WireCommands::StreamSegmentInfo(
                StreamSegmentInfoCommand::read_from(input)?,
            )),
            CreateSegmentCommand::TYPE_CODE => Ok(WireCommands::CreateSegment(
                CreateSegmentCommand::read_from(input)?,
            )),
            CreateTableSegmentCommand::TYPE_CODE => Ok(WireCommands::CreateTableSegment(
                CreateTableSegmentCommand::read_from(input)?,
            )),
            SegmentCreatedCommand::TYPE_CODE => Ok(WireCommands::SegmentCreated(
                SegmentCreatedCommand::read_from(input)?,
            )),
            UpdateSegmentPolicyCommand::TYPE_CODE => Ok(WireCommands::UpdateSegmentPolicy(
                UpdateSegmentPolicyCommand::read_from(input)?,
            )),
            SegmentPolicyUpdatedCommand::TYPE_CODE => Ok(WireCommands::SegmentPolicyUpdated(
                SegmentPolicyUpdatedCommand::read_from(input)?,
            )),
            MergeSegmentsCommand::TYPE_CODE => Ok(WireCommands::MergeSegments(
                MergeSegmentsCommand::read_from(input)?,
            )),
            MergeTableSegmentsCommand::TYPE_CODE => Ok(WireCommands::MergeTableSegments(
                MergeTableSegmentsCommand::read_from(input)?,
            )),
            SegmentsMergedCommand::TYPE_CODE => Ok(WireCommands::SegmentsMerged(
                SegmentsMergedCommand::read_from(input)?,
            )),
            SealSegmentCommand::TYPE_CODE => Ok(WireCommands::SealSegment(
                SealSegmentCommand::read_from(input)?,
            )),
            SealTableSegmentCommand::TYPE_CODE => Ok(WireCommands::SealTableSegment(
                SealTableSegmentCommand::read_from(input)?,
            )),
            SegmentSealedCommand::TYPE_CODE => Ok(WireCommands::SegmentSealed(
                SegmentSealedCommand::read_from(input)?,
            )),
            TruncateSegmentCommand::TYPE_CODE => Ok(WireCommands::TruncateSegment(
                TruncateSegmentCommand::read_from(input)?,
            )),
            SegmentTruncatedCommand::TYPE_CODE => Ok(WireCommands::SegmentTruncated(
                SegmentTruncatedCommand::read_from(input)?,
            )),
            DeleteSegmentCommand::TYPE_CODE => Ok(WireCommands::DeleteSegment(
                DeleteSegmentCommand::read_from(input)?,
            )),
            DeleteTableSegmentCommand::TYPE_CODE => Ok(WireCommands::DeleteTableSegment(
                DeleteTableSegmentCommand::read_from(input)?,
            )),
            SegmentDeletedCommand::TYPE_CODE => Ok(WireCommands::SegmentDeleted(
                SegmentDeletedCommand::read_from(input)?,
            )),
            KeepAliveCommand::TYPE_CODE => {
                Ok(WireCommands::KeepAlive(KeepAliveCommand::read_from(input)?))
            }
            AuthTokenCheckFailedCommand::TYPE_CODE => Ok(WireCommands::AuthTokenCheckFailed(
                AuthTokenCheckFailedCommand::read_from(input)?,
            )),
            UpdateTableEntriesCommand::TYPE_CODE => Ok(WireCommands::UpdateTableEntries(
                UpdateTableEntriesCommand::read_from(input)?,
            )),
            TableEntriesUpdatedCommand::TYPE_CODE => Ok(WireCommands::TableEntriesUpdated(
                TableEntriesUpdatedCommand::read_from(input)?,
            )),
            RemoveTableKeysCommand::TYPE_CODE => Ok(WireCommands::RemoveTableKeys(
                RemoveTableKeysCommand::read_from(input)?,
            )),
            TableKeysRemovedCommand::TYPE_CODE => Ok(WireCommands::TableKeysRemoved(
                TableKeysRemovedCommand::read_from(input)?,
            )),
            ReadTableCommand::TYPE_CODE => {
                Ok(WireCommands::ReadTable(ReadTableCommand::read_from(input)?))
            }
            TableReadCommand::TYPE_CODE => {
                Ok(WireCommands::TableRead(TableReadCommand::read_from(input)?))
            }
            ReadTableKeysCommand::TYPE_CODE => Ok(WireCommands::ReadTableKeys(
                ReadTableKeysCommand::read_from(input)?,
            )),
            TableKeysReadCommand::TYPE_CODE => Ok(WireCommands::TableKeysRead(
                TableKeysReadCommand::read_from(input)?,
            )),
            ReadTableEntriesCommand::TYPE_CODE => Ok(WireCommands::ReadTableEntries(
                ReadTableEntriesCommand::read_from(input)?,
            )),
            TableEntriesReadCommand::TYPE_CODE => Ok(WireCommands::TableEntriesRead(
                TableEntriesReadCommand::read_from(input)?,
            )),
            TableKeyDoesNotExistCommand::TYPE_CODE => Ok(WireCommands::TableKeyDoesNotExist(
                TableKeyDoesNotExistCommand::read_from(input)?,
            )),
            TableKeyBadVersionCommand::TYPE_CODE => Ok(WireCommands::TableKeyBadVersion(
                TableKeyBadVersionCommand::read_from(input)?,
            )),
            _ => Ok(WireCommands::UnknownCommand),
        }
    }
}
