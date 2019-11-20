extern crate pravega_client_rust;

#[cfg(test)]
mod tests {
    use pravega_client_rust::wirecommands::*;
    use pravega_client_rust::commands::*;

    #[test]
    fn test_hello() {
        let hello_command = WireCommands::Hello(HelloCommand { high_version: 9, low_version: 5 });
        test_command(hello_command);
    }

    #[test]
    fn test_wrong_host() {
        let correct_host_name = JavaString(String::from("foo"));
        let segment_name = JavaString(String::from("segment-1"));
        let stack_trace = JavaString(String::from("some exception"));
        let wrong_host_command = WireCommands::WrongHost(WrongHostCommand{request_id: 1,
            segment: segment_name, correct_host: correct_host_name, server_stack_trace: stack_trace});
        test_command( wrong_host_command);
    }

    #[test]
    fn test_segment_is_sealed() {
        let segment_name = JavaString(String::from("segment-1"));
        let stack_trace = JavaString(String::from("some exception"));
        let offset_pos = 100i64;
        let segment_is_sealed_command = WireCommands::SegmentIsSealed(SegmentIsSealedCommand{
            request_id: 1, segment: segment_name, server_stack_trace: stack_trace, offset: offset_pos
        });
        test_command(segment_is_sealed_command);
    }

    #[test]
    fn test_segment_already_exists() {
        let segment_name = JavaString(String::from("segment-1"));
        let stack_trace = JavaString(String::from("some exception"));
        let segment_already_exists_command = WireCommands::SegmentAlreadyExists(SegmentAlreadyExistsCommand{
            request_id: 1, segment: segment_name, server_stack_trace: stack_trace,
        });
        test_command( segment_already_exists_command);
    }

    #[test]
    fn test_segment_is_truncated() {
        let segment_name = JavaString(String::from("segment-1"));
        let stack_trace = JavaString(String::from("some exception"));
        let start_offset_pos = 0i64;
        let offset_pos = 100i64;
        let segment_is_truncated_command = WireCommands::SegmentIsTruncated(SegmentIsTruncatedCommand{
            request_id: 1, segment: segment_name, server_stack_trace: stack_trace, start_offset: start_offset_pos,
            offset: offset_pos
        });
        test_command( segment_is_truncated_command);
    }

    #[test]
    fn test_no_such_segment() {
        let segment_name = JavaString(String::from("segment-1"));
        let stack_trace = JavaString(String::from("some exception"));
        let offset_pos = 100i64;
        let no_such_segment_command = WireCommands::NoSuchSegment(NoSuchSegmentCommand{
            request_id: 1, segment: segment_name, server_stack_trace: stack_trace, offset: offset_pos
        });
        test_command(no_such_segment_command);
    }

    #[test]
    fn test_table_segment_not_empty() {
        let segment_name = JavaString(String::from("segment-1"));
        let stack_trace = JavaString(String::from("some exception"));
        let table_segment_not_empty_command = WireCommands::TableSegmentNotEmpty(TableSegmentNotEmptyCommand{
            request_id: 1, segment: segment_name, server_stack_trace: stack_trace
        });
        test_command( table_segment_not_empty_command);
    }

    #[test]
    fn test_invalid_event_number() {
        let writer_id_number: u128 = 123;
        let event_num : i64 = 100;
        let stack_trace = JavaString(String::from("some exception"));
        let invalid_event_number_command = WireCommands::InvalidEventNumber(InvalidEventNumberCommand{
            writer_id: writer_id_number, server_stack_trace: stack_trace, event_number: event_num
        });
        test_command(invalid_event_number_command);
    }

    #[test]
    fn test_operation_unsupported() {
        let name = JavaString(String::from("operation"));;
        let stack_trace = JavaString(String::from("some exception"));
        let test_operation_unsupported_command = WireCommands::OperationUnsupported(OperationUnsupportedCommand{
            request_id: 1, operation_name: name, server_stack_trace: stack_trace
        });
        test_command(test_operation_unsupported_command);
    }

    #[test]
    fn test_padding() {
        let length = 10;
        let padding_command = WireCommands::Padding(PaddingCommand{length});
        let decoded  = test_command(padding_command);
    }

    #[test]
    fn test_partial_event() {
        let data = String::from("event-1").into_bytes();
        let partial_event = WireCommands::PartialEvent(PartialEventCommand{data});
        test_command(partial_event);
    }

    #[test]
    fn test_event() {
        let data = String::from("event-1").into_bytes();
        let event = WireCommands::Event(EventCommand{data});
        let decoded = test_command(event);
        if let WireCommands::Event(event_struct) = decoded {
            assert_eq!(String::from_utf8(event_struct.data).unwrap(), "event-1");
        } else {
            panic!("test failed");
        }
    }

    #[test]
    fn test_setup_append() {
        let writer_id_number: u128 = 123;
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let setup_append_command = WireCommands::SetupAppend(SetupAppendCommand{request_id:1, writer_id: writer_id_number,
            segment: segment_name, delegation_token: token});
        test_command(setup_append_command);
    }

    #[test]
    fn test_append_block() {
        let writer_id_number: u128 = 123;
        let data = String::from("event-1").into_bytes();
        let append_block_command = WireCommands::AppendBlock(AppendBlockCommand{writer_id: writer_id_number, data});
        test_command(append_block_command);
    }

    #[test]
    fn test_append_block_end() {
        let writer_id_number: u128 = 123;
        let data = String::from("event-1").into_bytes();
        let size_of_events = data.len() as i32;
        let append_block_end_command = WireCommands::AppendBlockEnd(AppendBlockEndCommand{
            writer_id: writer_id_number, size_of_whole_events: size_of_events, data, num_event: 1,
            last_event_number: 1, request_id: 1
        });

        test_command(append_block_end_command);
    }

    #[test]
    fn test_conditional_append() {
        let writer_id_number: u128 = 123;
        let data = String::from("event-1").into_bytes();
        let event = EventCommand{data};
        let conditional_append_command = WireCommands::ConditionalAppend(ConditionalAppendCommand{
            writer_id: writer_id_number, event_number: 1, expected_offset: 0, event, request_id: 1
        });

        let decoded = test_command(conditional_append_command);
        if let WireCommands::ConditionalAppend(command)  = decoded {
            let data = String::from("event-1").into_bytes();
            assert_eq!(command.event, EventCommand{data});
        } else {
            panic!("test failed");
        }
    }

    #[test]
    fn test_append_setup() {
        let writer_id_number: u128 = 123;
        let segment_name = JavaString(String::from("segment-1"));
        let append_setup_cmd = WireCommands::AppendSetup(AppendSetupCommand{
            request_id:1, segment: segment_name, writer_id: writer_id_number, last_event_number: 1
        });
        test_command(append_setup_cmd);
    }

    #[test]
    fn test_data_appended() {
        let writer_id_number: u128 = 123;
        let data_appended_cmd = WireCommands::DataAppended(DataAppendedCommand{
            writer_id: writer_id_number,
            event_number: 1,
            previous_event_number: 0,
            request_id: 1,
            current_segment_write_offset: 0
        });
        test_command(data_appended_cmd);
    }

    #[test]
    fn test_conditional_check_failed() {
        let writer_id_number : u128 = 123;
        let conditional_check_failed_cmd = WireCommands::ConditionalCheckFailed(ConditionalCheckFailedCommand{
            writer_id: writer_id_number,
            event_number: 1,
            request_id: 1
        });
        test_command(conditional_check_failed_cmd);
    }

    #[test]
    fn test_read_segment() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let read_segment_command = WireCommands::ReadSegment(ReadSegmentCommand{
            segment: segment_name,
            offset: 0,
            suggested_length: 10,
            delegation_token: token,
            request_id: 1
        });
        test_command(read_segment_command);
    }

    #[test]
    fn test_segment_read() {
        let segment_name = JavaString(String::from("segment-1"));
        let data = String::from("event-1").into_bytes();
        let segment_read_command = WireCommands::SegmentRead(SegmentReadCommand{
            segment: segment_name,
            offset: 0,
            at_tail: true,
            end_of_segment: true,
            data,
            request_id: 1
        });
        test_command(segment_read_command);
    }

    #[test]
    fn test_get_segment_attribute() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let attribute_id: u128 = 123;
        let get_segment_attribute_command = WireCommands::GetSegmentAttribute(GetSegmentAttributeCommand{
            request_id: 1,
            segment_name,
            attribute_id,
            delegation_token: token
        });
        test_command(get_segment_attribute_command);
    }

    #[test]
    fn test_segment_attribute() {
        let segment_attribute_command = WireCommands::SegmentAttribute(SegmentAttributeCommand{
            request_id: 1,
            value: 0
        });
        test_command(segment_attribute_command);
    }

    #[test]
    fn test_update_segment_attribute() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let attribute_id: u128 = 123;
        let update_segment_attribute = WireCommands::UpdateSegmentAttribute(UpdateSegmentAttributeCommand{
            request_id: 1,
            segment_name,
            attribute_id,
            new_value: 2,
            expected_value: 2,
            delegation_token: token
        });

        test_command(update_segment_attribute);
    }

    #[test]
    fn test_segment_attribute_updated() {
        let segment_attribute_updated = WireCommands::SegmentAttributeUpdated(SegmentAttributeUpdatedCommand{
            request_id: 1,
            success: true
        });
        test_command(segment_attribute_updated);
    }

    #[test]
    fn test_get_stream_segment_info() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let get_stream_segment_info = WireCommands::GetStreamSegmentInfo(GetStreamSegmentInfoCommand{
            request_id: 1,
            segment_name,
            delegation_token: token
        });
        test_command(get_stream_segment_info);
    }

    #[test]
    fn test_stream_segment_info() {
        let segment_name = JavaString(String::from("segment-1"));
        let stream_segment_info = WireCommands::StreamSegmentInfo(StreamSegmentInfoCommand{
            request_id: 0,
            segment_name,
            exists: false,
            is_sealed: false,
            is_deleted: false,
            last_modified: 0,
            write_offset: 0,
            start_offset: 0
        });
        test_command(stream_segment_info);
    }

    #[test]
    fn test_create_segment() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let create_segment_command = WireCommands::CreateSegment(CreateSegmentCommand{
            request_id: 1,
            segment: segment_name,
            target_rate: 1,
            scale_type: 0,
            delegation_token: token
        });
        test_command(create_segment_command);
    }

    #[test]
    fn test_create_table_segment() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let create_table_segment_command = WireCommands::CreateTableSegment(CreateTableSegmentCommand{
            request_id: 1,
            segment: segment_name,
            delegation_token: token
        });
        test_command(create_table_segment_command);
    }

    #[test]
    fn test_segment_created() {
        let segment_name = JavaString(String::from("segment-1"));
        let segment_created_cmd = WireCommands::SegmentCreated(SegmentCreatedCommand{
            request_id: 1,
            segment: segment_name
        });
        test_command(segment_created_cmd);
    }

    #[test]
    fn test_update_segment_policy() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let update_segment_policy_cmd = WireCommands::UpdateSegmentPolicy(UpdateSegmentPolicyCommand{
            request_id: 1,
            segment: segment_name,
            target_rate: 1,
            scale_type: 0,
            delegation_token: token
        });
        test_command(update_segment_policy_cmd);
    }

    #[test]
    fn test_segment_policy_updated() {
        let segment_name = JavaString(String::from("segment-1"));
        let segment_policy_updated = WireCommands::SegmentPolicyUpdated(SegmentPolicyUpdatedCommand{
            request_id: 0,
            segment: segment_name
        });
        test_command(segment_policy_updated);
    }

    #[test]
    fn test_merge_segment() {
        let target = JavaString(String::from("segment-1"));
        let source = JavaString(String::from("segment-2"));
        let token = JavaString(String::from("delegation_token"));
        let merge_segment = WireCommands::MergeSegments(MergeSegmentsCommand{
            request_id: 1,
            target,
            source,
            delegation_token: token
        });
        test_command(merge_segment);
    }

    #[test]
    fn test_merge_table_segment() {
        let target = JavaString(String::from("segment-1"));
        let source = JavaString(String::from("segment-2"));
        let token = JavaString(String::from("delegation_token"));
        let merge_table_segment = WireCommands::MergeTableSegments(MergeTableSegmentsCommand{
            request_id: 1,
            target,
            source,
            delegation_token: token
        });
        test_command(merge_table_segment);
    }

    #[test]
    fn test_segment_merged() {
        let target = JavaString(String::from("segment-1"));
        let source = JavaString(String::from("segment-2"));
        let segment_merged = WireCommands::SegmentsMerged(SegmentsMergedCommand{
            request_id: 1,
            target,
            source,
            new_target_write_offset: 10
        });
        test_command(segment_merged);
    }

    #[test]
    fn test_seal_segment() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let seal_segment = WireCommands::SealSegment(SealSegmentCommand{
            request_id: 1,
            segment: segment_name,
            delegation_token: token
        });
        test_command(seal_segment);
    }

    #[test]
    fn test_seal_table_segment() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let seal_table_segment = WireCommands::SealTableSegment(SealTableSegmentCommand{
            request_id: 1,
            segment: segment_name,
            delegation_token: token
        });
        test_command(seal_table_segment);
    }

    #[test]
    fn test_segment_sealed() {
        let segment_name = JavaString(String::from("segment-1"));
        let segment_sealed = WireCommands::SegmentSealed(SegmentSealedCommand{
            request_id:1,
            segment: segment_name
        });
        test_command(segment_sealed);
    }

    #[test]
    fn test_truncate_segment() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let truncate_segment = WireCommands::TruncateSegment(TruncateSegmentCommand {
            request_id: 1,
            segment: segment_name,
            truncation_offset: 10,
            delegation_token: token
        });
        test_command(truncate_segment);
    }

    #[test]
    fn test_segment_truncated() {
        let segment_name = JavaString(String::from("segment-1"));
        let segment_truncated = WireCommands::SegmentTruncated(SegmentTruncatedCommand {
            request_id: 1,
            segment: segment_name
        });
        test_command(segment_truncated);
    }

    #[test]
    fn test_delete_segment() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let delete_segment_command = WireCommands::DeleteSegment(DeleteSegmentCommand {
            request_id: 1,
            segment: segment_name,
            delegation_token: token
        });
        test_command(delete_segment_command);
    }

    #[test]
    fn test_segment_deleted() {
        let segment = JavaString(String::from("segment-1"));
        let segment_deleted = WireCommands::SegmentDeleted(SegmentDeletedCommand {
            request_id: 1,
            segment
        });
        test_command(segment_deleted);
    }

    #[test]
    fn test_delete_table_segment() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let delete_table_segment = WireCommands::DeleteTableSegment(DeleteTableSegmentCommand {
            request_id: 0,
            segment: segment_name,
            must_be_empty: true,
            delegation_token: token
        });
        test_command(delete_table_segment);
    }

    #[test]
    fn test_keep_alive() {
        let keep_alive = WireCommands::KeepAlive(KeepAliveCommand {});
        test_command(keep_alive);
    }

    #[test]
    fn test_auth_checked_failed() {
        let stack_trace = JavaString(String::from("some exception"));
        let auth_checked_failed = WireCommands::AuthTokenCheckFailed(AuthTokenCheckFailedCommand{
            request_id: 1,
            server_stack_trace: stack_trace,
            error_code: -1
        });
        test_command(auth_checked_failed);
    }

    #[test]
    fn test_update_table_entries() {
        let mut entries = Vec::<(TableKey, TableValue)>::new();
        let key_data = String::from("key-1").into_bytes();
        let value_data = String::from("value-1").into_bytes();
        entries.push((TableKey::new(key_data, 1), TableValue::new(value_data)));
        let table_entries = TableEntries { entries };
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let update_table_entries = WireCommands::UpdateTableEntries(UpdateTableEntriesCommand {
            request_id: 1,
            segment: segment_name,
            delegation_token: token,
            table_entries
        });

        test_command(update_table_entries);
    }

    #[test]
    fn test_table_entries_updated() {
        let updated_versions: Vec<i64> = vec![1, 2, 3, 4];
        let table_entries_updated = WireCommands::TableEntriesUpdated(TableEntriesUpdatedCommand {
            request_id: 1,
            updated_versions
        });
        test_command(table_entries_updated);
    }

    #[test]
    fn test_remove_table_keys() {
        let segment = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let mut keys = Vec::<(TableKey)>::new();
        let key_data = String::from("key-1").into_bytes();
        keys.push(TableKey::new(key_data, 1));
        let remove_table_keys_command = WireCommands::RemoveTableKeys(RemoveTableKeysCommand {
            request_id: 1,
            segment,
            delegation_token: token,
            keys
        });
        test_command(remove_table_keys_command);
    }

    #[test]
    fn test_table_keys_removed() {
        let segment = JavaString(String::from("segment-1"));
        let table_key_removed = WireCommands::TableKeysRemoved(TableKeysRemovedCommand {
            request_id: 1,
            segment
        });
        test_command(table_key_removed);
    }

    #[test]
    fn test_read_table() {
        let segment = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let mut keys = Vec::<(TableKey)>::new();
        let key_data = String::from("key-1").into_bytes();
        keys.push(TableKey::new(key_data, 1));
        let read_table_command = WireCommands::ReadTable(ReadTableCommand{
            request_id: 1,
            segment,
            delegation_token: token,
            keys
        });

        test_command(read_table_command);
    }

    #[test]
    fn test_table_read() {
        let mut entries = Vec::<(TableKey, TableValue)>::new();
        let key_data = String::from("key-1").into_bytes();
        let value_data = String::from("value-1").into_bytes();
        entries.push((TableKey::new(key_data, 1), TableValue::new(value_data)));
        let table_entries = TableEntries { entries };
        let segment_name = JavaString(String::from("segment-1"));
        let table_read = WireCommands::TableRead(TableReadCommand{
            request_id: 1,
            segment: segment_name,
            entries: table_entries
        });

        test_command(table_read);
    }

    #[test]
    fn test_read_table_keys() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let continuation_token :Vec<u8> = vec![1, 2, 3];
        let read_table_keys = WireCommands::ReadTableKeys(ReadTableKeysCommand{
            request_id: 0,
            segment: segment_name,
            delegation_token: token,
            suggested_key_count: 3,
            continuation_token
        });
        test_command(read_table_keys);
    }

    #[test]
    fn test_table_keys_read() {
        let segment = JavaString(String::from("segment-1"));
        let mut keys = Vec::<(TableKey)>::new();
        let key_data = String::from("key-1").into_bytes();
        keys.push(TableKey::new(key_data, 1));
        let continuation_token :Vec<u8> = vec![1, 2, 3];
        let table_keys_read_command = WireCommands::TableKeysRead(TableKeysReadCommand{
            request_id: 1,
            segment,
            keys,
            continuation_token
        });
        test_command(table_keys_read_command);
    }

    #[test]
    fn test_read_table_entries() {
        let segment_name = JavaString(String::from("segment-1"));
        let token = JavaString(String::from("delegation_token"));
        let continuation_token :Vec<u8> = vec![1, 2, 3];
        let read_table_entries = WireCommands::ReadTableEntries(ReadTableEntriesCommand{
            request_id: 0,
            segment: segment_name,
            delegation_token: token,
            suggested_entry_count: 3,
            continuation_token
        });
        test_command(read_table_entries);
    }

    #[test]
    fn test_table_entries_read() {
        let segment_name = JavaString(String::from("segment-1"));
        let continuation_token :Vec<u8> = vec![1, 2, 3];
        let mut entries = Vec::<(TableKey, TableValue)>::new();
        let key_data = String::from("key-1").into_bytes();
        let value_data = String::from("value-1").into_bytes();
        entries.push((TableKey::new(key_data, 1), TableValue::new(value_data)));
        let table_entries = TableEntries { entries };
        let table_entries_read = WireCommands::TableEntriesRead(TableEntriesReadCommand{
            request_id: 1,
            segment: segment_name,
            entries: table_entries,
            continuation_token
        });
        test_command(table_entries_read);
    }

    #[test]
    fn table_key_does_not_exist() {
        let segment_name = JavaString(String::from("segment-1"));
        let stack_trace = JavaString(String::from("some exception"));
        let table_key_does_not_exist = WireCommands::TableKeyDoesNotExist(TableKeyDoesNotExistCommand{
            request_id: 0,
            segment: segment_name,
            server_stack_trace: stack_trace
        });
        test_command(table_key_does_not_exist);
    }

    #[test]
    fn table_key_bad_version() {
        let segment_name = JavaString(String::from("segment-1"));
        let stack_trace = JavaString(String::from("some exception"));
        let table_key_bad_version = WireCommands::TableKeyBadVersion(TableKeyBadVersionCommand{
            request_id: 0,
            segment: segment_name,
            server_stack_trace: stack_trace
        });
        test_command(table_key_bad_version);
    }

    fn test_command(command: WireCommands) -> WireCommands {
        let encoded: Vec<u8> = command.write_fields();
        let decoded = WireCommands::read_from( &encoded);
        assert_eq!(command, decoded);
        decoded
    }
}