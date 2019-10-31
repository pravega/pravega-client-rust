/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#[macro_use]
extern crate lazy_static;

mod commands;
mod wirecommands;

#[cfg(test)]
mod tests {
    use crate::wirecommands::*;
    use crate::commands::*;

    #[test]
    fn test_hello() {
        let hello_command = WireCommands::Hello(HelloCommand { high_version: 9, low_version: 5 });
        test_command(HelloCommand::TYPE_CODE, hello_command);
    }


    #[test]
    fn test_wrong_host() {
        let correct_host_name = JavaString(String::from("foo"));
        let segment_name = JavaString(String::from("segment-1"));
        let stack_trace = JavaString(String::from("some exception"));
        let wrong_host_command = WireCommands::WrongHost(WrongHostCommand{request_id: 1,
            segment: segment_name, correct_host: correct_host_name, server_stack_trace: stack_trace});
        test_command(WrongHostCommand::TYPE_CODE, wrong_host_command);
    }

    #[test]
    fn test_segment_is_sealed() {
        let segment_name = JavaString(String::from("segment-1"));
        let stack_trace = JavaString(String::from("some exception"));
        let offset_pos = 100i64;
        let segment_is_sealed_command = WireCommands::SegmentIsSealed(SegmentIsSealedCommand{
           request_id: 1, segment: segment_name, server_stack_trace: stack_trace, offset: offset_pos
        });
        test_command(SegmentIsSealedCommand::TYPE_CODE, segment_is_sealed_command);
    }

    #[test]
    fn test_segment_already_exists() {
        let segment_name = JavaString(String::from("segment-1"));
        let stack_trace = JavaString(String::from("some exception"));
        let segment_already_exists_command = WireCommands::SegmentAlreadyExists(SegmentAlreadyExistsCommand{
           request_id: 1, segment: segment_name, server_stack_trace: stack_trace,
        });
        test_command(SegmentAlreadyExistsCommand::TYPE_CODE, segment_already_exists_command);
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
        test_command(SegmentIsTruncatedCommand::TYPE_CODE, segment_is_truncated_command);
    }

    #[test]
    fn test_no_such_segment() {
        let segment_name = JavaString(String::from("segment-1"));
        let stack_trace = JavaString(String::from("some exception"));
        let offset_pos = 100i64;
        let no_such_segment_command = WireCommands::NoSuchSegment(NoSuchSegmentCommand{
            request_id: 1, segment: segment_name, server_stack_trace: stack_trace, offset: offset_pos
        });
        test_command(NoSuchSegmentCommand::TYPE_CODE, no_such_segment_command);
    }

    #[test]
    fn test_table_segment_not_empty() {
        let segment_name = JavaString(String::from("segment-1"));
        let stack_trace = JavaString(String::from("some exception"));
        let table_segment_not_empty_command = WireCommands::TableSegmentNotEmpty(TableSegmentNotEmptyCommand{
            request_id: 1, segment: segment_name, server_stack_trace: stack_trace
        });
        test_command(TableSegmentNotEmptyCommand::TYPE_CODE, table_segment_not_empty_command);
    }

    #[test]
    fn test_invalid_event_number() {
        let write_id_number : u128 = 123;
        let event_num : i64 = 100;
        let stack_trace = JavaString(String::from("some exception"));
        let invalid_event_number_command = WireCommands::InvalidEventNumber(InvalidEventNumberCommand{
            write_id: write_id_number, server_stack_trace: stack_trace, event_number: event_num
        });
        test_command(InvalidEventNumberCommand::TYPE_CODE, invalid_event_number_command);
    }

    fn test_command(type_code: i32, command: WireCommands) {
        let encoded: Vec<u8> = command.write_fields();
        let decoded = WireCommands::read_from( type_code,&encoded);
        assert_eq!(command, decoded);
    }
}
