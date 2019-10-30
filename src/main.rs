/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

mod commands;
mod wirecommands;

#[cfg(test)]
mod tests {
    use crate::wirecommands::WireCommands;
    use crate::wirecommands::Encode;
    use crate::wirecommands::Decode;
    use crate::commands;
    #[test]
    fn test_hello() {
        let hello_command = WireCommands::Hello(commands::HelloCommand{high_version: 9, low_version:5});
        let encoded: Vec<u8> = hello_command.write_fields();
        println!("{:?}", encoded);
        let decoded = WireCommands::read_from(-127, &encoded);
        if let WireCommands::Hello(hello_cmd) = decoded {
            assert_eq!(hello_cmd.high_version, 9);
            assert_eq!(hello_cmd.low_version, 5);
        } else {
            panic!("test failed")
        }
    }

    #[test]
    fn test_wrong_host() {
        let segment_name = commands::JavaString(String::from("segment-1"));
        let correct_host_name = commands::JavaString(String::from("foo"));
        let stack_trace = commands::JavaString(String::from("stack_trace"));
        let wrong_host_command = WireCommands::WrongHost(commands::WrongHostCommand{request_id:1,
            segment: segment_name, correct_host:  correct_host_name, server_stack_trace: stack_trace});
        let encoded: Vec<u8> = wrong_host_command.write_fields();
        println!("{:?}", encoded);
        let decoded = WireCommands::read_from(50, &encoded);
        if let WireCommands::WrongHost(wrong_host_cmd) = decoded {
            assert_eq!(wrong_host_cmd.request_id, 1);
            assert_eq!(wrong_host_cmd.segment.0, "segment-1");
            assert_eq!(wrong_host_cmd.correct_host.0, "foo");
            assert_eq!(wrong_host_cmd.server_stack_trace.0, "stack_trace");
        } else {
            panic!("test failed")
        }
    }

    #[test]
    fn test_segment_is_sealed() {
        let segment_name = commands::JavaString(String::from("segment-2"));
        let stack_trace = commands::JavaString(String::from("stack_trace"));
        let offset_pos = 100i64;
        let segment_is_sealed_command = WireCommands::SegmentIsSealed(commands::SegmentIsSealedCommand{
           request_id:2, segment: segment_name, server_stack_trace: stack_trace, offset: offset_pos
        });
        let encoded: Vec<u8> = segment_is_sealed_command.write_fields();
        let decoded = WireCommands::read_from( 51,&encoded);
        if let WireCommands::SegmentIsSealed(sealed_cmd) = decoded {
            assert_eq!(sealed_cmd.request_id, 2);
            assert_eq!(sealed_cmd.segment.0, "segment-2");
            assert_eq!(sealed_cmd.server_stack_trace.0, "stack_trace");
            assert_eq!(sealed_cmd.offset, 100);
        } else {
            panic!("test failed");
        }
    }
}
