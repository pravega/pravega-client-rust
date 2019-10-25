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

use wirecommands::WireCommands;
use wirecommands::Encode;
use wirecommands::Decode;

fn main() {
    test_hello();
    test_wrong_host();
}

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

fn test_wrong_host() {
    let wrong_host_command = WireCommands::WrongHost(commands::WrongHostCommand{request_id:1,
        segment: String::from("a"), correct_host:  String::from("b"), server_stack_trace: String::from("c")});
    let encoded: Vec<u8> = wrong_host_command.write_fields();
    println!("{:?}", encoded);
    let decoded = WireCommands::read_from(50, &encoded);
    if let WireCommands::WrongHost(wrong_host_cmd) = decoded {
        assert_eq!(wrong_host_cmd.request_id, 1);
        assert_eq!(wrong_host_cmd.segment, "a");
        assert_eq!(wrong_host_cmd.correct_host, "b");
        assert_eq!(wrong_host_cmd.server_stack_trace, "c");
    } else {
        panic!("test failed")
    }
}



#[cfg(test)]
mod tests {
}
