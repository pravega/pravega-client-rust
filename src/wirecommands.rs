use serde::{Serialize, Deserialize};
use crate::commands::{HelloCommand, WrongHostCommand, SegmentIsSealedCommand, Command};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum  WireCommands {
    Hello(HelloCommand),
    WrongHost(WrongHostCommand),
    SegmentIsSealed(SegmentIsSealedCommand),
}

pub trait Encode {
    fn write_fields(&self) -> Vec<u8>;
}

pub trait Decode {
    fn read_from(type_code: i32, input: &Vec<u8>) -> WireCommands;
}


impl Encode for WireCommands {
    fn write_fields(&self) -> Vec<u8> {
        match self {
            WireCommands::Hello(hello_cmd) => hello_cmd.write_fields(),
            WireCommands::WrongHost(wrong_host_cmd) => wrong_host_cmd.write_fields(),
            WireCommands::SegmentIsSealed(seg_is_sealed_cmd) => seg_is_sealed_cmd.write_fields(),
        }
    }
}

impl Decode for WireCommands {
    fn read_from(type_code: i32, input: &Vec<u8>) -> WireCommands {
        match type_code {
            HelloCommand::TYPE_CODE => WireCommands::Hello(HelloCommand::read_from(input)),
            WrongHostCommand::TYPE_CODE => WireCommands::WrongHost(WrongHostCommand::read_from(input)),
            SegmentIsSealedCommand::TYPE_CODE => WireCommands::SegmentIsSealed(SegmentIsSealedCommand::read_from(input)),
            _ => panic!("Wrong input")
        }
    }
}