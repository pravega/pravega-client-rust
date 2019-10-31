use serde::{Serialize, Deserialize};
use crate::commands::*;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum  WireCommands {
    Hello(HelloCommand),
    WrongHost(WrongHostCommand),
    SegmentIsSealed(SegmentIsSealedCommand),
    SegmentAlreadyExists(SegmentAlreadyExistsCommand),
    SegmentIsTruncated(SegmentIsTruncatedCommand),
    NoSuchSegment(NoSuchSegmentCommand),
    TableSegmentNotEmpty(TableSegmentNotEmptyCommand),
    InvalidEventNumber(InvalidEventNumberCommand),
}

pub trait Encode {
    fn write_fields(&self) -> Vec<u8>;
}

pub trait Decode {
    fn read_from(type_code: i32, input: &Vec<u8>) -> WireCommands;
}


impl Encode for WireCommands {
    fn write_fields(&self) -> Vec<u8> {
        //TODO: we need encode type_code here.
        match self {
            WireCommands::Hello(hello_cmd) => hello_cmd.write_fields(),
            WireCommands::WrongHost(wrong_host_cmd) => wrong_host_cmd.write_fields(),
            WireCommands::SegmentIsSealed(seg_is_sealed_cmd) => seg_is_sealed_cmd.write_fields(),
            WireCommands::SegmentAlreadyExists(seg_already_exists_cmd) => seg_already_exists_cmd.write_fields(),
            WireCommands::SegmentIsTruncated(seg_is_truncated_cmd) => seg_is_truncated_cmd.write_fields(),
            WireCommands::NoSuchSegment(no_such_seg_cmd) =>no_such_seg_cmd.write_fields(),
            WireCommands::TableSegmentNotEmpty(table_seg_not_empty_cmd) => table_seg_not_empty_cmd.write_fields(),
            WireCommands::InvalidEventNumber(invalid_event_num_cmd) => invalid_event_num_cmd.write_fields(),
        }
    }
}

impl Decode for WireCommands {
    fn read_from(type_code: i32, input: &Vec<u8>) -> WireCommands {
        //TODO: Remove type_code in method parameters, decode from input.
        match type_code {
            HelloCommand::TYPE_CODE => WireCommands::Hello(HelloCommand::read_from(input)),
            WrongHostCommand::TYPE_CODE => WireCommands::WrongHost(WrongHostCommand::read_from(input)),
            SegmentIsSealedCommand::TYPE_CODE => WireCommands::SegmentIsSealed(SegmentIsSealedCommand::read_from(input)),
            SegmentAlreadyExistsCommand::TYPE_CODE => WireCommands::SegmentAlreadyExists(SegmentAlreadyExistsCommand::read_from(input)),
            SegmentIsTruncatedCommand::TYPE_CODE => WireCommands::SegmentIsTruncated(SegmentIsTruncatedCommand::read_from(input)),
            NoSuchSegmentCommand::TYPE_CODE => WireCommands::NoSuchSegment(NoSuchSegmentCommand::read_from(input)),
            TableSegmentNotEmptyCommand::TYPE_CODE => WireCommands::TableSegmentNotEmpty(TableSegmentNotEmptyCommand::read_from(input)),
            InvalidEventNumberCommand::TYPE_CODE => WireCommands::InvalidEventNumber(InvalidEventNumberCommand::read_from(input)),
            _ => panic!("Wrong input")
        }
    }
}