//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use super::error::CommandError;
use super::error::InvalidData;
use super::error::Io;
use bincode2::Config;
use bincode2::LengthOption;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use lazy_static::*;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::i64;
use std::io::Cursor;
use std::io::{Read, Write};

pub const WIRE_VERSION: i32 = 11;
pub const OLDEST_COMPATIBLE_VERSION: i32 = 5;
pub const TYPE_SIZE: u32 = 4;
pub const TYPE_PLUS_LENGTH_SIZE: u32 = 8;
pub const MAX_WIRECOMMAND_SIZE: u32 = 0x00FF_FFFF; // 16MB-1

/**
 * trait for Command.
 */
pub trait Command {
    const TYPE_CODE: i32;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError>;
    fn read_from(input: &[u8]) -> Result<Self, CommandError>
    where
        Self: Sized;
}

/**
 * trait for Request
 */
pub trait Request {
    fn get_request_id(&self) -> i64;
    fn must_log(&self) -> bool {
        true
    }
}

/**
 * trait for Reply
 */
pub trait Reply {
    fn get_request_id(&self) -> i64;
    fn is_failure(&self) -> bool {
        false
    }
}

/*
 * bincode serialize and deserialize config
 */
lazy_static! {
    static ref CONFIG: Config = {
        let mut config = bincode2::config();
        config.big_endian();
        config.limit(MAX_WIRECOMMAND_SIZE.into());
        config.array_length(LengthOption::U32);
        config.string_length(LengthOption::U16);
        config
    };
}

/**
 * 1. Hello Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct HelloCommand {
    pub high_version: i32,
    pub low_version: i32,
}

impl Command for HelloCommand {
    const TYPE_CODE: i32 = -127;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: HelloCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for HelloCommand {
    fn get_request_id(&self) -> i64 {
        0
    }
}

impl Reply for HelloCommand {
    fn get_request_id(&self) -> i64 {
        0
    }
}

/**
 * 2. WrongHost Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct WrongHostCommand {
    pub request_id: i64,
    pub segment: String,
    pub correct_host: String,
    pub server_stack_trace: String,
}

impl Command for WrongHostCommand {
    const TYPE_CODE: i32 = 50;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }
    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: WrongHostCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for WrongHostCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
    fn is_failure(&self) -> bool {
        true
    }
}

/**
 * 3. SegmentIsSealed Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SegmentIsSealedCommand {
    pub request_id: i64,
    pub segment: String,
    pub server_stack_trace: String,
    pub offset: i64,
}

impl Command for SegmentIsSealedCommand {
    const TYPE_CODE: i32 = 51;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SegmentIsSealedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for SegmentIsSealedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
    fn is_failure(&self) -> bool {
        true
    }
}

/**
 * 4. SegmentIsTruncated Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SegmentIsTruncatedCommand {
    pub request_id: i64,
    pub segment: String,
    pub start_offset: i64,
    pub server_stack_trace: String,
    pub offset: i64,
}

impl Command for SegmentIsTruncatedCommand {
    const TYPE_CODE: i32 = 56;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SegmentIsTruncatedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for SegmentIsTruncatedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
    fn is_failure(&self) -> bool {
        true
    }
}

/**
 * 5. SegmentAlreadyExists Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SegmentAlreadyExistsCommand {
    pub request_id: i64,
    pub segment: String,
    pub server_stack_trace: String,
}

impl Command for SegmentAlreadyExistsCommand {
    const TYPE_CODE: i32 = 52;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SegmentAlreadyExistsCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for SegmentAlreadyExistsCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
    fn is_failure(&self) -> bool {
        true
    }
}

impl fmt::Display for SegmentAlreadyExistsCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Segment already exists: {}", self.segment)
    }
}

/**
 * 6. NoSuchSegment Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct NoSuchSegmentCommand {
    pub request_id: i64,
    pub segment: String,
    pub server_stack_trace: String,
    pub offset: i64,
}

impl Command for NoSuchSegmentCommand {
    const TYPE_CODE: i32 = 53;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: NoSuchSegmentCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for NoSuchSegmentCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
    fn is_failure(&self) -> bool {
        true
    }
}

impl fmt::Display for NoSuchSegmentCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "No such segment: {}", self.segment)
    }
}

/**
 * 7. TableSegmentNotEmpty Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TableSegmentNotEmptyCommand {
    pub request_id: i64,
    pub segment: String,
    pub server_stack_trace: String,
}

impl Command for TableSegmentNotEmptyCommand {
    const TYPE_CODE: i32 = 80;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: TableSegmentNotEmptyCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for TableSegmentNotEmptyCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
    fn is_failure(&self) -> bool {
        true
    }
}

impl fmt::Display for TableSegmentNotEmptyCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Table Segment is not empty: {}", self.segment)
    }
}

/**
 * 8. InvalidEventNumber Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct InvalidEventNumberCommand {
    pub writer_id: u128,
    pub event_number: i64,
    pub server_stack_trace: String,
}

impl Command for InvalidEventNumberCommand {
    const TYPE_CODE: i32 = 55;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: InvalidEventNumberCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for InvalidEventNumberCommand {
    fn get_request_id(&self) -> i64 {
        self.event_number
    }
    fn is_failure(&self) -> bool {
        true
    }
}

impl fmt::Display for InvalidEventNumberCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Invalid event number: {} for writer: {}",
            self.event_number, self.writer_id,
        )
    }
}

/**
 * 9. OperationUnsupported Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct OperationUnsupportedCommand {
    pub request_id: i64,
    pub operation_name: String,
    pub server_stack_trace: String,
}

impl Command for OperationUnsupportedCommand {
    const TYPE_CODE: i32 = 57;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: OperationUnsupportedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for OperationUnsupportedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
    fn is_failure(&self) -> bool {
        true
    }
}

/**
 * 10. Padding Command
 */
#[derive(PartialEq, Debug, Clone)]
pub struct PaddingCommand {
    pub length: i32,
}

impl Command for PaddingCommand {
    const TYPE_CODE: i32 = -1;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let res = vec![0; self.length as usize];
        Ok(res)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        //FIXME: In java we use skipBytes to remove these padding bytes.
        //FIXME: I think we don't need to do in rust.
        Ok(PaddingCommand {
            length: input.len() as i32,
        })
    }
}

/**
 * 11. PartialEvent Command
 */
#[derive(PartialEq, Debug, Clone)]
pub struct PartialEventCommand {
    pub data: Vec<u8>,
}

impl Command for PartialEventCommand {
    const TYPE_CODE: i32 = -2;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        //FIXME: In java, we use data.getBytes(data.readerIndex(), (OutputStream) out, data.readableBytes());
        // which means the result would not contain the prefix length;
        // so in rust we can directly return data.
        Ok(self.data.clone())
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        Ok(PartialEventCommand { data: input.to_vec() })
    }
}

/**
 * 12. Event Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct EventCommand {
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

impl Command for EventCommand {
    const TYPE_CODE: i32 = 0;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let mut res = Vec::new();
        res.extend_from_slice(&EventCommand::TYPE_CODE.to_be_bytes());
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        res.extend(encoded);
        Ok(res)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        //read the type_code.
        let _type_code = BigEndian::read_i32(input);
        let decoded: EventCommand = CONFIG.deserialize(&input[4..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

/**
 * 13. SetupAppend Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SetupAppendCommand {
    pub request_id: i64,
    pub writer_id: u128,
    pub segment: String,
    pub delegation_token: String,
}

impl Command for SetupAppendCommand {
    const TYPE_CODE: i32 = 1;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SetupAppendCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for SetupAppendCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 14. AppendBlock Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct AppendBlockCommand {
    pub writer_id: u128,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

impl Command for AppendBlockCommand {
    const TYPE_CODE: i32 = 3;
    //FIXME: The serialize and deserialize method need to customize;
    // In JAVA, it doesn't write data(because it'empty), but here it will write the prefix length(0).
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: AppendBlockCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

/**
 * 15. AppendBlockEnd Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct AppendBlockEndCommand {
    pub writer_id: u128,
    pub size_of_whole_events: i32,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub num_event: i32,
    pub last_event_number: i64,
    pub request_id: i64,
}

impl Command for AppendBlockEndCommand {
    const TYPE_CODE: i32 = 4;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: AppendBlockEndCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

/**
 * 16.ConditionalAppend Command
 */
#[derive(PartialEq, Debug, Clone)]
pub struct ConditionalAppendCommand {
    pub writer_id: u128,
    pub event_number: i64,
    pub expected_offset: i64,
    pub event: EventCommand,
    pub request_id: i64,
}

impl ConditionalAppendCommand {
    fn read_event(rdr: &mut Cursor<&[u8]>) -> Result<EventCommand, std::io::Error> {
        let _type_code = rdr.read_i32::<BigEndian>()?;
        let event_length = rdr.read_u32::<BigEndian>()?;
        // read the data in event
        let mut msg: Vec<u8> = vec![0; event_length as usize];
        rdr.read_exact(&mut msg)?;
        Ok(EventCommand { data: msg })
    }
}

impl Command for ConditionalAppendCommand {
    const TYPE_CODE: i32 = 5;
    // Customize the serialize and deserialize method.
    // Because in ConditionalAppend the event should be serialize as |type_code|length|data|
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let mut res = Vec::new();
        res.extend_from_slice(&self.writer_id.to_be_bytes());
        res.extend_from_slice(&self.event_number.to_be_bytes());
        res.extend_from_slice(&self.expected_offset.to_be_bytes());
        res.write_all(&self.event.write_fields()?).context(Io {
            command_type: Self::TYPE_CODE,
        })?;
        res.extend_from_slice(&self.request_id.to_be_bytes());
        Ok(res)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let mut rdr = Cursor::new(input);
        let ctx = Io {
            command_type: Self::TYPE_CODE,
        };
        let writer_id = rdr.read_u128::<BigEndian>().context(ctx)?;
        let event_number = rdr.read_i64::<BigEndian>().context(ctx)?;
        let expected_offset = rdr.read_i64::<BigEndian>().context(ctx)?;
        let event = ConditionalAppendCommand::read_event(&mut rdr).context(ctx)?;
        let request_id = rdr.read_i64::<BigEndian>().context(ctx)?;
        Ok(ConditionalAppendCommand {
            writer_id,
            event_number,
            expected_offset,
            event,
            request_id,
        })
    }
}

impl Request for ConditionalAppendCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 *  17. AppendSetup Command.
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct AppendSetupCommand {
    pub request_id: i64,
    pub segment: String,
    pub writer_id: u128,
    pub last_event_number: i64,
}

impl Command for AppendSetupCommand {
    const TYPE_CODE: i32 = 2;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: AppendSetupCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for AppendSetupCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 18. DataAppended Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct DataAppendedCommand {
    pub writer_id: u128,
    pub event_number: i64,
    pub previous_event_number: i64,
    pub request_id: i64,
    pub current_segment_write_offset: i64,
}

impl Command for DataAppendedCommand {
    const TYPE_CODE: i32 = 7;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: DataAppendedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for DataAppendedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 19. ConditionalCheckFailed Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ConditionalCheckFailedCommand {
    pub writer_id: u128,
    pub event_number: i64,
    pub request_id: i64,
}

impl Command for ConditionalCheckFailedCommand {
    const TYPE_CODE: i32 = 8;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: ConditionalCheckFailedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for ConditionalCheckFailedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 20. ReadSegment Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ReadSegmentCommand {
    pub segment: String,
    pub offset: i64,
    pub suggested_length: i32,
    pub delegation_token: String,
    pub request_id: i64,
}

impl Command for ReadSegmentCommand {
    const TYPE_CODE: i32 = 9;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: ReadSegmentCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for ReadSegmentCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 21. SegmentRead Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SegmentReadCommand {
    pub segment: String,
    pub offset: i64,
    pub at_tail: bool,
    pub end_of_segment: bool,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub request_id: i64,
}

impl Command for SegmentReadCommand {
    const TYPE_CODE: i32 = 10;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SegmentReadCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for SegmentReadCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 22. GetSegmentAttribute Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct GetSegmentAttributeCommand {
    pub request_id: i64,
    pub segment_name: String,
    pub attribute_id: u128,
    pub delegation_token: String,
}

impl Command for GetSegmentAttributeCommand {
    const TYPE_CODE: i32 = 34;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: GetSegmentAttributeCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for GetSegmentAttributeCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 23. SegmentAttribute Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SegmentAttributeCommand {
    pub request_id: i64,
    pub value: i64,
}

impl Command for SegmentAttributeCommand {
    const TYPE_CODE: i32 = 35;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SegmentAttributeCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for SegmentAttributeCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 24. UpdateSegmentAttribute Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct UpdateSegmentAttributeCommand {
    pub request_id: i64,
    pub segment_name: String,
    pub attribute_id: u128,
    pub new_value: i64,
    pub expected_value: i64,
    pub delegation_token: String,
}

impl Command for UpdateSegmentAttributeCommand {
    const TYPE_CODE: i32 = 36;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: UpdateSegmentAttributeCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for UpdateSegmentAttributeCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 25. SegmentAttributeUpdated Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SegmentAttributeUpdatedCommand {
    pub request_id: i64,
    pub success: bool,
}

impl Command for SegmentAttributeUpdatedCommand {
    const TYPE_CODE: i32 = 37;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SegmentAttributeUpdatedCommand =
            CONFIG.deserialize(&input[..]).context(InvalidData {
                command_type: Self::TYPE_CODE,
            })?;
        Ok(decoded)
    }
}

impl Reply for SegmentAttributeUpdatedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 26. GetStreamSegmentInfo Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct GetStreamSegmentInfoCommand {
    pub request_id: i64,
    pub segment_name: String,
    pub delegation_token: String,
}

impl Command for GetStreamSegmentInfoCommand {
    const TYPE_CODE: i32 = 11;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: GetStreamSegmentInfoCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for GetStreamSegmentInfoCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 27. StreamSegmentInfo Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct StreamSegmentInfoCommand {
    pub request_id: i64,
    pub segment_name: String,
    pub exists: bool,
    pub is_sealed: bool,
    pub is_deleted: bool,
    pub last_modified: i64,
    pub write_offset: i64,
    pub start_offset: i64,
}

impl Command for StreamSegmentInfoCommand {
    const TYPE_CODE: i32 = 12;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: StreamSegmentInfoCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for StreamSegmentInfoCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 28. CreateSegment Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct CreateSegmentCommand {
    pub request_id: i64,
    pub segment: String,
    pub target_rate: i32,
    pub scale_type: u8,
    pub delegation_token: String,
}

impl Command for CreateSegmentCommand {
    const TYPE_CODE: i32 = 20;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: CreateSegmentCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for CreateSegmentCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 29. CreateTableSegment Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct CreateTableSegmentCommand {
    pub request_id: i64,
    pub segment: String,
    pub delegation_token: String,
}

impl Command for CreateTableSegmentCommand {
    const TYPE_CODE: i32 = 70;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: CreateTableSegmentCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for CreateTableSegmentCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 30. SegmentCreated Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SegmentCreatedCommand {
    pub request_id: i64,
    pub segment: String,
}

impl Command for SegmentCreatedCommand {
    const TYPE_CODE: i32 = 21;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SegmentCreatedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for SegmentCreatedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 31. UpdateSegmentPolicy Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct UpdateSegmentPolicyCommand {
    pub request_id: i64,
    pub segment: String,
    pub target_rate: i32,
    pub scale_type: u8,
    pub delegation_token: String,
}

impl Command for UpdateSegmentPolicyCommand {
    const TYPE_CODE: i32 = 32;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: UpdateSegmentPolicyCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for UpdateSegmentPolicyCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 32. SegmentPolicyUpdated Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SegmentPolicyUpdatedCommand {
    pub request_id: i64,
    pub segment: String,
}

impl Command for SegmentPolicyUpdatedCommand {
    const TYPE_CODE: i32 = 33;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SegmentPolicyUpdatedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for SegmentPolicyUpdatedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 33. MergeSegments Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct MergeSegmentsCommand {
    pub request_id: i64,
    pub target: String,
    pub source: String,
    pub delegation_token: String,
}

impl Command for MergeSegmentsCommand {
    const TYPE_CODE: i32 = 58;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: MergeSegmentsCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for MergeSegmentsCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}
/**
 * 34. MergeTableSegments Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct MergeTableSegmentsCommand {
    pub request_id: i64,
    pub target: String,
    pub source: String,
    pub delegation_token: String,
}

impl Command for MergeTableSegmentsCommand {
    const TYPE_CODE: i32 = 72;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: MergeTableSegmentsCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for MergeTableSegmentsCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 35. SegmentsMerged Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SegmentsMergedCommand {
    pub request_id: i64,
    pub target: String,
    pub source: String,
    pub new_target_write_offset: i64,
}

impl Command for SegmentsMergedCommand {
    const TYPE_CODE: i32 = 59;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SegmentsMergedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for SegmentsMergedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 36. SealSegment Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SealSegmentCommand {
    pub request_id: i64,
    pub segment: String,
    pub delegation_token: String,
}

impl Command for SealSegmentCommand {
    const TYPE_CODE: i32 = 28;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SealSegmentCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for SealSegmentCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 37. SealTableSegment Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SealTableSegmentCommand {
    pub request_id: i64,
    pub segment: String,
    pub delegation_token: String,
}

impl Command for SealTableSegmentCommand {
    const TYPE_CODE: i32 = 73;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SealTableSegmentCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for SealTableSegmentCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 38. SegmentSealed Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SegmentSealedCommand {
    pub request_id: i64,
    pub segment: String,
}

impl Command for SegmentSealedCommand {
    const TYPE_CODE: i32 = 29;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SegmentSealedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for SegmentSealedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 39. TruncateSegment Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TruncateSegmentCommand {
    pub request_id: i64,
    pub segment: String,
    pub truncation_offset: i64,
    pub delegation_token: String,
}

impl Command for TruncateSegmentCommand {
    const TYPE_CODE: i32 = 38;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: TruncateSegmentCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for TruncateSegmentCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 40. SegmentTruncated Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SegmentTruncatedCommand {
    pub request_id: i64,
    pub segment: String,
}

impl Command for SegmentTruncatedCommand {
    const TYPE_CODE: i32 = 39;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SegmentTruncatedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for SegmentTruncatedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 41. DeleteSegment Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct DeleteSegmentCommand {
    pub request_id: i64,
    pub segment: String,
    pub delegation_token: String,
}

impl Command for DeleteSegmentCommand {
    const TYPE_CODE: i32 = 30;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: DeleteSegmentCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for DeleteSegmentCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 42. DeleteTableSegment Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct DeleteTableSegmentCommand {
    pub request_id: i64,
    pub segment: String,
    pub must_be_empty: bool, // If true, the Table Segment will only be deleted if it is empty (contains no keys)
    pub delegation_token: String,
}

impl Command for DeleteTableSegmentCommand {
    const TYPE_CODE: i32 = 71;
    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: DeleteTableSegmentCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for DeleteTableSegmentCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 43. SegmentDeleted Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SegmentDeletedCommand {
    pub request_id: i64,
    pub segment: String,
}

impl Command for SegmentDeletedCommand {
    const TYPE_CODE: i32 = 31;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: SegmentDeletedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for SegmentDeletedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}
/**
 * 44. KeepAlive Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct KeepAliveCommand {}

impl Command for KeepAliveCommand {
    const TYPE_CODE: i32 = 100;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let res: Vec<u8> = Vec::new();
        Ok(res)
    }

    fn read_from(_input: &[u8]) -> Result<Self, CommandError> {
        Ok(KeepAliveCommand {})
    }
}

impl Request for KeepAliveCommand {
    fn get_request_id(&self) -> i64 {
        -1
    }
    fn must_log(&self) -> bool {
        false
    }
}

impl Reply for KeepAliveCommand {
    fn get_request_id(&self) -> i64 {
        -1
    }
}

/**
 * 45. AuthTokenCheckFailed Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct AuthTokenCheckFailedCommand {
    pub request_id: i64,
    pub server_stack_trace: String,
    pub error_code: i32,
}

impl AuthTokenCheckFailedCommand {
    pub fn is_token_expired(&self) -> bool {
        ErrorCode::value_of(self.error_code) == ErrorCode::TokenExpired
    }

    pub fn get_error_code(&self) -> ErrorCode {
        ErrorCode::value_of(self.error_code)
    }
}

impl Command for AuthTokenCheckFailedCommand {
    const TYPE_CODE: i32 = 60;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: AuthTokenCheckFailedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for AuthTokenCheckFailedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum ErrorCode {
    Unspecified,
    TokenCheckedFailed,
    TokenExpired,
}

impl ErrorCode {
    pub fn get_code(error_code: &ErrorCode) -> i32 {
        match error_code {
            ErrorCode::Unspecified => -1,
            ErrorCode::TokenCheckedFailed => 0,
            ErrorCode::TokenExpired => 1,
        }
    }
    pub fn value_of(code: i32) -> ErrorCode {
        match code {
            -1 => ErrorCode::Unspecified,
            0 => ErrorCode::TokenCheckedFailed,
            1 => ErrorCode::TokenExpired,
            _ => panic!("Unknown value: {}", code),
        }
    }
}

/**
 * 46. UpdateTableEntries Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct UpdateTableEntriesCommand {
    pub request_id: i64,
    pub segment: String,
    pub delegation_token: String,
    pub table_entries: TableEntries,
    pub table_segment_offset: i64,
}

impl Command for UpdateTableEntriesCommand {
    const TYPE_CODE: i32 = 74;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: UpdateTableEntriesCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for UpdateTableEntriesCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 47. TableEntriesUpdated Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TableEntriesUpdatedCommand {
    pub request_id: i64,
    pub updated_versions: Vec<i64>,
}

impl Command for TableEntriesUpdatedCommand {
    const TYPE_CODE: i32 = 75;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: TableEntriesUpdatedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for TableEntriesUpdatedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 48. RemoveTableKeys Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct RemoveTableKeysCommand {
    pub request_id: i64,
    pub segment: String,
    pub delegation_token: String,
    pub keys: Vec<TableKey>,
    pub table_segment_offset: i64,
}

impl Command for RemoveTableKeysCommand {
    const TYPE_CODE: i32 = 76;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: RemoveTableKeysCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for RemoveTableKeysCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 49. TableKeysRemoved Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TableKeysRemovedCommand {
    pub request_id: i64,
    pub segment: String,
}

impl Command for TableKeysRemovedCommand {
    const TYPE_CODE: i32 = 77;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: TableKeysRemovedCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for TableKeysRemovedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 50. ReadTable Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ReadTableCommand {
    pub request_id: i64,
    pub segment: String,
    pub delegation_token: String,
    pub keys: Vec<TableKey>,
}

impl Command for ReadTableCommand {
    const TYPE_CODE: i32 = 78;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: ReadTableCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for ReadTableCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 51. TableRead Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TableReadCommand {
    pub request_id: i64,
    pub segment: String,
    pub entries: TableEntries,
}

impl Command for TableReadCommand {
    const TYPE_CODE: i32 = 79;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: TableReadCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for TableReadCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 52. ReadTableKeys Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ReadTableKeysCommand {
    pub request_id: i64,
    pub segment: String,
    pub delegation_token: String,
    pub suggested_key_count: i32,
    pub continuation_token: Vec<u8>,
}

impl Command for ReadTableKeysCommand {
    const TYPE_CODE: i32 = 83;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: ReadTableKeysCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for ReadTableKeysCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 53. TableKeysRead Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TableKeysReadCommand {
    pub request_id: i64,
    pub segment: String,
    pub keys: Vec<TableKey>,
    pub continuation_token: Vec<u8>, // this is used to indicate the point from which the next keys should be fetched.
}

impl Command for TableKeysReadCommand {
    const TYPE_CODE: i32 = 84;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: TableKeysReadCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for TableKeysReadCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 54. ReadTableEntries Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ReadTableEntriesCommand {
    pub request_id: i64,
    pub segment: String,
    pub delegation_token: String,
    pub suggested_entry_count: i32,
    pub continuation_token: Vec<u8>,
}

impl Command for ReadTableEntriesCommand {
    const TYPE_CODE: i32 = 85;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: ReadTableEntriesCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for ReadTableEntriesCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 55. TableEntriesRead Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TableEntriesReadCommand {
    pub request_id: i64,
    pub segment: String,
    pub entries: TableEntries,
    pub continuation_token: Vec<u8>,
}

impl Command for TableEntriesReadCommand {
    const TYPE_CODE: i32 = 86;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: TableEntriesReadCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for TableEntriesReadCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 56. TableKeyDoesNotExist Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TableKeyDoesNotExistCommand {
    pub request_id: i64,
    pub segment: String,
    pub server_stack_trace: String,
}

impl Command for TableKeyDoesNotExistCommand {
    const TYPE_CODE: i32 = 81;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: TableKeyDoesNotExistCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for TableKeyDoesNotExistCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }

    fn is_failure(&self) -> bool {
        true
    }
}

impl fmt::Display for TableKeyDoesNotExistCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Conditional table update failed since the key does not exist : {}",
            self.segment
        )
    }
}

/**
 * 57. TableKeyBadVersion Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TableKeyBadVersionCommand {
    pub request_id: i64,
    pub segment: String,
    pub server_stack_trace: String,
}

impl Command for TableKeyBadVersionCommand {
    const TYPE_CODE: i32 = 82;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: TableKeyBadVersionCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for TableKeyBadVersionCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }

    fn is_failure(&self) -> bool {
        true
    }
}

impl fmt::Display for TableKeyBadVersionCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Conditional table update failed since the key version is incorrect : {}",
            self.segment
        )
    }
}

/**
 * Table Key Struct.
 * Need to override the serialize
 */
#[derive(Serialize, Deserialize, Debug, Clone, Eq)]
pub struct TableKey {
    pub payload: i32,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub key_version: i64,
}

impl TableKey {
    pub const KEY_NO_VERSION: i64 = i64::min_value();
    pub const KEY_NOT_EXISTS: i64 = -1i64;
    const HEADER_BYTES: i32 = 2 * 4;
    pub fn new(data: Vec<u8>, key_version: i64) -> TableKey {
        let payload = (4 + data.len() + 8) as i32;
        TableKey {
            payload,
            data,
            key_version,
        }
    }
}

impl Hash for TableKey {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.data.hash(state);
    }
}

impl PartialEq for TableKey {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TableValue {
    pub payload: i32,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

impl TableValue {
    const HEADER_BYTES: i32 = 2 * 4;
    pub fn new(data: Vec<u8>) -> TableValue {
        let payload = (data.len() + 4) as i32;
        TableValue { payload, data }
    }
}

/**
 * TableEntries Struct.
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TableEntries {
    pub entries: Vec<(TableKey, TableValue)>,
}

impl TableEntries {
    fn get_header_byte(entry_count: i32) -> i32 {
        4 + entry_count * (TableKey::HEADER_BYTES + TableValue::HEADER_BYTES)
    }

    pub fn size(&self) -> i32 {
        let mut data_bytes = 0;
        for x in &self.entries {
            data_bytes += (x.0.data.len() + 8 + x.1.data.len()) as i32
        }

        data_bytes + TableEntries::get_header_byte(self.entries.len() as i32)
    }
}

/**
 * 58 ReadTableEntriesDelta Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ReadTableEntriesDeltaCommand {
    pub request_id: i64,
    pub segment: String,
    pub delegation_token: String,
    pub from_position: i64,
    pub suggested_entry_count: i32,
}

impl Command for ReadTableEntriesDeltaCommand {
    const TYPE_CODE: i32 = 88;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: ReadTableEntriesDeltaCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for ReadTableEntriesDeltaCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 59 TableEntriesDeltaRead Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TableEntriesDeltaReadCommand {
    pub request_id: i64,
    pub segment: String,
    pub entries: TableEntries,
    pub should_clear: bool,
    pub reached_end: bool,
    pub last_position: i64,
}

impl Command for TableEntriesDeltaReadCommand {
    const TYPE_CODE: i32 = 87;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: TableEntriesDeltaReadCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Reply for TableEntriesDeltaReadCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 60.ConditionalAppendRawBytes Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ConditionalBlockEndCommand {
    pub writer_id: u128,
    pub event_number: i64,
    pub expected_offset: i64,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub request_id: i64,
}

impl Command for ConditionalBlockEndCommand {
    const TYPE_CODE: i32 = 89;

    fn write_fields(&self) -> Result<Vec<u8>, CommandError> {
        let encoded = CONFIG.serialize(&self).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(encoded)
    }

    fn read_from(input: &[u8]) -> Result<Self, CommandError> {
        let decoded: ConditionalBlockEndCommand = CONFIG.deserialize(&input[..]).context(InvalidData {
            command_type: Self::TYPE_CODE,
        })?;
        Ok(decoded)
    }
}

impl Request for ConditionalBlockEndCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}
