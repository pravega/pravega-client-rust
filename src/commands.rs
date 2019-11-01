use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::de::{self, Visitor, Unexpected};
use std::fmt;
use byteorder::{BigEndian, WriteBytesExt};
use bincode::Config;

/**
 * trait for Command.
 */
pub trait Command {
    const TYPE_CODE: i32;
    fn write_fields(&self) -> Vec<u8>;
    fn read_from(input :&Vec<u8>) -> Self;

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

/**
 * Wrap String to follow Java Serialize/Deserialize Style.
 *
 */
pub struct JavaString(pub String);

impl fmt::Debug for JavaString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for JavaString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialEq for JavaString {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Serialize for JavaString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        // get the length of the String.
        let length = self.0.len() as u16;
        // get the content of the String.
        let binary = self.0.as_bytes();
        // Serialize
        let mut content = vec![];
        content.write_u16::<BigEndian>(length).unwrap();
        content.extend(binary);
        serializer.serialize_bytes(&content)
    }
}

struct JavaStringVisitor;

impl<'de> Visitor<'de> for JavaStringVisitor {
    type Value = JavaString;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> fmt::Result {
        formatter.write_str("A Byte buffer which contains length and content")
    }

    fn visit_borrowed_bytes<E>(self, value: &'de [u8]) -> Result<Self::Value, E>
        where
            E: de::Error
    {
        // get the length
        let _length = ((value[0] as u16) << 8) | value[1] as u16;
        // construct the JavaString
        let content = String::from_utf8_lossy(&value[2..]).into_owned();

        if _length == content.len() as u16 {
            Ok(JavaString(content))
        } else {
            Err(de::Error::invalid_value(Unexpected::Bytes(value), &self))
        }

    }
}

impl <'de>Deserialize<'de> for JavaString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(JavaStringVisitor)
    }
}


/*
 * bincode serialize and deserialize config
 */
lazy_static! {
    static ref CONFIG: Config =  {
        let mut config = bincode::config();
        config.big_endian();
        config
    };
}

/**
 * 1. Hello Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct HelloCommand {
    pub high_version: i32,
    pub low_version: i32,
}

impl Command for HelloCommand {
    const TYPE_CODE: i32 = -127;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> HelloCommand {
        let decoded: HelloCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct WrongHostCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub correct_host: JavaString,
    pub server_stack_trace: JavaString,
}

impl Command for WrongHostCommand {
    const TYPE_CODE: i32 = 50;
    fn write_fields(&self) -> Vec<u8> {
        let encoded =CONFIG.serialize(&self).unwrap();
        encoded
    }
    fn read_from(input: &Vec<u8>) -> WrongHostCommand {
        let decoded: WrongHostCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SegmentIsSealedCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub server_stack_trace: JavaString,
    pub offset: i64,
}

impl Command for SegmentIsSealedCommand {
    const TYPE_CODE: i32 = 51;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentIsSealedCommand {
        let decoded: SegmentIsSealedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SegmentIsTruncatedCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub start_offset: i64,
    pub server_stack_trace: JavaString,
    pub offset: i64,

}

impl Command for SegmentIsTruncatedCommand {
    const TYPE_CODE: i32 = 56;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentIsTruncatedCommand {
        let decoded: SegmentIsTruncatedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SegmentAlreadyExistsCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub server_stack_trace: JavaString,
}

impl Command for SegmentAlreadyExistsCommand {
    const TYPE_CODE: i32 = 52;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentAlreadyExistsCommand {
        let decoded: SegmentAlreadyExistsCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct NoSuchSegmentCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub server_stack_trace: JavaString,
    pub offset: i64,
}

impl Command for NoSuchSegmentCommand {
    const TYPE_CODE: i32 = 53;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> NoSuchSegmentCommand {
        let decoded: NoSuchSegmentCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableSegmentNotEmptyCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub server_stack_trace: JavaString,
}

impl Command for TableSegmentNotEmptyCommand {
    const TYPE_CODE: i32 = 80;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> TableSegmentNotEmptyCommand {
        let decoded: TableSegmentNotEmptyCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct InvalidEventNumberCommand {
    pub writer_id: u128,
    pub event_number: i64,
    pub server_stack_trace: JavaString,
}

impl Command for InvalidEventNumberCommand {
    const TYPE_CODE: i32 = 55;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> InvalidEventNumberCommand {
        let decoded: InvalidEventNumberCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
        write!(f, "Invalid event number: {} for writer: {}", self.event_number, self.writer_id)
    }
}

/**
 * 9. OperationUnsupported Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct OperationUnsupportedCommand {
    pub request_id: i64,
    pub operation_name: JavaString,
    pub server_stack_trace: JavaString,
}

impl Command for OperationUnsupportedCommand {
    const TYPE_CODE: i32 = 57;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> OperationUnsupportedCommand {
        let decoded: OperationUnsupportedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PaddingCommand {
    pub length: i32,
}

impl Command for PaddingCommand {
    const TYPE_CODE: i32 = -1;
    // TODO: The padding command needs custom serialize and deserialize method
    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> PaddingCommand {
        let decoded: PaddingCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
    }
}

/**
 * 11. PartialEvent Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PartialEventCommand {
    pub data: Vec<u8>
}

impl Command for PartialEventCommand {
    const TYPE_CODE: i32 = -2;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> PartialEventCommand {
        let decoded: PartialEventCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
    }
}

/**
 * 12. Event Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct EventCommand {
    pub data: Vec<u8>
}

impl Command for EventCommand {
    const TYPE_CODE: i32 = 0;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> EventCommand {
        let decoded: EventCommand = CONFIG.deserialize(&input[..]).unwrap();
    }
}

/**
 * 13. SetupAppend Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SetupAppendCommand {
    pub request_id: i64,
    pub writer_id: u128,
    pub segment: JavaString,
    pub delegation_token: JavaString,
}

impl Command for SetupAppendCommand {
    const TYPE_CODE: i32 = 1;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SetupAppendCommand {
        let decoded: SetupAppendCommand = CONFIG.deserialize(&input[..]).unwrap();
       
    }
}

impl Request for SetupAppendCommand {
    fn get_request_id(&self) -> i64  {
        self.request_id
    }
 }


/**
 * 14. AppendBlock Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AppendBlockCommand {
    pub writer_id: u128,
    pub data: Vec<u8>,

}

impl Command for AppendBlockCommand {
    const TYPE_CODE: i32 = 3;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> AppendBlockCommand {
        let decoded: AppendBlockCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
    }
}

/**
 * 15. AppendBlockEnd Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AppendBlockEndCommand {
    pub writer_id: u128,
    pub size_of_whole_events: i32,
    pub data: Vec<u8>,
    pub num_event: i32,
    pub last_event_number: i64,
    pub request_id: i64,
}

impl Command for AppendBlockEndCommand {
    const TYPE_CODE: i32 = 4;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> AppendBlockEndCommand {
        let decoded: AppendBlockEndCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
    }
}

/**
 * 16.ConditionalAppend Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ConditionalAppendCommand {
    pub writer_id: u128,
    pub event_number: i64,
    pub expected_offset: i64,
    pub request_id: i64,
    pub event: EventCommand
}

impl Command for ConditionalAppendCommand {
    const TYPE_CODE: i32 = 5;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> ConditionalAppendCommand {
        let decoded: ConditionalAppendCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AppendSetupCommand {
    pub writer_id: u128,
    pub request_id: i64,
    pub last_event_number: i64,
    pub segment: JavaString,
}

impl Command for AppendSetupCommand {
    const TYPE_CODE: i32 = 2;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> Self {
        let decoded: AppendSetupCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
    }
}

impl Reply for AppendSetupCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}
