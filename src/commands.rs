use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::de::{self, Visitor, Unexpected};
use std::fmt;
use std::i64;
use byteorder::{BigEndian, WriteBytesExt};
use bincode::Config;
use std::collections::HashMap;

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
    // FIXME: The padding command needs custom serialize and deserialize method
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
    //FIXME: The event command needs custom serialize and deseialize methods
    //In JAVA Code the format is |type_code|length|data|
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
    //FIXME: The serialize and deserialize method need to customize.
    //In JAVA, it doesn't write data(because it'empty), but here it will write the data length(0).
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
    pub event: EventCommand,
    pub request_id: i64,

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
    pub request_id: i64,
    pub segment: JavaString,
    pub writer_id: u128,
    pub last_event_number: i64,

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

/**
 * 18. DataAppended Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DataAppendedCommand {
    pub request_id: i64,
    pub writer_id: u128,
    pub event_number: i64,
    pub previous_event_number: i64,
    pub current_segment_write_offset: i64
}

impl Command for DataAppendedCommand {
    const TYPE_CODE: i32 = 7;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> DataAppendedCommand {
        let decoded: DataAppendedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ConditionalCheckFailedCommand {
    pub writer_id: u128,
    pub request_id: i64,
    pub event_number: i64,
}

impl Command for ConditionalCheckFailedCommand  {
    const TYPE_CODE: i32 = 8;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> ConditionalCheckFailedCommand {
        let decoded: ConditionalCheckFailedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
    }
}

impl Reply for ConditionalAppendCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

/**
 * 20. ReadSegment Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ReadSegmentCommand {
    pub request_id: i64,
    pub offset: i64,
    pub segment: JavaString,
    pub suggested_length: i64,
    pub delegation_token: JavaString,
}

impl Command for ReadSegmentCommand  {
    const TYPE_CODE: i32 = 9;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> ReadSegmentCommand {
        let decoded: ReadSegmentCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SegmentReadCommand {
    pub request_id: i64,
    pub offset: i64,
    pub segment: JavaString,
    pub at_tail: bool,
    pub end_of_segment: bool,
    pub data: Vec<u8>
}

impl Command for SegmentReadCommand {
    const TYPE_CODE: i32 = 10;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentReadCommand {
        let decoded: SegmentReadCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct GetSegmentAttributeCommand {
    pub request_id: i64,
    pub segment_name: JavaString,
    pub attribute_id: u128,
    pub delegation_token: JavaString,
}

impl Command for GetSegmentAttributeCommand {
    const TYPE_CODE: i32 = 34;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> GetSegmentAttributeCommand {
        let decoded: GetSegmentAttributeCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SegmentAttributeCommand {
    pub request_id: i64,
    pub value: i64,
}

impl Command for SegmentAttributeCommand {
    const TYPE_CODE: i32 = 35;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentAttributeCommand {
        let decoded: SegmentAttributeCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct UpdateSegmentAttributeCommand {
    pub request_id: i64,
    pub segment_name: JavaString,
    pub attribute_id: u128,
    pub delegation_token: JavaString,
    pub new_value: i64,
    pub expected_value: i64,
}

impl Command for UpdateSegmentAttributeCommand {
    const TYPE_CODE: i32 = 36;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> UpdateSegmentAttributeCommand {
        let decoded: UpdateSegmentAttributeCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SegmentAttributeUpdatedCommand {
    pub request_id: i64,
    pub success: bool,
}

impl Command for SegmentAttributeUpdatedCommand {
    const TYPE_CODE: i32 = 37;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentAttributeUpdatedCommand {
        let decoded: SegmentAttributeUpdatedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct GetStreamSegmentInfoCommand {
    pub request_id: i64,
    pub segment_name: JavaString,
    pub delegation_token: JavaString,
}

impl Command for GetStreamSegmentInfoCommand {
    const TYPE_CODE: i32 = 11;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> GetStreamSegmentInfoCommand {
        let decoded: GetStreamSegmentInfoCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct StreamSegmentInfoCommand {
    pub request_id: i64,
    pub segment_name: JavaString,
    pub exists: bool,
    pub is_sealed: bool,
    pub is_deleted: bool,
    pub last_modified: i64,
    pub write_offset: i64,
    pub start_offset: i64,
}

impl Command for StreamSegmentInfoCommand {
    const TYPE_CODE: i32 = 12;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> StreamSegmentInfoCommand {
        let decoded: StreamSegmentInfoCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct CreateSegmentCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub scale_type: u8,
    pub target_rate: i32,
    pub delegation_token: JavaString,
}

impl Command for CreateSegmentCommand {
    const TYPE_CODE: i32 = 20;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> CreateSegmentCommand {
        let decoded: CreateSegmentCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct CreateTableSegmentCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub delegation_token: JavaString,
}

impl Command for CreateTableSegmentCommand {
    const TYPE_CODE: i32 = 70;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> CreateTableSegmentCommand {
        let decoded: CreateTableSegmentCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SegmentCreatedCommand {
    pub request_id: i64,
    pub segment: JavaString,
}

impl Command for SegmentCreatedCommand {
    const TYPE_CODE: i32 = 21;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentCreatedCommand {
        let decoded: SegmentCreatedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct UpdateSegmentPolicyCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub scale_type: u8,
    pub target_rate: i32,
    pub delegation_token: JavaString,
}

impl Command for UpdateSegmentPolicyCommand {
    const TYPE_CODE: i32 = 32;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> UpdateSegmentPolicyCommand {
        let decoded: UpdateSegmentPolicyCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SegmentPolicyUpdatedCommand {
    pub request_id: i64,
    pub segment: JavaString,
}

impl Command for SegmentPolicyUpdatedCommand {
    const TYPE_CODE: i32 = 33;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentPolicyUpdatedCommand {
        let decoded: SegmentPolicyUpdatedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct MergeSegmentsCommand {
    pub request_id: i64,
    pub target: JavaString,
    pub source: JavaString,
    pub delegation_token: JavaString,
}

impl Command for MergeSegmentsCommand {
    const TYPE_CODE: i32 = 58;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> MergeSegmentsCommand {
        let decoded: MergeSegmentsCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct MergeTableSegmentsCommand {
    pub request_id: i64,
    pub target: JavaString,
    pub source: JavaString,
    pub delegation_token: JavaString,
}

impl Command for MergeTableSegmentsCommand {
    const TYPE_CODE: i32 = 72;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> MergeTableSegmentsCommand {
        let decoded: MergeTableSegmentsCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct  SegmentsMergedCommand {
    pub request_id: i64,
    pub target: JavaString,
    pub source: JavaString,
    pub new_target_write_offset: i64,
}

impl Command for SegmentsMergedCommand {
    const TYPE_CODE: i32 = 59;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentsMergedCommand {
        let decoded: SegmentsMergedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SealSegmentCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub delegation_token: JavaString,
}

impl Command for SealSegmentCommand {
    const TYPE_CODE: i32 = 28;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SealSegmentCommand {
        let decoded: SealSegmentCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SealTableSegmentCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub delegation_token: JavaString,
}

impl Command for SealTableSegmentCommand {
    const TYPE_CODE: i32 = 73;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SealTableSegmentCommand {
        let decoded: SealTableSegmentCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SegmentSealedCommand {
    pub request_id: i64,
    pub segment: JavaString,
}

impl Command for SegmentSealedCommand {
    const TYPE_CODE: i32 = 29;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentSealedCommand {
        let decoded: SegmentSealedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TruncateSegmentCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub truncation_offset: i64,
    pub delegation_token: JavaString,

}

impl Command for TruncateSegmentCommand {
    const TYPE_CODE: i32 = 38;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> TruncateSegmentCommand {
        let decoded: TruncateSegmentCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SegmentTruncatedCommand {
    pub request_id: i64,
    pub segment: JavaString,
}

impl Command for SegmentTruncatedCommand {
    const TYPE_CODE: i32 = 39;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentTruncatedCommand {
        let decoded: SegmentTruncatedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DeleteSegmentCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub delegation_token: JavaString,
}

impl Command for DeleteSegmentCommand {
    const TYPE_CODE: i32 = 30;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> DeleteSegmentCommand {
        let decoded: DeleteSegmentCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DeleteTableSegmentCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub must_be_empty: bool, // If true, the Table Segment will only be deleted if it is empty (contains no keys)
    pub delegation_token: JavaString,
}

impl Command for DeleteTableSegmentCommand {
    const TYPE_CODE: i32 = 71;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> DeleteTableSegmentCommand {
        let decoded: DeleteTableSegmentCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SegmentDeletedCommand {
    pub request_id: i64,
    pub segment: JavaString,
}

impl Command for SegmentDeletedCommand {
    const TYPE_CODE: i32 = 31;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentDeletedCommand {
        let decoded: SegmentDeletedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct KeepAliveCommand {}

impl Command for KeepAliveCommand {
    const TYPE_CODE: i32 = 100;

    //FIXME: The java code doesn't serialize any information, but here we will add the length(0)
    fn write_fields(&self) -> Vec<u8> {
        let res: Vec<u8> = Vec::new();
        res
    }

    fn read_from(input: &Vec<u8>) -> Self {
        KeepAliveCommand{}
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AuthTokenCheckFailedCommand {
    pub request_id: i64,
    pub server_stack_trace: JavaString,
    pub error_code: ErrorCode,
}
//Todo: wait for impl
pub enum ErrorCode {

}

/**
 * 46. UpdateTableEntries Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct UpdateTableEntriesCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub delegation_token: JavaString,
    pub table_entries: TableEntries,
}

impl Command for UpdateTableEntriesCommand {
    const TYPE_CODE: i32 =74;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> UpdateTableEntriesCommand {
        let decoded: UpdateTableEntriesCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableEntriesUpdatedCommand {
    pub request_id: i64,
    pub updated_versions: Vec<i64>
}

impl Command for TableEntriesUpdatedCommand {
    const TYPE_CODE: i32 = 75;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> TableEntriesUpdatedCommand {
        let decoded: TableEntriesUpdatedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RemoveTableKeysCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub delegation_token: JavaString,
    pub keys: Vec<TableKey>
}

impl Command for RemoveTableKeysCommand {
    const TYPE_CODE: i32 = 76;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> RemoveTableKeysCommand {
        let decoded: RemoveTableKeysCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableKeysRemovedCommand {
    pub request_id: i64,
    pub segment: JavaString,
}

impl Command for TableKeysRemovedCommand{
    const TYPE_CODE: i32 = 77;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> TableKeysRemovedCommand {
        let decoded: TableKeysRemovedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ReadTableCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub delegation_token: JavaString,
    pub keys: Vec<TableKey>,
}

impl Command for ReadTableCommand {
    const TYPE_CODE: i32 = 78;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> ReadTableCommand {
        let decoded: ReadTableCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableReadCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub entries: TableEntries,
}

impl Command for TableReadCommand {
    const TYPE_CODE: i32 = 79;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> TableReadCommand {
        let decoded: TableReadCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ReadTableKeysCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub delegation_token: JavaString,
    pub suggested_key_count: i32,
    pub continuation_token: Vec<u8>,
}

impl Command for ReadTableKeysCommand {
    const TYPE_CODE: i32 = 83;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> ReadTableKeysCommand {
        let decoded: ReadTableKeysCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableKeysReadCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub keys: Vec<TableKey>,
    pub continuation_token: Vec<u8>, // this is used to indicate the point from which the next keys should be fetched.
}

impl Command for TableKeysReadCommand {
    const TYPE_CODE: i32 = 84;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> TableKeysReadCommand {
        let decoded: TableKeysReadCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ReadTableEntriesCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub delegation_token: JavaString,
    pub suggested_entry_count: i32,
    pub continuation_token: Vec<u8>,
}

impl Command for ReadTableEntriesCommand {
    const TYPE_CODE: i32 = 85;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> ReadTableEntriesCommand {
        let decoded: ReadTableEntriesCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableEntriesReadCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub entries: TableEntries,
    pub continuation_token: Vec<u8>,
}

impl Command for TableEntriesReadCommand {
    const TYPE_CODE: i32 = 86;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> TableEntriesReadCommand {
        let decoded: TableEntriesReadCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableKeyDoesNotExistCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub server_stack_trace: JavaString,
}

impl Command for TableKeyDoesNotExistCommand {
    const TYPE_CODE: i32 = 81;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> TableKeyDoesNotExistCommand {
        let decoded: TableKeyDoesNotExistCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
        write!(f, "Conditional table update failed since the key does not exist : {}", self.segment)
    }
}

/**
 * 57. TableKeyBadVersion Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableKeyBadVersionCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub server_stack_trace: JavaString,
}

impl Command for TableKeyBadVersionCommand {
    const TYPE_CODE: i32 = 82;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> TableKeyBadVersionCommand {
        let decoded: TableKeyBadVersionCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
        write!(f, "Conditional table update failed since the key version is incorrect : {}", self.segment)
    }
}


/**
 * Table Key Struct.
 * FIXME: Do we need to manually implement serialize and deserialize method for TableKey?
 * As the Java code serialize this into |payload_size|data_length|data|key_version|?
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableKey {
    data: Vec<u8>,
    key_version: i64,
}

impl TableKey {
    const NO_VERSION: i64 = i64::min_value();
    const NOT_EXISTS: i64 = -1;
    //FIXME: the i32 in rust doesn't have byte() method or i32::BYTES
    const HEADER_BYTES: i32 = 2 * 4;
    // FIXME: I think we don't need EMPTY variable in rust, because we don't serialize manually.
    // const EMPTY: TableKey = TableKey{data: Vec::new(), key_version: i64::min_value()};
}

/**
 * Table Key Struct.
 * FIXME: Do we need to manually implement serialize and deserialize method for TableValue?
 * As the Java code serialize this into |payload_size|data_length|data
 * And the rust would miss the payload_size
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableValue {
    data: Vec<u8>,
}

impl TableValue {
    const HEADER_BYTES: i32 = 2 * 4;
    // FIXME: I think we don't need EMPTY variable in rust, because we don't serialize manually.
    // const EMPTY: TableValue = TableValue{data: Vec::new()};
}

/**
 * TableEntries Struct.
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableEntries {
    // FIXME: if it okay to change Map.Entry<TableKey, TableValue> to tuple?
    entries: Vec<(TableKey, TableEntries)>,
}

impl TableEntries {

    fn get_header_byte(entry_count: i32) -> i32 {
        4 + entry_count * (TableKey::HEADER_BYTES + TableValue::HEADER_BYTES)
    }

    fn size(&self) -> i32 {

    }
}