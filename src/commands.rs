use bincode::Config;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use serde::de::{self, Deserializer, Unexpected, Visitor};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::i64;
use std::io::Cursor;
use std::io::{Read, Write};
use crate::error::SerializeError;

/**
 * trait for Command.
 */
pub trait Command {
    const TYPE_CODE: i32;
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError>;
    fn read_from(input: &[u8]) -> Self;
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

    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // get the length
        let _length = ((value[0] as u16) << 8) | value[1] as u16;
        // construct the JavaString
        let content = String::from_utf8_lossy(&value[2..]).into_owned();

        if _length == content.len() as u16 {
            Ok(JavaString(content))
        } else {
            Err(de::Error::invalid_value(Unexpected::Bytes(&value), &self))
        }
    }
}

impl<'de> Deserialize<'de> for JavaString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_byte_buf(JavaStringVisitor)
    }
}

/*
 * bincode serialize and deserialize config
 */
lazy_static! {
    static ref CONFIG: Config = {
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
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> HelloCommand {
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
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }
    fn read_from(input: &[u8]) -> WrongHostCommand {
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
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> SegmentIsSealedCommand {
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
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> SegmentIsTruncatedCommand {
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
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> SegmentAlreadyExistsCommand {
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
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> NoSuchSegmentCommand {
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
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> TableSegmentNotEmptyCommand {
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
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> InvalidEventNumberCommand {
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct OperationUnsupportedCommand {
    pub request_id: i64,
    pub operation_name: JavaString,
    pub server_stack_trace: JavaString,
}

impl Command for OperationUnsupportedCommand {
    const TYPE_CODE: i32 = 57;
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> OperationUnsupportedCommand {
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
#[derive(PartialEq, Debug)]
pub struct PaddingCommand {
    pub length: i32,
}

impl Command for PaddingCommand {
    const TYPE_CODE: i32 = -1;

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let mut res = Vec::new();
        for _i in 0..(self.length / 8) {
            res.write_i64::<BigEndian>(0).unwrap();
        }
        for _i in 0..(self.length % 8) {
            res.write_u8(0).unwrap();
        }
        res
    }

    fn read_from(input: &[u8]) -> PaddingCommand {
        //FIXME: In java we use skipBytes to remove these padding bytes.
        //FIXME: I think we don't need to do in rust.
        PaddingCommand {
            length: input.len() as i32,
        }
    }
}

/**
 * 11. PartialEvent Command
 */
#[derive(PartialEq, Debug)]
pub struct PartialEventCommand {
    pub data: Vec<u8>,
}

impl Command for PartialEventCommand {
    const TYPE_CODE: i32 = -2;
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        //FIXME: In java, we use data.getBytes(data.readerIndex(), (OutputStream) out, data.readableBytes());
        // which means the result would not contain the prefix length;
        // so in rust we can directly return data.
        self.data.clone()
    }

    fn read_from(input: &[u8]) -> PartialEventCommand {
        PartialEventCommand {
            data: input.to_vec(),
        }
    }
}

/**
 * 12. Event Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct EventCommand {
    pub data: Vec<u8>,
}

impl Command for EventCommand {
    const TYPE_CODE: i32 = 0;
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let mut res = Vec::new();
        res.write_i32::<BigEndian>(EventCommand::TYPE_CODE).unwrap();
        let encoded = CONFIG.serialize(&self).unwrap();
        res.extend(encoded);
        res
    }

    fn read_from(input: &[u8]) -> EventCommand {
        //read the type_code.
        let _type_code = BigEndian::read_i32(input);
        let decoded: EventCommand = CONFIG.deserialize(&input[4..]).unwrap();
        decoded
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
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> SetupAppendCommand {
        let decoded: SetupAppendCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AppendBlockCommand {
    pub writer_id: u128,
    pub data: Vec<u8>,
}

impl Command for AppendBlockCommand {
    const TYPE_CODE: i32 = 3;
    //FIXME: The serialize and deserialize method need to customize;
    // In JAVA, it doesn't write data(because it'empty), but here it will write the prefix length(0).
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> AppendBlockCommand {
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

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> AppendBlockEndCommand {
        let decoded: AppendBlockEndCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
    }
}

/**
 * 16.ConditionalAppend Command
 */
#[derive(PartialEq, Debug)]
pub struct ConditionalAppendCommand {
    pub writer_id: u128,
    pub event_number: i64,
    pub expected_offset: i64,
    pub event: EventCommand,
    pub request_id: i64,
}

impl ConditionalAppendCommand {
    fn read_event(rdr: &mut Cursor<&[u8]>) -> EventCommand {
        let _type_code = rdr.read_i32::<BigEndian>().unwrap();
        let event_length = rdr.read_u64::<BigEndian>().unwrap();
        // read the data in event
        let mut msg: Vec<u8> = vec![0; event_length as usize];
        rdr.read_exact(&mut msg).unwrap();
        EventCommand { data: msg }
    }
}

impl Command for ConditionalAppendCommand {
    const TYPE_CODE: i32 = 5;
    // Customize the serialize and deserialize method.
    // Because in CondtionalAppend the event should be serialize as |type_code|length|data|
    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let mut res = Vec::new();
        res.write_u128::<BigEndian>(self.writer_id).unwrap();
        res.write_i64::<BigEndian>(self.event_number).unwrap();
        res.write_i64::<BigEndian>(self.expected_offset).unwrap();
        res.write_all(&self.event.write_fields()).unwrap();
        res.write_i64::<BigEndian>(self.request_id).unwrap();
        res
    }

    fn read_from(input: &[u8]) -> ConditionalAppendCommand {
        let mut rdr = Cursor::new(input);
        let writer_id = rdr.read_u128::<BigEndian>().unwrap();
        let event_number = rdr.read_i64::<BigEndian>().unwrap();
        let expected_offset = rdr.read_i64::<BigEndian>().unwrap();
        let event = ConditionalAppendCommand::read_event(&mut rdr);
        let request_id = rdr.read_i64::<BigEndian>().unwrap();
        ConditionalAppendCommand {
            writer_id,
            event_number,
            expected_offset,
            event,
            request_id,
        }
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

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> Self {
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
    pub writer_id: u128,
    pub event_number: i64,
    pub previous_event_number: i64,
    pub request_id: i64,
    pub current_segment_write_offset: i64,
}

impl Command for DataAppendedCommand {
    const TYPE_CODE: i32 = 7;

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> DataAppendedCommand {
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
    pub event_number: i64,
    pub request_id: i64,
}

impl Command for ConditionalCheckFailedCommand {
    const TYPE_CODE: i32 = 8;

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> ConditionalCheckFailedCommand {
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
    pub segment: JavaString,
    pub offset: i64,
    pub suggested_length: i64,
    pub delegation_token: JavaString,
    pub request_id: i64,
}

impl Command for ReadSegmentCommand {
    const TYPE_CODE: i32 = 9;

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> ReadSegmentCommand {
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
    pub segment: JavaString,
    pub offset: i64,
    pub at_tail: bool,
    pub end_of_segment: bool,
    pub data: Vec<u8>,
    pub request_id: i64,
}

impl Command for SegmentReadCommand {
    const TYPE_CODE: i32 = 10;

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> SegmentReadCommand {
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

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> GetSegmentAttributeCommand {
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

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> SegmentAttributeCommand {
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
    pub new_value: i64,
    pub expected_value: i64,
    pub delegation_token: JavaString,
}

impl Command for UpdateSegmentAttributeCommand {
    const TYPE_CODE: i32 = 36;

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> UpdateSegmentAttributeCommand {
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

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> SegmentAttributeUpdatedCommand {
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

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> GetStreamSegmentInfoCommand {
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

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> StreamSegmentInfoCommand {
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
    pub target_rate: i32,
    pub scale_type: u8,
    pub delegation_token: JavaString,
}

impl Command for CreateSegmentCommand {
    const TYPE_CODE: i32 = 20;

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> CreateSegmentCommand {
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

    fn write_fields(&self) -> Result<Vec<u8>, SerializeError> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> CreateTableSegmentCommand {
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

    fn read_from(input: &[u8]) -> SegmentCreatedCommand {
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
    pub target_rate: i32,
    pub scale_type: u8,
    pub delegation_token: JavaString,
}

impl Command for UpdateSegmentPolicyCommand {
    const TYPE_CODE: i32 = 32;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> UpdateSegmentPolicyCommand {
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

    fn read_from(input: &[u8]) -> SegmentPolicyUpdatedCommand {
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

    fn read_from(input: &[u8]) -> MergeSegmentsCommand {
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

    fn read_from(input: &[u8]) -> MergeTableSegmentsCommand {
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
pub struct SegmentsMergedCommand {
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

    fn read_from(input: &[u8]) -> SegmentsMergedCommand {
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

    fn read_from(input: &[u8]) -> SealSegmentCommand {
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

    fn read_from(input: &[u8]) -> SealTableSegmentCommand {
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

    fn read_from(input: &[u8]) -> SegmentSealedCommand {
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

    fn read_from(input: &[u8]) -> TruncateSegmentCommand {
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

    fn read_from(input: &[u8]) -> SegmentTruncatedCommand {
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

    fn read_from(input: &[u8]) -> DeleteSegmentCommand {
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

    fn read_from(input: &[u8]) -> DeleteTableSegmentCommand {
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

    fn read_from(input: &[u8]) -> SegmentDeletedCommand {
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

    fn write_fields(&self) -> Vec<u8> {
        let res: Vec<u8> = Vec::new();
        res
    }

    fn read_from(_input: &[u8]) -> KeepAliveCommand {
        KeepAliveCommand {}
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

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> AuthTokenCheckFailedCommand {
        let decoded: AuthTokenCheckFailedCommand = CONFIG.deserialize(&input[..]).unwrap();
        decoded
    }
}

impl Reply for AuthTokenCheckFailedCommand {
    fn get_request_id(&self) -> i64 {
        self.request_id
    }
}

#[derive(PartialEq, Debug)]
pub enum ErrorCode {
    Unspecified,
    TokenCheckedFailed,
    TokenExpired,
}

impl ErrorCode {
    fn get_code(error_code: &ErrorCode) -> i32 {
        match error_code {
            ErrorCode::Unspecified => -1,
            ErrorCode::TokenCheckedFailed => 0,
            ErrorCode::TokenExpired => 1,
        }
    }
    fn value_of(code: i32) -> ErrorCode {
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct UpdateTableEntriesCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub delegation_token: JavaString,
    pub table_entries: TableEntries,
}

impl Command for UpdateTableEntriesCommand {
    const TYPE_CODE: i32 = 74;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> UpdateTableEntriesCommand {
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
    pub updated_versions: Vec<i64>,
}

impl Command for TableEntriesUpdatedCommand {
    const TYPE_CODE: i32 = 75;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> TableEntriesUpdatedCommand {
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
    pub keys: Vec<TableKey>,
}

impl Command for RemoveTableKeysCommand {
    const TYPE_CODE: i32 = 76;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> RemoveTableKeysCommand {
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

impl Command for TableKeysRemovedCommand {
    const TYPE_CODE: i32 = 77;

    fn write_fields(&self) -> Vec<u8> {
        let encoded = CONFIG.serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &[u8]) -> TableKeysRemovedCommand {
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

    fn read_from(input: &[u8]) -> ReadTableCommand {
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

    fn read_from(input: &[u8]) -> TableReadCommand {
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

    fn read_from(input: &[u8]) -> ReadTableKeysCommand {
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

    fn read_from(input: &[u8]) -> TableKeysReadCommand {
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

    fn read_from(input: &[u8]) -> ReadTableEntriesCommand {
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

    fn read_from(input: &[u8]) -> TableEntriesReadCommand {
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

    fn read_from(input: &[u8]) -> TableKeyDoesNotExistCommand {
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

    fn read_from(input: &[u8]) -> TableKeyBadVersionCommand {
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
        write!(
            f,
            "Conditional table update failed since the key version is incorrect : {}",
            self.segment
        )
    }
}

/**
 * Table Key Struct.
 * Need overide the serialize
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableKey {
    pub payload: i32,
    pub data: Vec<u8>,
    pub key_version: i64,
}

impl TableKey {
    const HEADER_BYTES: i32 = 2 * 4;
    pub fn new(data: Vec<u8>, key_version: i64) -> TableKey {
        let payload = (data.len() + 8 + 8 + 4) as i32;
        TableKey {
            payload,
            data,
            key_version,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableValue {
    pub payload: i32,
    pub data: Vec<u8>,
}

impl TableValue {
    const HEADER_BYTES: i32 = 2 * 4;
    pub fn new(data: Vec<u8>) -> TableValue {
        let payload = (data.len() + 8 + 8 + 4) as i32;
        TableValue { payload, data }
    }
}

/**
 * TableEntries Struct.
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
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
            data_bytes += (x.0.data.len() + x.1.data.len()) as i32
        }

        data_bytes + TableEntries::get_header_byte(self.entries.len() as i32)
    }
}
