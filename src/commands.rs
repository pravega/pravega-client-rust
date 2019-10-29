use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::de::{self, Visitor};
use uuid::Uuid;
use uuid::adapter::compact::serialize;
use serde::de::Unexpected::Str;

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
 * Wrap String to follow Java Serialize/Deserialize Style.
 *
 */

struct JavaString(String);

impl Serialize for JavaString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        let length: usize = self.0.len();
        //serialize the length as u16
        serializer.serialize_u16(length as u16);
        let binary = self.0.as_bytes();
        serializer.serialize_bytes(binary);
        Ok(())
    }
}


struct JavaStringVisitor;

impl<'de> Visitor<'de> for JavaStringVisitor {
    type Value = JavaString;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("A Byte buffer which contains length and content")
    }

    fn visit_bytes(self, value: &Vec<u8>) -> Result<JavaString, E>
        where
            E: de::Error,
    {
        // get the length
        let length = ((value[0] as u16) << 8) | value[1] as u16;
        // construct the JavaString
        // Fixme: If there is a better way to change a &Vec[2..] to String
        let content = JavaString(String::from_utf8_lossy(&value[2..]).into_owned());
        if length == content.len() {
            Ok(content)
        } else {
            Err(E::custom("The length and content mismatch"))
        }
    }
}

impl <'de>Deserialize<'de> for JavaString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
    {
        //FIXME: what should I use deserialize_bytes() or deserialize_byte_buf
        deserializer.deserialize_bytes(JavaStringVisitor)
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
 * Hello Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Display)]
pub struct HelloCommand {
    pub high_version: i32,
    pub low_version: i32,
}

impl Command for HelloCommand {
    const TYPE_CODE: i32 = -127;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = bincode::serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> HelloCommand {
        let decoded: HelloCommand = bincode::deserialize(&input[..]).unwrap();
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
 * WrongHost Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Display)]
pub struct WrongHostCommand {
    pub request_id: i64,
    pub segment: JavaString,
    pub correct_host: JavaString,
    pub server_stack_trace: JavaString,
}

impl Command for WrongHostCommand {
    const TYPE_CODE: i32 = 50;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = bincode::serialize(&self).unwrap();
        encoded
    }
    fn read_from(input: &Vec<u8>) -> WrongHostCommand {
        let decoded: WrongHostCommand = bincode::deserialize(&input[..]).unwrap();
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
 * SegmentIsSealed Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Display)]
pub struct SegmentIsSealedCommand {
    pub request_id: i64,
    pub segment: String,
    pub server_stack_trace: String,
    pub offset: i64,
}

impl Command for SegmentIsSealedCommand {
    const TYPE_CODE: i32 = 51;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = bincode::serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentIsSealedCommand {
        let decoded: SegmentIsSealedCommand = bincode::deserialize(&input[..]).unwrap();
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
 * SegmentIsTruncated Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Display)]
pub struct SegmentIsTruncatedCommand {
    pub request_id: i64,
    pub segment: String,
    pub start_offset: i64,
    pub server_stack_trace: String,
    pub offset: i64,

}

impl Command for SegmentIsTruncatedCommand {
    const TYPE_CODE: i32 = 56;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = bincode::serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentIsTruncatedCommand {
        let decoded: SegmentIsTruncatedCommand = bincode::deserialize(&input[..]).unwrap();
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
 * SegmentAlreadyExists Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Display)]
pub struct SegmentAlreadyExistsCommand {
    pub request_id: i64,
    pub segment: String,
    pub server_stack_trace: String,
}

impl Command for SegmentAlreadyExistsCommand {
    const TYPE_CODE: i32 = 52;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = bincode::serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> SegmentAlreadyExistsCommand {
        let decoded: SegmentAlreadyExistsCommand = bincode::deserialize(&input[..]).unwrap();
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


/**
 * NoSuchSegment Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Display)]
pub struct NoSuchSegmentCommand {
    pub request_id: i64,
    pub segment: String,
    pub server_stack_trace: String,
    pub offset: i64,
}

impl Command for NoSuchSegmentCommand {
    const TYPE_CODE: i32 = 53;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = bincode::serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> NoSuchSegmentCommand {
        let decoded: NoSuchSegmentCommand = bincode::deserialize(&input[..]).unwrap();
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

/**
 * TableSegmentNotEmpty Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug, Display)]
pub struct TableSegmentNotEmptyCommand {
    pub request_id: i64,
    pub segment: String,
    pub server_stack_trace: String,
}

impl Command for TableSegmentNotEmptyCommand {
    const TYPE_CODE: i32 = 80;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = bincode::serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> TableSegmentNotEmptyCommand {
        let decoded: TableSegmentNotEmptyCommand = bincode::deserialize(&input[..]).unwrap();
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

/**
 * InvalidEventNumber Command
 */
pub struct InvalidEventNumberCommand {
    pub write_id: Uuid,
    pub event_number: i64,
    pub server_stack_trace: String,
}

impl Command for InvalidEventNumberCommand {
    const TYPE_CODE: i32 = 55;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = bincode::serialize(&self).unwrap();
        encoded
    }

    fn read_from(input: &Vec<u8>) -> InvalidEventNumberCommand {
        let decoded: InvalidEventNumberCommand = bincode::deserialize(&input[..]).unwrap();
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