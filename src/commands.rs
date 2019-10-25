use serde::{Serialize, Deserialize};
/**
 * trait for Command.
 */
pub trait Command {
    const TYPE_CODE: i32;
    fn write_fields(&self) -> Vec<u8> ;
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
 * Hello Command
 */
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct HelloCommand {
    pub high_version: i32,
    pub low_version: i32,
}

impl HelloCommand {
    pub fn read_from(input: &Vec<u8>) -> HelloCommand {
        let decoded: HelloCommand = bincode::deserialize(&input[..]).unwrap();
        decoded
    }
}

impl Command for HelloCommand {
    const TYPE_CODE: i32 = -127;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = bincode::serialize(&self).unwrap();
        encoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct WrongHostCommand {
    pub request_id: i64,
    pub segment: String,
    pub correct_host: String,
    pub server_stack_trace: String,
}

impl WrongHostCommand {
    pub fn read_from(input: &Vec<u8>) -> WrongHostCommand {
        let decoded: WrongHostCommand = bincode::deserialize(&input[..]).unwrap();
        decoded
    }
}

impl Command for WrongHostCommand {
    const TYPE_CODE: i32 = 50;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = bincode::serialize(&self).unwrap();
        encoded
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
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SegmentIsSealedCommand {
    request_id: i64,
    segment: String,
    server_stack_trace: String,
    offset: i64,
}

impl SegmentIsSealedCommand {
    pub fn read_from(input: &Vec<u8>) -> SegmentIsSealedCommand {
        let decoded: SegmentIsSealedCommand = bincode::deserialize(&input[..]).unwrap();
        decoded
    }
}

impl Command for SegmentIsSealedCommand {
    const TYPE_CODE: i32 = 51;
    fn write_fields(&self) -> Vec<u8> {
        let encoded = bincode::serialize(&self).unwrap();
        encoded
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
