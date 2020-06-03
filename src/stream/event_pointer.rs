//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
use super::BINCODE_CONFIG;
use crate::error::*;
use pravega_rust_client_shared::Segment;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

/// EventPointerVersioned enum contains all versions of EventPointer
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) enum EventPointerVersioned {
    V1(EventPointerV1),
}

impl EventPointerVersioned {
    fn to_bytes(&self) -> Result<Vec<u8>, SerdeError> {
        let encoded = BINCODE_CONFIG.serialize(&self).context(Serde {
            msg: String::from("serialize EventPointerVersioned"),
        })?;
        Ok(encoded)
    }

    fn from_bytes(input: &[u8]) -> Result<EventPointerVersioned, SerdeError> {
        let decoded: EventPointerVersioned = BINCODE_CONFIG.deserialize(&input[..]).context(Serde {
            msg: String::from("deserialize EventPointerVersioned"),
        })?;
        Ok(decoded)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) struct EventPointerV1 {
    segment: Segment,
    event_start_offset: i64,
    event_length: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_pointer_serde() {
        let v1 = EventPointerV1 {
            segment: Segment {
                number: 0,
                tx_id: None,
            },
            event_start_offset: 0,
            event_length: 0,
        };
        let event_pointer = EventPointerVersioned::V1(v1.clone());

        let encoded = event_pointer.to_bytes().expect("encode to byte array");
        let decoded = EventPointerVersioned::from_bytes(&encoded).expect("decode from byte array");
        assert_eq!(EventPointerVersioned::V1(v1), decoded);
    }
}
