//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
use crate::error::*;
use pravega_rust_client_shared::Segment;
use serde::{Deserialize, Serialize};
use serde_cbor::from_slice;
use serde_cbor::to_vec;
use snafu::ResultExt;

/// EventPointerVersioned enum contains all versions of EventPointer
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub(crate) enum EventPointerVersioned {
    V1(EventPointerV1),
}

impl EventPointerVersioned {
    fn to_bytes(&self) -> Result<Vec<u8>, SerdeError> {
        let encoded = to_vec(&self).context(Cbor {
            msg: "serialize EventPointerVersioned".to_owned(),
        })?;
        Ok(encoded)
    }

    fn from_bytes(input: &[u8]) -> Result<EventPointerVersioned, SerdeError> {
        let decoded: EventPointerVersioned = from_slice(&input[..]).context(Cbor {
            msg: "deserialize EventPointerVersioned".to_owned(),
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
