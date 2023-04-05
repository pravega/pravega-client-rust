//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use std::fmt;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ConnectionType {
    Mock(MockType),
    Tokio,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum MockType {
    Happy,
    SegmentIsSealed,
    SegmentIsTruncated,
    WrongHost,
}

impl fmt::Display for ConnectionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
