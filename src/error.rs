//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//

use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Conditional check failed: {}", msg))]
    ConditionalCheckFailure { msg: String },

    #[snafu(display("Internal error: {}", msg))]
    InternalFailure { msg: String },

    #[snafu(display("Input is invalid: {}", msg))]
    InvalidInput { msg: String },
}
