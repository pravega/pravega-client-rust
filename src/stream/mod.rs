//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

pub(crate) mod event_pointer;
pub(crate) mod position;

use bincode2::Config;
use bincode2::LengthOption;
use lazy_static::*;

lazy_static! {
    static ref BINCODE_CONFIG: Config = {
        let mut config = bincode2::config();
        config.big_endian();
        config.limit(0x007f_ffff);
        config.array_length(LengthOption::U32);
        config.string_length(LengthOption::U16);
        config
    };
}
