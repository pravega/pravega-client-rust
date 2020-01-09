//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//

#[macro_use]
extern crate lazy_static;

#[allow(dead_code)]
pub mod wire_protocol {
    mod commands;
    mod error;
    mod wire_commands;
    //Public for docs to build. (TODO: Is there a better way to do this?)
    pub mod connection_factory;
    #[cfg(test)]
    mod tests;
}
