//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use getset::Getters;

#[derive(Default, Builder, Debug, Getters)]
#[builder(setter(into))]
pub struct ClientConfig {
     #[get]
     max_connections_per_segmentstore: u32,
}

#[cfg(test)]
mod tests {
     use super::*;

     #[test]
     fn test_get_set() {
          let config = ClientConfigBuilder::default();
     }
}