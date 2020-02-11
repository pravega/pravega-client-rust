//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::connection_factory::ConnectionType;
use derive_builder::*;
use getset::CopyGetters;

#[derive(Default, Builder, Debug, CopyGetters, Clone)]
#[builder(setter(into))]
pub struct ClientConfig {
    #[get_copy = "pub"]
    #[builder(default = "u32::max_value()")]
    pub max_connections_per_segmentstore: u32,

    #[get_copy = "pub"]
    #[builder(default = "ConnectionType::Tokio")]
    pub connection_type: ConnectionType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_set() {
        let config = ClientConfigBuilder::default()
            .max_connections_per_segmentstore(15 as u32)
            .connection_type(ConnectionType::Tokio)
            .build()
            .unwrap();

        assert_eq!(config.max_connections_per_segmentstore(), 15 as u32);
        assert_eq!(config.connection_type(), ConnectionType::Tokio);
    }

    #[test]
    fn test_get_default() {
        let config = ClientConfigBuilder::default().build().unwrap();

        assert_eq!(config.max_connections_per_segmentstore(), u32::max_value() as u32);
        assert_eq!(config.connection_type(), ConnectionType::Tokio);
    }
}
