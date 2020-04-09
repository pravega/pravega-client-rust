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
use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
use std::net::SocketAddr;

#[derive(Builder, Debug, CopyGetters, Clone)]
#[builder(setter(into))]
pub struct ClientConfig {
    #[get_copy = "pub"]
    #[builder(default = "u32::max_value()")]
    pub max_connections_in_pool: u32,

    #[get_copy = "pub"]
    #[builder(default = "ConnectionType::Tokio")]
    pub connection_type: ConnectionType,

    #[get_copy = "pub"]
    #[builder(default = "RetryWithBackoff::default()")]
    pub retry_policy: RetryWithBackoff,

    #[get_copy = "pub"]
    pub controller_uri: SocketAddr,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_get_set() {
        let config = ClientConfigBuilder::default()
            .max_connections_in_pool(15 as u32)
            .connection_type(ConnectionType::Tokio)
            .retry_policy(RetryWithBackoff::from_millis(1000))
            .controller_uri("127.0.0.1:9090"
                                .parse::<SocketAddr>()
                                .expect("parse to socketaddr"))
            .build()
            .unwrap();

        assert_eq!(config.max_connections_in_pool(), 15 as u32);
        assert_eq!(config.connection_type(), ConnectionType::Tokio);
        assert_eq!(config.retry_policy(), RetryWithBackoff::from_millis(1000));
        assert_eq!(config.controller_uri().ip(), Ipv4Addr::new(127, 0, 0, 1));
        assert_eq!(config.controller_uri().port(), 9090);
    }

    #[test]
    fn test_get_default() {
        let config = ClientConfigBuilder::default().controller_uri("127.0.0.1:9090"
            .parse::<SocketAddr>()
            .expect("parse to socketaddr")).build().unwrap();

        assert_eq!(config.max_connections_in_pool(), u32::max_value() as u32);
        assert_eq!(config.connection_type(), ConnectionType::Tokio);
        assert_eq!(config.retry_policy(), RetryWithBackoff::default());
    }
}
