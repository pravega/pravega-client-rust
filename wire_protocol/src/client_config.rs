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
use std::net::{Ipv4Addr, SocketAddr};

pub const TEST_CONTROLLER_URI: (Ipv4Addr, u16) = (Ipv4Addr::new(127, 0, 0, 1), 9090);

#[derive(Builder, Debug, CopyGetters, Clone)]
#[builder(setter(into))]
pub struct ClientConfig {
    #[get_copy = "pub"]
    #[builder(default = "u32::max_value()")]
    pub max_connections_in_pool: u32,

    #[get_copy = "pub"]
    #[builder(default = "3u32")]
    pub max_controller_connections: u32,

    #[get_copy = "pub"]
    #[builder(default = "ConnectionType::Tokio")]
    pub connection_type: ConnectionType,

    #[get_copy = "pub"]
    #[builder(default = "RetryWithBackoff::default()")]
    pub retry_policy: RetryWithBackoff,

    #[get_copy = "pub"]
    #[builder(setter(into))]
    pub controller_uri: SocketAddr,

    #[get_copy = "pub"]
    #[builder(default = "90 * 1000 - 1")]
    pub transaction_timeout_time: u64,
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
            .controller_uri(
                "127.0.0.2:9091"
                    .parse::<SocketAddr>()
                    .expect("parse to socketaddr"),
            )
            .build()
            .unwrap();

        assert_eq!(config.max_connections_in_pool(), 15 as u32);
        assert_eq!(config.connection_type(), ConnectionType::Tokio);
        assert_eq!(config.retry_policy(), RetryWithBackoff::from_millis(1000));
        assert_eq!(config.controller_uri().ip(), Ipv4Addr::new(127, 0, 0, 2));
        assert_eq!(config.controller_uri().port(), 9091);
    }

    #[test]
    fn test_get_default() {
        let config = ClientConfigBuilder::default()
            .controller_uri(TEST_CONTROLLER_URI)
            .build()
            .unwrap();

        assert_eq!(config.max_connections_in_pool(), u32::max_value() as u32);
        assert_eq!(config.max_controller_connections(), 3u32);
        assert_eq!(config.connection_type(), ConnectionType::Tokio);
        assert_eq!(config.retry_policy(), RetryWithBackoff::default());
    }
}
