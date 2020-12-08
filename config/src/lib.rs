//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
#![deny(
    clippy::all,
    clippy::cargo,
    clippy::else_if_without_else,
    clippy::empty_line_after_outer_attr,
    clippy::multiple_inherent_impl,
    clippy::mut_mut,
    clippy::path_buf_push_overwrite
)]
#![warn(
    clippy::cargo_common_metadata,
    clippy::mutex_integer,
    clippy::needless_borrow,
    clippy::similar_names
)]
#![allow(clippy::multiple_crate_versions)]
pub mod connection_type;
pub mod credentials;

use crate::connection_type::ConnectionType;
use crate::credentials::Credentials;
use derive_builder::*;
use getset::{CopyGetters, Getters};
use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
use pravega_rust_client_shared::PravegaNodeUri;
use std::collections::HashMap;
use std::env;
use std::time::Duration;

pub const MOCK_CONTROLLER_URI: (&str, u16) = ("localhost", 9090);

#[derive(Builder, Debug, Getters, CopyGetters, Clone)]
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

    #[get]
    #[builder(setter(into))]
    pub controller_uri: PravegaNodeUri,

    #[get_copy = "pub"]
    #[builder(default = "90 * 1000")]
    pub transaction_timeout_time: u64,

    #[get_copy = "pub"]
    #[builder(default = "false")]
    pub mock: bool,

    #[builder(default = "self.default_trustcert()")]
    pub trustcert: String,

    #[get_copy = "pub"]
    #[builder(default = "false")]
    pub is_tls_enabled: bool,

    #[builder(default = "self.default_credentials()")]
    pub credentials: Credentials,

    #[get_copy = "pub"]
    #[builder(default = "false")]
    pub is_auth_enabled: bool,

    #[get_copy = "pub"]
    #[builder(default = "1024 * 1024")]
    pub reader_wrapper_buffer_size: usize,

    #[get_copy = "pub"]
    #[builder(default = "self.default_timeout()")]
    pub request_timeout: Duration,
}

impl ClientConfigBuilder {
    fn default_trustcert(&self) -> String {
        "./ca-cert.crt".to_owned()
    }

    fn default_credentials(&self) -> Credentials {
        Credentials::basic("admin".into(), "1111_aaaa".into())
    }

    fn default_timeout(&self) -> Duration {
        Duration::from_secs(3600)
    }
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
            .controller_uri(PravegaNodeUri::from("127.0.0.2:9091".to_string()))
            .build()
            .unwrap();

        assert_eq!(config.max_connections_in_pool(), 15 as u32);
        assert_eq!(config.connection_type(), ConnectionType::Tokio);
        assert_eq!(config.retry_policy(), RetryWithBackoff::from_millis(1000));
        assert_eq!(
            config.controller_uri().to_socket_addr().ip(),
            Ipv4Addr::new(127, 0, 0, 2)
        );
        assert_eq!(config.controller_uri().to_socket_addr().port(), 9091);
    }

    #[test]
    fn test_get_default() {
        let config = ClientConfigBuilder::default()
            .controller_uri(MOCK_CONTROLLER_URI)
            .build()
            .unwrap();

        assert_eq!(config.max_connections_in_pool(), u32::max_value() as u32);
        assert_eq!(config.max_controller_connections(), 3u32);
        assert_eq!(config.connection_type(), ConnectionType::Tokio);
        assert_eq!(config.retry_policy(), RetryWithBackoff::default());
    }
}
