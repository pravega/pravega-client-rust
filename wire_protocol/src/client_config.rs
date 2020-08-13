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
use crate::credentials::Credentials;
use derive_builder::*;
use getset::CopyGetters;
use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
use std::collections::HashMap;
use std::env;
use std::net::{Ipv4Addr, SocketAddr};

pub const TEST_CONTROLLER_URI: (Ipv4Addr, u16) = (Ipv4Addr::new(127, 0, 0, 1), 9090);
const AUTH_PROPS_PREFIX: &str = "pravega.client.auth.";
const AUTH_METHOD: &str = "method";
const AUTH_TOKEN: &str = "token";
const AUTH_PROPS_PREFIX_ENV: &str = "pravega_client_auth_";

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
    #[builder(default = "90 * 1000")]
    pub transaction_timeout_time: u64,

    #[get_copy = "pub"]
    #[builder(default = "false")]
    pub mock: bool,

    #[builder(default = "self.default_truststore()")]
    pub truststore: String,

    #[get_copy = "pub"]
    #[builder(default = "false")]
    pub is_tls_enabled: bool,

    #[builder(default = "self.extract_credentials()")]
    pub credentials: Credentials,

    #[get_copy = "pub"]
    #[builder(default = "false")]
    pub is_auth_enabled: bool,
}

impl ClientConfigBuilder {
    fn default_truststore(&self) -> String {
        "../../ca-cert.crt".to_owned()
    }

    fn extract_credentials(&self) -> Credentials {
        let ret_val = env::vars()
            .filter(|(k, _v)| k.starts_with(AUTH_PROPS_PREFIX_ENV))
            .map(|(k, v)| {
                let k = k.replace("_", ".");
                let k = &k[AUTH_PROPS_PREFIX.len()..];
                (k.to_owned(), v)
            })
            .collect::<HashMap<String, String>>();
        if ret_val.contains_key(AUTH_METHOD) {
            let method = ret_val.get(AUTH_METHOD).expect("get auth method").to_owned();
            let token = ret_val.get(AUTH_TOKEN).expect("get auth token").to_owned();
            Credentials::new(method, token)
        } else {
            Credentials::default("admin".into(), "1111_aaaa".into())
        }
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
