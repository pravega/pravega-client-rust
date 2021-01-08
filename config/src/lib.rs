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
use pravega_client_retry::retry_policy::RetryWithBackoff;
use pravega_client_shared::PravegaNodeUri;
use std::collections::HashMap;
use std::env;
use std::time::Duration;

pub const MOCK_CONTROLLER_URI: (&str, u16) = ("localhost", 9090);
const AUTH_METHOD: &str = "method";
const AUTH_USERNAME: &str = "username";
const AUTH_PASSWORD: &str = "password";
const AUTH_TOKEN: &str = "token";
const AUTH_KEYCLOAK_PATH: &str = "keycloak";
const AUTH_PROPS_PREFIX_ENV: &str = "pravega_client_auth_";

const TLS_CERT_PATH: &str = "pravega_client_tls_cert_path";

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

    #[builder(default = "self.extract_trustcert()")]
    pub trustcert: String,

    #[get_copy = "pub"]
    #[builder(default = "false")]
    pub is_tls_enabled: bool,

    #[builder(default = "self.extract_credentials()")]
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
    fn extract_trustcert(&self) -> String {
        let ret_val = env::vars()
            .filter(|(k, _v)| k.starts_with(TLS_CERT_PATH))
            .collect::<HashMap<String, String>>();
        if ret_val.contains_key(TLS_CERT_PATH) {
            let cert_path = ret_val.get(TLS_CERT_PATH).expect("get tls cert path").to_owned();
            return cert_path;
        }
        "./ca-cert.crt".to_owned()
    }

    fn extract_credentials(&self) -> Credentials {
        let ret_val = env::vars()
            .filter(|(k, _v)| k.starts_with(AUTH_PROPS_PREFIX_ENV))
            .map(|(k, v)| {
                let k = &k[AUTH_PROPS_PREFIX_ENV.len()..];
                (k.to_owned(), v)
            })
            .collect::<HashMap<String, String>>();
        if ret_val.contains_key(AUTH_METHOD) {
            let method = ret_val.get(AUTH_METHOD).expect("get auth method").to_owned();
            if method == credentials::BASIC {
                if let Some(token) = ret_val.get(AUTH_TOKEN) {
                    return Credentials::basic_with_token(token.to_string());
                }
                let username = ret_val.get(AUTH_USERNAME).expect("get auth username").to_owned();
                let password = ret_val.get(AUTH_PASSWORD).expect("get auth password").to_owned();
                return Credentials::basic(username, password);
            }
            if method == credentials::BEARER {
                let path = ret_val.get(AUTH_KEYCLOAK_PATH).expect("get keycloak json file");
                return Credentials::keycloak(path);
            }
        }
        Credentials::basic("".into(), "".into())
    }

    fn default_timeout(&self) -> Duration {
        Duration::from_secs(3600)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::encode;
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

    #[test]
    fn test_extract_credentials() {
        // test empty env
        let config = ClientConfigBuilder::default()
            .controller_uri("127.0.0.2:9091".to_string())
            .build()
            .unwrap();

        let token = encode(":");

        assert_eq!(
            config.credentials.get_request_metadata(),
            format!("{} {}", "Basic", token)
        );

        // retrieve from env
        env::set_var("pravega_client_auth_method", "Basic");
        env::set_var("pravega_client_auth_username", "hello");
        env::set_var("pravega_client_auth_password", "12345");

        let config = ClientConfigBuilder::default()
            .controller_uri("127.0.0.2:9091".to_string())
            .build()
            .unwrap();

        let token = encode("hello:12345");
        assert_eq!(
            config.credentials.get_request_metadata(),
            format!("{} {}", "Basic", token)
        );

        // retrieve from env with priority
        env::set_var("pravega_client_auth_token", "ABCDE");
        let config = ClientConfigBuilder::default()
            .controller_uri("127.0.0.2:9091".to_string())
            .build()
            .unwrap();
        assert_eq!(
            config.credentials.get_request_metadata(),
            format!("{} {}", "Basic", "ABCDE")
        )
    }

    #[test]
    fn test_extract_tls_cert_path() {
        // test default
        let config = ClientConfigBuilder::default()
            .controller_uri("127.0.0.2:9091".to_string())
            .build()
            .unwrap();
        assert_eq!(config.trustcert, "./ca-cert.crt");

        // test with env var set
        env::set_var("pravega_client_tls_cert_path", "/");
        let config = ClientConfigBuilder::default()
            .controller_uri("127.0.0.2:9091".to_string())
            .build()
            .unwrap();
        assert_eq!(config.trustcert, "/");
    }
}
