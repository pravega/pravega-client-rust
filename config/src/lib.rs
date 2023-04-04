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
use std::time::Duration;
use std::{env, fs};
use tracing::debug;

pub const MOCK_CONTROLLER_URI: (&str, u16) = ("localhost", 9090);
const AUTH_METHOD: &str = "method";
const AUTH_USERNAME: &str = "username";
const AUTH_PASSWORD: &str = "password";
const AUTH_TOKEN: &str = "token";
const AUTH_KEYCLOAK_PATH: &str = "keycloak";
const AUTH_PROPS_PREFIX_ENV: &str = "pravega_client_auth_";

const TLS_CERT_PATH_ENV: &str = "pravega_client_tls_cert_path";
const DEFAULT_TLS_CERT_PATH: &str = "./certs";
const TLS_SCHEMES: [&str; 4] = ["tls", "ssl", "tcps", "pravegas"];

#[derive(Builder, Debug, Getters, CopyGetters, Clone)]
#[builder(setter(into), build_fn(validate = "Self::validate"))]
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
    #[builder(default = "RetryWithBackoff::default_setting()")]
    pub retry_policy: RetryWithBackoff,

    #[get]
    pub controller_uri: PravegaNodeUri,

    #[get_copy = "pub"]
    #[builder(default = "90 * 1000")]
    pub transaction_timeout_time: u64,

    #[get_copy = "pub"]
    #[builder(default = "false")]
    pub mock: bool,

    #[get_copy = "pub"]
    #[builder(default = "self.default_is_tls_enabled()")]
    pub is_tls_enabled: bool,

    #[builder(default = "false")]
    pub disable_cert_verification: bool,

    #[builder(default = "self.extract_trustcerts()")]
    pub trustcerts: Vec<String>,

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
    fn extract_trustcerts(&self) -> Vec<String> {
        if let Some(enable) = self.is_tls_enabled {
            if !enable {
                return vec![];
            }
        } else if !self.default_is_tls_enabled() {
            return vec![];
        } else {
            // fall through to parsing certs path
        }
        let ret_val = env::vars()
            .filter(|(k, _v)| k.starts_with(TLS_CERT_PATH_ENV))
            .collect::<HashMap<String, String>>();
        let cert_path = if ret_val.contains_key(TLS_CERT_PATH_ENV) {
            ret_val.get(TLS_CERT_PATH_ENV).expect("get tls cert path")
        } else {
            DEFAULT_TLS_CERT_PATH
        };
        let mut certs = vec![];
        let meta = std::fs::metadata(cert_path).expect("should be valid path");
        if meta.is_dir() {
            let cert_paths =
                std::fs::read_dir(cert_path).expect("cannot read from the provided cert directory");
            for entry in cert_paths {
                let path = entry.expect("get the cert file").path();
                debug!("reading cert file {}", path.display());
                certs.push(fs::read_to_string(path.clone()).expect("read cert file"));
            }
        } else if meta.is_file() {
            certs.push(fs::read_to_string(cert_path).expect("read cert file"));
        } else {
            panic!("invalid cert path");
        }
        certs
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
                let mut disable_cert_verification = false;
                if self.disable_cert_verification.is_some() && self.disable_cert_verification.unwrap() {
                    disable_cert_verification = true;
                }
                return Credentials::keycloak(path, disable_cert_verification);
            }
        }
        Credentials::basic("".into(), "".into())
    }

    fn default_timeout(&self) -> Duration {
        Duration::from_secs(30)
    }

    fn default_is_tls_enabled(&self) -> bool {
        if let Some(controller_uri) = &self.controller_uri {
            return match controller_uri.scheme() {
                Ok(scheme) => TLS_SCHEMES.contains(&&*scheme),
                Err(_) => false,
            };
        }
        false
    }
    /// validate the builder before returning it
    ///
    /// if is_tls_enabled, controller_uri have been set and if controller_uri
    /// contains a scheme, then verify that the uri scheme matches the is_tls_enabled
    /// value.
    fn validate(&self) -> Result<(), String> {
        if self.is_tls_enabled.is_none()    // is_tls_enabled not specified
            || self.controller_uri.is_none()    // controller_uri not specified
            || self
                .controller_uri
                .as_ref()
                .unwrap()
                .scheme()
                .unwrap_or_default()
                .is_empty()
        {
            // at least one option has not been specified or uri has no scheme,
            // therefore cannot have a conflict with is_tls_enabled
            return Ok(());
        }
        let is_tls_enabled = self.is_tls_enabled.unwrap();
        let scheme_is_type_tls = self.default_is_tls_enabled();
        if is_tls_enabled != scheme_is_type_tls {
            Err(format!(
                "is_tls_enabled option {} does not match scheme in uri {}",
                is_tls_enabled,
                **self.controller_uri.as_ref().unwrap()
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::encode;
    use serial_test::serial;
    use std::net::Ipv4Addr;

    #[test]
    #[serial]
    fn test_get_set() {
        let config = ClientConfigBuilder::default()
            .max_connections_in_pool(15 as u32)
            .connection_type(ConnectionType::Tokio)
            .retry_policy(RetryWithBackoff::default_setting().initial_delay(Duration::from_millis(1000)))
            .controller_uri(PravegaNodeUri::from("127.0.0.2:9091".to_string()))
            .build()
            .unwrap();

        assert_eq!(config.max_connections_in_pool(), 15 as u32);
        assert_eq!(config.connection_type(), ConnectionType::Tokio);
        assert_eq!(
            config.retry_policy(),
            RetryWithBackoff::default_setting().initial_delay(Duration::from_millis(1000))
        );
        assert_eq!(
            config.controller_uri().to_socket_addr().ip(),
            Ipv4Addr::new(127, 0, 0, 2)
        );
        assert_eq!(config.controller_uri().to_socket_addr().port(), 9091);
    }

    #[test]
    #[serial]
    fn test_get_default() {
        let config = ClientConfigBuilder::default()
            .controller_uri(MOCK_CONTROLLER_URI)
            .build()
            .unwrap();

        assert_eq!(config.max_connections_in_pool(), u32::MAX as u32);
        assert_eq!(config.max_controller_connections(), 3u32);
        assert_eq!(config.connection_type(), ConnectionType::Tokio);
        assert_eq!(config.retry_policy(), RetryWithBackoff::default_setting());
    }

    #[test]
    #[serial]
    fn test_extract_credentials() {
        let rt = tokio::runtime::Runtime::new().expect("create runtime");
        // test empty env
        let config = ClientConfigBuilder::default()
            .controller_uri("127.0.0.2:9091".to_string())
            .build()
            .unwrap();

        let token = encode(":");

        assert_eq!(
            rt.block_on(config.credentials.get_request_metadata()),
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
            rt.block_on(config.credentials.get_request_metadata()),
            format!("{} {}", "Basic", token)
        );

        // retrieve from env with priority
        env::set_var("pravega_client_auth_token", "ABCDE");
        let config = ClientConfigBuilder::default()
            .controller_uri("127.0.0.2:9091".to_string())
            .build()
            .unwrap();
        assert_eq!(
            rt.block_on(config.credentials.get_request_metadata()),
            format!("{} {}", "Basic", "ABCDE")
        );
        env::remove_var("pravega_client_auth_method");
        env::remove_var("pravega_client_auth_username");
        env::remove_var("pravega_client_auth_password");
        env::remove_var("pravega_client_auth_token");
    }

    #[test]
    #[serial]
    fn test_extract_tls_cert_path() {
        // test default
        fs::create_dir_all(DEFAULT_TLS_CERT_PATH).expect("create default cert path");
        fs::File::create(format!("{}/foo.crt", DEFAULT_TLS_CERT_PATH)).expect("create crt");
        let config = ClientConfigBuilder::default()
            .controller_uri("tls://127.0.0.2:9091".to_string())
            .is_tls_enabled(true)
            .build()
            .unwrap();
        assert_eq!(config.trustcerts.len(), 1);

        // test w/ tls uri prefix
        let config = ClientConfigBuilder::default()
            .controller_uri("tls://127.0.0.2:9091")
            .build()
            .unwrap();
        assert_eq!(config.trustcerts.len(), 1);

        // test w/o tls uri prefix
        let config = ClientConfigBuilder::default()
            .controller_uri("tcp://127.0.0.2:9091".to_string())
            .build()
            .unwrap();
        assert_eq!(config.trustcerts.len(), 0);

        // test conflicting tls setting vs scheme
        let conflicted_config1 = ClientConfigBuilder::default()
            .controller_uri("pravegas://127.0.0.2:9091")
            .is_tls_enabled(false)
            .build();
        assert!(conflicted_config1.is_err());

        // test alternate conflicting tlst setting vs scheme
        let conflicted_config2 = ClientConfigBuilder::default()
            .controller_uri("tcp://127.0.0.2:9091".to_string())
            .is_tls_enabled(true)
            .build();

        assert!(conflicted_config2.is_err());

        fs::remove_dir_all(DEFAULT_TLS_CERT_PATH).expect("remove dir");
        // test with env var set
        fs::create_dir_all("./bar").expect("create default cert path");
        fs::File::create(format!("./bar/foo.crt")).expect("create crt");
        env::set_var("pravega_client_tls_cert_path", "./bar");
        let config = ClientConfigBuilder::default()
            .controller_uri("tls://127.0.0.2:9091".to_string())
            .is_tls_enabled(true)
            .build()
            .unwrap();
        assert_eq!(config.trustcerts.len(), 1);
        // test with file path
        env::set_var("pravega_client_tls_cert_path", "./bar/foo.crt");
        let config = ClientConfigBuilder::default()
            .controller_uri("tls://127.0.0.2:9091".to_string())
            .is_tls_enabled(true)
            .build()
            .unwrap();
        assert_eq!(config.trustcerts.len(), 1);
        env::set_var("pravega_client_tls_cert_path", "./wrong/path");
        // test with invalid path
        let result = std::panic::catch_unwind(|| {
            ClientConfigBuilder::default()
                .controller_uri("tls://127.0.0.2:9091".to_string())
                .is_tls_enabled(true)
                .build()
                .unwrap()
        });
        assert!(result.is_err());
        fs::remove_dir_all("./bar").expect("remove dir");
        env::remove_var("pravega_client_tls_cert_path");
    }
}
