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

use async_trait::async_trait;
use base64::decode;
use lazy_static::*;
use pravega_controller_client::ControllerClient;
use pravega_rust_client_shared::{DelegationToken, ScopedStream};
use regex::Regex;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;

/// A client-side proxy for obtaining a delegation token from the server.
///
/// Note: Delegation tokens are used by Segment Store services to authorize requests. They are created by Controller at
/// client's behest.
///
#[async_trait]
pub trait DelegationTokenProvider: Send + Sync {
    /// Retrieve delegation token.
    async fn retrieve_token(&self) -> String;
}

/// Provides empty delegation tokens. This provider is useful when auth is disabled.
pub struct EmptyTokenProviderImpl {}

#[async_trait]
impl DelegationTokenProvider for EmptyTokenProviderImpl {
    async fn retrieve_token(&self) -> String {
        "".to_owned()
    }
}

pub struct JwtTokenProviderImpl {
    stream: ScopedStream,
    controller: Arc<Box<dyn ControllerClient>>,
    token: Mutex<Option<DelegationToken>>,
}

const DEFAULT_REFRESH_THRESHOLD_SECONDS: u64 = 5;

#[async_trait]
impl DelegationTokenProvider for JwtTokenProviderImpl {
    /// Returns the delegation token. It returns an existing delegation token if it is not close to expiry.
    /// If the token is close to expiry, it obtains a new delegation token and returns that one instead.
    async fn retrieve_token(&self) -> String {
        let guard = self.token.lock().await;
        if let Some(ref token) = *guard {
            if JwtTokenProviderImpl::is_token_valid(token.get_expiry_time()) {
                return token.get_value();
            }
        }
        self.refresh_token().await.get_value()
    }
}

impl JwtTokenProviderImpl {
    pub fn new(stream: ScopedStream, controller: Arc<Box<dyn ControllerClient>>) -> Self {
        JwtTokenProviderImpl {
            stream,
            controller,
            token: Mutex::new(None),
        }
    }

    async fn refresh_token(&self) -> DelegationToken {
        let token = self
            .controller
            .get_or_refresh_delegation_token_for(self.stream.clone())
            .await
            .expect("controller error when refreshing token");
        DelegationToken::new(token.clone(), extract_expiration_time(token))
    }

    fn is_token_valid(time: Option<u64>) -> bool {
        if let Some(t) = time {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("get unix time");
            if now.as_secs() + DEFAULT_REFRESH_THRESHOLD_SECONDS >= t {
                return false;
            }
        }
        true
    }
}

fn extract_expiration_time(json_web_token: String) -> Option<u64> {
    if json_web_token.trim() == "" {
        return None;
    }

    let token_parts: Vec<&str> = json_web_token.split('.').collect();

    // A JWT token has 3 parts: the header, the body and the signature.
    assert_eq!(token_parts.len(), 3);

    // The second part of the JWT token is the body, which contains the expiration time if present.
    let encoded_body = token_parts[1].to_owned();
    let decoded_json_body = decode(encoded_body).expect("decode JWT body");
    let string_body = String::from_utf8(decoded_json_body).expect("parse JWT raw bytes body to Rust string");
    Some(parse_expiration_time(string_body))
}

lazy_static! {
    static ref RE: Regex = Regex::new(r#""exp":\s?(?P<body>\d+)"#).unwrap();
}

/// The regex pattern for extracting "exp" field from the JWT.
/// Examples:
///     Input:- {"sub":"subject","aud":"segmentstore","iat":1569837384,"exp":1569837434}, output:- "exp":1569837434
///     Input:- {"sub": "subject","aud": "segmentstore","iat": 1569837384,"exp": 1569837434}, output:- "exp": 1569837434
fn parse_expiration_time(jwt_body: String) -> u64 {
    let cap = RE.captures(&jwt_body).expect("regex matching jwt body");
    let matched_value = cap
        .name("body")
        .map(|body| body.as_str())
        .expect("get expiry time");
    u64::from_str(matched_value).expect("convert to u64")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_extract_expiration_time() {
        let encoded_jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJzdWJqZWN0IiwiYXVkIjoic2VnbWVudHN0b3JlIiwiaWF0IjoxNTY5ODM3Mzg0LCJleHAiOjE1Njk4Mzc0MzR9.wYSsKf8BirFoT2KY4dhzSFiWaUc9b4xe_jECKJWnR-k";
        let time = extract_expiration_time(encoded_jwt.to_owned());

        assert!(time.is_some());
        let time = time.expect("extract expiry time");
        assert_eq!(1569837434 as u64, time);
    }

    #[test]
    fn test_parse_expiration_time() {
        let input1 = r#"{"sub":"subject","aud":"segmentstore","iat":1569837384,"exp":1569837434}, output:- "exp":1569837434"#;
        let input2 = r#"{"sub": "subject","aud": "segmentstore","iat": 1569837384,"exp": 1569837434}, output:- "exp": 1569837434"#;

        assert_eq!(1569837434 as u64, parse_expiration_time(input1.to_owned()));
        assert_eq!(1569837434 as u64, parse_expiration_time(input2.to_owned()));
    }
}
