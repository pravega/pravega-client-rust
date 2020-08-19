/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
use base64::encode;

/// HTTP "Basic" authentication scheme.
pub const BASIC: &str = "Basic";

/// HTTP "Authorization" header.
pub const AUTHORIZATION: &str = "authorization";

/// Credentials could use Basic method, which is user_name and password combination, and Bearer method,
/// which is token based. The default method is Basic type.
#[derive(Debug, Clone)]
pub struct Credentials {
    method: String,
    token: String,
}

impl Credentials {
    pub fn new(method: String, token: String) -> Self {
        Credentials { method, token }
    }

    pub fn default(user_name: String, password: String) -> Self {
        let decoded = format!("{}:{}", user_name, password);
        let token = encode(decoded);
        Credentials {
            method: BASIC.to_owned(),
            token,
        }
    }

    pub fn get_authentication_type(&self) -> String {
        self.method.clone()
    }

    pub fn get_authentication_token(&self) -> String {
        self.token.clone()
    }

    pub fn get_request_metadata(&self) -> String {
        format!("{} {}", self.method, self.token)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_base64_compat_with_java() {
        let cred = Credentials::default("admin".to_owned(), "1111_aaaa".to_owned());
        assert_eq!(cred.token, "YWRtaW46MTExMV9hYWFh");
    }
}
