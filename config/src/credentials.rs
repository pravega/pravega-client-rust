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
const BASIC: &str = "Basic";

/// HTTP "Bearer" authentication scheme.
const BEARER: &str = "Bearer";

/// HTTP "Authorization" header.
const AUTHORIZATION: &str = "Authorization";

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
}
