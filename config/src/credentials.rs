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
use reqwest::header::{HeaderMap, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, Read};

pub const URL_TOKEN: &str = "/realms/{realm-name}/protocol/openid-connect/token";
pub const BASIC: &str = "Basic";
pub const BEARER: &str = "Bearer";
pub const UMA_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:uma-ticket";
pub const CLIENT_CREDENTIAL_GRANT_TYPE: &str = "client_credentials";
pub const AUTHORIZATION: &str = "authorization";
pub const AUDIENCE: &str = "pravega-controller";

#[derive(Debug, Clone)]
pub struct Credentials {
    pub method: String,
    pub token: String,
}

impl Credentials {
    pub fn new(method: String, token: String) -> Self {
        Credentials { method, token }
    }

    pub fn basic(user_name: String, password: String) -> Self {
        let decoded = format!("{}:{}", user_name, password);
        let token = encode(decoded);
        Credentials {
            method: BASIC.to_owned(),
            token,
        }
    }

    pub async fn keycloak(path: &str) -> Self {
        // read keycloak json
        let file = File::open(path).expect("open keycloak.json");
        let mut buf_reader = BufReader::new(file);
        let mut buffer = Vec::new();
        buf_reader.read_to_end(&mut buffer).expect("read to the end");

        // decode json string to struct
        let key_cloak_json: KeyCloakJson = serde_json::from_slice(&buffer).expect("decode slice to struct");

        // first POST request for access token
        let access_token = obtain_access_token(
            &key_cloak_json.auth_server_url,
            &key_cloak_json.realm,
            &key_cloak_json.resource,
            &key_cloak_json.credentials.secret,
        )
        .await
        .expect("obtain access token");

        // second POST request for rpt
        let rpt = authorize(
            &key_cloak_json.auth_server_url,
            &key_cloak_json.realm,
            &access_token,
        )
        .await
        .expect("get rpt");
        Credentials {
            method: BEARER.to_owned(),
            token: rpt,
        }
    }

    pub fn get_request_metadata(&self) -> String {
        format!("{} {}", self.method, self.token)
    }
}

#[derive(Serialize, Deserialize)]
struct KeyCloakJson {
    realm: String,
    #[serde(rename(deserialize = "bearer-only"))]
    bearer_only: bool,
    #[serde(rename(deserialize = "auth-server-url"))]
    auth_server_url: String,
    #[serde(rename(deserialize = "ssl-required"))]
    ssl_required: String,
    resource: String,
    #[serde(rename(deserialize = "confidential-port"))]
    confidential_port: i32,
    #[serde(rename(deserialize = "public-client"))]
    public_client: bool,
    credentials: Credential,
}

#[derive(Serialize, Deserialize)]
struct Credential {
    secret: String,
}

async fn obtain_access_token(
    base_url: &str,
    realm: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<String, reqwest::Error> {
    let url = URL_TOKEN.replace("{realm-name}", realm);

    let payload = serde_json::json!({
        "client_id": client_id.to_owned(),
        "client_secret": client_secret.to_owned(),
        "grant_type": CLIENT_CREDENTIAL_GRANT_TYPE.to_owned(),
    });

    let path = base_url.to_owned() + &url.to_owned();

    let mut header_map = HeaderMap::new();
    header_map.insert(CONTENT_TYPE, "application/json".parse().unwrap());
    let token = send_http_request(&path, payload, header_map).await?;
    Ok(token.access_token)
}

async fn authorize(base_url: &str, realm: &str, token: &str) -> Result<String, reqwest::Error> {
    let url = URL_TOKEN.replace("{realm-name}", realm);

    let payload = serde_json::json!({
        "grant_type": UMA_GRANT_TYPE.to_owned(),
        "audience": AUDIENCE.to_owned(),
    });

    let path = base_url.to_owned() + &url.to_owned();

    let mut header_map = HeaderMap::new();
    let bearer = format!("{} {}", BEARER, token);
    header_map.insert(AUTHORIZATION, bearer.parse().unwrap());
    let rpt = send_http_request(&path, payload, header_map).await?;
    Ok(rpt.access_token)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Token {
    pub access_token: String,
}

async fn send_http_request(
    path: &str,
    payload: serde_json::Value,
    header_map: HeaderMap,
) -> Result<Token, reqwest::Error> {
    let client = reqwest::Client::new();
    let response = client
        .post(path)
        .headers(header_map)
        .form(&payload)
        .send()
        .await?
        .error_for_status()?;
    response.json().await
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_base64_compat_with_java() {
        let cred = Credentials::basic("admin".to_owned(), "1111_aaaa".to_owned());
        assert_eq!(cred.token, "YWRtaW46MTExMV9hYWFh");
    }

    #[test]
    fn test_json_deserialize() {
        let json_string = r#"{"realm":"nautilus","auth-server-url":"http://keycloak.jarviscb.nautilus-lab-ns.com/auth","ssl-required":"NONE","bearer-only":false,"public-client":false,"resource":"pravega-controller","confidential-port":0,"credentials":{"secret":"123456"}}"#;
        let v: KeyCloakJson = serde_json::from_str(json_string).unwrap();
        assert_eq!(v.realm, "nautilus");
        assert_eq!(
            v.auth_server_url,
            "http://keycloak.jarviscb.nautilus-lab-ns.com/auth"
        );
        assert_eq!(v.ssl_required, "NONE");
        assert!(!v.bearer_only);
        assert!(!v.public_client);
        assert_eq!(v.resource, "pravega-controller");
        assert_eq!(v.confidential_port, 0);
        assert_eq!(v.confidential_port, 0);
        assert_eq!(v.credentials.secret, "123456");
    }
}
