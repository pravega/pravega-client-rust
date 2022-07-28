/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
use async_trait::async_trait;
use base64::encode;
use reqwest::header::{HeaderMap, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;

pub const URL_TOKEN: &str = "/realms/{realm-name}/protocol/openid-connect/token";
pub const BASIC: &str = "Basic";
pub const BEARER: &str = "Bearer";
pub const UMA_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:uma-ticket";
pub const CLIENT_CREDENTIAL_GRANT_TYPE: &str = "client_credentials";
pub const AUTHORIZATION: &str = "authorization";
pub const AUDIENCE: &str = "pravega-controller";

const REFRESH_THRESHOLD_SECONDS: u64 = 5;

#[derive(Debug)]
pub struct Credentials {
    inner: Box<dyn Cred>,
}

impl Credentials {
    pub fn basic(user_name: String, password: String) -> Self {
        let decoded = format!("{}:{}", user_name, password);
        let token = encode(decoded);
        let basic = Basic {
            method: BASIC.to_owned(),
            token,
        };
        Credentials {
            inner: Box::new(basic) as Box<dyn Cred>,
        }
    }

    pub fn basic_with_token(token: String) -> Self {
        let basic = Basic {
            method: BASIC.to_owned(),
            token,
        };
        Credentials {
            inner: Box::new(basic) as Box<dyn Cred>,
        }
    }

    pub fn keycloak(path: &str, disable_cert_verification: bool) -> Self {
        // read keycloak json
        let file = File::open(path).expect("open keycloak.json");
        let mut buf_reader = BufReader::new(file);
        let mut buffer = Vec::new();
        buf_reader.read_to_end(&mut buffer).expect("read to the end");

        // decode json string to struct
        let key_cloak_json: KeyCloakJson = serde_json::from_slice(&buffer).expect("decode slice to struct");

        let keycloak = KeyCloak {
            method: BEARER.to_string(),
            token: Arc::new(Mutex::new("".to_string())),
            json: key_cloak_json,
            expires_at: Arc::new(AtomicU64::new(0)),
            disable_cert_verification,
        };
        Credentials {
            inner: Box::new(keycloak) as Box<dyn Cred>,
        }
    }

    pub fn keycloak_from_json_string(json: &str, disable_cert_verification: bool) -> Self {
        // decode json string to struct
        let key_cloak_json: KeyCloakJson = serde_json::from_str(json).expect("decode slice to struct");
        let keycloak = KeyCloak {
            method: BEARER.to_string(),
            token: Arc::new(Mutex::new("".to_string())),
            json: key_cloak_json,
            expires_at: Arc::new(AtomicU64::new(0)),
            disable_cert_verification,
        };
        Credentials {
            inner: Box::new(keycloak) as Box<dyn Cred>,
        }
    }

    pub async fn get_request_metadata(&self) -> String {
        self.inner.get_request_metadata().await
    }

    pub fn is_expired(&self) -> bool {
        self.inner.is_expired()
    }
}

impl Clone for Credentials {
    fn clone(&self) -> Self {
        Credentials {
            inner: self.inner.clone_box(),
        }
    }
}

#[async_trait]
trait Cred: Debug + CredClone + Send + Sync {
    async fn get_request_metadata(&self) -> String;
    fn is_expired(&self) -> bool;
}

trait CredClone {
    fn clone_box(&self) -> Box<dyn Cred>;
}

impl<T> CredClone for T
where
    T: 'static + Cred + Clone,
{
    fn clone_box(&self) -> Box<dyn Cred> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
struct Basic {
    method: String,
    token: String,
}

#[async_trait]
impl Cred for Basic {
    async fn get_request_metadata(&self) -> String {
        format!("{} {}", self.method, self.token)
    }

    fn is_expired(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone)]
struct KeyCloak {
    method: String,
    token: Arc<Mutex<String>>,
    json: KeyCloakJson,
    expires_at: Arc<AtomicU64>,
    disable_cert_verification: bool,
}

#[async_trait]
impl Cred for KeyCloak {
    async fn get_request_metadata(&self) -> String {
        if self.is_expired() {
            self.refresh_rpt_token().await;
        }
        format!("{} {}", self.method, *self.token.lock().await)
    }

    fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("get unix time");
        now.as_secs() + REFRESH_THRESHOLD_SECONDS >= self.expires_at.load(Ordering::SeqCst)
    }
}

impl KeyCloak {
    async fn refresh_rpt_token(&self) {
        // first POST request for access token
        let access_token = obtain_access_token(
            &self.json.auth_server_url,
            &self.json.realm,
            &self.json.resource,
            &self.json.credentials.secret,
            self.disable_cert_verification,
        )
        .await
        .expect("obtain access token");

        // second POST request for rpt
        let rpt = authorize(
            &self.json.auth_server_url,
            &self.json.realm,
            &access_token,
            self.disable_cert_verification,
        )
        .await
        .expect("get rpt");

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("get unix time");
        let expires_at = now.as_secs() + rpt.expires_in;

        *self.token.lock().await = rpt.access_token;
        self.expires_at.store(expires_at, Ordering::SeqCst);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct KeyCloakJson {
    realm: String,
    #[serde(rename(deserialize = "auth-server-url"))]
    auth_server_url: String,
    resource: String,
    credentials: Credential,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Credential {
    secret: String,
}

async fn obtain_access_token(
    base_url: &str,
    realm: &str,
    client_id: &str,
    client_secret: &str,
    disable_cert_verification: bool,
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
    let token = send_http_request(&path, payload, header_map, disable_cert_verification).await?;
    Ok(token.access_token)
}

async fn authorize(
    base_url: &str,
    realm: &str,
    token: &str,
    disable_cert_verification: bool,
) -> Result<Token, reqwest::Error> {
    let url = URL_TOKEN.replace("{realm-name}", realm);

    let payload = serde_json::json!({
        "grant_type": UMA_GRANT_TYPE.to_owned(),
        "audience": AUDIENCE.to_owned(),
    });

    let path = base_url.to_owned() + &url.to_owned();

    let mut header_map = HeaderMap::new();
    let bearer = format!("{} {}", BEARER, token);
    header_map.insert(AUTHORIZATION, bearer.parse().unwrap());
    let rpt = send_http_request(&path, payload, header_map, disable_cert_verification).await?;
    Ok(rpt)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Token {
    pub access_token: String,
    pub expires_in: u64,
}

async fn send_http_request(
    path: &str,
    payload: serde_json::Value,
    header_map: HeaderMap,
    disable_cert_verification: bool,
) -> Result<Token, reqwest::Error> {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(disable_cert_verification)
        .build()?;
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
    fn test_json_deserialize() {
        let json_string = r#"{"realm":"nautilus","auth-server-url":"http://localhost","ssl-required":"NONE","bearer-only":false,"public-client":false,"resource":"pravega-controller","confidential-port":0,"credentials":{"secret":"123456"}}"#;
        let v: KeyCloakJson = serde_json::from_str(json_string).unwrap();
        assert_eq!(v.realm, "nautilus");
        assert_eq!(v.auth_server_url, "http://localhost");
        assert_eq!(v.resource, "pravega-controller");
        assert_eq!(v.credentials.secret, "123456");
    }
}
