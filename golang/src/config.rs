
use libc::c_char;
use pravega_client_shared::*;
use pravega_client_config::*;
use std::ffi::{CStr, CString};
use std::str::FromStr;
use std::time::Duration;
use pravega_client_config::credentials::Credentials;
use pravega_client_retry::retry_policy::RetryWithBackoff;


const SPLIT: &str = ",";


#[repr(C)]
pub struct BStreamConfiguration {
    pub scoped_stream: BScopedStream,
    pub scaling: BScaling,
    pub retention: BRetention,
    pub tags: *const c_char, // split by ','
}

impl BStreamConfiguration {
    unsafe fn to_stream_configuration(&self) -> StreamConfiguration {
        StreamConfiguration {
            scoped_stream: self.scoped_stream.to_scoped_stream(),
            scaling: self.scaling.to_scaling(),
            retention: self.retention.to_retention(),
            tags: str_to_tags(self.tags),
        }
    }
}

#[repr(C)]
pub struct BScopedStream {
    pub scope: *const c_char,
    pub stream: *const c_char,
}

impl BScopedStream {
    unsafe fn to_scoped_stream(&self) -> ScopedStream {
        return ScopedStream {
            scope: Scope { name: String::from(to_str(self.scope)) },
            stream: Stream { name: String::from(to_str(self.stream)) },
        };
    }
}

unsafe fn to_str<'a>(p_str: *const c_char) -> &'a str {
    let raw = CStr::from_ptr(p_str);
    return raw.to_str().unwrap();
}

#[repr(C)]
pub struct BRetention {
    pub retention_type: BRetentionType,
    pub retention_param: usize,
}

impl BRetention {
    fn to_retention(&self) -> Retention {
        Retention {
            retention_type: self.retention_type.to_retention_type(),
            retention_param: self.retention_param as i64,
        }
    }
}


#[repr(C)]
pub enum BRetentionType {
    None = 0,
    Time = 1,
    Size = 2,
}

impl BRetentionType {
    fn to_retention_type(&self) -> RetentionType {
        match self {
            BRetentionType::None => { RetentionType::None }
            BRetentionType::Time => { RetentionType::Time }
            BRetentionType::Size => { RetentionType::Size }
        }
    }
}


#[repr(C)]
pub struct BScaling {
    pub scale_type: BScaleType,
    pub target_rate: usize,
    pub scale_factor: usize,
    pub min_num_segments: usize,
}


impl BScaling {
    fn to_scaling(&self) -> Scaling {
        Scaling {
            scale_type: self.scale_type.to_scale_type(),
            target_rate: self.target_rate as i32,
            scale_factor: self.scale_factor as i32,
            min_num_segments: self.min_num_segments as i32,
        }
    }
}


#[repr(C)]
pub enum BScaleType {
    FixedNumSegments = 0,
    ByRateInKbytesPerSec = 1,
    ByRateInEventsPerSec = 2,
}

impl BScaleType {
    fn to_scale_type(&self) -> ScaleType {
        match self {
            BScaleType::FixedNumSegments => { ScaleType::FixedNumSegments }
            BScaleType::ByRateInKbytesPerSec => { ScaleType::ByRateInKbytesPerSec }
            BScaleType::ByRateInEventsPerSec => { ScaleType::ByRateInEventsPerSec }
        }
    }
}



#[repr(C)]
pub struct BClientConfig {
    pub max_connections_in_pool: usize,

    pub max_controller_connections: usize,

    pub retry_policy: BRetryWithBackoff,

    pub controller_uri: *const c_char,

    pub transaction_timeout_time: usize,

    pub is_tls_enabled: bool,

    pub disable_cert_verification: bool,

    pub trustcerts: *const c_char,

    pub credentials: BCredentials,

    pub is_auth_enabled: bool,

    pub reader_wrapper_buffer_size: usize,

    pub request_timeout: usize,
}

impl BClientConfig {
    pub unsafe fn to_client_config(&self) -> ClientConfig {
        let raw = CStr::from_ptr(self.controller_uri);
        let controller_uri = raw.to_str().unwrap();
        let mut config: ClientConfig = ClientConfigBuilder::default()
            .controller_uri(controller_uri)
            .build().unwrap();
        config.max_connections_in_pool = self.max_connections_in_pool as u32;
        config.max_controller_connections = self.max_controller_connections as u32;
        config.disable_cert_verification = self.disable_cert_verification;
        config.is_auth_enabled = self.is_auth_enabled;
        config.is_tls_enabled = self.is_tls_enabled;
        config.reader_wrapper_buffer_size = self.reader_wrapper_buffer_size as usize;
        config.transaction_timeout_time = self.transaction_timeout_time as u64;
        config.request_timeout = Duration::from_millis(self.request_timeout as u64);
        config.trustcerts = split_to_vec(self.trustcerts);
        config.credentials = self.credentials.to_credentials();
        config.retry_policy = self.retry_policy.to_retry_with_backoff();
        return config;
    }
}


unsafe fn split_to_vec(s: *const c_char) -> Vec<String> {
    let mut splits = to_str(s).split(SPLIT);
    let vec = splits.collect::<Vec<&str>>();
    let mut v: Vec<String> = Vec::new();
    for x in vec {
        v.push(String::from(x));
    }
    return v;
}

unsafe fn str_to_tags(s: *const c_char) -> Option<Vec<String>> {
    if s.is_null() {
        return Option::None;
    }
    return Some(split_to_vec(s));
}

#[test]
fn test_str_to_tags() {
    let strs = CString::new("Hello,World").unwrap().into_raw();
    unsafe {
        let vec = str_to_tags(strs).unwrap();
        println!("{:?}",vec);
        assert_eq!(vec[0], "Hello")
    }

}
#[repr(C)]
pub struct BRetryWithBackoff {
    initial_delay: usize,
    backoff_coefficient: usize,
    max_attempt: usize,
    max_delay: usize,
    expiration_time: usize,
}

impl BRetryWithBackoff {
    pub unsafe fn to_retry_with_backoff(&self) -> RetryWithBackoff {
        //TODO: set expiration_time
        let backoff_coefficient = self.backoff_coefficient as u32;
        let initial_delay = Duration::from_millis(self.initial_delay as u64);
        let max_delay = Duration::from_millis(self.max_delay as u64);
        return  RetryWithBackoff::default().backoff_coefficient(backoff_coefficient).initial_delay(initial_delay)
            .max_attempt(self.max_attempt as usize).max_delay(max_delay);
    }

}

#[repr(C)]
pub struct BCredentials {
    credential_type: CredentialsType,
    username: *const c_char,
    password: *const c_char,
    token: *const c_char,
    path: *const c_char,
    json: *const c_char,
    disable_cert_verification: bool,
}

impl BCredentials {
    unsafe fn to_credentials(&self) -> Credentials {
        return match self.credential_type {
            CredentialsType::Basic => unsafe {
                let username = String::from(to_str(self.username));
                let password = String::from(to_str(self.password));
                Credentials::basic(username, password)
            }
            CredentialsType::BasicWithToken => {
                let token = String::from(to_str(self.token));
                Credentials::basic_with_token(token)
            }
            CredentialsType::Keycloak => {
                Credentials::keycloak(to_str(self.path), self.disable_cert_verification)
            }
            CredentialsType::KeycloakFromJsonString => {
                Credentials::keycloak(to_str(self.json), self.disable_cert_verification)
            }
        };
    }
}

#[repr(C)]
pub enum CredentialsType {
    Basic = 0,
    BasicWithToken = 1,
    Keycloak = 2,
    KeycloakFromJsonString = 3,
}