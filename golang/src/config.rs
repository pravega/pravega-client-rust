use libc::c_char;
use pravega_client_config::credentials::Credentials;
use pravega_client_config::*;
use pravega_client_retry::retry_policy::RetryWithBackoff;
use pravega_client_shared::*;
use std::ffi::CStr;
use std::ops::Add;
use std::time::{Duration, Instant};

const SPLIT: &str = ",";

#[repr(C)]
pub struct StreamConfigurationMapping {
    pub scope: *const c_char,
    pub stream: *const c_char,
    pub scaling: ScalingMapping,
    pub retention: RetentionMapping,
    pub tags: *const c_char, // split by ','
}

impl StreamConfigurationMapping {
    pub fn to_stream_configuration(&self) -> StreamConfiguration {
        let scope = to_str(self.scope);
        let stream = to_str(self.stream);
        let scope_stream = scope.to_string() + "/" + stream;
        StreamConfiguration {
            scoped_stream: ScopedStream::from(scope_stream.as_str()),
            scaling: self.scaling.to_scaling(),
            retention: self.retention.to_retention(),
            tags: str_to_tags(self.tags),
        }
    }
}

fn to_str<'a>(p_str: *const c_char) -> &'a str {
    unsafe {
        let raw = CStr::from_ptr(p_str);
        return raw.to_str().unwrap();
    }
}

#[repr(C)]
pub struct RetentionMapping {
    pub retention_type: RetentionTypeMapping,
    pub retention_param: i64,
}

impl RetentionMapping {
    fn to_retention(&self) -> Retention {
        Retention {
            retention_type: self.retention_type.to_retention_type(),
            retention_param: self.retention_param,
        }
    }
}

#[repr(C)]
pub struct RetentionTypeMapping {
    pub value: i32,
}

impl RetentionTypeMapping {
    fn to_retention_type(&self) -> RetentionType {
        match self.value {
            0 => RetentionType::None,
            1 => RetentionType::Time,
            2 => RetentionType::Size,
            _ => RetentionType::None,
        }
    }
}

#[repr(C)]
pub struct ScalingMapping {
    pub scale_type: ScaleTypeMapping,
    pub target_rate: i32,
    pub scale_factor: i32,
    pub min_num_segments: i32,
}

impl ScalingMapping {
    fn to_scaling(&self) -> Scaling {
        Scaling {
            scale_type: self.scale_type.to_scale_type(),
            target_rate: self.target_rate,
            scale_factor: self.scale_factor,
            min_num_segments: self.min_num_segments,
        }
    }
}

#[repr(C)]
pub struct ScaleTypeMapping {
    pub value: i32,
}

impl ScaleTypeMapping {
    fn to_scale_type(&self) -> ScaleType {
        match self.value {
            0 => ScaleType::FixedNumSegments,
            1 => ScaleType::ByRateInKbytesPerSec,
            2 => ScaleType::ByRateInEventsPerSec,
            _ => ScaleType::FixedNumSegments,
        }
    }
}

#[repr(C)]
pub struct ClientConfigMapping {
    pub max_connections_in_pool: u32,

    pub max_controller_connections: u32,

    pub retry_policy: RetryWithBackoffMapping,

    pub controller_uri: *const c_char,

    pub transaction_timeout_time: u64,

    pub is_tls_enabled: bool,

    pub disable_cert_verification: bool,

    pub trustcerts: *const c_char,

    pub credentials: CredentialsMapping,

    pub is_auth_enabled: bool,

    pub reader_wrapper_buffer_size: usize,

    pub request_timeout: u64,
}

impl ClientConfigMapping {
    pub unsafe fn to_client_config(&self) -> ClientConfig {
        let raw = CStr::from_ptr(self.controller_uri);
        let controller_uri = raw.to_str().unwrap();
        let mut config: ClientConfig = ClientConfigBuilder::default()
            .controller_uri(controller_uri)
            .build()
            .unwrap();
        config.max_connections_in_pool = self.max_connections_in_pool;
        config.max_controller_connections = self.max_controller_connections;
        config.disable_cert_verification = self.disable_cert_verification;
        config.is_auth_enabled = self.is_auth_enabled;
        config.is_tls_enabled = self.is_tls_enabled;
        config.reader_wrapper_buffer_size = self.reader_wrapper_buffer_size;
        config.transaction_timeout_time = self.transaction_timeout_time;
        config.request_timeout = Duration::from_millis(self.request_timeout);
        config.trustcerts = split_to_vec(self.trustcerts);
        config.credentials = self.credentials.to_credentials();
        config.retry_policy = self.retry_policy.to_retry_with_backoff();
        config
    }
}

unsafe fn split_to_vec(s: *const c_char) -> Vec<String> {
    let splits = to_str(s).split(SPLIT);
    let vec = splits.collect::<Vec<&str>>();
    let mut v: Vec<String> = Vec::new();
    for x in vec {
        v.push(String::from(x));
    }
    v
}

fn str_to_tags(s: *const c_char) -> Option<Vec<String>> {
    if s.is_null() {
        return Option::None;
    }
    unsafe { Some(split_to_vec(s)) }
}

#[repr(C)]
pub struct RetryWithBackoffMapping {
    initial_delay: u64,
    backoff_coefficient: u32,
    max_delay: u64,
    max_attempt: i32,
    expiration_time: i64,
}

impl RetryWithBackoffMapping {
    pub unsafe fn to_retry_with_backoff(&self) -> RetryWithBackoff {
        let backoff_coefficient = self.backoff_coefficient as u32;
        let initial_delay = Duration::from_millis(self.initial_delay);
        let max_delay = Duration::from_millis(self.max_delay);
        let backoff = RetryWithBackoff::default()
            .backoff_coefficient(backoff_coefficient)
            .initial_delay(initial_delay)
            .max_delay(max_delay);
        if self.max_attempt > 0 {
            backoff.max_attempt(self.max_attempt as usize);
        }
        if self.expiration_time > 0 {
            let duration = Duration::from_millis(self.expiration_time as u64);
            backoff.expiration_time(Instant::now().add(duration));
        }
        backoff
    }
}

#[repr(C)]
pub struct CredentialsMapping {
    credential_type: CredentialsType,
    username: *const c_char,
    password: *const c_char,
    token: *const c_char,
    path: *const c_char,
    json: *const c_char,
    disable_cert_verification: bool,
}

impl CredentialsMapping {
    unsafe fn to_credentials(&self) -> Credentials {
        return match self.credential_type.value {
            0 => {
                let username = String::from(to_str(self.username));
                let password = String::from(to_str(self.password));
                Credentials::basic(username, password)
            }
            1 => {
                let token = String::from(to_str(self.token));
                Credentials::basic_with_token(token)
            }
            2 => Credentials::keycloak(to_str(self.path), self.disable_cert_verification),
            3 => Credentials::keycloak(to_str(self.json), self.disable_cert_verification),
            _ => Credentials::basic(String::from(""), String::from("")),
        };
    }
}

#[repr(C)]
pub struct CredentialsType {
    pub value: i32,
}
