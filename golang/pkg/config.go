package pkg

/*
#include "pravega_client.h"
*/
import "C"

import (
	"math"
	"time"
)

type CredentialsType int

const (
	CredentialsBasic          CredentialsType = 0
	CredentialsBasicWithToken CredentialsType = 1
	CredentialsKeycloak       CredentialsType = 2
	CredentialsJson           CredentialsType = 3
)

type RetryWithBackoff struct {
	InitialDelay       uint64
	BackoffCoefficient uint32
	MaxAttempt         int32
	MaxDelay           uint64
	ExpirationTime     int64
}

func (r *RetryWithBackoff) toCtype() *C.RetryWithBackoffMapping {
	backoff := &C.RetryWithBackoffMapping{}
	backoff.initial_delay = cu64(r.InitialDelay)
	backoff.backoff_coefficient = cu32(r.BackoffCoefficient)
	backoff.max_attempt = ci32(r.MaxAttempt)
	backoff.max_delay = cu64(r.MaxDelay)
	backoff.expiration_time = ci64(r.ExpirationTime)
	return backoff

}

func NewRetryWithBackoff() *RetryWithBackoff {
	return &RetryWithBackoff{
		InitialDelay:       1,
		BackoffCoefficient: 10,
		MaxDelay:           10000,
		ExpirationTime:     -1,
		MaxAttempt:         -1,
	}
}

type Credentials struct {
	Type                    CredentialsType
	Username                string
	Password                string
	Token                   string
	Path                    string
	Json                    string
	DisableCertVerification bool
}

func NewCredentials() *Credentials {
	return &Credentials{
		Type:                    CredentialsBasic,
		DisableCertVerification: false,
	}
}

func (c *Credentials) toCtype() *C.CredentialsMapping {
	cm := &C.CredentialsMapping{}
	switch c.Type {
	case CredentialsBasic:
		cm.credential_type.value = ci32(CredentialsBasic)
	case CredentialsBasicWithToken:
		cm.credential_type.value = ci32(CredentialsBasicWithToken)
	case CredentialsKeycloak:
		cm.credential_type.value = ci32(CredentialsKeycloak)
	case CredentialsJson:
		cm.credential_type.value = ci32(CredentialsJson)
	}
	cm.username = C.CString(c.Username)
	cm.password = C.CString(c.Password)
	cm.token = C.CString(c.Token)
	cm.path = C.CString(c.Path)
	cm.json = C.CString(c.Json)
	cm.disable_cert_verification = C.bool(c.DisableCertVerification)
	return cm
}

type ClientConfig struct {
	MaxConnectionsInPool     uint32
	MaxControllerConnections uint32
	RetryPolicy              *RetryWithBackoff
	ControllerUri            string
	TransactionTimeout       uint64
	TlsEnabled               bool
	AuthEnabled              bool
	ReaderWrapperBufferSize  uint64
	RequestTimeout           uint64
	Trustcerts               string
	Credentials              *Credentials
	DisableCertVerification  bool
}

func NewClientConfig() *ClientConfig {
	return &ClientConfig{
		MaxConnectionsInPool:     math.MaxUint32,
		MaxControllerConnections: 3,
		RetryPolicy:              NewRetryWithBackoff(),
		ControllerUri:            "localhost:9090",
		TransactionTimeout:       90 * 1000,
		TlsEnabled:               false,
		AuthEnabled:              false,
		ReaderWrapperBufferSize:  1024 * 1024,
		RequestTimeout:           uint64(30 * time.Second.Milliseconds()),
		Trustcerts:               "",
		Credentials:              NewCredentials(),
		DisableCertVerification:  false,
	}
}

func (clientConfig *ClientConfig) toCtype() C.ClientConfigMapping {
	bconfig := C.ClientConfigMapping{}
	bconfig.max_connections_in_pool = cu32(clientConfig.MaxConnectionsInPool)
	bconfig.max_controller_connections = cu32(clientConfig.MaxControllerConnections)
	bconfig.controller_uri = C.CString(clientConfig.ControllerUri)
	bconfig.transaction_timeout_time = cu64(clientConfig.TransactionTimeout)
	bconfig.is_tls_enabled = cbool(clientConfig.TlsEnabled)
	bconfig.is_auth_enabled = cbool(clientConfig.AuthEnabled)
	bconfig.trustcerts = C.CString(clientConfig.Trustcerts)
	bconfig.reader_wrapper_buffer_size = usize(clientConfig.ReaderWrapperBufferSize)

	bconfig.credentials = *clientConfig.Credentials.toCtype()

	bconfig.disable_cert_verification = cbool(clientConfig.DisableCertVerification)
	bconfig.request_timeout = cu64(clientConfig.RequestTimeout)

	bconfig.retry_policy = *clientConfig.RetryPolicy.toCtype()

	return bconfig
}

func freeClientConfig(c *C.ClientConfigMapping) {
	//The C.CString and C.CBytes functions are documented as doing so internally, and requiring the use of C.free, but other structs are managed by go GC
	freeCString(c.credentials.json)
	freeCString(c.credentials.password)
	freeCString(c.credentials.path)
	freeCString(c.credentials.token)
	freeCString(c.credentials.username)
	freeCString(c.controller_uri)
	freeCString(c.trustcerts)
}

type RetentionType int
type ScaleType int

const (
	RetentionNone        RetentionType = 0
	RetentionTime        RetentionType = 1
	RetentionSize        RetentionType = 2
	FixedNumSegments     ScaleType     = 0
	ByRateInKbytesPerSec ScaleType     = 1
	ByRateInEventsPerSec ScaleType     = 2
)

type ScalePolicy struct {
	Type        ScaleType
	TargetRate  int32
	ScaleFactor int32
	MinSegments int32
}
type RetentionPolicy struct {
	Type           RetentionType
	RetentionParam int64
}

type StreamConfiguration struct {
	Scope     string
	Stream    string
	Scale     ScalePolicy
	Retention RetentionPolicy
	Tags      string
}

func NewStreamConfiguration(scope, stream string) *StreamConfiguration {
	return &StreamConfiguration{
		Scope:  scope,
		Stream: stream,
		Scale: ScalePolicy{
			Type:        FixedNumSegments,
			TargetRate:  0,
			ScaleFactor: 0,
			MinSegments: 3,
		},
		Retention: RetentionPolicy{
			Type:           RetentionNone,
			RetentionParam: 0,
		},
	}
}

func (sp *ScalePolicy) toCtype() C.ScalingMapping {
	scaling := C.ScalingMapping{}
	switch sp.Type {
	case FixedNumSegments:
		scaling.scale_type.value = ci32(FixedNumSegments)
	case ByRateInKbytesPerSec:
		scaling.scale_type.value = ci32(ByRateInKbytesPerSec)
	case ByRateInEventsPerSec:
		scaling.scale_type.value = ci32(ByRateInEventsPerSec)
	}
	scaling.target_rate = ci32(sp.TargetRate)
	scaling.scale_factor = ci32(sp.ScaleFactor)
	scaling.min_num_segments = ci32(sp.MinSegments)
	return scaling
}

func (rp *RetentionPolicy) toCtype() C.RetentionMapping {
	retention := C.RetentionMapping{}
	switch rp.Type {
	case RetentionNone:
		retention.retention_type.value = ci32(RetentionNone)
	case RetentionTime:
		retention.retention_type.value = ci32(RetentionTime)
	case RetentionSize:
		retention.retention_type.value = ci32(RetentionSize)
	}
	retention.retention_param = ci64(rp.RetentionParam)
	return retention
}

func (cfg *StreamConfiguration) toCtype() *C.StreamConfigurationMapping {
	c_stream_config := &C.StreamConfigurationMapping{}
	c_stream_config.scope = C.CString(cfg.Scope)
	c_stream_config.stream = C.CString(cfg.Stream)
	c_stream_config.scaling = cfg.Scale.toCtype()
	c_stream_config.retention = cfg.Retention.toCtype()
	return c_stream_config
}

func freeStreamConfiguration(scf *C.StreamConfigurationMapping) {
	freeCString(scf.tags)
}
