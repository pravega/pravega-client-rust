package pkg

/*
#include "pravega_client.h"
*/
import "C"

import (
	"fmt"
	"math"
	"time"
)

type CredentialsType int

const (
	Credentials_Basic          CredentialsType = 0
	Credentials_BasicWithToken CredentialsType = 1
	Credentials_Keycloak       CredentialsType = 2
	Credentials_Json           CredentialsType = 3
)

type RetryWithBackoff struct {
	Initial_delay       uint64
	Backoff_coefficient uint32
	Max_attempt         int32
	Max_delay           uint64
	Expiration_time     int64
}

func (r *RetryWithBackoff) toCtype() *C.BRetryWithBackoff {
	backoff := &C.BRetryWithBackoff{}
	backoff.initial_delay = usize(r.Initial_delay)
	backoff.backoff_coefficient = usize(r.Backoff_coefficient)
	backoff.max_attempt = usize(r.Max_attempt)
	backoff.max_delay = usize(r.Max_delay)
	backoff.expiration_time = usize(r.Expiration_time)
	return backoff

}

func NewRetryWithBackoff() *RetryWithBackoff {
	return &RetryWithBackoff{
		Initial_delay:       1,
		Backoff_coefficient: 10,
		Max_delay:           10000,
		Expiration_time:     -1,
		Max_attempt:         -1,
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
		Type:                    Credentials_Basic,
		DisableCertVerification: false,
	}
}

func (c *Credentials) toCtype() *C.BCredentials {
	bc := &C.BCredentials{}
	switch c.Type {
	case Credentials_Basic:
		bc.credential_type = C.Basic
	case Credentials_BasicWithToken:
		bc.credential_type = C.BasicWithToken
	case Credentials_Keycloak:
		bc.credential_type = C.Keycloak
	case Credentials_Json:
		bc.credential_type = C.KeycloakFromJsonString
	}
	bc.username = C.CString(c.Username)
	bc.password = C.CString(c.Password)
	bc.token = C.CString(c.Token)
	bc.path = C.CString(c.Path)
	bc.json = C.CString(c.Json)
	bc.disable_cert_verification = C.bool(c.DisableCertVerification)
	return bc
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

func (clientConfig *ClientConfig) toCtype() C.BClientConfig {
	bconfig := C.BClientConfig{}
	bconfig.max_connections_in_pool = usize(clientConfig.MaxConnectionsInPool)
	bconfig.max_controller_connections = usize(clientConfig.MaxControllerConnections)
	bconfig.controller_uri = C.CString(clientConfig.ControllerUri)
	bconfig.transaction_timeout_time = usize(clientConfig.TransactionTimeout)
	bconfig.is_tls_enabled = C.bool(clientConfig.TlsEnabled)
	bconfig.is_auth_enabled = C.bool(clientConfig.AuthEnabled)
	bconfig.trustcerts = C.CString(clientConfig.Trustcerts)
	bconfig.reader_wrapper_buffer_size = usize(clientConfig.ReaderWrapperBufferSize)
	bconfig.credentials = *clientConfig.Credentials.toCtype()
	bconfig.disable_cert_verification = C.bool(clientConfig.DisableCertVerification)
	bconfig.request_timeout = usize(clientConfig.RequestTimeout)
	return bconfig
}
