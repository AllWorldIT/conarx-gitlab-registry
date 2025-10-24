//go:build !cli_test

package registry

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"syscall"
	"testing"
	"time"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/health"
	"github.com/docker/distribution/registry/internal/testutil"
	_ "github.com/docker/distribution/registry/storage/driver/inmemory"
	rngtestutil "github.com/docker/distribution/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/labkit/monitoring"
)

// Tests to ensure nextProtos returns the correct protocols when:
// * config.HTTP.HTTP2.Disabled is not explicitly set => [h2 http/1.1]
// * config.HTTP.HTTP2.Disabled is explicitly set to false [h2 http/1.1]
// * config.HTTP.HTTP2.Disabled is explicitly set to true [http/1.1]
func TestNextProtos(t *testing.T) {
	config := &configuration.Configuration{}
	config.HTTP.HTTP2.Disabled = false
	protos := nextProtos(config.HTTP.HTTP2.Disabled)
	assert.ElementsMatch(t, []string{"h2", "http/1.1"}, protos)
	config.HTTP.HTTP2.Disabled = true
	protos = nextProtos(config.HTTP.HTTP2.Disabled)
	assert.ElementsMatch(t, []string{"http/1.1"}, protos)
}

func setupRegistry() (*Registry, error) {
	config := &configuration.Configuration{}
	configuration.ApplyDefaults(config)
	// probe free port where the server can listen
	ln, err := net.Listen("tcp", ":")
	if err != nil {
		return nil, err
	}
	defer ln.Close()
	config.HTTP.Addr = ln.Addr().String()
	config.HTTP.DrainTimeout = time.Duration(10) * time.Second
	config.Storage = map[string]configuration.Parameters{"inmemory": make(map[string]any)}
	return NewRegistry(context.Background(), config)
}

type registryTLSConfig struct {
	cipherSuites    []string
	certificatePath string
	privateKeyPath  string
	certificate     *tls.Certificate
}

func TestGracefulShutdown(t *testing.T) {
	tests := []struct {
		name                string
		cleanServerShutdown bool
		httpDrainTimeout    time.Duration
	}{
		{
			name:                "http draintimeout greater than 0 runs server.Shutdown",
			cleanServerShutdown: true,
			httpDrainTimeout:    10 * time.Second,
		},
		{
			name:                "http draintimeout 0 or less does not run server.Shutdown",
			cleanServerShutdown: false,
			httpDrainTimeout:    0 * time.Second,
		},
	}

	for _, tt := range tests {
		registry, err := setupRegistry()
		require.NoError(t, err)

		registry.config.HTTP.DrainTimeout = tt.httpDrainTimeout

		// Register on shutdown fuction to detect if server.Shutdown() was ran.
		var cleanServerShutdown bool
		registry.server.RegisterOnShutdown(func() {
			cleanServerShutdown = true
		})

		// run registry server
		var errChan chan error
		go func() {
			errChan <- registry.ListenAndServe()
		}()

		timer := time.NewTimer(3 * time.Second)
		// nolint: revive // defer
		defer timer.Stop()
		select {
		case err = <-errChan:
			require.NoError(t, err, "error listening")
		case <-timer.C:
			// Wait for some unknown random time for server to start listening
		}

		// Send quit signal, this does not track to the signals that the registry
		// is actually configured to listen to since we're interacting with the
		// channel directly — any signal sent on this channel triggers the shutdown.
		quit <- syscall.SIGTERM
		time.Sleep(100 * time.Millisecond)

		assert.Equal(t, tt.cleanServerShutdown, cleanServerShutdown)
	}
}

func TestGracefulShutdown_HTTPDrainTimeout(t *testing.T) {
	registry, err := setupRegistry()
	require.NoError(t, err)

	// run registry server
	var errChan chan error
	go func() {
		errChan <- registry.ListenAndServe()
	}()
	select {
	case err = <-errChan:
		require.NoError(t, err, "error listening")
	default:
	}

	// Wait for some unknown random time for server to start listening
	time.Sleep(3 * time.Second)

	// send incomplete request
	conn, err := net.Dial("tcp", registry.config.HTTP.Addr)
	require.NoError(t, err)
	_, err = fmt.Fprintf(conn, "GET /v2/ ")
	require.NoError(t, err)

	// send stop signal
	quit <- os.Interrupt
	time.Sleep(100 * time.Millisecond)

	// try connecting again. it shouldn't
	_, err = net.Dial("tcp", registry.config.HTTP.Addr)
	require.Error(t, err)

	// make sure earlier request is not disconnected and response can be received
	_, err = fmt.Fprintf(conn, "HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
	require.NoError(t, err)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, "200 OK", resp.Status)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "{}", string(body))
}

func requireEnvNotSet(t *testing.T, names ...string) {
	for _, name := range names {
		_, ok := os.LookupEnv(name)
		require.False(t, ok)
	}
}

// nolint:unparam //(`name` always receives `"GITLAB_CONTINUOUS_PROFILING"`)
func requireEnvSet(t *testing.T, name, value string) {
	require.Equal(t, value, os.Getenv(name))
}

func TestConfigureStackDriver_Disabled(t *testing.T) {
	config := &configuration.Configuration{}

	requireEnvNotSet(t, "GITLAB_CONTINUOUS_PROFILING")
	require.NoError(t, configureStackdriver(config))
	requireEnvNotSet(t, "GITLAB_CONTINUOUS_PROFILING")
}

func TestConfigureStackDriver_Enabled(t *testing.T) {
	config := &configuration.Configuration{
		Profiling: configuration.Profiling{
			Stackdriver: configuration.StackdriverProfiler{
				Enabled: true,
			},
		},
	}

	requireEnvNotSet(t, "GITLAB_CONTINUOUS_PROFILING")
	require.NoError(t, configureStackdriver(config))
	requireEnvSet(t, "GITLAB_CONTINUOUS_PROFILING", "stackdriver")
	require.NoError(t, os.Unsetenv("GITLAB_CONTINUOUS_PROFILING"))
}

func TestConfigureStackDriver_WithParams(t *testing.T) {
	config := &configuration.Configuration{
		Profiling: configuration.Profiling{
			Stackdriver: configuration.StackdriverProfiler{
				Enabled:        true,
				Service:        "registry",
				ServiceVersion: "2.9.1",
				ProjectID:      "internal",
			},
		},
	}

	requireEnvNotSet(t, "GITLAB_CONTINUOUS_PROFILING")
	require.NoError(t, configureStackdriver(config))
	defer os.Unsetenv("GITLAB_CONTINUOUS_PROFILING")

	requireEnvSet(t, "GITLAB_CONTINUOUS_PROFILING", "stackdriver?project_id=internal&service=registry&service_version=2.9.1")
}

func TestConfigureStackDriver_WithKeyFile(t *testing.T) {
	config := &configuration.Configuration{
		Profiling: configuration.Profiling{
			Stackdriver: configuration.StackdriverProfiler{
				Enabled: true,
				KeyFile: "/path/to/credentials.json",
			},
		},
	}

	requireEnvNotSet(t, "GITLAB_CONTINUOUS_PROFILING")
	require.NoError(t, configureStackdriver(config))
	defer os.Unsetenv("GITLAB_CONTINUOUS_PROFILING")

	requireEnvSet(t, "GITLAB_CONTINUOUS_PROFILING", "stackdriver")
}

func TestConfigureStackDriver_DoesNotOverrideGitlabContinuousProfilingEnvVar(t *testing.T) {
	value := "stackdriver?project_id=foo&service=bar&service_version=1"
	require.NoError(t, os.Setenv("GITLAB_CONTINUOUS_PROFILING", value))

	config := &configuration.Configuration{
		Profiling: configuration.Profiling{
			Stackdriver: configuration.StackdriverProfiler{
				Enabled:        true,
				Service:        "registry",
				ServiceVersion: "2.9.1",
				ProjectID:      "internal",
			},
		},
	}

	require.NoError(t, configureStackdriver(config))
	defer os.Unsetenv("GITLAB_CONTINUOUS_PROFILING")

	requireEnvSet(t, "GITLAB_CONTINUOUS_PROFILING", value)
}

func freeLnAddr(t *testing.T) net.Addr {
	ln, err := net.Listen("tcp", ":")
	require.NoError(t, err)
	addr := ln.Addr()
	require.NoError(t, ln.Close())

	return addr
}

func assertMonitoringResponse(t *testing.T, scheme, addr, targetPath string, expectedStatus int) {
	u := url.URL{Scheme: scheme, Host: addr, Path: targetPath}

	c := &http.Client{Timeout: 100 * time.Millisecond, Transport: http.DefaultTransport.(*http.Transport).Clone()}
	if scheme == "https" {
		// disable checking TLS certificate for testutil cert
		c.Transport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	req, err := c.Get(u.String())
	require.NoError(t, err)
	defer req.Body.Close()
	require.Equal(t, expectedStatus, req.StatusCode, targetPath)
}

func TestConfigureMonitoring(t *testing.T) {
	tcs := map[string]struct {
		config            func() *configuration.Configuration
		monitorConfigFunc func(ttt *testing.T, config *configuration.Configuration) func()
		assertionPaths    map[string]int
	}{
		"health_handler": {
			config: func() *configuration.Configuration {
				addr := freeLnAddr(t).String()
				config := &configuration.Configuration{}
				config.HTTP.Debug.Addr = addr
				return config
			},
			monitorConfigFunc: func(ttt *testing.T, config *configuration.Configuration) func() {
				opts, err := configureMonitoring(context.Background(), config, &health.DBStatusChecker{})
				require.NoError(ttt, err)
				return func() {
					err := monitoring.Start(opts...)
					assert.NoError(ttt, err)
				}
			},
			assertionPaths: map[string]int{
				"/debug/health": http.StatusOK,
				"/debug/pprof":  http.StatusNotFound,
				"/metrics":      http.StatusNotFound,
			},
		},
		"metrics_handler": {
			config: func() *configuration.Configuration {
				addr := freeLnAddr(t).String()
				config := &configuration.Configuration{}
				config.HTTP.Debug.Addr = addr
				config.HTTP.Debug.Prometheus.Enabled = true
				config.HTTP.Debug.Prometheus.Path = "/metrics"
				return config
			},
			monitorConfigFunc: func(ttt *testing.T, config *configuration.Configuration) func() {
				opts, err := configureMonitoring(context.Background(), config, &health.DBStatusChecker{})
				require.NoError(ttt, err)
				// Use local Prometheus registry for each test, otherwise different tests may attempt to register the same
				// metrics in the default Prometheus registry, causing a panic.
				opts = append(opts, monitoring.WithPrometheusRegisterer(prometheus.NewRegistry()))
				return func() {
					err := monitoring.Start(opts...)
					assert.NoError(t, err)
				}
			},
			assertionPaths: map[string]int{
				"/debug/health": http.StatusOK,
				"/debug/pprof":  http.StatusNotFound,
				"/metrics":      http.StatusOK,
			},
		},
		"all_handlers": {
			config: func() *configuration.Configuration {
				addr := freeLnAddr(t).String()
				config := &configuration.Configuration{}
				config.HTTP.Debug.Addr = addr
				config.HTTP.Debug.Pprof.Enabled = true
				config.HTTP.Debug.Prometheus.Enabled = true
				config.HTTP.Debug.Prometheus.Path = "/metrics"
				return config
			},
			monitorConfigFunc: func(ttt *testing.T, config *configuration.Configuration) func() {
				opts, err := configureMonitoring(context.Background(), config, &health.DBStatusChecker{})
				require.NoError(ttt, err)
				// Use local Prometheus registry for each test, otherwise different tests may attempt to register the same
				// metrics in the default Prometheus registry, causing a panic.
				opts = append(opts, monitoring.WithPrometheusRegisterer(prometheus.NewRegistry()))
				return func() {
					err := monitoring.Start(opts...)
					assert.NoError(ttt, err)
				}
			},
			assertionPaths: map[string]int{
				"/debug/health": http.StatusOK,
				"/debug/pprof":  http.StatusOK,
				"/metrics":      http.StatusOK,
			},
		},
	}

	for tn, tc := range tcs {
		for _, scheme := range []string{"http", "https"} {
			t.Run(fmt.Sprintf("%s_%s", tn, scheme), func(tt *testing.T) {
				config := tc.config()
				if scheme == "https" {
					config.HTTP.Debug.TLS = configuration.DebugTLS{
						Enabled:     true,
						Certificate: testutil.TLSCertFilename(tt),
						Key:         testutil.TLSKeytFilename(tt),
					}
				}

				go tc.monitorConfigFunc(tt, config)()

				for path, expectedStatus := range tc.assertionPaths {
					require.Eventually(t, func() bool {
						assertMonitoringResponse(t, scheme, config.HTTP.Debug.Addr, path, expectedStatus)
						return true
					}, 5*time.Second, 500*time.Millisecond)
				}
			})
		}
	}
}

func Test_validate_redirect(t *testing.T) {
	tests := []struct {
		name          string
		redirect      map[string]any
		expectedError error
	}{
		{
			name:     "no redirect section",
			redirect: nil,
		},
		{
			name:     "no parameters",
			redirect: make(map[string]any),
		},
		{
			name: "no disable parameter",
			redirect: map[string]any{
				"expirydelay": 2 * time.Minute,
			},
		},
		{
			name: "bool disable parameter",
			redirect: map[string]any{
				"disable": true,
			},
		},
		{
			name: "invalid disable parameter",
			redirect: map[string]any{
				"disable": "true",
			},
			// nolint: revive // error-strings
			expectedError: errors.New("1 error occurred:\n\t* invalid type string for 'storage.redirect.disable' (boolean)\n\n"),
		},
		{
			name: "no expiry delay parameter",
			redirect: map[string]any{
				"disable": true,
			},
		},
		{
			name: "duration expiry delay parameter",
			redirect: map[string]any{
				"expirydelay": 2 * time.Minute,
			},
		},
		{
			name: "string expiry delay parameter",
			redirect: map[string]any{
				"expirydelay": "2ms",
			},
		},
		{
			name: "invalid expiry delay parameter",
			redirect: map[string]any{
				"expirydelay": 1,
			},
			// nolint: revive // error-strings
			expectedError: errors.New("1 error occurred:\n\t* invalid type int for 'storage.redirect.expirydelay' (duration)\n\n"),
		},
		{
			name: "invalid string expiry delay parameter",
			redirect: map[string]any{
				"expirydelay": "2mm",
			},
			// nolint: revive // error-strings
			expectedError: errors.New("1 error occurred:\n\t* \"2mm\" value for 'storage.redirect.expirydelay' is not a valid duration\n\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &configuration.Configuration{
				Storage: make(map[string]configuration.Parameters),
			}

			if tt.redirect != nil {
				cfg.Storage["redirect"] = tt.redirect
			}

			if tt.expectedError != nil {
				require.EqualError(t, validate(cfg), tt.expectedError.Error())
			} else {
				require.NoError(t, validate(cfg))
			}
		})
	}
}

func TestGetCipherSuite(t *testing.T) {
	resp, err := getCipherSuites([]string{"TLS_RSA_WITH_AES_128_CBC_SHA"})
	require.NoError(t, err)
	require.ElementsMatch(t, []uint16{tls.TLS_RSA_WITH_AES_128_CBC_SHA}, resp)

	resp, err = getCipherSuites([]string{
		"TLS_RSA_WITH_AES_128_CBC_SHA",
		"TLS_RSA_WITH_AES_256_CBC_SHA",
	})
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]uint16{tls.TLS_RSA_WITH_AES_128_CBC_SHA, tls.TLS_RSA_WITH_AES_256_CBC_SHA},
		resp,
	)

	_, err = getCipherSuites([]string{"TLS_RSA_WITH_AES_128_CBC_SHA", "bad_input"})
	if err == nil {
		t.Error("did not return expected error about unknown cipher suite")
	}

	invalidCipherSuites := []string{
		"TLS_MARYNA_WITH_BORYNA7",
		"TLS_RSA_WITH_RC4_128_SHA",
		"TLS_RSA_WITH_AES_128_CBC_SHA256",
		"TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
		"TLS_ECDHE_RSA_WITH_RC4_128_SHA",
		"TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
		"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
	}

	for _, suite := range invalidCipherSuites {
		_, err = getCipherSuites([]string{suite})
		require.Error(t, err)
	}
}

func buildRegistryTLSConfig(t *testing.T, name string, cipherSuites []string) *registryTLSConfig {
	rng := mrand.NewChaCha8([32]byte(rngtestutil.MustChaChaSeed(t)))
	rsaKey, err := rsa.GenerateKey(rng, 2048)
	require.NoError(t, err, "failed to create rsa private key")
	pub := rsaKey.Public()

	notBefore := time.Now().Add(-10 * time.Second)
	notAfter := notBefore.Add(5 * time.Minute)
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rng, serialNumberLimit)
	require.NoError(t, err, "failed to create serial number")

	cert := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"registry_test"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
		IsCA:                  true,
	}
	derBytes, err := x509.CreateCertificate(rng, &cert, &cert, pub, rsaKey)
	require.NoError(t, err, "failed to create certificate")

	tmpDir := t.TempDir()

	certPath := path.Join(tmpDir, name+".pem")
	certOut, err := os.Create(certPath)
	require.NoError(t, err, "failed to create pem")
	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	require.NoError(t, err, "failed to write data")
	err = certOut.Close()
	require.NoError(t, err, "error closing")

	keyPath := path.Join(tmpDir, name+".key")
	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	require.NoErrorf(t, err, "failed to open %s for writing", keyPath)

	privBytes, err := x509.MarshalPKCS8PrivateKey(rsaKey)
	require.NoError(t, err, "unable to marshal private key")
	err = pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})
	require.NoError(t, err, "failed to write data to key.pem")
	err = keyOut.Close()
	require.NoErrorf(t, err, "error closing %s", keyPath)

	tlsCert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  rsaKey,
	}

	tlsTestCfg := registryTLSConfig{
		cipherSuites:    cipherSuites,
		certificatePath: certPath,
		privateKeyPath:  keyPath,
		certificate:     &tlsCert,
	}

	return &tlsTestCfg
}

func TestRegistrySupportedCipherSuite(t *testing.T) {
	name := t.Name()
	cipherSuites := []string{"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"}
	serverTLSconfig := buildRegistryTLSConfig(t, name, cipherSuites)

	registry, err := setupRegistry()
	require.NoError(t, err, "setting up registry")
	registry.config.HTTP.TLS.CipherSuites = serverTLSconfig.cipherSuites
	registry.config.HTTP.TLS.Certificate = serverTLSconfig.certificatePath
	registry.config.HTTP.TLS.Key = serverTLSconfig.privateKeyPath

	// run registry server
	var errChan chan error
	go func() {
		errChan <- registry.ListenAndServe()
	}()

	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case err = <-errChan:
		require.FailNow(t, fmt.Sprintf("error listening: %v", err))
	case <-timer.C:
		// Wait for some unknown random time for server to start listening
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSHandshakeTimeout: 2 * time.Second,
			TLSClientConfig: &tls.Config{
				MaxVersion: tls.VersionTLS12,
				CipherSuites: []uint16{
					tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				},
				InsecureSkipVerify: true,
			},
		},
	}

	resp, err := client.Get(fmt.Sprintf("https://%s/v2/", registry.config.HTTP.Addr))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "{}", string(body))

	// send stop signal
	quit <- os.Interrupt
	time.Sleep(100 * time.Millisecond)
}

func TestRegistryUnsupportedCipherSuite(t *testing.T) {
	name := t.Name()
	serverTLSconfig := buildRegistryTLSConfig(t, name, []string{"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"})

	registry, err := setupRegistry()
	require.NoError(t, err)
	registry.config.HTTP.TLS.CipherSuites = serverTLSconfig.cipherSuites
	registry.config.HTTP.TLS.Certificate = serverTLSconfig.certificatePath
	registry.config.HTTP.TLS.Key = serverTLSconfig.privateKeyPath

	// run registry server
	var errChan chan error
	go func() {
		errChan <- registry.ListenAndServe()
	}()

	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case err = <-errChan:
		require.FailNow(t, fmt.Sprintf("error listening: %v", err))
	case <-timer.C:
		// Wait for some unknown random time for server to start listening
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSHandshakeTimeout: 2 * time.Second,
			TLSClientConfig: &tls.Config{
				MaxVersion: tls.VersionTLS12,
				CipherSuites: []uint16{
					tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
				},
				InsecureSkipVerify: true,
			},
		},
	}

	resp, err := client.Get(fmt.Sprintf("https://%s/v2/", registry.config.HTTP.Addr))
	if err == nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	require.ErrorContains(t, err, "handshake failure")

	// send stop signal
	quit <- os.Interrupt
	time.Sleep(100 * time.Millisecond)
}

func TestRegistryTLS13(t *testing.T) {
	name := t.Name()
	serverTLSconfig := buildRegistryTLSConfig(t, name, []string{"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"})

	registry, err := setupRegistry()
	require.NoError(t, err)
	// NOTE(prozlach): This test makes sure that cipher suites in tls1.3 mode
	// are ignored, so we need to define them here:
	registry.config.HTTP.TLS.CipherSuites = serverTLSconfig.cipherSuites
	registry.config.HTTP.TLS.Certificate = serverTLSconfig.certificatePath
	registry.config.HTTP.TLS.Key = serverTLSconfig.privateKeyPath

	// run registry server
	var errChan chan error
	go func() {
		errChan <- registry.ListenAndServe()
	}()

	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case err = <-errChan:
		require.FailNow(t, fmt.Sprintf("error listening: %v", err))
	case <-timer.C:
		// Wait for some unknown random time for server to start listening
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSHandshakeTimeout: 2 * time.Second,
			TLSClientConfig: &tls.Config{
				MinVersion:         tls.VersionTLS13,
				InsecureSkipVerify: true,
			},
		},
	}

	resp, err := client.Get(fmt.Sprintf("https://%s/v2/", registry.config.HTTP.Addr))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "{}", string(body))

	// send stop signal
	quit <- os.Interrupt
	time.Sleep(100 * time.Millisecond)
}
