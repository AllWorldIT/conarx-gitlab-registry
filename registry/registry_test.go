package registry

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/registry/internal/testutil"
	_ "github.com/docker/distribution/registry/storage/driver/inmemory"
	"github.com/prometheus/client_golang/prometheus"
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
	if !reflect.DeepEqual(protos, []string{"h2", "http/1.1"}) {
		t.Fatalf("expected protos to equal [h2 http/1.1], got %s", protos)
	}
	config.HTTP.HTTP2.Disabled = true
	protos = nextProtos(config.HTTP.HTTP2.Disabled)
	if !reflect.DeepEqual(protos, []string{"http/1.1"}) {
		t.Fatalf("expected protos to equal [http/1.1], got %s", protos)
	}
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
	config.Storage = map[string]configuration.Parameters{"inmemory": map[string]interface{}{}}
	return NewRegistry(context.Background(), config)
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
		if err != nil {
			t.Fatal(err)
		}

		registry.config.HTTP.DrainTimeout = tt.httpDrainTimeout

		// Register on shutdown fuction to detect if server.Shutdown() was ran.
		var cleanServerShutdown bool
		registry.server.RegisterOnShutdown(func() {
			cleanServerShutdown = true
		})

		// run registry server
		var errchan chan error
		go func() {
			errchan <- registry.ListenAndServe()
		}()
		select {
		case err = <-errchan:
			t.Fatalf("Error listening: %v", err)
		default:
		}

		// Wait for some unknown random time for server to start listening
		time.Sleep(3 * time.Second)

		// Send quit signal, this does not track to the signals that the registry
		// is actually configured to listen to since we're interacting with the
		// channel directly — any signal sent on this channel triggers the shutdown.
		quit <- syscall.SIGTERM
		time.Sleep(100 * time.Millisecond)

		if cleanServerShutdown != tt.cleanServerShutdown {
			t.Fatalf("expected clean shutdown to be %v, got %v", tt.cleanServerShutdown, cleanServerShutdown)
		}
	}
}

func TestGracefulShutdown_HTTPDrainTimeout(t *testing.T) {
	registry, err := setupRegistry()
	if err != nil {
		t.Fatal(err)
	}

	// run registry server
	var errchan chan error
	go func() {
		errchan <- registry.ListenAndServe()
	}()
	select {
	case err = <-errchan:
		t.Fatalf("Error listening: %v", err)
	default:
	}

	// Wait for some unknown random time for server to start listening
	time.Sleep(3 * time.Second)

	// send incomplete request
	conn, err := net.Dial("tcp", registry.config.HTTP.Addr)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(conn, "GET /v2/ ")

	// send stop signal
	quit <- os.Interrupt
	time.Sleep(100 * time.Millisecond)

	// try connecting again. it shouldn't
	_, err = net.Dial("tcp", registry.config.HTTP.Addr)
	if err == nil {
		t.Fatal("Managed to connect after stopping.")
	}

	// make sure earlier request is not disconnected and response can be received
	fmt.Fprintf(conn, "HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Status != "200 OK" {
		t.Error("response status is not 200 OK: ", resp.Status)
	}
	if body, err := io.ReadAll(resp.Body); err != nil || string(body) != "{}" {
		t.Error("Body is not {}; ", string(body))
	}
}

func requireEnvNotSet(t *testing.T, names ...string) {
	t.Helper()

	for _, name := range names {
		_, ok := os.LookupEnv(name)
		require.False(t, ok)
	}
}

//nolint:unparam //(`name` always receives `"GITLAB_CONTINUOUS_PROFILING"`)
func requireEnvSet(t *testing.T, name, value string) {
	t.Helper()

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
	t.Helper()

	ln, err := net.Listen("tcp", ":")
	require.NoError(t, err)
	addr := ln.Addr()
	require.NoError(t, ln.Close())

	return addr
}

func assertMonitoringResponse(t *testing.T, scheme, addr, path string, expectedStatus int) {
	t.Helper()

	u := url.URL{Scheme: scheme, Host: addr, Path: path}

	c := &http.Client{Timeout: 100 * time.Millisecond, Transport: http.DefaultTransport.(*http.Transport).Clone()}
	if scheme == "https" {
		// disable checking TLS certificate for testutil cert
		c.Transport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	req, err := c.Get(u.String())
	require.NoError(t, err)
	defer req.Body.Close()
	require.Equal(t, expectedStatus, req.StatusCode, path)
}

func TestConfigureMonitoring(t *testing.T) {
	tcs := map[string]struct {
		config            func() *configuration.Configuration
		monitorConfigFunc func(config *configuration.Configuration) func()
		assertionPaths    map[string]int
	}{
		"health_handler": {
			config: func() *configuration.Configuration {
				addr := freeLnAddr(t).String()
				config := &configuration.Configuration{}
				config.HTTP.Debug.Addr = addr
				return config
			},
			monitorConfigFunc: func(config *configuration.Configuration) func() {
				return func() {
					opts, err := configureMonitoring(context.Background(), config)
					require.NoError(t, err)
					err = monitoring.Start(opts...)
					require.NoError(t, err)
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
			monitorConfigFunc: func(config *configuration.Configuration) func() {
				return func() {
					opts, err := configureMonitoring(context.Background(), config)
					require.NoError(t, err)
					// Use local Prometheus registry for each test, otherwise different tests may attempt to register the same
					// metrics in the default Prometheus registry, causing a panic.
					opts = append(opts, monitoring.WithPrometheusRegisterer(prometheus.NewRegistry()))
					err = monitoring.Start(opts...)
					require.NoError(t, err)
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
			monitorConfigFunc: func(config *configuration.Configuration) func() {
				return func() {
					opts, err := configureMonitoring(context.Background(), config)
					require.NoError(t, err)
					// Use local Prometheus registry for each test, otherwise different tests may attempt to register the same
					// metrics in the default Prometheus registry, causing a panic.
					opts = append(opts, monitoring.WithPrometheusRegisterer(prometheus.NewRegistry()))
					err = monitoring.Start(opts...)
					require.NoError(t, err)
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
			t.Run(fmt.Sprintf("%s_%s", tn, scheme), func(t *testing.T) {
				config := tc.config()
				if scheme == "https" {
					config.HTTP.Debug.TLS = configuration.DebugTLS{
						Enabled:     true,
						Certificate: testutil.TLSCertFilename(t),
						Key:         testutil.TLSKeytFilename(t),
					}
				}

				go tc.monitorConfigFunc(config)()

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
		redirect      map[string]interface{}
		expectedError error
	}{
		{
			name:     "no redirect section",
			redirect: nil,
		},
		{
			name:     "no parameters",
			redirect: map[string]interface{}{},
		},
		{
			name: "no disable parameter",
			redirect: map[string]interface{}{
				"expirydelay": 2 * time.Minute,
			},
		},
		{
			name: "bool disable parameter",
			redirect: map[string]interface{}{
				"disable": true,
			},
		},
		{
			name: "invalid disable parameter",
			redirect: map[string]interface{}{
				"disable": "true",
			},
			expectedError: errors.New("1 error occurred:\n\t* invalid type string for 'storage.redirect.disable' (boolean)\n\n"),
		},
		{
			name: "no expiry delay parameter",
			redirect: map[string]interface{}{
				"disable": true,
			},
		},
		{
			name: "duration expiry delay parameter",
			redirect: map[string]interface{}{
				"expirydelay": 2 * time.Minute,
			},
		},
		{
			name: "string expiry delay parameter",
			redirect: map[string]interface{}{
				"expirydelay": "2ms",
			},
		},
		{
			name: "invalid expiry delay parameter",
			redirect: map[string]interface{}{
				"expirydelay": 1,
			},
			expectedError: errors.New("1 error occurred:\n\t* invalid type int for 'storage.redirect.expirydelay' (duration)\n\n"),
		},
		{
			name: "invalid string expiry delay parameter",
			redirect: map[string]interface{}{
				"expirydelay": "2mm",
			},
			expectedError: errors.New("1 error occurred:\n\t* \"2mm\" value for 'storage.redirect.expirydelay' is not a valid duration\n\n"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &configuration.Configuration{
				Storage: map[string]configuration.Parameters{},
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
