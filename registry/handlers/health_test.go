package handlers

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/health"
	dtestutil "github.com/docker/distribution/testutil"
	"github.com/docker/distribution/version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileHealthCheck(t *testing.T) {
	interval := time.Second

	tmpfile, err := os.CreateTemp(t.TempDir(), "healthcheck")
	require.NoError(t, err, "could not create temporary file")
	defer tmpfile.Close()

	config := &configuration.Configuration{
		Storage: configuration.Storage{
			"inmemory": configuration.Parameters{},
			"maintenance": configuration.Parameters{"uploadpurging": map[any]any{
				"enabled": false,
			}},
		},
		Health: configuration.Health{
			FileCheckers: []configuration.FileChecker{
				{
					Interval: interval,
					File:     tmpfile.Name(),
				},
			},
		},
	}

	ctx := dtestutil.NewContextWithLogger(t)

	app, err := NewApp(ctx, config)
	require.NoError(t, err)
	t.Cleanup(
		func() {
			err := app.GracefulShutdown(ctx)
			require.NoError(t, err)
		},
	)

	healthRegistry := health.NewRegistry()
	err = app.RegisterHealthChecks(healthRegistry)
	require.NoError(t, err)

	// Wait for health check to happen
	<-time.After(2 * interval)

	status := healthRegistry.CheckStatus()
	require.Len(t, status, 1, "expected 1 item in health check results")
	assert.Equal(t, "file exists", status[tmpfile.Name()], `did not get "file exists" result for health check`)

	err = os.Remove(tmpfile.Name())
	require.NoError(t, err)

	<-time.After(2 * interval)
	assert.Empty(t, healthRegistry.CheckStatus(), "expected 0 items in health check results")
}

func TestTCPHealthCheck(t *testing.T) {
	interval := time.Second

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "could not create listener")
	addrStr := ln.Addr().String()

	// Start accepting
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				// listener was closed
				return
			}
			defer conn.Close()
		}
	}()

	config := &configuration.Configuration{
		Storage: configuration.Storage{
			"inmemory": configuration.Parameters{},
			"maintenance": configuration.Parameters{"uploadpurging": map[any]any{
				"enabled": false,
			}},
		},
		Health: configuration.Health{
			TCPCheckers: []configuration.TCPChecker{
				{
					Interval: interval,
					Addr:     addrStr,
					Timeout:  500 * time.Millisecond,
				},
			},
		},
	}

	ctx := dtestutil.NewContextWithLogger(t)

	app, err := NewApp(ctx, config)
	require.NoError(t, err)
	t.Cleanup(
		func() {
			err := app.GracefulShutdown(ctx)
			require.NoError(t, err)
		},
	)

	healthRegistry := health.NewRegistry()
	err = app.RegisterHealthChecks(healthRegistry)
	require.NoError(t, err)

	// Wait for health check to happen
	<-time.After(2 * interval)

	require.Empty(t, healthRegistry.CheckStatus(), "expected 0 items in health check results")

	err = ln.Close()
	require.NoError(t, err)
	<-time.After(2 * interval)

	// Health check should now fail
	status := healthRegistry.CheckStatus()
	require.Len(t, status, 1, "expected 1 item in health check results")
	assert.Equal(t, "connection to "+addrStr+" failed", status[addrStr], `did not get "connection failed" result for health check`)
}

func TestHTTPHealthCheck(t *testing.T) {
	testcases := []struct {
		name            string
		headersConfig   http.Header
		expectedHeaders http.Header
	}{
		{
			name:          "default user agent",
			headersConfig: http.Header{},
			expectedHeaders: http.Header{
				"User-Agent": []string{
					fmt.Sprintf("container-registry-httpcheck/%s-%s", version.Version, version.Revision),
				},
			},
		},
		{
			name: "custom user agent",
			headersConfig: http.Header{
				"User-Agent": []string{"marynian sodowy/1.1"},
			},
			expectedHeaders: http.Header{
				"User-Agent": []string{"marynian sodowy/1.1"},
			},
		},
		{
			name: "custom header set",
			headersConfig: http.Header{
				"boryna": []string{"maryna"},
			},
			expectedHeaders: http.Header{
				"Boryna": []string{"maryna"},
				"User-Agent": []string{
					fmt.Sprintf("container-registry-httpcheck/%s-%s", version.Version, version.Revision),
				},
			},
		},
		// Below results in:
		// Host: 127.0.0.1:37117
		// User-Agent: container-registry-httpcheck/unknown-
		// Boryna: maryna1
		// Boryna: maryna2
		// Boryna: maryna3
		{
			name: "repetitive custom headers set",
			headersConfig: http.Header{
				"boryna": []string{"maryna1", "maryna2", "maryna3"},
			},
			expectedHeaders: http.Header{
				"Boryna": []string{"maryna1", "maryna2", "maryna3"},
				"User-Agent": []string{
					fmt.Sprintf("container-registry-httpcheck/%s-%s", version.Version, version.Revision),
				},
			},
		},
	}

	interval := time.Second
	threshold := 3

	for _, test := range testcases {
		t.Run(test.name, func(t *testing.T) {
			stopFailing := make(chan struct{})

			checkedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodHead, r.Method, "expected HEAD request")
				// NOTE(prozlach): we can't use require (which internally uses
				// `FailNow` from testing package) in a goroutine as we may get
				// an undefined behavior
				assert.Equal(t, test.expectedHeaders, r.Header)
				select {
				case <-stopFailing:
					w.WriteHeader(http.StatusOK)
				default:
					w.WriteHeader(http.StatusInternalServerError)
				}
			}))
			t.Cleanup(checkedServer.Close)

			config := &configuration.Configuration{
				Storage: configuration.Storage{
					"inmemory": configuration.Parameters{},
					"maintenance": configuration.Parameters{"uploadpurging": map[any]any{
						"enabled": false,
					}},
				},
				Health: configuration.Health{
					HTTPCheckers: []configuration.HTTPChecker{
						{
							Interval:  interval,
							URI:       checkedServer.URL,
							Threshold: threshold,
							Headers:   test.headersConfig,
						},
					},
				},
			}

			ctx := dtestutil.NewContextWithLogger(t)

			app, err := NewApp(ctx, config)
			require.NoError(t, err)
			t.Cleanup(
				func() {
					err := app.GracefulShutdown(ctx)
					require.NoError(t, err)
				},
			)

			healthRegistry := health.NewRegistry()
			app.RegisterHealthChecks(healthRegistry)

			for i := 0; ; i++ {
				<-time.After(interval)

				status := healthRegistry.CheckStatus()

				if i < threshold-1 {
					// definitely shouldn't have hit the threshold yet
					assert.Empty(t, status, "expected 1 item in health check results")
					continue
				}
				if i < threshold+1 {
					// right on the threshold - don't expect a failure yet
					continue
				}

				require.Len(t, status, 1, "expected 1 item in health check results")
				require.Equal(t, "downstream service returned unexpected status: 500", status[checkedServer.URL], "did not get expected result for health check")

				break
			}

			// Signal HTTP handler to start returning 200
			close(stopFailing)

			<-time.After(2 * interval)

			assert.Empty(t, healthRegistry.CheckStatus(), "expected 0 items in health check results")
		})
	}
}
