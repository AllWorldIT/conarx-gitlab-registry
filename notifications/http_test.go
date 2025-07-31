package notifications

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/distribution/manifest/schema1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTPSink mocks out an http endpoint and notifies it under a couple of
// conditions, ensuring correct behavior.
func TestHTTPSink(t *testing.T) {
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		// NOTE(prozlach): we can't use require (which internally uses
		// `FailNow` from testing package) in a goroutine as we may get an
		// undefined behavior

		if !assert.Equal(t, http.MethodPost, r.Method, "unexpected request method") {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// Extract the content type and make sure it matches
		contentType := r.Header.Get("Content-Type")
		mediaType, _, err := mime.ParseMediaType(contentType)
		if !assert.NoError(t, err, "error parsing media type: contenttype=%q", contentType) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if !assert.Equal(t, EventsMediaType, mediaType, "incorrect media type") {
			w.WriteHeader(http.StatusUnsupportedMediaType)
			return
		}

		var envelope Envelope
		dec := json.NewDecoder(r.Body)

		if !assert.NoError(t, dec.Decode(&envelope), "error decoding request body") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Let caller choose the status
		status, err := strconv.Atoi(r.FormValue("status"))
		if err != nil {
			t.Logf("error parsing status: %v", err)

			// May just be empty, set status to 200
			status = http.StatusOK
		}

		w.WriteHeader(status)
	})
	server := httptest.NewTLSServer(serverHandler)

	metrics := newSafeMetrics(t.Name())
	sink := newHTTPSink(server.URL, 0, nil, nil,
		&endpointMetricsHTTPStatusListener{safeMetrics: metrics})

	// first make sure that the default transport gives x509 untrusted cert error
	event := &Event{}
	err := sink.Write(event)
	require.Regexp(t, "x509|unknown ca", err.Error())
	require.NoError(t, sink.Close())

	// make sure that passing in the transport no longer gives this error
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	sink = newHTTPSink(server.URL, 0, nil, tr,
		&endpointMetricsHTTPStatusListener{safeMetrics: metrics})
	err = sink.Write(event)
	require.NoError(t, err)

	server.Close()

	// reset server to standard http server and sink to a basic sink
	server = httptest.NewServer(serverHandler)
	defer server.Close()

	// reset metrics for following tests
	metrics = newSafeMetrics(t.Name())
	sink = newHTTPSink(server.URL, 0, nil, nil,
		&endpointMetricsHTTPStatusListener{safeMetrics: metrics})
	var expectedMetrics EndpointMetrics
	expectedMetrics.Endpoint = t.Name()
	expectedMetrics.Statuses = make(map[string]int64)

	closeL, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer closeL.Close()
	go func() {
		for {
			c, err := closeL.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	for _, tc := range []struct {
		name       string
		events     []Event // events to send
		url        string
		failure    bool // true if there should be a failure.
		statusCode int  // if not set, no status code should be incremented.
	}{
		{
			name:       "single_200",
			statusCode: http.StatusOK,
			events: []Event{
				createTestEvent("push", schema1.MediaTypeSignedManifest),
			},
		},
		{
			name:       "multiple_200",
			statusCode: http.StatusOK,
			events: []Event{
				createTestEvent("push", schema1.MediaTypeSignedManifest),
				createTestEvent("push", layerMediaType),
				createTestEvent("push", layerMediaType),
			},
		},
		{
			name:       "redirect_307",
			statusCode: http.StatusTemporaryRedirect,
			events: []Event{
				createTestEvent("push", schema1.MediaTypeSignedManifest),
			},
		},
		{
			name:       "bad_request_400",
			statusCode: http.StatusBadRequest,
			events: []Event{
				createTestEvent("push", schema1.MediaTypeSignedManifest),
			},
			failure: true,
		},
		{
			name: "connection_closed",
			// Case where connection is immediately closed
			url:     closeL.Addr().String(),
			failure: true,
		},
	} {
		if tc.failure {
			expectedMetrics.Failures += int64(len(tc.events))
		} else {
			expectedMetrics.Successes += int64(len(tc.events))
		}

		if tc.statusCode > 0 {
			expectedMetrics.Statuses[fmt.Sprintf("%d %s", tc.statusCode, http.StatusText(tc.statusCode))] += int64(len(tc.events))
		}

		url := tc.url
		if url == "" {
			url = server.URL + "/"
		}
		// setup endpoint to respond with expected status code.
		url += fmt.Sprintf("?status=%v", tc.statusCode)
		sink.url = url

		t.Logf("testcase: %v, fail=%v", tc.name, tc.failure)
		// Try a simple event emission.
		for _, ev := range tc.events {
			err := sink.Write(&ev)
			if tc.failure {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		}

		require.Equal(t, expectedMetrics.Endpoint, metrics.endpoint)
		require.Equal(t, expectedMetrics.Pending, metrics.pending.Load())
		require.Equal(t, expectedMetrics.Events, metrics.events.Load())
		require.Equal(t, expectedMetrics.Successes, metrics.successes.Load())
		require.Equal(t, expectedMetrics.Failures, metrics.failures.Load())
		require.Equal(t, expectedMetrics.Errors, metrics.errors.Load())
		require.Equal(t, expectedMetrics.Statuses, syncMapToPlainMap(metrics.statuses))
	}

	require.NoError(t, sink.Close())

	// double close returns error
	require.Error(t, sink.Close())
}

func syncMapToPlainMap(sm *sync.Map) map[string]int64 {
	result := make(map[string]int64)
	sm.Range(func(key, value any) bool {
		k := key.(string)
		v := value.(*atomic.Int64)
		result[k] = v.Load()
		return true
	})
	return result
}

func createTestEvent(action, mt string) Event {
	event := createEvent(action)

	event.Target.MediaType = mt
	event.Target.Repository = "library/test"

	return *event
}

func TestHTTPSink_Errors(t *testing.T) {
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		// set a long sleep bigger than the sink's timeout to force an error in the httpSink.Write method
		time.Sleep(time.Second)
		w.WriteHeader(http.StatusOK)
	})

	server := httptest.NewServer(serverHandler)
	defer server.Close()

	metrics := newSafeMetrics(t.Name())

	// make sure that passing in the transport no longer gives this error
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	sink := newHTTPSink(server.URL, 10*time.Millisecond, nil, tr,
		&endpointMetricsHTTPStatusListener{safeMetrics: metrics})
	defer sink.Close()

	events := []Event{
		createTestEvent("push", schema1.MediaTypeSignedManifest),
		createTestEvent("push", layerMediaType),
		createTestEvent("push", layerMediaType),
	}

	// all events should time out
	var expectedMetrics EndpointMetrics
	expectedMetrics.Endpoint = t.Name()
	expectedMetrics.Statuses = make(map[string]int64)
	expectedMetrics.Errors += int64(len(events))

	for _, event := range events {
		err := sink.Write(&event)
		// either client timeout or context deadline exceeded, asserting this error can be flaky
		require.Error(t, err)
	}

	require.Equal(t, expectedMetrics.Endpoint, metrics.endpoint)
	require.Equal(t, expectedMetrics.Pending, metrics.pending.Load())
	require.Equal(t, expectedMetrics.Events, metrics.events.Load())
	require.Equal(t, expectedMetrics.Successes, metrics.successes.Load())
	require.Equal(t, expectedMetrics.Failures, metrics.failures.Load())
	require.Equal(t, expectedMetrics.Errors, metrics.errors.Load())
	require.Equal(t, expectedMetrics.Statuses, syncMapToPlainMap(metrics.statuses))
}
