package notifications

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/distribution/manifest/schema1"
	"github.com/stretchr/testify/require"
)

// TestHTTPSink mocks out an http endpoint and notifies it under a couple of
// conditions, ensuring correct behavior.
func TestHTTPSink(t *testing.T) {
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			t.Fatalf("unexpected request method: %v", r.Method)
			return
		}

		// Extract the content type and make sure it matches
		contentType := r.Header.Get("Content-Type")
		mediaType, _, err := mime.ParseMediaType(contentType)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			t.Fatalf("error parsing media type: %v, contenttype=%q", err, contentType)
			return
		}

		if mediaType != EventsMediaType {
			w.WriteHeader(http.StatusUnsupportedMediaType)
			t.Fatalf("incorrect media type: %q != %q", mediaType, EventsMediaType)
			return
		}

		var envelope Envelope
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&envelope); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			t.Fatalf("error decoding request body: %v", err)
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
	if !strings.Contains(err.Error(), "x509") && !strings.Contains(err.Error(), "unknown ca") {
		t.Fatal("TLS server with default transport should give unknown CA error")
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing http sink: %v", err)
	}

	// make sure that passing in the transport no longer gives this error
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	sink = newHTTPSink(server.URL, 0, nil, tr,
		&endpointMetricsHTTPStatusListener{safeMetrics: metrics})
	err = sink.Write(event)
	if err != nil {
		t.Fatalf("unexpected error writing events: %v", err)
	}

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
	expectedMetrics.Statuses = make(map[string]int)

	closeL, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("unexpected error creating listener: %v", err)
	}
	defer closeL.Close()
	go func() {
		for {
			c, err := closeL.Accept()
			if err != nil {
				return
			}
			c.Close()
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
				createTestEvent("push", "library/test", schema1.MediaTypeSignedManifest),
			},
		},
		{
			name:       "multiple_200",
			statusCode: http.StatusOK,
			events: []Event{
				createTestEvent("push", "library/test", schema1.MediaTypeSignedManifest),
				createTestEvent("push", "library/test", layerMediaType),
				createTestEvent("push", "library/test", layerMediaType),
			},
		},
		{
			name:       "redirect_307",
			statusCode: http.StatusTemporaryRedirect,
			events: []Event{
				createTestEvent("push", "library/test", schema1.MediaTypeSignedManifest),
			},
		},
		{
			name:       "bad_request_400",
			statusCode: http.StatusBadRequest,
			events: []Event{
				createTestEvent("push", "library/test", schema1.MediaTypeSignedManifest),
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
			expectedMetrics.Failures += len(tc.events)
		} else {
			expectedMetrics.Successes += len(tc.events)
		}

		if tc.statusCode > 0 {
			expectedMetrics.Statuses[fmt.Sprintf("%d %s", tc.statusCode, http.StatusText(tc.statusCode))] += len(tc.events)
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
			if !tc.failure {
				if err != nil {
					t.Fatalf("unexpected error send event: %v", err)
				}
			} else {
				if err == nil {
					t.Fatalf("the endpoint should have rejected the request")
				}
			}
		}

		if !reflect.DeepEqual(metrics.EndpointMetrics, expectedMetrics) {
			t.Fatalf("metrics not as expected: %#v != got: %#v", metrics.EndpointMetrics, expectedMetrics)
		}
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing http sink: %v", err)
	}

	// double close returns error
	if err := sink.Close(); err == nil {
		t.Fatalf("second close should have returned error: %v", err)
	}
}

func createTestEvent(action, repo, typ string) Event {
	event := createEvent(action)

	event.Target.MediaType = typ
	event.Target.Repository = repo

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
		createTestEvent("push", "library/test", schema1.MediaTypeSignedManifest),
		createTestEvent("push", "library/test", layerMediaType),
		createTestEvent("push", "library/test", layerMediaType),
	}

	// all events should time out
	var expectedMetrics EndpointMetrics
	expectedMetrics.Endpoint = t.Name()
	expectedMetrics.Statuses = make(map[string]int)
	expectedMetrics.Errors += len(events)

	for _, event := range events {
		err := sink.Write(&event)
		// either client timeout or context deadline exceeded, asserting this error can be flaky
		require.Error(t, err)
	}

	require.Equal(t, metrics.EndpointMetrics, expectedMetrics)
}
