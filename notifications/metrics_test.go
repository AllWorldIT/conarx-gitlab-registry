package notifications

import (
	"bytes"
	"encoding/json"
	"expvar"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsExpvar(t *testing.T) {
	endpointsVar := expvar.Get("registry").(*expvar.Map).Get("notifications").(*expvar.Map).Get("endpoints")

	var v any
	err := json.Unmarshal([]byte(endpointsVar.String()), &v)
	require.NoError(t, err, "unexpected error unmarshaling endpoints")

	require.Nil(t, v, "expected nil")

	NewEndpoint("x", "y", EndpointConfig{})

	err = json.Unmarshal([]byte(endpointsVar.String()), &v)
	require.NoError(t, err, "unexpected error unmarshaling endpoints")

	if slice, ok := v.([]any); !ok || len(slice) != 1 {
		t.Logf("expected one-element []interface{}, got %#v", v)
	}
}

func TestNotificationsMetrics(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	// Test event ingress
	event := &Event{
		ID:     "test-id",
		Action: "push",
		Target: Target{
			Repository: "test/repo",
			Descriptor: distribution.Descriptor{
				MediaType: "application/vnd.docker.distribution.manifest.v2+json",
				Digest:    "sha256:1234567890",
			},
		},
	}

	// Simulate events counter
	eventsCounter.WithLabelValues("Events", event.Action, event.artifact(), "webhook-endpoint").Inc()
	eventsCounter.WithLabelValues("Events", "pull", "manifest", "webhook-endpoint").Inc()
	eventsCounter.WithLabelValues("Events", "push", "blob", "webhook-endpoint").Inc()

	// Simulate pending gauge
	pendingGauge.Inc()
	pendingGauge.Inc()
	pendingGauge.Dec()

	// Simulate status counter
	statusCounter.WithLabelValues("200 OK").Inc()
	statusCounter.WithLabelValues("200 OK").Inc()
	statusCounter.WithLabelValues("404 Not Found").Inc()
	statusCounter.WithLabelValues("500 Internal Server Error").Inc()

	// Simulate error counter
	errorCounter.WithLabelValues("webhook-endpoint").Inc()
	errorCounter.WithLabelValues("backup-endpoint").Inc()
	errorCounter.WithLabelValues("backup-endpoint").Inc()

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_notifications_errors The number of events that were not sent due to internal errors
# TYPE registry_notifications_errors counter
registry_notifications_errors{endpoint="backup-endpoint"} 2
registry_notifications_errors{endpoint="webhook-endpoint"} 1
# HELP registry_notifications_events The number of total events
# TYPE registry_notifications_events counter
registry_notifications_events{action="pull",artifact="manifest",endpoint="webhook-endpoint",type="Events"} 1
registry_notifications_events{action="push",artifact="blob",endpoint="webhook-endpoint",type="Events"} 1
registry_notifications_events{action="push",artifact="manifest",endpoint="webhook-endpoint",type="Events"} 1
# HELP registry_notifications_pending The gauge of pending events in queue
# TYPE registry_notifications_pending gauge
registry_notifications_pending 1
# HELP registry_notifications_status The number of status code
# TYPE registry_notifications_status counter
registry_notifications_status{code="200 OK"} 2
registry_notifications_status{code="404 Not Found"} 1
registry_notifications_status{code="500 Internal Server Error"} 1
`)
	require.NoError(t, err)

	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, eventsCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, pendingGaugeName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, statusCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, errorCounterName),
	}

	err = testutil.GatherAndCompare(registry, &expected, names...)
	require.NoError(t, err)
}

func TestEndpointMetricsHTTPStatusListener(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("test-endpoint")
	listener := sm.httpStatusListener()

	event := &Event{
		ID:     "test-id",
		Action: "push",
		Target: Target{
			Repository: "test/repo",
			Descriptor: distribution.Descriptor{
				MediaType: "application/vnd.docker.distribution.manifest.v2+json",
				Digest:    "sha256:1234567890",
			},
		},
	}

	// Test success
	listener.success(200, event)
	listener.success(201, event)

	// Test failure
	listener.failure(404, event)
	listener.failure(500, event)

	// Test error
	listener.err(event)
	listener.err(event)

	// Verify safeMetrics internal state
	require.EqualValues(t, 2, sm.successes.Load())
	require.EqualValues(t, 2, sm.failures.Load())
	require.EqualValues(t, 2, sm.errors.Load())

	v, ok := sm.statuses.Load("200 OK")
	require.True(t, ok)
	require.Equal(t, int64(1), v.(*atomic.Int64).Load())

	v, ok = sm.statuses.Load("201 Created")
	require.True(t, ok)
	require.Equal(t, int64(1), v.(*atomic.Int64).Load())

	v, ok = sm.statuses.Load("404 Not Found")
	require.True(t, ok)
	require.Equal(t, int64(1), v.(*atomic.Int64).Load())

	v, ok = sm.statuses.Load("500 Internal Server Error")
	require.True(t, ok)
	require.Equal(t, int64(1), v.(*atomic.Int64).Load())

	// Verify Prometheus metrics
	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_notifications_errors The number of events that were not sent due to internal errors
# TYPE registry_notifications_errors counter
registry_notifications_errors{endpoint="test-endpoint"} 2
# HELP registry_notifications_events The number of total events
# TYPE registry_notifications_events counter
registry_notifications_events{action="push",artifact="manifest",endpoint="test-endpoint",type="Failures"} 2
registry_notifications_events{action="push",artifact="manifest",endpoint="test-endpoint",type="Successes"} 2
# HELP registry_notifications_status The number of status code
# TYPE registry_notifications_status counter
registry_notifications_status{code="200 OK"} 1
registry_notifications_status{code="201 Created"} 1
registry_notifications_status{code="404 Not Found"} 1
registry_notifications_status{code="500 Internal Server Error"} 1
`)
	require.NoError(t, err)

	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, eventsCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, statusCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, errorCounterName),
	}

	err = testutil.GatherAndCompare(registry, &expected, names...)
	require.NoError(t, err)
}

func TestEndpointMetricsEventQueueListener(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("queue-endpoint")
	listener := sm.eventQueueListener()

	event1 := &Event{
		ID:     "test-id-1",
		Action: "push",
		Target: Target{
			Repository: "test/repo",
			Descriptor: distribution.Descriptor{
				MediaType: "application/vnd.docker.distribution.manifest.v2+json",
				Digest:    "sha256:1234567890",
			},
		},
	}

	event2 := &Event{
		ID:     "test-id-2",
		Action: "pull",
		Target: Target{
			Repository: "test/repo",
			Descriptor: distribution.Descriptor{
				MediaType: "application/octet-stream",
				Digest:    "sha256:1234567890",
			},
		},
	}

	// Test ingress
	listener.ingress(event1)
	listener.ingress(event2)
	listener.ingress(event1)

	// Verify internal state after ingress
	require.EqualValues(t, 3, sm.events.Load())
	require.EqualValues(t, 3, sm.pending.Load())

	// Test egress
	listener.egress(event1)
	listener.egress(event2)

	// Verify internal state after egress
	require.EqualValues(t, 3, sm.events.Load())
	require.EqualValues(t, 1, sm.pending.Load())

	// Verify Prometheus metrics
	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_notifications_events The number of total events
# TYPE registry_notifications_events counter
registry_notifications_events{action="pull",artifact="blob",endpoint="queue-endpoint",type="Events"} 1
registry_notifications_events{action="push",artifact="manifest",endpoint="queue-endpoint",type="Events"} 2
# HELP registry_notifications_pending The gauge of pending events in queue
# TYPE registry_notifications_pending gauge
registry_notifications_pending 1
`)
	require.NoError(t, err)

	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, eventsCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, pendingGaugeName),
	}

	err = testutil.GatherAndCompare(registry, &expected, names...)
	require.NoError(t, err)
}

func TestConcurrentMetricsUpdates(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("concurrent-endpoint")
	httpListener := sm.httpStatusListener()
	queueListener := sm.eventQueueListener()

	event := &Event{
		ID:     "test-id",
		Action: "push",
		Target: Target{
			Repository: "test/repo",
			Descriptor: distribution.Descriptor{
				MediaType: "application/vnd.docker.distribution.manifest.v2+json",
				Digest:    "sha256:1234567890",
			},
		},
	}

	// Launch multiple goroutines to update metrics concurrently
	var wg sync.WaitGroup
	numGoroutines := 10
	operationsPerGoroutine := 100

	// HTTP status updates
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			base := (i * operationsPerGoroutine)
			for j := 0; j < operationsPerGoroutine; j++ {
				switch (base + j) % 3 {
				case 0:
					httpListener.success(200, event)
				case 1:
					httpListener.failure(404, event)
				case 2:
					httpListener.err(event)
				}
			}
		}(i)
	}

	// Queue updates
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			base := i * operationsPerGoroutine
			for j := 0; j < operationsPerGoroutine; j++ {
				queueListener.ingress(event)
				if (base+j)%2 == 0 {
					queueListener.egress(event)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify internal state
	totalOperations := numGoroutines * operationsPerGoroutine
	assert.EqualValues(t, totalOperations, sm.events.Load())
	assert.EqualValues(t, totalOperations/2, sm.pending.Load()) // Half were egressed

	// Calculate expected counts based on switch statement distribution
	expectedSuccesses := (totalOperations / 3)
	if totalOperations%3 > 0 {
		expectedSuccesses++
	}
	expectedFailures := (totalOperations / 3)
	if totalOperations%3 > 1 {
		expectedFailures++
	}
	expectedErrors := totalOperations / 3

	assert.EqualValues(t, expectedSuccesses, sm.successes.Load())
	assert.EqualValues(t, expectedFailures, sm.failures.Load())
	assert.EqualValues(t, expectedErrors, sm.errors.Load())
}

func TestSafeMetricsInitialization(t *testing.T) {
	sm := newSafeMetrics("init-test")

	// Verify initial state
	require.Equal(t, "init-test", sm.endpoint)
	require.Zero(t, sm.pending.Load())
	require.Zero(t, sm.events.Load())
	require.Zero(t, sm.successes.Load())
	require.Zero(t, sm.failures.Load())
	require.Zero(t, sm.errors.Load())
	require.NotNil(t, sm.statuses)
	require.Empty(t, syncMapToPlainMap(sm.statuses))
}

func TestHTTPStatusCodes(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("status-test")
	listener := sm.httpStatusListener()

	event := &Event{
		ID:     "test-id",
		Action: "push",
		Target: Target{
			Repository: "test/repo",
			Descriptor: distribution.Descriptor{
				MediaType: "application/vnd.docker.distribution.manifest.v2+json",
				Digest:    "sha256:1234567890",
			},
		},
	}

	// Test various HTTP status codes
	testCases := []struct {
		status    int
		isSuccess bool
	}{
		{http.StatusOK, true},
		{http.StatusCreated, true},
		{http.StatusAccepted, true},
		{http.StatusNoContent, true},
		{http.StatusBadRequest, false},
		{http.StatusUnauthorized, false},
		{http.StatusForbidden, false},
		{http.StatusNotFound, false},
		{http.StatusInternalServerError, false},
		{http.StatusBadGateway, false},
		{http.StatusServiceUnavailable, false},
	}

	for _, tc := range testCases {
		if tc.isSuccess {
			listener.success(tc.status, event)
		} else {
			listener.failure(tc.status, event)
		}
	}

	// Verify status distribution
	for _, tc := range testCases {
		key := fmt.Sprintf("%d %s", tc.status, http.StatusText(tc.status))
		v, ok := sm.statuses.Load(key)
		require.True(t, ok)
		require.Equal(t, int64(1), v.(*atomic.Int64).Load(), "Status %s should have count 1", key)
	}
}
