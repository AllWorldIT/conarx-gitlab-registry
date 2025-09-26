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
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/metrics"
	"github.com/opencontainers/go-digest"
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
		t.Logf("expected one-element []any, got %#v", v)
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

	// Simulate pending gauge - now includes endpoint label
	pendingGauge.WithLabelValues("webhook-endpoint").Inc()
	pendingGauge.WithLabelValues("webhook-endpoint").Inc()
	pendingGauge.WithLabelValues("webhook-endpoint").Dec()

	// Simulate status counter - now includes endpoint label
	statusCounter.WithLabelValues("200 OK", "webhook-endpoint").Inc()
	statusCounter.WithLabelValues("200 OK", "webhook-endpoint").Inc()
	statusCounter.WithLabelValues("404 Not Found", "webhook-endpoint").Inc()
	statusCounter.WithLabelValues("500 Internal Server Error", "webhook-endpoint").Inc()

	// Simulate error counter
	errorCounter.WithLabelValues("webhook-endpoint").Inc()
	errorCounter.WithLabelValues("backup-endpoint").Inc()
	errorCounter.WithLabelValues("backup-endpoint").Inc()

	// Simulate delivery counter
	deliveryCounter.WithLabelValues("webhook-endpoint", "delivered").Inc()
	deliveryCounter.WithLabelValues("webhook-endpoint", "delivered").Inc()
	deliveryCounter.WithLabelValues("webhook-endpoint", "lost").Inc()
	deliveryCounter.WithLabelValues("backup-endpoint", "delivered").Inc()
	deliveryCounter.WithLabelValues("backup-endpoint", "lost").Inc()
	deliveryCounter.WithLabelValues("backup-endpoint", "lost").Inc()

	// Simulate retries histogram
	retriesHist.WithLabelValues("webhook-endpoint").Observe(0)
	retriesHist.WithLabelValues("webhook-endpoint").Observe(1)
	retriesHist.WithLabelValues("webhook-endpoint").Observe(3)
	retriesHist.WithLabelValues("backup-endpoint").Observe(5)
	retriesHist.WithLabelValues("backup-endpoint").Observe(10)
	retriesHist.WithLabelValues("backup-endpoint").Observe(10)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_notifications_delivery_total The number of events delivered or lost. Event is lost once the number of retries was exhausted.
# TYPE registry_notifications_delivery_total counter
registry_notifications_delivery_total{delivery_type="delivered",endpoint="backup-endpoint"} 1
registry_notifications_delivery_total{delivery_type="delivered",endpoint="webhook-endpoint"} 2
registry_notifications_delivery_total{delivery_type="lost",endpoint="backup-endpoint"} 2
registry_notifications_delivery_total{delivery_type="lost",endpoint="webhook-endpoint"} 1
# HELP registry_notifications_errors_total The number of events where an error occurred during sending. Sending them MAY be retried.
# TYPE registry_notifications_errors_total counter
registry_notifications_errors_total{endpoint="backup-endpoint"} 2
registry_notifications_errors_total{endpoint="webhook-endpoint"} 1
# HELP registry_notifications_events_total The total number of events
# TYPE registry_notifications_events_total counter
registry_notifications_events_total{action="pull",artifact="manifest",endpoint="webhook-endpoint",type="Events"} 1
registry_notifications_events_total{action="push",artifact="blob",endpoint="webhook-endpoint",type="Events"} 1
registry_notifications_events_total{action="push",artifact="manifest",endpoint="webhook-endpoint",type="Events"} 1
# HELP registry_notifications_pending The gauge of pending events in queue - queue length
# TYPE registry_notifications_pending gauge
registry_notifications_pending{endpoint="webhook-endpoint"} 1
# HELP registry_notifications_retries_count The histogram of delivery retries done
# TYPE registry_notifications_retries_count histogram
registry_notifications_retries_count_bucket{endpoint="backup-endpoint",le="0"} 0
registry_notifications_retries_count_bucket{endpoint="backup-endpoint",le="1"} 0
registry_notifications_retries_count_bucket{endpoint="backup-endpoint",le="2"} 0
registry_notifications_retries_count_bucket{endpoint="backup-endpoint",le="3"} 0
registry_notifications_retries_count_bucket{endpoint="backup-endpoint",le="5"} 1
registry_notifications_retries_count_bucket{endpoint="backup-endpoint",le="10"} 3
registry_notifications_retries_count_bucket{endpoint="backup-endpoint",le="15"} 3
registry_notifications_retries_count_bucket{endpoint="backup-endpoint",le="20"} 3
registry_notifications_retries_count_bucket{endpoint="backup-endpoint",le="30"} 3
registry_notifications_retries_count_bucket{endpoint="backup-endpoint",le="50"} 3
registry_notifications_retries_count_bucket{endpoint="backup-endpoint",le="+Inf"} 3
registry_notifications_retries_count_sum{endpoint="backup-endpoint"} 25
registry_notifications_retries_count_count{endpoint="backup-endpoint"} 3
registry_notifications_retries_count_bucket{endpoint="webhook-endpoint",le="0"} 1
registry_notifications_retries_count_bucket{endpoint="webhook-endpoint",le="1"} 2
registry_notifications_retries_count_bucket{endpoint="webhook-endpoint",le="2"} 2
registry_notifications_retries_count_bucket{endpoint="webhook-endpoint",le="3"} 3
registry_notifications_retries_count_bucket{endpoint="webhook-endpoint",le="5"} 3
registry_notifications_retries_count_bucket{endpoint="webhook-endpoint",le="10"} 3
registry_notifications_retries_count_bucket{endpoint="webhook-endpoint",le="15"} 3
registry_notifications_retries_count_bucket{endpoint="webhook-endpoint",le="20"} 3
registry_notifications_retries_count_bucket{endpoint="webhook-endpoint",le="30"} 3
registry_notifications_retries_count_bucket{endpoint="webhook-endpoint",le="50"} 3
registry_notifications_retries_count_bucket{endpoint="webhook-endpoint",le="+Inf"} 3
registry_notifications_retries_count_sum{endpoint="webhook-endpoint"} 4
registry_notifications_retries_count_count{endpoint="webhook-endpoint"} 3
# HELP registry_notifications_status_total The number HTTP responses per status code received from notifications endpoint
# TYPE registry_notifications_status_total counter
registry_notifications_status_total{code="200 OK",endpoint="webhook-endpoint"} 2
registry_notifications_status_total{code="404 Not Found",endpoint="webhook-endpoint"} 1
registry_notifications_status_total{code="500 Internal Server Error",endpoint="webhook-endpoint"} 1
`)
	require.NoError(t, err)

	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, eventsCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, pendingGaugeName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, statusCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, errorCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, deliveryCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, retriesName),
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
# HELP registry_notifications_errors_total The number of events where an error occurred during sending. Sending them MAY be retried.
# TYPE registry_notifications_errors_total counter
registry_notifications_errors_total{endpoint="test-endpoint"} 2
# HELP registry_notifications_events_total The total number of events
# TYPE registry_notifications_events_total counter
registry_notifications_events_total{action="push",artifact="manifest",endpoint="test-endpoint",type="Failures"} 2
registry_notifications_events_total{action="push",artifact="manifest",endpoint="test-endpoint",type="Successes"} 2
# HELP registry_notifications_status_total The number HTTP responses per status code received from notifications endpoint
# TYPE registry_notifications_status_total counter
registry_notifications_status_total{code="200 OK",endpoint="test-endpoint"} 1
registry_notifications_status_total{code="201 Created",endpoint="test-endpoint"} 1
registry_notifications_status_total{code="404 Not Found",endpoint="test-endpoint"} 1
registry_notifications_status_total{code="500 Internal Server Error",endpoint="test-endpoint"} 1
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
# HELP registry_notifications_events_total The total number of events
# TYPE registry_notifications_events_total counter
registry_notifications_events_total{action="pull",artifact="blob",endpoint="queue-endpoint",type="Events"} 1
registry_notifications_events_total{action="push",artifact="manifest",endpoint="queue-endpoint",type="Events"} 2
# HELP registry_notifications_pending The gauge of pending events in queue - queue length
# TYPE registry_notifications_pending gauge
registry_notifications_pending{endpoint="queue-endpoint"} 1
`)
	require.NoError(t, err)

	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, eventsCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, pendingGaugeName),
	}

	err = testutil.GatherAndCompare(registry, &expected, names...)
	require.NoError(t, err)
}

func TestEndpointMetricsDeliveryListener(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("delivery-endpoint")
	listener := sm.deliveryListener()

	// Test successful deliveries with different retry counts
	listener.eventDelivered(0) // No retries
	listener.eventDelivered(1) // 1 retry
	listener.eventDelivered(3) // 3 retries
	listener.eventDelivered(2) // 2 retries

	// Test lost events with different retry counts
	listener.eventLost(5)  // 5 retries before giving up
	listener.eventLost(10) // 10 retries before giving up
	listener.eventLost(15) // 15 retries before giving up

	// Verify safeMetrics internal state
	require.EqualValues(t, 4, sm.delivered.Load())
	require.EqualValues(t, 3, sm.lost.Load())
	require.EqualValues(t, 36, sm.retries.Load()) // 0+1+3+2+5+10+15 = 36

	// Verify Prometheus metrics
	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_notifications_delivery_total The number of events delivered or lost. Event is lost once the number of retries was exhausted.
# TYPE registry_notifications_delivery_total counter
registry_notifications_delivery_total{delivery_type="delivered",endpoint="delivery-endpoint"} 4
registry_notifications_delivery_total{delivery_type="lost",endpoint="delivery-endpoint"} 3
# HELP registry_notifications_retries_count The histogram of delivery retries done
# TYPE registry_notifications_retries_count histogram
registry_notifications_retries_count_bucket{endpoint="delivery-endpoint",le="0"} 1
registry_notifications_retries_count_bucket{endpoint="delivery-endpoint",le="1"} 2
registry_notifications_retries_count_bucket{endpoint="delivery-endpoint",le="2"} 3
registry_notifications_retries_count_bucket{endpoint="delivery-endpoint",le="3"} 4
registry_notifications_retries_count_bucket{endpoint="delivery-endpoint",le="5"} 5
registry_notifications_retries_count_bucket{endpoint="delivery-endpoint",le="10"} 6
registry_notifications_retries_count_bucket{endpoint="delivery-endpoint",le="15"} 7
registry_notifications_retries_count_bucket{endpoint="delivery-endpoint",le="20"} 7
registry_notifications_retries_count_bucket{endpoint="delivery-endpoint",le="30"} 7
registry_notifications_retries_count_bucket{endpoint="delivery-endpoint",le="50"} 7
registry_notifications_retries_count_bucket{endpoint="delivery-endpoint",le="+Inf"} 7
registry_notifications_retries_count_sum{endpoint="delivery-endpoint"} 36
registry_notifications_retries_count_count{endpoint="delivery-endpoint"} 7
`)
	require.NoError(t, err)

	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, deliveryCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, retriesName),
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
	deliveryListener := sm.deliveryListener()

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

	// Delivery updates
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			base := i * operationsPerGoroutine
			for j := 0; j < operationsPerGoroutine; j++ {
				retries := int64((base + j) % 10)
				if (base+j)%3 == 0 {
					deliveryListener.eventLost(retries)
				} else {
					deliveryListener.eventDelivered(retries)
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

	// Verify delivery metrics
	expectedLost := (totalOperations / 3)
	if totalOperations%3 > 0 {
		expectedLost++
	}
	expectedDelivered := totalOperations - expectedLost

	assert.EqualValues(t, expectedDelivered, sm.delivered.Load())
	assert.EqualValues(t, expectedLost, sm.lost.Load())
	assert.Positive(t, sm.retries.Load())
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
	require.Zero(t, sm.retries.Load())
	require.Zero(t, sm.delivered.Load())
	require.Zero(t, sm.lost.Load())
	require.NotNil(t, sm.statuses)

	// Check that statuses map is empty
	count := 0
	sm.statuses.Range(func(_, _ any) bool {
		count++
		return true
	})
	require.Zero(t, count, "statuses map should be empty")
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

func TestPendingGaugeOperations(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	endpoint := "gauge-test-endpoint"

	// Test Inc method
	pendingGauge.WithLabelValues(endpoint).Inc()
	pendingGauge.WithLabelValues(endpoint).Inc()
	pendingGauge.WithLabelValues(endpoint).Inc()

	// Test Dec method
	pendingGauge.WithLabelValues(endpoint).Dec()

	// Verify the gauge value
	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_notifications_pending The gauge of pending events in queue - queue length
# TYPE registry_notifications_pending gauge
registry_notifications_pending{endpoint="gauge-test-endpoint"} 2
`)
	require.NoError(t, err)

	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, pendingGaugeName),
	}

	err = testutil.GatherAndCompare(registry, &expected, names...)
	require.NoError(t, err)
}

func TestDeliveryMetricsEdgeCases(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("edge-case-endpoint")
	listener := sm.deliveryListener()

	// Test edge cases for retry counts
	testCases := []struct {
		name        string
		retries     int64
		isDelivered bool
	}{
		{"delivered_no_retries", 0, true},
		{"delivered_max_bucket", 50, true},
		{"delivered_over_max", 100, true},
		{"lost_no_retries", 0, false},
		{"lost_max_bucket", 50, false},
		{"lost_over_max", 100, false},
	}

	for _, tc := range testCases {
		if tc.isDelivered {
			listener.eventDelivered(tc.retries)
		} else {
			listener.eventLost(tc.retries)
		}
	}

	// Verify internal state
	require.EqualValues(t, 3, sm.delivered.Load())
	require.EqualValues(t, 3, sm.lost.Load())
	require.EqualValues(t, 300, sm.retries.Load()) // 0+50+100+0+50+100 = 300

	// Verify histogram buckets for edge cases
	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_notifications_delivery_total The number of events delivered or lost. Event is lost once the number of retries was exhausted.
# TYPE registry_notifications_delivery_total counter
registry_notifications_delivery_total{delivery_type="delivered",endpoint="edge-case-endpoint"} 3
registry_notifications_delivery_total{delivery_type="lost",endpoint="edge-case-endpoint"} 3
# HELP registry_notifications_retries_count The histogram of delivery retries done
# TYPE registry_notifications_retries_count histogram
registry_notifications_retries_count_bucket{endpoint="edge-case-endpoint",le="0"} 2
registry_notifications_retries_count_bucket{endpoint="edge-case-endpoint",le="1"} 2
registry_notifications_retries_count_bucket{endpoint="edge-case-endpoint",le="2"} 2
registry_notifications_retries_count_bucket{endpoint="edge-case-endpoint",le="3"} 2
registry_notifications_retries_count_bucket{endpoint="edge-case-endpoint",le="5"} 2
registry_notifications_retries_count_bucket{endpoint="edge-case-endpoint",le="10"} 2
registry_notifications_retries_count_bucket{endpoint="edge-case-endpoint",le="15"} 2
registry_notifications_retries_count_bucket{endpoint="edge-case-endpoint",le="20"} 2
registry_notifications_retries_count_bucket{endpoint="edge-case-endpoint",le="30"} 2
registry_notifications_retries_count_bucket{endpoint="edge-case-endpoint",le="50"} 4
registry_notifications_retries_count_bucket{endpoint="edge-case-endpoint",le="+Inf"} 6
registry_notifications_retries_count_sum{endpoint="edge-case-endpoint"} 300
registry_notifications_retries_count_count{endpoint="edge-case-endpoint"} 6
`)
	require.NoError(t, err)

	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, deliveryCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, retriesName),
	}

	err = testutil.GatherAndCompare(registry, &expected, names...)
	require.NoError(t, err)
}

func TestEndpointMetricsRetriesFlow(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("integration-endpoint")
	httpListener := sm.httpStatusListener()
	queueListener := sm.eventQueueListener()
	deliveryListener := sm.deliveryListener()

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

	// Simulate a typical event flow
	// 1. Event enters the queue
	queueListener.ingress(event)

	// 2. First delivery attempt fails
	httpListener.failure(500, event)

	// 3. Second delivery attempt fails
	httpListener.failure(503, event)

	// 4. Third delivery attempt succeeds
	httpListener.success(200, event)

	// 5. Event leaves the queue
	queueListener.egress(event)

	// 6. Report successful delivery with 2 retries
	deliveryListener.eventDelivered(2)

	// Simulate another event that fails all retries
	event2 := &Event{
		ID:     "test-id-2",
		Action: "pull",
		Target: Target{
			Repository: "test/repo",
			Descriptor: distribution.Descriptor{
				MediaType: "application/octet-stream",
				Digest:    "sha256:0987654321",
			},
		},
	}

	queueListener.ingress(event2)

	// Multiple failed attempts
	for i := 0; i < 5; i++ {
		httpListener.failure(500, event2)
	}

	// Give up after 5 retries
	httpListener.err(event2)
	queueListener.egress(event2)
	deliveryListener.eventLost(5)

	// Verify final state
	require.EqualValues(t, 2, sm.events.Load())
	require.EqualValues(t, 0, sm.pending.Load())
	require.EqualValues(t, 1, sm.successes.Load())
	require.EqualValues(t, 7, sm.failures.Load())
	require.EqualValues(t, 1, sm.errors.Load())
	require.EqualValues(t, 1, sm.delivered.Load())
	require.EqualValues(t, 1, sm.lost.Load())
	require.EqualValues(t, 7, sm.retries.Load())
}

func TestHTTPLatencyMetrics(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("latency-endpoint")
	httpListener := sm.httpStatusListener()

	// Test various latencies
	latencies := []time.Duration{
		1 * time.Millisecond,
		5 * time.Millisecond,
		25 * time.Millisecond,
		100 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		5 * time.Second,
		30 * time.Second,
	}

	for _, latency := range latencies {
		httpListener.latency(latency)
	}

	// Verify Prometheus metrics
	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_notifications_http_latency_seconds The histogram of HTTP delivery latency
# TYPE registry_notifications_http_latency_seconds histogram
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="0.005"} 2
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="0.01"} 2
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="0.025"} 3
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="0.05"} 3
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="0.1"} 4
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="0.25"} 4
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="0.5"} 5
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="1"} 6
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="2.5"} 6
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="5"} 7
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="10"} 7
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="25"} 7
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="50"} 8
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="100"} 8
registry_notifications_http_latency_seconds_bucket{endpoint="latency-endpoint",le="+Inf"} 8
registry_notifications_http_latency_seconds_sum{endpoint="latency-endpoint"} 36.631
registry_notifications_http_latency_seconds_count{endpoint="latency-endpoint"} 8
`)
	require.NoError(t, err)

	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, httpLatencyName),
	}

	err = testutil.GatherAndCompare(registry, &expected, names...)
	require.NoError(t, err)
}

func TestTotalLatencyMetrics(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("total-latency-endpoint")
	queueListener := sm.eventQueueListener()

	// Create events with different timestamps
	now := time.Now()
	events := []*Event{
		{
			ID:        "event-1",
			Action:    "push",
			Timestamp: now.Add(-2 * time.Millisecond),
			Target: Target{
				Repository: "test/repo",
				Descriptor: distribution.Descriptor{
					MediaType: "application/vnd.docker.distribution.manifest.v2+json",
					Digest:    "sha256:1234567890",
				},
			},
		},
		{
			ID:        "event-2",
			Action:    "pull",
			Timestamp: now.Add(-50 * time.Millisecond),
			Target: Target{
				Repository: "test/repo",
				Descriptor: distribution.Descriptor{
					MediaType: "application/octet-stream",
					Digest:    "sha256:1234567890",
				},
			},
		},
		{
			ID:        "event-3",
			Action:    "push",
			Timestamp: now.Add(-500 * time.Millisecond),
			Target: Target{
				Repository: "test/repo",
				Descriptor: distribution.Descriptor{
					MediaType: "application/vnd.docker.distribution.manifest.v2+json",
					Digest:    "sha256:0987654321",
				},
			},
		},
		{
			ID:        "event-4",
			Action:    "delete",
			Timestamp: now.Add(-2 * time.Second),
			Target: Target{
				Repository: "test/repo",
				Descriptor: distribution.Descriptor{
					MediaType: "application/vnd.docker.distribution.manifest.v2+json",
					Digest:    "sha256:1111111111",
				},
			},
		},
	}

	// Ingress all events
	for _, event := range events {
		queueListener.ingress(event)
	}

	// Sleep briefly to ensure time difference
	time.Sleep(10 * time.Millisecond)

	// Egress events - this triggers the total latency measurement
	for _, event := range events {
		queueListener.egress(event)
	}

	// Verify that the total latency metric was recorded
	// We can't check exact values due to timing, but we can verify it was recorded
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	var totalLatencyFound bool
	var observationCount uint64
	for _, mf := range metricFamilies {
		if mf.GetName() != fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, totalLatencyName) {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() != "endpoint" || label.GetValue() != "total-latency-endpoint" {
					continue
				}

				totalLatencyFound = true
				observationCount = metric.GetHistogram().GetSampleCount()
				require.Equal(t, uint64(4), observationCount, "Expected 4 latency observations")
				require.Positive(t, metric.GetHistogram().GetSampleSum(), "Expected positive latency sum")
				break
			}
		}
	}
	require.True(t, totalLatencyFound, "Expected to find total latency metric for our endpoint")
}

func TestConcurrentLatencyMetrics(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("concurrent-latency-endpoint")
	httpListener := sm.httpStatusListener()
	queueListener := sm.eventQueueListener()

	var wg sync.WaitGroup
	numGoroutines := 10
	operationsPerGoroutine := 100

	// Concurrent HTTP latency updates
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				// Generate varied latencies
				latency := time.Duration(j%100) * time.Millisecond
				httpListener.latency(latency)
			}
		}()
	}

	// Concurrent event processing for total latency
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				event := &Event{
					ID:        fmt.Sprintf("event-%d-%d", i, j),
					Action:    "push",
					Timestamp: time.Now().Add(-time.Duration(j%60) * time.Second),
					Target: Target{
						Repository: "test/repo",
						Descriptor: distribution.Descriptor{
							MediaType: "application/vnd.docker.distribution.manifest.v2+json",
							Digest:    "sha256:0000000000000000000000000000000000000000000000000000000000000000",
						},
					},
				}
				queueListener.ingress(event)
				// Small delay to simulate processing
				time.Sleep(time.Microsecond)
				queueListener.egress(event)
			}
		}(i)
	}

	wg.Wait()

	// Verify that metrics were recorded correctly
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	var httpLatencyCount, totalLatencyCount uint64
	for _, mf := range metricFamilies {
		switch mf.GetName() {
		case fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, httpLatencyName):
			for _, metric := range mf.GetMetric() {
				for _, label := range metric.GetLabel() {
					if label.GetName() == "endpoint" && label.GetValue() == "concurrent-latency-endpoint" {
						httpLatencyCount = metric.GetHistogram().GetSampleCount()
					}
				}
			}
		case fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, totalLatencyName):
			for _, metric := range mf.GetMetric() {
				for _, label := range metric.GetLabel() {
					if label.GetName() == "endpoint" && label.GetValue() == "concurrent-latency-endpoint" {
						totalLatencyCount = metric.GetHistogram().GetSampleCount()
					}
				}
			}
		}
	}

	require.Equal(t, uint64(numGoroutines*operationsPerGoroutine), httpLatencyCount)
	require.Equal(t, uint64(numGoroutines*operationsPerGoroutine), totalLatencyCount)
}

func TestLatencyMetricsWithFullFlow(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("flow-endpoint")
	httpListener := sm.httpStatusListener()
	queueListener := sm.eventQueueListener()
	deliveryListener := sm.deliveryListener()

	// Simulate a complete event flow with latency tracking
	event := &Event{
		ID:        "flow-event",
		Action:    "push",
		Timestamp: time.Now().Add(-30 * time.Second), // Event created 30 seconds ago
		Target: Target{
			Repository: "test/repo",
			Descriptor: distribution.Descriptor{
				MediaType: "application/vnd.docker.distribution.manifest.v2+json",
				Digest:    "sha256:flowtest",
			},
		},
	}

	// 1. Event enters queue
	queueListener.ingress(event)

	// 2. First delivery attempt with HTTP latency
	httpListener.latency(50 * time.Millisecond)
	httpListener.failure(500, event)

	// 3. Retry with backoff
	time.Sleep(100 * time.Millisecond)
	httpListener.latency(75 * time.Millisecond)
	httpListener.failure(503, event)

	// 4. Final successful delivery
	time.Sleep(200 * time.Millisecond)
	httpListener.latency(25 * time.Millisecond)
	httpListener.success(200, event)

	// 5. Event leaves queue (triggers total latency measurement)
	queueListener.egress(event)

	// 6. Report delivery with retries
	deliveryListener.eventDelivered(2)

	// Verify all metrics are properly recorded
	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_notifications_delivery_total The number of events delivered or lost. Event is lost once the number of retries was exhausted.
# TYPE registry_notifications_delivery_total counter
registry_notifications_delivery_total{delivery_type="delivered",endpoint="flow-endpoint"} 1
# HELP registry_notifications_events_total The total number of events
# TYPE registry_notifications_events_total counter
registry_notifications_events_total{action="push",artifact="manifest",endpoint="flow-endpoint",type="Events"} 1
registry_notifications_events_total{action="push",artifact="manifest",endpoint="flow-endpoint",type="Failures"} 2
registry_notifications_events_total{action="push",artifact="manifest",endpoint="flow-endpoint",type="Successes"} 1
# HELP registry_notifications_http_latency_seconds The histogram of HTTP delivery latency
# TYPE registry_notifications_http_latency_seconds histogram
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="0.005"} 0
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="0.01"} 0
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="0.025"} 1
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="0.05"} 2
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="0.1"} 3
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="0.25"} 3
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="0.5"} 3
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="1"} 3
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="2.5"} 3
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="5"} 3
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="10"} 3
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="25"} 3
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="50"} 3
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="100"} 3
registry_notifications_http_latency_seconds_bucket{endpoint="flow-endpoint",le="+Inf"} 3
registry_notifications_http_latency_seconds_sum{endpoint="flow-endpoint"} 0.15
registry_notifications_http_latency_seconds_count{endpoint="flow-endpoint"} 3
# HELP registry_notifications_pending The gauge of pending events in queue - queue length
# TYPE registry_notifications_pending gauge
registry_notifications_pending{endpoint="flow-endpoint"} 0
# HELP registry_notifications_retries_count The histogram of delivery retries done
# TYPE registry_notifications_retries_count histogram
registry_notifications_retries_count_bucket{endpoint="flow-endpoint",le="0"} 0
registry_notifications_retries_count_bucket{endpoint="flow-endpoint",le="1"} 0
registry_notifications_retries_count_bucket{endpoint="flow-endpoint",le="2"} 1
registry_notifications_retries_count_bucket{endpoint="flow-endpoint",le="3"} 1
registry_notifications_retries_count_bucket{endpoint="flow-endpoint",le="5"} 1
registry_notifications_retries_count_bucket{endpoint="flow-endpoint",le="10"} 1
registry_notifications_retries_count_bucket{endpoint="flow-endpoint",le="15"} 1
registry_notifications_retries_count_bucket{endpoint="flow-endpoint",le="20"} 1
registry_notifications_retries_count_bucket{endpoint="flow-endpoint",le="30"} 1
registry_notifications_retries_count_bucket{endpoint="flow-endpoint",le="50"} 1
registry_notifications_retries_count_bucket{endpoint="flow-endpoint",le="+Inf"} 1
registry_notifications_retries_count_sum{endpoint="flow-endpoint"} 2
registry_notifications_retries_count_count{endpoint="flow-endpoint"} 1
# HELP registry_notifications_status_total The number HTTP responses per status code received from notifications endpoint
# TYPE registry_notifications_status_total counter
registry_notifications_status_total{code="200 OK",endpoint="flow-endpoint"} 1
registry_notifications_status_total{code="500 Internal Server Error",endpoint="flow-endpoint"} 1
registry_notifications_status_total{code="503 Service Unavailable",endpoint="flow-endpoint"} 1
`)
	require.NoError(t, err)

	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, eventsCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, pendingGaugeName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, statusCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, deliveryCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, retriesName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, httpLatencyName),
	}

	err = testutil.GatherAndCompare(registry, &expected, names...)
	require.NoError(t, err)

	// Also verify that total latency was recorded (can't predict exact value due to timing)
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	var totalLatencyRecorded bool
	for _, mf := range metricFamilies {
		if mf.GetName() == fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, totalLatencyName) {
			for _, metric := range mf.GetMetric() {
				for _, label := range metric.GetLabel() {
					if label.GetName() == "endpoint" && label.GetValue() == "flow-endpoint" {
						require.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
						require.Greater(t, metric.GetHistogram().GetSampleSum(), float64(30), "Total latency should be > 30 seconds")
						totalLatencyRecorded = true
						break
					}
				}
			}
		}
	}
	require.True(t, totalLatencyRecorded, "Total latency should have been recorded")
}

func TestEndpointMetricsDropFlow(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("integration-endpoint")
	httpListener := sm.httpStatusListener()
	queueListener := sm.eventQueueListener()
	deliveryListener := sm.deliveryListener()

	// Event 1: Successfully delivered
	event1 := &Event{
		ID:     "success-event",
		Action: "push",
		Target: Target{
			Repository: "test/repo",
			Descriptor: distribution.Descriptor{
				MediaType: "application/vnd.docker.distribution.manifest.v2+json",
				Digest:    "sha256:1234567890",
			},
		},
	}

	queueListener.ingress(event1)
	httpListener.success(200, event1)
	queueListener.egress(event1)
	deliveryListener.eventDelivered(0)

	// Event 2: Dropped due to queue overflow
	event2 := &Event{
		ID:     "dropped-event",
		Action: "pull",
		Target: Target{
			Repository: "test/repo",
			Descriptor: distribution.Descriptor{
				MediaType: "application/octet-stream",
				Digest:    "sha256:0987654321",
			},
		},
	}

	queueListener.ingress(event2)
	queueListener.drop(event2)

	// Event 3: Lost after retries
	event3 := &Event{
		ID:     "lost-event",
		Action: "delete",
		Target: Target{
			Repository: "test/repo",
			Descriptor: distribution.Descriptor{
				MediaType: "application/vnd.docker.distribution.manifest.v2+json",
				Digest:    "sha256:abcdef1234",
			},
		},
	}

	queueListener.ingress(event3)
	for i := 0; i < 3; i++ {
		httpListener.failure(500, event3)
	}
	httpListener.err(event3)
	queueListener.egress(event3)
	deliveryListener.eventLost(3)

	// Verify final state
	require.EqualValues(t, 3, sm.events.Load())
	require.EqualValues(t, 0, sm.pending.Load())
	require.EqualValues(t, 1, sm.successes.Load())
	require.EqualValues(t, 3, sm.failures.Load())
	require.EqualValues(t, 1, sm.errors.Load())
	require.EqualValues(t, 1, sm.delivered.Load())
	require.EqualValues(t, 1, sm.lost.Load())
	require.EqualValues(t, 1, sm.dropped.Load())
	require.EqualValues(t, 3, sm.retries.Load())
}

func TestEndpointMetricsDropListener(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("drop-endpoint")
	listener := sm.eventQueueListener()

	event1 := &Event{
		ID:     "drop-test-1",
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
		ID:     "drop-test-2",
		Action: "pull",
		Target: Target{
			Repository: "test/repo",
			Descriptor: distribution.Descriptor{
				MediaType: "application/octet-stream",
				Digest:    "sha256:0987654321",
			},
		},
	}

	event3 := &Event{
		ID:     "drop-test-3",
		Action: "delete",
		Target: Target{
			Repository: "test/repo",
			Descriptor: distribution.Descriptor{
				MediaType: "application/vnd.docker.distribution.manifest.v2+json",
				Digest:    "sha256:1111111111",
			},
		},
	}

	// Test drop functionality
	listener.drop(event1)
	listener.drop(event2)
	listener.drop(event3)
	listener.drop(event1) // Drop the same event type again

	// Verify internal state
	require.EqualValues(t, 4, sm.dropped.Load())

	// Verify Prometheus metrics
	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_notifications_events_total The total number of events
# TYPE registry_notifications_events_total counter
registry_notifications_events_total{action="delete",artifact="manifest",endpoint="drop-endpoint",type="Dropped"} 1
registry_notifications_events_total{action="pull",artifact="blob",endpoint="drop-endpoint",type="Dropped"} 1
registry_notifications_events_total{action="push",artifact="manifest",endpoint="drop-endpoint",type="Dropped"} 2
`)
	require.NoError(t, err)

	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, eventsCounterName),
	}

	err = testutil.GatherAndCompare(registry, &expected, names...)
	require.NoError(t, err)
}

func TestEndpointMetricsEventQueueWithDrops(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("queue-drop-endpoint")
	listener := sm.eventQueueListener()

	events := []*Event{
		{
			ID:     "test-1",
			Action: "push",
			Target: Target{
				Repository: "test/repo",
				Descriptor: distribution.Descriptor{
					MediaType: "application/vnd.docker.distribution.manifest.v2+json",
					Digest:    "sha256:1111111111",
				},
			},
		},
		{
			ID:     "test-2",
			Action: "pull",
			Target: Target{
				Repository: "test/repo",
				Descriptor: distribution.Descriptor{
					MediaType: "application/octet-stream",
					Digest:    "sha256:2222222222",
				},
			},
		},
		{
			ID:     "test-3",
			Action: "push",
			Target: Target{
				Repository: "test/repo",
				Descriptor: distribution.Descriptor{
					MediaType: "application/vnd.docker.distribution.manifest.v2+json",
					Digest:    "sha256:3333333333",
				},
			},
		},
	}

	// Simulate a scenario where some events are processed and some are dropped
	// Ingress all events
	for _, event := range events {
		listener.ingress(event)
	}

	// Process first event successfully
	listener.egress(events[0])

	// Drop second event (e.g., due to queue overflow or timeout)
	listener.drop(events[1])

	// Process third event successfully
	listener.egress(events[2])

	// Verify internal state
	require.EqualValues(t, 3, sm.events.Load())
	require.EqualValues(t, 0, sm.pending.Load())
	require.EqualValues(t, 1, sm.dropped.Load())

	// Note: The drop operation doesn't affect the pending count since the event
	// was never removed from the queue through egress

	// Verify Prometheus metrics
	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_notifications_events_total The total number of events
# TYPE registry_notifications_events_total counter
registry_notifications_events_total{action="pull",artifact="blob",endpoint="queue-drop-endpoint",type="Dropped"} 1
registry_notifications_events_total{action="pull",artifact="blob",endpoint="queue-drop-endpoint",type="Events"} 1
registry_notifications_events_total{action="push",artifact="manifest",endpoint="queue-drop-endpoint",type="Events"} 2
# HELP registry_notifications_pending The gauge of pending events in queue - queue length
# TYPE registry_notifications_pending gauge
registry_notifications_pending{endpoint="queue-drop-endpoint"} 1
`)
	require.NoError(t, err)

	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, eventsCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, pendingGaugeName),
	}

	err = testutil.GatherAndCompare(registry, &expected, names...)
	require.NoError(t, err)
}

func TestConcurrentDropMetrics(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("concurrent-drop-endpoint")
	listener := sm.eventQueueListener()

	var wg sync.WaitGroup
	numGoroutines := 10
	dropsPerGoroutine := 50

	// Launch multiple goroutines to drop events concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < dropsPerGoroutine; j++ {
				event := &Event{
					ID:     fmt.Sprintf("drop-%d-%d", goroutineID, j),
					Action: "push",
					Target: Target{
						Repository: "test/repo",
						Descriptor: distribution.Descriptor{
							MediaType: "application/vnd.docker.distribution.manifest.v2+json",
							Digest:    "sha256:0000000000",
						},
					},
				}
				listener.drop(event)
			}
		}(i)
	}

	wg.Wait()

	// Verify total drops
	expectedDrops := int64(numGoroutines * dropsPerGoroutine)
	require.Equal(t, expectedDrops, sm.dropped.Load())

	// Verify Prometheus metrics
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	var droppedCount float64
	for _, mf := range metricFamilies {
		if mf.GetName() == fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, eventsCounterName) {
			for _, metric := range mf.GetMetric() {
				hasDroppedType := false
				hasCorrectEndpoint := false
				for _, label := range metric.GetLabel() {
					if label.GetName() == "type" && label.GetValue() == "Dropped" {
						hasDroppedType = true
					}
					if label.GetName() == "endpoint" && label.GetValue() == "concurrent-drop-endpoint" {
						hasCorrectEndpoint = true
					}
				}
				if hasDroppedType && hasCorrectEndpoint {
					droppedCount = metric.GetCounter().GetValue()
				}
			}
		}
	}

	require.InEpsilon(t, float64(expectedDrops), droppedCount, 0.001)
}

func TestCompleteFlowWithDrops(t *testing.T) {
	// Create a new registry for isolated testing
	registry := prometheus.NewRegistry()
	registerMetrics(registry)

	sm := newSafeMetrics("complete-flow-endpoint")
	httpListener := sm.httpStatusListener()
	queueListener := sm.eventQueueListener()
	deliveryListener := sm.deliveryListener()

	// Create multiple events
	events := make([]*Event, 5)
	for i := range events {
		events[i] = &Event{
			ID:     fmt.Sprintf("flow-event-%d", i),
			Action: "push",
			Target: Target{
				Repository: "test/repo",
				Descriptor: distribution.Descriptor{
					MediaType: "application/vnd.docker.distribution.manifest.v2+json",
					Digest:    digest.Digest(fmt.Sprintf("sha256:%d%d%d%d%d%d%d%d%d%d", i, i, i, i, i, i, i, i, i, i)),
				},
			},
		}
	}

	// Simulate different outcomes for each event
	// Event 0: Successfully delivered
	queueListener.ingress(events[0])
	httpListener.success(200, events[0])
	queueListener.egress(events[0])
	deliveryListener.eventDelivered(0)

	// Event 1: Delivered after retries
	queueListener.ingress(events[1])
	httpListener.failure(500, events[1])
	httpListener.failure(503, events[1])
	httpListener.success(200, events[1])
	queueListener.egress(events[1])
	deliveryListener.eventDelivered(2)

	// Event 2: Lost after max retries
	queueListener.ingress(events[2])
	for i := 0; i < 5; i++ {
		httpListener.failure(500, events[2])
	}
	httpListener.err(events[2])
	queueListener.egress(events[2])
	deliveryListener.eventLost(5)

	// Event 3: Dropped (e.g., queue overflow)
	queueListener.ingress(events[3])
	queueListener.drop(events[3])
	// Note: dropped events don't go through egress

	// Event 4: Dropped after some failed attempts
	queueListener.ingress(events[4])
	httpListener.failure(400, events[4])
	httpListener.failure(400, events[4])
	queueListener.drop(events[4])

	// Verify final state
	require.EqualValues(t, 5, sm.events.Load())
	require.EqualValues(t, 0, sm.pending.Load())
	require.EqualValues(t, 2, sm.successes.Load())
	require.EqualValues(t, 9, sm.failures.Load())
	require.EqualValues(t, 1, sm.errors.Load())
	require.EqualValues(t, 2, sm.delivered.Load())
	require.EqualValues(t, 1, sm.lost.Load())
	require.EqualValues(t, 2, sm.dropped.Load())
	require.EqualValues(t, 7, sm.retries.Load()) // 0 + 2 + 5 = 7

	// Verify specific status codes
	v, ok := sm.statuses.Load("200 OK")
	require.True(t, ok)
	require.Equal(t, int64(2), v.(*atomic.Int64).Load())

	v, ok = sm.statuses.Load("400 Bad Request")
	require.True(t, ok)
	require.Equal(t, int64(2), v.(*atomic.Int64).Load())

	v, ok = sm.statuses.Load("500 Internal Server Error")
	require.True(t, ok)
	require.Equal(t, int64(6), v.(*atomic.Int64).Load())

	v, ok = sm.statuses.Load("503 Service Unavailable")
	require.True(t, ok)
	require.Equal(t, int64(1), v.(*atomic.Int64).Load())
}
