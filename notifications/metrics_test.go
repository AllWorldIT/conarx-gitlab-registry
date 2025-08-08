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
# HELP registry_notifications_delivery The number of events delivered or lost. Event is lost once the number of retries was exhausted.
# TYPE registry_notifications_delivery counter
registry_notifications_delivery{delivery_type="delivered",endpoint="backup-endpoint"} 1
registry_notifications_delivery{delivery_type="delivered",endpoint="webhook-endpoint"} 2
registry_notifications_delivery{delivery_type="lost",endpoint="backup-endpoint"} 2
registry_notifications_delivery{delivery_type="lost",endpoint="webhook-endpoint"} 1
# HELP registry_notifications_errors The number of events where an error occurred during sending. Sending them MAY be retried.
# TYPE registry_notifications_errors counter
registry_notifications_errors{endpoint="backup-endpoint"} 2
registry_notifications_errors{endpoint="webhook-endpoint"} 1
# HELP registry_notifications_events The total number of events
# TYPE registry_notifications_events counter
registry_notifications_events{action="pull",artifact="manifest",endpoint="webhook-endpoint",type="Events"} 1
registry_notifications_events{action="push",artifact="blob",endpoint="webhook-endpoint",type="Events"} 1
registry_notifications_events{action="push",artifact="manifest",endpoint="webhook-endpoint",type="Events"} 1
# HELP registry_notifications_pending The gauge of pending events in queue - queue length
# TYPE registry_notifications_pending gauge
registry_notifications_pending{endpoint="webhook-endpoint"} 1
# HELP registry_notifications_retries The histogram of delivery retries done
# TYPE registry_notifications_retries histogram
registry_notifications_retries_bucket{endpoint="backup-endpoint",le="0"} 0
registry_notifications_retries_bucket{endpoint="backup-endpoint",le="1"} 0
registry_notifications_retries_bucket{endpoint="backup-endpoint",le="2"} 0
registry_notifications_retries_bucket{endpoint="backup-endpoint",le="3"} 0
registry_notifications_retries_bucket{endpoint="backup-endpoint",le="5"} 1
registry_notifications_retries_bucket{endpoint="backup-endpoint",le="10"} 3
registry_notifications_retries_bucket{endpoint="backup-endpoint",le="15"} 3
registry_notifications_retries_bucket{endpoint="backup-endpoint",le="20"} 3
registry_notifications_retries_bucket{endpoint="backup-endpoint",le="30"} 3
registry_notifications_retries_bucket{endpoint="backup-endpoint",le="50"} 3
registry_notifications_retries_bucket{endpoint="backup-endpoint",le="+Inf"} 3
registry_notifications_retries_sum{endpoint="backup-endpoint"} 25
registry_notifications_retries_count{endpoint="backup-endpoint"} 3
registry_notifications_retries_bucket{endpoint="webhook-endpoint",le="0"} 1
registry_notifications_retries_bucket{endpoint="webhook-endpoint",le="1"} 2
registry_notifications_retries_bucket{endpoint="webhook-endpoint",le="2"} 2
registry_notifications_retries_bucket{endpoint="webhook-endpoint",le="3"} 3
registry_notifications_retries_bucket{endpoint="webhook-endpoint",le="5"} 3
registry_notifications_retries_bucket{endpoint="webhook-endpoint",le="10"} 3
registry_notifications_retries_bucket{endpoint="webhook-endpoint",le="15"} 3
registry_notifications_retries_bucket{endpoint="webhook-endpoint",le="20"} 3
registry_notifications_retries_bucket{endpoint="webhook-endpoint",le="30"} 3
registry_notifications_retries_bucket{endpoint="webhook-endpoint",le="50"} 3
registry_notifications_retries_bucket{endpoint="webhook-endpoint",le="+Inf"} 3
registry_notifications_retries_sum{endpoint="webhook-endpoint"} 4
registry_notifications_retries_count{endpoint="webhook-endpoint"} 3
# HELP registry_notifications_status The number HTTP responses per status code received from notifications endpoint
# TYPE registry_notifications_status counter
registry_notifications_status{code="200 OK",endpoint="webhook-endpoint"} 2
registry_notifications_status{code="404 Not Found",endpoint="webhook-endpoint"} 1
registry_notifications_status{code="500 Internal Server Error",endpoint="webhook-endpoint"} 1
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
# HELP registry_notifications_errors The number of events where an error occurred during sending. Sending them MAY be retried.
# TYPE registry_notifications_errors counter
registry_notifications_errors{endpoint="test-endpoint"} 2
# HELP registry_notifications_events The total number of events
# TYPE registry_notifications_events counter
registry_notifications_events{action="push",artifact="manifest",endpoint="test-endpoint",type="Failures"} 2
registry_notifications_events{action="push",artifact="manifest",endpoint="test-endpoint",type="Successes"} 2
# HELP registry_notifications_status The number HTTP responses per status code received from notifications endpoint
# TYPE registry_notifications_status counter
registry_notifications_status{code="200 OK",endpoint="test-endpoint"} 1
registry_notifications_status{code="201 Created",endpoint="test-endpoint"} 1
registry_notifications_status{code="404 Not Found",endpoint="test-endpoint"} 1
registry_notifications_status{code="500 Internal Server Error",endpoint="test-endpoint"} 1
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
# HELP registry_notifications_events The total number of events
# TYPE registry_notifications_events counter
registry_notifications_events{action="pull",artifact="blob",endpoint="queue-endpoint",type="Events"} 1
registry_notifications_events{action="push",artifact="manifest",endpoint="queue-endpoint",type="Events"} 2
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
# HELP registry_notifications_delivery The number of events delivered or lost. Event is lost once the number of retries was exhausted.
# TYPE registry_notifications_delivery counter
registry_notifications_delivery{delivery_type="delivered",endpoint="delivery-endpoint"} 4
registry_notifications_delivery{delivery_type="lost",endpoint="delivery-endpoint"} 3
# HELP registry_notifications_retries The histogram of delivery retries done
# TYPE registry_notifications_retries histogram
registry_notifications_retries_bucket{endpoint="delivery-endpoint",le="0"} 1
registry_notifications_retries_bucket{endpoint="delivery-endpoint",le="1"} 2
registry_notifications_retries_bucket{endpoint="delivery-endpoint",le="2"} 3
registry_notifications_retries_bucket{endpoint="delivery-endpoint",le="3"} 4
registry_notifications_retries_bucket{endpoint="delivery-endpoint",le="5"} 5
registry_notifications_retries_bucket{endpoint="delivery-endpoint",le="10"} 6
registry_notifications_retries_bucket{endpoint="delivery-endpoint",le="15"} 7
registry_notifications_retries_bucket{endpoint="delivery-endpoint",le="20"} 7
registry_notifications_retries_bucket{endpoint="delivery-endpoint",le="30"} 7
registry_notifications_retries_bucket{endpoint="delivery-endpoint",le="50"} 7
registry_notifications_retries_bucket{endpoint="delivery-endpoint",le="+Inf"} 7
registry_notifications_retries_sum{endpoint="delivery-endpoint"} 36
registry_notifications_retries_count{endpoint="delivery-endpoint"} 7
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
# HELP registry_notifications_delivery The number of events delivered or lost. Event is lost once the number of retries was exhausted.
# TYPE registry_notifications_delivery counter
registry_notifications_delivery{delivery_type="delivered",endpoint="edge-case-endpoint"} 3
registry_notifications_delivery{delivery_type="lost",endpoint="edge-case-endpoint"} 3
# HELP registry_notifications_retries The histogram of delivery retries done
# TYPE registry_notifications_retries histogram
registry_notifications_retries_bucket{endpoint="edge-case-endpoint",le="0"} 2
registry_notifications_retries_bucket{endpoint="edge-case-endpoint",le="1"} 2
registry_notifications_retries_bucket{endpoint="edge-case-endpoint",le="2"} 2
registry_notifications_retries_bucket{endpoint="edge-case-endpoint",le="3"} 2
registry_notifications_retries_bucket{endpoint="edge-case-endpoint",le="5"} 2
registry_notifications_retries_bucket{endpoint="edge-case-endpoint",le="10"} 2
registry_notifications_retries_bucket{endpoint="edge-case-endpoint",le="15"} 2
registry_notifications_retries_bucket{endpoint="edge-case-endpoint",le="20"} 2
registry_notifications_retries_bucket{endpoint="edge-case-endpoint",le="30"} 2
registry_notifications_retries_bucket{endpoint="edge-case-endpoint",le="50"} 4
registry_notifications_retries_bucket{endpoint="edge-case-endpoint",le="+Inf"} 6
registry_notifications_retries_sum{endpoint="edge-case-endpoint"} 300
registry_notifications_retries_count{endpoint="edge-case-endpoint"} 6
`)
	require.NoError(t, err)

	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, deliveryCounterName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, retriesName),
	}

	err = testutil.GatherAndCompare(registry, &expected, names...)
	require.NoError(t, err)
}

func TestEndpointMetricsIntegration(t *testing.T) {
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
