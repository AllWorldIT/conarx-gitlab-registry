package notifications

import (
	"expvar"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/docker/distribution/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	eventsCounter   *prometheus.CounterVec
	pendingGauge    *prometheus.GaugeVec
	statusCounter   *prometheus.CounterVec
	errorCounter    *prometheus.CounterVec
	deliveryCounter *prometheus.CounterVec
	retriesHist     *prometheus.HistogramVec
)

const (
	subsystem     = "notifications"
	endpointLabel = "endpoint"

	// Events counter
	eventsCounterName   = "events"
	eventsCounterDesc   = "The total number of events"
	eventsTypeLabel     = "type"
	eventsActionLabel   = "action"
	eventsArtifactLabel = "artifact"

	// Pending gauge
	pendingGaugeName = "pending"
	pendingGaugeDesc = "The gauge of pending events in queue - queue length"

	// Status counter
	statusCounterName = "status"
	statusCounterDesc = "The number HTTP responses per status code received from notifications endpoint"
	statusCodeLabel   = "code"

	// Error counter
	errorCounterName = "errors"
	errorCounterDesc = "The number of events where an error occurred during sending. Sending them MAY be retried."

	// Message lost counter
	deliveryCounterName = "delivery"
	deliveryCounterDesc = "The number of events delivered or lost. Event is lost once the number of retries was exhausted."
	deliveryTypeLabel   = "delivery_type"

	// Retries Histogram
	retriesName = "retries"
	retriesDesc = "The histogram of delivery retries done"
)

func registerMetrics(registerer prometheus.Registerer) {
	retryBuckets := []float64{0, 1, 2, 3, 5, 10, 15, 20, 30, 50}

	eventsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      eventsCounterName,
			Help:      eventsCounterDesc,
		},
		[]string{eventsTypeLabel, eventsActionLabel, eventsArtifactLabel, endpointLabel},
	)

	pendingGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      pendingGaugeName,
			Help:      pendingGaugeDesc,
		},
		[]string{endpointLabel},
	)

	statusCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      statusCounterName,
			Help:      statusCounterDesc,
		},
		[]string{statusCodeLabel, endpointLabel},
	)

	errorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      errorCounterName,
			Help:      errorCounterDesc,
		},
		[]string{endpointLabel},
	)

	deliveryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      deliveryCounterName,
			Help:      deliveryCounterDesc,
		},
		[]string{endpointLabel, deliveryTypeLabel},
	)

	retriesHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      retriesName,
			Help:      retriesDesc,
			Buckets:   retryBuckets,
		},
		[]string{endpointLabel},
	)

	registerer.MustRegister(eventsCounter)
	registerer.MustRegister(pendingGauge)
	registerer.MustRegister(statusCounter)
	registerer.MustRegister(errorCounter)
	registerer.MustRegister(retriesHist)
	registerer.MustRegister(deliveryCounter)
}

// EndpointMetrics track various actions taken by the endpoint, typically by
// number of events. The goal of this to export it via expvar but we may find
// some other future solution to be better.
type EndpointMetrics struct {
	Endpoint  string           // endpoint name to be added to the metrics
	Pending   int64            // events pending in queue
	Events    int64            // total events incoming
	Successes int64            // total events written successfully
	Failures  int64            // total events failed
	Errors    int64            // total events errored
	Retries   int64            // total number of retries done
	Delivered int64            // total number of delivered events
	Dropped   int64            // total number of dropped events
	Lost      int64            // total number of lost events
	Statuses  map[string]int64 // status code histogram, per call event
}

// safeMetrics guards the metrics implementation with a lock and provides a
// safe update function.
type safeMetrics struct {
	endpoint  string
	pending   *atomic.Int64
	events    *atomic.Int64
	successes *atomic.Int64
	failures  *atomic.Int64
	errors    *atomic.Int64
	retries   *atomic.Int64
	delivered *atomic.Int64
	dropped   *atomic.Int64
	lost      *atomic.Int64
	statuses  *sync.Map
}

// newSafeMetrics returns safeMetrics with map allocated.
func newSafeMetrics(endpoint string) *safeMetrics {
	var sm safeMetrics
	sm.endpoint = endpoint
	sm.pending = new(atomic.Int64)
	sm.events = new(atomic.Int64)
	sm.successes = new(atomic.Int64)
	sm.failures = new(atomic.Int64)
	sm.errors = new(atomic.Int64)
	sm.retries = new(atomic.Int64)
	sm.delivered = new(atomic.Int64)
	sm.dropped = new(atomic.Int64)
	sm.lost = new(atomic.Int64)
	sm.statuses = new(sync.Map)
	return &sm
}

// httpStatusListener returns the listener for the http sink that updates the
// relevant counters.
func (sm *safeMetrics) httpStatusListener() httpStatusListener {
	return &endpointMetricsHTTPStatusListener{
		safeMetrics: sm,
	}
}

// eventQueueListener returns a listener that maintains queue related counters.
func (sm *safeMetrics) eventQueueListener() eventQueueListener {
	return &endpointMetricsEventQueueListener{
		safeMetrics: sm,
	}
}

// eventQueueListener returns a listener that maintains queue related counters.
func (sm *safeMetrics) deliveryListener() deliveryListener {
	return &endpointMetricsDeliveryListener{
		safeMetrics: sm,
	}
}

// endpointMetricsHTTPStatusListener increments counters related to http sinks
// for the relevant events.
type endpointMetricsHTTPStatusListener struct {
	*safeMetrics
}

var _ httpStatusListener = new(endpointMetricsHTTPStatusListener)

func (emsl *endpointMetricsHTTPStatusListener) success(status int, event *Event) {
	key := fmt.Sprintf("%d %s", status, http.StatusText(status))
	actual, _ := emsl.statuses.LoadOrStore(key, new(atomic.Int64))
	actual.(*atomic.Int64).Add(1)
	emsl.successes.Add(1)

	statusCounter.WithLabelValues(key, emsl.endpoint).Inc()
	eventsCounter.WithLabelValues("Successes", event.Action, event.artifact(), emsl.endpoint).Inc()
}

func (emsl *endpointMetricsHTTPStatusListener) failure(status int, event *Event) {
	key := fmt.Sprintf("%d %s", status, http.StatusText(status))
	actual, _ := emsl.statuses.LoadOrStore(key, new(atomic.Int64))
	actual.(*atomic.Int64).Add(1)
	emsl.failures.Add(1)

	statusCounter.WithLabelValues(key, emsl.endpoint).Inc()
	eventsCounter.WithLabelValues("Failures", event.Action, event.artifact(), emsl.endpoint).Inc()
}

func (emsl *endpointMetricsHTTPStatusListener) err(_ *Event) {
	emsl.errors.Add(1)

	errorCounter.WithLabelValues(emsl.endpoint).Inc()
}

// endpointMetricsDeliveryListener maintains the incoming events counter and
// the queues pending count.
type endpointMetricsDeliveryListener struct {
	*safeMetrics
}

var _ deliveryListener = new(endpointMetricsDeliveryListener)

func (edl *endpointMetricsDeliveryListener) eventDelivered(retriesCount int64) {
	edl.retries.Add(retriesCount)
	edl.delivered.Add(1)

	retriesHist.WithLabelValues(edl.endpoint).Observe(float64(retriesCount))
	deliveryCounter.WithLabelValues(edl.endpoint, "delivered").Inc()
}

func (edl *endpointMetricsDeliveryListener) eventLost(retriesCount int64) {
	edl.retries.Add(retriesCount)
	edl.lost.Add(1)

	retriesHist.WithLabelValues(edl.endpoint).Observe(float64(retriesCount))
	deliveryCounter.WithLabelValues(edl.endpoint, "lost").Inc()
}

// endpointMetricsEventQueueListener maintains the incoming events counter and
// the queues pending count.
type endpointMetricsEventQueueListener struct {
	*safeMetrics
}

var _ eventQueueListener = new(endpointMetricsEventQueueListener)

func (eqc *endpointMetricsEventQueueListener) ingress(event *Event) {
	eqc.events.Add(1)
	eqc.pending.Add(1)

	eventsCounter.WithLabelValues("Events", event.Action, event.artifact(), eqc.endpoint).Inc()
	pendingGauge.WithLabelValues(eqc.endpoint).Inc()
}

func (eqc *endpointMetricsEventQueueListener) egress(_ *Event) {
	eqc.pending.Add(-1)

	pendingGauge.WithLabelValues(eqc.endpoint).Dec()
}

func (eqc *endpointMetricsEventQueueListener) drop(event *Event) {
	eqc.dropped.Add(1)
	eqc.pending.Add(-1)

	eventsCounter.WithLabelValues("Dropped", event.Action, event.artifact(), eqc.endpoint).Inc()
}

// endpoints is global registry of endpoints used to report metrics to expvar
var endpoints struct {
	registered []*Endpoint
	mu         sync.Mutex
}

// register places the endpoint into expvar so that stats are tracked.
func register(e *Endpoint) {
	endpoints.mu.Lock()
	endpoints.registered = append(endpoints.registered, e)
	endpoints.mu.Unlock()
}

func init() {
	// NOTE(stevvooe): Setup registry metrics structure to report to expvar.
	// Ideally, we do more metrics through logging but we need some nice
	// realtime metrics for queue state for now.

	registry := expvar.Get("registry")

	if registry == nil {
		registry = expvar.NewMap("registry")
	}

	var notifications expvar.Map
	notifications.Init()
	notifications.Set("endpoints", expvar.Func(func() any {
		endpoints.mu.Lock()
		defer endpoints.mu.Unlock()

		var names []any
		for _, v := range endpoints.registered {
			var epjson struct {
				Name string `json:"name"`
				URL  string `json:"url"`
				EndpointConfig

				Metrics EndpointMetrics
			}

			epjson.Name = v.Name()
			epjson.URL = v.URL()
			epjson.EndpointConfig = v.EndpointConfig

			v.ReadMetrics(&epjson.Metrics)

			names = append(names, epjson)
		}

		return names
	}))

	registry.(*expvar.Map).Set("notifications", &notifications)

	// NOTE(prozlach): functions are split in order to make this code more
	// testable. This requires some bigger refactoring though.
	registerMetrics(prometheus.DefaultRegisterer)
}
