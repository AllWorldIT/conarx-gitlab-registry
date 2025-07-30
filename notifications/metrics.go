package notifications

import (
	"expvar"
	"fmt"
	"net/http"
	"sync"

	"github.com/docker/distribution/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	eventsCounter *prometheus.CounterVec
	pendingGauge  prometheus.Gauge
	statusCounter *prometheus.CounterVec
	errorCounter  *prometheus.CounterVec
)

const (
	subsystem = "notifications"

	// Events counter
	eventsCounterName   = "events"
	eventsCounterDesc   = "The number of total events"
	eventsTypeLabel     = "type"
	eventsActionLabel   = "action"
	eventsArtifactLabel = "artifact"
	eventsEndpointLabel = "endpoint"

	// Pending gauge
	pendingGaugeName = "pending"
	pendingGaugeDesc = "The gauge of pending events in queue"

	// Status counter
	statusCounterName = "status"
	statusCounterDesc = "The number of status code"
	statusCodeLabel   = "code"

	// Error counter
	errorCounterName   = "errors"
	errorCounterDesc   = "The number of events that were not sent due to internal errors"
	errorEndpointLabel = "endpoint"
)

func registerMetrics(registerer prometheus.Registerer) {
	eventsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      eventsCounterName,
			Help:      eventsCounterDesc,
		},
		[]string{eventsTypeLabel, eventsActionLabel, eventsArtifactLabel, eventsEndpointLabel},
	)

	pendingGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      pendingGaugeName,
			Help:      pendingGaugeDesc,
		},
	)

	statusCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      statusCounterName,
			Help:      statusCounterDesc,
		},
		[]string{statusCodeLabel},
	)

	errorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      errorCounterName,
			Help:      errorCounterDesc,
		},
		[]string{errorEndpointLabel},
	)

	registerer.MustRegister(eventsCounter)
	registerer.MustRegister(pendingGauge)
	registerer.MustRegister(statusCounter)
	registerer.MustRegister(errorCounter)
}

// EndpointMetrics track various actions taken by the endpoint, typically by
// number of events. The goal of this to export it via expvar but we may find
// some other future solution to be better.
type EndpointMetrics struct {
	Endpoint  string         // endpoint name to be added to the metrics
	Pending   int            // events pending in queue
	Events    int            // total events incoming
	Successes int            // total events written successfully
	Failures  int            // total events failed
	Errors    int            // total events errored
	Statuses  map[string]int // status code histogram, per call event
}

// safeMetrics guards the metrics implementation with a lock and provides a
// safe update function.
type safeMetrics struct {
	EndpointMetrics
	sync.Mutex // protects statuses map
}

// newSafeMetrics returns safeMetrics with map allocated.
func newSafeMetrics(endpoint string) *safeMetrics {
	var sm safeMetrics
	sm.Endpoint = endpoint
	sm.Statuses = make(map[string]int)
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

// endpointMetricsHTTPStatusListener increments counters related to http sinks
// for the relevant events.
type endpointMetricsHTTPStatusListener struct {
	*safeMetrics
}

var _ httpStatusListener = new(endpointMetricsHTTPStatusListener)

func (emsl *endpointMetricsHTTPStatusListener) success(status int, event *Event) {
	emsl.safeMetrics.Lock()
	emsl.Statuses[fmt.Sprintf("%d %s", status, http.StatusText(status))]++
	emsl.Successes++
	emsl.safeMetrics.Unlock()

	statusCounter.WithLabelValues(fmt.Sprintf("%d %s", status, http.StatusText(status))).Inc()
	eventsCounter.WithLabelValues("Successes", event.Action, event.artifact(), emsl.Endpoint).Inc()
}

func (emsl *endpointMetricsHTTPStatusListener) failure(status int, event *Event) {
	emsl.safeMetrics.Lock()
	emsl.Statuses[fmt.Sprintf("%d %s", status, http.StatusText(status))]++
	emsl.Failures++
	emsl.safeMetrics.Unlock()

	statusCounter.WithLabelValues(fmt.Sprintf("%d %s", status, http.StatusText(status))).Inc()
	eventsCounter.WithLabelValues("Failures", event.Action, event.artifact(), emsl.Endpoint).Inc()
}

func (emsl *endpointMetricsHTTPStatusListener) err(_ *Event) {
	emsl.safeMetrics.Lock()
	emsl.Errors++
	emsl.safeMetrics.Unlock()

	errorCounter.WithLabelValues(emsl.Endpoint).Inc()
}

// endpointMetricsEventQueueListener maintains the incoming events counter and
// the queues pending count.
type endpointMetricsEventQueueListener struct {
	*safeMetrics
}

var _ eventQueueListener = new(endpointMetricsEventQueueListener)

func (eqc *endpointMetricsEventQueueListener) ingress(event *Event) {
	eqc.Lock()
	eqc.Events++
	eqc.Pending++
	eqc.Unlock()

	eventsCounter.WithLabelValues("Events", event.Action, event.artifact(), eqc.Endpoint).Inc()
	pendingGauge.Inc()
}

func (eqc *endpointMetricsEventQueueListener) egress(_ *Event) {
	eqc.Lock()
	eqc.Pending--
	eqc.Unlock()

	pendingGauge.Dec()
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
