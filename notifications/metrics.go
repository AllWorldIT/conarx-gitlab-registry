package notifications

import (
	"expvar"
	"fmt"
	"net/http"
	"sync"

	prometheus "github.com/docker/distribution/metrics"
	"github.com/docker/go-metrics"
)

var (
	// eventsCounter counts total events of incoming, success and failure
	eventsCounter = prometheus.NotificationsNamespace.NewLabeledCounter("events", "The number of total events", "type", "action", "artifact", "endpoint")
	// pendingGauge measures the pending queue size
	pendingGauge = prometheus.NotificationsNamespace.NewGauge("pending", "The gauge of pending events in queue", metrics.Total)
	// statusCounter counts the total notification call per each status code
	statusCounter = prometheus.NotificationsNamespace.NewLabeledCounter("status", "The number of status code", "code")
	// errorCounter counts the total nuymber of events that were not sent due to internal errors
	errorCounter = prometheus.NotificationsNamespace.NewLabeledCounter("errors", "The number of events that were not sent due to internal errors", "endpoint")
)

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

var _ httpStatusListener = &endpointMetricsHTTPStatusListener{}

func (emsl *endpointMetricsHTTPStatusListener) success(status int, event *Event) {
	emsl.safeMetrics.Lock()
	defer emsl.safeMetrics.Unlock()
	emsl.Statuses[fmt.Sprintf("%d %s", status, http.StatusText(status))]++
	emsl.Successes++

	statusCounter.WithValues(fmt.Sprintf("%d %s", status, http.StatusText(status))).Inc(1)
	eventsCounter.WithValues("Successes", event.Action, event.artifact(), emsl.Endpoint).Inc(1)
}

func (emsl *endpointMetricsHTTPStatusListener) failure(status int, event *Event) {
	emsl.safeMetrics.Lock()
	defer emsl.safeMetrics.Unlock()
	emsl.Statuses[fmt.Sprintf("%d %s", status, http.StatusText(status))]++
	emsl.Failures++

	statusCounter.WithValues(fmt.Sprintf("%d %s", status, http.StatusText(status))).Inc(1)
	eventsCounter.WithValues("Failures", event.Action, event.artifact(), emsl.Endpoint).Inc(1)
}

func (emsl *endpointMetricsHTTPStatusListener) err(_ *Event) {
	emsl.safeMetrics.Lock()
	defer emsl.safeMetrics.Unlock()
	emsl.Errors++

	errorCounter.WithValues(emsl.Endpoint).Inc(1)
}

// endpointMetricsEventQueueListener maintains the incoming events counter and
// the queues pending count.
type endpointMetricsEventQueueListener struct {
	*safeMetrics
}

func (eqc *endpointMetricsEventQueueListener) ingress(event *Event) {
	eqc.Lock()
	defer eqc.Unlock()

	eqc.Events++
	eqc.Pending++

	eventsCounter.WithValues("Events", event.Action, event.artifact(), eqc.Endpoint).Inc()
	pendingGauge.Inc(1)
}

func (eqc *endpointMetricsEventQueueListener) egress(_ *Event) {
	eqc.Lock()
	defer eqc.Unlock()
	eqc.Pending--

	pendingGauge.Dec(1)
}

// endpoints is global registry of endpoints used to report metrics to expvar
var endpoints struct {
	registered []*Endpoint
	mu         sync.Mutex
}

// register places the endpoint into expvar so that stats are tracked.
func register(e *Endpoint) {
	endpoints.mu.Lock()
	defer endpoints.mu.Unlock()

	endpoints.registered = append(endpoints.registered, e)
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

	// register prometheus metrics
	metrics.Register(prometheus.NotificationsNamespace)
}
