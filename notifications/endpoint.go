package notifications

import (
	"net/http"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/docker/distribution/configuration"
)

// DefaultQueueSizeLimit defines the default limit for the queue size. Once
// reached the events will start being dropped.
// NOTE(prozlach): The average event size in memory is around 3000 bytes, and
// in production, the queue does not normally go higher that 150 on a single
// pod so this should give enough headroom for most of the cases at the expense
// of around 8.5 MiB of RAM.
const DefaultQueueSizeLimit = 3000

// DefaultQueuePurgeTimeout is the default time the queue tries to deliver
// remaining notifications before shutting down.
// NOTE(prozlach): Value chosen arbitrary. Intention was to make registry try
// to deliver as many notifications as possible, while still being under the
// threshold which e.g. Kubernetes uses to determine when to start SIGKILL pod
// that does not stop after SIGINT. NOTE(prozlach): There is no delivery
// guarantee for notifications ATM, this is best effort.
const DefaultQueuePurgeTimeout = 5 * time.Second

// EndpointConfig covers the optional configuration parameters for an active
// endpoint.
type EndpointConfig struct {
	Headers http.Header
	Timeout time.Duration
	// Deprecated: use MaxRetries instead https://gitlab.com/gitlab-org/container-registry/-/issues/1243
	Threshold         int
	MaxRetries        int
	Backoff           time.Duration
	IgnoredMediaTypes []string
	Transport         *http.Transport `json:"-"`
	Ignore            configuration.Ignore
	QueuePurgeTimeout time.Duration
	QueueSizeLimit    int
}

// defaults set any zero-valued fields to a reasonable default.
func (ec *EndpointConfig) defaults() {
	if ec.Timeout <= 0 {
		ec.Timeout = time.Second
	}

	if ec.Threshold <= 0 {
		ec.Threshold = 10
	}

	if ec.Backoff <= 0 {
		ec.Backoff = time.Second
	}

	if ec.QueuePurgeTimeout <= 0 {
		ec.QueuePurgeTimeout = DefaultQueuePurgeTimeout
	}

	if ec.QueueSizeLimit <= 0 {
		ec.QueueSizeLimit = DefaultQueueSizeLimit
	}

	if ec.Transport == nil {
		ec.Transport = http.DefaultTransport.(*http.Transport)
	}
}

// Endpoint is a reliable, queued, thread-safe sink that notify external http
// services when events are written. Writes are non-blocking and always
// succeed for callers but events may be queued internally.
type Endpoint struct {
	Sink
	url  string
	name string

	EndpointConfig

	metrics *safeMetrics
}

// NewEndpoint returns a running endpoint, ready to receive events.
func NewEndpoint(name, url string, config EndpointConfig) *Endpoint {
	var endpoint Endpoint
	endpoint.name = name
	endpoint.url = url
	endpoint.EndpointConfig = config
	endpoint.defaults()
	endpoint.metrics = newSafeMetrics(name)

	// Configures the inmemory queue, retry, http pipeline.
	endpoint.Sink = newHTTPSink(
		endpoint.url, endpoint.Timeout, endpoint.Headers,
		endpoint.Transport, endpoint.metrics.httpStatusListener())

	// TODO: threshold has been deprecated and we should use MaxRetries with backoffSink instead.
	// Remove this check along with https://gitlab.com/gitlab-org/container-registry/-/issues/1244.
	if endpoint.MaxRetries != 0 {
		endpoint.Sink = newBackoffSink(
			endpoint.Sink, endpoint.Backoff, endpoint.MaxRetries,
			endpoint.metrics.deliveryListener(),
		)
	} else {
		log.Warn("notifications `threshold` is deprecated, use maxretries instead. See https://gitlab.com/gitlab-org/container-registry/-/issues/1243.")
		endpoint.Sink = newRetryingSink(
			endpoint.Sink, endpoint.Threshold, endpoint.Backoff,
			endpoint.metrics.deliveryListener(),
		)
	}

	endpoint.Sink = newEventQueue(
		endpoint.Sink,
		endpoint.QueuePurgeTimeout,
		endpoint.QueueSizeLimit,
		endpoint.metrics.eventQueueListener(),
	)
	mediaTypes := make([]string, len(config.Ignore.MediaTypes), len(config.Ignore.MediaTypes)+len(config.IgnoredMediaTypes))
	copy(mediaTypes, config.Ignore.MediaTypes)
	mediaTypes = append(mediaTypes, config.IgnoredMediaTypes...)
	endpoint.Sink = newIgnoredSink(endpoint.Sink, mediaTypes, config.Ignore.Actions)

	register(&endpoint)
	return &endpoint
}

// Name returns the name of the endpoint, generally used for debugging.
func (e *Endpoint) Name() string {
	return e.name
}

// URL returns the url of the endpoint.
func (e *Endpoint) URL() string {
	return e.url
}

// ReadMetrics populates em with metrics from the endpoint.
func (e *Endpoint) ReadMetrics(em *EndpointMetrics) {
	em.Endpoint = e.metrics.endpoint
	em.Pending = e.metrics.pending.Load()
	em.Events = e.metrics.events.Load()
	em.Successes = e.metrics.successes.Load()
	em.Failures = e.metrics.failures.Load()
	em.Errors = e.metrics.errors.Load()
	em.Retries = e.metrics.retries.Load()
	em.Delivered = e.metrics.delivered.Load()
	em.Dropped = e.metrics.dropped.Load()
	em.Lost = e.metrics.lost.Load()

	// Map still need to copied in a threadsafe manner.
	em.Statuses = make(map[string]int64)
	e.metrics.statuses.Range(func(k, v any) bool {
		em.Statuses[k.(string)] = v.(*atomic.Int64).Load()

		return true
	})
}
