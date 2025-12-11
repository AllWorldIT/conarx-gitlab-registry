package notifications

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
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
// that does not stop after SIGINT. We currently use the default of 30s on
// gprd. Reference:
//
//	https://gitlab.com/gitlab-org/charts/gitlab/-/blob/9f66fdb4dc9d05b3699703551cf14fe7e7dfaa56/doc/charts/registry/_index.md#L172-L172
//
// NOTE(prozlach): There is no delivery guarantee for notifications ATM, this
// is best effort.
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

// translateBackoffParams translates old backoff parameters (threshold, backoff)
// into new parameters (maxretries, backoff) based on a time window calculation.
//
// The time window scale factor (120s for 1s backoff) was arbitrarily chosen based on:
//   - GitLab's production configuration for their large SaaS installation (1s backoff, 10 max retries)
//   - The necessity to limit retry attempts to avoid head-of-line blocking
//   - The need to prevent infinite retries which would ultimately lead to dropped notifications
//     anyway, as the registry will start dropping events to protect itself from RAM exhaustion
//
// Parameters:
//   - threshold: number of immediate retries in the old system (used as minimum for maxretries)
//   - backoff: base backoff duration in the old system, becomes InitialInterval in the new system
//
// Returns:
//   - maxretries: calculated number of maximum retries for the new system (at least threshold)
func translateBackoffParams(threshold int, backoffTime time.Duration) int {
	// Calculate time window: 120s for 1s backoff, scales linearly
	// timeWindow = 120 * backoff_in_seconds
	timeWindow := 120 * backoffTime

	// Cap time window at DefaultMaxElapsedTime
	if timeWindow > backoff.DefaultMaxElapsedTime {
		timeWindow = backoff.DefaultMaxElapsedTime
	}

	// Calculate maxretries using reverse calculation of backoff algorithm
	// Assumes RandomizationFactor = 0 for predictable calculations
	currentInterval := backoffTime
	cumulativeTime := time.Duration(0)
	retryCount := 0

	// Simulate the backoff algorithm until we exceed the time window
	for cumulativeTime < timeWindow {
		// Add the delay to cumulative time
		cumulativeTime += currentInterval

		// If we've exceeded the time window, don't count this retry
		if cumulativeTime > timeWindow {
			break
		}

		retryCount++

		// Calculate next interval using the same logic as the library
		currentInterval = time.Duration(float64(currentInterval) * backoff.DefaultMultiplier)
		if currentInterval > backoff.DefaultMaxInterval {
			currentInterval = backoff.DefaultMaxInterval
		}
	}

	if retryCount < threshold {
		return threshold
	}
	return retryCount
}

// defaults set any zero-valued fields to a reasonable default.
func (ec *EndpointConfig) defaults() {
	if ec.Timeout <= 0 {
		ec.Timeout = time.Second
	}

	if ec.Backoff <= 0 {
		ec.Backoff = time.Second
	}

	if ec.MaxRetries == 0 {
		// NOTE(prozlach): in order to keep old behavior intact if possible,
		// if maxRetries is not defined, we check if threshold is. If not - we
		// assume defaults for maxRetries. If yes - we translate threshold to
		// maxRetries.
		if ec.Threshold <= 0 {
			log.Info("defaulting maxRetries parameter to 10")
			ec.MaxRetries = 10
		} else {
			ec.MaxRetries = translateBackoffParams(ec.Threshold, ec.Backoff)
			log.Warnf(
				"notifications `threshold` is deprecated, please use `maxretries` instead. "+
					"Value `threshold` of %d has been converted to `maxretries` of %d. "+
					"See https://gitlab.com/gitlab-org/container-registry/-/issues/1243 for more details.",
				ec.Threshold, ec.MaxRetries,
			)
		}
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
		endpoint.url, endpoint.Timeout, endpoint.Headers, endpoint.Transport, endpoint.metrics.httpStatusListener(),
	)

	endpoint.Sink = newBackoffSink(
		endpoint.Sink, endpoint.Backoff, endpoint.MaxRetries, endpoint.metrics.deliveryListener(),
	)

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
