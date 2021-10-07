package gc

import (
	"context"
	"io"
	"math/rand"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/cenkalti/backoff/v4"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/gc/internal"
	"github.com/docker/distribution/registry/gc/internal/metrics"
	"github.com/docker/distribution/registry/gc/worker"
	reginternal "github.com/docker/distribution/registry/internal"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/labkit/correlation"
)

const (
	componentKey = "component"
	agentName    = "registry.gc.Agent"
)

var (
	defaultInitialInterval = 5 * time.Second
	defaultMaxBackoff      = 24 * time.Hour
	backoffJitterFactor    = 0.33
	startJitterMaxSeconds  = 60

	// for testing purposes (mocks)
	backoffConstructor                   = newBackoff
	systemClock        reginternal.Clock = clock.New()
	newCorrelationID                     = correlation.SafeRandomID
)

// Agent manages a online garbage collection worker.
type Agent struct {
	worker          worker.Worker
	logger          log.Logger
	initialInterval time.Duration
	maxBackoff      time.Duration
	noIdleBackoff   bool
}

// AgentOption provides functional options for NewAgent.
type AgentOption func(*Agent)

// WithLogger sets the logger.
func WithLogger(l log.Logger) AgentOption {
	return func(a *Agent) {
		a.logger = l
	}
}

// WithInitialInterval sets the initial interval between worker runs. Defaults to 5 seconds.
func WithInitialInterval(d time.Duration) AgentOption {
	return func(a *Agent) {
		a.initialInterval = d
	}
}

// WithMaxBackoff sets the maximum exponential back off duration used to sleep between worker runs when an error occurs.
// It is also applied when there are no tasks to be processed, unless WithoutIdleBackoff is provided. Please note that
// this is not the absolute maximum, as a randomized jitter factor of up to 33% is always added. Defaults to 24 hours.
func WithMaxBackoff(d time.Duration) AgentOption {
	return func(a *Agent) {
		a.maxBackoff = d
	}
}

// WithoutIdleBackoff disables exponential back offs between worker runs when there are no task to be processed.
func WithoutIdleBackoff() AgentOption {
	return func(a *Agent) {
		a.noIdleBackoff = true
	}
}

func (a *Agent) applyDefaults() {
	if a.logger == nil {
		defaultLogger := logrus.New()
		defaultLogger.SetOutput(io.Discard)
		a.logger = log.FromLogrusLogger(defaultLogger)
	}
	if a.initialInterval == 0 {
		a.initialInterval = defaultInitialInterval
	}
	if a.maxBackoff == 0 {
		a.maxBackoff = defaultMaxBackoff
	}
}

// NewAgent creates a new Agent.
func NewAgent(w worker.Worker, opts ...AgentOption) *Agent {
	a := &Agent{worker: w}
	a.applyDefaults()

	for _, opt := range opts {
		opt(a)
	}

	a.logger = a.logger.WithFields(log.Fields{componentKey: agentName})

	return a
}

// Start starts the Agent. This is a blocking call that runs the worker in a loop. The loop can be stopped if the
// provided context is canceled. Each worker run is separate by an initial sleep interval (configured through
// WithInitialInterval) with an additional exponential back off up to a given limit (configured through WithMaxBackoff).
// The exponential back off is incremented after every failed run or when no task was found (unless
// WithoutIdleBackoff was provided). The sleep interval is reset to the initial value (removing the exponential back off
// delay) after every successful run, unless no task was found and WithoutIdleBackoff was not provided. The Agent starts
// with a randomized jitter of up to 60 seconds to ease concurrency in clustered environments.
func (a *Agent) Start(ctx context.Context) error {
	l := a.logger.WithFields(log.Fields{"worker": a.worker.Name()})
	b := backoffConstructor(a.initialInterval, a.maxBackoff)

	rand.Seed(systemClock.Now().UnixNano())
	/* #nosec G404 */
	jitter := time.Duration(rand.Intn(startJitterMaxSeconds)) * time.Second
	l.WithFields(log.Fields{"jitter_s": jitter.Seconds()}).Info("starting online GC agent")
	systemClock.Sleep(jitter)

	for {
		select {
		case <-ctx.Done():
			l.Warn("context cancelled, exiting")
			return ctx.Err()
		default:
			start := systemClock.Now()

			id := newCorrelationID()
			wCtx := correlation.ContextWithCorrelation(ctx, id)
			l := l.WithFields(log.Fields{correlation.FieldName: id})

			l.Info("running worker")

			report := metrics.WorkerRun(a.worker.Name())
			found, err := a.worker.Run(wCtx)
			if err != nil {
				l.WithError(err).Error("failed run")
			} else if found || a.noIdleBackoff {
				b.Reset()
			}
			report(!found, err)
			l.WithFields(log.Fields{"duration_s": systemClock.Since(start).Seconds()}).Info("run complete")

			sleep := b.NextBackOff()
			l.WithFields(log.Fields{"duration_s": sleep.Seconds()}).Info("sleeping")
			metrics.WorkerSleep(a.worker.Name(), sleep)
			systemClock.Sleep(sleep)
		}
	}
}

func newBackoff(initInterval, maxInterval time.Duration) internal.Backoff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = initInterval
	b.MaxInterval = maxInterval
	b.RandomizationFactor = backoffJitterFactor
	b.MaxElapsedTime = 0
	b.Clock = systemClock
	b.Reset()

	return b
}
