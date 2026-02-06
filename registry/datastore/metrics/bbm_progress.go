package metrics

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bsm/redislock"
	dlog "github.com/docker/distribution/log"
	"github.com/docker/distribution/metrics"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/internal/errorreporting"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const (
	// Lock key for distributed coordination
	bbmProgressLockKey = "registry:db:{metrics}:bbm_progress_lock"
)

var bbmProgressGauge *prometheus.GaugeVec

func init() {
	bbmProgressGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      "bbm_progress_percent",
			Help:      "Background migration progress percentage (0-100).",
		},
		[]string{"migration_id", "migration_name", "status"},
	)
}

// BBMProgressExecutor runs a query and returns rows for scanning.
type BBMProgressExecutor func(ctx context.Context) ([]*models.BackgroundMigrationProgress, error)

// BBMProgressCollector periodically collects BBM progress metrics with distributed locking.
type BBMProgressCollector struct {
	executor         BBMProgressExecutor
	locker           *redislock.Client
	leaseDuration    time.Duration
	interval         time.Duration
	metricsRegistrar *Registrar
	stopCh           chan struct{}
	wg               sync.WaitGroup
	logger           dlog.Logger
}

// ProgressOption configures BBMProgressCollector creation
type ProgressOption func(*BBMProgressCollector)

// WithProgressInterval sets the collection interval (default: 10s)
func WithProgressInterval(interval time.Duration) ProgressOption {
	return func(c *BBMProgressCollector) {
		c.interval = interval
	}
}

// WithProgressLeaseDuration sets the distributed lock lease duration (default: 30s)
func WithProgressLeaseDuration(leaseDuration time.Duration) ProgressOption {
	return func(c *BBMProgressCollector) {
		c.leaseDuration = leaseDuration
	}
}

// NewBBMProgressCollector creates a new collector with defaults.
func NewBBMProgressCollector(executor BBMProgressExecutor, redisClient redis.UniversalClient, opts ...ProgressOption) (*BBMProgressCollector, error) {
	c := &BBMProgressCollector{
		executor:         executor,
		locker:           redislock.New(redisClient),
		leaseDuration:    defaultLeaseDuration,
		interval:         defaultInterval,
		metricsRegistrar: NewRegistrar(bbmProgressGauge),
		stopCh:           make(chan struct{}),
		logger:           dlog.GetLogger(),
	}

	// Apply options to override defaults
	for _, opt := range opts {
		opt(c)
	}

	// Validate config
	if c.leaseDuration <= c.interval {
		return nil, fmt.Errorf("bbm metrics lease duration (%v) must be longer than interval (%v)", c.leaseDuration, c.interval)
	}

	return c, nil
}

// Start begins periodic collection.
func (c *BBMProgressCollector) Start(ctx context.Context) {
	c.wg.Add(1)
	go c.run(ctx)
	c.logger.WithFields(dlog.Fields{
		"interval_s":       c.interval.Seconds(),
		"lease_duration_s": c.leaseDuration.Seconds(),
	}).Info("bbm progress metrics collection started")
}

// Stop gracefully stops collection.
func (c *BBMProgressCollector) Stop() {
	close(c.stopCh)
	c.wg.Wait()
}

func (c *BBMProgressCollector) run(ctx context.Context) {
	defer c.wg.Done()

	// Ensure metrics are registered
	if err := c.metricsRegistrar.Register(); err != nil {
		c.logger.WithError(err).Error("failed to register bbm progress metrics")
		errorreporting.Capture(ctx, fmt.Errorf("bbm progress metrics: failed to register metrics: %w", err))
		return
	}
	defer c.metricsRegistrar.Unregister()

	// Try to acquire the lock periodically
	ticker := time.NewTicker(lockRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		lock, err := c.locker.Obtain(ctx, bbmProgressLockKey, c.leaseDuration, nil)
		if err != nil {
			if !errors.Is(err, redislock.ErrNotObtained) {
				c.logger.WithError(err).Error("failed to obtain bbm progress lock")
			}
			select {
			case <-c.stopCh:
				return
			case <-ticker.C:
				continue
			}
		}

		// Log leadership acquisition
		c.logger.Info("obtained bbm progress metrics lock")

		// We are leader; collect until lock expires or stop
		collectionTicker := time.NewTicker(c.interval)
		lockRefreshTicker := time.NewTicker(c.leaseDuration / 2)

		c.collect(ctx)
		for {
			select {
			case <-c.stopCh:
				// nolint:revive // max-control-nesting - acceptable for error handling logic
				if err := lock.Release(ctx); err != nil {
					c.logger.WithError(err).Error("failed to release bbm progress lock on stop")
				}
				collectionTicker.Stop()
				lockRefreshTicker.Stop()
				return
			case <-collectionTicker.C:
				c.collect(ctx)
			case <-lockRefreshTicker.C:
				// nolint:revive // max-control-nesting - acceptable for error handling logic
				if err := lock.Refresh(ctx, c.leaseDuration, nil); err != nil {
					c.releaseLockWithLog(ctx, lock, "failed to refresh bbm progress lock; releasing leadership")
					collectionTicker.Stop()
					lockRefreshTicker.Stop()
					goto retry
				}
			}
		}
	retry:
		continue
	}
}

func (c *BBMProgressCollector) collect(ctx context.Context) {
	progressItems, err := c.executor(ctx)
	if err != nil {
		c.logger.WithError(err).Error("failed to fetch bbm progress rows")
		return
	}

	for _, v := range progressItems {
		c.logger.WithFields(dlog.Fields{
			"capped":            v.Capped,
			"migration_id":      v.MigrationId,
			"migration_name":    v.MigrationName,
			"bbm_status":        v.Status,
			"batch_size":        v.BatchSize,
			"finished_jobs":     v.FinishedJobs,
			"total_tuple_count": v.TotalTupleCount,
			"progress_percent":  v.Progress,
		}).Info("bbm progress")

		bbmProgressGauge.
			WithLabelValues(fmt.Sprint(v.MigrationId), v.MigrationName, v.Status).
			Set(v.Progress)
	}
}

// releaseLockWithLog releases the distributed lock and logs both refresh and release errors.
func (c *BBMProgressCollector) releaseLockWithLog(ctx context.Context, lock *redislock.Lock, refreshMsg string) {
	c.logger.Error(refreshMsg)
	if err := lock.Release(ctx); err != nil {
		c.logger.WithError(err).Error("failed to release bbm progress lock after refresh failure")
	}
}
