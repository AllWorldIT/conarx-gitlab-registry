package metrics

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bsm/redislock"
	dlog "github.com/docker/distribution/log"
	"github.com/docker/distribution/metrics"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"gitlab.com/gitlab-org/labkit/errortracking"
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
type BBMProgressExecutor func(ctx context.Context, query string, args ...any) (*sql.Rows, error)

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
		errortracking.Capture(
			fmt.Errorf("bbm progress metrics: failed to register metrics: %w", err),
			errortracking.WithContext(ctx),
			errortracking.WithStackTrace(),
		)
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
	defer InstrumentQuery("bbm_collect_progress")()
	// Use a single SQL query to get progress inputs per migration
	// status values are stored as ints in the DB; models.BackgroundMigrationFinished indicates finished status
	q := `SELECT
                m.id,
                m.name,
                m.batch_size,
                m.status,
                m.total_tuple_count,
                COALESCE(j.finished_jobs, 0) AS finished_jobs
            FROM
                batched_background_migrations m
                LEFT JOIN (
                    SELECT
                        batched_background_migration_id AS bbm_id,
                        COUNT(*) AS finished_jobs
                    FROM
                        batched_background_migration_jobs
                    WHERE
                        status = $1
                    GROUP BY
                        batched_background_migration_id) j ON j.bbm_id = m.id
            ORDER BY
                m.id`

	rows, err := c.executor(ctx, q, int(models.BackgroundMigrationFinished))
	if err != nil {
		c.logger.WithError(err).Error("failed to fetch bbm progress rows")
		return
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id           int
			name         string
			batchSize    int
			statusInt    int
			total        sql.NullInt64
			finishedJobs int64
		)
		if err := rows.Scan(&id, &name, &batchSize, &statusInt, &total, &finishedJobs); err != nil {
			c.logger.WithError(err).Error("failed to scan bbm progress row")
			continue
		}

		status := models.BackgroundMigrationStatus(statusInt).String()

		// Default: only report progress when total_tuple_count present; finished always 100.
		var (
			progress float64
			capped   bool
		)
		switch models.BackgroundMigrationStatus(statusInt) {
		case models.BackgroundMigrationFinished:
			progress = 100.0
		default:
			p, ok, c := estimateProgress(total, finishedJobs, batchSize)
			if !ok {
				continue
			}
			progress = p
			capped = c
		}

		c.logger.WithFields(dlog.Fields{
			"capped":            capped,
			"migration_id":      id,
			"migration_name":    name,
			"bbm_status":        status,
			"batch_size":        batchSize,
			"finished_jobs":     finishedJobs,
			"total_tuple_count": total.Int64,
			"progress_percent":  progress,
		}).Info("bbm progress")

		bbmProgressGauge.WithLabelValues(fmt.Sprint(id), name, status).Set(progress)
	}

	if err := rows.Err(); err != nil {
		c.logger.WithError(err).Error("error iterating bbm progress rows")
	}
}

// estimateProgress derives progress percent from finished job count and batch size.
// Returns (progress, ok, capped):
//   - ok is false when total is NULL or <= 0, in which case no progress is reported.
//   - progress is min(finishedJobs*batchSize, total)/total * 100.
//   - capped is true when computed progress would reach 100%; progress is capped at 99.9
//     to reserve 100% for explicitly finished migrations.
func estimateProgress(total sql.NullInt64, finishedJobs int64, batchSize int) (float64, bool, bool) {
	capped := false
	if !total.Valid || total.Int64 <= 0 {
		return 0, false, capped
	}
	processed := finishedJobs * int64(batchSize)
	if processed > total.Int64 {
		processed = total.Int64
	}
	progress := (float64(processed) / float64(total.Int64)) * 100.0
	if progress >= 100.0 {
		progress = 99.9
		capped = true
	}
	return progress, true, capped
}

// releaseLockWithLog releases the distributed lock and logs both refresh and release errors.
func (c *BBMProgressCollector) releaseLockWithLog(ctx context.Context, lock *redislock.Lock, refreshMsg string) {
	c.logger.Error(refreshMsg)
	if err := lock.Release(ctx); err != nil {
		c.logger.WithError(err).Error("failed to release bbm progress lock after refresh failure")
	}
}
