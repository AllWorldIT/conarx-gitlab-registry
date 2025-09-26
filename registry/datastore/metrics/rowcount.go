package metrics

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/bsm/redislock"
	dlog "github.com/docker/distribution/log"
	"github.com/docker/distribution/metrics"
	"github.com/docker/distribution/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"gitlab.com/gitlab-org/labkit/errortracking"
)

const (
	// Lock key for distributed coordination using CROSSSLOT compatible format. We want this lock key to reside in a
	// single hash slot so that there are no risk of failure due to a distributed slot design. Using a specific key
	// suffix for row count queries to allow for concurrent usage with other metrics (beside row counts) in the future.
	rowCountLockKey = "registry:db:{metrics}:row_count_lock"
	// defaultInterval is the default interval between metrics collection runs
	defaultInterval = 10 * time.Second
	// defaultLeaseDuration is the default duration of the distributed lock lease
	defaultLeaseDuration = 30 * time.Second
	// lockRetryInterval is the fixed interval between lock acquisition attempts
	lockRetryInterval = 15 * time.Second
	// lockExtensionMaxRetries is the number of retries for lock extension failures before giving up leadership
	lockExtensionMaxRetries = 3
)

// RowCountExecutor is a function that executes a database query and returns the count result.
// This allows us to decouple metrics and datastore packages (avoiding import loops) through dependency injection.
type RowCountExecutor func(ctx context.Context, query string, args ...any) (int64, error)

// CollectorOption configures RowCountCollector creation
type CollectorOption func(*RowCountCollector)

// WithInterval sets the collection interval (default: 10s)
func WithInterval(interval time.Duration) CollectorOption {
	return func(c *RowCountCollector) {
		c.interval = interval
	}
}

// WithLeaseDuration sets the distributed lock lease duration (default: 30s)
func WithLeaseDuration(leaseDuration time.Duration) CollectorOption {
	return func(c *RowCountCollector) {
		c.leaseDuration = leaseDuration
	}
}

// RowCountQuery defines a database table to collect row count metrics
type RowCountQuery struct {
	Name        string // Metric name/label for Prometheus
	Description string // Human-readable description
	Query       string // SQL query to execute (should return a single count)
	Args        []any  // Query arguments
}

// RowCountRegistrar manages registration/deregistration of row count metrics.
// Prevents stale metrics when pods lose leadership in distributed environments.
type RowCountRegistrar struct {
	*Registrar
	databaseRows *prometheus.GaugeVec
}

// NewRowCountRegistrar creates a new registrar for row count metrics
func NewRowCountRegistrar() *RowCountRegistrar {
	databaseRows := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      databaseRowsName,
			Help:      databaseRowsDesc,
		},
		[]string{databaseRowsLabel},
	)

	return &RowCountRegistrar{
		Registrar:    NewRegistrar(databaseRows),
		databaseRows: databaseRows,
	}
}

// SetRowCount sets the row count for a specific query
func (r *RowCountRegistrar) SetRowCount(queryName string, count float64) {
	if r.IsRegistered() {
		r.databaseRows.WithLabelValues(queryName).Set(count)
	}
}

// RowCountCollector handles database row count metrics collection with distributed locking
type RowCountCollector struct {
	executor         RowCountExecutor
	metricsRegistrar *RowCountRegistrar
	locker           *redislock.Client
	leaseDuration    time.Duration
	interval         time.Duration
	queries          []RowCountQuery
	stopCh           chan struct{}
	wg               sync.WaitGroup
	logger           dlog.Logger
	mu               sync.RWMutex
}

// NewRowCountCollector creates a new database row count metrics collector with defaults
func NewRowCountCollector(executor RowCountExecutor, redisClient redis.UniversalClient, opts ...CollectorOption) (*RowCountCollector, error) {
	collector := &RowCountCollector{
		executor:         executor,
		metricsRegistrar: NewRowCountRegistrar(),
		locker:           redislock.New(redisClient),
		leaseDuration:    defaultLeaseDuration,
		interval:         defaultInterval,
		queries:          make([]RowCountQuery, 0),
		stopCh:           make(chan struct{}),
		logger:           dlog.GetLogger(),
	}

	// Apply options to override defaults
	for _, opt := range opts {
		opt(collector)
	}

	// Validate configuration - lease duration must be longer than interval
	if collector.leaseDuration <= collector.interval {
		return nil, fmt.Errorf(
			"database metrics lease duration (%v) must be longer than interval (%v)",
			collector.leaseDuration,
			collector.interval,
		)
	}

	// Register queries
	collector.registerQueries()

	return collector, nil
}

// RegisterQuery adds a new row count query to be collected
func (c *RowCountCollector) RegisterQuery(query RowCountQuery) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, query)
}

// registerQueries registers the set of row count queries
func (c *RowCountCollector) registerQueries() {
	c.RegisterQuery(RowCountQuery{
		Name:        "gc_blob_review_queue",
		Description: "Number of rows in gc_blob_review_queue table",
		Query:       "SELECT COUNT(*) FROM gc_blob_review_queue",
		Args:        nil,
	})
}

// Start begins the row count metrics collection process
func (c *RowCountCollector) Start(ctx context.Context) {
	c.wg.Add(1)
	go c.run(ctx)
	c.logger.WithFields(dlog.Fields{
		"interval_s":       c.interval.Seconds(),
		"lease_duration_s": c.leaseDuration.Seconds(),
	}).Info("database row count metrics collection started")
}

// Stop gracefully stops the row count metrics collection
func (c *RowCountCollector) Stop() {
	close(c.stopCh)
	c.wg.Wait()
	c.logger.Info("database row count metrics collection stopped")
}

// unregisterMetrics unregisters row count metrics from Prometheus
func (c *RowCountCollector) unregisterMetrics() {
	c.metricsRegistrar.Unregister()
	c.logger.Info("unregistered row count metrics from Prometheus registry")
}

// registerMetrics registers row count metrics with Prometheus
func (c *RowCountCollector) registerMetrics() error {
	if err := c.metricsRegistrar.Register(); err != nil {
		c.logger.WithError(err).Error("failed to register row count metrics")
		return err
	}
	c.logger.Info("registered row count metrics with Prometheus registry")
	return nil
}

// acquireLock attempts to acquire the distributed lock with fixed interval retry
func (c *RowCountCollector) acquireLock(ctx context.Context) (*redislock.Lock, int) {
	// We need to monitor both c.stopCh and ctx.Done() for shutdown signals. We use a fixed retry interval instead of
	// exponential backoff for simplicity, as lock acquisition is a lightweight operation and the scenarios are limited
	// (e.g., instance shutdown, Redis unavailable, network issues).

	// Create randomness for jitter calculation
	// nolint: gosec // used only for jitter calculation
	r := rand.New(rand.NewChaCha8(testutil.SeedFromUnixNano(time.Now().UnixNano())))

	// Track attempt count
	attemptCount := 0

	// Check for shutdown before attempting
	select {
	case <-c.stopCh:
		return nil, attemptCount
	case <-ctx.Done():
		return nil, attemptCount
	default:
	}

	for {
		attemptCount++
		c.logger.Info("attempting to obtain database row count metrics lock")

		lock, err := c.locker.Obtain(ctx, rowCountLockKey, c.leaseDuration, nil)
		if err == nil {
			// Successfully obtained lock
			return lock, attemptCount
		}

		// Calculate retry timing with jitter
		jitter := time.Duration(r.IntN(1000)) * time.Millisecond
		retryAfter := lockRetryInterval + jitter
		l := c.logger.WithFields(dlog.Fields{"retry_after_s": retryAfter.Seconds()})

		if errors.Is(err, redislock.ErrNotObtained) {
			// This is expected when another instance holds the lock
			l.Info("database row count metrics lock already obtained by another instance, will retry...")
		} else {
			// Other errors (network issues, Redis down, etc.)
			l.WithError(err).Warn("failed to obtain database row count metrics lock, will retry...")
		}

		// Sleep with interruption support for both shutdown channels
		timer := time.NewTimer(retryAfter)
		select {
		case <-c.stopCh:
			timer.Stop()
			return nil, attemptCount
		case <-ctx.Done():
			timer.Stop()
			return nil, attemptCount
		case <-timer.C:
			// Continue to next retry
		}
	}
}

// run is the main collection loop
func (c *RowCountCollector) run(ctx context.Context) {
	defer c.wg.Done()

	// Try to acquire the lock with retries
	lock, _ := c.acquireLock(ctx)
	if lock == nil {
		return // Failed to acquire lock
	}

	c.logger.Info("obtained database row count metrics lock")

	// Register metrics when gaining leadership
	if err := c.registerMetrics(); err != nil {
		errortracking.Capture(
			fmt.Errorf("database row count metrics: failed to register metrics after obtaining lock: %w", err),
			errortracking.WithContext(ctx),
			errortracking.WithStackTrace(),
		)

		// Release lock before returning
		if releaseErr := lock.Release(ctx); releaseErr != nil {
			c.logger.WithError(releaseErr).Warn("failed to release database row count metrics lock after registration failure")
		}
		return
	}

	defer func() {
		// Unregister metrics when losing leadership
		c.unregisterMetrics()

		// Use a fresh context with timeout for cleanup to avoid using a potentially canceled context
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := lock.Release(cleanupCtx); err != nil {
			c.logger.WithError(err).Warn("failed to release database row count metrics lock")
		}
	}()

	// Run lock refresh in a separate goroutine to prevent losing leadership. If metrics collection takes longer than
	// the refresh interval, the lock refresh would be blocked, causing us to lose leadership while working.
	lockRefreshDone := make(chan struct{})
	go func() {
		defer close(lockRefreshDone)
		c.runLockRefresh(ctx, lock)
	}()

	// Set up collection ticker
	collectionTicker := time.NewTicker(c.interval)
	defer collectionTicker.Stop()

	// Perform initial collection
	c.collectMetrics(ctx)

	// Main collection loop
	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case <-lockRefreshDone:
			c.logger.Warn("lock refresh goroutine stopped, releasing leadership")
			return
		case <-collectionTicker.C:
			c.collectMetrics(ctx)
		}
	}
}

// collectMetrics executes all registered queries and updates Prometheus metrics
func (c *RowCountCollector) collectMetrics(ctx context.Context) {
	// Instrument the entire collection duration
	defer InstrumentRowCountCollection()()

	// Create a defensive copy of queries to avoid holding the lock during database operations
	c.mu.RLock()
	queries := make([]RowCountQuery, len(c.queries))
	copy(queries, c.queries)
	c.mu.RUnlock()

	for _, query := range queries {
		count, err := c.executeQuery(ctx, query)
		if err != nil {
			c.logger.WithFields(dlog.Fields{"query_name": query.Name}).WithError(err).Error("failed to execute row count query")
			continue
		}

		// Update the Prometheus metric
		c.metricsRegistrar.SetRowCount(query.Name, float64(count))

		c.logger.WithFields(dlog.Fields{
			"query_name": query.Name,
			"count":      count,
		}).Info("database row count metric collected")
	}
}

// runLockRefresh handles periodic lock refresh to maintain leadership
func (c *RowCountCollector) runLockRefresh(ctx context.Context, lock *redislock.Lock) {
	// Extend lock at half the lease duration to ensure we maintain it
	extensionInterval := c.leaseDuration / 2
	extensionTicker := time.NewTicker(extensionInterval)
	defer extensionTicker.Stop()

	// Track consecutive failures locally
	consecutiveFailures := 0

	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case <-extensionTicker.C:
			err := lock.Refresh(ctx, c.leaseDuration, nil)
			if err != nil {
				consecutiveFailures++
				c.logger.WithError(err).WithFields(dlog.Fields{
					"retry_count": consecutiveFailures,
				}).Warn("failed to extend database row count metrics lock")

				// Give up leadership after max consecutive failures
				// nolint:revive // max-control-nesting - acceptable for error handling logic
				if consecutiveFailures >= lockExtensionMaxRetries {
					c.logger.WithError(err).Error("failed to extend lock after retries, releasing leadership")
					errortracking.Capture(
						fmt.Errorf("database row count metrics: failed to extend lock after %d retries, releasing leadership: %w", consecutiveFailures, err),
						errortracking.WithContext(ctx),
						errortracking.WithStackTrace(),
					)
					return // Too many failures, release leadership
				}
				continue
			}

			// Success - reset failure counter if needed
			if consecutiveFailures > 0 {
				c.logger.WithFields(dlog.Fields{"retry_count": consecutiveFailures}).Info("successfully extended lock after failures")
				consecutiveFailures = 0
			} else {
				c.logger.Info("extended database row count metrics lock")
			}
		}
	}
}

// executeQuery executes a single row count query and returns the count
func (c *RowCountCollector) executeQuery(ctx context.Context, query RowCountQuery) (int64, error) {
	// Use InstrumentQuery to track query performance
	defer InstrumentQuery(fmt.Sprintf("count_%s", query.Name))()

	return c.executor(ctx, query.Query, query.Args...)
}
