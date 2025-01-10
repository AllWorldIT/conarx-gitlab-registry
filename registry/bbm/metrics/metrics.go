package metrics

import (
	"time"

	"github.com/docker/distribution/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	timeSince           = time.Since // for test purposes only
	queryDurationHist   *prometheus.HistogramVec
	jobBatchSize        *prometheus.GaugeVec
	jobDurationHist     *prometheus.HistogramVec
	runDurationHist     *prometheus.HistogramVec
	runCounter          *prometheus.CounterVec
	migratedTuplesTotal *prometheus.CounterVec
	buckets             = []float64{.5, 1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600} // 0.5s to 1h
)

const (
	subsystem = "bbm"

	runDurationName = "run_duration_seconds"
	runDurationDesc = "A histogram of latencies for batched migration worker runs."

	runTotalName = "runs_total"
	runTotalDesc = "A counter for batched migration worker runs."

	queryDurationName = "query_duration_seconds"
	queryDurationDesc = "A histogram of latencies for batched migration database queries."

	jobBatchSizeName = "job_batch_size"
	jobBatchSizeDesc = "A gauge for the batch size of a batched migration job."

	jobDurationName = "job_duration_seconds"
	jobDurationDesc = "A histogram of latencies for a batched migration job."

	migratedTuplesTotalName = "migrated_tuples_total"
	migratedTuplesTotalDesc = "A counter for total batched migration records migrated."

	migrationNameLabel = "migration_name"
	migrationIDLabel   = "migration_id"
)

func init() {
	runDurationHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      runDurationName,
			Help:      runDurationDesc,
			Buckets:   buckets,
		},
		make([]string, 0),
	)

	runCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      runTotalName,
			Help:      runTotalDesc,
		},
		make([]string, 0),
	)

	jobBatchSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      jobBatchSizeName,
			Help:      jobBatchSizeDesc,
		}, []string{migrationNameLabel, migrationIDLabel},
	)

	jobDurationHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      jobDurationName,
			Help:      jobDurationDesc,
			Buckets:   buckets,
		},
		[]string{migrationNameLabel, migrationIDLabel},
	)

	migratedTuplesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      migratedTuplesTotalName,
			Help:      migratedTuplesTotalDesc,
		},
		[]string{migrationNameLabel, migrationIDLabel},
	)

	queryDurationHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      queryDurationName,
			Help:      queryDurationDesc,
			Buckets:   buckets,
		},
		[]string{migrationNameLabel, migrationIDLabel},
	)

	prometheus.MustRegister(queryDurationHist)
	prometheus.MustRegister(jobBatchSize)
	prometheus.MustRegister(jobDurationHist)
	prometheus.MustRegister(runDurationHist)
	prometheus.MustRegister(runCounter)
	prometheus.MustRegister(migratedTuplesTotal)
}

// InstrumentQuery starts a timer to measure the duration of a migration query.
// Returns a function to stop the timer and capture the query's latency.
func InstrumentQuery(migrationName, migrationID string) func() {
	start := time.Now()
	return func() {
		queryDurationHist.WithLabelValues(migrationName, migrationID).Observe(timeSince(start).Seconds())
	}
}

// Job starts a timer to measure the duration of a migration job and records its batch size.
// Returns a function to stop the timer and capture the job's latency and batch size.
func Job(batchSize int, migrationName, migrationID string) func() {
	start := time.Now()
	jobBatchSize.WithLabelValues(migrationName, migrationID).Set(float64(batchSize))
	return func() {
		jobDurationHist.WithLabelValues(migrationName, migrationID).Observe(timeSince(start).Seconds())
	}
}

// WorkerRun starts a timer to measure the duration of a migration worker run.
// Returns a function to stop the timer and capture the run's latency and count.
func WorkerRun() func() {
	start := time.Now()
	return func() {
		runDurationHist.WithLabelValues().Observe(timeSince(start).Seconds())
		runCounter.WithLabelValues().Inc()
	}
}

// MigrationRecord captures the total number of records migrated for a background migration.
func MigrationRecord(size int, migrationName, migrationID string) {
	migratedTuplesTotal.WithLabelValues(migrationName, migrationID).Add(float64(size))
}
