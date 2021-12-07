package metrics

import (
	"strconv"
	"time"

	"github.com/docker/distribution/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	runDurationHist           *prometheus.HistogramVec
	runCounter                *prometheus.CounterVec
	deleteDurationHist        *prometheus.HistogramVec
	deleteCounter             *prometheus.CounterVec
	storageDeleteBytesCounter *prometheus.CounterVec
	postponeCounter           *prometheus.CounterVec
	sleepDurationHist         *prometheus.HistogramVec

	timeSince = time.Since // for test purposes only
)

const (
	subsystem = "gc"

	workerLabel    = "worker"
	errorLabel     = "error"
	noopLabel      = "noop"
	artifactLabel  = "artifact"
	backendLabel   = "backend"
	mediaTypeLabel = "media_type"
	danglingLabel  = "dangling"

	blobArtifact     = "blob"
	manifestArtifact = "manifest"
	storageBackend   = "storage"
	databaseBackend  = "database"

	runDurationName = "run_duration_seconds"
	runDurationDesc = "A histogram of latencies for online GC worker runs."
	runTotalName    = "runs_total"
	runTotalDesc    = "A counter for online GC worker runs."

	deleteTotalName    = "deletes_total"
	deleteTotalDesc    = "A counter of artifacts deleted during online GC."
	deleteDurationName = "delete_duration_seconds"
	deleteDurationDesc = "A histogram of latencies for artifact deletions during online GC."

	storageDeleteBytesTotalName = "storage_deleted_bytes_total"
	storageDeleteBytesTotalDesc = "A counter for bytes deleted from storage during online GC."

	postponeTotalName = "postpones_total"
	postponeTotalDesc = "A counter for online GC review postpones."

	sleepDurationName = "sleep_duration_seconds"
	sleepDurationDesc = "A histogram of sleep durations between online GC worker runs."
)

func init() {
	runDurationHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      runDurationName,
			Help:      runDurationDesc,
			Buckets:   prometheus.DefBuckets,
		},
		[]string{workerLabel, noopLabel, errorLabel, danglingLabel},
	)

	runCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      runTotalName,
			Help:      runTotalDesc,
		},
		[]string{workerLabel, noopLabel, errorLabel, danglingLabel},
	)

	deleteDurationHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      deleteDurationName,
			Help:      deleteDurationDesc,
			Buckets:   prometheus.DefBuckets,
		},
		[]string{backendLabel, artifactLabel, errorLabel},
	)

	deleteCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      deleteTotalName,
			Help:      deleteTotalDesc,
		},
		[]string{backendLabel, artifactLabel},
	)

	storageDeleteBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      storageDeleteBytesTotalName,
			Help:      storageDeleteBytesTotalDesc,
		},
		[]string{mediaTypeLabel},
	)

	postponeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      postponeTotalName,
			Help:      postponeTotalDesc,
		},
		[]string{workerLabel},
	)

	sleepDurationHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      sleepDurationName,
			Help:      sleepDurationDesc,
			// 500ms to 24h
			Buckets: []float64{.5, 1, 5, 15, 30, 60, 300, 600, 900, 1800, 3600, 7200, 10800, 21600, 43200, 86400},
		},
		[]string{workerLabel},
	)

	prometheus.MustRegister(runDurationHist)
	prometheus.MustRegister(runCounter)
	prometheus.MustRegister(deleteDurationHist)
	prometheus.MustRegister(deleteCounter)
	prometheus.MustRegister(postponeCounter)
	prometheus.MustRegister(storageDeleteBytesCounter)
	prometheus.MustRegister(sleepDurationHist)
}

func WorkerRun(name string) func(noop, dangling bool, err error) {
	start := time.Now()
	return func(noop, dangling bool, err error) {
		failed := strconv.FormatBool(err != nil)
		np := strconv.FormatBool(noop)
		d := strconv.FormatBool(dangling)

		runCounter.WithLabelValues(name, np, failed, d).Inc()
		runDurationHist.WithLabelValues(name, np, failed, d).Observe(timeSince(start).Seconds())
	}
}

func workerDelete(backend, artifact string) func(err error) {
	start := time.Now()
	return func(err error) {
		if err == nil {
			deleteCounter.WithLabelValues(backend, artifact).Inc()
		}
		failed := strconv.FormatBool(err != nil)
		deleteDurationHist.WithLabelValues(backend, artifact, failed).Observe(timeSince(start).Seconds())
	}
}

func blobDelete(backend string) func(err error) {
	return workerDelete(backend, blobArtifact)
}

func BlobDatabaseDelete() func(err error) {
	return blobDelete(databaseBackend)
}

func BlobStorageDelete() func(err error) {
	return blobDelete(storageBackend)
}

func ManifestDelete() func(err error) {
	return workerDelete(databaseBackend, manifestArtifact)
}

func StorageDeleteBytes(bytes int64, mediaType string) {
	storageDeleteBytesCounter.WithLabelValues(mediaType).Add(float64(bytes))
}

func ReviewPostpone(workerName string) {
	postponeCounter.WithLabelValues(workerName).Inc()
}

func WorkerSleep(name string, d time.Duration) {
	sleepDurationHist.WithLabelValues(name).Observe(d.Seconds())
}
