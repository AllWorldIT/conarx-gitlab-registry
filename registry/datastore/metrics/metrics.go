package metrics

import (
	"time"

	"github.com/docker/distribution/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	queryDurationHist *prometheus.HistogramVec
	queryTotal        *prometheus.CounterVec
	timeSince         = time.Since // for test purposes only
	lbPoolSize        prometheus.Gauge
)

const (
	subsystem      = "database"
	queryNameLabel = "name"

	queryDurationName = "query_duration_seconds"
	queryDurationDesc = "A histogram of latencies for database queries."

	queryTotalName = "queries_total"
	queryTotalDesc = "A counter for database queries."

	lbPoolSizeName = "lb_pool_size"
	lbPoolSizeDesc = "A gauge for the current number of replicas in the load balancer pool."
)

func init() {
	queryDurationHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      queryDurationName,
			Help:      queryDurationDesc,
			Buckets:   prometheus.DefBuckets,
		},
		[]string{queryNameLabel},
	)

	queryTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      queryTotalName,
			Help:      queryTotalDesc,
		},
		[]string{queryNameLabel},
	)

	lbPoolSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      lbPoolSizeName,
			Help:      lbPoolSizeDesc,
		})

	prometheus.MustRegister(queryDurationHist)
	prometheus.MustRegister(queryTotal)
	prometheus.MustRegister(lbPoolSize)
}

func InstrumentQuery(name string) func() {
	start := time.Now()
	return func() {
		queryTotal.WithLabelValues(name).Inc()
		queryDurationHist.WithLabelValues(name).Observe(timeSince(start).Seconds())
	}
}

// ReplicaPoolSize captures the current number of replicas in the load balancer pool.
func ReplicaPoolSize(size int) {
	lbPoolSize.Set(float64(size))
}
