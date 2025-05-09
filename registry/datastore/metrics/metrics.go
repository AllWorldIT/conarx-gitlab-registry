package metrics

import (
	"strconv"
	"time"

	"github.com/docker/distribution/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	queryDurationHist       *prometheus.HistogramVec
	queryTotal              *prometheus.CounterVec
	timeSince               = time.Since // for test purposes only
	lbPoolSize              prometheus.Gauge
	lbLSNCacheOpDuration    *prometheus.HistogramVec
	lbLSNCacheHits          *prometheus.CounterVec
	lbDNSLookupDurationHist *prometheus.HistogramVec
	lbPoolEvents            *prometheus.CounterVec
	lbTargets               *prometheus.CounterVec
	lbLagBytes              *prometheus.GaugeVec
	lbLagSeconds            *prometheus.HistogramVec
)

const (
	subsystem      = "database"
	queryNameLabel = "name"
	errorLabel     = "error"
	replicaLabel   = "replica"

	queryDurationName = "query_duration_seconds"
	queryDurationDesc = "A histogram of latencies for database queries."

	queryTotalName = "queries_total"
	queryTotalDesc = "A counter for database queries."

	lbPoolSizeName = "lb_pool_size"
	lbPoolSizeDesc = "A gauge for the current number of replicas in the load balancer pool."

	lbLSNCacheOpDurationName = "lb_lsn_cache_operation_duration_seconds"
	lbLSNCacheOpDurationDesc = "A histogram of latencies for database load balancing LSN cache operations."
	lbLSNCacheOpLabel        = "operation"
	lbLSNCacheOpSet          = "set"
	lbLSNCacheOpGet          = "get"

	lbLSNCacheHitsName    = "lb_lsn_cache_hits_total"
	lbLSNCacheHitsDesc    = "A counter for database load balancing LSN cache hits and misses."
	lbLSNCacheResultLabel = "result"
	lbLSNCacheResultHit   = "hit"
	lbLSNCacheResultMiss  = "miss"

	lbDNSLookupDurationName = "lb_lookup_seconds"
	lbDNSLookupDurationDesc = "A histogram of latencies for database load balancing DNS lookups."
	lookupTypeLabel         = "lookup_type"
	srvLookupType           = "srv"
	hostLookupType          = "host"

	lbPoolEventsName           = "lb_pool_events_total"
	lbPoolEventsDesc           = "A counter of replicas added or removed from the database load balancer pool."
	lbPoolEventsEventLabel     = "event"
	lbPoolEventsReplicaAdded   = "replica_added"
	lbPoolEventsReplicaRemoved = "replica_removed"

	lbTargetsName         = "lb_targets_total"
	lbTargetsDesc         = "A counter for primary and replica target elections during database load balancing."
	lbTargetTypeLabel     = "target_type"
	lbFallbackLabel       = "fallback"
	lbPrimaryType         = "primary"
	lbReplicaType         = "replica"
	lbReasonLabel         = "reason"
	lbFallbackNoCache     = "no_cache"
	lbFallbackNoReplica   = "no_replica"
	lbFallbackError       = "error"
	lbFallbackNotUpToDate = "not_up_to_date"
	lbReasonSelected      = "selected"

	lbLagBytesName   = "lb_lag_bytes"
	lbLagBytesDesc   = "A gauge for the replication lag in bytes for each replica."
	lbLagSecondsName = "lb_lag_seconds"
	lbLagSecondsDesc = "A histogram of replication lag in seconds for each replica."
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

	lbLSNCacheOpDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      lbLSNCacheOpDurationName,
			Help:      lbLSNCacheOpDurationDesc,
			Buckets:   prometheus.DefBuckets,
		},
		[]string{lbLSNCacheOpLabel, errorLabel},
	)

	lbLSNCacheHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      lbLSNCacheHitsName,
			Help:      lbLSNCacheHitsDesc,
		},
		[]string{lbLSNCacheResultLabel},
	)

	lbDNSLookupDurationHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      lbDNSLookupDurationName,
			Help:      lbDNSLookupDurationDesc,
			Buckets:   prometheus.DefBuckets,
		},
		[]string{lookupTypeLabel, errorLabel},
	)

	lbPoolEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      lbPoolEventsName,
			Help:      lbPoolEventsDesc,
		},
		[]string{lbPoolEventsEventLabel},
	)

	lbTargets = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      lbTargetsName,
			Help:      lbTargetsDesc,
		},
		[]string{lbTargetTypeLabel, lbFallbackLabel, lbReasonLabel},
	)

	lbLagBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      lbLagBytesName,
			Help:      lbLagBytesDesc,
		},
		[]string{replicaLabel},
	)

	lbLagSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      lbLagSecondsName,
			Help:      lbLagSecondsDesc,
			Buckets:   []float64{0.001, 0.01, 0.1, 0.5, 1, 5, 10, 20, 30, 60}, // 1ms to 60s
		},
		[]string{replicaLabel},
	)

	prometheus.MustRegister(queryDurationHist)
	prometheus.MustRegister(queryTotal)
	prometheus.MustRegister(lbPoolSize)
	prometheus.MustRegister(lbLSNCacheOpDuration)
	prometheus.MustRegister(lbLSNCacheHits)
	prometheus.MustRegister(lbDNSLookupDurationHist)
	prometheus.MustRegister(lbPoolEvents)
	prometheus.MustRegister(lbTargets)
	prometheus.MustRegister(lbLagBytes)
	prometheus.MustRegister(lbLagSeconds)
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

func lsnCacheOperation(operation string) func(error) {
	start := time.Now()
	return func(err error) {
		failed := strconv.FormatBool(err != nil)
		lbLSNCacheOpDuration.WithLabelValues(operation, failed).Observe(timeSince(start).Seconds())
	}
}

// LSNCacheGet captures the duration and result of load balancing LSN get operations.
func LSNCacheGet() func(error) {
	return lsnCacheOperation(lbLSNCacheOpGet)
}

// LSNCacheSet captures the duration and result of load balancing LSN set operations.
func LSNCacheSet() func(error) {
	return lsnCacheOperation(lbLSNCacheOpSet)
}

// LSNCacheHit increments the load balancing LSN cache hit counter.
func LSNCacheHit() {
	lbLSNCacheHits.WithLabelValues(lbLSNCacheResultHit).Inc()
}

// LSNCacheMiss increments the load balancing LSN cache miss counter.
func LSNCacheMiss() {
	lbLSNCacheHits.WithLabelValues(lbLSNCacheResultMiss).Inc()
}

func dnsLookup(lookupType string) func(error) {
	start := time.Now()
	return func(err error) {
		failed := strconv.FormatBool(err != nil)
		lbDNSLookupDurationHist.WithLabelValues(lookupType, failed).Observe(timeSince(start).Seconds())
	}
}

// SRVLookup returns a function that can be used to instrument the count and duration of DNS SRV record lookups during
// database load balancing.
func SRVLookup() func(error) {
	return dnsLookup(srvLookupType)
}

// HostLookup returns a function that can be used to instrument the count and duration of DNS host lookups during
// database load balancing.
func HostLookup() func(error) {
	return dnsLookup(hostLookupType)
}

// ReplicaAdded increments the counter for load balancing replicas added to the pool.
func ReplicaAdded() {
	lbPoolEvents.WithLabelValues(lbPoolEventsReplicaAdded).Inc()
}

// ReplicaRemoved increments the counter for load balancing replicas removed from the pool.
func ReplicaRemoved() {
	lbPoolEvents.WithLabelValues(lbPoolEventsReplicaRemoved).Inc()
}

// PrimaryTarget increments the counter for primary targets selected during load balancing.
// This method is used when the primary is selected as the intended target, not as a fallback.
func PrimaryTarget() {
	lbTargets.WithLabelValues(lbPrimaryType, "false", lbReasonSelected).Inc()
}

// PrimaryFallbackNoCache increments the counter for primary targets selected during load balancing
// as a fallback due to the absence of an LSN cache.
func PrimaryFallbackNoCache() {
	lbTargets.WithLabelValues(lbPrimaryType, "true", lbFallbackNoCache).Inc()
}

// PrimaryFallbackNoReplica increments the counter for primary targets selected during load balancing
// as a fallback due to no replicas being available.
func PrimaryFallbackNoReplica() {
	lbTargets.WithLabelValues(lbPrimaryType, "true", lbFallbackNoReplica).Inc()
}

// PrimaryFallbackError increments the counter for primary targets selected during load balancing
// as a fallback due to an error.
func PrimaryFallbackError() {
	lbTargets.WithLabelValues(lbPrimaryType, "true", lbFallbackError).Inc()
}

// PrimaryFallbackNotUpToDate increments the counter for primary targets selected during load balancing
// as a fallback because the selected replica is not up-to-date with the primary.
func PrimaryFallbackNotUpToDate() {
	lbTargets.WithLabelValues(lbPrimaryType, "true", lbFallbackNotUpToDate).Inc()
}

// ReplicaTarget increments the counter for replica targets successfully selected during load balancing.
func ReplicaTarget() {
	lbTargets.WithLabelValues(lbReplicaType, "false", lbReasonSelected).Inc()
}

// ReplicaLagBytes records the byte lag for a replica.
func ReplicaLagBytes(replicaAddr string, bytes float64) {
	lbLagBytes.WithLabelValues(replicaAddr).Set(bytes)
}

// ReplicaLagSeconds records the time lag for a replica in seconds.
func ReplicaLagSeconds(replicaAddr string, seconds float64) {
	lbLagSeconds.WithLabelValues(replicaAddr).Observe(seconds)
}
