package metrics

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"text/template"
	"time"

	"github.com/docker/distribution/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func mockTimeSince(d time.Duration) func() {
	bkp := timeSince
	timeSince = func(_ time.Time) time.Duration { return d }
	return func() { timeSince = bkp }
}

func TestInstrumentQuery(t *testing.T) {
	queryName := "foo_find_by_id"

	restore := mockTimeSince(10 * time.Millisecond)
	defer restore()
	InstrumentQuery(queryName)()

	mockTimeSince(20 * time.Millisecond)
	InstrumentQuery(queryName)()

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_database_queries_total A counter for database queries.
# TYPE registry_database_queries_total counter
registry_database_queries_total{name="foo_find_by_id"} 2
# HELP registry_database_query_duration_seconds A histogram of latencies for database queries.
# TYPE registry_database_query_duration_seconds histogram
registry_database_query_duration_seconds_bucket{name="foo_find_by_id",le="0.005"} 0
registry_database_query_duration_seconds_bucket{name="foo_find_by_id",le="0.01"} 1
registry_database_query_duration_seconds_bucket{name="foo_find_by_id",le="0.025"} 2
registry_database_query_duration_seconds_bucket{name="foo_find_by_id",le="0.05"} 2
registry_database_query_duration_seconds_bucket{name="foo_find_by_id",le="0.1"} 2
registry_database_query_duration_seconds_bucket{name="foo_find_by_id",le="0.25"} 2
registry_database_query_duration_seconds_bucket{name="foo_find_by_id",le="0.5"} 2
registry_database_query_duration_seconds_bucket{name="foo_find_by_id",le="1"} 2
registry_database_query_duration_seconds_bucket{name="foo_find_by_id",le="2.5"} 2
registry_database_query_duration_seconds_bucket{name="foo_find_by_id",le="5"} 2
registry_database_query_duration_seconds_bucket{name="foo_find_by_id",le="10"} 2
registry_database_query_duration_seconds_bucket{name="foo_find_by_id",le="+Inf"} 2
registry_database_query_duration_seconds_sum{name="foo_find_by_id"} 0.03
registry_database_query_duration_seconds_count{name="foo_find_by_id"} 2
`)
	require.NoError(t, err)
	durationFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, queryDurationName)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, queryTotalName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, durationFullName, totalFullName)
	require.NoError(t, err)
}

func TestReplicaPoolSize(t *testing.T) {
	ReplicaPoolSize(10)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_database_lb_pool_size A gauge for the current number of replicas in the load balancer pool.
# TYPE registry_database_lb_pool_size gauge
registry_database_lb_pool_size 10
`)
	require.NoError(t, err)
	fullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, lbPoolSizeName)
	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, fullName)
	require.NoError(t, err)
}

func testLSNCacheOperation(t *testing.T, operation string, opFunc func() func(error)) {
	restore := mockTimeSince(10 * time.Millisecond)
	defer func() {
		restore()
		lbLSNCacheOpDuration.Reset()
	}()

	reg := prometheus.NewRegistry()
	reg.MustRegister(lbLSNCacheOpDuration)

	report := opFunc()
	report(nil)
	report(errors.New("foo"))

	mockTimeSince(20 * time.Millisecond)
	report(nil)

	tmplFormat := `
# HELP registry_database_lb_lsn_cache_operation_duration_seconds A histogram of latencies for database load balancing LSN cache operations.
# TYPE registry_database_lb_lsn_cache_operation_duration_seconds histogram
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="false",operation="{{.Operation}}",le="0.005"} 0
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="false",operation="{{.Operation}}",le="0.01"} 1
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="false",operation="{{.Operation}}",le="0.025"} 2
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="false",operation="{{.Operation}}",le="0.05"} 2
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="false",operation="{{.Operation}}",le="0.1"} 2
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="false",operation="{{.Operation}}",le="0.25"} 2
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="false",operation="{{.Operation}}",le="0.5"} 2
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="false",operation="{{.Operation}}",le="1"} 2
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="false",operation="{{.Operation}}",le="2.5"} 2
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="false",operation="{{.Operation}}",le="5"} 2
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="false",operation="{{.Operation}}",le="10"} 2
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="false",operation="{{.Operation}}",le="Inf"} 2
registry_database_lb_lsn_cache_operation_duration_seconds_sum{error="false",operation="{{.Operation}}"} 0.03
registry_database_lb_lsn_cache_operation_duration_seconds_count{error="false",operation="{{.Operation}}"} 2
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="true",operation="{{.Operation}}",le="0.005"} 0
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="true",operation="{{.Operation}}",le="0.01"} 1
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="true",operation="{{.Operation}}",le="0.025"} 1
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="true",operation="{{.Operation}}",le="0.05"} 1
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="true",operation="{{.Operation}}",le="0.1"} 1
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="true",operation="{{.Operation}}",le="0.25"} 1
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="true",operation="{{.Operation}}",le="0.5"} 1
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="true",operation="{{.Operation}}",le="1"} 1
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="true",operation="{{.Operation}}",le="2.5"} 1
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="true",operation="{{.Operation}}",le="5"} 1
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="true",operation="{{.Operation}}",le="10"} 1
registry_database_lb_lsn_cache_operation_duration_seconds_bucket{error="true",operation="{{.Operation}}",le="Inf"} 1
registry_database_lb_lsn_cache_operation_duration_seconds_sum{error="true",operation="{{.Operation}}"} 0.01
registry_database_lb_lsn_cache_operation_duration_seconds_count{error="true",operation="{{.Operation}}"} 1
`
	tmplData := struct{ Operation string }{operation}

	var expected bytes.Buffer
	tmpl, err := template.New(t.Name()).Parse(tmplFormat)
	require.NoError(t, err)
	require.NoError(t, tmpl.Execute(&expected, tmplData))

	fullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, lbLSNCacheOpDurationName)
	err = testutil.GatherAndCompare(reg, &expected, fullName)
	require.NoError(t, err)
}

func TestLSNCacheSet(t *testing.T) {
	testLSNCacheOperation(t, lbLSNCacheOpSet, LSNCacheSet)
}

func TestLSNCacheGet(t *testing.T) {
	testLSNCacheOperation(t, lbLSNCacheOpGet, LSNCacheGet)
}

func testLSNCacheHitMiss(t *testing.T, result string, hitMissFunc func()) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(lbLSNCacheHits)
	defer func() { lbLSNCacheHits.Reset() }()

	hitMissFunc()
	hitMissFunc()

	tmplFormat := `
# HELP registry_database_lb_lsn_cache_hits_total A counter for database load balancing LSN cache hits and misses.
# TYPE registry_database_lb_lsn_cache_hits_total counter
registry_database_lb_lsn_cache_hits_total{result="{{.Result}}"} 2
`
	tmplData := struct{ Result string }{result}

	var expected bytes.Buffer
	tmpl, err := template.New(t.Name()).Parse(tmplFormat)
	require.NoError(t, err)
	require.NoError(t, tmpl.Execute(&expected, tmplData))

	fullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, lbLSNCacheHitsName)
	err = testutil.GatherAndCompare(reg, &expected, fullName)
	require.NoError(t, err)
}

func TestLSNCacheHit(t *testing.T) {
	testLSNCacheHitMiss(t, lbLSNCacheResultHit, LSNCacheHit)
}

func TestLSNCacheMiss(t *testing.T) {
	testLSNCacheHitMiss(t, lbLSNCacheResultMiss, LSNCacheMiss)
}

func testDNSLookup(t *testing.T, lookupFunc func() func(error), lookupType string) {
	restore := mockTimeSince(10 * time.Millisecond)
	defer func() {
		restore()
		lbDNSLookupDurationHist.Reset()
	}()

	reg := prometheus.NewRegistry()
	reg.MustRegister(lbDNSLookupDurationHist)

	report := lookupFunc()
	report(errors.New("foo"))
	report(errors.New("foo")) // to see the aggregated counter increase to 2
	report(nil)

	mockTimeSince(20 * time.Millisecond)
	report = lookupFunc()
	report(nil)

	tmplFormat := `
# HELP registry_database_lb_lookup_seconds A histogram of latencies for database load balancing DNS lookups.
# TYPE registry_database_lb_lookup_seconds histogram
registry_database_lb_lookup_seconds_bucket{error="false",lookup_type="{{.LookupType}}",le="0.005"} 0
registry_database_lb_lookup_seconds_bucket{error="false",lookup_type="{{.LookupType}}",le="0.01"} 1
registry_database_lb_lookup_seconds_bucket{error="false",lookup_type="{{.LookupType}}",le="0.025"} 2
registry_database_lb_lookup_seconds_bucket{error="false",lookup_type="{{.LookupType}}",le="0.05"} 2
registry_database_lb_lookup_seconds_bucket{error="false",lookup_type="{{.LookupType}}",le="0.1"} 2
registry_database_lb_lookup_seconds_bucket{error="false",lookup_type="{{.LookupType}}",le="0.25"} 2
registry_database_lb_lookup_seconds_bucket{error="false",lookup_type="{{.LookupType}}",le="0.5"} 2
registry_database_lb_lookup_seconds_bucket{error="false",lookup_type="{{.LookupType}}",le="1"} 2
registry_database_lb_lookup_seconds_bucket{error="false",lookup_type="{{.LookupType}}",le="2.5"} 2
registry_database_lb_lookup_seconds_bucket{error="false",lookup_type="{{.LookupType}}",le="5"} 2
registry_database_lb_lookup_seconds_bucket{error="false",lookup_type="{{.LookupType}}",le="10"} 2
registry_database_lb_lookup_seconds_bucket{error="false",lookup_type="{{.LookupType}}",le="Inf"} 2
registry_database_lb_lookup_seconds_sum{error="false",lookup_type="{{.LookupType}}"} 0.03
registry_database_lb_lookup_seconds_count{error="false",lookup_type="{{.LookupType}}"} 2
registry_database_lb_lookup_seconds_bucket{error="true",lookup_type="{{.LookupType}}",le="0.005"} 0
registry_database_lb_lookup_seconds_bucket{error="true",lookup_type="{{.LookupType}}",le="0.01"} 2
registry_database_lb_lookup_seconds_bucket{error="true",lookup_type="{{.LookupType}}",le="0.025"} 2
registry_database_lb_lookup_seconds_bucket{error="true",lookup_type="{{.LookupType}}",le="0.05"} 2
registry_database_lb_lookup_seconds_bucket{error="true",lookup_type="{{.LookupType}}",le="0.1"} 2
registry_database_lb_lookup_seconds_bucket{error="true",lookup_type="{{.LookupType}}",le="0.25"} 2
registry_database_lb_lookup_seconds_bucket{error="true",lookup_type="{{.LookupType}}",le="0.5"} 2
registry_database_lb_lookup_seconds_bucket{error="true",lookup_type="{{.LookupType}}",le="1"} 2
registry_database_lb_lookup_seconds_bucket{error="true",lookup_type="{{.LookupType}}",le="2.5"} 2
registry_database_lb_lookup_seconds_bucket{error="true",lookup_type="{{.LookupType}}",le="5"} 2
registry_database_lb_lookup_seconds_bucket{error="true",lookup_type="{{.LookupType}}",le="10"} 2
registry_database_lb_lookup_seconds_bucket{error="true",lookup_type="{{.LookupType}}",le="Inf"} 2
registry_database_lb_lookup_seconds_sum{error="true",lookup_type="{{.LookupType}}"} 0.02
registry_database_lb_lookup_seconds_count{error="true",lookup_type="{{.LookupType}}"} 2
# HELP registry_database_lb_lookups_total A counter for database load balancing DNS lookups.
# TYPE registry_database_lb_lookups_total counter
registry_database_lb_lookups_total{error="false",lookup_type="{{.LookupType}}"} 2
registry_database_lb_lookups_total{error="true",lookup_type="{{.LookupType}}"} 2
`

	tmplData := struct{ LookupType string }{lookupType}

	var expected bytes.Buffer
	tmpl, err := template.New(t.Name()).Parse(tmplFormat)
	require.NoError(t, err)
	require.NoError(t, tmpl.Execute(&expected, tmplData))

	fullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, lbDNSLookupDurationName)
	err = testutil.GatherAndCompare(reg, &expected, fullName)
	require.NoError(t, err)
}

func TestSRVLookup(t *testing.T) {
	testDNSLookup(t, SRVLookup, srvLookupType)
}

func TestHostLookup(t *testing.T) {
	testDNSLookup(t, HostLookup, hostLookupType)
}

func testPoolOperation(t *testing.T, event string, eventFunc func()) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(lbPoolEvents)
	defer func() { lbPoolEvents.Reset() }()

	eventFunc()
	eventFunc()

	tmplFormat := `
# HELP registry_database_lb_pool_events_total A counter of replicas added or removed from the database load balancer pool.
# TYPE registry_database_lb_pool_events_total counter
registry_database_lb_pool_events_total{event="{{.Event}}"} 2
`
	tmplData := struct{ Event string }{event}

	var expected bytes.Buffer
	tmpl, err := template.New(t.Name()).Parse(tmplFormat)
	require.NoError(t, err)
	require.NoError(t, tmpl.Execute(&expected, tmplData))

	fullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, lbPoolEventsName)
	err = testutil.GatherAndCompare(reg, &expected, fullName)
	require.NoError(t, err)
}

func TestReplicaAdded(t *testing.T) {
	testPoolOperation(t, lbPoolEventsReplicaAdded, ReplicaAdded)
}

func TestReplicaRemoved(t *testing.T) {
	testPoolOperation(t, lbPoolEventsReplicaRemoved, ReplicaRemoved)
}

func testTarget(t *testing.T, targetType string, fallback bool, reason string, targetFunc func()) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(lbTargets)
	defer func() { lbTargets.Reset() }()

	targetFunc()
	targetFunc()

	tmplFormat := `
# HELP registry_database_lb_targets_total A counter for primary and replica target elections during database load balancing.
# TYPE registry_database_lb_targets_total counter
registry_database_lb_targets_total{fallback="{{.Fallback}}",reason="{{.Reason}}",target_type="{{.Type}}"} 2
`
	tmplData := struct {
		Type     string
		Fallback string
		Reason   string
	}{
		Type:     targetType,
		Fallback: strconv.FormatBool(fallback),
		Reason:   reason,
	}

	var expected bytes.Buffer
	tmpl, err := template.New(t.Name()).Parse(tmplFormat)
	require.NoError(t, err)
	require.NoError(t, tmpl.Execute(&expected, tmplData))

	fullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, lbTargetsName)
	err = testutil.GatherAndCompare(reg, &expected, fullName)
	require.NoError(t, err)
}

func TestPrimaryTarget(t *testing.T) {
	testTarget(t, lbPrimaryType, false, lbReasonSelected, PrimaryTarget)
}

func TestPrimaryFallbackNoCache(t *testing.T) {
	testTarget(t, lbPrimaryType, true, lbFallbackNoCache, PrimaryFallbackNoCache)
}

func TestPrimaryFallbackNoReplica(t *testing.T) {
	testTarget(t, lbPrimaryType, true, lbFallbackNoReplica, PrimaryFallbackNoReplica)
}

func TestPrimaryFallbackError(t *testing.T) {
	testTarget(t, lbPrimaryType, true, lbFallbackError, PrimaryFallbackError)
}

func TestPrimaryFallbackNotUpToDate(t *testing.T) {
	testTarget(t, lbPrimaryType, true, lbFallbackNotUpToDate, PrimaryFallbackNotUpToDate)
}

func TestReplicaTarget(t *testing.T) {
	testTarget(t, lbReplicaType, false, lbReasonSelected, ReplicaTarget)
}

func TestReplicaLagBytes(t *testing.T) {
	// Create test registry to avoid conflicts with other tests
	reg := prometheus.NewRegistry()
	reg.MustRegister(lbLagBytes)
	defer func() { lbLagBytes.Reset() }()

	// Set values for two different replicas
	replicaAddr1 := "replica1:5432"
	replicaAddr2 := "replica2:5432"

	ReplicaLagBytes(replicaAddr1, 1048576)
	ReplicaLagBytes(replicaAddr2, 2097152)
	ReplicaLagBytes(replicaAddr1, 524288)

	// Expected metrics output
	tmplFormat := `
# HELP registry_database_lb_lag_bytes A gauge for the replication lag in bytes for each replica.
# TYPE registry_database_lb_lag_bytes gauge
registry_database_lb_lag_bytes{replica="{{.Replica1}}"} 524288
registry_database_lb_lag_bytes{replica="{{.Replica2}}"} 2097152
`
	tmplData := struct {
		Replica1 string
		Replica2 string
	}{
		Replica1: replicaAddr1,
		Replica2: replicaAddr2,
	}

	var expected bytes.Buffer
	tmpl, err := template.New(t.Name()).Parse(tmplFormat)
	require.NoError(t, err)
	require.NoError(t, tmpl.Execute(&expected, tmplData))

	// Verify metrics
	fullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, lbLagBytesName)
	err = testutil.GatherAndCompare(reg, &expected, fullName)
	require.NoError(t, err)
}

func TestReplicaLagSeconds(t *testing.T) {
	// Create test registry to avoid conflicts with other tests
	reg := prometheus.NewRegistry()
	reg.MustRegister(lbLagSeconds)
	defer func() { lbLagSeconds.Reset() }()

	// Set values for two different replicas
	replicaAddr1 := "replica1:5432"
	replicaAddr2 := "replica2:5432"

	// Add observations
	ReplicaLagSeconds(replicaAddr1, 0.5)
	ReplicaLagSeconds(replicaAddr1, 1.5)
	ReplicaLagSeconds(replicaAddr1, 2.0)
	ReplicaLagSeconds(replicaAddr2, 25.0)
	ReplicaLagSeconds(replicaAddr2, 0.1)

	// Expected metrics output
	tmplFormat := `
# HELP registry_database_lb_lag_seconds A histogram of replication lag in seconds for each replica.
# TYPE registry_database_lb_lag_seconds histogram
registry_database_lb_lag_seconds_bucket{replica="{{.Replica1}}",le="0.001"} 0
registry_database_lb_lag_seconds_bucket{replica="{{.Replica1}}",le="0.01"} 0
registry_database_lb_lag_seconds_bucket{replica="{{.Replica1}}",le="0.1"} 0
registry_database_lb_lag_seconds_bucket{replica="{{.Replica1}}",le="0.5"} 1
registry_database_lb_lag_seconds_bucket{replica="{{.Replica1}}",le="1"} 1
registry_database_lb_lag_seconds_bucket{replica="{{.Replica1}}",le="5"} 3
registry_database_lb_lag_seconds_bucket{replica="{{.Replica1}}",le="10"} 3
registry_database_lb_lag_seconds_bucket{replica="{{.Replica1}}",le="20"} 3
registry_database_lb_lag_seconds_bucket{replica="{{.Replica1}}",le="30"} 3
registry_database_lb_lag_seconds_bucket{replica="{{.Replica1}}",le="60"} 3
registry_database_lb_lag_seconds_bucket{replica="{{.Replica1}}",le="+Inf"} 3
registry_database_lb_lag_seconds_sum{replica="{{.Replica1}}"} 4
registry_database_lb_lag_seconds_count{replica="{{.Replica1}}"} 3
registry_database_lb_lag_seconds_bucket{replica="{{.Replica2}}",le="0.001"} 0
registry_database_lb_lag_seconds_bucket{replica="{{.Replica2}}",le="0.01"} 0
registry_database_lb_lag_seconds_bucket{replica="{{.Replica2}}",le="0.1"} 1
registry_database_lb_lag_seconds_bucket{replica="{{.Replica2}}",le="0.5"} 1
registry_database_lb_lag_seconds_bucket{replica="{{.Replica2}}",le="1"} 1
registry_database_lb_lag_seconds_bucket{replica="{{.Replica2}}",le="5"} 1
registry_database_lb_lag_seconds_bucket{replica="{{.Replica2}}",le="10"} 1
registry_database_lb_lag_seconds_bucket{replica="{{.Replica2}}",le="20"} 1
registry_database_lb_lag_seconds_bucket{replica="{{.Replica2}}",le="30"} 2
registry_database_lb_lag_seconds_bucket{replica="{{.Replica2}}",le="60"} 2
registry_database_lb_lag_seconds_bucket{replica="{{.Replica2}}",le="+Inf"} 2
registry_database_lb_lag_seconds_sum{replica="{{.Replica2}}"} 25.1
registry_database_lb_lag_seconds_count{replica="{{.Replica2}}"} 2
`
	tmplData := struct {
		Replica1 string
		Replica2 string
	}{
		Replica1: replicaAddr1,
		Replica2: replicaAddr2,
	}

	var expected bytes.Buffer
	tmpl, err := template.New(t.Name()).Parse(tmplFormat)
	require.NoError(t, err)
	require.NoError(t, tmpl.Execute(&expected, tmplData))

	// Verify metrics
	fullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, lbLagSecondsName)
	err = testutil.GatherAndCompare(reg, &expected, fullName)
	require.NoError(t, err)
}
