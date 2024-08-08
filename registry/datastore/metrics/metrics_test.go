package metrics

import (
	"bytes"
	"fmt"
	"testing"
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
	expected.WriteString(`
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
	durationFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, queryDurationName)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, queryTotalName)

	err := testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, durationFullName, totalFullName)
	require.NoError(t, err)
}

func TestReplicaPoolSize(t *testing.T) {
	ReplicaPoolSize(10)

	var expected bytes.Buffer
	expected.WriteString(`
# HELP registry_database_lb_pool_size A gauge for the current number of replicas in the load balancer pool.
# TYPE registry_database_lb_pool_size gauge
registry_database_lb_pool_size 10
`)
	fullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, lbPoolSizeName)
	err := testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, fullName)
	require.NoError(t, err)
}
