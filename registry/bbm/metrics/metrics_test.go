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

var (
	migrationName = "foo_migration_name"
	migrationID   = "foo_migration_id"
)

func mockTimeSince(t *testing.T, d time.Duration) {
	t.Helper()
	bkp := timeSince
	timeSince = func(_ time.Time) time.Duration { return d }
	t.Cleanup(func() { timeSince = bkp })
}

func TestInstrumentQuery(t *testing.T) {
	mockTimeSince(t, 1*time.Second)
	InstrumentQuery(migrationName, migrationID)()

	mockTimeSince(t, 2*time.Second)
	InstrumentQuery(migrationName, migrationID)()

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_bbm_query_duration_seconds A histogram of latencies for batched migration database queries.
# TYPE registry_bbm_query_duration_seconds histogram
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="0.5"} 0
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="1"} 1
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="2"} 2
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="5"} 2
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="10"} 2
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="15"} 2
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="30"} 2
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="60"} 2
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="120"} 2
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="300"} 2
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="600"} 2
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="900"} 2
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="1800"} 2
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="3600"} 2
registry_bbm_query_duration_seconds_bucket{migration_id="foo_migration_id",migration_name="foo_migration_name",le="+Inf"} 2
registry_bbm_query_duration_seconds_sum{migration_id="foo_migration_id",migration_name="foo_migration_name"} 3
registry_bbm_query_duration_seconds_count{migration_id="foo_migration_id",migration_name="foo_migration_name"} 2
`)
	require.NoError(t, err)
	durationFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, queryDurationName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, durationFullName)
	require.NoError(t, err)
}

func TestJob(t *testing.T) {
	mockTimeSince(t, 1*time.Second)
	Job(10, migrationName, migrationID)()

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_bbm_job_duration_seconds A histogram of latencies for a batched migration job.
# TYPE registry_bbm_job_duration_seconds histogram
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="0.5"} 0
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="1"} 1
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="2"} 1
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="5"} 1
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="10"} 1
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="15"} 1
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="30"} 1
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="60"} 1
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="120"} 1
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="300"} 1
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="600"} 1
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="900"} 1
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="1800"} 1
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="3600"} 1
registry_bbm_job_duration_seconds{migration_id="foo_migration_id",migration_name="foo_migration_name",le="+Inf"} 1
registry_bbm_job_duration_seconds_sum{migration_id="foo_migration_id",migration_name="foo_migration_name"} 1
registry_bbm_job_duration_seconds_count{migration_id="foo_migration_id",migration_name="foo_migration_name"} 1
# HELP registry_bbm_job_batch_size A gauge for the batch size of a batched migration job.
# TYPE registry_bbm_job_batch_size gauge
registry_bbm_job_batch_size {migration_id="foo_migration_id",migration_name="foo_migration_name"} 10
`)
	require.NoError(t, err)
	fullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, jobBatchSizeName)
	durationFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, jobDurationName)
	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, fullName, durationFullName)
	require.NoError(t, err)
}

func TestWorkerRun(t *testing.T) {
	mockTimeSince(t, 1*time.Second)
	WorkerRun()()

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_bbm_runs_total A counter for batched migration worker runs.
# TYPE registry_bbm_runs_total counter
registry_bbm_runs_total 1
# HELP registry_bbm_run_duration_seconds A histogram of latencies for batched migration worker runs.
# TYPE registry_bbm_run_duration_seconds histogram
registry_bbm_run_duration_seconds_bucket{le="0.5"} 0
registry_bbm_run_duration_seconds_bucket{le="1"} 1
registry_bbm_run_duration_seconds_bucket{le="2"} 1
registry_bbm_run_duration_seconds_bucket{le="5"} 1
registry_bbm_run_duration_seconds_bucket{le="10"} 1
registry_bbm_run_duration_seconds_bucket{le="15"} 1
registry_bbm_run_duration_seconds_bucket{le="30"} 1
registry_bbm_run_duration_seconds_bucket{le="60"} 1
registry_bbm_run_duration_seconds_bucket{le="120"} 1
registry_bbm_run_duration_seconds_bucket{le="300"} 1
registry_bbm_run_duration_seconds_bucket{le="600"} 1
registry_bbm_run_duration_seconds_bucket{le="900"} 1
registry_bbm_run_duration_seconds_bucket{le="1800"} 1
registry_bbm_run_duration_seconds_bucket{le="3600"} 1
registry_bbm_run_duration_seconds_bucket{le="+Inf"} 1
registry_bbm_run_duration_seconds_sum 1
registry_bbm_run_duration_seconds_count 1
`)
	require.NoError(t, err)
	durationFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, runDurationName)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, runTotalName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, totalFullName, durationFullName)
	require.NoError(t, err)
}

func TestMigrationRecord(t *testing.T) {
	MigrationRecord(10, migrationName, migrationID)
	MigrationRecord(10, migrationName, migrationID)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_bbm_migrated_tuples_total A counter for total batched migration records migrated.
# TYPE registry_bbm_migrated_tuples_total counter
registry_bbm_migrated_tuples_total{migration_id="foo_migration_id",migration_name="foo_migration_name"} 20
`)
	require.NoError(t, err)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, migratedTuplesTotalName)
	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, totalFullName)
	require.NoError(t, err)
}

func TestWorkerSleep(t *testing.T) {
	WorkerSleep("foo", 10*time.Second)
	WorkerSleep("foo", 10*time.Millisecond)
	WorkerSleep("bar", 100*time.Hour)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_bbm_sleep_duration_seconds A histogram of sleep durations between bbm worker runs.
# TYPE registry_bbm_sleep_duration_seconds histogram
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="0.5"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="1"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="5"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="15"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="30"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="60"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="300"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="600"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="900"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="1800"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="3600"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="7200"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="10800"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="21600"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="43200"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="86400"} 0
registry_bbm_sleep_duration_seconds_bucket{worker="bar",le="+Inf"} 1
registry_bbm_sleep_duration_seconds_sum{worker="bar"} 360000
registry_bbm_sleep_duration_seconds_count{worker="bar"} 1
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="0.5"} 1
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="1"} 1
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="5"} 1
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="15"} 2
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="30"} 2
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="60"} 2
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="300"} 2
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="600"} 2
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="900"} 2
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="1800"} 2
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="3600"} 2
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="7200"} 2
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="10800"} 2
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="21600"} 2
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="43200"} 2
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="86400"} 2
registry_bbm_sleep_duration_seconds_bucket{worker="foo",le="+Inf"} 2
registry_bbm_sleep_duration_seconds_sum{worker="foo"} 10.01
registry_bbm_sleep_duration_seconds_count{worker="foo"} 2
`)
	require.NoError(t, err)
	durationFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, sleepDurationName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, durationFullName)
	require.NoError(t, err)
}
