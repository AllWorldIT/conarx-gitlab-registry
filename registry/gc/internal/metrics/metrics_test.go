package metrics

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/docker/distribution/metrics"
	"github.com/prometheus/client_golang/prometheus"
	testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func mockTimeSince(d time.Duration) func() {
	bkp := timeSince
	timeSince = func(_ time.Time) time.Duration { return d }
	return func() { timeSince = bkp }
}

func TestWorkerRun(t *testing.T) {
	name := "foo"

	restore := mockTimeSince(10 * time.Millisecond)
	defer restore()

	report := WorkerRun(name)
	report(true, true, errors.New("foo"), "tag_switch")
	report(true, true, errors.New("foo"), "tag_switch") // to see the aggregated counter increase to 2
	report(true, false, nil, "blob_upload")

	mockTimeSince(20 * time.Millisecond)
	report = WorkerRun(name)
	report(false, true, nil, "manifest_delete")
	report(false, true, errors.New("foo"), "tag_delete")

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_gc_run_duration_seconds A histogram of latencies for online GC worker runs.
# TYPE registry_gc_run_duration_seconds histogram
registry_gc_run_duration_seconds_bucket{dangling="false",error="false",event="blob_upload",noop="true",worker="foo",le="0.005"} 0
registry_gc_run_duration_seconds_bucket{dangling="false",error="false",event="blob_upload",noop="true",worker="foo",le="0.01"} 1
registry_gc_run_duration_seconds_bucket{dangling="false",error="false",event="blob_upload",noop="true",worker="foo",le="0.025"} 1
registry_gc_run_duration_seconds_bucket{dangling="false",error="false",event="blob_upload",noop="true",worker="foo",le="0.05"} 1
registry_gc_run_duration_seconds_bucket{dangling="false",error="false",event="blob_upload",noop="true",worker="foo",le="0.1"} 1
registry_gc_run_duration_seconds_bucket{dangling="false",error="false",event="blob_upload",noop="true",worker="foo",le="0.25"} 1
registry_gc_run_duration_seconds_bucket{dangling="false",error="false",event="blob_upload",noop="true",worker="foo",le="0.5"} 1
registry_gc_run_duration_seconds_bucket{dangling="false",error="false",event="blob_upload",noop="true",worker="foo",le="1"} 1
registry_gc_run_duration_seconds_bucket{dangling="false",error="false",event="blob_upload",noop="true",worker="foo",le="2.5"} 1
registry_gc_run_duration_seconds_bucket{dangling="false",error="false",event="blob_upload",noop="true",worker="foo",le="5"} 1
registry_gc_run_duration_seconds_bucket{dangling="false",error="false",event="blob_upload",noop="true",worker="foo",le="10"} 1
registry_gc_run_duration_seconds_bucket{dangling="false",error="false",event="blob_upload",noop="true",worker="foo",le="+Inf"} 1
registry_gc_run_duration_seconds_sum{dangling="false",error="false",event="blob_upload",noop="true",worker="foo"} 0.01
registry_gc_run_duration_seconds_count{dangling="false",error="false",event="blob_upload",noop="true",worker="foo"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo",le="0.005"} 0
registry_gc_run_duration_seconds_bucket{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo",le="0.01"} 0
registry_gc_run_duration_seconds_bucket{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo",le="0.025"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo",le="0.05"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo",le="0.1"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo",le="0.25"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo",le="0.5"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo",le="1"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo",le="2.5"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo",le="5"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo",le="10"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo",le="+Inf"} 1
registry_gc_run_duration_seconds_sum{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo"} 0.02
registry_gc_run_duration_seconds_count{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_delete",noop="false",worker="foo",le="0.005"} 0
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_delete",noop="false",worker="foo",le="0.01"} 0
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_delete",noop="false",worker="foo",le="0.025"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_delete",noop="false",worker="foo",le="0.05"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_delete",noop="false",worker="foo",le="0.1"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_delete",noop="false",worker="foo",le="0.25"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_delete",noop="false",worker="foo",le="0.5"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_delete",noop="false",worker="foo",le="1"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_delete",noop="false",worker="foo",le="2.5"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_delete",noop="false",worker="foo",le="5"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_delete",noop="false",worker="foo",le="10"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_delete",noop="false",worker="foo",le="+Inf"} 1
registry_gc_run_duration_seconds_sum{dangling="true",error="true",event="tag_delete",noop="false",worker="foo"} 0.02
registry_gc_run_duration_seconds_count{dangling="true",error="true",event="tag_delete",noop="false",worker="foo"} 1
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_switch",noop="true",worker="foo",le="0.005"} 0
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_switch",noop="true",worker="foo",le="0.01"} 2
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_switch",noop="true",worker="foo",le="0.025"} 2
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_switch",noop="true",worker="foo",le="0.05"} 2
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_switch",noop="true",worker="foo",le="0.1"} 2
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_switch",noop="true",worker="foo",le="0.25"} 2
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_switch",noop="true",worker="foo",le="0.5"} 2
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_switch",noop="true",worker="foo",le="1"} 2
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_switch",noop="true",worker="foo",le="2.5"} 2
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_switch",noop="true",worker="foo",le="5"} 2
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_switch",noop="true",worker="foo",le="10"} 2
registry_gc_run_duration_seconds_bucket{dangling="true",error="true",event="tag_switch",noop="true",worker="foo",le="+Inf"} 2
registry_gc_run_duration_seconds_sum{dangling="true",error="true",event="tag_switch",noop="true",worker="foo"} 0.02
registry_gc_run_duration_seconds_count{dangling="true",error="true",event="tag_switch",noop="true",worker="foo"} 2
# HELP registry_gc_runs_total A counter for online GC worker runs.
# TYPE registry_gc_runs_total counter
registry_gc_runs_total{dangling="false",error="false",event="blob_upload",noop="true",worker="foo"} 1
registry_gc_runs_total{dangling="true",error="false",event="manifest_delete",noop="false",worker="foo"} 1
registry_gc_runs_total{dangling="true",error="true",event="tag_delete",noop="false",worker="foo"} 1
registry_gc_runs_total{dangling="true",error="true",event="tag_switch",noop="true",worker="foo"} 2
`)
	require.NoError(t, err)
	durationFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, runDurationName)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, runTotalName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, durationFullName, totalFullName)
	require.NoError(t, err)
}

func TestBlobDatabaseDelete(t *testing.T) {
	restore := mockTimeSince(10 * time.Millisecond)
	defer restore()

	// Cleanup metrics from previous tests (there are multiple making use of deleteDurationHist and deleteCounter) and
	// create local prometheus registry
	deleteDurationHist.Reset()
	deleteCounter.Reset()
	reg := prometheus.NewRegistry()
	reg.MustRegister(deleteDurationHist)
	reg.MustRegister(deleteCounter)

	report := BlobDatabaseDelete()
	report(errors.New("foo"))
	report(errors.New("foo")) // to see the aggregated counter increase to 2
	report(nil)

	mockTimeSince(20 * time.Millisecond)
	report = BlobDatabaseDelete()
	report(nil)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_gc_delete_duration_seconds A histogram of latencies for artifact deletions during online GC.
# TYPE registry_gc_delete_duration_seconds histogram
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="false",le="0.005"} 0
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="false",le="0.01"} 1
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="false",le="0.025"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="false",le="0.05"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="false",le="0.1"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="false",le="0.25"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="false",le="0.5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="false",le="1"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="false",le="2.5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="false",le="5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="false",le="10"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="false",le="+Inf"} 2
registry_gc_delete_duration_seconds_sum{artifact="blob",backend="database",error="false"} 0.03
registry_gc_delete_duration_seconds_count{artifact="blob",backend="database",error="false"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="true",le="0.005"} 0
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="true",le="0.01"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="true",le="0.025"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="true",le="0.05"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="true",le="0.1"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="true",le="0.25"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="true",le="0.5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="true",le="1"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="true",le="2.5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="true",le="5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="true",le="10"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="database",error="true",le="+Inf"} 2
registry_gc_delete_duration_seconds_sum{artifact="blob",backend="database",error="true"} 0.02
registry_gc_delete_duration_seconds_count{artifact="blob",backend="database",error="true"} 2
# HELP registry_gc_deletes_total A counter of artifacts deleted during online GC.
# TYPE registry_gc_deletes_total counter
registry_gc_deletes_total{artifact="blob",backend="database"} 2
`)
	require.NoError(t, err)
	durationFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, deleteDurationName)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, deleteTotalName)

	err = testutil.GatherAndCompare(reg, &expected, durationFullName, totalFullName)
	require.NoError(t, err)
}

func TestBlobStorageDelete(t *testing.T) {
	restore := mockTimeSince(10 * time.Millisecond)
	defer restore()

	// Cleanup metrics from previous tests (there are multiple making use of deleteDurationHist and deleteCounter) and
	// create local prometheus registry
	deleteDurationHist.Reset()
	deleteCounter.Reset()
	reg := prometheus.NewRegistry()
	reg.MustRegister(deleteDurationHist)
	reg.MustRegister(deleteCounter)

	report := BlobStorageDelete()
	report(errors.New("foo"))
	report(errors.New("foo")) // to see the aggregated counter increase to 2
	report(nil)

	mockTimeSince(20 * time.Millisecond)
	report = BlobStorageDelete()
	report(nil)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_gc_delete_duration_seconds A histogram of latencies for artifact deletions during online GC.
# TYPE registry_gc_delete_duration_seconds histogram
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="false",le="0.005"} 0
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="false",le="0.01"} 1
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="false",le="0.025"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="false",le="0.05"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="false",le="0.1"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="false",le="0.25"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="false",le="0.5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="false",le="1"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="false",le="2.5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="false",le="5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="false",le="10"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="false",le="+Inf"} 2
registry_gc_delete_duration_seconds_sum{artifact="blob",backend="storage",error="false"} 0.03
registry_gc_delete_duration_seconds_count{artifact="blob",backend="storage",error="false"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="true",le="0.005"} 0
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="true",le="0.01"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="true",le="0.025"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="true",le="0.05"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="true",le="0.1"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="true",le="0.25"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="true",le="0.5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="true",le="1"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="true",le="2.5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="true",le="5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="true",le="10"} 2
registry_gc_delete_duration_seconds_bucket{artifact="blob",backend="storage",error="true",le="+Inf"} 2
registry_gc_delete_duration_seconds_sum{artifact="blob",backend="storage",error="true"} 0.02
registry_gc_delete_duration_seconds_count{artifact="blob",backend="storage",error="true"} 2
# HELP registry_gc_deletes_total A counter of artifacts deleted during online GC.
# TYPE registry_gc_deletes_total counter
registry_gc_deletes_total{artifact="blob",backend="storage"} 2
`)
	require.NoError(t, err)
	durationFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, deleteDurationName)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, deleteTotalName)

	err = testutil.GatherAndCompare(reg, &expected, durationFullName, totalFullName)
	require.NoError(t, err)
}

func TestManifestDelete(t *testing.T) {
	restore := mockTimeSince(10 * time.Millisecond)
	defer restore()

	// Cleanup metrics from previous tests (there are multiple making use of deleteDurationHist and deleteCounter) and
	// create local prometheus registry
	deleteDurationHist.Reset()
	deleteCounter.Reset()
	reg := prometheus.NewRegistry()
	reg.MustRegister(deleteDurationHist)
	reg.MustRegister(deleteCounter)

	report := ManifestDelete()
	report(errors.New("foo"))
	report(errors.New("foo")) // to see the aggregated counter increase to 2
	report(nil)

	mockTimeSince(20 * time.Millisecond)
	report = ManifestDelete()
	report(nil)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_gc_delete_duration_seconds A histogram of latencies for artifact deletions during online GC.
# TYPE registry_gc_delete_duration_seconds histogram
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="false",le="0.005"} 0
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="false",le="0.01"} 1
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="false",le="0.025"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="false",le="0.05"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="false",le="0.1"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="false",le="0.25"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="false",le="0.5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="false",le="1"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="false",le="2.5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="false",le="5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="false",le="10"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="false",le="+Inf"} 2
registry_gc_delete_duration_seconds_sum{artifact="manifest",backend="database",error="false"} 0.03
registry_gc_delete_duration_seconds_count{artifact="manifest",backend="database",error="false"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="true",le="0.005"} 0
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="true",le="0.01"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="true",le="0.025"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="true",le="0.05"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="true",le="0.1"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="true",le="0.25"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="true",le="0.5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="true",le="1"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="true",le="2.5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="true",le="5"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="true",le="10"} 2
registry_gc_delete_duration_seconds_bucket{artifact="manifest",backend="database",error="true",le="+Inf"} 2
registry_gc_delete_duration_seconds_sum{artifact="manifest",backend="database",error="true"} 0.02
registry_gc_delete_duration_seconds_count{artifact="manifest",backend="database",error="true"} 2
# HELP registry_gc_deletes_total A counter of artifacts deleted during online GC.
# TYPE registry_gc_deletes_total counter
registry_gc_deletes_total{artifact="manifest",backend="database"} 2
`)
	require.NoError(t, err)
	durationFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, deleteDurationName)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, deleteTotalName)

	err = testutil.GatherAndCompare(reg, &expected, durationFullName, totalFullName)
	require.NoError(t, err)
}

func TestStorageDeleteBytes(t *testing.T) {
	StorageDeleteBytes(123, "foo")
	StorageDeleteBytes(321, "foo")
	StorageDeleteBytes(1, "bar")

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_gc_storage_deleted_bytes_total A counter for bytes deleted from storage during online GC.
# TYPE registry_gc_storage_deleted_bytes_total counter
registry_gc_storage_deleted_bytes_total{media_type="bar"} 1
registry_gc_storage_deleted_bytes_total{media_type="foo"} 444
`)
	require.NoError(t, err)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, storageDeleteBytesTotalName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, totalFullName)
	require.NoError(t, err)
}

func TestReviewPostpone(t *testing.T) {
	ReviewPostpone("foo")
	ReviewPostpone("foo")
	ReviewPostpone("bar")

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_gc_postpones_total A counter for online GC review postpones.
# TYPE registry_gc_postpones_total counter
registry_gc_postpones_total{worker="bar"} 1
registry_gc_postpones_total{worker="foo"} 2
`)
	require.NoError(t, err)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, postponeTotalName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, totalFullName)
	require.NoError(t, err)
}

func TestWorkerSleep(t *testing.T) {
	WorkerSleep("foo", 10*time.Second)
	WorkerSleep("foo", 10*time.Millisecond)
	WorkerSleep("bar", 100*time.Hour)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_gc_sleep_duration_seconds A histogram of sleep durations between online GC worker runs.
# TYPE registry_gc_sleep_duration_seconds histogram
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="0.5"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="1"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="5"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="15"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="30"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="60"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="300"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="600"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="900"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="1800"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="3600"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="7200"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="10800"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="21600"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="43200"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="86400"} 0
registry_gc_sleep_duration_seconds_bucket{worker="bar",le="+Inf"} 1
registry_gc_sleep_duration_seconds_sum{worker="bar"} 360000
registry_gc_sleep_duration_seconds_count{worker="bar"} 1
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="0.5"} 1
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="1"} 1
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="5"} 1
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="15"} 2
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="30"} 2
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="60"} 2
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="300"} 2
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="600"} 2
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="900"} 2
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="1800"} 2
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="3600"} 2
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="7200"} 2
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="10800"} 2
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="21600"} 2
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="43200"} 2
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="86400"} 2
registry_gc_sleep_duration_seconds_bucket{worker="foo",le="+Inf"} 2
registry_gc_sleep_duration_seconds_sum{worker="foo"} 10.01
registry_gc_sleep_duration_seconds_count{worker="foo"} 2
`)
	require.NoError(t, err)
	durationFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, sleepDurationName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, durationFullName)
	require.NoError(t, err)
}
