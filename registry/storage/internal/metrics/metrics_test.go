package metrics

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/docker/distribution/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestBlobDownload(t *testing.T) {
	BlobDownload(false, 512)
	BlobDownload(true, 1024)
	BlobDownload(true, 2048)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_storage_blob_download_bytes A histogram of blob download sizes for the storage backend.
# TYPE registry_storage_blob_download_bytes histogram
registry_storage_blob_download_bytes_bucket{redirect="false",le="524288"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="1.048576e+06"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="6.7108864e+07"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="1.34217728e+08"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="2.68435456e+08"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="5.36870912e+08"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="1.073741824e+09"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="2.147483648e+09"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="3.221225472e+09"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="4.294967296e+09"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="5.36870912e+09"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="6.442450944e+09"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="7.516192768e+09"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="8.589934592e+09"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="9.663676416e+09"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="1.073741824e+10"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="2.147483648e+10"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="3.221225472e+10"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="4.294967296e+10"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="5.36870912e+10"} 1
registry_storage_blob_download_bytes_bucket{redirect="false",le="+Inf"} 1
registry_storage_blob_download_bytes_sum{redirect="false"} 512
registry_storage_blob_download_bytes_count{redirect="false"} 1
registry_storage_blob_download_bytes_bucket{redirect="true",le="524288"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="1.048576e+06"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="6.7108864e+07"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="1.34217728e+08"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="2.68435456e+08"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="5.36870912e+08"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="1.073741824e+09"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="2.147483648e+09"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="3.221225472e+09"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="4.294967296e+09"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="5.36870912e+09"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="6.442450944e+09"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="7.516192768e+09"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="8.589934592e+09"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="9.663676416e+09"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="1.073741824e+10"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="2.147483648e+10"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="3.221225472e+10"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="4.294967296e+10"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="5.36870912e+10"} 2
registry_storage_blob_download_bytes_bucket{redirect="true",le="+Inf"} 2
registry_storage_blob_download_bytes_sum{redirect="true"} 3072
registry_storage_blob_download_bytes_count{redirect="true"} 2
`)
	require.NoError(t, err)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, blobDownloadBytesName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, totalFullName)
	require.NoError(t, err)
}

func TestCDNRedirect(t *testing.T) {
	CDNRedirect("cdn", false, "")
	CDNRedirect("storage", true, "ip")

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_storage_cdn_redirects_total A counter of CDN redirections for blob downloads.
# TYPE registry_storage_cdn_redirects_total counter
registry_storage_cdn_redirects_total{backend="cdn",bypass="false",bypass_reason=""} 1
registry_storage_cdn_redirects_total{backend="storage",bypass="true",bypass_reason="ip"} 1
`)
	require.NoError(t, err)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, cdnRedirectTotalName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, totalFullName)
	require.NoError(t, err)
}

func TestStorageRatelimit(t *testing.T) {
	StorageRatelimit()

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_storage_rate_limit_total A counter of requests to the storage driver that hit a rate limit.
# TYPE registry_storage_rate_limit_total counter
registry_storage_rate_limit_total 1
`)
	require.NoError(t, err)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, rateLimitStorageName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, totalFullName)
	require.NoError(t, err)
}

func TestBlobUpload(t *testing.T) {
	BlobUpload(512)
	BlobUpload(1024)
	BlobUpload(2048)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_storage_blob_upload_bytes A histogram of new blob upload bytes for the storage backend.
# TYPE registry_storage_blob_upload_bytes histogram
registry_storage_blob_upload_bytes_bucket{le="524288"} 3
registry_storage_blob_upload_bytes_bucket{le="1.048576e+06"} 3
registry_storage_blob_upload_bytes_bucket{le="6.7108864e+07"} 3
registry_storage_blob_upload_bytes_bucket{le="1.34217728e+08"} 3
registry_storage_blob_upload_bytes_bucket{le="2.68435456e+08"} 3
registry_storage_blob_upload_bytes_bucket{le="5.36870912e+08"} 3
registry_storage_blob_upload_bytes_bucket{le="1.073741824e+09"} 3
registry_storage_blob_upload_bytes_bucket{le="2.147483648e+09"} 3
registry_storage_blob_upload_bytes_bucket{le="3.221225472e+09"} 3
registry_storage_blob_upload_bytes_bucket{le="4.294967296e+09"} 3
registry_storage_blob_upload_bytes_bucket{le="5.36870912e+09"} 3
registry_storage_blob_upload_bytes_bucket{le="6.442450944e+09"} 3
registry_storage_blob_upload_bytes_bucket{le="7.516192768e+09"} 3
registry_storage_blob_upload_bytes_bucket{le="8.589934592e+09"} 3
registry_storage_blob_upload_bytes_bucket{le="9.663676416e+09"} 3
registry_storage_blob_upload_bytes_bucket{le="1.073741824e+10"} 3
registry_storage_blob_upload_bytes_bucket{le="2.147483648e+10"} 3
registry_storage_blob_upload_bytes_bucket{le="3.221225472e+10"} 3
registry_storage_blob_upload_bytes_bucket{le="4.294967296e+10"} 3
registry_storage_blob_upload_bytes_bucket{le="5.36870912e+10"} 3
registry_storage_blob_upload_bytes_bucket{le="+Inf"} 3
registry_storage_blob_upload_bytes_sum 3584
registry_storage_blob_upload_bytes_count 3
`)
	require.NoError(t, err)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, blobUploadBytesName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, totalFullName)
	require.NoError(t, err)
}

func TestStorageBackendRetry(t *testing.T) {
	// Test native retries
	StorageBackendRetry(true)
	StorageBackendRetry(true)

	// Test custom retries
	StorageBackendRetry(false)
	StorageBackendRetry(false)
	StorageBackendRetry(false)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_storage_storage_backend_retries_total A counter of retires made while communicating with storage backend.
# TYPE registry_storage_storage_backend_retries_total counter
registry_storage_storage_backend_retries_total{retry_type="custom"} 3
registry_storage_storage_backend_retries_total{retry_type="native"} 2
`)
	require.NoError(t, err)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, storageBackendRetriesTotalName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, totalFullName)
	require.NoError(t, err)
}

func TestURLCacheRequest(t *testing.T) {
	// Test cache hits
	URLCacheRequest(true, "")
	URLCacheRequest(true, "")

	// Test cache misses with different reasons
	URLCacheRequest(false, "not_found")
	URLCacheRequest(false, "error")

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_storage_urlcache_requests_total A counter of the URL cache middleware requests.
# TYPE registry_storage_urlcache_requests_total counter
registry_storage_urlcache_requests_total{reason="",result="hit"} 2
registry_storage_urlcache_requests_total{reason="error",result="miss"} 1
registry_storage_urlcache_requests_total{reason="not_found",result="miss"} 1
`)
	require.NoError(t, err)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, urlCacheRequestsTotalName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, totalFullName)
	require.NoError(t, err)
}

func TestURLCacheObjectSize(t *testing.T) {
	URLCacheObjectSize(150)  // 150 bytes
	URLCacheObjectSize(800)  // 800 bytes
	URLCacheObjectSize(1200) // 1.2KiB
	URLCacheObjectSize(3000) // ~3KiB

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_storage_urlcache_object_size A histogram of object sizes in the url cache
# TYPE registry_storage_urlcache_object_size histogram
registry_storage_urlcache_object_size_bucket{le="100"} 0
registry_storage_urlcache_object_size_bucket{le="250"} 1
registry_storage_urlcache_object_size_bucket{le="500"} 1
registry_storage_urlcache_object_size_bucket{le="750"} 1
registry_storage_urlcache_object_size_bucket{le="1000"} 2
registry_storage_urlcache_object_size_bucket{le="1500"} 3
registry_storage_urlcache_object_size_bucket{le="2048"} 3
registry_storage_urlcache_object_size_bucket{le="3072"} 4
registry_storage_urlcache_object_size_bucket{le="5120"} 4
registry_storage_urlcache_object_size_bucket{le="10240"} 4
registry_storage_urlcache_object_size_bucket{le="+Inf"} 4
registry_storage_urlcache_object_size_sum 5150
registry_storage_urlcache_object_size_count 4
`)
	require.NoError(t, err)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, urlCacheObjectSizeName)

	err = testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, totalFullName)
	require.NoError(t, err)
}

// AccessTrackerTestSuite tests the AccessTracker functionality
type AccessTrackerTestSuite struct {
	suite.Suite
	registerer *prometheus.Registry
}

func (s *AccessTrackerTestSuite) SetupTest() {
	s.registerer = prometheus.NewRegistry()
	registerMetrics(s.registerer)
}

func (s *AccessTrackerTestSuite) TestBasicFunctionality() {
	// Create an access tracker with a short tick interval for testing
	at, cleanup := NewAccessTracker(50*time.Millisecond, 10)
	defer cleanup()

	// Track some accesses
	for i := uint64(1); i < 11; i++ {
		for j := uint64(0); j < i; j++ {
			at.Track(i)
		}
	}

	// Wait for the ticker to fire and calculate metrics
	time.Sleep(100 * time.Millisecond)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_storage_access_tracker_dropped_events A counter of dropped events in the access tracker due to timeout
# TYPE registry_storage_access_tracker_dropped_events counter
registry_storage_access_tracker_dropped_events 0
# HELP registry_storage_object_accesses_distribution Distribution of access counts across all objects
# TYPE registry_storage_object_accesses_distribution histogram
registry_storage_object_accesses_distribution_bucket{le="0"} 0
registry_storage_object_accesses_distribution_bucket{le="500"} 10
registry_storage_object_accesses_distribution_bucket{le="1000"} 10
registry_storage_object_accesses_distribution_bucket{le="1500"} 10
registry_storage_object_accesses_distribution_bucket{le="2000"} 10
registry_storage_object_accesses_distribution_bucket{le="2500"} 10
registry_storage_object_accesses_distribution_bucket{le="3000"} 10
registry_storage_object_accesses_distribution_bucket{le="3500"} 10
registry_storage_object_accesses_distribution_bucket{le="4000"} 10
registry_storage_object_accesses_distribution_bucket{le="4500"} 10
registry_storage_object_accesses_distribution_bucket{le="5000"} 10
registry_storage_object_accesses_distribution_bucket{le="+Inf"} 10
registry_storage_object_accesses_distribution_sum 55
registry_storage_object_accesses_distribution_count 10
# HELP registry_storage_object_accesses_topn Total accesses for top N most frequently accessed objects
# TYPE registry_storage_object_accesses_topn gauge
registry_storage_object_accesses_topn{top_n="1"} 10
registry_storage_object_accesses_topn{top_n="10"} 55
registry_storage_object_accesses_topn{top_n="all"} 55
`)
	require.NoError(s.T(), err)
	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, objectAccessesTopNName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, objectAccessesDistributionName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, accessTrackerDroppedEventsName),
	}

	err = testutil.GatherAndCompare(s.registerer, &expected, names...)
	require.NoError(s.T(), err)
}

func (s *AccessTrackerTestSuite) TestConcurrentAccess() {
	at, cleanup := NewAccessTracker(50*time.Millisecond, 1000)
	defer cleanup()

	// Launch multiple goroutines to track events concurrently
	var wg sync.WaitGroup
	numGoroutines := 11
	eventsPerGoroutine := 102

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			for j := 0; j < eventsPerGoroutine; j++ {
				at.Track(id)
			}
		}(uint64(i))
	}

	wg.Wait()

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_storage_access_tracker_dropped_events A counter of dropped events in the access tracker due to timeout
# TYPE registry_storage_access_tracker_dropped_events counter
registry_storage_access_tracker_dropped_events 0
# HELP registry_storage_object_accesses_distribution Distribution of access counts across all objects
# TYPE registry_storage_object_accesses_distribution histogram
registry_storage_object_accesses_distribution_bucket{le="0"} 0
registry_storage_object_accesses_distribution_bucket{le="500"} 11
registry_storage_object_accesses_distribution_bucket{le="1000"} 11
registry_storage_object_accesses_distribution_bucket{le="1500"} 11
registry_storage_object_accesses_distribution_bucket{le="2000"} 11
registry_storage_object_accesses_distribution_bucket{le="2500"} 11
registry_storage_object_accesses_distribution_bucket{le="3000"} 11
registry_storage_object_accesses_distribution_bucket{le="3500"} 11
registry_storage_object_accesses_distribution_bucket{le="4000"} 11
registry_storage_object_accesses_distribution_bucket{le="4500"} 11
registry_storage_object_accesses_distribution_bucket{le="5000"} 11
registry_storage_object_accesses_distribution_bucket{le="+Inf"} 11
registry_storage_object_accesses_distribution_sum 1122
registry_storage_object_accesses_distribution_count 11
# HELP registry_storage_object_accesses_topn Total accesses for top N most frequently accessed objects
# TYPE registry_storage_object_accesses_topn gauge
registry_storage_object_accesses_topn{top_n="1"} 102
registry_storage_object_accesses_topn{top_n="10"} 1020
registry_storage_object_accesses_topn{top_n="all"} 1122
`)
	require.NoError(s.T(), err)
	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, objectAccessesTopNName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, objectAccessesDistributionName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, accessTrackerDroppedEventsName),
	}

	require.EventuallyWithT(
		s.T(),
		func(tt *assert.CollectT) {
			err := testutil.GatherAndCompare(s.registerer, &expected, names...)
			require.NoError(tt, err)
		},
		3*time.Second, 100*time.Millisecond,
	)
}

func (s *AccessTrackerTestSuite) TestLargeObjectIDs() {
	at, cleanup := NewAccessTracker(50*time.Millisecond, 10)
	defer cleanup()

	// Track with maximum uint64 values
	maxUint64 := ^uint64(0)
	at.Track(maxUint64)
	at.Track(maxUint64 - 1)
	at.Track(maxUint64 - 2)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_storage_access_tracker_dropped_events A counter of dropped events in the access tracker due to timeout
# TYPE registry_storage_access_tracker_dropped_events counter
registry_storage_access_tracker_dropped_events 0
# HELP registry_storage_object_accesses_distribution Distribution of access counts across all objects
# TYPE registry_storage_object_accesses_distribution histogram
registry_storage_object_accesses_distribution_bucket{le="0"} 0
registry_storage_object_accesses_distribution_bucket{le="500"} 3
registry_storage_object_accesses_distribution_bucket{le="1000"} 3
registry_storage_object_accesses_distribution_bucket{le="1500"} 3
registry_storage_object_accesses_distribution_bucket{le="2000"} 3
registry_storage_object_accesses_distribution_bucket{le="2500"} 3
registry_storage_object_accesses_distribution_bucket{le="3000"} 3
registry_storage_object_accesses_distribution_bucket{le="3500"} 3
registry_storage_object_accesses_distribution_bucket{le="4000"} 3
registry_storage_object_accesses_distribution_bucket{le="4500"} 3
registry_storage_object_accesses_distribution_bucket{le="5000"} 3
registry_storage_object_accesses_distribution_bucket{le="+Inf"} 3
registry_storage_object_accesses_distribution_sum 3
registry_storage_object_accesses_distribution_count 3
# HELP registry_storage_object_accesses_topn Total accesses for top N most frequently accessed objects
# TYPE registry_storage_object_accesses_topn gauge
registry_storage_object_accesses_topn{top_n="1"} 1
registry_storage_object_accesses_topn{top_n="all"} 3
`)
	require.NoError(s.T(), err)
	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, objectAccessesTopNName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, objectAccessesDistributionName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, accessTrackerDroppedEventsName),
	}

	require.EventuallyWithT(
		s.T(),
		func(tt *assert.CollectT) {
			err := testutil.GatherAndCompare(s.registerer, &expected, names...)
			require.NoError(tt, err)
		},
		3*time.Second, 100*time.Millisecond,
	)
}

func (s *AccessTrackerTestSuite) TestNoAccesses() {
	_, cleanup := NewAccessTracker(50*time.Millisecond, 10)
	defer cleanup()

	// Wait for a tick without tracking anything
	time.Sleep(100 * time.Millisecond)

	var expected bytes.Buffer
	_, err := expected.WriteString(`
# HELP registry_storage_access_tracker_dropped_events A counter of dropped events in the access tracker due to timeout
# TYPE registry_storage_access_tracker_dropped_events counter
registry_storage_access_tracker_dropped_events 0
# HELP registry_storage_object_accesses_distribution Distribution of access counts across all objects
# TYPE registry_storage_object_accesses_distribution histogram
registry_storage_object_accesses_distribution_bucket{le="0"} 0
registry_storage_object_accesses_distribution_bucket{le="500"} 0
registry_storage_object_accesses_distribution_bucket{le="1000"} 0
registry_storage_object_accesses_distribution_bucket{le="1500"} 0
registry_storage_object_accesses_distribution_bucket{le="2000"} 0
registry_storage_object_accesses_distribution_bucket{le="2500"} 0
registry_storage_object_accesses_distribution_bucket{le="3000"} 0
registry_storage_object_accesses_distribution_bucket{le="3500"} 0
registry_storage_object_accesses_distribution_bucket{le="4000"} 0
registry_storage_object_accesses_distribution_bucket{le="4500"} 0
registry_storage_object_accesses_distribution_bucket{le="5000"} 0
registry_storage_object_accesses_distribution_bucket{le="+Inf"} 0
registry_storage_object_accesses_distribution_sum 0
registry_storage_object_accesses_distribution_count 0
`)
	require.NoError(s.T(), err)
	names := []string{
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, objectAccessesTopNName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, objectAccessesDistributionName),
		fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, accessTrackerDroppedEventsName),
	}

	err = testutil.GatherAndCompare(s.registerer, &expected, names...)
	require.NoError(s.T(), err)
}

func TestAccessTrackerSuite(t *testing.T) {
	suite.Run(t, new(AccessTrackerTestSuite))
}
