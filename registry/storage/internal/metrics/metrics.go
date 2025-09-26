package metrics

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/docker/distribution/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	blobDownloadBytesHist, blobUploadBytesHist *prometheus.HistogramVec
	cdnRedirectTotal                           *prometheus.CounterVec
	rateLimitStorageTotal                      prometheus.Counter

	storageBackendRetriesTotal *prometheus.CounterVec

	urlCacheRequestsTotal      *prometheus.CounterVec
	urlCacheObjectSizeHist     prometheus.Histogram
	objectAccessesTopN         *prometheus.GaugeVec
	objectAccessesDistribution prometheus.Histogram

	accessTrackerDroppedEvents prometheus.Counter
)

const (
	subsystem             = "storage"
	redirectLabel         = "redirect"
	blobDownloadBytesName = "blob_download_bytes"
	blobDownloadBytesDesc = "A histogram of blob download sizes for the storage backend."
	blobUploadBytesName   = "blob_upload_bytes"
	blobUploadBytesDesc   = "A histogram of new blob upload bytes for the storage backend."

	cdnRedirectBackendLabel      = "backend"
	cdnRedirectBypassLabel       = "bypass"
	cdnRedirectBypassReasonLabel = "bypass_reason"
	cdnRedirectTotalName         = "cdn_redirects_total"
	cdnRedirectTotalDesc         = "A counter of CDN redirections for blob downloads."
	rateLimitStorageName         = "rate_limit_total"
	rateLimitStorageDesc         = "A counter of requests to the storage driver that hit a rate limit."

	storageBackendRetriesTotalName = "storage_backend_retries_total"
	storageBackendRetriesTotalDesc = "A counter of retires made while communicating with storage backend."
	// `native` - done using native retry mechanism
	// `custom` -done using our own/custom retry mechanism
	storageBackendRetriesTypeLabel = "retry_type"

	urlCacheRequestsTotalName = "urlcache_requests_total"
	urlCacheRequestsTotalDesc = "A counter of the URL cache middleware requests."
	urlCacheResultLabel       = "result"
	urlCacheReasonLabel       = "reason"
	urlCacheObjectSizeName    = "urlcache_object_size"
	urlCacheObjectSizeDesc    = "A histogram of object sizes in the url cache"

	objectAccessesTopNName  = "object_accesses_topn"
	objectAccessesTopNDesc  = "Total accesses for top N most frequently accessed objects"
	objectAccessesTopNLabel = "top_n"

	objectAccessesDistributionName = "object_accesses_distribution"
	objectAccessesDistributionDesc = "Distribution of access counts across all objects"

	accessTrackerDroppedEventsName = "access_tracker_dropped_events"
	accessTrackerDroppedEventsDesc = "A counter of dropped events in the access tracker due to timeout"
)

func init() {
	// NOTE(prozlach): functions are split in order to make this code more
	// testable. This requires some bigger refactoring though.
	registerMetrics(prometheus.DefaultRegisterer)
}

func registerMetrics(registerer prometheus.Registerer) {
	blobSizeBuckets := []float64{
		512 * 1024,              // 512KiB
		1024 * 1024,             // 1MiB
		1024 * 1024 * 64,        // 64MiB
		1024 * 1024 * 128,       // 128MiB
		1024 * 1024 * 256,       // 256MiB
		1024 * 1024 * 512,       // 512MiB
		1024 * 1024 * 1024,      // 1GiB
		1024 * 1024 * 1024 * 2,  // 2GiB
		1024 * 1024 * 1024 * 3,  // 3GiB
		1024 * 1024 * 1024 * 4,  // 4GiB
		1024 * 1024 * 1024 * 5,  // 5GiB
		1024 * 1024 * 1024 * 6,  // 6GiB
		1024 * 1024 * 1024 * 7,  // 7GiB
		1024 * 1024 * 1024 * 8,  // 8GiB
		1024 * 1024 * 1024 * 9,  // 9GiB
		1024 * 1024 * 1024 * 10, // 10GiB
		1024 * 1024 * 1024 * 20, // 20GiB
		1024 * 1024 * 1024 * 30, // 30GiB
		1024 * 1024 * 1024 * 40, // 40GiB
		1024 * 1024 * 1024 * 50, // 50GiB
	}

	// urlCacheObjectSizeBuckets buckets were calculated with the assumption
	// that the mean object size will be around 1038 bytes.
	urlCacheObjectSizeBuckets := []float64{
		100,       // 100 bytes
		250,       // 250 bytes
		500,       // 500 bytes
		750,       // 750 bytes
		1000,      // 1KiB
		1500,      // 1.5KiB
		2 * 1024,  // 2KiB
		3 * 1024,  // 3KiB
		5 * 1024,  // 5KiB
		10 * 1024, // 10KiB
	}

	blobDownloadBytesHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      blobDownloadBytesName,
			Help:      blobDownloadBytesDesc,
			Buckets:   blobSizeBuckets,
		},
		[]string{redirectLabel},
	)

	blobUploadBytesHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      blobUploadBytesName,
			Help:      blobUploadBytesDesc,
			Buckets:   blobSizeBuckets,
		},
		make([]string, 0),
	)

	cdnRedirectTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      cdnRedirectTotalName,
			Help:      cdnRedirectTotalDesc,
		},
		[]string{cdnRedirectBackendLabel, cdnRedirectBypassLabel, cdnRedirectBypassReasonLabel},
	)

	rateLimitStorageTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      rateLimitStorageName,
			Help:      rateLimitStorageDesc,
		},
	)

	storageBackendRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      storageBackendRetriesTotalName,
			Help:      storageBackendRetriesTotalDesc,
		},
		[]string{storageBackendRetriesTypeLabel},
	)

	urlCacheRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      urlCacheRequestsTotalName,
			Help:      urlCacheRequestsTotalDesc,
		},
		[]string{urlCacheResultLabel, urlCacheReasonLabel},
	)

	urlCacheObjectSizeHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      urlCacheObjectSizeName,
			Help:      urlCacheObjectSizeDesc,
			Buckets:   urlCacheObjectSizeBuckets,
		},
	)

	objectAccessesTopN = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      objectAccessesTopNName,
			Help:      objectAccessesTopNDesc,
		},
		[]string{objectAccessesTopNLabel},
	)

	objectAccessesDistribution = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      objectAccessesDistributionName,
			Help:      objectAccessesDistributionDesc,
			Buckets:   prometheus.ExponentialBuckets(10, 2.0, 11),
		},
	)

	accessTrackerDroppedEvents = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      accessTrackerDroppedEventsName,
			Help:      accessTrackerDroppedEventsDesc,
		},
	)

	registerer.MustRegister(blobDownloadBytesHist)
	registerer.MustRegister(blobUploadBytesHist)
	registerer.MustRegister(cdnRedirectTotal)
	registerer.MustRegister(rateLimitStorageTotal)

	registerer.MustRegister(storageBackendRetriesTotal)

	registerer.MustRegister(urlCacheRequestsTotal)
	registerer.MustRegister(urlCacheObjectSizeHist)
	registerer.MustRegister(objectAccessesTopN)
	registerer.MustRegister(objectAccessesDistribution)

	registerer.MustRegister(accessTrackerDroppedEvents)
}

func BlobDownload(redirect bool, size int64) {
	blobDownloadBytesHist.WithLabelValues(strconv.FormatBool(redirect)).Observe(float64(size))
}

func CDNRedirect(backend string, bypass bool, bypassReason string) {
	cdnRedirectTotal.WithLabelValues(backend, strconv.FormatBool(bypass), bypassReason).Inc()
}

func URLCacheRequest(hit bool, reason string) {
	var counter prometheus.Counter
	if hit {
		counter = urlCacheRequestsTotal.WithLabelValues("hit", "")
	} else {
		counter = urlCacheRequestsTotal.WithLabelValues("miss", reason)
	}
	counter.Inc()
}

func URLCacheObjectSize(size int64) {
	urlCacheObjectSizeHist.Observe(float64(size))
}

func StorageRatelimit() {
	rateLimitStorageTotal.Inc()
}

func BlobUpload(size int64) {
	blobUploadBytesHist.WithLabelValues().Observe(float64(size))
}

func StorageBackendRetry(nativeRetry bool) {
	if nativeRetry {
		storageBackendRetriesTotal.WithLabelValues("native").Inc()
	} else {
		storageBackendRetriesTotal.WithLabelValues("custom").Inc()
	}
}

// AccessTracker tracks object access patterns and emits Prometheus metrics
type AccessTracker struct {
	// Input channel for submitting access events
	submitCh chan uint64

	// Internal state (no mutex needed - single goroutine access)
	counts map[uint64]uint32

	ctx context.Context
}

// NewAccessTracker creates a new AccessTracker
func NewAccessTracker(tickInterval time.Duration, submitBufferLength int) (*AccessTracker, func() error) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := new(sync.WaitGroup)

	at := &AccessTracker{
		submitCh: make(chan uint64, submitBufferLength),
		counts:   make(map[uint64]uint32),
		ctx:      ctx,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		at.run(ctx, tickInterval)
		close(at.submitCh)
	}()

	// Return a cleanup function that cancels context and waits for goroutine
	cleanup := func() error {
		cancel()
		wg.Wait()

		return nil
	}

	return at, cleanup
}

// Track submits an object access event
func (at *AccessTracker) Track(objectID uint64) {
	select {
	case <-at.ctx.Done():
		return
	default:
		select {
		case at.submitCh <- objectID:
		// NOTE(prozlach): Value chosen arbitraly - should not be too high to
		// increase request latency and not to low to give submitting the metrics a
		// chance
		case <-time.After(330 * time.Millisecond):
			// Timeout after 250ms, drop the event, but add a metric
			accessTrackerDroppedEvents.Inc()
		}
	}
}

// run is the main event loop
func (at *AccessTracker) run(ctx context.Context, tickInterval time.Duration) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case objectID := <-at.submitCh:
			at.counts[objectID]++

		case <-ticker.C:
			at.calculateMetrics()
		}
	}
}

// calculateMetrics processes the current counts and emits metrics
func (at *AccessTracker) calculateMetrics() {
	objectIDs := make([]uint64, 0, len(at.counts))
	for id := range at.counts {
		objectIDs = append(objectIDs, id)
	}

	sort.Slice(objectIDs, func(i, j int) bool {
		return at.counts[objectIDs[i]] > at.counts[objectIDs[j]]
	})

	totalAccesses := uint64(0)
	for i, objectID := range objectIDs {
		count := at.counts[objectID]
		totalAccesses += uint64(count)

		// Emit histogram observation for this object
		objectAccessesDistribution.Observe(float64(count))

		// Emit top-N metrics at specific positions
		switch i + 1 {
		case 1:
			objectAccessesTopN.WithLabelValues("1").Set(float64(totalAccesses))
		case 10:
			objectAccessesTopN.WithLabelValues("10").Set(float64(totalAccesses))
		case 100:
			objectAccessesTopN.WithLabelValues("100").Set(float64(totalAccesses))
		case 1000:
			objectAccessesTopN.WithLabelValues("1000").Set(float64(totalAccesses))
		case 10000:
			objectAccessesTopN.WithLabelValues("10000").Set(float64(totalAccesses))
		}

		// Emit the "all" metric
		objectAccessesTopN.WithLabelValues("all").Set(float64(totalAccesses))
	}

	// Clear the map for the next period
	at.counts = make(map[uint64]uint32)
}
