package metrics

import (
	"strconv"
	"time"

	"github.com/docker/distribution/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	blobDownloadBytesHist, blobUploadBytesHist *prometheus.HistogramVec
	cdnRedirectTotal                           *prometheus.CounterVec
	rateLimitStorageTotal                      prometheus.Counter

	timeSince = time.Since // for test purposes only
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
)

func init() {
	buckets := []float64{
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

	blobDownloadBytesHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      blobDownloadBytesName,
			Help:      blobDownloadBytesDesc,
			Buckets:   buckets,
		},
		[]string{redirectLabel},
	)

	blobUploadBytesHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metrics.NamespacePrefix,
			Subsystem: subsystem,
			Name:      blobUploadBytesName,
			Help:      blobUploadBytesDesc,
			Buckets:   buckets,
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

	prometheus.MustRegister(blobDownloadBytesHist)
	prometheus.MustRegister(blobUploadBytesHist)
	prometheus.MustRegister(cdnRedirectTotal)
	prometheus.MustRegister(rateLimitStorageTotal)
}

func BlobDownload(redirect bool, size int64) {
	blobDownloadBytesHist.WithLabelValues(strconv.FormatBool(redirect)).Observe(float64(size))
}

func CDNRedirect(backend string, bypass bool, bypassReason string) {
	cdnRedirectTotal.WithLabelValues(backend, strconv.FormatBool(bypass), bypassReason).Inc()
}

func StorageRatelimit() {
	rateLimitStorageTotal.Inc()
}

func BlobUpload(size int64) {
	blobUploadBytesHist.WithLabelValues().Observe(float64(size))
}
