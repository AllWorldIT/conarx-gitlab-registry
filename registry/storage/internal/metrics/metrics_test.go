package metrics

import (
	"bytes"
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

func TestBlobDownload(t *testing.T) {
	restore := mockTimeSince(10 * time.Millisecond)
	defer restore()

	BlobDownload(false, 512)
	BlobDownload(true, 1024)
	BlobDownload(true, 2048)

	var expected bytes.Buffer
	expected.WriteString(`
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
	durationFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, blobDownloadBytesName)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, blobDownloadBytesName)

	err := testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, durationFullName, totalFullName)
	require.NoError(t, err)
}

func TestCDNRedirect(t *testing.T) {
	restore := mockTimeSince(10 * time.Millisecond)
	defer restore()

	CDNRedirect("cdn", false, "")
	CDNRedirect("storage", true, "ip")

	var expected bytes.Buffer
	expected.WriteString(`
# HELP registry_storage_cdn_redirects_total A counter of CDN redirections for blob downloads.
# TYPE registry_storage_cdn_redirects_total counter
registry_storage_cdn_redirects_total{backend="cdn",bypass="false",bypass_reason=""} 1
registry_storage_cdn_redirects_total{backend="storage",bypass="true",bypass_reason="ip"} 1
`)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, cdnRedirectTotalName)

	err := testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, totalFullName)
	require.NoError(t, err)
}

func TestBlobUpload(t *testing.T) {
	restore := mockTimeSince(10 * time.Millisecond)
	defer restore()

	BlobUpload("old", 512)
	BlobUpload("new", 1024)
	BlobUpload("new", 2048)

	var expected bytes.Buffer
	expected.WriteString(`
# HELP registry_storage_blob_upload_bytes A histogram of new blob upload bytes for the storage backend.
# TYPE registry_storage_blob_upload_bytes histogram
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="524288"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="1.048576e+06"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="6.7108864e+07"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="1.34217728e+08"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="2.68435456e+08"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="5.36870912e+08"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="1.073741824e+09"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="2.147483648e+09"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="3.221225472e+09"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="4.294967296e+09"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="5.36870912e+09"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="6.442450944e+09"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="7.516192768e+09"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="8.589934592e+09"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="9.663676416e+09"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="1.073741824e+10"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="2.147483648e+10"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="3.221225472e+10"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="4.294967296e+10"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="5.36870912e+10"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="new",le="+Inf"} 2
registry_storage_blob_upload_bytes_sum{migration_path="new"} 3072
registry_storage_blob_upload_bytes_count{migration_path="new"} 2
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="524288"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="1.048576e+06"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="6.7108864e+07"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="1.34217728e+08"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="2.68435456e+08"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="5.36870912e+08"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="1.073741824e+09"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="2.147483648e+09"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="3.221225472e+09"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="4.294967296e+09"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="5.36870912e+09"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="6.442450944e+09"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="7.516192768e+09"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="8.589934592e+09"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="9.663676416e+09"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="1.073741824e+10"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="2.147483648e+10"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="3.221225472e+10"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="4.294967296e+10"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="5.36870912e+10"} 1
registry_storage_blob_upload_bytes_bucket{migration_path="old",le="+Inf"} 1
registry_storage_blob_upload_bytes_sum{migration_path="old"} 512
registry_storage_blob_upload_bytes_count{migration_path="old"} 1
`)
	durationFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, blobUploadBytesName)
	totalFullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subsystem, blobUploadBytesName)

	err := testutil.GatherAndCompare(prometheus.DefaultGatherer, &expected, durationFullName, totalFullName)
	require.NoError(t, err)
}
