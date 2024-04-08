//go:build include_gcs && integration

/*
These tests require a GCS bucket and a functional Cloud CDN endpoint.

The following environment variables must be set:
   	- REGISTRY_STORAGE_GCS_BUCKET
   	- GOOGLE_APPLICATION_CREDENTIALS // path to service account JSON credentials file
   	- REGISTRY_MIDDLEWARE_STORAGE_GOOGLECDN_BASEURL
   	- REGISTRY_MIDDLEWARE_STORAGE_GOOGLECDN_PRIVATEKEY
   	- REGISTRY_MIDDLEWARE_STORAGE_GOOGLECDN_KEYNAME

Run the following command to execute these tests:
	$ go test -v -tags=include_gcs,integration github.com/docker/distribution/registry/storage/driver/middleware/googlecdn
*/

package googlecdn

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/benbjohnson/clock"

	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/internal/testutil"
	dstorage "github.com/docker/distribution/registry/storage"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/gcs"
	"github.com/stretchr/testify/require"
)

func skipGCSTest(t *testing.T) {
	bucket := os.Getenv("REGISTRY_STORAGE_GCS_BUCKET")
	creds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")

	if bucket == "" || creds == "" {
		t.Skip(`skipping test as REGISTRY_STORAGE_GCS_BUCKET and GOOGLE_APPLICATION_CREDENTIALS env vars are not 
all set`)
	}
}

func skipCDNTest(t *testing.T) {
	skipGCSTest(t)

	baseURL := os.Getenv("REGISTRY_MIDDLEWARE_STORAGE_GOOGLECDN_BASEURL")
	keyFile := os.Getenv("REGISTRY_MIDDLEWARE_STORAGE_GOOGLECDN_PRIVATEKEY")
	keyName := os.Getenv("REGISTRY_MIDDLEWARE_STORAGE_GOOGLECDN_KEYNAME")

	if baseURL == "" || keyFile == "" || keyName == "" {
		t.Skip(`skipping test as REGISTRY_MIDDLEWARE_STORAGE_GOOGLECDN_BASEURL, 
REGISTRY_MIDDLEWARE_STORAGE_GOOGLECDN_PRIVATEKEY and REGISTRY_MIDDLEWARE_STORAGE_GOOGLECDN_KEYNAME env vars are not all set`)
	}
}

func newGCSDriver(t *testing.T) (driver.StorageDriver, string) {
	t.Helper()

	// generate unique root directory for each test to make them safe for parallel execution
	root := t.TempDir()

	d, err := gcs.FromParameters(map[string]interface{}{
		"bucket":        os.Getenv("REGISTRY_STORAGE_GCS_BUCKET"),
		"keyfile":       os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"),
		"rootdirectory": root,
	})
	require.NoError(t, err)
	require.NotNil(t, d)

	return d, root
}

func TestURLFor(t *testing.T) {
	t.Parallel()
	skipGCSTest(t)

	gcsDriver, root := newGCSDriver(t)

	// we don't need a real CDN and/or object to point to for these tests, we're just doing syntax/logic validations
	keyFile := createTmpKeyFile(t).Name()
	baseURL := "https://my.google.cdn.com"
	keyName := "my-key"
	objectPath := "/foo/bar"

	keyBytes, err := readKeyFile(keyFile)
	require.NoError(t, err)

	// freeze system clock for reproducible URL expiration durations
	clockMock := clock.NewMock()
	clockMock.Set(time.Now())
	testutil.StubClock(t, &systemClock, clockMock)

	// default behavior
	cdnDriver, err := newGoogleCDNStorageMiddleware(gcsDriver, map[string]interface{}{
		"baseurl":    baseURL,
		"privatekey": keyFile,
		"keyname":    keyName,
	})
	require.NoError(t, err)

	cdnURL, err := cdnDriver.URLFor(context.Background(), objectPath, nil)
	require.NoError(t, err)

	expectedURL, err := signURLWithPrefix(
		baseURL+root+objectPath,
		keyName,
		keyBytes,
		systemClock.Now().Add(defaultDuration),
	)

	require.NoError(t, err)
	require.Equal(t, expectedURL, cdnURL)

	// custom duration
	d := 5 * time.Second
	cdnDriver, err = newGoogleCDNStorageMiddleware(gcsDriver, map[string]interface{}{
		"baseurl":    baseURL,
		"privatekey": keyFile,
		"keyname":    keyName,
		"duration":   d,
	})
	require.NoError(t, err)

	cdnURL, err = cdnDriver.URLFor(context.Background(), objectPath, nil)
	require.NoError(t, err)

	expectedURL, err = signURLWithPrefix(
		baseURL+root+objectPath,
		keyName,
		keyBytes,
		clockMock.Now().Add(d),
	)
	require.NoError(t, err)
	require.Equal(t, expectedURL, cdnURL)

	// IP filter ON - generate GCS URL on IP match
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := `{"prefixes": [{"ipv4Prefix": "10.0.0.0/24"}]}`
			fmt.Fprintln(w, resp)
		}),
	)
	defer srv.Close()

	cdnDriver, err = newGoogleCDNStorageMiddleware(gcsDriver, map[string]interface{}{
		"baseurl":      baseURL,
		"privatekey":   keyFile,
		"keyname":      keyName,
		"iprangesurl":  srv.URL,
		"ipfilteredby": "gcp",
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.1"
	ctx := dcontext.WithRequest(context.Background(), req)
	gcsURL, err := cdnDriver.URLFor(ctx, objectPath, nil)
	require.NoError(t, err)
	require.Regexp(t, "^https://storage.googleapis.com/.*", gcsURL)

	// IP filter ON - generate CDN URL if IP does not match
	req.RemoteAddr = "11.0.0.1"
	ctx = dcontext.WithRequest(context.Background(), req)

	cdnURL, err = cdnDriver.URLFor(ctx, objectPath, nil)
	require.NoError(t, err)

	expectedURL, err = signURLWithPrefix(
		baseURL+root+objectPath,
		keyName,
		keyBytes,
		clockMock.Now().Add(defaultDuration),
	)
	require.NoError(t, err)
	require.Equal(t, expectedURL, cdnURL)

	// IP filter OFF - generate CDN URL even if IP matches
	cdnDriver, err = newGoogleCDNStorageMiddleware(gcsDriver, map[string]interface{}{
		"baseurl":      baseURL,
		"privatekey":   keyFile,
		"keyname":      keyName,
		"ipfilteredby": "none",
	})
	require.NoError(t, err)

	req.RemoteAddr = "10.0.0.1"
	ctx = dcontext.WithRequest(context.Background(), req)

	cdnURL, err = cdnDriver.URLFor(ctx, objectPath, nil)
	require.NoError(t, err)

	expectedURL, err = signURLWithPrefix(
		baseURL+root+objectPath,
		keyName,
		keyBytes,
		clockMock.Now().Add(defaultDuration),
	)
	require.NoError(t, err)
	require.Equal(t, expectedURL, cdnURL)

	// IP filter OFF - generate CDN URL if IP does not match
	req.RemoteAddr = "11.0.0.1"
	ctx = dcontext.WithRequest(context.Background(), req)

	cdnURL, err = cdnDriver.URLFor(ctx, objectPath, nil)
	require.NoError(t, err)

	expectedURL, err = signURLWithPrefix(
		baseURL+root+objectPath,
		keyName,
		keyBytes,
		clockMock.Now().Add(defaultDuration),
	)
	require.NoError(t, err)
	require.Equal(t, expectedURL, cdnURL)
}

func TestURLFor_Download(t *testing.T) {
	t.Parallel()
	skipCDNTest(t)

	gcsDriver, _ := newGCSDriver(t)

	// upload sample object to bucket
	objPath := "/foo/bar"
	objContent := []byte("content")
	objChecksum := sha256.Sum256(objContent)

	ctx := dcontext.Background()

	err := gcsDriver.PutContent(ctx, objPath, objContent)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := gcsDriver.Delete(ctx, objPath)
		require.NoError(t, err)
	})

	// probe standard GCS URL
	gcsURL, err := gcsDriver.URLFor(ctx, objPath, nil)
	require.NoError(t, err)
	require.Regexp(t, "^https://storage.googleapis.com/.*", gcsURL)

	resp, err := http.Get(gcsURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, objChecksum, sha256.Sum256(body))

	// probe CDN URL
	baseURL := os.Getenv("REGISTRY_MIDDLEWARE_STORAGE_GOOGLECDN_BASEURL")
	keyFile := os.Getenv("REGISTRY_MIDDLEWARE_STORAGE_GOOGLECDN_PRIVATEKEY")
	keyName := os.Getenv("REGISTRY_MIDDLEWARE_STORAGE_GOOGLECDN_KEYNAME")
	opts := map[string]interface{}{
		"baseurl":    baseURL,
		"privatekey": keyFile,
		"keyname":    keyName,
	}

	cdnDriver, err := newGoogleCDNStorageMiddleware(gcsDriver, opts)
	require.NoError(t, err)

	var tests = []struct {
		name string
		opts map[string]any
	}{
		{
			name: "without custom params",
			opts: nil,
		},
		{
			name: "with custom params",
			opts: map[string]any{
				dstorage.AuthTypeKey:    "pat",
				dstorage.ProjectIdKey:   int64(123),
				dstorage.NamespaceIdKey: int64(456),
				dstorage.SizeBytesKey:   int64(123),
			},
		}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test := test
			t.Parallel()
			cdnURL, err := cdnDriver.URLFor(ctx, objPath, test.opts)
			require.NoError(t, err)
			require.Regexp(t, fmt.Sprintf("^%s.*", baseURL), cdnURL)
			verifyCustomURLParamsExist(t, cdnURL, test.opts)

			resp, err = http.Get(cdnURL)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			body, err = io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, objChecksum, sha256.Sum256(body))
		})
	}
}

// verifyCustomURLParamsExist asserts that all known and present `opts`
// are mapped to their equivalent custom query params value in the `urlString`.
func verifyCustomURLParamsExist(t *testing.T, urlString string, opts map[string]any) {
	if opts == nil {
		return
	}
	urlValue, err := url.Parse(urlString)
	require.NoError(t, err)
	urlQueryParam := urlValue.Query()

	for key, val := range opts {
		if customGitlabKey, ok := customParamKeys[key]; ok {
			require.Equal(t, urlQueryParam.Get(customGitlabKey), fmt.Sprintf("%v", val))
		}
	}
}
