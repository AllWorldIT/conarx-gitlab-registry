//go:build include_gcs

// Package googlecdn provides a Google CDN middleware wrapper for the Google Cloud Storage (GCS) storage driver.
package googlecdn

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/internal"
	dstorage "github.com/docker/distribution/registry/storage"
	"github.com/docker/distribution/registry/storage/driver"
	storagemiddleware "github.com/docker/distribution/registry/storage/driver/middleware"
	"github.com/docker/distribution/registry/storage/internal/metrics"
)

// googleCDNStorageMiddleware provides a simple implementation of driver.StorageDriver that constructs temporary
// signed Google CDN URLs for the GCS storage driver layer URL, then issues HTTP Temporary Redirects to this content URL.
type googleCDNStorageMiddleware struct {
	driver.StorageDriver
	googleIPs *googleIPs
	urlSigner *urlSigner
	baseURL   string
	duration  time.Duration
}

var _ driver.StorageDriver = &googleCDNStorageMiddleware{}

// defaultDuration is the default expiration delay for CDN signed URLs
const defaultDuration = 20 * time.Minute

// customGitlabGoogle... are the query params appended to googlecdn signed redirect url
const (
	customGitlabGoogleNamespaceParam  = "gitlab-namespace"
	customGitlabGoogleProjectParam    = "gitlab-project"
	customGitlabGoogleAuthTypeParam   = "gitlab-auth-type"
	customGitlabGoogleObjectSizeParam = "gitlab-size-bytes"
)

// customParamKeys is the mappping between gitlab keys to googlecdn signed-redirect-url query parameter keys
var customParamKeys = map[string]string{
	dstorage.NamespaceKey:   customGitlabGoogleNamespaceParam,
	dstorage.ProjectPathKey: customGitlabGoogleProjectParam,
	dstorage.AuthTypeKey:    customGitlabGoogleAuthTypeParam,
	dstorage.SizeBytesKey:   customGitlabGoogleObjectSizeParam,
}

// newGoogleCDNStorageMiddleware constructs and returns a new Google CDN driver.StorageDriver implementation.
// Required options: baseurl, authtype, privatekey, keyname
// Optional options: duration, updatefrequency, iprangesurl, ipfilteredby
func newGoogleCDNStorageMiddleware(storageDriver driver.StorageDriver, options map[string]interface{}) (driver.StorageDriver, error) {
	// parse baseurl
	base, ok := options["baseurl"]
	if !ok {
		return nil, fmt.Errorf("no baseurl provided")
	}
	baseURL, ok := base.(string)
	if !ok {
		return nil, fmt.Errorf("baseurl must be a string")
	}
	if !strings.Contains(baseURL, "://") {
		baseURL = "https://" + baseURL
	}
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	if _, err := url.Parse(baseURL); err != nil {
		return nil, fmt.Errorf("invalid baseurl: %v", err)
	}

	// parse privatekey
	pk, ok := options["privatekey"]
	if !ok {
		return nil, fmt.Errorf("no privatekey provided")
	}
	keyName, ok := pk.(string)
	if !ok {
		return nil, fmt.Errorf("privatekey must be a string")
	}
	key, err := readKeyFile(keyName)
	if err != nil {
		return nil, fmt.Errorf("failed to read privatekey file: %s", err)
	}

	// parse keyname
	v, ok := options["keyname"]
	if !ok {
		return nil, fmt.Errorf("no keyname provided")
	}
	pkName, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("keyname must be a string")
	}

	urlSigner := newURLSigner(pkName, key)

	// parse duration
	duration := defaultDuration
	if d, ok := options["duration"]; ok {
		switch d := d.(type) {
		case time.Duration:
			duration = d
		case string:
			dur, err := time.ParseDuration(d)
			if err != nil {
				return nil, fmt.Errorf("invalid duration: %s", err)
			}
			duration = dur
		}
	}

	// parse updatefrequency
	updateFrequency := defaultUpdateFrequency
	if v, ok := options["updatefrequency"]; ok {
		switch v := v.(type) {
		case time.Duration:
			updateFrequency = v
		case string:
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("invalid updatefrequency: %s", err)
			}
			updateFrequency = d
		}
	}

	// parse iprangesurl
	ipRangesURL := defaultIPRangesURL
	if v, ok := options["iprangesurl"]; ok {
		if s, ok := v.(string); ok {
			ipRangesURL = s
		} else {
			return nil, fmt.Errorf("iprangesurl must be a string")
		}
	}

	// parse ipfilteredby
	var googleIPs *googleIPs
	if v, ok := options["ipfilteredby"]; ok {
		if ipFilteredBy, ok := v.(string); ok {
			switch strings.ToLower(strings.TrimSpace(ipFilteredBy)) {
			case "", "none":
				googleIPs = nil
			case "gcp":
				googleIPs = newGoogleIPs(ipRangesURL, updateFrequency)
			default:
				return nil, fmt.Errorf("ipfilteredby must be one of the following values: none|gcp")
			}
		} else {
			return nil, fmt.Errorf("ipfilteredby must be a string")
		}
	}

	return &googleCDNStorageMiddleware{
		StorageDriver: storageDriver,
		urlSigner:     urlSigner,
		baseURL:       baseURL,
		duration:      duration,
		googleIPs:     googleIPs,
	}, nil
}

// for testing purposes
var systemClock internal.Clock = clock.New()

// gcsBucketKeyer is any type that is capable of returning the GCS bucket key which should be cached by Google CDN.
type gcsBucketKeyer interface {
	GCSBucketKey(path string) string
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path, possibly using the given
// options.
func (lh *googleCDNStorageMiddleware) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	l := log.GetLogger(log.WithContext(ctx))

	keyer, ok := lh.StorageDriver.(gcsBucketKeyer)
	if !ok {
		l.Warn("the Google CDN middleware does not support this backend storage driver, bypassing")
		metrics.CDNRedirect("gcs", true, "unsupported")
		return lh.StorageDriver.URLFor(ctx, path, options)
	}
	if eligibleForGCS(ctx, lh.googleIPs) {
		metrics.CDNRedirect("gcs", true, "gcp")
		return lh.StorageDriver.URLFor(ctx, path, options)
	}

	metrics.CDNRedirect("cdn", false, "")

	url := lh.baseURL + keyer.GCSBucketKey(path)

	// sign the url
	url, err := lh.urlSigner.Sign(url, time.Now().Add(lh.duration))
	if err != nil {
		return url, err
	}

	// add custom params
	customQueryParams := driver.CustomParams(options, customParamKeys)
	if len(customQueryParams) != 0 {
		url = url + "&" + customQueryParams.Encode()
	}
	return url, err
}

// init registers the Google CDN middleware.
func init() {
	storagemiddleware.Register("googlecdn", newGoogleCDNStorageMiddleware)
}
