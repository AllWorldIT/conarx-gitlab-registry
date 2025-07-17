// Package urlcache provides a Redis caching middleware wrapper for URLFor calls.
package urlcache

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/internal"
	iredis "github.com/docker/distribution/registry/internal/redis"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/internal/parse"
	storagemiddleware "github.com/docker/distribution/registry/storage/driver/middleware"
	"github.com/docker/distribution/registry/storage/internal/metrics"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// cacheOpTimeout defines the timeout applied to cache operations against Redis
const cacheOpTimeout = 500 * time.Millisecond

// urlCacheStorageMiddleware provides a caching layer for URLFor calls using an in-memory LRU cache.
type urlCacheStorageMiddleware struct {
	driver.StorageDriver
	cache              *iredis.Cache
	dryRun             bool
	minURLValidity     time.Duration
	defaultURLValidity time.Duration
	accessTracker      *metrics.AccessTracker
}

var _ driver.StorageDriver = &urlCacheStorageMiddleware{}

// CacheEntry represents a cached URL with its generation time and expected validity
type CacheEntry struct {
	URL string
}

func (ce CacheEntry) size() int64 {
	const (
		structOverhead = 40 // unsafe.Sizeof(cacheEntry{}) == stringOverhead + timeSize
		stringOverhead = 16 // string header (ptr + len)
	)

	return int64(structOverhead + len(ce.URL))
}

// defaultMinURLValidity is the minimum time a URL must be valid before we serve it from cache
const defaultMinURLValidity = 10 * time.Minute

// newURLCacheStorageMiddleware constructs and returns a new URL cache driver.StorageDriver implementation.
// Required options: none (all options are optional with sensible defaults)
// Optional options: minurlvalidity, defaulturlvalidity, maxcacheentries
func newURLCacheStorageMiddleware(
	storageDriver driver.StorageDriver,
	options map[string]any,
) (driver.StorageDriver, func() error, error) {
	recognizedKeys := map[string]bool{
		"min_url_validity":     true,
		"default_url_validity": true,
		"dry_run":              true,
		"_redisCache":          true,
	}

	// Validate that no extra keys are present
	for key := range options {
		if !recognizedKeys[key] {
			return nil, nil, fmt.Errorf("unrecognized configuration key: %s", key)
		}
	}

	// After: Using the Duration function
	minURLValidity, err := parse.Duration(options, "min_url_validity", defaultMinURLValidity)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid min_url_validity: %w", err)
	}

	// Similarly for defaultURLValidity
	defaultURLValidity, err := parse.Duration(options, "default_url_validity", driver.DefaultSignedURLExpiry)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid default_url_validity: %w", err)
	}

	// Validate that minURLValidity is reasonable compared to defaultURLValidity
	if minURLValidity >= defaultURLValidity {
		return nil, nil, fmt.Errorf("min_url_validity (%v) must be less than defaulturlvalidity (%v)", minURLValidity, defaultURLValidity)
	}

	dryRun, err := parse.Bool(options, "dry_run", false)
	if err != nil {
		return nil, nil, fmt.Errorf("dry_run option must be a boolean or a string that describes boolean: %w", err)
	}

	v, ok := options["_redisCache"]
	if !ok {
		return nil, nil, fmt.Errorf("urlcache middleware requires `cache` Redis configured")
	}
	redisCache, ok := v.(*iredis.Cache)
	if !ok {
		return nil, nil, fmt.Errorf("unusable redis cache object passed to the middleware: %T", v)
	}

	at, stopF := metrics.NewAccessTracker(60*time.Second, 100)

	return &urlCacheStorageMiddleware{
		StorageDriver:      storageDriver,
		dryRun:             dryRun,
		cache:              redisCache,
		minURLValidity:     minURLValidity,
		defaultURLValidity: defaultURLValidity,
		accessTracker:      at,
	}, stopF, nil
}

// for testing purposes
var systemClock internal.Clock = clock.New()

// entryKey creates a consistent Redis key and stats key from the path and options
// For Redis cache we use full sha256 sum, as we can't allow any collisions, as
// they would pose a security risk. Stats key are used for metrics, so 64 bit
// is a good compromise wrt. speed, efficiency and accuracy wrt. collisions.
func (*urlCacheStorageMiddleware) entryKey(path string, options map[string]any) (string, uint64) {
	h := sha256.New()
	_, _ = h.Write([]byte(path))

	if options != nil {
		keys := make([]string, 0, len(options))
		for k := range options {
			if k == "expiry" {
				// NOTE(prozlach): we can't use expiry as a cache key, as it is
				// unique for every cached URL.
				continue
			}
			keys = append(keys, k)
		}

		sort.Strings(keys)

		for _, k := range keys {
			_, _ = fmt.Fprintf(h, "%s:%v", k, options[k])
		}
	}

	bytesSum := h.Sum(nil)
	return fmt.Sprintf("registry:storage:urlcache:%x", bytesSum), binary.BigEndian.Uint64(bytesSum[:8])
}

// getExpiresAt extracts the expiry duration from options or returns default
func (*urlCacheStorageMiddleware) getExpiresAt(options map[string]any) (time.Time, error) {
	if options == nil {
		return systemClock.Now().Add(driver.DefaultSignedURLExpiry), nil
	}

	expiryOption, hasExpiryOption := options["expiry"]
	if !hasExpiryOption {
		return systemClock.Now().Add(driver.DefaultSignedURLExpiry), nil
	}

	expiryTime, ok := expiryOption.(time.Time)
	if !ok {
		return time.Time{}, fmt.Errorf("expiry option must be of type time.Time")
	}
	return expiryTime, nil
}

// logger returns a log.Logger decorated with a key/value pair that uniquely
// identifies all entries as being emitted by this middleware.
func (*urlCacheStorageMiddleware) logger(ctx context.Context) log.Logger {
	return log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
		"component": "registry.storage.middleware.urlcache",
	})
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path, possibly using the given
// options. This implementation adds caching to reduce redundant URLFor calls.
func (uc *urlCacheStorageMiddleware) URLFor(ctx context.Context, path string, options map[string]any) (string, error) {
	l := uc.logger(ctx).WithFields(logrus.Fields{"path": path})

	cacheKey, statsKey := uc.entryKey(path, options)
	entry := new(CacheEntry)

	uc.accessTracker.Track(statsKey)

	getCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	err := uc.cache.UnmarshalGet(getCtx, cacheKey, entry)
	if err == nil {
		l.Debug("URLFor cache hit")
		metrics.URLCacheRequest(true, "")

		if uc.dryRun {
			return uc.StorageDriver.URLFor(ctx, path, options)
		}
		return entry.URL, nil
	}
	if !errors.Is(err, redis.Nil) {
		// NOTE(prozlach): Not critical, we can simply fetch it from backend,
		// but let's make sure it is reflected in stats and in the logs.
		l.WithError(err).Info("fetching signed url entry from Redis")
		metrics.URLCacheRequest(false, "error")

		return uc.StorageDriver.URLFor(ctx, path, options)
	}

	l.Debug("URLFor cache miss")
	metrics.URLCacheRequest(false, "not_found")

	// NOTE(prozlach): currently all storage drivers use
	// driver.DefaultSignedURLExpiry if `expiry` parameter is not set, but in
	// case this changes in the future, it could cause issues when using this
	// middleware as default values would start depending on the type of the
	// driver.
	// Hence we unify the default expiry time. If the expiry was set then this
	// operation will not change anything as expiresAt will be equal to the
	// value in `expiry` key anyway.
	expiresAt, err := uc.getExpiresAt(options)
	if err != nil {
		return "", fmt.Errorf("unable to determine URL expiry time: %w", err)
	}
	if options == nil {
		options = map[string]any{
			"expiry": expiresAt,
		}
	} else {
		options["expiry"] = expiresAt
	}

	url, err := uc.StorageDriver.URLFor(ctx, path, options)
	if err != nil {
		return url, fmt.Errorf("fetching URL from driver: %w", err)
	}

	remainingValidity := expiresAt.Sub(systemClock.Now())
	if remainingValidity <= uc.minURLValidity {
		// NOTE(prozlach): No point in caching it as it will be immediately
		// expired in Redis anyway. Operator needs to adjust the config or the
		// bug needs to be fixed. Log and carry on.
		l.WithFields(logrus.Fields{"remainingValidity": remainingValidity}).Error("requested expiry time violates minimum URL validity setting")
		return url, nil
	}

	setCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	entry = &CacheEntry{
		URL: url,
	}
	metrics.URLCacheObjectSize(entry.size())
	err = uc.cache.MarshalSet(setCtx, cacheKey, entry, iredis.WithTTL(remainingValidity))
	if err != nil {
		l.WithError(err).Info("storing cache entry in Redis")
	}

	return url, nil
}

func init() {
	// nolint: gosec // ignore when backend is already registered
	_ = storagemiddleware.Register("urlcache", newURLCacheStorageMiddleware)
}
