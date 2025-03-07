package cache

import (
	"context"

	"github.com/docker/distribution"
	"github.com/docker/distribution/log"
	prometheus "github.com/docker/distribution/metrics"
	"github.com/opencontainers/go-digest"
)

// Metrics is used to hold metric counters
// related to the number of times a cache was
// hit or missed.
type Metrics struct {
	Requests uint64
	Hits     uint64
	Misses   uint64
}

// Logger can be provided on the MetricsTracker to log errors.
//
// Usually, this is just a proxy to log.GetLogger.
type Logger interface {
	Errorf(format string, args ...any)
}

// MetricsTracker represents a metric tracker
// which simply counts the number of hits and misses.
type MetricsTracker interface {
	Hit()
	Miss()
	Metrics() Metrics
	Logger(context.Context) Logger
}

type cachedBlobStatter struct {
	cache   distribution.BlobDescriptorService
	backend distribution.BlobDescriptorService
	tracker MetricsTracker
}

// cacheCount is the number of total cache request received/hits/misses
var cacheCount = prometheus.StorageNamespace.NewLabeledCounter("cache", "The number of cache request received", "type")

// NewCachedBlobStatter creates a new statter which prefers a cache and
// falls back to a backend.
func NewCachedBlobStatter(cache, backend distribution.BlobDescriptorService) distribution.BlobDescriptorService {
	return &cachedBlobStatter{
		cache:   cache,
		backend: backend,
	}
}

func (cbds *cachedBlobStatter) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	cacheCount.WithValues("Request").Inc(1)

	// try getting from cache
	desc, cacheErr := cbds.cache.Stat(ctx, dgst)
	if cacheErr == nil {
		cacheCount.WithValues("Hit").Inc(1)
		if cbds.tracker != nil {
			cbds.tracker.Hit()
		}
		return desc, nil
	}

	// couldn't get from cache; get from backend
	desc, err := cbds.backend.Stat(ctx, dgst)
	if err != nil {
		return desc, err
	}

	if cacheErr == distribution.ErrBlobUnknown {
		// cache doesn't have info. update it with info got from backend
		cacheCount.WithValues("Miss").Inc(1)
		if cbds.tracker != nil {
			cbds.tracker.Miss()
		}
		if err := cbds.cache.SetDescriptor(ctx, dgst, desc); err != nil {
			log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"blob": dgst}).WithError(err).Error("error from cache setting desc")
		}
		// we don't need to return cache error upstream if any. continue returning value from backend
		return desc, nil
	}

	// unknown error from cache. just log and error. do not store cache as it may be trigger many set calls
	log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"blob": dgst}).WithError(cacheErr).Error("error from cache stat(ing) blob")
	cacheCount.WithValues("Error").Inc(1)

	return desc, nil
}

func (cbds *cachedBlobStatter) Clear(ctx context.Context, dgst digest.Digest) error {
	err := cbds.cache.Clear(ctx, dgst)
	if err != nil {
		return err
	}

	err = cbds.backend.Clear(ctx, dgst)
	if err != nil {
		return err
	}
	return nil
}

func (cbds *cachedBlobStatter) SetDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.Descriptor) error {
	if err := cbds.cache.SetDescriptor(ctx, dgst, desc); err != nil {
		log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"blob": dgst}).WithError(err).Error("error from cache setting desc")
	}
	return nil
}
