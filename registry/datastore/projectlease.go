package datastore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/docker/distribution/log"
	"github.com/opencontainers/go-digest"

	gocache "github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/marshaler"
	"github.com/eko/gocache/lib/v4/store"
	"github.com/redis/go-redis/v9"
)

var errLeasePathIsEmpty = errors.New("project lease path can not be empty")

// ProjectLeaseStore is used to manage access to a project lease resource in the cache
type projectLeaseStore struct {
	*centralProjectLeaseCache
}

// NewProjectLeaseStore builds a new projectLeaseStore.
func NewProjectLeaseStore(cache *centralProjectLeaseCache) (*projectLeaseStore, error) {
	if cache == nil {
		return nil, errors.New("cache can not be empty")
	}
	rlStore := &projectLeaseStore{cache}
	return rlStore, nil
}

// centralProjectLeaseCache is the interface for the centralized project lease cache backed by Redis.
type centralProjectLeaseCache struct {
	// cache provides access to the raw gocache interface
	cache *gocache.Cache[any]
	// marshaler provides access to a MessagePack backed marshaling interface
	marshaler *marshaler.Marshaler
}

// NewCentralProjectLeaseCache creates an interface for the centralized project lease cache backed by Redis.
func NewCentralProjectLeaseCache(cache *gocache.Cache[any]) *centralProjectLeaseCache {
	return &centralProjectLeaseCache{cache, marshaler.New(cache)}
}

// key generates a valid Redis key string for a given project lease object. The used key format is described in
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/redis-dev-guidelines.md#key-format.
func (c *centralProjectLeaseCache) key(path string) string {
	groupPrefix := strings.Split(path, "/")[0]
	hex := digest.FromString(path).Hex()
	return fmt.Sprintf("registry:api:{project-lease:%s:%s}", groupPrefix, hex)
}

// Exists checks if a project lease exists in the cache.
func (c *centralProjectLeaseCache) Exists(ctx context.Context, path string) (bool, error) {
	getCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	_, err := c.cache.Get(getCtx, c.key(path))
	if err != nil {
		// redis.Nil is returned when the key is not found in Redis
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	return true, err
}

// Set a project lease in the cache.
func (c *centralProjectLeaseCache) Set(ctx context.Context, path string, ttl time.Duration) error {
	if path == "" {
		return errLeasePathIsEmpty
	}
	setCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	return c.cache.Set(setCtx, c.key(path), path, store.WithExpiration(ttl))
}

// Invalidate the lease for a given project path in the cache.
func (c *centralProjectLeaseCache) Invalidate(ctx context.Context, path string) error {
	invalCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	err := c.cache.Delete(invalCtx, c.key(path))
	if err != nil {
		log.GetLogger(log.WithContext(ctx)).WithError(err).WithFields(log.Fields{"lease_path": path}).Warn("failed to invalidate project lease")
	}
	return err
}
