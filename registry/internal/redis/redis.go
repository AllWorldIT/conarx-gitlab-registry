package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/docker/distribution/log"
	gocache "github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/marshaler"
	libstore "github.com/eko/gocache/lib/v4/store"
	redisstore "github.com/eko/gocache/store/redis/v4"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	ErrorNoPrimaryCacheConfigured = errors.New("dual cache: no primary cache was configured")
	ErrorNoStandByCacheConfigured = errors.New("dual cache: no standby cache was configured")
)

// CacheInterface defines the contract for Redis cache operations.
// This interface is implemented by both Cache and DualCache to provide consistent
// caching behavior across different cache configurations.
type CacheInterface interface {
	Get(ctx context.Context, key string) (string, error)
	GetWithTTL(ctx context.Context, key string) (string, time.Duration, error)
	Set(ctx context.Context, key, value string, opts ...SetOption) error
	Delete(ctx context.Context, key string) error
	UnmarshalGet(ctx context.Context, key string, object any) error
	UnmarshalGetWithTTL(ctx context.Context, key string, object any) (time.Duration, error)
	MarshalSet(ctx context.Context, key string, object any, opts ...SetOption) error
	RunScript(ctx context.Context, script *redis.Script, keys []string, args ...any) (any, error)
}

// DualCache provides a cache abstraction that operates with two Redis cache instances:
// a primary cache and a standby cache. It supports dual writes and configurable read
// routing to enable gradual migration between Redis clusters.
//
// Read behavior:
//   - When isReadFromStandByCache is false (default), reads from primary cache
//   - When isReadFromStandByCache is true, reads from standby cache
//
// Write behavior:
//   - When isDualWriteEnabled is false (default), writes only to primary cache
//   - When isDualWriteEnabled is true, writes to both primary and standby caches
//
// Migration strategies:
//
// Strategy 1 - Application-based migration:
//  1. Enable dual writes to both caches (REGISTRY_FF_DUAL_CACHE_WRITE=true)
//  2. Wait for all pre-migration keys in primary to expire (based on TTL)
//  3. Switch reads to standby while continuing dual writes (REGISTRY_FF_DUAL_CACHE_READ_FROM_STANDBY=true)
//  4. Promote standby to primary and disable dual writes (REGISTRY_FF_DUAL_CACHE_SWAP_INSTANCES=true, REGISTRY_FF_DUAL_CACHE_WRITE=false)
//
// Strategy 2 - Config-based migration:
//  1. Enable dual writes to both caches (REGISTRY_FF_DUAL_CACHE_WRITE=true)
//  2. Wait for all pre-migration keys in primary to expire (based on TTL)
//  3. Update primary cache config to point to standby cluster in registry deployment
//  4. Disable dual writes (REGISTRY_FF_DUAL_CACHE_WRITE=false)

type DualCache struct {
	isReadFromStandByCache bool
	isDualWriteEnabled     bool
	useStandByAsPrimary    bool
	primary                CacheInterface
	standBy                CacheInterface
}

// DualCacheOption defines the functional option type for configuring DualCache.
type DualCacheOption func(*DualCache)

// WithReadFromStandBy sets the dual cache to read from the stand by only.
func WithReadFromStandBy() DualCacheOption {
	return func(c *DualCache) {
		c.isReadFromStandByCache = true
	}
}

// WithEnableDualWrite sets the dual cache to write to both the primary and standby cache clusters.
func WithEnableDualWrite() DualCacheOption {
	return func(c *DualCache) {
		c.isDualWriteEnabled = true
	}
}

// Cache is an abstraction on top of Redis, providing caching functionality with TTL and marshaling support.
type Cache struct {
	cache      *gocache.Cache[any]
	marshaler  *marshaler.Marshaler
	client     redis.UniversalClient
	defaultTTL time.Duration
}

// CacheOption defines the functional option type for configuring Cache.
type CacheOption func(*Cache)

// WithDefaultTTL sets the default expiration time for cached keys in the Cache.
func WithDefaultTTL(ttl time.Duration) CacheOption {
	return func(c *Cache) {
		c.defaultTTL = ttl
	}
}

// SetOption defines the functional option type for Set methods.
type SetOption func(*setOptions)

type setOptions struct {
	ttl time.Duration
}

// WithTTL sets a custom TTL for a cache entry when using Set methods.
func WithTTL(ttl time.Duration) SetOption {
	return func(o *setOptions) {
		o.ttl = ttl
	}
}

// NewCache creates a new Cache instance with a Redis client and applies any functional options.
func NewCache(client redis.UniversalClient, opts ...CacheOption) *Cache {
	c := &Cache{client: client}
	for _, opt := range opts {
		opt(c)
	}

	redisStore := redisstore.NewRedis(client, libstore.WithExpiration(c.defaultTTL))
	c.cache = gocache.New[any](redisStore)
	c.marshaler = marshaler.New(c.cache)

	return c
}

// NewDualCache creates a new DualCache instance with primary and standby Redis caches
// and applies any functional options for configuring read/write behavior.
func NewDualCache(primary, standBy CacheInterface, opts ...DualCacheOption) *DualCache {
	c := &DualCache{
		primary: primary,
		standBy: standBy,
	}
	for _, opt := range opts {
		opt(c)
	}

	return c
}

// InjectStandByCache replaces the standby cache with the provided cache instance.
func (c *DualCache) InjectStandByCache(standBy CacheInterface) {
	c.standBy = standBy
}

// UseStandByAsPrimary swaps the primary and standby cache instances, making the
// standby cache become the new primary and vice versa. This is useful for
// when promoting the standby cache to a primary role when migrating to the standby.
func (c *DualCache) UseStandByAsPrimary() {
	c.useStandByAsPrimary = true
	c.primary, c.standBy = c.standBy, c.primary
}

// IsPrimaryCacheEnabled checks if the primary cache is set.
func (c *DualCache) IsPrimaryCacheEnabled() bool {
	return c.primary != nil
}

// Get retrieves a string value from the cache by its key.
func (c *Cache) Get(ctx context.Context, key string) (string, error) {
	value, err := c.cache.Get(ctx, key)
	if err != nil {
		return "", err
	}
	v, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("invalid key value type %T", value)
	}

	return v, nil
}

// Get retrieves a string value from the cache by its key. It reads from either
// the primary or standby cache based on the isReadFromStandByCache configuration.
func (c *DualCache) Get(ctx context.Context, key string) (string, error) {
	if c.primary == nil {
		return "", ErrorNoPrimaryCacheConfigured
	}

	if !c.isReadFromStandByCache {
		result, err := c.primary.Get(ctx, key)
		if err != nil {
			return result, fmt.Errorf("primary cache: %w", err)
		}
		return result, nil
	}

	if c.standBy != nil {
		result, err := c.standBy.Get(ctx, key)
		if err != nil {
			return result, fmt.Errorf("standby cache: %w", err)
		}
		return result, nil
	}

	return "", ErrorNoStandByCacheConfigured
}

// GetWithTTL retrieves a string value and its TTL from the cache by its key.
func (c *Cache) GetWithTTL(ctx context.Context, key string) (string, time.Duration, error) {
	value, ttl, err := c.cache.GetWithTTL(ctx, key)
	if err != nil {
		return "", 0, err
	}
	v, ok := value.(string)
	if !ok {
		return "", 0, fmt.Errorf("invalid key value type %T", value)
	}

	return v, ttl, nil
}

// GetWithTTL retrieves a string value and its TTL from the cache by its key. It reads from either
// the primary or standby cache based on the isReadFromStandByCache configuration.
func (c *DualCache) GetWithTTL(ctx context.Context, key string) (string, time.Duration, error) {
	if c.primary == nil {
		return "", 0, ErrorNoPrimaryCacheConfigured
	}

	if !c.isReadFromStandByCache {
		result, ttl, err := c.primary.GetWithTTL(ctx, key)
		if err != nil {
			return result, ttl, fmt.Errorf("primary cache: %w", err)
		}
		return result, ttl, nil
	}

	if c.standBy != nil {
		result, ttl, err := c.standBy.GetWithTTL(ctx, key)
		if err != nil {
			return result, ttl, fmt.Errorf("standby cache: %w", err)
		}
		return result, ttl, nil
	}
	return "", 0, ErrorNoStandByCacheConfigured
}

// Set stores a string value in the cache with optional TTL or other custom options.
func (c *Cache) Set(ctx context.Context, key, value string, opts ...SetOption) error {
	options := setOptions{
		ttl: c.defaultTTL,
	}
	for _, opt := range opts {
		opt(&options)
	}

	return c.cache.Set(ctx, key, value, libstore.WithExpiration(options.ttl))
}

// Set stores a string value in the cache with optional TTL. It writes to the primary cache
// and optionally to the standby cache when dual write is enabled. Standby write failures
// are logged as warnings but don't cause the operation to fail.
func (c *DualCache) Set(ctx context.Context, key, value string, opts ...SetOption) error {
	if c.primary == nil {
		return ErrorNoPrimaryCacheConfigured
	}

	err := c.primary.Set(ctx, key, value, opts...)
	if err != nil {
		return fmt.Errorf("primary cache: %w", err)
	}

	if c.isDualWriteEnabled && c.standBy != nil {
		err := c.standBy.Set(ctx, key, value, opts...)
		if err != nil {
			log.GetLogger(log.WithContext(ctx)).WithError(err).Warn("standby cache: failed to set key on cache")
		}
	}

	return nil
}

// Delete removes a cached item by its key.
func (c *Cache) Delete(ctx context.Context, key string) error {
	return c.cache.Delete(ctx, key)
}

// Delete removes a cached item by its key. It deletes from the primary cache
// and optionally from the standby cache when dual write is enabled. Standby delete
// failures are logged as warnings but don't cause the operation to fail.
func (c *DualCache) Delete(ctx context.Context, key string) error {
	if c.primary == nil {
		return ErrorNoPrimaryCacheConfigured
	}

	err := c.primary.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("primary cache: %w", err)
	}

	if c.isDualWriteEnabled && c.standBy != nil {
		err := c.standBy.Delete(ctx, key)
		if err != nil {
			log.GetLogger(log.WithContext(ctx)).WithError(err).Warn("standby cache: failed to delete key")
		}
	}

	return nil
}

// UnmarshalGet retrieves and unmarshal a cached object into the provided object argument.
func (c *Cache) UnmarshalGet(ctx context.Context, key string, object any) error {
	_, err := c.marshaler.Get(ctx, key, object)
	return err
}

// UnmarshalGet retrieves and unmarshals a cached object into the provided object argument.
// It reads from either the primary or standby cache based on the isReadFromStandByCache configuration.
func (c *DualCache) UnmarshalGet(ctx context.Context, key string, object any) error {
	if c.primary == nil {
		return ErrorNoPrimaryCacheConfigured
	}

	if !c.isReadFromStandByCache {
		err := c.primary.UnmarshalGet(ctx, key, object)
		if err != nil {
			return fmt.Errorf("primary cache: %w", err)
		}
		return nil
	}

	if c.standBy != nil {
		err := c.standBy.UnmarshalGet(ctx, key, object)
		if err != nil {
			return fmt.Errorf("standby cache: %w", err)
		}
		return nil
	}

	return ErrorNoStandByCacheConfigured
}

// UnmarshalGetWithTTL retrieves the TTL and unmarshal a cached object into the provided object argument.
func (c *Cache) UnmarshalGetWithTTL(ctx context.Context, key string, object any) (time.Duration, error) {
	value, ttl, err := c.cache.GetWithTTL(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key from cache: %w", err)
	}

	switch v := value.(type) {
	case []byte:
		err = msgpack.Unmarshal(v, object)
	case string:
		err = msgpack.Unmarshal([]byte(v), object)
	default:
		err = fmt.Errorf("unexpected key value type: %T", v)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to unmarshal key value: %w", err)
	}

	return ttl, nil
}

// UnmarshalGetWithTTL retrieves and unmarshals a cached object into the provided object argument
// and returns its TTL. It reads from either the primary or standby cache based on the
// isReadFromStandByCache configuration.
func (c *DualCache) UnmarshalGetWithTTL(ctx context.Context, key string, object any) (time.Duration, error) {
	if c.primary == nil {
		return 0, ErrorNoPrimaryCacheConfigured
	}

	if !c.isReadFromStandByCache {
		ttl, err := c.primary.UnmarshalGetWithTTL(ctx, key, object)
		if err != nil {
			return 0, fmt.Errorf("primary cache: %w", err)
		}
		return ttl, nil
	}

	if c.standBy != nil {
		ttl, err := c.standBy.UnmarshalGetWithTTL(ctx, key, object)
		if err != nil {
			return 0, fmt.Errorf("standby cache: %w", err)
		}
		return ttl, nil
	}

	return 0, ErrorNoStandByCacheConfigured
}

// MarshalSet marshals and stores an object in the cache with optional TTL or other custom options.
func (c *Cache) MarshalSet(ctx context.Context, key string, object any, opts ...SetOption) error {
	options := setOptions{
		ttl: c.defaultTTL,
	}
	for _, opt := range opts {
		opt(&options)
	}

	return c.marshaler.Set(ctx, key, object, libstore.WithExpiration(options.ttl))
}

// MarshalSet marshals and stores an object in the cache with optional TTL. It writes to the primary cache
// and optionally to the standby cache when dual write is enabled. Standby write failures
// are logged as warnings but don't cause the operation to fail.
func (c *DualCache) MarshalSet(ctx context.Context, key string, object any, opts ...SetOption) error {
	if c.primary == nil {
		return ErrorNoPrimaryCacheConfigured
	}

	err := c.primary.MarshalSet(ctx, key, object, opts...)
	if err != nil {
		return fmt.Errorf("primary cache: %w", err)
	}

	if c.isDualWriteEnabled && c.standBy != nil {
		err := c.standBy.MarshalSet(ctx, key, object, opts...)
		if err != nil {
			log.GetLogger(log.WithContext(ctx)).WithError(err).Warn("standby cache: failed to marshal set key")
		}
	}

	return nil
}

// RunScript runs a Lua script on Redis with the given keys and arguments.
func (c *Cache) RunScript(ctx context.Context, script *redis.Script, keys []string, args ...any) (any, error) {
	return script.Run(ctx, c.client, keys, args...).Result()
}

// RunScript runs a Lua script on Redis with the given keys and arguments.
// It executes on the primary cache and optionally on the standby cache when dual write is enabled.
// Returns the result from the primary cache. Standby script failures are logged but don't cause the operation to fail.
func (c *DualCache) RunScript(ctx context.Context, script *redis.Script, keys []string, args ...any) (any, error) {
	var result any
	var err error

	if c.primary == nil {
		return result, ErrorNoPrimaryCacheConfigured
	}

	result, err = c.primary.RunScript(ctx, script, keys, args...)
	if err != nil {
		return result, fmt.Errorf("primary cache: %w", err)
	}

	if c.isDualWriteEnabled && c.standBy != nil {
		_, err := c.standBy.RunScript(ctx, script, keys, args...)
		if err != nil {
			log.GetLogger(log.WithContext(ctx)).WithError(err).Warn("standby cache: failed to run script")
		}
	}

	return result, nil
}
