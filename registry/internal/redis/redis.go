package redis

import (
	"context"
	"fmt"
	"time"

	gocache "github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/marshaler"
	libstore "github.com/eko/gocache/lib/v4/store"
	redisstore "github.com/eko/gocache/store/redis/v4"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

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

// Set stores a string value in the cache with optional TTL or other custom options.
func (c *Cache) Set(ctx context.Context, key string, value string, opts ...SetOption) error {
	options := setOptions{
		ttl: c.defaultTTL,
	}
	for _, opt := range opts {
		opt(&options)
	}

	return c.cache.Set(ctx, key, value, libstore.WithExpiration(options.ttl))
}

// Delete removes a cached item by its key.
func (c *Cache) Delete(ctx context.Context, key string) error {
	return c.cache.Delete(ctx, key)
}

// MarshalGet retrieves and unmarshal a cached object into the provided object argument.
func (c *Cache) MarshalGet(ctx context.Context, key string, object any) error {
	_, err := c.marshaler.Get(ctx, key, object)
	return err
}

// MarshalGetWithTTL retrieves the TTL and unmarshal a cached object into the provided object argument.
func (c *Cache) MarshalGetWithTTL(ctx context.Context, key string, object any) (time.Duration, error) {
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
