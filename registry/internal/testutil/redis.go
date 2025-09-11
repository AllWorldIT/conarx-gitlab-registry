package testutil

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	iredis "github.com/docker/distribution/registry/internal/redis"
	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
)

const (
	// RedisCacheTTL defines a duration for the test cache TTL.
	RedisCacheTTL = 30 * time.Second
)

// RedisServer start a new miniredis server and registers the cleanup after the test is done.
// See https://github.com/alicebob/miniredis.
func RedisServer(tb testing.TB) *miniredis.Miniredis {
	tb.Helper()

	return miniredis.RunT(tb)
}

// redisClient starts a new miniredis server and gives back a properly configured client for that server. Also registers
// the cleanup after the test is done
func redisClient(tb testing.TB) redis.UniversalClient {
	tb.Helper()

	srv := RedisServer(tb)
	return redis.NewClient(&redis.Options{Addr: srv.Addr()})
}

// redisCacheImpl creates a new Redis cache for testing. If a client is not provided, a server/client pair is created
// using redisClient. A client can be provided when wanting to use a specific client, such as for mocking purposes. A
// global TTL for cached objects can be specific (defaults to no TTL).
func redisCacheImpl(tb testing.TB, client redis.UniversalClient, ttl time.Duration) *iredis.Cache {
	tb.Helper()

	if client == nil {
		client = redisClient(tb)
	}

	return iredis.NewCache(client, iredis.WithDefaultTTL(ttl))
}

// RedisCache creates a new Redis cache using a new miniredis server and redis client. A global TTL for
// cached objects can be specific (defaults to no TTL).
func RedisCache(tb testing.TB, ttl time.Duration) iredis.CacheInterface {
	tb.Helper()

	return redisCacheImpl(tb, redisClient(tb), ttl)
}

// RedisCacheMock is similar to RedisCache but here we use a redismock client. A global TTL for cached objects can be
// specific (defaults to no TTL).
func RedisCacheMock(tb testing.TB, ttl time.Duration) (iredis.CacheInterface, redismock.ClientMock) {
	client, mock := redismock.NewClientMock()

	return redisCacheImpl(tb, client, ttl), mock
}

// NewRedisCacheController creates a new Redis cache using a new miniredis server and redis client. A global TTL for
// cached objects can be specific (defaults to no TTL).
func NewRedisCacheController(tb testing.TB, ttl time.Duration) RedisCacheController {
	tb.Helper()

	srv := RedisServer(tb)
	return RedisCacheController{
		redisCacheImpl(tb, redis.NewClient(&redis.Options{Addr: srv.Addr()}), ttl),
		srv,
	}
}

// RedisCacheController contains the necessary cache client and underlying redis server used for a test
type RedisCacheController struct {
	*iredis.Cache
	*miniredis.Miniredis
}
