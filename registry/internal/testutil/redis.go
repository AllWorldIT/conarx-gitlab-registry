package testutil

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	gocache "github.com/eko/gocache/lib/v4/cache"
	libstore "github.com/eko/gocache/lib/v4/store"
	redisstore "github.com/eko/gocache/store/redis/v4"
	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
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

// redisCache creates a new gocache cache based on Redis. If a client is not provided, a server/client pair is created
// using redisClient. A client can be provided when wanting to use a specific client, such as for mocking purposes. A
// global TTL for cached objects can be specific (defaults to no TTL).
func redisCache(tb testing.TB, client redis.UniversalClient, ttl time.Duration) *gocache.Cache[any] {
	tb.Helper()

	if client == nil {
		client = redisClient(tb)
	}

	s := redisstore.NewRedis(client, libstore.WithExpiration(ttl))
	return gocache.New[any](s)
}

// RedisCache creates a new gocache cache based on Redis using a new miniredis server and redis client. A global TTL for
// cached objects can be specific (defaults to no TTL).
func RedisCache(tb testing.TB, ttl time.Duration) *gocache.Cache[any] {
	tb.Helper()

	return redisCache(tb, redisClient(tb), ttl)
}

// RedisCacheMock is similar to RedisCache but here we use a redismock client. A global TTL for cached objects can be
// specific (defaults to no TTL).
func RedisCacheMock(tb testing.TB, ttl time.Duration) (*gocache.Cache[any], redismock.ClientMock) {
	client, mock := redismock.NewClientMock()

	return redisCache(tb, client, ttl), mock
}
