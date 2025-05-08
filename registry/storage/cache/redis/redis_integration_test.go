//go:build integration && redis_test

package redis_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/docker/distribution/registry/storage/cache/cachecheck"
	rediscache "github.com/docker/distribution/registry/storage/cache/redis"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func poolOptsFromEnv(t *testing.T) *redis.UniversalOptions {
	var db int
	s := os.Getenv("REDIS_DB")
	if s == "" {
		db = 0
	} else {
		i, err := strconv.Atoi(s)
		require.NoError(t, err, "error parsing 'REDIS_DB' environment variable")
		db = i
	}

	return &redis.UniversalOptions{
		Addrs:            strings.Split(os.Getenv("REDIS_ADDR"), ","),
		DB:               db,
		Username:         os.Getenv("REDIS_USERNAME"),
		Password:         os.Getenv("REDIS_PASSWORD"),
		MasterName:       os.Getenv("REDIS_MAIN_NAME"),
		SentinelUsername: os.Getenv("REDIS_SENTINEL_USERNAME"),
		SentinelPassword: os.Getenv("REDIS_SENTINEL_PASSWORD"),
	}
}

func flushDB(t *testing.T, client redis.UniversalClient) {
	require.NoError(t, client.FlushDB(context.Background()).Err(), "unexpected error flushing redis db")
}

// TestRedisLayerInfoCache exercises a live redis instance using the cache
// implementation.
func TestRedisBlobDescriptorCacheProvider(t *testing.T) {
	client := redis.NewUniversalClient(poolOptsFromEnv(t))
	flushDB(t, client)

	cachecheck.CheckBlobDescriptorCache(t, rediscache.NewRedisBlobDescriptorCacheProvider(client))
}
