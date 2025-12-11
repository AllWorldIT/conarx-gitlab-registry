//go:build integration

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

func isEligible(t *testing.T) {
	t.Helper()

	if os.Getenv("KV_ADDR") == "" {
		t.Skip("the 'KV_ADDR' environment variable must be set to enable these tests")
	}
}

func poolOptsFromEnv(t *testing.T) *redis.UniversalOptions {
	var db int
	s := os.Getenv("KV_DB")
	if s == "" {
		db = 0
	} else {
		i, err := strconv.Atoi(s)
		require.NoError(t, err, "error parsing 'KV_DB' environment variable")
		db = i
	}

	return &redis.UniversalOptions{
		Addrs:            strings.Split(os.Getenv("KV_ADDR"), ","),
		DB:               db,
		Username:         os.Getenv("KV_USERNAME"),
		Password:         os.Getenv("KV_PASSWORD"),
		MasterName:       os.Getenv("KV_MAIN_NAME"),
		SentinelUsername: os.Getenv("KV_SENTINEL_USERNAME"),
		SentinelPassword: os.Getenv("KV_SENTINEL_PASSWORD"),
	}
}

func flushDB(t *testing.T, client redis.UniversalClient) {
	isEligible(t)

	require.NoError(t, client.FlushDB(context.Background()).Err(), "unexpected error flushing redis db")
}

// TestRedisLayerInfoCache exercises a live redis instance using the cache
// implementation.
func TestRedisBlobDescriptorCacheProvider(t *testing.T) {
	client := redis.NewUniversalClient(poolOptsFromEnv(t))
	flushDB(t, client)

	cachecheck.CheckBlobDescriptorCache(t, rediscache.NewRedisBlobDescriptorCacheProvider(client))
}
