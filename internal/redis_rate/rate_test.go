//go:build integration

package redis_rate_test

// Copyright (c) 2013 The github.com/go-redis/redis_rate Authors.
// https://github.com/go-redis/redis_rate/blob/v10/rate_test.go

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/docker/distribution/internal/redis_rate"
)

func rateLimiterWithKeyPrefix(keyPrefix string) *redis_rate.Limiter {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{"server0": os.Getenv("REDIS_ADDR")},
	})
	if err := ring.FlushDB(context.TODO()).Err(); err != nil {
		panic(err)
	}
	return redis_rate.NewLimiter(ring, redis_rate.WithKeyPrefix(keyPrefix))
}

func rateLimiter() *redis_rate.Limiter {
	return rateLimiterWithKeyPrefix(redis_rate.DefaultRedisPrefix)
}

func TestMain(m *testing.M) {
	if os.Getenv("REDIS_ADDR") == "" {
		fmt.Println("REDIS_ADDR env var must be set, skipping")
		os.Exit(0)
	}

	os.Exit(m.Run())
}

func TestAllow(t *testing.T) {
	ctx := context.Background()

	tcs := []struct {
		keyPrefix string
	}{
		{keyPrefix: ""},
		{keyPrefix: redis_rate.DefaultRedisPrefix},
		{keyPrefix: "test:"},
	}
	for _, tc := range tcs {
		t.Run(tc.keyPrefix, func(t *testing.T) {
			l := rateLimiterWithKeyPrefix(tc.keyPrefix)
			limit := redis_rate.PerSecond(10)
			require.Equal(t, limit.String(), "10 req/s (burst 10)")
			require.False(t, limit.IsZero())

			res, err := l.Allow(ctx, "test_id", limit)
			require.Nil(t, err)
			require.Equal(t, res.Allowed, 1)
			require.Equal(t, res.Remaining, 9)
			require.Equal(t, res.RetryAfter, time.Duration(-1))
			require.InDelta(t, res.ResetAfter, 100*time.Millisecond, float64(10*time.Millisecond))

			err = l.Reset(ctx, "test_id")
			require.Nil(t, err)
			res, err = l.Allow(ctx, "test_id", limit)
			require.Nil(t, err)
			require.Equal(t, res.Allowed, 1)
			require.Equal(t, res.Remaining, 9)
			require.Equal(t, res.RetryAfter, time.Duration(-1))
			require.InDelta(t, res.ResetAfter, 100*time.Millisecond, float64(10*time.Millisecond))

			res, err = l.AllowN(ctx, "test_id", limit, 2)
			require.Nil(t, err)
			require.Equal(t, res.Allowed, 2)
			require.Equal(t, res.Remaining, 7)
			require.Equal(t, res.RetryAfter, time.Duration(-1))
			require.InDelta(t, res.ResetAfter, 300*time.Millisecond, float64(10*time.Millisecond))

			res, err = l.AllowN(ctx, "test_id", limit, 7)
			require.Nil(t, err)
			require.Equal(t, res.Allowed, 7)
			require.Equal(t, res.Remaining, 0)
			require.Equal(t, res.RetryAfter, time.Duration(-1))
			require.InDelta(t, res.ResetAfter, 999*time.Millisecond, float64(10*time.Millisecond))

			res, err = l.AllowN(ctx, "test_id", limit, 1000)
			require.Nil(t, err)
			require.Equal(t, res.Allowed, 0)
			require.Equal(t, res.Remaining, 0)
			require.InDelta(t, res.RetryAfter, 99*time.Second, float64(time.Second))
			require.InDelta(t, res.ResetAfter, 999*time.Millisecond, float64(10*time.Millisecond))
		})
	}
}

func TestAllowN_IncrementZero(t *testing.T) {
	ctx := context.Background()
	l := rateLimiter()
	limit := redis_rate.PerSecond(10)

	// Check for a row that's not there
	res, err := l.AllowN(ctx, "test_id", limit, 0)
	require.Nil(t, err)
	require.Equal(t, res.Allowed, 0)
	require.Equal(t, res.Remaining, 10)
	require.Equal(t, res.RetryAfter, time.Duration(-1))
	require.Equal(t, res.ResetAfter, time.Duration(0))

	// Now increment it
	res, err = l.Allow(ctx, "test_id", limit)
	require.Nil(t, err)
	require.Equal(t, res.Allowed, 1)
	require.Equal(t, res.Remaining, 9)
	require.Equal(t, res.RetryAfter, time.Duration(-1))
	require.InDelta(t, res.ResetAfter, 100*time.Millisecond, float64(10*time.Millisecond))

	// Peek again
	res, err = l.AllowN(ctx, "test_id", limit, 0)
	require.Nil(t, err)
	require.Equal(t, res.Allowed, 0)
	require.Equal(t, res.Remaining, 9)
	require.Equal(t, res.RetryAfter, time.Duration(-1))
	require.InDelta(t, res.ResetAfter, 100*time.Millisecond, float64(10*time.Millisecond))
}

func TestRetryAfter(t *testing.T) {
	limit := redis_rate.Limit{
		Rate:   1,
		Period: time.Millisecond,
		Burst:  1,
	}

	ctx := context.Background()
	l := rateLimiter()

	for i := 0; i < 1000; i++ {
		res, err := l.Allow(ctx, "test_id", limit)
		require.Nil(t, err)

		if res.Allowed > 0 {
			continue
		}

		require.LessOrEqual(t, int64(res.RetryAfter), int64(time.Millisecond))
	}
}

func TestAllowAtMost(t *testing.T) {
	ctx := context.Background()

	l := rateLimiter()
	limit := redis_rate.PerSecond(10)

	res, err := l.Allow(ctx, "test_id", limit)
	require.Nil(t, err)
	require.Equal(t, res.Allowed, 1)
	require.Equal(t, res.Remaining, 9)
	require.Equal(t, res.RetryAfter, time.Duration(-1))
	require.InDelta(t, res.ResetAfter, 100*time.Millisecond, float64(10*time.Millisecond))

	res, err = l.AllowAtMost(ctx, "test_id", limit, 2)
	require.Nil(t, err)
	require.Equal(t, res.Allowed, 2)
	require.Equal(t, res.Remaining, 7)
	require.Equal(t, res.RetryAfter, time.Duration(-1))
	require.InDelta(t, res.ResetAfter, 300*time.Millisecond, float64(10*time.Millisecond))

	res, err = l.AllowN(ctx, "test_id", limit, 0)
	require.Nil(t, err)
	require.Equal(t, res.Allowed, 0)
	require.Equal(t, res.Remaining, 7)
	require.Equal(t, res.RetryAfter, time.Duration(-1))
	require.InDelta(t, res.ResetAfter, 300*time.Millisecond, float64(10*time.Millisecond))

	res, err = l.AllowAtMost(ctx, "test_id", limit, 10)
	require.Nil(t, err)
	require.Equal(t, res.Allowed, 7)
	require.Equal(t, res.Remaining, 0)
	require.Equal(t, res.RetryAfter, time.Duration(-1))
	require.InDelta(t, res.ResetAfter, 999*time.Millisecond, float64(10*time.Millisecond))

	res, err = l.AllowN(ctx, "test_id", limit, 0)
	require.Nil(t, err)
	require.Equal(t, res.Allowed, 0)
	require.Equal(t, res.Remaining, 0)
	require.Equal(t, res.RetryAfter, time.Duration(-1))
	require.InDelta(t, res.ResetAfter, 999*time.Millisecond, float64(10*time.Millisecond))

	res, err = l.AllowAtMost(ctx, "test_id", limit, 1000)
	require.Nil(t, err)
	require.Equal(t, res.Allowed, 0)
	require.Equal(t, res.Remaining, 0)
	require.InDelta(t, res.RetryAfter, 99*time.Millisecond, float64(10*time.Millisecond))
	require.InDelta(t, res.ResetAfter, 999*time.Millisecond, float64(10*time.Millisecond))

	res, err = l.AllowN(ctx, "test_id", limit, 1000)
	require.Nil(t, err)
	require.Equal(t, res.Allowed, 0)
	require.Equal(t, res.Remaining, 0)
	require.InDelta(t, res.RetryAfter, 99*time.Second, float64(time.Second))
	require.InDelta(t, res.ResetAfter, 999*time.Millisecond, float64(10*time.Millisecond))
}

func TestAllowAtMost_IncrementZero(t *testing.T) {
	ctx := context.Background()
	l := rateLimiter()
	limit := redis_rate.PerSecond(10)

	// Check for a row that isn't there
	res, err := l.AllowAtMost(ctx, "test_id", limit, 0)
	require.Nil(t, err)
	require.Equal(t, res.Allowed, 0)
	require.Equal(t, res.Remaining, 10)
	require.Equal(t, res.RetryAfter, time.Duration(-1))
	require.Equal(t, res.ResetAfter, time.Duration(0))

	// Now increment it
	res, err = l.Allow(ctx, "test_id", limit)
	require.Nil(t, err)
	require.Equal(t, res.Allowed, 1)
	require.Equal(t, res.Remaining, 9)
	require.Equal(t, res.RetryAfter, time.Duration(-1))
	require.InDelta(t, res.ResetAfter, 100*time.Millisecond, float64(10*time.Millisecond))

	// Peek again
	res, err = l.AllowAtMost(ctx, "test_id", limit, 0)
	require.Nil(t, err)
	require.Equal(t, res.Allowed, 0)
	require.Equal(t, res.Remaining, 9)
	require.Equal(t, res.RetryAfter, time.Duration(-1))
	require.InDelta(t, res.ResetAfter, 100*time.Millisecond, float64(10*time.Millisecond))
}

func BenchmarkAllow(b *testing.B) {
	ctx := context.Background()
	l := rateLimiter()
	limit := redis_rate.PerSecond(1e6)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res, err := l.Allow(ctx, "foo", limit)
			if err != nil {
				b.Fatal(err)
			}
			if res.Allowed == 0 {
				panic("not reached")
			}
		}
	})
}

func BenchmarkAllowAtMost(b *testing.B) {
	ctx := context.Background()
	l := rateLimiter()
	limit := redis_rate.PerSecond(1e6)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res, err := l.AllowAtMost(ctx, "foo", limit, 1)
			if err != nil {
				b.Fatal(err)
			}
			if res.Allowed == 0 {
				panic("not reached")
			}
		}
	})
}
