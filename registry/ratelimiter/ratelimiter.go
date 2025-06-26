package ratelimiter

import (
	"context"
	"time"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/internal/redis_rate"

	"github.com/redis/go-redis/v9"
)

// Limiter implements handlers.RateLimiter using Redis as the backend.
type Limiter struct {
	client *redis_rate.Limiter
	config *configuration.Limiter
}

// Result obtained from the Redis rate limiting algorithm.
type Result struct {
	Allowed    int
	Remaining  int
	RetryAfter time.Duration
}

// New creates a new rate limiter instance.
func New(client redis.UniversalClient, config *configuration.Limiter) *Limiter {
	return &Limiter{
		client: redis_rate.NewLimiter(client),
		config: config,
	}
}

// Allowed checks if the specified key is allowed to perform the action based on the configured rate limit.
func (rl *Limiter) Allowed(ctx context.Context, key string) (*Result, error) {
	res, err := rl.client.Allow(ctx, key, redis_rate.Limit{
		Rate:   int(rl.config.Limit.Rate),
		Burst:  int(rl.config.Limit.Burst),
		Period: rl.config.Limit.PeriodDuration,
	})
	if err != nil {
		return nil, err
	}

	return &Result{
		Allowed:    res.Allowed,
		Remaining:  res.Remaining,
		RetryAfter: res.RetryAfter,
	}, nil
}

// Config returns the configuration of this rate limiter.
func (rl *Limiter) Config() *configuration.Limiter {
	return rl.config
}
