package ratelimiter

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/docker/distribution/configuration"

	"github.com/redis/go-redis/v9"
)

type Limiter struct {
	client redis.UniversalClient
	config *configuration.Limiter
}

type Result struct {
	Allowed    int64
	Remaining  int64
	Reset      int64
	RetryAfter time.Duration
}

func New(client redis.UniversalClient, config *configuration.Limiter) *Limiter {
	return &Limiter{
		client: client,
		config: config,
	}
}

//go:embed gcra.lua
var distributedGCRAScript string

func (rl *Limiter) Allowed(ctx context.Context, key string, tokensRequested float64) (*Result, error) {
	currentTime := float64(time.Now().Unix())

	capacity := float64(rl.config.Limit.Burst)

	// Calculate refill rate in tokens per second
	// This handles second/minute/hour periods correctly
	refillRate := float64(rl.config.Limit.Rate) / rl.config.Limit.PeriodDuration.Seconds()

	result, err := rl.client.Eval(
		ctx,
		distributedGCRAScript,
		[]string{key},
		capacity, refillRate, currentTime, tokensRequested,
	).Result()
	if err != nil {
		return nil, fmt.Errorf("eval of the gcra script failed: %w", err)
	}

	if result == nil {
		return nil, errors.New("eval of the gcra script returned no results")
	}

	results := result.([]any)
	allowed := results[0].(int64)
	remaining := results[1].(int64)
	retryAfterSecs := results[2].(int64)
	reset := results[3].(int64)

	var retryAfter time.Duration
	if retryAfterSecs > 0 {
		retryAfter = time.Duration(retryAfterSecs) * time.Second
	}

	return &Result{
		Allowed:    allowed,
		Remaining:  remaining,
		RetryAfter: retryAfter,
		Reset:      reset,
	}, nil
}

// Config returns the configuration of this rate limiter.
func (rl *Limiter) Config() *configuration.Limiter {
	return rl.config
}
