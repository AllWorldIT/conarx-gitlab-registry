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

//go:embed gcra.lua
var distributedGCRAScript string

type Limiter struct {
	client redis.UniversalClient
	config *configuration.Limiter
	script *redis.Script
}

type Result struct {
	Allowed    int64
	Remaining  int64
	Reset      int64
	RetryAfter time.Duration
}

func New(client redis.UniversalClient, config *configuration.Limiter) (*Limiter, error) {
	script := redis.NewScript(distributedGCRAScript)

	// Pre-load the script to Redis during initialization
	// This ensures the script is cached and subsequent calls use EVALSHA
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Load the script - this caches it in Redis and returns the SHA
	if err := script.Load(ctx, client).Err(); err != nil {
		// Log the error but don't fail initialization
		// The script will be loaded on first use if this fails
		return nil, fmt.Errorf("loading redis rate-limit script into Redis failed: %w", err)
	}
	return &Limiter{
		client: client,
		config: config,
		script: script,
	}, nil
}

func (rl *Limiter) Allowed(ctx context.Context, key string, tokensRequested float64) (*Result, error) {
	currentTime := float64(time.Now().Unix())

	capacity := float64(rl.config.Limit.Burst)

	// Calculate refill rate in tokens per second
	// This handles second/minute/hour periods correctly
	refillRate := float64(rl.config.Limit.Rate) / rl.config.Limit.PeriodDuration.Seconds()

	result, err := rl.script.Run(
		ctx,
		rl.client,
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
