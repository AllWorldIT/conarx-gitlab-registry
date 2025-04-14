package v2

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/cenkalti/backoff/v4"
	"github.com/docker/distribution/registry/storage/driver/s3-aws/common"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

var _ retry.BackoffDelayer = (*customDelayer)(nil)

type customDelayer struct {
	backoff backoff.BackOff
}

// newCustomDelayer replaces the default delayer provided by aws-sdk-go-v2 with
// a more flexible `cenkalti/backoff/v4` one which also makes s3_v2 driver
// behave in a similar way to its predecessor `s3` driver.
func newCustomDelayer() retry.BackoffDelayer {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = common.DefaultInitialInterval
	b.RandomizationFactor = common.DefaultRandomizationFactor
	b.Multiplier = common.DefaultMultiplier
	b.MaxInterval = common.DefaultMaxInterval
	b.MaxElapsedTime = common.DefaultMaxElapsedTime

	// NOTE(prozlach): We do not specify the max attempts for the backoff here,
	// as it is handled by aws-sdk-go-v2 higher in the stack.

	return &customDelayer{backoff: b}
}

func (cd *customDelayer) BackoffDelay(attempt int, sourceErr error) (time.Duration, error) {
	delay := cd.backoff.NextBackOff()
	log.WithFields(
		log.Fields{
			"attempt": attempt,
			"error":   sourceErr,
			"delay_s": delay.Seconds(),
		},
	).Info("S3: retrying after error")

	return delay, nil
}

var _ aws.Retryer = (*customRetryer)(nil)

type customRetryer struct {
	aws.RetryerV2
	throttler *rate.Limiter
}

func NewCustomRetryer(maxRetries, requestsPerSecond int64, burst int) func() aws.Retryer {
	return func() aws.Retryer {
		var retryer aws.RetryerV2
		if maxRetries <= 0 {
			// Retries are disabled:
			retryer = aws.NopRetryer{}
		} else {
			retryer = retry.NewStandard(
				func(opts *retry.StandardOptions) {
					opts.Backoff = newCustomDelayer()
					opts.MaxAttempts = int(maxRetries)
				},
			)
		}

		return &customRetryer{
			RetryerV2: retryer,
			throttler: rate.NewLimiter(rate.Limit(requestsPerSecond), burst),
		}
	}
}

func (cr *customRetryer) GetRetryToken(ctx context.Context, opErr error) (func(error) error, error) {
	err := cr.throttler.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("throtling GetRetryToken request: %w", err)
	}

	return cr.RetryerV2.GetRetryToken(ctx, opErr)
}

func (cr *customRetryer) GetAttemptToken(ctx context.Context) (func(error) error, error) {
	err := cr.throttler.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("throtling GetAttepmtToken request: %w", err)
	}

	return cr.RetryerV2.GetAttemptToken(ctx)
}
