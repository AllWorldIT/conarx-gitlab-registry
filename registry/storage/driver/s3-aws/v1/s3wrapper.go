package v1

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/cenkalti/backoff/v4"
	"github.com/docker/distribution/registry/storage/driver/s3-aws/common"
	"golang.org/x/time/rate"
)

// The SDK does not define these constants in the s3 package, other packages
// define some of them but not both, and the context of each package
// name will make it confusing.
// Full list of errors https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
const (
	errCodeInternalError = "InternalError"
	errCodeSlowDown      = "SlowDown"
)

// retryableErrors contains a list of recommended errors to retry for certain S3 errors
// https://repost.aws/knowledge-center/http-5xx-errors-s3
var retryableErrors = []string{
	errCodeInternalError,
	errCodeSlowDown,
}

type backoffConstructor func() backoff.BackOff

func withNoExponentialBackoff() backoff.BackOff {
	return &noBackoff{}
}

// noBackoff disables exponential backoffs.
type noBackoff struct{}

// NextBackOff always returns backoff.Stop to signal the caller not to retry the operation.
func (*noBackoff) NextBackOff() time.Duration {
	return backoff.Stop
}

// Reset to initial state.
func (*noBackoff) Reset() {}

type WrapperOpt func(*S3wrapper)

func WithRateLimit(maximum int64, burst int) WrapperOpt {
	return func(w *S3wrapper) {
		w.Limiter = rate.NewLimiter(rate.Limit(maximum), burst)
	}
}

func WithBackoffNotify(n backoff.Notify) WrapperOpt {
	return func(w *S3wrapper) {
		w.notify = n
	}
}

func WithExponentialBackoff(maximum int64) WrapperOpt {
	if maximum < 0 {
		maximum = 0
	}

	return func(w *S3wrapper) {
		w.backoff = func() backoff.BackOff {
			b := backoff.NewExponentialBackOff()
			b.InitialInterval = common.DefaultInitialInterval
			b.RandomizationFactor = common.DefaultRandomizationFactor
			b.Multiplier = common.DefaultMultiplier
			b.MaxInterval = common.DefaultMaxInterval
			b.MaxElapsedTime = common.DefaultMaxElapsedTime

			// nolint:gosec // there is no overflow here, max is always positive
			return backoff.WithMaxRetries(b, uint64(maximum))
		}
	}
}

var _ common.S3WrapperIf = (*S3wrapper)(nil)

// S3wrapper implements a subset of s3iface.S3API allowing us to rate limit,
// retry, add trace logging, or otherwise improve s3 calls made by the driver.
type S3wrapper struct {
	s3 s3iface.S3API
	*rate.Limiter
	backoff backoffConstructor
	notify  backoff.Notify
}

func NewS3Wrapper(s3API s3iface.S3API, opts ...WrapperOpt) *S3wrapper {
	w := &S3wrapper{
		s3:      s3API,
		Limiter: rate.NewLimiter(rate.Inf, 0),
		backoff: withNoExponentialBackoff,
	}

	for _, o := range opts {
		o(w)
	}

	return w
}

func (w *S3wrapper) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	var out *s3.PutObjectOutput

	err := w.waitRetryNotify(
		ctx,
		func() error {
			var err error
			out, err = w.s3.PutObjectWithContext(ctx, input, opts...)

			// a nil response must be captured as an error (if no error is provided)
			if out == nil && err == nil {
				err = nilRespError("PutObjectWithContext")
			}
			return err
		},
	)

	return out, err
}

func (w *S3wrapper) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	var out *s3.GetObjectOutput

	err := w.waitRetryNotify(
		ctx,
		func() error {
			var err error
			out, err = w.s3.GetObjectWithContext(ctx, input, opts...)

			// a nil response must be captured as an error (if no error is provided)
			if out == nil && err == nil {
				err = nilRespError("GetObjectWithContext")
			}
			return err
		},
	)

	return out, err
}

func (w *S3wrapper) CreateMultipartUploadWithContext(ctx aws.Context, input *s3.CreateMultipartUploadInput, opts ...request.Option) (*s3.CreateMultipartUploadOutput, error) {
	var out *s3.CreateMultipartUploadOutput

	err := w.waitRetryNotify(
		ctx,
		func() error {
			var err error
			out, err = w.s3.CreateMultipartUploadWithContext(ctx, input, opts...)

			// a nil response must be captured as an error (if no error is provided)
			if out == nil && err == nil {
				err = nilRespError("CreateMultipartUploadWithContext")
			}
			return err
		},
	)

	return out, err
}

func (w *S3wrapper) ListMultipartUploadsWithContext(ctx aws.Context, input *s3.ListMultipartUploadsInput, opts ...request.Option) (*s3.ListMultipartUploadsOutput, error) {
	var out *s3.ListMultipartUploadsOutput

	err := w.waitRetryNotify(
		ctx,
		func() error {
			var err error
			out, err = w.s3.ListMultipartUploadsWithContext(ctx, input, opts...)

			// a nil response must be captured as an error (if no error is provided)
			if out == nil && err == nil {
				err = nilRespError("ListMultipartUploadsWithContext")
			}
			return err
		},
	)

	return out, err
}

func (w *S3wrapper) HeadObjectWithContext(ctx aws.Context, input *s3.HeadObjectInput, opts ...request.Option) (*s3.HeadObjectOutput, error) {
	var out *s3.HeadObjectOutput

	err := w.waitRetryNotify(
		ctx,
		func() error {
			var err error
			out, err = w.s3.HeadObjectWithContext(ctx, input, opts...)

			// a nil response must be captured as an error (if no error is provided)
			if out == nil && err == nil {
				err = nilRespError("HeadObjectWithContext")
			}
			return err
		},
	)

	return out, err
}

func (w *S3wrapper) ListPartsWithContext(ctx aws.Context, input *s3.ListPartsInput, opts ...request.Option) (*s3.ListPartsOutput, error) {
	var out *s3.ListPartsOutput

	err := w.waitRetryNotify(
		ctx,
		func() error {
			var err error
			out, err = w.s3.ListPartsWithContext(ctx, input, opts...)
			// make sure we never have a situation where `IsTruncated` is empty if we have a response
			if out != nil {
				if out.IsTruncated == nil {
					out.IsTruncated = new(bool)
				}
			}

			// a nil response must be captured as an error (if no error is provided)
			if out == nil && err == nil {
				err = nilRespError("ListPartsWithContext")
			}
			return err
		},
	)

	return out, err
}

func (w *S3wrapper) ListObjectsV2WithContext(ctx aws.Context, input *s3.ListObjectsV2Input, opts ...request.Option) (*s3.ListObjectsV2Output, error) {
	var out *s3.ListObjectsV2Output

	err := w.waitRetryNotify(
		ctx,
		func() error {
			var err error
			out, err = w.s3.ListObjectsV2WithContext(ctx, input, opts...)

			// a nil response must be captured as an error (if no error is provided)
			if out == nil && err == nil {
				err = nilRespError("ListObjectsV2WithContext")
			}
			return err
		},
	)

	return out, err
}

func (w *S3wrapper) CopyObjectWithContext(ctx aws.Context, input *s3.CopyObjectInput, opts ...request.Option) (*s3.CopyObjectOutput, error) {
	var out *s3.CopyObjectOutput

	err := w.waitRetryNotify(
		ctx,
		func() error {
			var err error
			out, err = w.s3.CopyObjectWithContext(ctx, input, opts...)

			// a nil response must be captured as an error (if no error is provided)
			if out == nil && err == nil {
				err = nilRespError("CopyObjectWithContext")
			}
			return err
		},
	)

	return out, err
}

func (w *S3wrapper) UploadPartCopyWithContext(ctx aws.Context, input *s3.UploadPartCopyInput, opts ...request.Option) (*s3.UploadPartCopyOutput, error) {
	var out *s3.UploadPartCopyOutput

	err := w.waitRetryNotify(
		ctx,
		func() error {
			var err error
			out, err = w.s3.UploadPartCopyWithContext(ctx, input, opts...)

			// a nil response must be captured as an error (if no error is provided)
			if out == nil && err == nil {
				err = nilRespError("UploadPartCopyWithContext")
			}
			return err
		},
	)

	return out, err
}

func (w *S3wrapper) CompleteMultipartUploadWithContext(ctx aws.Context, input *s3.CompleteMultipartUploadInput, opts ...request.Option) (*s3.CompleteMultipartUploadOutput, error) {
	var out *s3.CompleteMultipartUploadOutput

	err := w.waitRetryNotify(
		ctx,
		func() error {
			var err error
			out, err = w.s3.CompleteMultipartUploadWithContext(ctx, input, opts...)

			// a nil response must be captured as an error (if no error is provided)
			if out == nil && err == nil {
				err = nilRespError("CompleteMultipartUploadWithContext")
			}
			return err
		},
	)

	return out, err
}

func (w *S3wrapper) DeleteObjectsWithContext(ctx aws.Context, input *s3.DeleteObjectsInput, opts ...request.Option) (*s3.DeleteObjectsOutput, error) {
	var out *s3.DeleteObjectsOutput

	f := func() error {
		var err error
		out, err = w.s3.DeleteObjectsWithContext(ctx, input, opts...)
		if err != nil {
			return err
		}

		// a nil response must be captured as an error (if no error is provided)
		if out == nil {
			return nilRespError("DeleteObjectsWithContext")
		}

		for _, e := range out.Errors {
			if e != nil {
				if slices.Contains(retryableErrors, *e.Code) {
					return errors.New(*e.Code)
				}
			}
		}

		return err
	}

	err := w.waitRetryNotify(ctx, f)

	if err != nil && !slices.Contains(retryableErrors, err.Error()) {
		return out, err
	}

	return out, nil
}

func (w *S3wrapper) GetObjectRequest(input *s3.GetObjectInput) (*request.Request, *s3.GetObjectOutput) {
	// This does not make network calls, no need to rate limit.
	return w.s3.GetObjectRequest(input)
}

func (w *S3wrapper) HeadObjectRequest(input *s3.HeadObjectInput) (*request.Request, *s3.HeadObjectOutput) {
	// This does not make network calls, no need to rate limit.
	return w.s3.HeadObjectRequest(input)
}

func (w *S3wrapper) ListObjectsV2PagesWithContext(ctx aws.Context, input *s3.ListObjectsV2Input, f func(*s3.ListObjectsV2Output, bool) bool, opts ...request.Option) error {
	return w.waitRetryNotify(ctx, func() error {
		return w.s3.ListObjectsV2PagesWithContext(ctx, input, f, opts...)
	})
}

func (w *S3wrapper) AbortMultipartUploadWithContext(ctx aws.Context, input *s3.AbortMultipartUploadInput, opts ...request.Option) (*s3.AbortMultipartUploadOutput, error) {
	var out *s3.AbortMultipartUploadOutput

	err := w.waitRetryNotify(
		ctx,
		func() error {
			var err error
			out, err = w.s3.AbortMultipartUploadWithContext(ctx, input, opts...)

			// a nil response must be captured as an error (if no error is provided)
			if out == nil && err == nil {
				err = nilRespError("AbortMultipartUploadWithContext")
			}

			return err
		},
	)

	return out, err
}

func (w *S3wrapper) UploadPartWithContext(ctx aws.Context, input *s3.UploadPartInput, opts ...request.Option) (*s3.UploadPartOutput, error) {
	var out *s3.UploadPartOutput

	err := w.waitRetryNotify(
		ctx,
		func() error {
			var err error
			out, err = w.s3.UploadPartWithContext(ctx, input, opts...)

			// a nil response must be captured as an error (if no error is provided)
			if out == nil && err == nil {
				err = nilRespError("UploadPartWithContext")
			}

			return err
		},
	)

	return out, err
}

func (w *S3wrapper) waitRetryNotify(ctx aws.Context, f backoff.Operation) error {
	err := backoff.RetryNotify(
		func() error {
			if err := w.Wait(ctx); err != nil {
				return backoff.Permanent(err)
			}

			awsErr := f()
			return wrapAWSerr(awsErr)
		},
		w.backoff(),
		w.notify,
	)

	return err
}

// wrapAWSerr wraps the original error with backoff.Permanent if the error
// should not be retried.
func wrapAWSerr(e error) error {
	if e == nil {
		return nil
	}

	// Retry any request failures that are server errors.
	var reqErr awserr.RequestFailure
	if errors.As(e, &reqErr) {
		if reqErr.StatusCode() != http.StatusTooManyRequests &&
			reqErr.StatusCode() < http.StatusInternalServerError &&
			reqErr.Code() != request.ErrCodeSerialization {
			return backoff.Permanent(e)
		}

		return e
	}

	// Some retryable errors are not specifically awserr.RequestFailure, continue
	// evaluating errors to see if we can retry.

	// Don't attempt to backoff from errors that are known to be client errors.
	var awsErr awserr.Error
	if errors.As(e, &awsErr) {
		if awsErr.Code() == request.ErrCodeInvalidPresignExpire {
			return backoff.Permanent(e)
		}
	}

	return e
}

func nilRespError(s3API string) error {
	return fmt.Errorf("received a nil response for %q from s3", s3API)
}
