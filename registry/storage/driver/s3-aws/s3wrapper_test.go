package s3

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/cenkalti/backoff/v4"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	aPermanentAWSRequestError = awserr.NewRequestFailure(
		awserr.New("test-code", "test-error-message", errors.New("test-original error")),
		http.StatusFailedDependency, "testReqID")

	aNonPermanentAWSRequestError = awserr.NewRequestFailure(
		awserr.New("test-code", "test-error-message", errors.New("test-original error")),
		http.StatusInternalServerError, "testReqID")

	anAWSInvalidPresignExpireError = awserr.New(request.ErrCodeInvalidPresignExpire, "test-error-message", errors.New("test-original error"))

	aRequestAWSSerializationError = awserr.NewRequestFailure(
		awserr.New(request.ErrCodeSerialization,
			"failed to decode REST XML response", errors.New("test-original error")),
		http.StatusAccepted,
		"1234",
	)

	anAWSSerializationError = awserr.New(request.ErrCodeSerialization, "test-error-message", errors.New("test-original error"))
)

func TestWrapAWSerr(t *testing.T) {
	tests := []struct {
		name          string
		inputError    error
		expectedError error
	}{
		{
			name:          "no error",
			inputError:    nil,
			expectedError: nil,
		},
		{
			name:          "aws server request failure",
			inputError:    aPermanentAWSRequestError,
			expectedError: backoff.Permanent(aPermanentAWSRequestError),
		},
		{
			name:          "aws non-server request failure",
			inputError:    aNonPermanentAWSRequestError,
			expectedError: aNonPermanentAWSRequestError,
		},
		{
			name:          "aws InvalidPresignExpireError (not-request-specific)",
			inputError:    anAWSInvalidPresignExpireError,
			expectedError: backoff.Permanent(anAWSInvalidPresignExpireError),
		},
		{
			name:          "aws SerializationError error (not-request-specific)",
			inputError:    anAWSSerializationError,
			expectedError: anAWSSerializationError,
		},
		{
			name:          "aws SerializationError error (request-specific)",
			inputError:    aRequestAWSSerializationError,
			expectedError: aRequestAWSSerializationError,
		},
	}
	t.Logf("Running %s test", t.Name())
	t.Parallel()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorIs(t, test.expectedError, wrapAWSerr(test.inputError))
		})
	}
}

type mockDeleteObjectsWithContext struct {
	s3iface.S3API
	failedRequests int32
	counter        atomic.Int32
}

func (m *mockDeleteObjectsWithContext) DeleteObjectsWithContext(ctx aws.Context, input *s3.DeleteObjectsInput, opts ...request.Option) (*s3.DeleteObjectsOutput, error) {
	out := &s3.DeleteObjectsOutput{}
	if m.counter.Load() < m.failedRequests {
		out.Errors = []*s3.Error{
			{
				Code: aws.String(errCodeInternalError),
			},
		}
		m.counter.Add(1)
	}
	return out, nil
}

func deleteObjectsWithContext(failedRequests, maxRetries int) (*s3.DeleteObjectsOutput, int32, error) {
	mock := &mockDeleteObjectsWithContext{failedRequests: int32(failedRequests)}

	w := newS3Wrapper(
		mock,
		withExponentialBackoff(int64(maxRetries)),
		withBackoffNotify(func(err error, t time.Duration) {
			log.WithFields(log.Fields{"error": err, "delay_s": t.Seconds()}).Info("S3: retrying after error")
		}),
	)

	out, err := w.DeleteObjectsWithContext(context.Background(), &s3.DeleteObjectsInput{
		Bucket: aws.String("testbucket"),
		Delete: &s3.Delete{
			Objects: []*s3.ObjectIdentifier{
				{Key: aws.String("testobject")},
			},
		},
	})

	return out, mock.counter.Load(), err
}

func TestDeleteObjectsWithContext_retryableErrors(t *testing.T) {
	tests := []struct {
		name                 string
		failedRequests       int
		maxRetries           int
		expectedOutputErrors int
	}{
		{
			name:                 "max retries not reached",
			failedRequests:       2,
			maxRetries:           5,
			expectedOutputErrors: 0,
		},
		{
			name:                 "max retries reached",
			failedRequests:       3,
			maxRetries:           1,
			expectedOutputErrors: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out, counter, err := deleteObjectsWithContext(test.failedRequests, test.maxRetries)

			require.NotNil(t, out, "expected output, got nil")

			require.NoError(t, err, "expected no error")

			require.Len(t, out.Errors, test.expectedOutputErrors, "output error counts do not match")

			totalRequests := int32(test.failedRequests)
			if test.maxRetries < test.failedRequests {
				totalRequests = int32(test.maxRetries) + 1
			}
			require.Equal(t, counter, int32(totalRequests), "request counts do not match")
		})
	}
}
