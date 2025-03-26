package common

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/benbjohnson/clock"
	"github.com/cenkalti/backoff/v4"
	"github.com/docker/distribution/registry/internal"
)

// S3WrapperIf is the subset of methods exposed in s3 SDKs S3API object that we
// use in our code.
type S3WrapperIf interface {
	PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error)
	GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error)
	CreateMultipartUploadWithContext(ctx aws.Context, input *s3.CreateMultipartUploadInput, opts ...request.Option) (*s3.CreateMultipartUploadOutput, error)
	ListMultipartUploadsWithContext(ctx aws.Context, input *s3.ListMultipartUploadsInput, opts ...request.Option) (*s3.ListMultipartUploadsOutput, error)
	ListPartsWithContext(ctx aws.Context, input *s3.ListPartsInput, opts ...request.Option) (*s3.ListPartsOutput, error)
	ListObjectsV2WithContext(ctx aws.Context, input *s3.ListObjectsV2Input, opts ...request.Option) (*s3.ListObjectsV2Output, error)
	CopyObjectWithContext(ctx aws.Context, input *s3.CopyObjectInput, opts ...request.Option) (*s3.CopyObjectOutput, error)
	UploadPartCopyWithContext(ctx aws.Context, input *s3.UploadPartCopyInput, opts ...request.Option) (*s3.UploadPartCopyOutput, error)
	CompleteMultipartUploadWithContext(ctx aws.Context, input *s3.CompleteMultipartUploadInput, opts ...request.Option) (*s3.CompleteMultipartUploadOutput, error)
	DeleteObjectsWithContext(ctx aws.Context, input *s3.DeleteObjectsInput, opts ...request.Option) (*s3.DeleteObjectsOutput, error)
	GetObjectRequest(input *s3.GetObjectInput) (*request.Request, *s3.GetObjectOutput)
	HeadObjectRequest(input *s3.HeadObjectInput) (*request.Request, *s3.HeadObjectOutput)
	ListObjectsV2PagesWithContext(ctx aws.Context, input *s3.ListObjectsV2Input, f func(*s3.ListObjectsV2Output, bool) bool, opts ...request.Option) error
	AbortMultipartUploadWithContext(ctx aws.Context, input *s3.AbortMultipartUploadInput, opts ...request.Option) (*s3.AbortMultipartUploadOutput, error)
	UploadPartWithContext(ctx aws.Context, input *s3.UploadPartInput, opts ...request.Option) (*s3.UploadPartOutput, error)
	HeadObjectWithContext(ctx aws.Context, input *s3.HeadObjectInput, opts ...request.Option) (*s3.HeadObjectOutput, error)
}

// S3BucketKeyer is any type that is capable of returning the S3 bucket key
// which should be cached by AWS CloudFront.
type S3BucketKeyer interface {
	S3BucketKey(path string) string
}

const (
	// MinChunkSize defines the minimum multipart upload chunk size S3 API
	// requires multipart upload chunks to be at least 5MB
	MinChunkSize int64 = 5 << 20

	// MaxChunkSize defines the maximum multipart upload chunk size allowed by
	// S3.
	MaxChunkSize int64 = 5 << 30

	DefaultChunkSize int64 = 2 * MinChunkSize

	// DefaultMultipartCopyChunkSize defines the default chunk size for all but
	// the last Upload Part - Copy operation of a multipart copy. Empirically,
	// 32 MB is optimal.
	DefaultMultipartCopyChunkSize int64 = 32 << 20

	// DefaultMultipartCopyMaxConcurrency defines the default maximum number of
	// concurrent Upload Part - Copy operations for a multipart copy.
	DefaultMultipartCopyMaxConcurrency = 100

	// DefaultMultipartCopyThresholdSize defines the default object size above
	// which multipart copy will be used. (PUT Object - Copy is used for
	// objects at or below this size.)  Empirically, 32 MB is optimal.
	DefaultMultipartCopyThresholdSize int64 = 32 << 20

	// DefaultMaxRequestsPerSecond defines the default maximum number of
	// requests per second that can be made to the S3 API per driver instance.
	// 350 is 10% of the requestsPerSecondUpperLimit based on the figures
	// listed in:
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance.html
	DefaultMaxRequestsPerSecond = 350

	// DefaultMaxRetries is how many times the driver will retry failed
	// requests.
	DefaultMaxRetries = 5

	// deleteMax is the largest amount of objects you can request to be deleted in S3 using a DeleteObjects call. This is
	// currently set to 1000 as per the S3 specification https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
	DeleteMax = 1000
)

// defaults related to exponential backoff
const (
	DefaultInitialInterval     = backoff.DefaultInitialInterval
	DefaultRandomizationFactor = backoff.DefaultRandomizationFactor
	DefaultMultiplier          = backoff.DefaultMultiplier
	DefaultMaxInterval         = backoff.DefaultMaxInterval
	DefaultMaxElapsedTime      = backoff.DefaultMaxElapsedTime
)

// SystemClock is a stub clock used for tesitng
var SystemClock internal.Clock = clock.New()
