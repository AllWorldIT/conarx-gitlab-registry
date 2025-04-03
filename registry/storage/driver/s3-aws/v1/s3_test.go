package v1

import (
	"fmt"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	dcontext "github.com/docker/distribution/context"
	rngtestutil "github.com/docker/distribution/testutil"
	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/require"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/s3-aws/common"
)

var (
	// Credentials
	accessKey    string
	secretKey    string
	sessionToken string

	// S3 configuration
	bucket         string
	region         string
	regionEndpoint string
	objectACL      string

	// Security settings
	encrypt    bool
	keyID      string
	secure     bool
	skipVerify bool
	v4Auth     bool
	pathStyle  bool

	// Performance settings
	maxRequestsPerSecond int64
	maxRetries           int64

	// Logging
	logLevel        string
	objectOwnership bool

	missing []string
)

type envConfig struct {
	env      string
	value    any
	required bool
}

func init() {
	fetchEnvVarsConfiguration()
}

func fetchEnvVarsConfiguration() {
	vars := []envConfig{
		{common.EnvAccessKey, &accessKey, true},
		{common.EnvSecretKey, &secretKey, true},
		{common.EnvSessionToken, &sessionToken, false},

		{common.EnvBucket, &bucket, true},
		{common.EnvRegion, &region, true},
		{common.EnvRegionEndpoint, &regionEndpoint, false},
		{common.EnvEncrypt, &encrypt, true},

		{common.EnvObjectACL, &objectACL, false},
		{common.EnvKeyID, &keyID, false},
		{common.EnvSecure, &secure, false},
		{common.EnvSkipVerify, &skipVerify, false},
		{common.EnvV4Auth, &v4Auth, false},
		{common.EnvPathStyle, &pathStyle, false},

		{common.EnvMaxRequestsPerSecond, &maxRequestsPerSecond, false},
		{common.EnvMaxRetries, &maxRetries, false},

		{common.EnvLogLevel, &logLevel, false},
		{common.EnvObjectOwnership, &objectOwnership, false},
	}

	// Set defaults for boolean values
	secure = true
	v4Auth = true
	maxRequestsPerSecond = common.DefaultMaxRequestsPerSecond
	maxRetries = common.DefaultMaxRetries

	missing = make([]string, 0)
	for _, v := range vars {
		val := os.Getenv(v.env)
		if val == "" {
			if v.required {
				missing = append(missing, v.env)
			}
			continue
		}

		var err error
		switch vv := v.value.(type) {
		case *string:
			*vv = val
		case *bool:
			*vv, err = strconv.ParseBool(val)
		case *int64:
			*vv, err = strconv.ParseInt(val, 10, 64)
		}

		if err != nil {
			missing = append(
				missing,
				fmt.Sprintf("invalid value for %q: %s", v.env, val),
			)
		}
	}

	if regionEndpoint != "" {
		pathStyle = true // force pathstyle when endpoint is set
	}
}

func fetchDriverConfig(rootDirectory, storageClass string, logger dcontext.Logger) (*common.DriverParameters, error) {
	rawParams := map[string]any{
		common.ParamAccessKey:                   accessKey,
		common.ParamSecretKey:                   secretKey,
		common.ParamBucket:                      bucket,
		common.ParamRegion:                      region,
		common.ParamRootDirectory:               rootDirectory,
		common.ParamStorageClass:                storageClass,
		common.ParamSecure:                      secure,
		common.ParamV4Auth:                      v4Auth,
		common.ParamEncrypt:                     encrypt,
		common.ParamKeyID:                       keyID,
		common.ParamSkipVerify:                  skipVerify,
		common.ParamPathStyle:                   pathStyle,
		common.ParamSessionToken:                sessionToken,
		common.ParamRegionEndpoint:              regionEndpoint,
		common.ParamMaxRequestsPerSecond:        maxRequestsPerSecond,
		common.ParamMaxRetries:                  maxRetries,
		common.ParamParallelWalk:                true,
		common.ParamObjectOwnership:             objectOwnership,
		common.ParamChunkSize:                   common.MinChunkSize,
		storagedriver.ParamLogger:               logger,
		common.ParamMultipartCopyChunkSize:      common.DefaultMultipartCopyChunkSize,
		common.ParamMultipartCopyMaxConcurrency: common.DefaultMultipartCopyMaxConcurrency,
		common.ParamMultipartCopyThresholdSize:  common.DefaultMultipartCopyThresholdSize,
	}

	if objectACL != "" {
		rawParams[common.ParamObjectACL] = objectACL
	}
	if logLevel != "" {
		rawParams[common.ParamLogLevel] = common.ParseLogLevelParamV1(logger, logLevel)
	} else {
		rawParams[common.ParamLogLevel] = aws.LogOff
	}

	parsedParams, err := common.ParseParameters(common.V1DriverName, rawParams)
	if err != nil {
		return nil, fmt.Errorf("parsing s3 parameters: %w", err)
	}
	return parsedParams, nil
}

func prefixedMockedS3DriverConstructorT(t *testing.T, s3Mock common.S3WrapperIf) storagedriver.StorageDriver {
	rootDir := t.TempDir()
	parsedParams, err := fetchDriverConfig(rootDir, s3.StorageClassStandard, rngtestutil.NewTestLogger(t))
	require.NoError(t, err)

	parsedParams.S3APIImpl = s3Mock

	d, err := New(parsedParams)
	require.NoError(t, err)

	return d
}

func skipCheck() string {
	if len(missing) > 0 {
		return fmt.Sprintf(
			"Invalid value or missing environment values required to run S3 tests: %s",
			strings.Join(missing, ", "),
		)
	}
	return ""
}

type mockDeleteObjectsError struct {
	s3iface.S3API
}

// DeleteObjects mocks a presign expire error while processing a DeleteObjects response.
func (*mockDeleteObjectsError) DeleteObjectsWithContext(_ aws.Context, _ *s3.DeleteObjectsInput, _ ...request.Option) (*s3.DeleteObjectsOutput, error) {
	return nil, awserr.New(request.ErrCodeInvalidPresignExpire, "failed reading response body", nil)
}

func testDeleteFilesError(t *testing.T, mock s3iface.S3API, numFiles int) (int, error) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	// mock the underlying S3 client
	d := prefixedMockedS3DriverConstructorT(t, common.NewS3Wrapper(mock))

	// simulate deleting numFiles files
	paths := make([]string, 0, numFiles)
	for i := 0; i < numFiles; i++ {
		paths = append(paths, strconv.Itoa(mrand.Int()))
	}

	return d.DeleteFiles(dcontext.Background(), paths)
}

// TestDeleteFilesError checks that DeleteFiles handles network/service errors correctly.
func TestS3DriverDeleteFilesError(t *testing.T) {
	// Simulate deleting 2*common.DeleteMax files, should run two iterations even if the first errors out.
	count, err := testDeleteFilesError(t, &mockDeleteObjectsError{}, 2*common.DeleteMax)
	if err == nil {
		t.Error("expected error, got nil")
	}
	if count != 0 {
		t.Errorf("expected the deleted files count to be 0, got %d", count)
	}

	errs, ok := err.(*multierror.Error)
	if !ok {
		t.Errorf("expected error to be of type multierror.Error, got %T", err)
	}
	if errs.Len() != 2 {
		t.Errorf("expected the number of errors to be 2, got %d", errs.Len())
	}

	expected := awserr.New(request.ErrCodeInvalidPresignExpire, "failed reading response body", nil).Error()
	for _, e := range errs.Errors {
		if e.Error() != expected {
			t.Errorf("expected error %q, got %q", expected, e)
		}
	}
}

type mockDeleteObjectsPartialError struct {
	s3iface.S3API
}

// DeleteObjects mocks an S3 DeleteObjects partial error. Half of the objects are successfully deleted, and the other
// half fails due to an 'AccessDenied' error.
func (*mockDeleteObjectsPartialError) DeleteObjectsWithContext(_ aws.Context, input *s3.DeleteObjectsInput, _ ...request.Option) (*s3.DeleteObjectsOutput, error) {
	var deleted []*s3.DeletedObject
	var errored []*s3.Error
	errCode := "AccessDenied"
	errMsg := "Access Denied"

	for i, o := range input.Delete.Objects {
		if i%2 == 0 {
			// error
			errored = append(errored, &s3.Error{
				Key:     o.Key,
				Code:    &errCode,
				Message: &errMsg,
			})
		} else {
			// success
			deleted = append(deleted, &s3.DeletedObject{Key: o.Key})
		}
	}

	return &s3.DeleteObjectsOutput{Deleted: deleted, Errors: errored}, nil
}

// TestDeleteFilesPartialError checks that DeleteFiles handles partial deletion errors correctly.
func TestS3DriverDeleteFilesPartialError(t *testing.T) {
	// Simulate deleting 2*common.DeleteMax files, should run two iterations even if
	// the first response contains inner errors.
	n := 2 * common.DeleteMax
	half := n / 2
	count, err := testDeleteFilesError(t, &mockDeleteObjectsPartialError{}, n)
	if err == nil {
		t.Error("expected error, got nil")
	}
	if count != half {
		t.Errorf("expected the deleted files count to be %d, got %d", half, count)
	}

	errs, ok := err.(*multierror.Error)
	if !ok {
		t.Errorf("expected error to be of type multierror.Error, got %T", err)
	}
	if errs.Len() != half {
		t.Errorf("expected the number of errors to be %d, got %d", half, errs.Len())
	}

	re := regexp.MustCompile(`deleting file '.*': 'Access Denied'`)
	for _, e := range errs.Errors {
		require.Regexp(t, re, e.Error())
	}
}

type mockPutObjectWithContextRetryableError struct {
	s3iface.S3API
}

func (*mockPutObjectWithContextRetryableError) PutObjectWithContext(_ aws.Context, _ *s3.PutObjectInput, _ ...request.Option) (*s3.PutObjectOutput, error) {
	return nil, awserr.New(request.ErrCodeRequestError, "expected test failure", nil)
}

func (*mockPutObjectWithContextRetryableError) ListObjectsV2PagesWithContext(_ aws.Context, _ *s3.ListObjectsV2Input, _ func(*s3.ListObjectsV2Output, bool) bool, _ ...request.Option) error {
	return awserr.NewRequestFailure(nil, http.StatusInternalServerError, "expected test failure")
}

func TestS3DriverBackoffDisabledByDefault(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	var retries int

	notifyFn := func(_ error, _ time.Duration) {
		retries++
	}

	// mock the underlying S3 client
	d := prefixedMockedS3DriverConstructorT(
		t,
		common.NewS3Wrapper(
			&mockPutObjectWithContextRetryableError{},
			common.WithBackoffNotify(notifyFn),
		),
	)

	err := d.PutContent(dcontext.Background(), "/test/file", make([]byte, 0))
	require.Error(t, err)
	require.Zero(t, retries)
}

func TestS3DriverBackoffDisabledBySettingZeroRetries(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	var retries int

	notifyFn := func(_ error, _ time.Duration) {
		retries++
	}

	// mock the underlying S3 client
	d := prefixedMockedS3DriverConstructorT(
		t,
		common.NewS3Wrapper(
			&mockPutObjectWithContextRetryableError{},
			common.WithExponentialBackoff(0),
			common.WithBackoffNotify(notifyFn),
		),
	)

	err := d.PutContent(dcontext.Background(), "/test/file", make([]byte, 0))
	require.Error(t, err)
	require.Zero(t, retries)
}

func TestS3DriverBackoffRetriesRetryableErrors(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	var retries int

	notifyFn := func(_ error, _ time.Duration) {
		retries++
	}

	// mock the underlying S3 client
	d := prefixedMockedS3DriverConstructorT(
		t,
		common.NewS3Wrapper(
			&mockPutObjectWithContextRetryableError{},
			common.WithBackoffNotify(notifyFn),
			common.WithExponentialBackoff(common.DefaultMaxRetries),
		),
	)

	start := time.Now()
	err := d.PutContent(dcontext.Background(), "/test/file", make([]byte, 0))
	require.Error(t, err)
	require.Equal(t, common.DefaultMaxRetries, retries)
	require.WithinDuration(t, time.Now(), start, time.Second*10)

	start = time.Now()
	err = d.Walk(dcontext.Background(), "test/", func(storagedriver.FileInfo) error { return nil })
	require.Error(t, err)
	require.Equal(t, common.DefaultMaxRetries, retries)
	require.WithinDuration(t, time.Now(), start, time.Second*10)
}

type mockPutObjectWithContextPermanentError struct {
	s3iface.S3API
}

func (*mockPutObjectWithContextPermanentError) PutObjectWithContext(_ aws.Context, _ *s3.PutObjectInput, _ ...request.Option) (*s3.PutObjectOutput, error) {
	return nil, awserr.New(request.ErrCodeInvalidPresignExpire, "expected test failure", nil)
}

func (*mockPutObjectWithContextPermanentError) ListObjectsV2WithContext(_ aws.Context, _ *s3.ListObjectsV2Input, _ ...request.Option) (*s3.ListObjectsV2Output, error) {
	return nil, awserr.NewRequestFailure(nil, http.StatusForbidden, "expected test failure")
}

func TestS3DriverBackoffDoesNotRetryPermanentErrors(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	var retries int

	notifyFn := func(_ error, _ time.Duration) {
		retries++
	}

	d := prefixedMockedS3DriverConstructorT(
		t,
		common.NewS3Wrapper(
			&mockPutObjectWithContextPermanentError{},
			common.WithBackoffNotify(notifyFn),
			common.WithExponentialBackoff(200),
		),
	)

	err := d.PutContent(dcontext.Background(), "/test/file", make([]byte, 0))
	require.Error(t, err)
	require.Zero(t, retries)

	_, err = d.List(dcontext.Background(), "/test/")
	require.Error(t, err)
	require.Zero(t, retries)
}

func TestS3DriverBackoffDoesNotRetryNonRequestErrors(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	var retries int

	notifyFn := func(_ error, _ time.Duration) {
		retries++
	}

	d := prefixedMockedS3DriverConstructorT(
		t,
		common.NewS3Wrapper(
			&mockDeleteObjectsError{},
			common.WithBackoffNotify(notifyFn),
			common.WithExponentialBackoff(200),
		),
	)

	_, err := d.DeleteFiles(dcontext.Background(), []string{"/test/file1", "/test/file2"})
	require.Error(t, err)
	require.Zero(t, retries)
}
