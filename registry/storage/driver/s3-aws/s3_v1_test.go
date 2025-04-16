package s3

import (
	mrand "math/rand/v2"
	"net/http"
	"regexp"
	"strconv"
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
	v1 "github.com/docker/distribution/registry/storage/driver/s3-aws/v1"
)

func prefixedMockedS3DriverConstructorT(t *testing.T, s3Mock common.S3WrapperIf) storagedriver.StorageDriver {
	rootDir := t.TempDir()
	parsedParams, err := fetchDriverConfig(rootDir, s3.StorageClassStandard, rngtestutil.NewTestLogger(t))
	require.NoError(t, err)

	parsedParams.S3APIImpl = s3Mock

	d, err := v1.New(parsedParams)
	require.NoError(t, err)

	return d
}

type mockDeleteObjectsError struct {
	s3iface.S3API
}

// DeleteObjects mocks a presign expire error while processing a DeleteObjects response.
func (*mockDeleteObjectsError) DeleteObjectsWithContext(_ aws.Context, _ *s3.DeleteObjectsInput, _ ...request.Option) (*s3.DeleteObjectsOutput, error) {
	return nil, awserr.New(request.ErrCodeInvalidPresignExpire, "failed reading response body", nil)
}

func testDeleteFilesError(t *testing.T, mock s3iface.S3API, numFiles int) (int, error) {
	if skipMsg := skipCheck(false, driverVersion, common.V1DriverName, common.V1DriverNameAlt); skipMsg != "" {
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
	if skipMsg := skipCheck(false, driverVersion, common.V1DriverName, common.V1DriverNameAlt); skipMsg != "" {
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
	if skipMsg := skipCheck(false, driverVersion, common.V1DriverName, common.V1DriverNameAlt); skipMsg != "" {
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
	if skipMsg := skipCheck(false, driverVersion, common.V1DriverName, common.V1DriverNameAlt); skipMsg != "" {
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
	if skipMsg := skipCheck(false, driverVersion, common.V1DriverName, common.V1DriverNameAlt); skipMsg != "" {
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
	if skipMsg := skipCheck(false, driverVersion, common.V1DriverName, common.V1DriverNameAlt); skipMsg != "" {
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
