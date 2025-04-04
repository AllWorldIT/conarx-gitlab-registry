package s3

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
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
	"github.com/benbjohnson/clock"
	"github.com/docker/distribution/registry/internal/testutil"
	rngtestutil "github.com/docker/distribution/testutil"
	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/s3-aws/common"
	v1 "github.com/docker/distribution/registry/storage/driver/s3-aws/v1"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
)

var (
	// Driver version control
	driverVersion    string
	fromParametersFn func(map[string]any) (storagedriver.StorageDriver, error)
	newDriverFn      func(*common.DriverParameters) (storagedriver.StorageDriver, error)
	newS3APIFn       func(*common.DriverParameters) (s3iface.S3API, error)

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
	driverVersion = os.Getenv(common.EnvDriverVersion)
	switch driverVersion {
	case v1.DriverName, v1.DriverNameAlt:
		// v1 is the default
		fallthrough
	default:
		// backwards compatibility - if no version is defined, default to v2
		fromParametersFn = v1.FromParameters
		newDriverFn = v1.New
		newS3APIFn = v1.NewS3API
	}

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

func fetchDriverConfig(rootDirectory, storageClass string) (*common.DriverParameters, error) {
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
		common.ParamMultipartCopyChunkSize:      common.DefaultMultipartCopyChunkSize,
		common.ParamMultipartCopyMaxConcurrency: common.DefaultMultipartCopyMaxConcurrency,
		common.ParamMultipartCopyThresholdSize:  common.DefaultMultipartCopyThresholdSize,
	}

	if objectACL != "" {
		rawParams[common.ParamObjectACL] = objectACL
	}
	if logLevel != "" {
		rawParams[common.ParamLogLevel] = common.ParseLogLevelParam(logLevel)
	} else {
		rawParams[common.ParamLogLevel] = aws.LogOff
	}

	parsedParams, err := common.ParseParameters(rawParams)
	if err != nil {
		return nil, fmt.Errorf("parsing s3 parameters: %w", err)
	}
	return parsedParams, nil
}

func s3DriverConstructor(rootDirectory, storageClass string) (storagedriver.StorageDriver, error) {
	parsedParams, err := fetchDriverConfig(rootDirectory, storageClass)
	if err != nil {
		return nil, fmt.Errorf("parsing s3 parameters: %w", err)
	}

	return newDriverFn(parsedParams)
}

func s3DriverConstructorT(t *testing.T, rootDirectory, storageClass string) storagedriver.StorageDriver {
	d, err := s3DriverConstructor(rootDirectory, storageClass)
	require.NoError(t, err)

	return d
}

func prefixedS3DriverConstructorT(t *testing.T) storagedriver.StorageDriver {
	rootDir := t.TempDir()
	d, err := s3DriverConstructor(rootDir, s3.StorageClassStandard)
	require.NoError(t, err)

	return d
}

func prefixedMockedS3DriverConstructorT(t *testing.T, s3Mock common.S3WrapperIf) storagedriver.StorageDriver {
	rootDir := t.TempDir()
	parsedParams, err := fetchDriverConfig(rootDir, s3.StorageClassStandard)
	require.NoError(t, err)

	parsedParams.S3APIImpl = s3Mock

	d, err := newDriverFn(parsedParams)
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

func TestS3DriverSuite(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	root := t.TempDir()

	ts := testsuites.NewDriverSuite(
		context.Background(),
		func() (storagedriver.StorageDriver, error) {
			return s3DriverConstructor(root, s3.StorageClassStandard)
		},
		func() (storagedriver.StorageDriver, error) {
			return s3DriverConstructor("", s3.StorageClassStandard)
		},
		nil,
	)
	suite.Run(t, ts)
}

func BenchmarkS3DriverSuite(b *testing.B) {
	if skipMsg := skipCheck(); skipMsg != "" {
		b.Skip(skipMsg)
	}

	root := b.TempDir()

	ts := testsuites.NewDriverSuite(
		context.Background(),
		func() (storagedriver.StorageDriver, error) {
			return s3DriverConstructor(root, s3.StorageClassStandard)
		},
		func() (storagedriver.StorageDriver, error) {
			return s3DriverConstructor("", s3.StorageClassStandard)
		},
		nil,
	)

	ts.SetupSuiteWithB(b)
	b.Cleanup(func() { ts.TearDownSuiteWithB(b) })

	// NOTE(prozlach): This is a method of embedded function, we need to pass
	// the reference to "outer" struct directly
	benchmarks := ts.EnumerateBenchmarks()

	for _, benchmark := range benchmarks {
		b.Run(benchmark.Name, benchmark.Func)
	}
}

func TestS3DriverPathStyle(t *testing.T) {
	// Helper function to extract domain from S3 URL
	extractDomain := func(urlStr string) string {
		parsedURL, err := url.Parse(urlStr)
		require.NoError(t, err)
		return parsedURL.Host
	}

	// Helper function to check if URL uses path style
	isPathStyle := func(urlStr, bucket string) bool {
		parsedURL, err := url.Parse(urlStr)
		require.NoError(t, err)
		return strings.HasPrefix(parsedURL.Path, "/"+bucket)
	}

	// Base configuration that's common across all tests
	baseParams := map[string]any{
		"region": "us-west-2",
		"bucket": "test-bucket",
		"v4auth": "true",
	}

	tests := []struct {
		name           string
		paramOverrides map[string]any
		wantPathStyle  bool
		wantedEndpoint string
	}{
		{
			name: "path style disabled without endpoint",
			paramOverrides: map[string]any{
				"pathstyle": "false",
			},
			wantPathStyle:  false,
			wantedEndpoint: "test-bucket.s3.us-west-2.amazonaws.com",
		},
		{
			name: "path style enabled without endpoint",
			paramOverrides: map[string]any{
				"pathstyle": "true",
			},
			wantPathStyle:  true,
			wantedEndpoint: "s3.us-west-2.amazonaws.com",
		},
		{
			name: "path style disabled with custom endpoint",
			paramOverrides: map[string]any{
				"regionendpoint": "custom-endpoint:9000",
				"pathstyle":      "false",
			},
			wantPathStyle:  false,
			wantedEndpoint: "test-bucket.custom-endpoint:9000",
		},
		{
			name: "path style enabled with custom endpoint",
			paramOverrides: map[string]any{
				"regionendpoint": "custom-endpoint:9000",
				"pathstyle":      "true",
			},
			wantPathStyle:  true,
			wantedEndpoint: "custom-endpoint:9000",
		},
		{
			name: "default path style with custom endpoint",
			paramOverrides: map[string]any{
				"regionendpoint": "custom-endpoint:9000",
			},
			wantPathStyle:  true, // Path style should be forced when endpoint is set
			wantedEndpoint: "custom-endpoint:9000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Merge base parameters with test-specific overrides
			params := make(map[string]any)
			for k, v := range baseParams {
				params[k] = v
			}
			for k, v := range tt.paramOverrides {
				params[k] = v
			}

			d, err := fromParametersFn(params)
			require.NoError(t, err, "unable to create driver")

			// Generate a signed URL to verify the path style and endpoint behavior
			testPath := "/test/file.txt"
			urlStr, err := d.URLFor(context.Background(), testPath, map[string]any{
				"method": "GET",
			})
			require.NoError(t, err, "unable to generate URL")

			// Verify the endpoint
			domain := extractDomain(urlStr)
			require.Equal(t, tt.wantedEndpoint, domain, "unexpected endpoint")

			// Verify path style vs virtual hosted style
			bucket := params["bucket"].(string)
			actualPathStyle := isPathStyle(urlStr, bucket)
			require.Equal(t, tt.wantPathStyle, actualPathStyle,
				"path style mismatch, URL: %s", urlStr)
		})
	}
}

func TestS3DriverEmptyRootList(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	validRoot := t.TempDir()

	rootedDriver := s3DriverConstructorT(t, validRoot, s3.StorageClassStandard)
	emptyRootDriver := s3DriverConstructorT(t, "", s3.StorageClassStandard)
	slashRootDriver := s3DriverConstructorT(t, "/", s3.StorageClassStandard)

	filename := "/test"
	contents := []byte("contents")
	ctx := context.Background()
	err := rootedDriver.PutContent(ctx, filename, contents)
	require.NoError(t, err, "unexpected error creating content")
	defer rootedDriver.Delete(ctx, filename)

	keys, _ := emptyRootDriver.List(ctx, "/")
	for _, path := range keys {
		require.Truef(t, storagedriver.PathRegexp.MatchString(path), "unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
	}

	keys, _ = slashRootDriver.List(ctx, "/")
	for _, path := range keys {
		require.Truef(t, storagedriver.PathRegexp.MatchString(path), "unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
	}
}

func TestS3DriverStorageClassStandard(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	rootDir := t.TempDir()

	standardDriver := s3DriverConstructorT(t, rootDir, s3.StorageClassStandard)
	standardDriverKeyer := standardDriver.(common.S3BucketKeyer)

	standardFilename := "/test-standard"
	contents := []byte("contents")
	ctx := context.Background()

	err := standardDriver.PutContent(ctx, standardFilename, contents)
	require.NoError(t, err)
	defer standardDriver.Delete(ctx, standardFilename)

	// NOTE(prozlach): Our storage driver does not expose API method that
	// allows fetching the storage class of the object, we need to create a
	// native S3 client to do that.
	parsedParams, err := fetchDriverConfig(rootDir, s3.StorageClassStandard)
	require.NoError(t, err)
	s3API, err := newS3APIFn(parsedParams)
	require.NoError(t, err)

	resp, err := s3API.GetObjectWithContext(
		ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(parsedParams.Bucket),
			Key:    aws.String(standardDriverKeyer.S3BucketKey(standardFilename)),
		})
	require.NoError(t, err, "unexpected error retrieving standard storage file")
	defer resp.Body.Close()
	// Amazon only populates this header value for non-standard storage classes
	require.Nil(t, resp.StorageClass, "unexpected storage class for standard file: %v", resp.StorageClass)
}

func TestS3DriverStorageClassReducedRedundancy(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	rootDir := t.TempDir()

	rrDriver := s3DriverConstructorT(t, rootDir, s3.StorageClassReducedRedundancy)
	rrDriverKeyer := rrDriver.(common.S3BucketKeyer)

	rrFilename := "/test-rr"
	contents := []byte("contents")
	ctx := context.Background()

	err := rrDriver.PutContent(ctx, rrFilename, contents)
	require.NoError(t, err, "unexpected error creating content")
	defer rrDriver.Delete(ctx, rrFilename)

	// NOTE(prozlach): Our storage driver does not expose API method that
	// allows fetching the storage class of the object, we need to create a
	// native S3 client to do that.
	parsedParams, err := fetchDriverConfig(rootDir, s3.StorageClassStandard)
	require.NoError(t, err)
	s3API, err := newS3APIFn(parsedParams)
	require.NoError(t, err)

	resp, err := s3API.GetObjectWithContext(
		ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(parsedParams.Bucket),
			Key:    aws.String(rrDriverKeyer.S3BucketKey(rrFilename)),
		})
	require.NoError(t, err, "unexpected error retrieving reduced-redundancy storage file")
	defer resp.Body.Close()
	require.NotNilf(t, resp.StorageClass, "unexpected storage class for reduced-redundancy file: %v", s3.StorageClassStandard)
	require.Equalf(t, s3.StorageClassReducedRedundancy, *resp.StorageClass, "unexpected storage class for reduced-redundancy file: %v", *resp.StorageClass)
}

func TestS3DriverStorageClassNone(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	rootDir := t.TempDir()

	_ = s3DriverConstructorT(t, rootDir, common.StorageClassNone)
}

func TestS3DriverOverThousandBlobs(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	standardDriver := prefixedS3DriverConstructorT(t)

	ctx := context.Background()
	for i := 0; i < 1005; i++ {
		filename := "/thousandfiletest/file" + strconv.Itoa(i)
		contents := []byte("contents")
		err := standardDriver.PutContent(ctx, filename, contents)
		require.NoError(t, err, "unexpected error creating content")
	}

	// cant actually verify deletion because read-after-delete is inconsistent, but can ensure no errors
	err := standardDriver.Delete(ctx, "/thousandfiletest")
	require.NoError(t, err, "unexpected error deleting thousand files")
}

func TestS3DriverURLFor_Expiry(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	ctx := context.Background()
	d := prefixedS3DriverConstructorT(t)

	fp := "/foo"
	err := d.PutContent(ctx, fp, make([]byte, 0))
	require.NoError(t, err)

	// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
	param := "X-Amz-Expires"

	mock := clock.NewMock()
	mock.Set(time.Now())
	testutil.StubClock(t, &common.SystemClock, mock)

	// default
	s, err := d.URLFor(ctx, fp, nil)
	require.NoError(t, err)

	u, err := url.Parse(s)
	require.NoError(t, err)
	require.Equal(t, "1200", u.Query().Get(param))

	// custom
	dt := mock.Now().Add(1 * time.Hour)
	s, err = d.URLFor(ctx, fp, map[string]any{"expiry": dt})
	require.NoError(t, err)

	u, err = url.Parse(s)
	require.NoError(t, err)
	expected := dt.Sub(mock.Now()).Seconds()
	require.Equal(t, fmt.Sprint(expected), u.Query().Get(param))
}

func TestS3DriverMoveWithMultipartCopy(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	d := prefixedS3DriverConstructorT(t)

	ctx := context.Background()
	sourcePath := "/source"
	destPath := "/dest"

	defer d.Delete(ctx, sourcePath)
	defer d.Delete(ctx, destPath)

	// An object larger than d's MultipartCopyThresholdSize will cause d.Move() to perform a multipart copy.
	contents := rngtestutil.RandomBlob(t, int(2*common.DefaultMultipartCopyThresholdSize))

	err := d.PutContent(ctx, sourcePath, contents)
	require.NoError(t, err, "unexpected error creating content")

	err = d.Move(ctx, sourcePath, destPath)
	require.NoError(t, err, "unexpected error moving file")

	received, err := d.GetContent(ctx, destPath)
	require.NoError(t, err, "unexpected error getting content")
	require.Equal(t, contents, received, "content differs")

	_, err = d.GetContent(ctx, sourcePath)
	require.ErrorAs(t, err, new(storagedriver.PathNotFoundError))
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

	return d.DeleteFiles(context.Background(), paths)
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

	err := d.PutContent(context.Background(), "/test/file", make([]byte, 0))
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

	err := d.PutContent(context.Background(), "/test/file", make([]byte, 0))
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
	err := d.PutContent(context.Background(), "/test/file", make([]byte, 0))
	require.Error(t, err)
	require.Equal(t, common.DefaultMaxRetries, retries)
	require.WithinDuration(t, time.Now(), start, time.Second*10)

	start = time.Now()
	err = d.Walk(context.Background(), "test/", func(storagedriver.FileInfo) error { return nil })
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

	err := d.PutContent(context.Background(), "/test/file", make([]byte, 0))
	require.Error(t, err)
	require.Zero(t, retries)

	_, err = d.List(context.Background(), "/test/")
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

	_, err := d.DeleteFiles(context.Background(), []string{"/test/file1", "/test/file2"})
	require.Error(t, err)
	require.Zero(t, retries)
}

func TestS3DriverClientTransport(t *testing.T) {
	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	rootDir := t.TempDir()
	driverConfig, err := fetchDriverConfig(rootDir, s3.StorageClassStandard)
	require.NoError(t, err)

	var hostport string
	var host string
	var scheme string
	if driverConfig.RegionEndpoint == "" {
		// Construct the AWS endpoint based on region
		hostport = fmt.Sprintf("s3.%s.amazonaws.com", driverConfig.Region)
		host = hostport
		scheme = "https"
	} else {
		parsedURL, err := url.Parse(driverConfig.RegionEndpoint)
		require.NoError(t, err)
		hostport = parsedURL.Host
		host, _, err = net.SplitHostPort(parsedURL.Host)
		scheme = parsedURL.Scheme
		require.NoError(t, err)
	}

	// Create a proxy that forwards to real S3
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = scheme
			req.URL.Host = hostport
			req.Host = host
		},
	}

	// Create a proxy server with self-signed certificate
	serverCert, err := generateSelfSignedCert([]string{"127.0.0.1"})
	require.NoError(t, err, "failed to generate test certificate")

	server := httptest.NewUnstartedServer(proxy)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}}
	server.StartTLS()
	defer server.Close()

	testCases := []struct {
		name       string
		skipVerify bool
		shouldFail bool
	}{
		{
			name:       "TLS verification enabled",
			skipVerify: false,
			shouldFail: true, // Should fail with self-signed cert when verification is enabled
		},
		{
			name:       "TLS verification disabled",
			skipVerify: true,
			shouldFail: false, // Should succeed with self-signed cert when verification is disabled
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parsedParams, err := fetchDriverConfig(rootDir, s3.StorageClassStandard)
			require.NoError(t, err)

			parsedParams.SkipVerify = tc.skipVerify
			parsedParams.RegionEndpoint = server.URL
			parsedParams.MaxRetries = 1
			parsedParams.PathStyle = true

			driver, err := newDriverFn(parsedParams)
			require.NoError(t, err)

			// Use List operation to trigger an HTTPS request
			ctx := context.Background()
			_, err = driver.List(ctx, "/")

			if tc.shouldFail {
				require.ErrorContainsf(
					t,
					err, "x509: certificate signed by unknown authority",
					"expected certificate verification error, got: %v", err,
				)
			} else {
				require.Error(t, err)
				var awsErr awserr.Error
				require.ErrorAsf(t, err, &awsErr,
					"expected AWS API error, got: %v", err)
				require.Equal(t, "SignatureDoesNotMatch", awsErr.Code())
				require.Contains(t, awsErr.Message(),
					"The request signature we calculated does not match the signature you provided",
				)
			}
		})
	}
}

// generateSelfSignedCert creates a self-signed certificate for testing
// addresses can contain both hostnames and IP addresses
func generateSelfSignedCert(addresses []string) (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	// Process addresses into DNS and IP SANs
	var dnsNames []string
	var ipAddresses []net.IP

	for _, addr := range addresses {
		if ip := net.ParseIP(addr); ip != nil {
			ipAddresses = append(ipAddresses, ip)
		} else {
			dnsNames = append(dnsNames, addr)
		}
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Corp"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(time.Hour),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,

		// Add the processed SANs
		DNSNames:    dnsNames,
		IPAddresses: ipAddresses,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}),
	)
}
