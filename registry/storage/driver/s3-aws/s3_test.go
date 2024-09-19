package s3

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/benbjohnson/clock"
	"github.com/docker/distribution/registry/internal/testutil"
	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/require"

	"gopkg.in/check.v1"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	dtestutil "github.com/docker/distribution/registry/storage/driver/internal/testutil"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

var (
	s3DriverConstructor func(rootDirectory, storageClass string) (*Driver, error)
	skipS3              func() string
)

func init() {
	accessKey := os.Getenv("AWS_ACCESS_KEY")
	secretKey := os.Getenv("AWS_SECRET_KEY")
	bucket := os.Getenv("S3_BUCKET")
	encrypt := os.Getenv("S3_ENCRYPT")
	keyID := os.Getenv("S3_KEY_ID")
	secure := os.Getenv("S3_SECURE")
	skipVerify := os.Getenv("S3_SKIP_VERIFY")
	v4Auth := os.Getenv("S3_V4_AUTH")
	region := os.Getenv("AWS_REGION")
	objectACL := os.Getenv("S3_OBJECT_ACL")
	root, err := os.MkdirTemp("", "driver-")
	regionEndpoint := os.Getenv("REGION_ENDPOINT")
	sessionToken := os.Getenv("AWS_SESSION_TOKEN")
	pathStyle := os.Getenv("AWS_PATH_STYLE")
	maxRequestsPerSecond := os.Getenv("S3_MAX_REQUESTS_PER_SEC")
	maxRetries := os.Getenv("S3_MAX_RETRIES")
	logLevel := os.Getenv("S3_LOG_LEVEL")
	objectOwnership := os.Getenv("S3_OBJECT_OWNERSHIP")

	if err != nil {
		panic(err)
	}
	defer os.Remove(root)

	s3DriverConstructor = func(rootDirectory, storageClass string) (*Driver, error) {
		encryptBool := false
		if encrypt != "" {
			encryptBool, err = strconv.ParseBool(encrypt)
			if err != nil {
				return nil, err
			}
		}

		secureBool := true
		if secure != "" {
			secureBool, err = strconv.ParseBool(secure)
			if err != nil {
				return nil, err
			}
		}

		skipVerifyBool := false
		if skipVerify != "" {
			skipVerifyBool, err = strconv.ParseBool(skipVerify)
			if err != nil {
				return nil, err
			}
		}

		v4Bool := true
		if v4Auth != "" {
			v4Bool, err = strconv.ParseBool(v4Auth)
			if err != nil {
				return nil, err
			}
		}

		pathStyleBool := false

		// If regionEndpoint is set, default to forcing pathstyle to preserve legacy behavior.
		if regionEndpoint != "" {
			pathStyleBool = true
		}

		if pathStyle != "" {
			pathStyleBool, err = strconv.ParseBool(pathStyle)
			if err != nil {
				return nil, err
			}
		}

		maxRequestsPerSecondInt64 := int64(defaultMaxRequestsPerSecond)

		if maxRequestsPerSecond != "" {
			if maxRequestsPerSecondInt64, err = strconv.ParseInt(maxRequestsPerSecond, 10, 64); err != nil {
				return nil, err
			}
		}

		maxRetriesInt64 := int64(defaultMaxRetries)

		if maxRetries != "" {
			if maxRetriesInt64, err = strconv.ParseInt(maxRetries, 10, 64); err != nil {
				return nil, err
			}
		}

		objectOwnershipBool := false
		if objectOwnership != "" {
			objectOwnershipBool, err = strconv.ParseBool(objectOwnership)
			if err != nil {
				return nil, err
			}
		}

		parallelWalkBool := true

		logLevelType := parseLogLevelParam(logLevel)

		parameters := &DriverParameters{
			accessKey,
			secretKey,
			bucket,
			region,
			regionEndpoint,
			encryptBool,
			keyID,
			secureBool,
			skipVerifyBool,
			v4Bool,
			minChunkSize,
			defaultMultipartCopyChunkSize,
			defaultMultipartCopyMaxConcurrency,
			defaultMultipartCopyThresholdSize,
			rootDirectory,
			storageClass,
			objectACL,
			sessionToken,
			pathStyleBool,
			maxRequestsPerSecondInt64,
			maxRetriesInt64,
			parallelWalkBool,
			logLevelType,
			objectOwnershipBool,
		}

		return New(parameters)
	}

	// Skip S3 storage driver tests if environment variable parameters are not provided
	skipS3 = func() string {
		if accessKey == "" || secretKey == "" || region == "" || bucket == "" || encrypt == "" {
			return "Must set AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION, S3_BUCKET, and S3_ENCRYPT to run S3 tests"
		}
		return ""
	}

	testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return s3DriverConstructor(root, s3.StorageClassStandard)
	}, skipS3)
}

func Test_parseParameters(t *testing.T) {
	p := map[string]interface{}{
		"region": "us-west-2",
		"bucket": "test",
		"v4auth": "true",
	}

	testFn := func(params map[string]interface{}) (interface{}, error) {
		return parseParameters(params)
	}

	tcs := map[string]struct {
		parameters      map[string]interface{}
		paramName       string
		driverParamName string
		required        bool
		nilAllowed      bool
		emptyAllowed    bool
		nonTypeAllowed  bool
		defaultt        interface{}
	}{
		"secure": {
			parameters:      p,
			paramName:       "secure",
			driverParamName: "Secure",
			defaultt:        true,
		},
		"encrypt": {
			parameters:      p,
			paramName:       "encrypt",
			driverParamName: "Encrypt",
			defaultt:        false,
		},
		"pathstyle_without_region_endpoint": {
			parameters:      p,
			paramName:       "pathstyle",
			driverParamName: "PathStyle",
			defaultt:        false,
		},
		"pathstyle_with_region_endpoint": {
			parameters: func() map[string]interface{} {
				pp := dtestutil.CopyMap(p)
				pp["regionendpoint"] = "region/endpoint"

				return pp
			}(),
			paramName:       "pathstyle",
			driverParamName: "PathStyle",
			defaultt:        true,
		},
		"skipverify": {
			parameters:      p,
			paramName:       "skipverify",
			driverParamName: "SkipVerify",
			defaultt:        false,
		},
		"v4auth": {
			parameters:      p,
			paramName:       "v4auth",
			driverParamName: "V4Auth",
			required:        true,
			defaultt:        true,
		},
		"parallelwalk": {
			parameters:      p,
			paramName:       "parallelwalk",
			driverParamName: "ParallelWalk",
			defaultt:        false,
		},
		"accesskey": {
			parameters:      p,
			paramName:       "accesskey",
			driverParamName: "AccessKey",
			nilAllowed:      true,
			emptyAllowed:    true,
			nonTypeAllowed:  true,
			defaultt:        "",
		},
		"secretkey": {
			parameters:      p,
			paramName:       "secretkey",
			driverParamName: "SecretKey",
			nilAllowed:      true,
			emptyAllowed:    true,
			nonTypeAllowed:  true,
			defaultt:        "",
		},
		"regionendpoint": {
			parameters:      p,
			paramName:       "regionendpoint",
			driverParamName: "RegionEndpoint",
			nilAllowed:      true,
			emptyAllowed:    true,
			nonTypeAllowed:  true,
			defaultt:        "",
		},
		"region": {
			parameters:      p,
			paramName:       "region",
			driverParamName: "Region",
			nilAllowed:      false,
			emptyAllowed:    false,
			// not allowed because we check validRegions[region] when regionendpoint is empty
			nonTypeAllowed: false,
			required:       true,
			defaultt:       "",
		},
		"region_with_regionendpoint": {
			parameters: func() map[string]interface{} {
				pp := dtestutil.CopyMap(p)
				pp["regionendpoint"] = "region/endpoint"

				return pp
			}(),
			paramName:       "region",
			driverParamName: "Region",
			nilAllowed:      false,
			emptyAllowed:    false,
			// allowed because we don't check validRegions[region] when regionendpoint is not empty
			nonTypeAllowed: true,
			required:       true,
			defaultt:       "",
		},
		"bucket": {
			parameters:      p,
			paramName:       "bucket",
			driverParamName: "Bucket",
			nilAllowed:      false,
			emptyAllowed:    false,
			nonTypeAllowed:  true,
			required:        true,
			defaultt:        "",
		},
		"keyid": {
			parameters:      p,
			paramName:       "keyid",
			driverParamName: "KeyID",
			nilAllowed:      true,
			emptyAllowed:    true,
			nonTypeAllowed:  true,
			defaultt:        "",
		},
		"rootdirectory": {
			parameters:      p,
			paramName:       "rootdirectory",
			driverParamName: "RootDirectory",
			nilAllowed:      true,
			emptyAllowed:    true,
			nonTypeAllowed:  true,
			defaultt:        "",
		},
		"storageclass": {
			parameters:      p,
			paramName:       "storageclass",
			driverParamName: "StorageClass",
			nilAllowed:      true,
			emptyAllowed:    false,
			nonTypeAllowed:  false,
			defaultt:        s3.StorageClassStandard,
		},
		"objectacl": {
			parameters:      p,
			paramName:       "objectacl",
			driverParamName: "ObjectACL",
			nilAllowed:      true,
			emptyAllowed:    false,
			nonTypeAllowed:  false,
			defaultt:        s3.ObjectCannedACLPrivate,
		},
		"objectownership": {
			parameters:      p,
			paramName:       "objectownership",
			driverParamName: "ObjectOwnership",
			nilAllowed:      true,
			emptyAllowed:    false,
			nonTypeAllowed:  false,
			defaultt:        false,
		},
	}

	for tn, tt := range tcs {
		t.Run(tn, func(t *testing.T) {
			opts := dtestutil.Opts{
				Defaultt:          tt.defaultt,
				ParamName:         tt.paramName,
				DriverParamName:   tt.driverParamName,
				OriginalParams:    tt.parameters,
				NilAllowed:        tt.nilAllowed,
				EmptyAllowed:      tt.emptyAllowed,
				NonTypeAllowed:    tt.nonTypeAllowed,
				Required:          tt.required,
				ParseParametersFn: testFn,
			}

			dtestutil.AssertByDefaultType(t, opts)
		})
	}
}

func TestFromParameters(t *testing.T) {
	// Minimal params needed to construct the driver.
	baseParams := map[string]interface{}{
		"region": "us-west-2",
		"bucket": "test",
		"v4auth": "true",
	}

	tests := []struct {
		params              map[string]interface{}
		wantedForcePathBool bool
	}{
		{
			map[string]interface{}{
				"pathstyle": "false",
			}, false,
		},
		{
			map[string]interface{}{
				"pathstyle": "true",
			}, true,
		},
		{
			map[string]interface{}{
				"regionendpoint": "test-endpoint",
				"pathstyle":      "false",
			}, false,
		},
		{
			map[string]interface{}{
				"regionendpoint": "test-endpoint",
				"pathstyle":      "true",
			}, true,
		},
		{
			map[string]interface{}{
				"regionendpoint": "",
			}, false,
		},
		{
			map[string]interface{}{
				"regionendpoint": "test-endpoint",
			}, true,
		},
	}

	for _, tt := range tests {
		// add baseParams to testing params
		for k, v := range baseParams {
			tt.params[k] = v
		}

		d, err := FromParameters(tt.params)
		if err != nil {
			t.Fatalf("unable to create a new S3 driver: %v", err)
		}

		pathStyle := d.baseEmbed.Base.StorageDriver.(*driver).S3.s3.(*s3.S3).Client.Config.S3ForcePathStyle
		if pathStyle == nil {
			t.Fatal("expected pathStyle not to be nil")
		}

		if *pathStyle != tt.wantedForcePathBool {
			t.Fatalf("expected S3ForcePathStyle to be %v, got %v, with params %#v", tt.wantedForcePathBool, *pathStyle, tt.params)
		}
	}
}

func TestEmptyRootList(t *testing.T) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	validRoot := t.TempDir()

	rootedDriver, err := s3DriverConstructor(validRoot, s3.StorageClassStandard)
	if err != nil {
		t.Fatalf("unexpected error creating rooted driver: %v", err)
	}

	emptyRootDriver, err := s3DriverConstructor("", s3.StorageClassStandard)
	if err != nil {
		t.Fatalf("unexpected error creating empty root driver: %v", err)
	}

	slashRootDriver, err := s3DriverConstructor("/", s3.StorageClassStandard)
	if err != nil {
		t.Fatalf("unexpected error creating slash root driver: %v", err)
	}

	filename := "/test"
	contents := []byte("contents")
	ctx := context.Background()
	err = rootedDriver.PutContent(ctx, filename, contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}
	defer rootedDriver.Delete(ctx, filename)

	keys, _ := emptyRootDriver.List(ctx, "/")
	for _, path := range keys {
		if !storagedriver.PathRegexp.MatchString(path) {
			t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
		}
	}

	keys, _ = slashRootDriver.List(ctx, "/")
	for _, path := range keys {
		if !storagedriver.PathRegexp.MatchString(path) {
			t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
		}
	}
}

func TestStorageClass(t *testing.T) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	rootDir := t.TempDir()

	standardDriver, err := s3DriverConstructor(rootDir, s3.StorageClassStandard)
	if err != nil {
		t.Fatalf("unexpected error creating driver with standard storage: %v", err)
	}

	rrDriver, err := s3DriverConstructor(rootDir, s3.StorageClassReducedRedundancy)
	if err != nil {
		t.Fatalf("unexpected error creating driver with reduced redundancy storage: %v", err)
	}

	if _, err = s3DriverConstructor(rootDir, noStorageClass); err != nil {
		t.Fatalf("unexpected error creating driver without storage class: %v", err)
	}

	standardFilename := "/test-standard"
	rrFilename := "/test-rr"
	contents := []byte("contents")
	ctx := context.Background()

	err = standardDriver.PutContent(ctx, standardFilename, contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}
	defer standardDriver.Delete(ctx, standardFilename)

	err = rrDriver.PutContent(ctx, rrFilename, contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}
	defer rrDriver.Delete(ctx, rrFilename)

	standardDriverUnwrapped := standardDriver.Base.StorageDriver.(*driver)
	resp, err := standardDriverUnwrapped.S3.GetObjectWithContext(
		ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(standardDriverUnwrapped.Bucket),
			Key:    aws.String(standardDriverUnwrapped.s3Path(standardFilename)),
		})
	if err != nil {
		t.Fatalf("unexpected error retrieving standard storage file: %v", err)
	}
	defer resp.Body.Close()
	// Amazon only populates this header value for non-standard storage classes
	if resp.StorageClass != nil {
		t.Fatalf("unexpected storage class for standard file: %v", resp.StorageClass)
	}

	rrDriverUnwrapped := rrDriver.Base.StorageDriver.(*driver)
	resp, err = rrDriverUnwrapped.S3.GetObjectWithContext(
		ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(rrDriverUnwrapped.Bucket),
			Key:    aws.String(rrDriverUnwrapped.s3Path(rrFilename)),
		})
	if err != nil {
		t.Fatalf("unexpected error retrieving reduced-redundancy storage file: %v", err)
	}
	defer resp.Body.Close()
	if resp.StorageClass == nil {
		t.Fatalf("unexpected storage class for reduced-redundancy file: %v", s3.StorageClassStandard)
	} else if *resp.StorageClass != s3.StorageClassReducedRedundancy {
		t.Fatalf("unexpected storage class for reduced-redundancy file: %v", *resp.StorageClass)
	}
}

func TestOverThousandBlobs(t *testing.T) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	standardDriver := newTempDirDriver(t)

	ctx := context.Background()
	for i := 0; i < 1005; i++ {
		filename := "/thousandfiletest/file" + strconv.Itoa(i)
		contents := []byte("contents")
		err := standardDriver.PutContent(ctx, filename, contents)
		if err != nil {
			t.Fatalf("unexpected error creating content: %v", err)
		}
	}

	// cant actually verify deletion because read-after-delete is inconsistent, but can ensure no errors
	err := standardDriver.Delete(ctx, "/thousandfiletest")
	if err != nil {
		t.Fatalf("unexpected error deleting thousand files: %v", err)
	}
}

func TestURLFor_Expiry(t *testing.T) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	ctx := context.Background()
	d := newTempDirDriver(t)

	fp := "/foo"
	err := d.PutContent(ctx, fp, []byte{})
	require.NoError(t, err)

	// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
	param := "X-Amz-Expires"

	mock := clock.NewMock()
	mock.Set(time.Now())
	testutil.StubClock(t, &systemClock, mock)

	// default
	s, err := d.URLFor(ctx, fp, nil)
	require.NoError(t, err)

	u, err := url.Parse(s)
	require.NoError(t, err)
	require.Equal(t, "1200", u.Query().Get(param))

	// custom
	dt := mock.Now().Add(1 * time.Hour)
	s, err = d.URLFor(ctx, fp, map[string]interface{}{"expiry": dt})
	require.NoError(t, err)

	u, err = url.Parse(s)
	require.NoError(t, err)
	expected := dt.Sub(mock.Now()).Seconds()
	require.Equal(t, fmt.Sprint(expected), u.Query().Get(param))
}

func TestMoveWithMultipartCopy(t *testing.T) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	d := newTempDirDriver(t)

	ctx := context.Background()
	sourcePath := "/source"
	destPath := "/dest"

	defer d.Delete(ctx, sourcePath)
	defer d.Delete(ctx, destPath)

	// An object larger than d's MultipartCopyThresholdSize will cause d.Move() to perform a multipart copy.
	multipartCopyThresholdSize := d.baseEmbed.Base.StorageDriver.(*driver).MultipartCopyThresholdSize
	contents := make([]byte, 2*multipartCopyThresholdSize)
	rand.Read(contents)

	err := d.PutContent(ctx, sourcePath, contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}

	err = d.Move(ctx, sourcePath, destPath)
	if err != nil {
		t.Fatalf("unexpected error moving file: %v", err)
	}

	received, err := d.GetContent(ctx, destPath)
	if err != nil {
		t.Fatalf("unexpected error getting content: %v", err)
	}
	if !bytes.Equal(contents, received) {
		t.Fatal("content differs")
	}

	_, err = d.GetContent(ctx, sourcePath)
	switch err.(type) {
	case storagedriver.PathNotFoundError:
	default:
		t.Fatalf("unexpected error getting content: %v", err)
	}
}

type mockDeleteObjectsError struct {
	s3iface.S3API
}

// DeleteObjects mocks a presign expire error while processing a DeleteObjects response.
func (m *mockDeleteObjectsError) DeleteObjectsWithContext(ctx aws.Context, input *s3.DeleteObjectsInput, opts ...request.Option) (*s3.DeleteObjectsOutput, error) {
	return nil, awserr.New(request.ErrCodeInvalidPresignExpire, "failed reading response body", nil)
}

func testDeleteFilesError(t *testing.T, mock s3iface.S3API, numFiles int) (int, error) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	d := newTempDirDriver(t)

	// mock the underlying S3 client
	d.baseEmbed.Base.StorageDriver.(*driver).S3 = newS3Wrapper(mock)

	// simulate deleting numFiles files
	paths := make([]string, 0, numFiles)
	for i := 0; i < numFiles; i++ {
		paths = append(paths, strconv.Itoa(rand.Int()))
	}

	return d.DeleteFiles(context.Background(), paths)
}

// TestDeleteFilesError checks that DeleteFiles handles network/service errors correctly.
func TestDeleteFilesError(t *testing.T) {
	// Simulate deleting 2*deleteMax files, should run two iterations even if the first errors out.
	count, err := testDeleteFilesError(t, &mockDeleteObjectsError{}, 2*deleteMax)
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
func (m *mockDeleteObjectsPartialError) DeleteObjectsWithContext(ctx aws.Context, input *s3.DeleteObjectsInput, opts ...request.Option) (*s3.DeleteObjectsOutput, error) {
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
func TestDeleteFilesPartialError(t *testing.T) {
	// Simulate deleting 2*deleteMax files, should run two iterations even if
	// the first response contains inner errors.
	n := 2 * deleteMax
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

func (m *mockPutObjectWithContextRetryableError) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	return nil, awserr.New(request.ErrCodeRequestError, "expected test failure", nil)
}

func (m *mockPutObjectWithContextRetryableError) ListObjectsV2PagesWithContext(ctx aws.Context, input *s3.ListObjectsV2Input, f func(*s3.ListObjectsV2Output, bool) bool, opts ...request.Option) error {
	return awserr.NewRequestFailure(nil, http.StatusInternalServerError, "expected test failure")
}

func TestBackoffDisabledByDefault(t *testing.T) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	d := newTempDirDriver(t)

	var retries int

	notifyFn := func(err error, t time.Duration) {
		retries++
	}

	// mock the underlying S3 client
	d.baseEmbed.Base.StorageDriver.(*driver).S3 = newS3Wrapper(
		&mockPutObjectWithContextRetryableError{},
		withBackoffNotify(notifyFn),
	)

	err := d.PutContent(context.Background(), "/test/file", []byte{})
	require.Error(t, err)
	require.Zero(t, retries)
}

func TestBackoffDisabledBySettingZeroRetries(t *testing.T) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	d := newTempDirDriver(t)

	var retries int

	notifyFn := func(err error, t time.Duration) {
		retries++
	}

	// mock the underlying S3 client
	d.baseEmbed.Base.StorageDriver.(*driver).S3 = newS3Wrapper(
		&mockPutObjectWithContextRetryableError{},
		withExponentialBackoff(0),
		withBackoffNotify(notifyFn),
	)

	err := d.PutContent(context.Background(), "/test/file", []byte{})
	require.Error(t, err)
	require.Zero(t, retries)
}

func TestBackoffRetriesRetryableErrors(t *testing.T) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	d := newTempDirDriver(t)

	var retries int

	notifyFn := func(err error, t time.Duration) {
		retries++
	}

	// mock the underlying S3 client
	d.baseEmbed.Base.StorageDriver.(*driver).S3 = newS3Wrapper(
		&mockPutObjectWithContextRetryableError{},
		withBackoffNotify(notifyFn),
		withExponentialBackoff(defaultMaxRetries),
	)

	start := time.Now()
	err := d.PutContent(context.Background(), "/test/file", []byte{})
	require.Error(t, err)
	require.Equal(t, defaultMaxRetries, retries)
	require.WithinDuration(t, time.Now(), start, time.Second*10)

	start = time.Now()
	err = d.Walk(context.Background(), "test/", func(storagedriver.FileInfo) error { return nil })
	require.Error(t, err)
	require.Equal(t, defaultMaxRetries, retries)
	require.WithinDuration(t, time.Now(), start, time.Second*10)
}

type mockPutObjectWithContextPermanentError struct {
	s3iface.S3API
}

func (m *mockPutObjectWithContextPermanentError) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	return nil, awserr.New(request.ErrCodeInvalidPresignExpire, "expected test failure", nil)
}

func (m *mockPutObjectWithContextPermanentError) ListObjectsV2WithContext(ctx aws.Context, input *s3.ListObjectsV2Input, opts ...request.Option) (*s3.ListObjectsV2Output, error) {
	return nil, awserr.NewRequestFailure(nil, http.StatusForbidden, "expected test failure")
}

func TestBackoffDoesNotRetryPermanentErrors(t *testing.T) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	d := newTempDirDriver(t)

	var retries int

	notifyFn := func(err error, t time.Duration) {
		retries++
	}

	// mock the underlying S3 client
	d.baseEmbed.Base.StorageDriver.(*driver).S3 = newS3Wrapper(
		&mockPutObjectWithContextPermanentError{},
		withBackoffNotify(notifyFn),
		withExponentialBackoff(200),
	)

	err := d.PutContent(context.Background(), "/test/file", []byte{})
	require.Error(t, err)
	require.Zero(t, retries)

	_, err = d.List(context.Background(), "/test/")
	require.Error(t, err)
	require.Zero(t, retries)
}

func TestBackoffDoesNotRetryNonRequestErrors(t *testing.T) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	d := newTempDirDriver(t)

	var retries int

	notifyFn := func(err error, t time.Duration) {
		retries++
	}

	// mock the underlying S3 client
	d.baseEmbed.Base.StorageDriver.(*driver).S3 = newS3Wrapper(
		&mockDeleteObjectsError{},
		withBackoffNotify(notifyFn),
		withExponentialBackoff(200),
	)

	_, err := d.DeleteFiles(context.Background(), []string{"/test/file1", "/test/file2"})
	require.Error(t, err)
	require.Zero(t, retries)
}

func newTempDirDriver(t *testing.T) *Driver {
	t.Helper()

	rootDir := t.TempDir()

	d, err := s3DriverConstructor(rootDir, s3.StorageClassStandard)
	require.NoError(t, err)

	return d
}

func TestExistsPath(t *testing.T) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	root := t.TempDir()
	d, err := s3DriverConstructor(root, s3.StorageClassStandard)
	require.NoError(t, err)

	ctx := context.Background()

	prefix := "/existing/path"
	path := fmt.Sprintf("%s/data", prefix)

	content := make([]byte, 10)
	rand.Read(content)

	err = d.PutContent(ctx, path, content)
	require.NoError(t, err)

	exists, err := d.ExistsPath(ctx, prefix)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestExistsPathNotFound(t *testing.T) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	root := t.TempDir()
	d, err := s3DriverConstructor(root, s3.StorageClassStandard)
	require.NoError(t, err)

	exists, err := d.ExistsPath(context.Background(), "/non-existing/path")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestClientTransport(t *testing.T) {
	if skipS3() != "" {
		t.Skip(skipS3())
	}

	testCases := []struct {
		skipverify bool
	}{
		{true},
		{false},
	}

	for _, tc := range testCases {
		params := map[string]interface{}{
			"region":     os.Getenv("AWS_REGION"),
			"bucket":     os.Getenv("S3_BUCKET"),
			"skipverify": tc.skipverify,
		}
		t.Run(fmt.Sprintf("SkipVerify %v", tc.skipverify), func(t *testing.T) {
			drv, err := FromParameters(params)
			if err != nil {
				t.Fatalf("failed to create driver: %v", err)
			}

			s3drv := drv.baseEmbed.Base.StorageDriver.(*driver)
			s3s, ok := s3drv.S3.s3.(*s3.S3)
			if !ok {
				t.Fatal("failed to cast storage driver to *s3.S3")
			}
			tr, ok := s3s.Config.HTTPClient.Transport.(*http.Transport)
			if !ok {
				t.Fatal("unexpected driver transport")
			}
			if tr.TLSClientConfig.InsecureSkipVerify != tc.skipverify {
				t.Errorf("unexpected TLS Config. Expected InsecureSkipVerify: %v, got %v",
					tc.skipverify,
					tr.TLSClientConfig.InsecureSkipVerify)
			}
			// make sure the proxy is always set
			if tr.Proxy == nil {
				t.Fatal("missing HTTP transport proxy config")
			}
		})
	}
}
