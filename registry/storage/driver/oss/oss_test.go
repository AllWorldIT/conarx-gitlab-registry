//go:build include_oss

package oss

import (
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	alioss "github.com/denverdino/aliyungo/oss"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/internal/testutil"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	dtestutil "github.com/docker/distribution/registry/storage/driver/internal/testutil"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	"github.com/stretchr/testify/require"
	"gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

var ossDriverConstructor func(rootDirectory string) (*Driver, error)

var skipCheck func() string

func init() {
	accessKey := os.Getenv("ALIYUN_ACCESS_KEY_ID")
	secretKey := os.Getenv("ALIYUN_ACCESS_KEY_SECRET")
	bucket := os.Getenv("OSS_BUCKET")
	region := os.Getenv("OSS_REGION")
	internal := os.Getenv("OSS_INTERNAL")
	encrypt := os.Getenv("OSS_ENCRYPT")
	secure := os.Getenv("OSS_SECURE")
	endpoint := os.Getenv("OSS_ENDPOINT")
	encryptionKeyID := os.Getenv("OSS_ENCRYPTIONKEYID")
	root, err := os.MkdirTemp("", "driver-")
	if err != nil {
		panic(err)
	}
	defer os.Remove(root)

	ossDriverConstructor = func(rootDirectory string) (*Driver, error) {
		encryptBool := false
		if encrypt != "" {
			encryptBool, err = strconv.ParseBool(encrypt)
			if err != nil {
				return nil, err
			}
		}

		secureBool := false
		if secure != "" {
			secureBool, err = strconv.ParseBool(secure)
			if err != nil {
				return nil, err
			}
		}

		internalBool := false
		if internal != "" {
			internalBool, err = strconv.ParseBool(internal)
			if err != nil {
				return nil, err
			}
		}

		parameters := &DriverParameters{
			AccessKeyID:     accessKey,
			AccessKeySecret: secretKey,
			Bucket:          bucket,
			Region:          alioss.Region(region),
			Internal:        internalBool,
			ChunkSize:       minChunkSize,
			RootDirectory:   rootDirectory,
			Encrypt:         encryptBool,
			Secure:          secureBool,
			Endpoint:        endpoint,
			EncryptionKeyID: encryptionKeyID,
		}

		return New(parameters)
	}

	// Skip OSS storage driver tests if environment variable parameters are not provided
	skipCheck = func() string {
		if accessKey == "" || secretKey == "" || region == "" || bucket == "" || encrypt == "" {
			return "Must set ALIYUN_ACCESS_KEY_ID, ALIYUN_ACCESS_KEY_SECRET, OSS_REGION, OSS_BUCKET, and OSS_ENCRYPT to run OSS tests"
		}
		return ""
	}

	testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return ossDriverConstructor(root)
	}, skipCheck)
}

func TestEmptyRootList(t *testing.T) {
	if skipCheck() != "" {
		t.Skip(skipCheck())
	}

	validRoot := t.TempDir()

	rootedDriver, err := ossDriverConstructor(validRoot)
	if err != nil {
		t.Fatalf("unexpected error creating rooted driver: %v", err)
	}

	emptyRootDriver, err := ossDriverConstructor("")
	if err != nil {
		t.Fatalf("unexpected error creating empty root driver: %v", err)
	}

	slashRootDriver, err := ossDriverConstructor("/")
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

	keys, err := emptyRootDriver.List(ctx, "/")
	for _, path := range keys {
		if !storagedriver.PathRegexp.MatchString(path) {
			t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
		}
	}

	keys, err = slashRootDriver.List(ctx, "/")
	for _, path := range keys {
		if !storagedriver.PathRegexp.MatchString(path) {
			t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
		}
	}
}

func Test_parseParameters_Bool(t *testing.T) {
	p := map[string]interface{}{
		"accesskeyid":     "accesskeyid",
		"accesskeysecret": "accesskeysecret",
		"region":          "region",
		"bucket":          "bucket",
	}

	testFn := func(params map[string]interface{}) (interface{}, error) {
		return parseParameters(params)
	}

	tcs := map[string]struct {
		parameters      map[string]interface{}
		paramName       string
		driverParamName string
		defaultt        interface{}
	}{
		"internal": {
			parameters:      p,
			paramName:       "internal",
			driverParamName: "Internal",
			defaultt:        false,
		},
		"encrypt": {
			parameters:      p,
			paramName:       "encrypt",
			driverParamName: "Encrypt",
			defaultt:        false,
		},
		"secure": {
			parameters:      p,
			paramName:       "secure",
			driverParamName: "Secure",
			defaultt:        true,
		},
		// TODO: add string test cases
	}

	for tn, tt := range tcs {
		t.Run(tn, func(t *testing.T) {
			opts := dtestutil.Opts{
				Defaultt:          tt.defaultt,
				ParamName:         tt.paramName,
				DriverParamName:   tt.driverParamName,
				OriginalParams:    tt.parameters,
				ParseParametersFn: testFn,
			}

			dtestutil.AssertByDefaultType(t, opts)
		})
	}
}

func TestURLFor_Expiry(t *testing.T) {
	if skipCheck() != "" {
		t.Skip(skipCheck())
	}

	ctx := context.Background()
	validRoot := t.TempDir()
	d, err := ossDriverConstructor(validRoot)
	require.NoError(t, err)

	fp := "/foo"
	err = d.PutContent(ctx, fp, []byte(`bar`))
	require.NoError(t, err)

	// https://www.alibabacloud.com/help/doc-detail/31952.htm
	param := "Expires"

	mock := clock.NewMock()
	mock.Set(time.Now())
	testutil.StubClock(t, &systemClock, mock)

	// default
	s, err := d.URLFor(ctx, fp, nil)
	require.NoError(t, err)
	u, err := url.Parse(s)
	require.NoError(t, err)

	dt := mock.Now().Add(20 * time.Minute)
	expected := strconv.FormatInt(dt.Unix(), 10)
	require.Equal(t, expected, u.Query().Get(param))

	// custom
	dt = mock.Now().Add(1 * time.Hour)
	s, err = d.URLFor(ctx, fp, map[string]interface{}{"expiry": dt})
	require.NoError(t, err)

	u, err = url.Parse(s)
	require.NoError(t, err)
	expected = strconv.FormatInt(dt.Unix(), 10)
	require.Equal(t, expected, u.Query().Get(param))
}
