package azure

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/docker/distribution/registry/internal/testutil"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	dtestutil "github.com/docker/distribution/registry/storage/driver/internal/testutil"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	envAccountName = "AZURE_STORAGE_ACCOUNT_NAME"
	envAccountKey  = "AZURE_STORAGE_ACCOUNT_KEY"
	envContainer   = "AZURE_STORAGE_CONTAINER"
	envRealm       = "AZURE_STORAGE_REALM"
)

var (
	accountName string
	accountKey  string
	container   string
	realm       string

	missing []string
)

func init() {
	config := []struct {
		env   string
		value *string
	}{
		{envAccountName, &accountName},
		{envAccountKey, &accountKey},
		{envContainer, &container},
		{envRealm, &realm},
	}

	missing = make([]string, 0)
	for _, v := range config {
		*v.value = os.Getenv(v.env)
		if *v.value == "" {
			missing = append(missing, v.env)
		}
	}
}

func azureDriverConstructor(rootDirectory string) (*Driver, error) {
	params := &driverParameters{
		accountName:          accountName,
		accountKey:           accountKey,
		container:            container,
		realm:                realm,
		root:                 rootDirectory,
		trimLegacyRootPrefix: true,
	}

	return New(params)
}

func skipCheck() string {
	if len(missing) > 0 {
		return fmt.Sprintf("Must set %s environment variables to run Azure tests", strings.Join(missing, ", "))
	}
	return ""
}

func TestAzureDriverSuite(t *testing.T) {
	root, err := os.MkdirTemp("", "azuredriver-test-")
	require.NoError(t, err)

	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	ts := testsuites.NewDriverSuite(
		context.Background(),
		func() (storagedriver.StorageDriver, error) {
			return azureDriverConstructor(root)
		},
		func() error {
			return os.Remove(root)
		},
	)
	suite.Run(t, ts)
}

func BenchmarkAzureDriverSuite(b *testing.B) {
	root, err := os.MkdirTemp("", "azuredriver-bench-")
	require.NoError(b, err)

	if skipMsg := skipCheck(); skipMsg != "" {
		b.Skip(skipMsg)
	}

	ts := testsuites.NewDriverSuite(
		context.Background(),
		func() (storagedriver.StorageDriver, error) {
			return azureDriverConstructor(root)
		},
		func() error {
			return os.Remove(root)
		},
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

func TestAzureDriverPathToKey(t *testing.T) {
	tests := []struct {
		name          string
		rootDirectory string
		providedPath  string
		expectedPath  string
		legacyPath    bool
	}{
		{
			name:          "legacy leading slash empty root directory",
			rootDirectory: "",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "/docker/registry/v2",
			legacyPath:    true,
		},
		{
			name:          "legacy leading slash single slash root directory",
			rootDirectory: "/",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "/docker/registry/v2",
			legacyPath:    true,
		},
		{
			name:          "empty root directory results in expected path",
			rootDirectory: "",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "docker/registry/v2",
		},
		{
			name:          "legacy empty root directory results in expected path",
			rootDirectory: "",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "/docker/registry/v2",
			legacyPath:    true,
		},
		{
			name:          "root directory no slashes prefixed to path with slash between root and path",
			rootDirectory: "opt",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "opt/docker/registry/v2",
		},
		{
			name:          "legacy root directory no slashes prefixed to path with slash between root and path",
			rootDirectory: "opt",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "/opt/docker/registry/v2",
			legacyPath:    true,
		},
		{
			name:          "root directory with slashes prefixed to path no leading slash",
			rootDirectory: "/opt/",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "opt/docker/registry/v2",
		},
		{
			name:          "dirty root directory prefixed to path cleanly",
			rootDirectory: "/opt////",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "opt/docker/registry/v2",
		},
		{
			name:          "nested custom root directory prefixed to path",
			rootDirectory: "a/b/c/d/",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "a/b/c/d/docker/registry/v2",
		},
		{
			name:          "legacy root directory results in expected root path",
			rootDirectory: "",
			providedPath:  "/",
			expectedPath:  "/",
			legacyPath:    true,
		},
		{
			name:          "root directory results in expected root path",
			rootDirectory: "",
			providedPath:  "/",
			expectedPath:  "",
		},
		{
			name:          "legacy root directory no slashes results in expected root path",
			rootDirectory: "opt",
			providedPath:  "/",
			expectedPath:  "/opt",
			legacyPath:    true,
		},
		{
			name:          "root directory no slashes results in expected root path",
			rootDirectory: "opt",
			providedPath:  "/",
			expectedPath:  "opt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rootDirectory := strings.Trim(tt.rootDirectory, "/")
			if rootDirectory != "" {
				rootDirectory += "/"
			}
			d := &driver{rootDirectory: rootDirectory, legacyPath: tt.legacyPath}
			require.Equal(t, tt.expectedPath, d.pathToKey(tt.providedPath))
		})
	}
}

func TestAzureDriverStatRootPath(t *testing.T) {
	if skipCheck() != "" {
		t.Skip(skipCheck())
	}

	tests := []struct {
		name          string
		rootDirectory string
		legacyPath    bool
	}{
		{
			name:          "legacy empty root directory",
			rootDirectory: "",
			legacyPath:    true,
		},
		{
			name:          "empty root directory",
			rootDirectory: "",
		},
		{
			name:          "legacy slash root directory",
			rootDirectory: "/",
			legacyPath:    true,
		},
		{
			name:          "slash root directory",
			rootDirectory: "/",
		},
		{
			name:          "root directory no slashes",
			rootDirectory: "opt",
		},
		{
			name:          "legacy root directory no slashes",
			rootDirectory: "opt",
			legacyPath:    true,
		},
		{
			name:          "nested custom root directory",
			rootDirectory: "a/b/c/d/",
		},
		{
			name:          "legacy nested custom root directory",
			rootDirectory: "a/b/c/d/",
			legacyPath:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := &driverParameters{
				accountName: accountName,
				accountKey:  accountKey,
				container:   container,
				realm:       realm,
				root:        tt.rootDirectory,
				// trimLegacyRootPrefix is negated during driver init inside `New`
				trimLegacyRootPrefix: !tt.legacyPath,
			}

			d, err := New(params)
			require.NoError(t, err)

			// Health checks stat "/" and expect either a not found error or a directory.
			fsInfo, err := d.Stat(context.Background(), "/")
			if !errors.As(err, &storagedriver.PathNotFoundError{}) {
				require.True(t, fsInfo.IsDir())
			}
		})
	}
}

func TestAzureDriver_parseParameters_Bool(t *testing.T) {
	p := map[string]interface{}{
		"accountname": "accountName",
		"accountkey":  "accountKey",
		"container":   "container",
		// TODO: add string test cases, if needed?
	}

	testFn := func(params map[string]interface{}) (interface{}, error) {
		return parseParameters(params)
	}

	opts := dtestutil.Opts{
		Defaultt:          true,
		ParamName:         paramTrimLegacyRootPrefix,
		DriverParamName:   "trimLegacyRootPrefix",
		OriginalParams:    p,
		ParseParametersFn: testFn,
	}

	dtestutil.AssertByDefaultType(t, opts)
}

func TestAzureDriverURLFor_Expiry(t *testing.T) {
	if skipCheck() != "" {
		t.Skip(skipCheck())
	}

	ctx := context.Background()
	validRoot := t.TempDir()
	d, err := azureDriverConstructor(validRoot)
	require.NoError(t, err)

	fp := "/foo"
	err = d.PutContent(ctx, fp, []byte(`bar`))
	require.NoError(t, err)

	// https://docs.microsoft.com/en-us/rest/api/storageservices/create-service-sas#specifying-the-access-policy
	param := "se"

	mock := clock.NewMock()
	mock.Set(time.Now())
	testutil.StubClock(t, &systemClock, mock)

	// default
	s, err := d.URLFor(ctx, fp, nil)
	require.NoError(t, err)
	u, err := url.Parse(s)
	require.NoError(t, err)

	dt := mock.Now().Add(20 * time.Minute)
	expected := dt.UTC().Format(time.RFC3339)
	require.Equal(t, expected, u.Query().Get(param))

	// custom
	dt = mock.Now().Add(1 * time.Hour)
	s, err = d.URLFor(ctx, fp, map[string]interface{}{"expiry": dt})
	require.NoError(t, err)

	u, err = url.Parse(s)
	require.NoError(t, err)
	expected = dt.UTC().Format(time.RFC3339)
	require.Equal(t, expected, u.Query().Get(param))
}

func TestAzureDriverInferRootPrefixConfiguration_Valid(t *testing.T) {
	tests := []struct {
		name                    string
		config                  map[string]interface{}
		expectedUseLegacyPrefix bool
	}{
		{
			name:                    "config: legacyrootprefix not set trimlegacyrootprefix not set",
			config:                  map[string]interface{}{},
			expectedUseLegacyPrefix: false,
		},
		{
			name: "config: legacyrootprefix set trimlegacyrootprefix not set",
			config: map[string]interface{}{
				paramLegacyRootPrefix: true,
			},
			expectedUseLegacyPrefix: true,
		},
		{
			name: "config: legacyrootprefix false trimlegacyrootprefix not set",
			config: map[string]interface{}{
				paramLegacyRootPrefix: false,
			},
			expectedUseLegacyPrefix: false,
		},
		{
			name: "config: legacyrootprefix not set trimlegacyrootprefix true",
			config: map[string]interface{}{
				paramTrimLegacyRootPrefix: true,
			},
			expectedUseLegacyPrefix: false,
		},
		{
			name: "config: legacyrootprefix not set trimlegacyrootprefix false",
			config: map[string]interface{}{
				paramTrimLegacyRootPrefix: false,
			},
			expectedUseLegacyPrefix: true,
		},
		{
			name: "config: legacyrootprefix true trimlegacyrootprefix false",
			config: map[string]interface{}{
				paramTrimLegacyRootPrefix: false,
				paramLegacyRootPrefix:     true,
			},
			expectedUseLegacyPrefix: true,
		},
		{
			name: "config: legacyrootprefix false trimlegacyrootprefix true",
			config: map[string]interface{}{
				paramTrimLegacyRootPrefix: true,
				paramLegacyRootPrefix:     false,
			},
			expectedUseLegacyPrefix: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualTrimLegacyPrefix, err := inferRootPrefixConfiguration(test.config)
			require.NoError(t, err)
			require.Equal(t, test.expectedUseLegacyPrefix, actualTrimLegacyPrefix)
		})
	}
}

func TestAzureDriverInferRootPrefixConfiguration_Invalid(t *testing.T) {
	tests := []struct {
		name                    string
		config                  map[string]interface{}
		expectedUseLegacyPrefix bool
	}{
		{
			name: "config: legacyrootprefix true trimlegacyrootprefix true",
			config: map[string]interface{}{
				paramTrimLegacyRootPrefix: true,
				paramLegacyRootPrefix:     true,
			},
		},
		{
			name: "config: legacyrootprefix false trimlegacyrootprefix false",
			config: map[string]interface{}{
				paramTrimLegacyRootPrefix: false,
				paramLegacyRootPrefix:     false,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			useLegacyRootPrefix, err := inferRootPrefixConfiguration(test.config)
			require.Error(t, err)
			require.ErrorContains(t, err, "storage.azure.trimlegacyrootprefix' and  'storage.azure.trimlegacyrootprefix' can not both be")
			require.False(t, useLegacyRootPrefix)
		})
	}
}
