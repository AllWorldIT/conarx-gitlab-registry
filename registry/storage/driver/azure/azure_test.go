package azure

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/docker/distribution/registry/internal/testutil"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/azure/common"
	v1 "github.com/docker/distribution/registry/storage/driver/azure/v1"
	v2 "github.com/docker/distribution/registry/storage/driver/azure/v2"
	dtestutil "github.com/docker/distribution/registry/storage/driver/internal/testutil"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var (
	credsType string

	accountName string
	accountKey  string

	tenantID string
	clientID string
	secret   string

	accountContainer string
	accountRealm     string

	missing []string

	debugLog bool

	driverVersion     string
	parseParametersFn func(map[string]any) (any, error)
	newDriverFn       func(any) (storagedriver.StorageDriver, error)
)

type envConfig struct {
	env   string
	value *string
}

func init() {
	fetchEnvVarsConfiguration()
}

func fetchEnvVarsConfiguration() {
	driverVersion = os.Getenv(common.EnvDriverVersion)
	switch driverVersion {
	case v1.DriverName:
		parseParametersFn = v1.ParseParameters
		newDriverFn = v1.New
	case v2.DriverName:
		parseParametersFn = v2.ParseParameters
		newDriverFn = v2.New
	default:
		msg := fmt.Sprintf("invalid azure driver version: %q", driverVersion)
		missing = []string{msg}
		return
	}

	credsType = os.Getenv(common.EnvCredentialsType)

	// NOTE(prozlach): Providing account name is not required for client-secret
	// and default credentials, but it allows to auto-derrive service URL in
	// case when it is not provided. It makes things easier for people that
	// want things to "just work" as account name is easy to find, while people
	// that know how to determine service URL themselves will likelly not care
	// anyway.

	var expected []envConfig
	switch credsType {
	case common.CredentialsTypeSharedKey, "":
		credsType = common.CredentialsTypeSharedKey
		expected = []envConfig{
			{common.EnvAccountName, &accountName},
			{common.EnvAccountKey, &accountKey},
			{common.EnvContainer, &accountContainer},
			{common.EnvRealm, &accountRealm},
		}
	case common.CredentialsTypeClientSecret:
		expected = []envConfig{
			{common.EnvTenantID, &tenantID},
			{common.EnvClientID, &clientID},
			{common.EnvSecret, &secret},
			{common.EnvAccountName, &accountName},
			{common.EnvContainer, &accountContainer},
			{common.EnvRealm, &accountRealm},
		}
	case common.CredentialsTypeDefaultCredentials:
		expected = []envConfig{
			{common.EnvAccountName, &accountName},
			{common.EnvContainer, &accountContainer},
			{common.EnvRealm, &accountRealm},
		}
	default:
		msg := fmt.Sprintf("invalid azure credentials type: %q", credsType)
		missing = []string{msg}
		return
	}

	missing = make([]string, 0)
	for _, v := range expected {
		*v.value = os.Getenv(v.env)
		if *v.value == "" {
			missing = append(missing, v.env)
		}
	}

	// NOTE(prozlach): debug logging is optional:
	var err error
	if v := os.Getenv(common.EnvDebugLog); v != "" {
		debugLog, err = strconv.ParseBool(v)
		if err != nil {
			msg := fmt.Sprintf("invalid value for %q: %q", common.EnvDebugLog, v)
			missing = []string{msg}
			return
		}
	}
}

func azureDriverConstructor(rootDirectory string, trimLegacyRootPrefix bool) (storagedriver.StorageDriver, error) {
	rawParams := map[string]any{
		common.ParamAccountName:          accountName,
		common.ParamContainer:            accountContainer,
		common.ParamRealm:                accountRealm,
		common.ParamRootDirectory:        rootDirectory,
		common.ParamTrimLegacyRootPrefix: trimLegacyRootPrefix,
	}

	switch credsType {
	case common.CredentialsTypeSharedKey:
		rawParams[common.ParamAccountKey] = accountKey
		rawParams[common.ParamCredentialsType] = common.CredentialsTypeSharedKey
	case common.CredentialsTypeClientSecret:
		rawParams[common.ParamTenantID] = tenantID
		rawParams[common.ParamClientID] = clientID
		rawParams[common.ParamSecret] = secret
		rawParams[common.ParamCredentialsType] = common.CredentialsTypeClientSecret
	case common.CredentialsTypeDefaultCredentials:
		rawParams[common.ParamCredentialsType] = common.CredentialsTypeDefaultCredentials
	}

	if debugLog {
		rawParams[common.ParamDebugLog] = "true"
		// logging all events is enabled by default, uncomment and adjust call
		// below to change it:
		//
		// rawParams[paramDebugLogEvents]="Request,Response,ResponseError,Retry,LongRunningOperation"
	}

	if debugLog {
		rawParams[common.ParamDebugLog] = "true"
		// logging all events is enabled by default, uncomment and adjust call
		// below to change it:
		//
		// rawParams[common.ParamDebugLogEvents] = "Response,ResponseError,LongRunningOperation"
	}

	parsedParams, err := parseParametersFn(rawParams)
	if err != nil {
		return nil, fmt.Errorf("parsing azure login credentials: %w", err)
	}

	return newDriverFn(parsedParams)
}

func skipCheck() string {
	if len(missing) > 0 {
		return fmt.Sprintf("Must set %s environment variables to run Azure tests", strings.Join(missing, ", "))
	}
	return ""
}

func TestAzureDriverSuite(t *testing.T) {
	root := t.TempDir()

	t.Logf("root directory for the tests set to %q", root)

	if skipMsg := skipCheck(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	ts := testsuites.NewDriverSuite(
		context.Background(),
		func() (storagedriver.StorageDriver, error) {
			return azureDriverConstructor(root, true)
		},
		func() (storagedriver.StorageDriver, error) {
			return azureDriverConstructor("", true)
		},
		nil,
	)
	suite.Run(t, ts)
}

func BenchmarkAzureDriverSuite(b *testing.B) {
	root := b.TempDir()

	b.Logf("root directory for the benchmarks set to %q", root)

	if skipMsg := skipCheck(); skipMsg != "" {
		b.Skip(skipMsg)
	}

	ts := testsuites.NewDriverSuite(
		context.Background(),
		func() (storagedriver.StorageDriver, error) {
			return azureDriverConstructor(root, true)
		},
		func() (storagedriver.StorageDriver, error) {
			return azureDriverConstructor("", true)
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
			// NOTE(prozlach): No need to use an unique root prefix here as we
			// are not doing any writes so there is no risk of collision with
			// other CI jobs running.
			d, err := azureDriverConstructor(tt.rootDirectory, !tt.legacyPath)
			require.NoError(t, err)

			// Health checks stat "/" and expect either a not found error or a directory.
			fsInfo, err := d.Stat(context.Background(), "/")
			if !errors.As(err, &storagedriver.PathNotFoundError{}) {
				require.NoError(t, err)
				require.True(t, fsInfo.IsDir())
			}
		})
	}
}

func TestAzureDriver_parseParameters_Bool(t *testing.T) {
	p := map[string]any{
		"accountname": "accountName",
		"accountkey":  "accountKey",
		"container":   "container",
		// TODO: add string test cases, if needed?
	}

	opts := dtestutil.Opts{
		Defaultt:          true,
		ParamName:         common.ParamTrimLegacyRootPrefix,
		DriverParamName:   "trimLegacyRootPrefix",
		OriginalParams:    p,
		ParseParametersFn: parseParametersFn,
	}

	dtestutil.AssertByDefaultType(t, opts)
}

func TestAzureDriverURLFor_Expiry(t *testing.T) {
	if skipCheck() != "" {
		t.Skip(skipCheck())
	}

	ctx := context.Background()
	validRoot := t.TempDir()
	d, err := azureDriverConstructor(validRoot, true)
	require.NoError(t, err)

	fp := "/foo"
	err = d.PutContent(ctx, fp, []byte(`bar`))
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Delete(ctx, "/foo") })

	// https://docs.microsoft.com/en-us/rest/api/storageservices/create-service-sas#specifying-the-access-policy
	param := "se"

	mock := clock.NewMock()
	mock.Set(time.Now())
	testutil.StubClock(t, &common.SystemClock, mock)

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
	s, err = d.URLFor(ctx, fp, map[string]any{"expiry": dt})
	require.NoError(t, err)

	u, err = url.Parse(s)
	require.NoError(t, err)
	expected = dt.UTC().Format(time.RFC3339)
	require.Equal(t, expected, u.Query().Get(param))
}

func TestAzureDriverInferRootPrefixConfiguration_Valid(t *testing.T) {
	tests := []struct {
		name                    string
		config                  map[string]any
		expectedUseLegacyPrefix bool
	}{
		{
			name:                    "config: legacyrootprefix not set trimlegacyrootprefix not set",
			config:                  make(map[string]any, 0),
			expectedUseLegacyPrefix: false,
		},
		{
			name: "config: legacyrootprefix set trimlegacyrootprefix not set",
			config: map[string]any{
				common.ParamLegacyRootPrefix: true,
			},
			expectedUseLegacyPrefix: true,
		},
		{
			name: "config: legacyrootprefix false trimlegacyrootprefix not set",
			config: map[string]any{
				common.ParamLegacyRootPrefix: false,
			},
			expectedUseLegacyPrefix: false,
		},
		{
			name: "config: legacyrootprefix not set trimlegacyrootprefix true",
			config: map[string]any{
				common.ParamTrimLegacyRootPrefix: true,
			},
			expectedUseLegacyPrefix: false,
		},
		{
			name: "config: legacyrootprefix not set trimlegacyrootprefix false",
			config: map[string]any{
				common.ParamTrimLegacyRootPrefix: false,
			},
			expectedUseLegacyPrefix: true,
		},
		{
			name: "config: legacyrootprefix true trimlegacyrootprefix false",
			config: map[string]any{
				common.ParamTrimLegacyRootPrefix: false,
				common.ParamLegacyRootPrefix:     true,
			},
			expectedUseLegacyPrefix: true,
		},
		{
			name: "config: legacyrootprefix false trimlegacyrootprefix true",
			config: map[string]any{
				common.ParamTrimLegacyRootPrefix: true,
				common.ParamLegacyRootPrefix:     false,
			},
			expectedUseLegacyPrefix: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualTrimLegacyPrefix, err := common.InferRootPrefixConfiguration(test.config)
			require.NoError(t, err)
			require.Equal(t, test.expectedUseLegacyPrefix, actualTrimLegacyPrefix)
		})
	}
}

func TestAzureDriverInferRootPrefixConfiguration_Invalid(t *testing.T) {
	tests := []struct {
		name                    string
		config                  map[string]any
		expectedUseLegacyPrefix bool
	}{
		{
			name: "config: legacyrootprefix true trimlegacyrootprefix true",
			config: map[string]any{
				common.ParamTrimLegacyRootPrefix: true,
				common.ParamLegacyRootPrefix:     true,
			},
		},
		{
			name: "config: legacyrootprefix false trimlegacyrootprefix false",
			config: map[string]any{
				common.ParamTrimLegacyRootPrefix: false,
				common.ParamLegacyRootPrefix:     false,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			useLegacyRootPrefix, err := common.InferRootPrefixConfiguration(test.config)
			require.Error(t, err)
			require.ErrorContains(t, err, "storage.azure.trimlegacyrootprefix' and  'storage.azure.trimlegacyrootprefix' can not both be")
			require.False(t, useLegacyRootPrefix)
		})
	}
}
