package azure

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
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
	case v2.DriverName:
		parseParametersFn = v2.ParseParameters
		newDriverFn = v2.New
	case v1.DriverName:
		// If driver name has not been specified, fallback to v1 as this is the
		// current GA version.
		fallthrough
	default:
		parseParametersFn = v1.ParseParameters
		newDriverFn = v1.New
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

func fetchDriverConfig(rootDirectory string, trimLegacyRootPrefix bool) (any, error) {
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
	return parsedParams, nil
}

func azureDriverConstructor(rootDirectory string, trimLegacyRootPrefix bool) (storagedriver.StorageDriver, error) {
	parsedParams, err := fetchDriverConfig(rootDirectory, trimLegacyRootPrefix)
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

// NOTE(prozlach): TestAzureDriverRootPathList and TestAzureDriverRootPathStat
// tests verify different configurations of root directory and legacy prefix
// config parameters. They use Azure code in ensureBlobFuncFactory function
// that is completely separate from the Storage driver being tested in order
// to avoid "self-confirming error" when testing root directory and legacy
// prefixing.
//
// If both the creation and verification processes shared the same underlying
// flaw or assumption, it could cause an error to appear valid because the
// verification method carries the same bias. By having the dir structure
// created by a different path, we prevent this bias and establish an anchor for
// all the remaining tests.
//
// We need the following folder structure to be present:
//
// "/emRoDiWiLePr"
// "/nested/root/dir/prefix/neCuRoDiWiLePr"
// "/root_dir_prefix/roDiNoSlWiLePr"
// "/slRoDiWiLePr"
// "emRoDi"
// "nested/root/dir/prefix/neCuRoDi"
// "root_dir_prefix/roDiNoSl"
// "slRoDi"
//
// The content of the files is irrelevant as long as the size is 3542 bytes.
const sampleFileSize = 3542

func ensureBlobFuncFactory(t *testing.T) (string, func(absPath string) func()) {
	ctx, cancelF := context.WithCancel(context.Background())
	t.Cleanup(cancelF)

	rawParams, err := fetchDriverConfig("", false)
	require.NoError(t, err)

	var azBlobClient *azblob.Client
	var container string

	switch params := rawParams.(type) {
	case *v1.DriverParameters:
		cred, err := azblob.NewSharedKeyCredential(params.AccountName, params.AccountKey)
		require.NoError(t, err)

		azBlobClient, err = azblob.NewClientWithSharedKeyCredential(fmt.Sprintf("https://%s.blob.core.windows.net", accountName), cred, nil)
		require.NoError(t, err)

		container = params.Container
	case *v2.DriverParameters:
		switch params.CredentialsType {
		case common.CredentialsTypeSharedKey:
			cred, err := azblob.NewSharedKeyCredential(params.AccountName, params.AccountKey)
			require.NoError(t, err)

			azBlobClient, err = azblob.NewClientWithSharedKeyCredential(params.ServiceURL, cred, nil)
			require.NoError(t, err)
		case common.CredentialsTypeClientSecret, common.CredentialsTypeDefaultCredentials:
			var cred azcore.TokenCredential

			if params.CredentialsType == common.CredentialsTypeClientSecret {
				cred, err = azidentity.NewClientSecretCredential(
					params.TenantID,
					params.ClientID,
					params.Secret,
					nil,
				)
				require.NoError(t, err)
			} else {
				// params.credentialsType == credentialsTypeDefaultCredentials
				cred, err = azidentity.NewDefaultAzureCredential(nil)
				require.NoError(t, err)
			}

			azBlobClient, err = azblob.NewClient(params.ServiceURL, cred, nil)
			require.NoError(t, err)
		default:
			require.FailNowf(t, "invalid credentials type: %q", params.CredentialsType)
		}
		container = params.Container
	}

	azBlobContainerClient := azBlobClient.ServiceClient().NewContainerClient(container)

	generateRandomString := func(strLen int) string {
		const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		b := make([]byte, strLen)
		for i := range b {
			b[i] = charset[rand.IntN(len(charset))]
		}
		return string(b)
	}
	randomSuffix := generateRandomString(16)
	contents := generateRandomString(sampleFileSize)

	ensureBlobF := func(absPath string) func() {
		uniqPath := fmt.Sprintf("%s-%s", absPath, randomSuffix)

		blobRef := azBlobContainerClient.NewBlobClient(uniqPath)
		// Check if the path is a blob
		props, err := blobRef.GetProperties(ctx, nil)
		if err != nil {
			if !v2.Is404(err) {
				require.NoError(t, err)
			}
		} else {
			if *props.ContentLength != sampleFileSize {
				_, err := blobRef.Delete(ctx, nil)
				require.NoError(t, err)
			}
		}

		_, err = azBlobContainerClient.NewBlockBlobClient(uniqPath).UploadBuffer(ctx, []byte(contents), nil)
		require.NoError(t, err)

		return func() {
			_, err := blobRef.Delete(ctx, nil)
			if err != nil && v2.Is404(err) {
				t.Logf("warning: file %q has already been deleted", uniqPath)
				return
			}
			require.NoError(t, err)
		}
	}

	return randomSuffix, ensureBlobF
}

func TestAzureDriverRootPathStat(t *testing.T) {
	if skipCheck() != "" {
		t.Skip(skipCheck())
	}

	suffix, ensureBlobF := ensureBlobFuncFactory(t)

	tests := []struct {
		name          string
		rootDirectory string
		legacyPath    bool
		filename      string
		fileToCreate  string
	}{
		{
			name:          "empty root directory with legacy prefix",
			rootDirectory: "",
			legacyPath:    true,
			filename:      "emRoDiWiLePr",
			fileToCreate:  "/emRoDiWiLePr",
		},
		{
			name:          "empty root directory",
			rootDirectory: "",
			filename:      "emRoDi",
			fileToCreate:  "emRoDi",
		},
		{
			name:          "slash root directory with legacy prefix",
			rootDirectory: "/",
			legacyPath:    true,
			filename:      "slRoDiWiLePr",
			fileToCreate:  "/slRoDiWiLePr",
		},
		{
			name:          "slash root directory",
			rootDirectory: "/",
			filename:      "slRoDi",
			fileToCreate:  "slRoDi",
		},
		{
			name:          "root directory no slashes with legacy prefix",
			rootDirectory: "root_dir_prefix",
			legacyPath:    true,
			filename:      "roDiNoSlWiLePr",
			fileToCreate:  "/root_dir_prefix/roDiNoSlWiLePr",
		},
		{
			name:          "root directory no slashes",
			rootDirectory: "root_dir_prefix",
			filename:      "roDiNoSl",
			fileToCreate:  "root_dir_prefix/roDiNoSl",
		},
		{
			name:          "nested custom root directory with legacy prefix",
			rootDirectory: "nested/root/dir/prefix",
			legacyPath:    true,
			filename:      "neCuRoDiWiLePr",
			fileToCreate:  "/nested/root/dir/prefix/neCuRoDiWiLePr",
		},
		{
			name:          "nested custom root directory",
			rootDirectory: "nested/root/dir/prefix",
			filename:      "neCuRoDi",
			fileToCreate:  "nested/root/dir/prefix/neCuRoDi",
		},
	}

	for _, tt := range tests {
		cleanupF := ensureBlobF(tt.fileToCreate)
		t.Cleanup(cleanupF)

		t.Run(tt.name, func(t *testing.T) {
			d, err := azureDriverConstructor(tt.rootDirectory, !tt.legacyPath)
			require.NoError(t, err)

			fsInfo, err := d.Stat(context.Background(), fmt.Sprintf("/%s-%s", tt.filename, suffix))
			require.NoError(t, err)
			require.False(t, fsInfo.IsDir())
			require.EqualValues(t, sampleFileSize, fsInfo.Size())
		})
	}
}

func TestAzureDriverRootPathList(t *testing.T) {
	if skipCheck() != "" {
		t.Skip(skipCheck())
	}

	suffix, ensureBlobF := ensureBlobFuncFactory(t)

	tests := []struct {
		name          string
		rootDirectory string
		legacyPath    bool
		filename      string
		fileToCreate  string
	}{
		{
			name:          "empty root directory with legacy prefix",
			rootDirectory: "",
			legacyPath:    true,
			filename:      "emRoDiWiLePr",
			fileToCreate:  "/emRoDiWiLePr",
		},
		{
			name:          "empty root directory",
			rootDirectory: "",
			filename:      "emRoDi",
			fileToCreate:  "emRoDi",
		},
		{
			name:          "slash root directory with legacy prefix",
			rootDirectory: "/",
			legacyPath:    true,
			filename:      "slRoDiWiLePr",
			fileToCreate:  "/slRoDiWiLePr",
		},
		{
			name:          "slash root directory",
			rootDirectory: "/",
			filename:      "slRoDi",
			fileToCreate:  "slRoDi",
		},
		{
			name:          "root directory no slashes with legacy prefix",
			rootDirectory: "root_dir_prefix",
			legacyPath:    true,
			filename:      "roDiNoSlWiLePr",
			fileToCreate:  "/root_dir_prefix/roDiNoSlWiLePr",
		},
		{
			name:          "root directory no slashes",
			rootDirectory: "root_dir_prefix",
			filename:      "roDiNoSl",
			fileToCreate:  "root_dir_prefix/roDiNoSl",
		},
		{
			name:          "nested custom root directory with legacy prefix",
			rootDirectory: "nested/root/dir/prefix",
			legacyPath:    true,
			filename:      "neCuRoDiWiLePr",
			fileToCreate:  "/nested/root/dir/prefix/neCuRoDiWiLePr",
		},
		{
			name:          "nested custom root directory",
			rootDirectory: "nested/root/dir/prefix",
			filename:      "neCuRoDi",
			fileToCreate:  "nested/root/dir/prefix/neCuRoDi",
		},
	}

	for _, tt := range tests {
		cleanupF := ensureBlobF(tt.fileToCreate)
		t.Cleanup(cleanupF)

		t.Run(tt.name, func(t *testing.T) {
			d, err := azureDriverConstructor(tt.rootDirectory, !tt.legacyPath)
			require.NoError(t, err)

			files, err := d.List(context.Background(), "/")
			require.NoError(t, err)
			require.Contains(t, files, fmt.Sprintf("/%s-%s", tt.filename, suffix))
		})
	}
}

func TestAzureDriver_parseParameters_Bool(t *testing.T) {
	if skipCheck() != "" {
		t.Skip(skipCheck())
	}

	p := map[string]any{
		"accountname": "accountName",
		"accountkey":  "accountKey",
		"container":   "container",
		// TODO: add string test cases, if needed?
	}

	opts := dtestutil.Opts{
		Defaultt:          true,
		ParamName:         common.ParamTrimLegacyRootPrefix,
		DriverParamName:   "TrimLegacyRootPrefix",
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
