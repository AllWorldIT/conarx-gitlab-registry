package azure

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/benbjohnson/clock"
	"github.com/docker/distribution/registry/internal/testutil"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/azure/common"
	v1 "github.com/docker/distribution/registry/storage/driver/azure/v1"
	v2 "github.com/docker/distribution/registry/storage/driver/azure/v2"
	dtestutil "github.com/docker/distribution/registry/storage/driver/internal/testutil"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	btestutil "github.com/docker/distribution/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var (
	driverVersion     string
	parseParametersFn func(map[string]any) (any, error)
	newDriverFn       func(any) (storagedriver.StorageDriver, error)

	missing []string

	credsType string

	accountName string
	accountKey  string

	tenantID string
	clientID string
	secret   string

	accountContainer string
	accountRealm     string

	maxRetries      int32
	retryTryTimeout time.Duration
	retryDelay      time.Duration
	maxRetryDelay   time.Duration

	debugLog       bool
	debugLogEvents string
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

	vars := []envConfig{
		{common.EnvContainer, &accountContainer, true},
		{common.EnvRealm, &accountRealm, true},
		{common.EnvAccountName, &accountName, true},
		{common.EnvDebugLog, &debugLog, false},
		{common.EnvDebugLogEvents, &debugLogEvents, false},
		{common.EnvMaxRetries, &maxRetries, false},
		{common.EnvRetryTryTimeout, &retryTryTimeout, false},
		{common.EnvRetryDelay, &retryDelay, false},
		{common.EnvMaxRetryDelay, &maxRetryDelay, false},
	}
	switch credsType {
	case common.CredentialsTypeSharedKey, "":
		credsType = common.CredentialsTypeSharedKey
		vars = append(vars, envConfig{common.EnvAccountKey, &accountKey, true})
	case common.CredentialsTypeClientSecret:
		vars = append(vars,
			envConfig{common.EnvTenantID, &tenantID, true},
			envConfig{common.EnvClientID, &clientID, true},
			envConfig{common.EnvSecret, &secret, true},
		)
	case common.CredentialsTypeDefaultCredentials:
	// no extra parameters needed
	default:
		msg := fmt.Sprintf("invalid azure credentials type: %q", credsType)
		missing = []string{msg}
		return
	}

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
		case *int32:
			var tmp int64
			tmp, err = strconv.ParseInt(val, 10, 32)
			*vv = int32(tmp)
		case *time.Duration:
			*vv, err = time.ParseDuration(val)
		}

		if err != nil {
			missing = append(
				missing,
				fmt.Sprintf("invalid value for %q: %s", v.env, val),
			)
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
		if debugLogEvents != "" {
			// NOTE(prozlach): unset means simply log everything
			rawParams[common.ParamDebugLogEvents] = debugLogEvents
		}
	}

	if maxRetries != 0 {
		rawParams[common.ParamMaxRetries] = maxRetries
	}
	if retryTryTimeout != 0 {
		rawParams[common.ParamRetryTryTimeout] = retryTryTimeout.String()
	}
	if retryDelay != 0 {
		rawParams[common.ParamRetryDelay] = retryDelay.String()
	}
	if maxRetryDelay != 0 {
		rawParams[common.ParamMaxRetryDelay] = maxRetryDelay.String()
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
		return fmt.Sprintf(
			"Invalid value or missing environment values required to run Azure tests: %s",
			strings.Join(missing, ", "),
		)
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

func TestAzureDriverRetriesHandling(t *testing.T) {
	if driverVersion != v2.DriverName {
		t.Skip("retries testing is only supported on Azure v2 ATM")
	}

	ctx := context.Background()
	rootDirectory := t.TempDir()
	blobber := btestutil.NewBlober(btestutil.RandomBlob(t, v2.MaxChunkSize*2+1<<20))

	testFuncMoveAllOK := func(
		tt *testing.T,
		sDriver storagedriver.StorageDriver,
		filename string,
		isTestValidf func() bool,
	) bool {
		contents := blobber.GetAllBytes()
		sourcePath := filename + "_src"
		destPath := filename + "_dst"

		err := sDriver.PutContent(ctx, sourcePath, contents)
		require.NoError(tt, err)

		err = sDriver.Move(ctx, sourcePath, destPath)
		if !isTestValidf() {
			return false
		}
		require.NoError(tt, err)

		received, err := sDriver.GetContent(ctx, destPath)
		require.NoError(tt, err)
		require.Equal(tt, contents, received)

		_, err = sDriver.GetContent(ctx, sourcePath)
		require.ErrorIs(tt, err, storagedriver.PathNotFoundError{
			DriverName: sDriver.Name(),
			Path:       sourcePath,
		})
		return true
	}

	testFuncWriterAllOK := func(
		tt *testing.T,
		sDriver storagedriver.StorageDriver,
		filename string,
		isTestValidf func() bool,
	) bool {
		writer, err := sDriver.Writer(ctx, filename, false)
		require.NoError(tt, err)

		nn, err := io.Copy(writer, blobber.GetReader())
		if !isTestValidf() {
			return false
		}
		require.NoError(tt, err)
		require.EqualValues(tt, blobber.Size(), nn)

		err = writer.Commit()
		require.NoError(tt, err)
		err = writer.Close()
		require.NoError(tt, err)
		require.EqualValues(tt, blobber.Size(), writer.Size())

		reader, err := sDriver.Reader(ctx, filename, 0)
		require.NoError(tt, err)
		defer reader.Close()

		blobber.RequireStreamEqual(tt, reader, 0, fmt.Sprintf("filename: %s", filename))
		return true
	}

	testFuncWriterErrorContains := func(errMsg string) func(*testing.T, storagedriver.StorageDriver, string, func() bool) bool {
		return func(
			tt *testing.T,
			sDriver storagedriver.StorageDriver,
			filename string,
			isTestValidf func() bool,
		) bool {
			writer, err := sDriver.Writer(ctx, filename, false)
			require.NoError(tt, err)

			_, err = io.Copy(writer, blobber.GetReader())
			if !isTestValidf() {
				return false
			}
			assert.ErrorContains(tt, err, errMsg)
			return true
		}
	}

	matchAlways := func(*testing.T) dtestutil.RequestMatcher {
		return func(*http.Request) bool { return true }
	}

	matchMoveFirstStartCopyRequest := func(*testing.T) dtestutil.RequestMatcher {
		reqNumber := 0

		return func(req *http.Request) bool {
			if req.Method != http.MethodPut {
				return false
			}

			if !strings.HasSuffix(req.URL.Path, "_dst") {
				return false
			}

			v := req.Header.Get("x-ms-copy-source")
			if v == "" {
				// (╯°□°)╯︵ ┻━┻
				// nolint: staticcheck // no, keys are not always canonicalized
				// it would seem
				vs := req.Header["x-ms-copy-source"]
				if len(vs) > 0 {
					v = vs[0]
				}
			}
			if !strings.HasSuffix(v, "_src") {
				return false
			}

			reqNumber++
			return reqNumber == 1
		}
	}

	matchWriterSecondUploadRequest := func(*testing.T) dtestutil.RequestMatcher {
		reqNumber := 0

		return func(req *http.Request) bool {
			if req.Method != http.MethodPut {
				return false
			}

			reqQP := req.URL.Query()
			comp := reqQP.Get("comp")
			if comp != "appendblock" {
				return false
			}

			// There are going to be three requests like this, we want to
			// break only the second one for maximum effect
			reqNumber++
			return reqNumber == 2
		}
	}

	makeResponseOperationTimeout := func(tt *testing.T) dtestutil.ResponseModifier {
		return func(resp *http.Response) (*http.Response, bool) {
			if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusAccepted {
				tt.Logf("makeResponseOperationTimeout, response not matched: %d", resp.StatusCode)
				return resp, false
			}

			resp.StatusCode = http.StatusInternalServerError
			resp.Status = "Operation timed out"
			resp.Header.Set("x-ms-error-code", string(bloberror.OperationTimedOut))
			tt.Log("response marked as timed out")

			return resp, true
		}
	}

	makeResponseNonretryableError := func(tt *testing.T) dtestutil.ResponseModifier {
		return func(resp *http.Response) (*http.Response, bool) {
			if resp.StatusCode != http.StatusCreated {
				tt.Logf("makeResponseNonretryableError, response not matched: %d", resp.StatusCode)
				return resp, false
			}

			// Make sure this is not a retryable error for Azure:
			resp.StatusCode = http.StatusInsufficientStorage
			resp.Header.Set("x-ms-error-code", "Boryna ran out of Maryna")
			tt.Log("response marked as non-retryable error")

			return resp, true
		}
	}

	corruptWriterUploadedChunk := func(tt *testing.T) dtestutil.RequestModifier {
		return func(req *http.Request) (*http.Request, bool) {
			var err error

			req.Body, err = dtestutil.RandomizeTail(req.Body)
			require.NoError(tt, err)
			tt.Log("body data has been corrupted")

			return req, true
		}
	}

	removeMd5header := func(tt *testing.T) dtestutil.ResponseModifier {
		return func(resp *http.Response) (*http.Response, bool) {
			if resp.Header.Get("Content-Md5") == "" {
				tt.Logf("removeMd5header, response not matched")
				return resp, false
			}
			resp.Header.Del("Content-Md5")
			tt.Log("Content-Md5 header has been removed")
			return resp, true
		}
	}

	makeAppenBlockRequestInvalid := func(tt *testing.T) dtestutil.RequestModifier {
		return func(req *http.Request) (*http.Request, bool) {
			q := req.URL.Query()

			if q.Get("comp") != "appendblock" {
				tt.Logf("makeAppenBlockRequestInvalid, response not matched: %s", q.Get("comp"))
				return req, false
			}

			q.Set("comp", "fooBar")
			req.URL.RawQuery = q.Encode()
			tt.Log("request marked as invalid")
			return req, true
		}
	}

	// (╯°□°)╯︵ ┻━┻
	// nolint: staticcheck // no, keys are not always canonicalized
	makeStartCopyRequestInvalid := func(tt *testing.T) dtestutil.RequestModifier {
		return func(req *http.Request) (*http.Request, bool) {
			_, okLowercase := req.Header["x-ms-copy-source"]
			okCanonicalized := req.Header.Get("x-ms-copy-source") != ""
			if !okLowercase && !okCanonicalized {
				return req, false
			}

			delete(req.Header, "x-ms-copy-source")
			req.Header.Del("x-ms-copy-source")
			tt.Log("x-ms-copy-source header has been removed")
			return req, true
		}
	}

	tests := []struct {
		name               string
		interceptorConfigs []dtestutil.InterceptorConfig
		testFunc           func(*testing.T, storagedriver.StorageDriver, string, func() bool) bool
	}{
		{
			name:               "writer happy path - no injected failure",
			interceptorConfigs: make([]dtestutil.InterceptorConfig, 0),
			testFunc:           testFuncWriterAllOK,
		},
		{
			name: "unrecoverable error from server",
			interceptorConfigs: []dtestutil.InterceptorConfig{
				{
					Matcher:                            matchWriterSecondUploadRequest,
					RequestModifier:                    nil,
					ResponseModifier:                   makeResponseNonretryableError,
					ExpectedRequestModificationsCount:  0,
					ExpectedResponseModificationsCount: 1,
				},
			},
			testFunc: testFuncWriterErrorContains("Boryna ran out of Maryna"),
		},
		{
			name: "recovered retry",
			interceptorConfigs: []dtestutil.InterceptorConfig{
				{
					Matcher:                            matchWriterSecondUploadRequest,
					RequestModifier:                    nil,
					ResponseModifier:                   makeResponseOperationTimeout,
					ExpectedRequestModificationsCount:  0,
					ExpectedResponseModificationsCount: 1,
				},
			},
			testFunc: testFuncWriterAllOK,
		},
		{
			name: "corrupted data using MD5 header",
			interceptorConfigs: []dtestutil.InterceptorConfig{
				{
					Matcher:                            matchWriterSecondUploadRequest,
					RequestModifier:                    corruptWriterUploadedChunk,
					ResponseModifier:                   makeResponseOperationTimeout,
					ExpectedRequestModificationsCount:  1,
					ExpectedResponseModificationsCount: 1,
				},
			},
			testFunc: testFuncWriterErrorContains("corrupted data found in the uploaded data"),
		},
		{
			name: "corrupted data using direct md5 calculation",
			interceptorConfigs: []dtestutil.InterceptorConfig{
				{
					Matcher:                            matchAlways,
					RequestModifier:                    nil,
					ResponseModifier:                   removeMd5header,
					ExpectedRequestModificationsCount:  2,
					ExpectedResponseModificationsCount: 1,
				},
				{
					Matcher:                            matchWriterSecondUploadRequest,
					RequestModifier:                    corruptWriterUploadedChunk,
					ResponseModifier:                   makeResponseOperationTimeout,
					ExpectedRequestModificationsCount:  1,
					ExpectedResponseModificationsCount: 1,
				},
			},
			testFunc: testFuncWriterErrorContains("corrupted data found in the uploaded data"),
		},
		{
			name: "no data uploaded",
			interceptorConfigs: []dtestutil.InterceptorConfig{
				{
					Matcher:                            matchWriterSecondUploadRequest,
					RequestModifier:                    makeAppenBlockRequestInvalid,
					ResponseModifier:                   makeResponseOperationTimeout,
					ExpectedRequestModificationsCount:  1,
					ExpectedResponseModificationsCount: 1,
				},
			},
			testFunc: testFuncWriterAllOK,
		},
		{
			name:               "move happy path - no injected failure",
			interceptorConfigs: make([]dtestutil.InterceptorConfig, 0),
			testFunc:           testFuncMoveAllOK,
		},
		{
			// This is the case where timeout operation did not succeed - the
			// copy operation has not been started.
			name: "move call timeouts are retried",
			interceptorConfigs: []dtestutil.InterceptorConfig{
				{
					Matcher:                            matchMoveFirstStartCopyRequest,
					RequestModifier:                    makeStartCopyRequestInvalid,
					ResponseModifier:                   makeResponseOperationTimeout,
					ExpectedRequestModificationsCount:  1,
					ExpectedResponseModificationsCount: 1,
				},
			},
			testFunc: testFuncMoveAllOK,
		},
		{
			// This is the case where timeout operation succeeded - the copy
			// operation has been started and retry is causing the second one
			// to start.
			name: "move call timeout duplicates are handled",
			interceptorConfigs: []dtestutil.InterceptorConfig{
				{
					Matcher:                            matchMoveFirstStartCopyRequest,
					RequestModifier:                    nil,
					ResponseModifier:                   makeResponseOperationTimeout,
					ExpectedRequestModificationsCount:  0,
					ExpectedResponseModificationsCount: 1,
				},
			},
			testFunc: testFuncMoveAllOK,
		},
	}

	fetchDriver := func(
		tt *testing.T,
		interceptorConfigs []dtestutil.InterceptorConfig,
	) (storagedriver.StorageDriver, func() bool) {
		parsedParams, err := fetchDriverConfig(rootDirectory, true)
		require.NoError(t, err)
		typedParsedParams := parsedParams.(*v2.DriverParameters)

		interceptor, err := dtestutil.NewInterceptor()
		require.NoError(tt, err)

		expectedRequestModificationsCount := 0
		expectedResponseModificationsCount := 0
		for _, ic := range interceptorConfigs {
			if ic.RequestModifier != nil {
				interceptor.AddRequestHook(ic.Matcher(tt), ic.RequestModifier(tt))
				expectedRequestModificationsCount += ic.ExpectedRequestModificationsCount
			}
			if ic.ResponseModifier != nil {
				interceptor.AddResponseHook(ic.Matcher(tt), ic.ResponseModifier(tt))
				expectedResponseModificationsCount += ic.ExpectedResponseModificationsCount
			}
		}

		typedParsedParams.Transport = interceptor
		sDriver, err := newDriverFn(typedParsedParams)
		require.NoError(t, err)

		isTestValidf := func() bool {
			if expectedRequestModificationsCount != interceptor.GetRequestHooksMatchedCount() {
				tt.Logf("expected vs. got request modifications: %d != %d", expectedRequestModificationsCount, interceptor.GetRequestHooksMatchedCount())
				return false
			}
			if expectedResponseModificationsCount != interceptor.GetResponseHooksMatchedCount() {
				tt.Logf("expected vs. got response modifications: %d != %d", expectedResponseModificationsCount, interceptor.GetResponseHooksMatchedCount())
				return false
			}
			return true
		}

		return sDriver, isTestValidf
	}

	maxAttempts := 3
	for _, test := range tests {
		testIsValid := false
		for i := 0; i < maxAttempts && !testIsValid; i++ {
			testName := fmt.Sprintf("%s_attempt_%d", test.name, i)
			t.Run(testName, func(tt *testing.T) {
				filename := dtestutil.RandomPath(4, 32)
				tt.Logf("blob path used for testing: %s", filename)

				sDriver, isTestValidf := fetchDriver(tt, test.interceptorConfigs)

				defer dtestutil.EnsurePathDeleted(ctx, tt, sDriver, rootDirectory)

				testIsValid = test.testFunc(tt, sDriver, filename, isTestValidf)
				if !testIsValid {
					if i+1 == maxAttempts { // last iteration
						require.FailNowf(tt, "number of attempts exceeded", "despite %d attempts, preconditions for this test were not reached", maxAttempts)
					} else {
						tt.Skipf("preconditions required to continue were not met, skipping this attempt")
					}
				}
			})
		}
	}
}
