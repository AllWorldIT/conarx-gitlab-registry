//go:build include_gcs

package gcs

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/docker/distribution/registry/internal/testutil"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	dcontext "github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	dtestutil "github.com/docker/distribution/registry/storage/driver/internal/testutil"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
)

var (
	bucket       = os.Getenv("REGISTRY_STORAGE_GCS_BUCKET")
	credentials  = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	parallelWalk = os.Getenv("GCS_PARALLEL_WALK")
)

const maxConcurrency = 10

func gcsDriverConstructor(rootDirectory string) (storagedriver.StorageDriver, error) {
	ctx := context.Background()
	creds, err := google.FindDefaultCredentials(ctx, storage.ScopeFullControl)
	if err != nil {
		return nil, fmt.Errorf("Error reading default credentials: %w", err)
	}

	ts := creds.TokenSource
	jwtConfig, err := google.JWTConfigFromJSON(creds.JSON)
	if err != nil {
		return nil, fmt.Errorf("Error reading JWT config: %w", err)
	}
	email := jwtConfig.Email
	if email == "" {
		return nil, fmt.Errorf("Error reading JWT config : missing client_email property")
	}
	privateKey := jwtConfig.PrivateKey
	if len(privateKey) == 0 {
		return nil, fmt.Errorf("Error reading JWT config : missing private_key property")
	}

	storageClient, err := storage.NewClient(dcontext.Background(), option.WithTokenSource(ts))
	if err != nil {
		return nil, fmt.Errorf("Error creating storage client: %w", err)
	}

	var parallelWalkBool bool

	if parallelWalk != "" {
		parallelWalkBool, err = strconv.ParseBool(parallelWalk)
		if err != nil {
			return nil, fmt.Errorf("Error parsing parallelwalk: %w", err)
		}
	}

	parameters := &driverParameters{
		bucket:         bucket,
		rootDirectory:  rootDirectory,
		email:          email,
		privateKey:     privateKey,
		client:         oauth2.NewClient(dcontext.Background(), ts),
		storageClient:  storageClient,
		chunkSize:      defaultChunkSize,
		maxConcurrency: maxConcurrency,
		parallelWalk:   parallelWalkBool,
	}

	return New(parameters)
}

func skipGCS() string {
	if bucket == "" || credentials == "" {
		return "The following environment variables must be set to enable these tests: REGISTRY_STORAGE_GCS_BUCKET, GOOGLE_APPLICATION_CREDENTIALS"
	}
	return ""
}

func TestGCSDriverSuite(t *testing.T) {
	root, err := os.MkdirTemp("", "gcsdriver-test-")
	require.NoError(t, err)

	if skipMsg := skipGCS(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	ts := testsuites.NewDriverSuite(
		context.Background(),
		func() (storagedriver.StorageDriver, error) {
			return gcsDriverConstructor(root)
		},
		func() error {
			return os.Remove(root)
		},
	)
	suite.Run(t, ts)
}

func BenchmarkGCSDriverSuite(b *testing.B) {
	root, err := os.MkdirTemp("", "gcsdriver-bench-")
	require.NoError(b, err)

	if skipMsg := skipGCS(); skipMsg != "" {
		b.Skip(skipMsg)
	}

	ts := testsuites.NewDriverSuite(
		context.Background(),
		func() (storagedriver.StorageDriver, error) {
			return gcsDriverConstructor(root)
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

// Test Committing a FileWriter without having called Write
func TestGCSDriverCommitEmpty(t *testing.T) {
	if skipMsg := skipGCS(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	driver := newTempDirDriver(t)

	filename := "/test"
	ctx := dcontext.Background()

	writer, err := driver.Writer(ctx, filename, false)
	defer driver.Delete(ctx, filename)
	if err != nil {
		t.Fatalf("driver.Writer: unexpected error: %v", err)
	}
	err = writer.Commit()
	if err != nil {
		t.Fatalf("writer.Commit: unexpected error: %v", err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatalf("writer.Close: unexpected error: %v", err)
	}
	if writer.Size() != 0 {
		t.Fatalf("writer.Size: %d != 0", writer.Size())
	}
	readContents, err := driver.GetContent(ctx, filename)
	if err != nil {
		t.Fatalf("driver.GetContent: unexpected error: %v", err)
	}
	if len(readContents) != 0 {
		t.Fatalf("len(driver.GetContent(..)): %d != 0", len(readContents))
	}
	fileInfo, err := driver.Stat(ctx, filename)
	if err != nil {
		t.Fatalf("driver.Stat: unexpected error: %v", err)
	}
	if fileInfo.Size() != 0 {
		t.Fatalf("stat.Size: %d != 0", fileInfo.Size())
	}
}

// Test Committing a FileWriter after having written exactly
// defaultChunksize bytes.
func TestGCSDriverCommit(t *testing.T) {
	if skipMsg := skipGCS(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	driver := newTempDirDriver(t)

	filename := "/test"
	ctx := dcontext.Background()

	contents := make([]byte, defaultChunkSize)
	writer, err := driver.Writer(ctx, filename, false)
	defer driver.Delete(ctx, filename)
	if err != nil {
		t.Fatalf("driver.Writer: unexpected error: %v", err)
	}
	_, err = writer.Write(contents)
	if err != nil {
		t.Fatalf("writer.Write: unexpected error: %v", err)
	}
	err = writer.Commit()
	if err != nil {
		t.Fatalf("writer.Commit: unexpected error: %v", err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatalf("writer.Close: unexpected error: %v", err)
	}
	if writer.Size() != int64(len(contents)) {
		t.Fatalf("writer.Size: %d != %d", writer.Size(), len(contents))
	}
	readContents, err := driver.GetContent(ctx, filename)
	if err != nil {
		t.Fatalf("driver.GetContent: unexpected error: %v", err)
	}
	if len(readContents) != len(contents) {
		t.Fatalf("len(driver.GetContent(..)): %d != %d", len(readContents), len(contents))
	}
	fileInfo, err := driver.Stat(ctx, filename)
	if err != nil {
		t.Fatalf("driver.Stat: unexpected error: %v", err)
	}
	if fileInfo.Size() != int64(len(contents)) {
		t.Fatalf("driver.Stat.Size: %d != %d", fileInfo.Size(), len(contents))
	}
}

func TestGCSDriverRetry(t *testing.T) {
	if skipMsg := skipGCS(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	assertError := func(expected string, observed error) {
		observedMsg := "<nil>"
		if observed != nil {
			observedMsg = observed.Error()
		}
		if observedMsg != expected {
			t.Fatalf("expected %v, observed %v\n", expected, observedMsg)
		}
	}

	err := retry(func() error {
		return &googleapi.Error{
			Code:    503,
			Message: "google api error",
		}
	})
	assertError("googleapi: Error 503: google api error", err)

	err = retry(func() error {
		return &googleapi.Error{
			Code:    404,
			Message: "google api error",
		}
	})
	assertError("googleapi: Error 404: google api error", err)

	err = retry(func() error {
		return fmt.Errorf("error")
	})
	assertError("error", err)
}

func TestGCSDriverEmptyRootList(t *testing.T) {
	if skipMsg := skipGCS(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	rootedDriver := newTempDirDriver(t)

	emptyRootDriver, err := gcsDriverConstructor("")
	if err != nil {
		t.Fatalf("unexpected error creating empty root driver: %v", err)
	}

	slashRootDriver, err := gcsDriverConstructor("/")
	if err != nil {
		t.Fatalf("unexpected error creating slash root driver: %v", err)
	}

	filename := "/test"
	contents := []byte("contents")
	ctx := dcontext.Background()
	err = rootedDriver.PutContent(ctx, filename, contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}
	defer func() {
		err := rootedDriver.Delete(ctx, filename)
		if err != nil {
			t.Fatalf("failed to remove %v due to %v\n", filename, err)
		}
	}()
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

// Test subpaths are included properly
func TestGCSDriverSubpathList(t *testing.T) {
	if skipMsg := skipGCS(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	rootedDriver := newTempDirDriver(t)

	filenames := []string{
		"/test/test1.txt",
		"/test/test2.txt",
		"/test/subpath/test3.txt",
		"/test/subpath/test4.txt",
		"/test/subpath/path/test5.txt",
	}
	contents := []byte("contents")
	ctx := dcontext.Background()

	for _, filename := range filenames {
		err := rootedDriver.PutContent(ctx, filename, contents)
		if err != nil {
			t.Fatalf("unexpected error creating content: %v", err)
		}
	}
	defer func() {
		for _, filename := range filenames {
			err := rootedDriver.Delete(ctx, filename)
			if err != nil {
				t.Fatalf("failed to remove %v due to %v\n", filename, err)
			}
		}
	}()

	keys, err := rootedDriver.List(ctx, "/test")
	require.NoError(t, err)

	expected := []string{"/test/test1.txt", "/test/test2.txt", "/test/subpath"}
	sort.Strings(expected)
	sort.Strings(keys)

	if !reflect.DeepEqual(expected, keys) {
		t.Fatalf("list %v does not match %v", keys, expected)
	}
}

// TestMoveDirectory checks that moving a directory returns an error.
func TestGCSDriverMoveDirectory(t *testing.T) {
	if skipMsg := skipGCS(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	driver := newTempDirDriver(t)

	ctx := dcontext.Background()
	contents := []byte("contents")
	// Create a regular file.
	err := driver.PutContent(ctx, "/parent/dir/foo", contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}
	defer func() {
		err := driver.Delete(ctx, "/parent")
		if err != nil {
			t.Fatalf("failed to remove /parent due to %v\n", err)
		}
	}()

	err = driver.Move(ctx, "/parent/dir", "/parent/other")
	if err == nil {
		t.Fatalf("Moving directory /parent/dir /parent/other should have return a non-nil error\n")
	}
}

func TestGCSDriverExistsPath(t *testing.T) {
	if skipMsg := skipGCS(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	root := t.TempDir()
	d, err := gcsDriverConstructor(root)
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

func TestGCSDriverExistsPath_NotFound(t *testing.T) {
	if skipMsg := skipGCS(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	root := t.TempDir()
	d, err := gcsDriverConstructor(root)
	require.NoError(t, err)

	exists, err := d.ExistsPath(context.Background(), "/non-existing/path")
	require.NoError(t, err)
	require.False(t, exists)
}

// TestExistsPath_Object asserts that if trying to use ExistsPath with an object path, this does not cause an
// internal error but rather return false.
func TestGCSDriverExistsPath_Object(t *testing.T) {
	if skipMsg := skipGCS(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	root := t.TempDir()
	d, err := gcsDriverConstructor(root)
	require.NoError(t, err)

	ctx := context.Background()

	path := "/existing/path/object"

	content := make([]byte, 10)
	rand.Read(content)

	err = d.PutContent(ctx, path, content)
	require.NoError(t, err)

	exists, err := d.ExistsPath(ctx, path)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestGCSDriver_parseParameters_Bool(t *testing.T) {
	p := map[string]interface{}{
		"bucket":  "bucket",
		"keyfile": "testdata/key.json",
		// TODO: add string test cases, if needed?
	}

	testFn := func(params map[string]interface{}) (interface{}, error) {
		return parseParameters(params)
	}

	opts := dtestutil.Opts{
		Defaultt:          false,
		ParamName:         "parallelwalk",
		DriverParamName:   "parallelWalk",
		OriginalParams:    p,
		ParseParametersFn: testFn,
	}
	dtestutil.AssertByDefaultType(t, opts)
}

func newTempDirDriver(tb testing.TB) storagedriver.StorageDriver {
	tb.Helper()

	root := tb.TempDir()

	d, err := gcsDriverConstructor(root)
	require.NoError(tb, err)

	return d
}

func TestGCSDriverURLFor_Expiry(t *testing.T) {
	if skipMsg := skipGCS(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	ctx := context.Background()
	validRoot := t.TempDir()
	d, err := gcsDriverConstructor(validRoot)
	require.NoError(t, err)

	fp := "/foo"
	err = d.PutContent(ctx, fp, []byte(`bar`))
	require.NoError(t, err)

	// https://cloud.google.com/storage/docs/access-control/signed-urls#example
	param := "X-Goog-Expires"

	mock := clock.NewMock()
	mock.Set(time.Now())
	testutil.StubClock(t, &systemClock, mock)

	// default
	s, err := d.URLFor(ctx, fp, nil)
	require.NoError(t, err)
	u, err := url.Parse(s)
	require.NoError(t, err)

	dt := mock.Now().Add(20 * time.Minute)
	expected := int(dt.Sub(mock.Now()).Seconds())
	actual, err := strconv.Atoi(u.Query().Get(param))
	require.NoError(t, err)
	require.LessOrEqual(t, expected-5, actual, "actual 'X-Goog-Expires' param is less than expected by more than 5 seconds")
	require.GreaterOrEqual(t, expected, actual, "actual 'X-Goog-Expires' param is greater than expected")

	// custom
	dt = mock.Now().Add(1 * time.Hour)
	s, err = d.URLFor(ctx, fp, map[string]interface{}{"expiry": dt})
	require.NoError(t, err)

	u, err = url.Parse(s)
	require.NoError(t, err)

	expected = int(dt.Sub(mock.Now()).Seconds())
	actual, err = strconv.Atoi(u.Query().Get(param))
	require.NoError(t, err)
	require.LessOrEqual(t, expected-5, actual, "actual 'X-Goog-Expires' param is less than expected by more than 5 seconds")
	require.GreaterOrEqual(t, expected, actual, "actual 'X-Goog-Expires' param is greater than expected")
}

func TestGCSDriverURLFor_AdditionalQueryParams(t *testing.T) {
	if skipMsg := skipGCS(); skipMsg != "" {
		t.Skip(skipMsg)
	}

	ctx := context.Background()
	validRoot := t.TempDir()
	d, err := gcsDriverConstructor(validRoot)
	require.NoError(t, err)

	fp := "/foo"
	err = d.PutContent(ctx, fp, []byte(`bar`))
	require.NoError(t, err)

	// default
	s, err := d.URLFor(ctx, fp, nil)
	require.NoError(t, err)
	u, err := url.Parse(s)
	require.NoError(t, err)
	require.Empty(t, u.Query().Get(customGitlabGoogleNamespaceIdParam))
	require.Empty(t, u.Query().Get(customGitlabGoogleProjectIdParam))
	require.Empty(t, u.Query().Get(customGitlabGoogleAuthTypeParam))
	require.Empty(t, u.Query().Get(customGitlabGoogleObjectSizeParam))

	// custom
	opts := map[string]any{
		"namespace_id": int64(345),
		"project_id":   int64(123),
		"auth_type":    "pat",
		"size_bytes":   int64(123),
	}
	s, err = d.URLFor(ctx, fp, opts)
	require.NoError(t, err)

	u, err = url.Parse(s)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%v", opts["namespace_id"]), u.Query().Get(customGitlabGoogleNamespaceIdParam))
	require.Equal(t, fmt.Sprintf("%v", opts["project_id"]), u.Query().Get(customGitlabGoogleProjectIdParam))
	require.Equal(t, opts["auth_type"], u.Query().Get(customGitlabGoogleAuthTypeParam))
	require.EqualValues(t, fmt.Sprintf("%v", opts["size_bytes"]), u.Query().Get(customGitlabGoogleObjectSizeParam))
}

func TestGCSDriverCustomParams(t *testing.T) {
	tests := []struct {
		name              string
		opt               map[string]any
		expectedURLValues url.Values
	}{
		{
			name: "all params",
			opt: map[string]any{
				"namespace_id": int64(345),
				"project_id":   int64(123),
				"auth_type":    "pat",
				"size_bytes":   int64(123),
			},
			expectedURLValues: url.Values{
				customGitlabGoogleAuthTypeParam:    []string{"pat"},
				customGitlabGoogleProjectIdParam:   []string{"123"},
				customGitlabGoogleNamespaceIdParam: []string{"345"},
				customGitlabGoogleObjectSizeParam:  []string{"123"},
			},
		},
		{
			name: "no known params",
			opt: map[string]any{
				"method": "GET",
			},
			expectedURLValues: nil,
		},
		{
			name: "some params",
			opt: map[string]any{
				"namespace_id": int64(345),
				"project_id":   int64(123),
			},
			expectedURLValues: url.Values{
				customGitlabGoogleProjectIdParam:   []string{"123"},
				customGitlabGoogleNamespaceIdParam: []string{"345"},
			},
		},
		{
			name: "unexpected type params",
			opt: map[string]any{
				"auth_type":    []string{"pat", "ldap"},
				"namespace_id": int64(345),
				"project_id":   int64(123),
				"size_bytes":   time.Now(),
			},
			expectedURLValues: url.Values{
				customGitlabGoogleProjectIdParam:   []string{"123"},
				customGitlabGoogleNamespaceIdParam: []string{"345"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expectedURLValues, storagedriver.CustomParams(test.opt, customParamKeys))
		})
	}
}
