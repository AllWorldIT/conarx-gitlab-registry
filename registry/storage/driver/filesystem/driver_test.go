package filesystem

import (
	"context"
	"os"
	"path"
	"testing"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestFilesystemDriverSuite(t *testing.T) {
	root, err := os.MkdirTemp("", "fsdriver-test-")
	require.NoError(t, err)

	ts := testsuites.NewDriverSuite(
		context.Background(),
		func() (storagedriver.StorageDriver, error) {
			return FromParameters(map[string]interface{}{
				"rootdirectory": root,
			})
		},
		func() error {
			return os.Remove(root)
		},
	)
	suite.Run(t, ts)
}

func BenchmarkFilesystemDriverSuite(b *testing.B) {
	root, err := os.MkdirTemp("", "fsdriver-bench-")
	require.NoError(b, err)

	ts := testsuites.NewDriverSuite(
		context.Background(),
		func() (storagedriver.StorageDriver, error) {
			return FromParameters(map[string]interface{}{
				"rootdirectory": root,
			})
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

func TestFilesystemDriverFromParametersImpl(t *testing.T) {
	tests := []struct {
		params   map[string]interface{} // technically the yaml can contain anything
		expected DriverParameters
		pass     bool
	}{
		// check we use default threads and root dirs
		{
			params: map[string]interface{}{},
			expected: DriverParameters{
				RootDirectory: defaultRootDirectory,
				MaxThreads:    defaultMaxThreads,
			},
			pass: true,
		},
		// Testing initiation with a string maxThreads which can't be parsed
		{
			params: map[string]interface{}{
				"maxthreads": "fail",
			},
			expected: DriverParameters{},
			pass:     false,
		},
		{
			params: map[string]interface{}{
				"maxthreads": "100",
			},
			expected: DriverParameters{
				RootDirectory: defaultRootDirectory,
				MaxThreads:    uint64(100),
			},
			pass: true,
		},
		{
			params: map[string]interface{}{
				"maxthreads": 100,
			},
			expected: DriverParameters{
				RootDirectory: defaultRootDirectory,
				MaxThreads:    uint64(100),
			},
			pass: true,
		},
		// check that we use minimum thread counts
		{
			params: map[string]interface{}{
				"maxthreads": 1,
			},
			expected: DriverParameters{
				RootDirectory: defaultRootDirectory,
				MaxThreads:    minThreads,
			},
			pass: true,
		},
	}

	for _, item := range tests {
		params, err := fromParametersImpl(item.params)

		if !item.pass {
			// We only need to assert that expected failures have an error
			require.Error(t, err)
			continue
		}

		require.NoError(t, err)

		// Note that we get a pointer to params back
		require.Equal(t, item.expected, *params)
	}
}

// TestDeleteFilesEmptyParentDir checks that DeleteFiles removes parent directories if empty.
func TestFilesystemDriverDeleteFilesEmptyParentDir(t *testing.T) {
	d := newTempDirDriver(t)

	parentDir := "/testdir"
	fp := path.Join(parentDir, "testfile")
	ctx := context.Background()

	err := d.PutContent(ctx, fp, []byte("contents"))
	require.NoError(t, err)

	_, err = d.DeleteFiles(ctx, []string{fp})
	require.NoError(t, err)

	// check deleted file
	_, err = d.Stat(ctx, fp)
	require.ErrorAs(t, err, new(storagedriver.PathNotFoundError))

	// make sure the parent directory has been removed
	_, err = d.Stat(ctx, parentDir)
	require.ErrorAs(t, err, new(storagedriver.PathNotFoundError))
}

// TestDeleteFilesNonEmptyParentDir checks that DeleteFiles does not remove parent directories if not empty.
func TestFilesystemDriverDeleteFilesNonEmptyParentDir(t *testing.T) {
	d := newTempDirDriver(t)

	parentDir := "/testdir"
	fp := path.Join(parentDir, "testfile")
	ctx := context.Background()

	err := d.PutContent(ctx, fp, []byte("contents"))
	require.NoError(t, err)

	// add another test file, this one is not going to be deleted
	err = d.PutContent(ctx, path.Join(parentDir, "testfile2"), []byte("contents"))
	require.NoError(t, err)

	_, err = d.DeleteFiles(ctx, []string{fp})
	require.NoError(t, err)

	// check deleted file
	_, err = d.Stat(ctx, fp)
	require.ErrorAs(t, err, new(storagedriver.PathNotFoundError))

	// make sure the parent directory has not been removed
	_, err = d.Stat(ctx, parentDir)
	require.NoError(t, err)
}

// TestDeleteFilesNonExistingParentDir checks that DeleteFiles is idempotent and doesn't return an error if a parent dir
// of a not found file doesn't exist as well.
func TestFilesystemDriverDeleteFilesNonExistingParentDir(t *testing.T) {
	d := newTempDirDriver(t)

	fp := path.Join("/non-existing-dir", "non-existing-file")
	count, err := d.DeleteFiles(context.Background(), []string{fp})
	if err != nil {
		t.Errorf("unexpected error deleting files: %v", err)
	}
	if count != 1 {
		t.Errorf("expected deleted count to be 1, got %d", count)
	}
}

func newTempDirDriver(t *testing.T) *Driver {
	t.Helper()

	rootDir := t.TempDir()

	d, err := FromParameters(map[string]interface{}{
		"rootdirectory": rootDir,
	})
	require.NoError(t, err)

	return d
}
