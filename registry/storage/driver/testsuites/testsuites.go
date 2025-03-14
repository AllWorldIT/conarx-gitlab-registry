package testsuites

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/libtrust"

	"github.com/docker/distribution/registry/storage"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	azure_v1 "github.com/docker/distribution/registry/storage/driver/azure/v1"
	azure_v2 "github.com/docker/distribution/registry/storage/driver/azure/v2"
	dtestutil "github.com/docker/distribution/registry/storage/driver/internal/testutil"
	"github.com/docker/distribution/testutil"
)

// rngCacheSize is used to pre-allocate a blob of pseudo-random data that is
// later re-used among tests. This way we speed up tests by not having to
// re-generate it multiple times and save memory as all blobs used by tests
// reference to this blob (or slices of it).
const rngCacheSize int64 = 2 * 1 << 30

// DriverConstructor is a function which returns a new
// storagedriver.StorageDriver.
type DriverConstructor func() (storagedriver.StorageDriver, error)

// DriverTeardown is a function which cleans up a suite's
// storagedriver.StorageDriver.
type DriverTeardown func() error

func NewDriverSuite(
	ctx context.Context,
	constructor, constructorRootLess DriverConstructor,
	destructor DriverTeardown,
) *DriverSuite {
	return &DriverSuite{
		ctx:                 ctx,
		Constructor:         constructor,
		ConstructorRootless: constructorRootLess,
		Teardown:            destructor,
	}
}

// DriverSuite is a test suite designed to test a
// storagedriver.StorageDriver.
type DriverSuite struct {
	suite.Suite

	Constructor DriverConstructor
	// The purpose for ConstructorRootless is to enable testing un-prefixed
	// functionality of storage drivers. Care needs to be taken though as the
	// tests are going to use the same storage container so there is a risk for
	// collisions.
	ConstructorRootless   DriverConstructor
	Teardown              DriverTeardown
	StorageDriver         storagedriver.StorageDriver
	StorageDriverRootless storagedriver.StorageDriver
	ctx                   context.Context

	blobberFactory *testutil.BlobberFactory
}

// SetupSuite sets up the test suite for tests.
func (s *DriverSuite) SetupSuite() {
	s.setupSuiteGeneric(s.T())
}

// SetupSuiteWithB sets up the test suite for benchmarks.
func (s *DriverSuite) SetupSuiteWithB(b *testing.B) {
	s.setupSuiteGeneric(b)
}

func (s *DriverSuite) setupSuiteGeneric(t testing.TB) {
	driver, err := s.Constructor()
	require.NoError(t, err)
	s.StorageDriver = driver

	if s.ConstructorRootless != nil {
		driver, err = s.ConstructorRootless()
		require.NoError(t, err)
		s.StorageDriverRootless = driver
	}

	seed := testutil.SeedFromUnixNano(time.Now().UnixNano())
	t.Logf("using rng seed %v for blobbers", seed)
	s.blobberFactory = testutil.NewBlobberFactory(rngCacheSize, seed)
}

// TearDownSuite tears down the test suite when testing.
func (s *DriverSuite) TearDownSuite() {
	s.tearDownSuiteGeneric(s.T())
}

// TearDownSuiteWithB tears down the test suite when benchmarking.
func (s *DriverSuite) TearDownSuiteWithB(b *testing.B) {
	s.tearDownSuiteGeneric(b)
}

func (s *DriverSuite) tearDownSuiteGeneric(t require.TestingT) {
	if s.Teardown == nil {
		return
	}

	err := s.Teardown()
	require.NoError(t, err)
}

// TearDownTest tears down the test.
// This causes the suite to abort if any files are left around in the storage
// driver.
func (s *DriverSuite) TearDownTest() {
	files, err := s.StorageDriver.List(s.ctx, "/")
	assert.NoError(s.T(), err)
	assert.Empty(s.T(), files, "Storage driver did not clean up properly")
}

type syncDigestSet struct {
	sync.Mutex
	members map[digest.Digest]struct{}
}

func newSyncDigestSet() syncDigestSet {
	return syncDigestSet{sync.Mutex{}, make(map[digest.Digest]struct{})}
}

// idempotently adds a digest to the set.
func (s *syncDigestSet) add(d digest.Digest) {
	s.Lock()
	defer s.Unlock()

	s.members[d] = struct{}{}
}

// contains reports the digest's membership within the set.
func (s *syncDigestSet) contains(d digest.Digest) bool {
	s.Lock()
	defer s.Unlock()

	_, ok := s.members[d]

	return ok
}

// len returns the number of members within the set.
func (s *syncDigestSet) len() int {
	s.Lock()
	defer s.Unlock()

	return len(s.members)
}

type BenchmarkFunc struct {
	Name string
	Func func(b *testing.B)
}

// EnumerateBenchmarks finds all the benchmark functions for the given object.
// testify suite does not have built-in benchmarking capabilities, so we need
// to write this ourselves. This function is not recursive so it does not work
// for embedded objects!
// NOTE(prozlch): I wrote this function as I did not want to hardcode the list
// of benchmarks defined in the suite. This would require careful
// maintenance/updating compared to simply automating it.
func (s *DriverSuite) EnumerateBenchmarks() []BenchmarkFunc {
	benchmarks := make([]BenchmarkFunc, 0)

	st := reflect.TypeOf(s)
	sv := reflect.ValueOf(s)

	for i := 0; i < st.NumMethod(); i++ {
		method := st.Method(i)

		if !strings.HasPrefix(method.Name, "Benchmark") {
			continue
		}
		benchMethod := sv.Method(i)
		benchFunc := func(b *testing.B) {
			benchMethod.Call([]reflect.Value{reflect.ValueOf(b)})
		}
		benchmarks = append(benchmarks, BenchmarkFunc{
			Name: strings.TrimPrefix(method.Name, "Benchmark"),
			Func: benchFunc,
		})
	}

	return benchmarks
}

// TestRootExists ensures that all storage drivers have a root path by default.
func (s *DriverSuite) TestRootExists() {
	_, err := s.StorageDriver.List(s.ctx, "/")
	require.NoError(s.T(), err, `the root path "/" should always exist`)
}

// TestValidPaths checks that various valid file paths are accepted by the
// storage driver.
func (s *DriverSuite) TestValidPaths() {
	contents := s.blobberFactory.GetBlobber(64).GetAllBytes()
	validFiles := []string{
		"/a",
		"/2",
		"/aa",
		"/a.a",
		"/0-9/abcdefg",
		"/abcdefg/z.75",
		"/abc/1.2.3.4.5-6_zyx/123.z/4",
		"/docker/docker-registry",
		"/123.abc",
		"/abc./abc",
		"/.abc",
		"/a--b",
		"/a-.b",
		"/_.abc",
		"/Docker/docker-registry",
		"/Abc/Cba",
	}

	for _, filename := range validFiles {
		err := s.StorageDriver.PutContent(s.ctx, filename, contents)
		// nolint: revive // defer
		defer s.deletePath(s.StorageDriver, firstPart(filename))
		require.NoError(s.T(), err)

		received, err := s.StorageDriver.GetContent(s.ctx, filename)
		require.NoError(s.T(), err)
		assert.Equal(s.T(), contents, received)
	}
}

func (s *DriverSuite) deletePath(driver storagedriver.StorageDriver, targetPath string) {
	dtestutil.EnsurePathDeleted(s.ctx, s.T(), driver, targetPath)
}

// TestInvalidPaths checks that various invalid file paths are rejected by the
// storage driver.
func (s *DriverSuite) TestInvalidPaths() {
	contents := s.blobberFactory.GetBlobber(64).GetAllBytes()
	invalidFiles := []string{
		"",
		"/",
		"abc",
		"123.abc",
		"//bcd",
		"/abc_123/",
	}

	for _, filename := range invalidFiles {
		err := s.StorageDriver.PutContent(s.ctx, filename, contents)
		// only delete if file was successfully written
		if err == nil {
			// nolint: revive // defer
			defer s.deletePath(s.StorageDriver, firstPart(filename))
		}
		require.Error(s.T(), err)
		require.ErrorIs(s.T(), err, storagedriver.InvalidPathError{
			DriverName: s.StorageDriver.Name(),
			Path:       filename,
		})

		_, err = s.StorageDriver.GetContent(s.ctx, filename)
		require.Error(s.T(), err)
		require.ErrorIs(s.T(), err, storagedriver.InvalidPathError{
			DriverName: s.StorageDriver.Name(),
			Path:       filename,
		})
	}
}

// TestWriteRead1 tests a simple write-read workflow.
func (s *DriverSuite) TestWriteRead1() {
	filename := dtestutil.RandomPath(1, 32)
	s.T().Logf("blob path used for testing: %s", filename)

	contents := []byte("a")
	s.writeReadCompare(s.T(), filename, contents)
}

// TestWriteRead2 tests a simple write-read workflow with unicode data.
func (s *DriverSuite) TestWriteRead2() {
	filename := dtestutil.RandomPath(1, 32)
	s.T().Logf("blob path used for testing: %s", filename)

	contents := []byte("\xc3\x9f")
	s.writeReadCompare(s.T(), filename, contents)
}

// TestWriteRead3 tests a simple write-read workflow with a small string.
func (s *DriverSuite) TestWriteRead3() {
	filename := dtestutil.RandomPath(1, 32)
	s.T().Logf("blob path used for testing: %s", filename)

	contents := s.blobberFactory.GetBlobber(32).GetAllBytes()
	s.writeReadCompare(s.T(), filename, contents)
}

// TestWriteRead4 tests a simple write-read workflow with 1MB of data.
func (s *DriverSuite) TestWriteRead4() {
	filename := dtestutil.RandomPath(1, 32)
	s.T().Logf("blob path used for testing: %s", filename)

	contents := s.blobberFactory.GetBlobber(1 << 20).GetAllBytes()
	s.writeReadCompare(s.T(), filename, contents)
}

// TestWriteReadNonUTF8 tests that non-utf8 data may be written to the storage
// driver safely.
func (s *DriverSuite) TestWriteReadNonUTF8() {
	filename := dtestutil.RandomPath(1, 32)
	s.T().Logf("blob path used for testing: %s", filename)

	contents := []byte{0x80, 0x80, 0x80, 0x80}
	s.writeReadCompare(s.T(), filename, contents)
}

// TestTruncate tests that putting smaller contents than an original file does
// remove the excess contents.
func (s *DriverSuite) TestTruncate() {
	filename := dtestutil.RandomPath(1, 32)
	s.T().Logf("blob path used for testing: %s", filename)

	// NOTE(prozlach): We explicitly need a different blob to confirm that
	// in-place overwrite and truncation was indeed successful. In order to
	// avoid allocations, we simply request a bigger blobber and then slice it.
	contents := s.blobberFactory.GetBlobber((1024 + 1) * 1024).GetAllBytes()
	s.writeReadCompare(s.T(), filename, contents[:1024*1024])

	s.writeReadCompare(s.T(), filename, contents[1024*1024:])
}

// TestReadNonexistent tests reading content from an empty path.
func (s *DriverSuite) TestReadNonexistent() {
	filename := dtestutil.RandomPath(1, 32)
	s.T().Logf("blob path used for testing: %s", filename)

	_, err := s.StorageDriver.GetContent(s.ctx, filename)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       filename,
	})
}

// TestWriteReadStreams1 tests a simple write-read streaming workflow.
func (s *DriverSuite) TestWriteReadStreams1() {
	filename := dtestutil.RandomPath(1, 32)
	s.T().Logf("blob path used for testing: %s", filename)

	blobber := testutil.NewBlober([]byte("a"))
	s.writeReadCompareStreams(s.T(), filename, blobber)
}

// TestWriteReadStreams2 tests a simple write-read streaming workflow with
// unicode data.
func (s *DriverSuite) TestWriteReadStreams2() {
	filename := dtestutil.RandomPath(1, 32)
	s.T().Logf("blob path used for testing: %s", filename)

	blobber := testutil.NewBlober([]byte("\xc3\x9f"))
	s.writeReadCompareStreams(s.T(), filename, blobber)
}

// TestWriteReadStreams3 tests a simple write-read streaming workflow with a
// small amount of data.
func (s *DriverSuite) TestWriteReadStreams3() {
	filename := dtestutil.RandomPath(1, 32)
	s.T().Logf("blob path used for testing: %s", filename)

	blobber := s.blobberFactory.GetBlobber(32)
	s.writeReadCompareStreams(s.T(), filename, blobber)
}

// TestWriteReadStreams4 tests a simple write-read streaming workflow with 1MB
// of data.
func (s *DriverSuite) TestWriteReadStreams4() {
	filename := dtestutil.RandomPath(1, 32)
	s.T().Logf("blob path used for testing: %s", filename)

	blobber := s.blobberFactory.GetBlobber(1 << 20)
	s.writeReadCompareStreams(s.T(), filename, blobber)
}

// TestWriteReadStreamsNonUTF8 tests that non-utf8 data may be written to the
// storage driver safely.
func (s *DriverSuite) TestWriteReadStreamsNonUTF8() {
	filename := dtestutil.RandomPath(1, 32)
	s.T().Logf("blob path used for testing: %s", filename)

	blobber := testutil.NewBlober([]byte{0x80, 0x80, 0x80, 0x80})
	s.writeReadCompareStreams(s.T(), filename, blobber)
}

// TestWriteReadLargeStreams tests that a 2GB file may be written to the storage
// driver safely.
func (s *DriverSuite) TestWriteReadLargeStreams() {
	filename := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(filename))
	s.T().Logf("blob path used for testing: %s", filename)

	var fileSize int64 = 2 * 1 << 30
	if testing.Short() {
		fileSize = 256 * 1 << 20
		s.T().Log("Reducing file size to 256MiB for short mode")
	}

	blobber := s.blobberFactory.GetBlobber(fileSize)

	writer, err := s.StorageDriver.Writer(s.ctx, filename, false)
	require.NoError(s.T(), err)

	// NOTE(prozlach): VERY IMPORTANT - DO NOT WRAP contents into a another
	// Reader that does not support WriterTo interface or limit the size of the
	// copy buffer. Doing so could cause us to miss critical test cases where a large enough buffer
	// is sent to the storage, triggering the driver's Write() method and causing chunking.
	// If chunking is not properly handled, this may result in errors such as the following on Azure:
	//
	// RESPONSE 413: 413 The request body is too large and exceeds the maximum permissible limit.
	//
	written, err := io.CopyBuffer(writer, blobber.GetReader(), make([]byte, 256*1<<20))
	require.NoError(s.T(), err)
	require.EqualValues(s.T(), fileSize, written)
	// BUG(prozlach): See https://gitlab.com/gitlab-org/container-registry/-/issues/1500
	// This is just a workaround for now to enforce correct behavior on other
	// drivers. The TLDR is that gcs Writer object does not report correct size
	// until the Commit() is called. This is not the case for other drivers.
	if s.StorageDriver.Name() != "gcs" {
		require.EqualValues(s.T(), fileSize, writer.Size())
	}

	err = writer.Commit()
	require.NoError(s.T(), err)
	err = writer.Close()
	require.NoError(s.T(), err)

	reader, err := s.StorageDriver.Reader(s.ctx, filename, 0)
	require.NoError(s.T(), err)
	defer reader.Close()

	blobber.RequireStreamEqual(s.T(), reader, 0, "main stream")
}

// TestWriteReadSmallStream tests that a small file may be written to the storage
// driver safely using Writer() interface.
func (s *DriverSuite) TestWriteReadSmallStream() {
	filename := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(filename))
	s.T().Logf("blob path used for testing: %s", filename)

	var fileSize int64 = 2 * 1 << 10
	blobber := s.blobberFactory.GetBlobber(fileSize)

	writer, err := s.StorageDriver.Writer(s.ctx, filename, false)
	require.NoError(s.T(), err)

	written, err := io.CopyBuffer(writer, blobber.GetReader(), make([]byte, 2*1<<20))
	require.NoError(s.T(), err)
	require.EqualValues(s.T(), fileSize, written)
	// BUG(prozlach): See https://gitlab.com/gitlab-org/container-registry/-/issues/1500
	// This is just a workaround for now to enforce correct behavior on other
	// drivers. The TLDR is that gcs Writer object does not report correct size
	// until the Commit() is called. This is not the case for other drivers.
	if s.StorageDriver.Name() != "gcs" {
		require.EqualValues(s.T(), fileSize, writer.Size())
	}

	err = writer.Commit()
	require.NoError(s.T(), err)
	err = writer.Close()
	require.NoError(s.T(), err)

	reader, err := s.StorageDriver.Reader(s.ctx, filename, 0)
	require.NoError(s.T(), err)
	defer reader.Close()

	blobber.RequireStreamEqual(s.T(), reader, 0, "main stream")
}

// TestConcurentWriteCausesError tests that a concurent write to the same file
// will cause an error instead of data corruption.
func (s *DriverSuite) TestConcurentWriteCausesError() {
	if s.StorageDriver.Name() != azure_v2.DriverName {
		s.T().Skip("only Azure v2 driver supports that strong consistency guarantees")
	}

	filename := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(filename))
	s.T().Logf("blob path used for testing: %s", filename)

	// Must be greater than chunk size so that there is more than one request
	// to the backend while writing:
	var fileSize int64 = 32 * 1 << 20
	contentsAB := s.blobberFactory.GetBlobber(fileSize).GetAllBytes()
	blobberA1 := testutil.NewBlober(contentsAB[:fileSize>>2])
	blobberA2 := testutil.NewBlober(contentsAB[fileSize>>2 : fileSize>>1])
	blobberB := testutil.NewBlober(contentsAB[:fileSize>>1])

	writerA, err := s.StorageDriver.Writer(s.ctx, filename, false)
	require.NoError(s.T(), err)

	written, err := io.Copy(writerA, blobberA1.GetReader())
	require.NoError(s.T(), err)
	require.EqualValues(s.T(), blobberA1.Size(), written)
	// BUG(prozlach): See https://gitlab.com/gitlab-org/container-registry/-/issues/1500
	// This is just a workaround for now to enforce correct behavior on other
	// drivers. The TLDR is that gcs Writer object does not report correct size
	// until the Commit() is called. This is not the case for other drivers.
	if s.StorageDriver.Name() != "gcs" {
		require.EqualValues(s.T(), blobberA1.Size(), writerA.Size())
	}

	writerB, err := s.StorageDriver.Writer(s.ctx, filename, false)
	require.NoError(s.T(), err)

	written, err = io.Copy(writerB, blobberB.GetReader())
	require.NoError(s.T(), err)
	require.EqualValues(s.T(), blobberB.Size(), written)
	// See the comment above
	if s.StorageDriver.Name() != "gcs" {
		require.EqualValues(s.T(), blobberB.Size(), writerB.Size())
	}

	_, err = io.Copy(writerA, blobberA2.GetReader())
	require.Error(s.T(), err)

	err = writerB.Commit()
	require.NoError(s.T(), err)
	err = writerB.Close()
	require.NoError(s.T(), err)

	reader, err := s.StorageDriver.Reader(s.ctx, filename, 0)
	require.NoError(s.T(), err)
	defer reader.Close()

	blobberB.RequireStreamEqual(s.T(), reader, 0, "main stream")
}

// TestReaderWithOffset tests that the appropriate data is streamed when
// reading with a given offset.
func (s *DriverSuite) TestReaderWithOffset() {
	filename := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(filename))
	s.T().Logf("blob path used for testing: %s", filename)

	var chunkSize int64 = 32
	blobber := s.blobberFactory.GetBlobber(3 * chunkSize)
	contentsChunk123 := blobber.GetAllBytes()
	contentsChunk1 := contentsChunk123[0:chunkSize]
	contentsChunk2 := contentsChunk123[chunkSize : 2*chunkSize]
	contentsChunk3 := contentsChunk123[2*chunkSize : 3*chunkSize]

	err := s.StorageDriver.PutContent(s.ctx, filename, append(append(contentsChunk1, contentsChunk2...), contentsChunk3...))
	require.NoError(s.T(), err)

	reader, err := s.StorageDriver.Reader(s.ctx, filename, 0)
	require.NoError(s.T(), err)
	defer reader.Close()

	blobber.RequireStreamEqual(s.T(), reader, 0, "offset 0")

	reader, err = s.StorageDriver.Reader(s.ctx, filename, chunkSize)
	require.NoError(s.T(), err)
	defer reader.Close()

	blobber.RequireStreamEqual(s.T(), reader, chunkSize, "offset equal to chunkSize")

	reader, err = s.StorageDriver.Reader(s.ctx, filename, chunkSize*2)
	require.NoError(s.T(), err)
	defer reader.Close()

	blobber.RequireStreamEqual(s.T(), reader, 2*chunkSize, "offset equal to 2*chunkSize")

	// Ensure we get invalid offset for negative offsets.
	reader, err = s.StorageDriver.Reader(s.ctx, filename, -1)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.InvalidOffsetError{
		DriverName: s.StorageDriver.Name(),
		Path:       filename,
		Offset:     -1,
	})
	require.Nil(s.T(), reader)

	// Read past the end of the content and make sure we get a reader that
	// returns 0 bytes and io.EOF
	reader, err = s.StorageDriver.Reader(s.ctx, filename, chunkSize*3)
	require.NoError(s.T(), err)
	defer reader.Close()

	buf := make([]byte, chunkSize)
	n, err := reader.Read(buf)
	require.ErrorIs(s.T(), err, io.EOF)
	require.Zero(s.T(), n)

	// Check the N-1 boundary condition, ensuring we get 1 byte then io.EOF.
	reader, err = s.StorageDriver.Reader(s.ctx, filename, chunkSize*3-1)
	require.NoError(s.T(), err)
	defer reader.Close()

	n, err = reader.Read(buf)
	require.Equal(s.T(), 1, n)

	// We don't care whether the io.EOF comes on the this read or the first
	// zero read, but the only error acceptable here is io.EOF.
	if err != nil {
		require.ErrorIs(s.T(), err, io.EOF)
	}

	// Any more reads should result in zero bytes and io.EOF
	n, err = reader.Read(buf)
	assert.Zero(s.T(), n)
	assert.ErrorIs(s.T(), err, io.EOF)
}

// TestContinueStreamAppendLarge tests that a stream write can be appended to without
// corrupting the data with a large chunk size.
func (s *DriverSuite) TestContinueStreamAppendLarge() {
	s.testContinueStreamAppend(s.T(), 10*1024*1024)
}

// TestContinueStreamAppendSmall is the same as TestContinueStreamAppendLarge, but only
// with a tiny chunk size in order to test corner cases for some cloud storage drivers.
func (s *DriverSuite) TestContinueStreamAppendSmall() {
	s.testContinueStreamAppend(s.T(), 32)
}

func (s *DriverSuite) testContinueStreamAppend(t *testing.T, chunkSize int64) {
	filename := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(filename))
	t.Logf("blob path used for testing: %s", filename)

	blobber := s.blobberFactory.GetBlobber(3 * chunkSize)
	contentsChunk123 := blobber.GetAllBytes()
	contentsChunk1 := contentsChunk123[0:chunkSize]
	contentsChunk2 := contentsChunk123[chunkSize : 2*chunkSize]
	contentsChunk3 := contentsChunk123[2*chunkSize : 3*chunkSize]

	writer, err := s.StorageDriver.Writer(s.ctx, filename, false)
	require.NoError(t, err)
	nn, err := io.Copy(writer, bytes.NewReader(contentsChunk1))
	require.NoError(t, err)
	require.EqualValues(t, chunkSize, nn)

	require.NoError(t, writer.Close())
	require.EqualValues(t, chunkSize, writer.Size())

	writer, err = s.StorageDriver.Writer(s.ctx, filename, true)
	require.NoError(t, err)
	require.EqualValues(t, chunkSize, writer.Size())

	nn, err = io.Copy(writer, bytes.NewReader(contentsChunk2))
	require.NoError(t, err)
	require.EqualValues(t, chunkSize, nn)

	require.NoError(t, writer.Close())
	require.EqualValues(t, 2*chunkSize, writer.Size())

	writer, err = s.StorageDriver.Writer(s.ctx, filename, true)
	require.NoError(t, err)
	require.EqualValues(t, 2*chunkSize, writer.Size())

	nn, err = io.Copy(writer, bytes.NewReader(contentsChunk3))
	require.NoError(t, err)
	require.EqualValues(t, chunkSize, nn)

	require.NoError(t, writer.Commit())
	require.NoError(t, writer.Close())

	received, err := s.StorageDriver.GetContent(s.ctx, filename)
	require.NoError(t, err)
	require.Equal(t, blobber.GetAllBytes(), received)
}

// TestReadNonexistentStream tests that reading a stream for a nonexistent path
// fails.
func (s *DriverSuite) TestReadNonexistentStream() {
	filename := dtestutil.RandomPath(1, 32)
	s.T().Logf("blob path used for testing: %s", filename)

	_, err := s.StorageDriver.Reader(s.ctx, filename, 0)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       filename,
	})

	_, err = s.StorageDriver.Reader(s.ctx, filename, 64)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       filename,
	})
}

// TestList1File tests the validity of List calls for 1 file.
func (s *DriverSuite) TestList1File() {
	s.testList(s.T(), 1)
}

// TestList1200Files tests the validity of List calls for 1200 files.
func (s *DriverSuite) TestList1200Files() {
	s.testList(s.T(), 1200)
}

// testList checks the returned list of keys after populating a directory tree.
func (s *DriverSuite) testList(t *testing.T, numFiles int) {
	rootDirectory := "/" + dtestutil.RandomFilenameRange(8, 8)
	defer s.deletePath(s.StorageDriver, rootDirectory)
	t.Logf("root directory path used for testing: %s", rootDirectory)

	doesnotexist := path.Join(rootDirectory, "nonexistent")
	_, err := s.StorageDriver.List(s.ctx, doesnotexist)
	require.ErrorIs(t, err, storagedriver.PathNotFoundError{
		Path:       doesnotexist,
		DriverName: s.StorageDriver.Name(),
	})

	parentDirectory := rootDirectory + "/" + dtestutil.RandomFilenameRange(8, 8)
	childFiles := s.buildFiles(t, parentDirectory, numFiles, 8)

	sort.Strings(childFiles)

	keys, err := s.StorageDriver.List(s.ctx, "/")
	require.NoError(t, err)
	require.Equal(t, []string{rootDirectory}, keys)

	keys, err = s.StorageDriver.List(s.ctx, rootDirectory)
	require.NoError(t, err)
	require.Equal(t, []string{parentDirectory}, keys)

	keys, err = s.StorageDriver.List(s.ctx, parentDirectory)
	require.NoError(t, err)

	sort.Strings(keys)
	require.Equal(t, childFiles, keys)

	// A few checks to add here (check out #819 for more discussion on this):
	// 1. Ensure that all paths are absolute.
	// 2. Ensure that listings only include direct children.
	// 3. Ensure that we only respond to directory listings that end with a slash (maybe?).
}

// TestListUnprefixed checks if listing root directory with no prefix
// configured works.
func (s *DriverSuite) TestListUnprefixed() {
	if s.StorageDriver.Name() == "filesystem" {
		s.T().Skip("filesystem driver does not support prefix-less operation")
	}
	// NOTE(prozlach): we are sharing the storage root with other tests, so the
	// idea is to create a very uniqe file name and simply look for it in the
	// results. This way there should be no collisions.
	destPath := "/" + dtestutil.RandomFilename(64)
	defer s.deletePath(s.StorageDriverRootless, destPath)
	s.T().Logf("destination blob path used for testing: %s", destPath)
	destContents := s.blobberFactory.GetBlobber(64).GetAllBytes()

	err := s.StorageDriverRootless.PutContent(s.ctx, destPath, destContents)
	require.NoError(s.T(), err)

	keys, err := s.StorageDriverRootless.List(s.ctx, "/")
	require.NoError(s.T(), err)
	require.Contains(s.T(), keys, destPath)
}

func (s *DriverSuite) TestCreateListDeleteTightLoop() {
	destPath := "/" + dtestutil.RandomFilename(64)
	destContents := s.blobberFactory.GetBlobber(64).GetAllBytes()
	s.T().Logf("blob path used for testing: %s", destPath)

	assertEventually := func(i int, invertCheck bool) {
		keys, err := s.StorageDriver.List(s.ctx, "/")
		require.NoErrorf(s.T(), err, "iteration %d", i)

		// NOTE(prozlach): the blob name not showing imediatelly after is
		// already a problem, but we want to also know after how much time it
		// shows up (if it does at all), hence only do the `assert` and not
		// `require`
		if !invertCheck && assert.Containsf(s.T(), keys, destPath, "List() call inconsistency detected, iteration %d - element should have been found", i) {
			return
		}
		if invertCheck && assert.NotContainsf(s.T(), keys, destPath, "List() call inconsistency detected, iteration %d - element should not have been found", i) {
			return
		}

		// NOTE(prozlach): we need to see if the file not showing up in the
		// list is a temporary or a permament situation.
		ticker := time.NewTicker(700 * time.Millisecond)
		defer ticker.Stop()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()

		for {
			select {
			case t := <-ticker.C:
				keys, err = s.StorageDriver.List(s.ctx, "/")
				require.NoErrorf(s.T(), err, "iteration %d, time %s", i, t.String())
				if !invertCheck && slices.Contains(keys, destPath) {
					s.T().Logf("OK: found - iteration %d, time %s", i, t.String())
					return
				}
				if invertCheck && !slices.Contains(keys, destPath) {
					s.T().Logf("OK: not found - iteration %d, time %s", i, t.String())
					return
				}
			case t := <-timer.C:
				notWord := "not"
				if invertCheck {
					notWord = "can be"
				}
				require.FailNow(s.T(), fmt.Sprintf("iteration %d, time %s - blob still %s found in output of List()", i, t.String(), notWord))
				return
			}
		}
	}
	assertEventuallyContains := func(i int) { assertEventually(i, false) }
	assertEventuallyDoesNotContain := func(i int) { assertEventually(i, true) }

	// NOTE(prozlach): Number of repetitions was chosen basing on how many
	// iterations on average are needed to reproduce issues with List call when
	// running things locally.
	for i := 0; i < 40; i++ {
		err := s.StorageDriver.PutContent(s.ctx, destPath, destContents)
		require.NoErrorf(s.T(), err, "iteration %d", i)

		assertEventuallyContains(i)

		err = s.StorageDriver.Delete(s.ctx, destPath)
		require.NoErrorf(s.T(), err, "iteration %d", i)

		assertEventuallyDoesNotContain(i)
	}
}

// TestMovePutContentBlob checks that driver can indeed move an object, and
// that the object no longer exists at the source path and does exist at the
// destination after the move.
// NOTE(prozlach): The reason why we differentiate between blobs created by
// PutContent() and Write() methods is to make sure that there are no
// differences in handling blobs created by PutContent() and Writer() calls
// during move operation.
func (s *DriverSuite) TestMovePutContentBlob() {
	contents := s.blobberFactory.GetBlobber(32).GetAllBytes()

	sourcePath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(sourcePath))
	s.T().Logf("source blob path used for testing: %s", sourcePath)

	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	s.T().Logf("destination blob path used for testing: %s", destPath)

	err := s.StorageDriver.PutContent(s.ctx, sourcePath, contents)
	require.NoError(s.T(), err)

	err = s.StorageDriver.Move(s.ctx, sourcePath, destPath)
	require.NoError(s.T(), err)

	received, err := s.StorageDriver.GetContent(s.ctx, destPath)
	require.NoError(s.T(), err)
	require.Equal(s.T(), contents, received)

	_, err = s.StorageDriver.GetContent(s.ctx, sourcePath)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       sourcePath,
	})
}

// TestMoveWritterBlob checks that driver can indeed move an object, and
// that the object no longer exists at the source path and does exist at the
// destination after the move.
// NOTE(prozlach): The reason why we differentiate between blobs created by
// PutContent() and Write() methods is to make sure that there are no
// differences in handling blobs created by PutContent() and Writer() calls
// during move operation.
func (s *DriverSuite) TestMoveWritterBlob() {
	contents := s.blobberFactory.GetBlobber(32).GetAllBytes()

	sourcePath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(sourcePath))
	s.T().Logf("source blob path used for testing: %s", sourcePath)

	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	s.T().Logf("destination blob path used for testing: %s", destPath)

	writer, err := s.StorageDriver.Writer(s.ctx, sourcePath, false)
	require.NoError(s.T(), err, "unexpected error from driver.Writer")

	_, err = writer.Write(contents)
	require.NoError(s.T(), err, "writer.Write: unexpected error")

	// NOTE(prozlach): For some drivers, Close(), does not imply Commit()
	err = writer.Commit()
	require.NoError(s.T(), err, "writer.Commit: unexpected error")

	err = writer.Close()
	require.NoError(s.T(), err, "writer.Close: unexpected error")

	err = s.StorageDriver.Move(s.ctx, sourcePath, destPath)
	require.NoError(s.T(), err)

	received, err := s.StorageDriver.GetContent(s.ctx, destPath)
	require.NoError(s.T(), err)
	require.Equal(s.T(), contents, received)

	_, err = s.StorageDriver.GetContent(s.ctx, sourcePath)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       sourcePath,
	})
}

func (s *DriverSuite) TestAppendInexistentBlob() {
	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	s.T().Logf("destination blob path used for testing: %s", destPath)

	_, err := s.StorageDriver.Writer(s.ctx, destPath, true)
	require.ErrorAs(s.T(), err, new(storagedriver.PathNotFoundError))
}

func (s *DriverSuite) TestWriterDoubleClose() {
	destPath := dtestutil.RandomPath(1, 32)
	s.T().Logf("destination path for blob: %s", destPath)

	defer s.deletePath(s.StorageDriver, firstPart(destPath))

	writer, err := s.StorageDriver.Writer(s.ctx, destPath, false)
	require.NoError(s.T(), err)

	err = writer.Close()
	require.NoError(s.T(), err)

	err = writer.Close()
	require.ErrorIs(s.T(), err, storagedriver.ErrAlreadyClosed)
}

func (s *DriverSuite) TestWriterDoubleCommit() {
	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	s.T().Logf("destination path for blob: %s", destPath)
	contents := s.blobberFactory.GetBlobber(96).GetAllBytes()

	writer, err := s.StorageDriver.Writer(s.ctx, destPath, false)
	require.NoError(s.T(), err)

	_, err = writer.Write(contents)
	require.NoError(s.T(), err)

	err = writer.Commit()
	require.NoError(s.T(), err)

	err = writer.Commit()
	require.ErrorIs(s.T(), err, storagedriver.ErrAlreadyCommited)
}

func (s *DriverSuite) TestWriterWriteAfterClose() {
	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	s.T().Logf("destination path for blob: %s", destPath)
	contents := s.blobberFactory.GetBlobber(96).GetAllBytes()

	writer, err := s.StorageDriver.Writer(s.ctx, destPath, false)
	require.NoError(s.T(), err)

	_, err = writer.Write(contents)
	require.NoError(s.T(), err)

	err = writer.Close()
	require.NoError(s.T(), err)

	_, err = writer.Write(contents)
	require.ErrorIs(s.T(), err, storagedriver.ErrAlreadyClosed)
}

func (s *DriverSuite) TestWriterCancelAfterClose() {
	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	s.T().Logf("destination path for blob: %s", destPath)

	writer, err := s.StorageDriver.Writer(s.ctx, destPath, false)
	require.NoError(s.T(), err)

	err = writer.Close()
	require.NoError(s.T(), err)

	err = writer.Cancel()
	require.ErrorIs(s.T(), err, storagedriver.ErrAlreadyClosed)
}

func (s *DriverSuite) TestWriterCommitAfterClose() {
	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	s.T().Logf("destination path for blob: %s", destPath)

	writer, err := s.StorageDriver.Writer(s.ctx, destPath, false)
	require.NoError(s.T(), err)

	err = writer.Close()
	require.NoError(s.T(), err)

	err = writer.Commit()
	require.ErrorIs(s.T(), err, storagedriver.ErrAlreadyClosed)
}

func (s *DriverSuite) TestWriterCommitAfterCancel() {
	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	contents := s.blobberFactory.GetBlobber(96).GetAllBytes()
	s.T().Logf("destination path for blob: %s", destPath)

	writer, err := s.StorageDriver.Writer(s.ctx, destPath, false)
	require.NoError(s.T(), err)

	_, err = writer.Write(contents)
	require.NoError(s.T(), err)

	err = writer.Commit()
	require.NoError(s.T(), err)

	err = writer.Cancel()
	require.ErrorIs(s.T(), err, storagedriver.ErrAlreadyCommited)
}

func (s *DriverSuite) TestWriterCancelAfterCommit() {
	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	s.T().Logf("destination path for blob: %s", destPath)

	writer, err := s.StorageDriver.Writer(s.ctx, destPath, false)
	require.NoError(s.T(), err)

	err = writer.Cancel()
	require.NoError(s.T(), err)

	err = writer.Commit()
	require.ErrorIs(s.T(), err, storagedriver.ErrAlreadyCanceled)
}

func (s *DriverSuite) TestWriterWriteAfterCommit() {
	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	contents := s.blobberFactory.GetBlobber(96).GetAllBytes()
	s.T().Logf("destination path for blob: %s", destPath)

	writer, err := s.StorageDriver.Writer(s.ctx, destPath, false)
	require.NoError(s.T(), err)

	_, err = writer.Write(contents)
	require.NoError(s.T(), err)

	err = writer.Commit()
	require.NoError(s.T(), err)

	_, err = writer.Write(contents)
	require.ErrorIs(s.T(), err, storagedriver.ErrAlreadyCommited)
}

func (s *DriverSuite) TestWriterWriteAfterCancel() {
	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	contents := s.blobberFactory.GetBlobber(96).GetAllBytes()
	s.T().Logf("destination path for blob: %s", destPath)

	writer, err := s.StorageDriver.Writer(s.ctx, destPath, false)
	require.NoError(s.T(), err)

	_, err = writer.Write(contents)
	require.NoError(s.T(), err)

	err = writer.Cancel()
	require.NoError(s.T(), err)

	_, err = writer.Write(contents)
	require.ErrorIs(s.T(), err, storagedriver.ErrAlreadyCanceled)
}

func (s *DriverSuite) TestWriterCancel() {
	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	contents := s.blobberFactory.GetBlobber(96).GetAllBytes()
	s.T().Logf("destination path for blob: %s", destPath)

	writer, err := s.StorageDriver.Writer(s.ctx, destPath, false)
	require.NoError(s.T(), err)

	_, err = writer.Write(contents)
	require.NoError(s.T(), err)

	err = writer.Cancel()
	require.NoError(s.T(), err)

	err = writer.Close()
	require.NoError(s.T(), err)

	_, err = s.StorageDriver.Stat(s.ctx, destPath)
	require.ErrorAs(s.T(), err, new(storagedriver.PathNotFoundError))
}

// TestOverwriteAppendBlob checks that driver can overwrite blob created using
// Write() call with PutContent() call. In case of e.g. Azure, Write() creates
// AppendBlob and PutContent() creates a BlockBlob and there is no in-place
// conversion - blob needs to be deleted and re-created when overwritting it.
func (s *DriverSuite) TestOverwriteAppendBlob() {
	contents := s.blobberFactory.GetBlobber(64).GetAllBytes()
	// NOTE(prozlach): We explicitly need a different blob here to confirm that
	// in-place overwrite was indeed successful. In order to avoid extra
	// allocations, we simply request a blobber double the size.
	contentsAppend := contents[:32]
	contentsBlock := contents[32:]
	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	s.T().Logf("destination blob path used for testing: %s", destPath)

	writer, err := s.StorageDriver.Writer(s.ctx, destPath, false)
	require.NoError(s.T(), err, "unexpected error from driver.Writer")

	_, err = writer.Write(contentsAppend)
	require.NoError(s.T(), err, "writer.Write: unexpected error")

	// NOTE(prozlach): For some drivers, Close(), does not imply Commit()
	err = writer.Commit()
	require.NoError(s.T(), err, "writer.Commit: unexpected error")

	err = writer.Close()
	require.NoError(s.T(), err, "writer.Close: unexpected error")

	err = s.StorageDriver.PutContent(s.ctx, destPath, contentsBlock)
	require.NoError(s.T(), err)

	received, err := s.StorageDriver.GetContent(s.ctx, destPath)
	require.NoError(s.T(), err)
	require.Equal(s.T(), contentsBlock, received)
}

// TestOverwriteBlockBlob checks that driver can overwrite blob created using
// PutContent() call using Write() call. In case of e.g. Azure Write() creates
// Append blob and Write() creates a Block blob and there is no in-place
// conversion - blob needs to be deleted.
func (s *DriverSuite) TestOverwriteBlockBlob() {
	contents := s.blobberFactory.GetBlobber(64).GetAllBytes()
	// NOTE(prozlach): We explicitly need a different blob here to confirm that
	// in-place overwrite was indeed successful. In order to avoid extra
	// allocations, we simply request a blobber double the size.
	contentsAppend := contents[:32]
	contentsBlock := contents[32:]
	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	s.T().Logf("destination blob path used for testing: %s", destPath)

	err := s.StorageDriver.PutContent(s.ctx, destPath, contentsBlock)
	require.NoError(s.T(), err)

	writer, err := s.StorageDriver.Writer(s.ctx, destPath, false)
	require.NoError(s.T(), err, "unexpected error from driver.Writer")

	_, err = writer.Write(contentsAppend)
	require.NoError(s.T(), err, "writer.Write: unexpected error")

	// NOTE(prozlach): For some drivers, Close(), does not imply Commit()
	err = writer.Commit()
	require.NoError(s.T(), err, "writer.Commit: unexpected error")

	err = writer.Close()
	require.NoError(s.T(), err, "writer.Close: unexpected error")

	received, err := s.StorageDriver.GetContent(s.ctx, destPath)
	require.NoError(s.T(), err)
	require.Equal(s.T(), contentsAppend, received)
}

// TestMoveOverwrite checks that a moved object no longer exists at the source
// path and overwrites the contents at the destination.
func (s *DriverSuite) TestMoveOverwrite() {
	sourcePath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(sourcePath))
	s.T().Logf("source blob path used for testing: %s", sourcePath)

	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	s.T().Logf("destination blob path used for testing: %s", destPath)

	// NOTE(prozlach): We explicitly need a different blob here to confirm that
	// in-place overwrite was indeed successful. In order to avoid extra
	// allocations, we simply request a blobber double the size.
	contents := s.blobberFactory.GetBlobber(96).GetAllBytes()
	sourceContents := contents[:32]
	destContents := contents[32:]

	err := s.StorageDriver.PutContent(s.ctx, sourcePath, sourceContents)
	require.NoError(s.T(), err)

	err = s.StorageDriver.PutContent(s.ctx, destPath, destContents)
	require.NoError(s.T(), err)

	err = s.StorageDriver.Move(s.ctx, sourcePath, destPath)
	require.NoError(s.T(), err)

	received, err := s.StorageDriver.GetContent(s.ctx, destPath)
	require.NoError(s.T(), err)
	require.Equal(s.T(), sourceContents, received)

	_, err = s.StorageDriver.GetContent(s.ctx, sourcePath)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       sourcePath,
	})
}

// TestMoveNonexistent checks that moving a nonexistent key fails and does not
// delete the data at the destination path.
func (s *DriverSuite) TestMoveNonexistent() {
	contents := s.blobberFactory.GetBlobber(32).GetAllBytes()
	sourcePath := dtestutil.RandomPath(1, 32)
	s.T().Logf("source blob path used for testing: %s", sourcePath)

	destPath := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(destPath))
	s.T().Logf("destination blob path used for testing: %s", destPath)

	err := s.StorageDriver.PutContent(s.ctx, destPath, contents)
	require.NoError(s.T(), err)

	err = s.StorageDriver.Move(s.ctx, sourcePath, destPath)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       sourcePath,
	})

	received, err := s.StorageDriver.GetContent(s.ctx, destPath)
	require.NoError(s.T(), err)
	require.Equal(s.T(), contents, received)
}

// TestMoveInvalid provides various checks for invalid moves.
func (s *DriverSuite) TestMoveInvalid() {
	contents := s.blobberFactory.GetBlobber(32).GetAllBytes()

	// Create a regular file.
	err := s.StorageDriver.PutContent(s.ctx, "/notadir", contents)
	require.NoError(s.T(), err)
	defer s.deletePath(s.StorageDriver, "/notadir")

	// Now try to move a non-existent file under it.
	err = s.StorageDriver.Move(s.ctx, "/notadir/foo", "/notadir/bar")
	require.Error(s.T(), err)
}

// TestDelete checks that the delete operation removes data from the storage
// driver
func (s *DriverSuite) TestDelete() {
	filename := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(filename))
	s.T().Logf("blob path used for testing: %s", filename)
	contents := s.blobberFactory.GetBlobber(32).GetAllBytes()

	err := s.StorageDriver.PutContent(s.ctx, filename, contents)
	require.NoError(s.T(), err)

	err = s.StorageDriver.Delete(s.ctx, filename)
	require.NoError(s.T(), err)

	_, err = s.StorageDriver.GetContent(s.ctx, filename)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       filename,
	})
}

// TestDeleteDir1File ensures the driver is able to delete all objects in a
// directory with 1 file.
func (s *DriverSuite) TestDeleteDir1File() {
	s.testDeleteDir(s.T(), 1)
}

// TestDeleteDir1200Files ensures the driver is able to delete all objects in a
// directory with 1200 files.
func (s *DriverSuite) TestDeleteDir1200Files() {
	s.testDeleteDir(s.T(), 1200)
}

func (s *DriverSuite) testDeleteDir(t *testing.T, numFiles int) {
	rootDirectory := "/" + dtestutil.RandomFilenameRange(8, 8)
	defer s.deletePath(s.StorageDriver, rootDirectory)
	t.Logf("root directory path used for testing: %s", rootDirectory)

	parentDirectory := rootDirectory + "/" + dtestutil.RandomFilenameRange(8, 8)
	childFiles := s.buildFiles(t, parentDirectory, numFiles, 8)

	err := s.StorageDriver.Delete(s.ctx, parentDirectory)
	require.NoError(t, err)

	// Most storage backends delete files in lexicographic order, so we'll access
	// them in the same way, this should help point out errors due to deletion order.
	sort.Strings(childFiles)

	// This test can be flaky when large numbers of objects are deleted for
	// storage backends which are eventually consistent. We'll log any files we
	// encounter which are not delete and fail later. This way information about
	// the failure/flake can be preserved to aid in debugging.
	var filesRemaining bool

	for i, f := range childFiles {
		if _, err = s.StorageDriver.GetContent(s.ctx, f); err == nil {
			filesRemaining = true
			t.Logf("able to access file %d after deletion", i)
		} else {
			require.Error(t, err)
			require.ErrorIs(t, err, storagedriver.PathNotFoundError{
				DriverName: s.StorageDriver.Name(),
				Path:       f,
			})
		}
	}

	require.False(t, filesRemaining, "Encountered files remaining after deletion")
}

// buildFiles builds a num amount of test files with a size of size under
// parentDir. Returns a slice with the path of the created files.
func (s *DriverSuite) buildFiles(t require.TestingT, parentDirectory string, numFiles int, size int64) []string {
	// NOTE(prozlach): chosen empirically, basing on how many CI tasks can run
	// in pararell before we hit limits.
	const concurencyFactor = 32
	var wg sync.WaitGroup
	var failed atomic.Bool

	sem := make(chan struct{}, concurencyFactor)
	for i := 0; i < concurencyFactor; i++ {
		sem <- struct{}{}
	}

	wg.Add(numFiles)

	childFiles := make([]string, numFiles)

	for i := range childFiles {
		go func(i int) {
			defer wg.Done()
			<-sem
			defer func() { sem <- struct{}{} }()

			childFile := parentDirectory + "/" + dtestutil.RandomFilenameRange(16, 32)
			childFiles[i] = childFile

			err := s.StorageDriver.PutContent(s.ctx, childFile, s.blobberFactory.GetBlobber(size).GetAllBytes())
			if !assert.NoError(t, err) {
				failed.Store(true)
			}
		}(i)
	}
	wg.Wait()
	require.False(t, failed.Load(), "One or more goroutines failed")

	return childFiles
}

// assertPathNotFound asserts that path does not exist in the storage driver filesystem.
func (s *DriverSuite) assertPathNotFound(t require.TestingT, p ...string) {
	for _, p := range p {
		_, err := s.StorageDriver.GetContent(s.ctx, p)
		require.Errorf(t, err, "path %q expected to be not found but it is still there", p)
		require.ErrorIs(t, err, storagedriver.PathNotFoundError{
			DriverName: s.StorageDriver.Name(),
			Path:       p,
		})
	}
}

// TestDeleteFiles checks that DeleteFiles removes data from the storage driver for a random (<10) number of files.
func (s *DriverSuite) TestDeleteFiles() {
	parentDir := dtestutil.RandomPath(1, 8)
	defer s.deletePath(s.StorageDriver, firstPart(parentDir))
	s.T().Logf("parent directory path used for testing: %s", parentDir)

	/* #nosec G404 */
	blobPaths := s.buildFiles(s.T(), parentDir, mrand.IntN(10), 32)

	count, err := s.StorageDriver.DeleteFiles(s.ctx, blobPaths)
	require.NoError(s.T(), err)
	require.Equal(s.T(), len(blobPaths), count)

	s.assertPathNotFound(s.T(), blobPaths...)
}

// TestDeleteFileEqualFolderFileName is a regression test for deleting files
// where the file name and folder where the file resides have the same names
func (s *DriverSuite) TestDeleteFileEqualFolderFileName() {
	parentDir := dtestutil.RandomPath(1, 8)
	fileName := "Maryna"
	p := path.Join(parentDir, fileName, fileName)
	defer s.deletePath(s.StorageDriver, firstPart(parentDir))
	s.T().Logf("parent directory path used for testing: %s", parentDir)

	err := s.StorageDriver.PutContent(s.ctx, p, s.blobberFactory.GetBlobber(32).GetAllBytes())
	require.NoError(s.T(), err)

	err = s.StorageDriver.Delete(s.ctx, p)
	require.NoError(s.T(), err)

	s.assertPathNotFound(s.T(), p)
}

// TestDeleteFilesNotFound checks that DeleteFiles is idempotent and doesn't return an error if a file was not found.
func (s *DriverSuite) TestDeleteFilesNotFound() {
	parentDir := dtestutil.RandomPath(1, 8)
	defer s.deletePath(s.StorageDriver, firstPart(parentDir))
	s.T().Logf("parent directory path used for testing: %s", parentDir)

	blobPaths := s.buildFiles(s.T(), parentDir, 5, 32)
	// delete the 1st, 3rd and last file so that they don't exist anymore
	s.deletePath(s.StorageDriver, blobPaths[0])
	s.deletePath(s.StorageDriver, blobPaths[2])
	s.deletePath(s.StorageDriver, blobPaths[4])

	count, err := s.StorageDriver.DeleteFiles(s.ctx, blobPaths)
	require.NoError(s.T(), err)
	require.Equal(s.T(), len(blobPaths), count)

	s.assertPathNotFound(s.T(), blobPaths...)
}

// benchmarkDeleteFiles benchmarks DeleteFiles for an amount of num files.
func (s *DriverSuite) benchmarkDeleteFiles(b *testing.B, num int) {
	parentDir := dtestutil.RandomPath(1, 8)
	defer s.deletePath(s.StorageDriver, firstPart(parentDir))

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		paths := s.buildFiles(b, parentDir, num, 32)
		b.StartTimer()
		count, err := s.StorageDriver.DeleteFiles(s.ctx, paths)
		b.StopTimer()
		require.NoError(b, err)
		require.Len(b, paths, count)
		s.assertPathNotFound(b, paths...)
	}
}

// BenchmarkDeleteFiles1File benchmarks DeleteFiles for 1 file.
func (s *DriverSuite) BenchmarkDeleteFiles1File(b *testing.B) {
	s.benchmarkDeleteFiles(b, 1)
}

// BenchmarkDeleteFiles100Files benchmarks DeleteFiles for 100 files.
func (s *DriverSuite) BenchmarkDeleteFiles100Files(b *testing.B) {
	s.benchmarkDeleteFiles(b, 100)
}

// TestURLFor checks that the URLFor method functions properly, but only if it
// is implemented
func (s *DriverSuite) TestURLFor() {
	filename := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(filename))
	contents := s.blobberFactory.GetBlobber(32).GetAllBytes()
	s.T().Logf("blob path used for testing: %s", filename)

	err := s.StorageDriver.PutContent(s.ctx, filename, contents)
	require.NoError(s.T(), err)

	url, err := s.StorageDriver.URLFor(s.ctx, filename, nil)
	if errors.As(err, new(storagedriver.ErrUnsupportedMethod)) {
		return
	}
	require.NoError(s.T(), err)

	response, err := http.Get(url)
	require.NoError(s.T(), err)
	defer response.Body.Close()

	read, err := io.ReadAll(response.Body)
	require.NoError(s.T(), err)
	require.Equal(s.T(), contents, read)

	url, err = s.StorageDriver.URLFor(s.ctx, filename, map[string]any{"method": http.MethodHead})
	if errors.As(err, new(storagedriver.ErrUnsupportedMethod)) {
		return
	}
	require.NoError(s.T(), err)

	response, err = http.Head(url)
	require.NoError(s.T(), err)
	err = response.Body.Close()
	require.NoError(s.T(), err)
	assert.Equal(s.T(), http.StatusOK, response.StatusCode)
	assert.Equal(s.T(), int64(32), response.ContentLength)
}

// TestDeleteNonexistent checks that removing a nonexistent key fails.
func (s *DriverSuite) TestDeleteNonexistent() {
	filename := dtestutil.RandomPath(1, 32)
	err := s.StorageDriver.Delete(s.ctx, filename)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       filename,
	})
}

// TestDeleteFolder checks that deleting a folder removes all child elements.
func (s *DriverSuite) TestDeleteFolder() {
	dirname := dtestutil.RandomPath(1, 32)
	filename1 := dtestutil.RandomPath(1, 32)
	filename2 := dtestutil.RandomPath(1, 32)
	filename3 := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(dirname))
	contents := s.blobberFactory.GetBlobber(32).GetAllBytes()
	s.T().Logf("parent directory: %s, filename1: %s, filename2: %s, filename3: %s", dirname, filename1, filename2, filename3)

	err := s.StorageDriver.PutContent(s.ctx, path.Join(dirname, filename1), contents)
	require.NoError(s.T(), err)

	err = s.StorageDriver.PutContent(s.ctx, path.Join(dirname, filename2), contents)
	require.NoError(s.T(), err)

	err = s.StorageDriver.PutContent(s.ctx, path.Join(dirname, filename3), contents)
	require.NoError(s.T(), err)

	err = s.StorageDriver.Delete(s.ctx, path.Join(dirname, filename1))
	require.NoError(s.T(), err)

	_, err = s.StorageDriver.GetContent(s.ctx, path.Join(dirname, filename1))
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       path.Join(dirname, filename1),
	})

	_, err = s.StorageDriver.GetContent(s.ctx, path.Join(dirname, filename2))
	require.NoError(s.T(), err)

	_, err = s.StorageDriver.GetContent(s.ctx, path.Join(dirname, filename3))
	require.NoError(s.T(), err)

	err = s.StorageDriver.Delete(s.ctx, dirname)
	require.NoError(s.T(), err)

	_, err = s.StorageDriver.GetContent(s.ctx, path.Join(dirname, filename1))
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       path.Join(dirname, filename1),
	})

	_, err = s.StorageDriver.GetContent(s.ctx, path.Join(dirname, filename2))
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       path.Join(dirname, filename2),
	})

	_, err = s.StorageDriver.GetContent(s.ctx, path.Join(dirname, filename3))
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       path.Join(dirname, filename3),
	})
}

// TestDeleteOnlyDeletesSubpaths checks that deleting path A does not
// delete path B when A is a prefix of B but B is not a subpath of A (so that
// deleting "/a" does not delete "/ab").  This matters for services like S3 that
// do not implement directories.
func (s *DriverSuite) TestDeleteOnlyDeletesSubpaths() {
	dirname := dtestutil.RandomPath(1, 32)
	filename := dtestutil.RandomPath(1, 32)
	contents := s.blobberFactory.GetBlobber(32).GetAllBytes()
	defer s.deletePath(s.StorageDriver, firstPart(dirname))
	s.T().Logf("directory: %s, filename: %s", dirname, filename)

	err := s.StorageDriver.PutContent(s.ctx, path.Join(dirname, filename), contents)
	require.NoError(s.T(), err)

	err = s.StorageDriver.PutContent(s.ctx, path.Join(dirname, filename+"suffix"), contents)
	require.NoError(s.T(), err)

	err = s.StorageDriver.PutContent(s.ctx, path.Join(dirname, dirname, filename), contents)
	require.NoError(s.T(), err)

	err = s.StorageDriver.PutContent(s.ctx, path.Join(dirname, dirname+"suffix", filename), contents)
	require.NoError(s.T(), err)

	err = s.StorageDriver.Delete(s.ctx, path.Join(dirname, filename))
	require.NoError(s.T(), err)

	_, err = s.StorageDriver.GetContent(s.ctx, path.Join(dirname, filename))
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       path.Join(dirname, filename),
	})

	_, err = s.StorageDriver.GetContent(s.ctx, path.Join(dirname, filename+"suffix"))
	require.NoError(s.T(), err)

	err = s.StorageDriver.Delete(s.ctx, path.Join(dirname, dirname))
	require.NoError(s.T(), err)

	_, err = s.StorageDriver.GetContent(s.ctx, path.Join(dirname, dirname, filename))
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       path.Join(dirname, dirname, filename),
	})

	_, err = s.StorageDriver.GetContent(s.ctx, path.Join(dirname, dirname+"suffix", filename))
	require.NoError(s.T(), err)
}

// TestStatCall runs verifies the implementation of the storagedriver's Stat call.
func (s *DriverSuite) TestStatCall() {
	// NOTE(prozlach): We explicitly need a different blob here to confirm that
	// in-place overwrite was indeed successful.
	// NOTE(prozlach): The idea is to create a hierarchy in the s3 bucket
	// where:
	// * there is one common directory for all the blobs (dirPathBase)
	// * there are two files in two different "subdirectories" under the same
	// common directory (dirA, dirB and filePath, filePathAux)
	// * both subdirectories should share the same prefix so that we could test
	// a stat on inexistant dir which is actually a common prefix for existing
	// subdirectories (DirPartialPrefix)
	contentAB := s.blobberFactory.GetBlobber(4096 * 2).GetAllBytes()
	contentA := contentAB[:4096]
	contentB := contentAB[4096:]
	dirPathBase := dtestutil.RandomPath(1, 24)
	dirA := "foo" + dtestutil.RandomFilename(13)
	dirB := "foo" + dtestutil.RandomFilename(13)
	partialPath := path.Join(dirPathBase, "foo")
	dirPath := path.Join(dirPathBase, dirA)
	dirPathAux := path.Join(dirPathBase, dirB)
	fileName := dtestutil.RandomFilename(32)
	filePath := path.Join(dirPath, fileName)
	// Trigger a case where for given prefix there is more than one object
	filePathAux := path.Join(dirPathAux, fileName)
	s.T().Logf("directory: %s, filename: %s, filename aux: %s", dirPath, fileName, filePathAux)

	err := s.StorageDriver.PutContent(s.ctx, filePath, contentA)
	require.NoError(s.T(), err)

	err = s.StorageDriver.PutContent(s.ctx, filePathAux, contentA)
	require.NoError(s.T(), err)

	defer s.deletePath(s.StorageDriver, firstPart(dirPath))

	// Call to stat on root directory. The storage healthcheck performs this
	// exact call to Stat.
	// PathNotFoundErrors are not considered health check failures. Some
	// drivers will return a not found here, while others will not return an
	// error at all. If we get an error, ensure it's a not found.
	s.Run("RootDirectory", func() {
		fi, err := s.StorageDriver.Stat(s.ctx, "/")
		if err != nil {
			assert.ErrorAs(s.T(), err, new(storagedriver.PathNotFoundError))
		} else {
			assert.NotNil(s.T(), fi)
			assert.Equal(s.T(), "/", fi.Path())
			assert.True(s.T(), fi.IsDir())
		}
	})

	s.Run("NonExistentDir", func() {
		fi, err := s.StorageDriver.Stat(s.ctx, dirPath+"foo")
		require.Error(s.T(), err)
		assert.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{ // nolint: testifylint
			DriverName: s.StorageDriver.Name(),
			Path:       dirPath + "foo",
		})
		assert.Nil(s.T(), fi)
	})

	s.Run("NonExistentPath", func() {
		fi, err := s.StorageDriver.Stat(s.ctx, filePath+"bar")
		require.Error(s.T(), err)
		assert.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{ // nolint: testifylint
			DriverName: s.StorageDriver.Name(),
			Path:       filePath + "bar",
		})
		assert.Nil(s.T(), fi)
	})

	s.Run("FileExists", func() {
		fi, err := s.StorageDriver.Stat(s.ctx, filePath)
		require.NoError(s.T(), err)
		require.NotNil(s.T(), fi)
		assert.Equal(s.T(), filePath, fi.Path())
		assert.Equal(s.T(), int64(len(contentA)), fi.Size())
		assert.False(s.T(), fi.IsDir())
	})

	s.Run("ModTime", func() {
		fi, err := s.StorageDriver.Stat(s.ctx, filePath)
		require.NoError(s.T(), err)
		assert.NotNil(s.T(), fi)
		createdTime := fi.ModTime()

		// Sleep and modify the file
		time.Sleep(time.Second * 10)
		err = s.StorageDriver.PutContent(s.ctx, filePath, contentB)
		require.NoError(s.T(), err)

		fi, err = s.StorageDriver.Stat(s.ctx, filePath)
		require.NoError(s.T(), err)
		require.NotNil(s.T(), fi)
		modTime := fi.ModTime()

		// Check if the modification time is after the creation time.
		// In case of cloud storage services, storage frontend nodes might have
		// time drift between them, however that should be solved with sleeping
		// before update.
		assert.Greaterf(
			s.T(),
			modTime,
			createdTime,
			"modtime (%s) is before the creation time (%s)", modTime, createdTime,
		)
	})

	// Call on directory with one "file"
	// (do not check ModTime as dirs don't need to support it)
	s.Run("DirWithFile", func() {
		fi, err := s.StorageDriver.Stat(s.ctx, dirPath)
		require.NoError(s.T(), err)
		require.NotNil(s.T(), fi)
		assert.Equal(s.T(), dirPath, fi.Path())
		assert.Zero(s.T(), fi.Size())
		assert.True(s.T(), fi.IsDir())
	})

	// Call on directory with another "subdirectory"
	s.Run("DirWithSubDir", func() {
		fi, err := s.StorageDriver.Stat(s.ctx, dirPathBase)
		require.NoError(s.T(), err)
		require.NotNil(s.T(), fi)
		assert.Equal(s.T(), dirPathBase, fi.Path())
		assert.Zero(s.T(), fi.Size())
		assert.True(s.T(), fi.IsDir())
	})

	// Call on a partial name of the directory. This should result in
	// not-found, as partial match is still not a match for a directory.
	s.Run("DirPartialPrefix", func() {
		fi, err := s.StorageDriver.Stat(s.ctx, partialPath)
		require.Error(s.T(), err)
		assert.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{ // nolint: testifylint
			DriverName: s.StorageDriver.Name(),
			Path:       partialPath,
		})
		assert.Nil(s.T(), fi)
	})
}

// TestPutContentMultipleTimes checks that if storage driver can overwrite the content
// in the subsequent puts. Validates that PutContent does not have to work
// with an offset like Writer does and overwrites the file entirely
// rather than writing the data to the [0,len(data)) of the file.
func (s *DriverSuite) TestPutContentMultipleTimes() {
	filename := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(filename))
	s.T().Logf("blob path used for testing: %s", filename)
	// NOTE(prozlach): We explicitly need a different blob here to confirm that
	// in-place overwrite was indeed successful.
	contentsAB := s.blobberFactory.GetBlobber(4096 + 2048).GetAllBytes()
	contentsA := contentsAB[:4096]
	contentsB := contentsAB[4096:]

	err := s.StorageDriver.PutContent(s.ctx, filename, contentsA)
	require.NoError(s.T(), err)

	err = s.StorageDriver.PutContent(s.ctx, filename, contentsB)
	require.NoError(s.T(), err)

	readContents, err := s.StorageDriver.GetContent(s.ctx, filename)
	require.NoError(s.T(), err)
	require.Equal(s.T(), contentsB, readContents)
}

// TestConcurrentStreamReads checks that multiple clients can safely read from
// the same file simultaneously with various offsets.
func (s *DriverSuite) TestConcurrentStreamReads() {
	var fileSize int64 = 128 * 1 << 20

	if testing.Short() {
		fileSize = 10 * 1 << 20
		s.T().Logf("Reducing file size to 10MB for short mode")
	}

	filename := dtestutil.RandomPath(1, 32)
	defer s.deletePath(s.StorageDriver, firstPart(filename))
	s.T().Logf("blob path used for testing: %s", filename)

	blobber := s.blobberFactory.GetBlobber(fileSize)

	err := s.StorageDriver.PutContent(s.ctx, filename, blobber.GetAllBytes())
	require.NoError(s.T(), err)

	var wg sync.WaitGroup

	readContents := func() {
		defer wg.Done()
		/* #nosec G404 */
		offset := mrand.Int64N(fileSize)
		reader, err := s.StorageDriver.Reader(s.ctx, filename, offset)
		// nolint: testifylint // require-error
		if !assert.NoError(s.T(), err) {
			return
		}
		defer reader.Close()

		blobber.AssertStreamEqual(s.T(), reader, offset, fmt.Sprintf("filename: %s, offset: %d", filename, offset))
	}

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go readContents()
	}
	wg.Wait()
}

// TestConcurrentFileStreams checks that multiple *os.File objects can be passed
// in to Writer concurrently without hanging.
func (s *DriverSuite) TestConcurrentFileStreams() {
	numStreams := 32

	if testing.Short() {
		numStreams = 8
		s.T().Log("Reducing number of streams to 8 for short mode")
	}

	var wg sync.WaitGroup

	testStream := func(size int64) {
		defer wg.Done()
		s.testFileStreams(s.T(), size)
	}

	wg.Add(numStreams)
	for i := numStreams; i > 0; i-- {
		go testStream(int64(numStreams) * 1 << 20)
	}

	wg.Wait()
}

// TODO (brianbland): evaluate the relevancy of this test
// TestEventualConsistency checks that if stat says that a file is a certain size, then
// you can freely read from the file (this is the only guarantee that the driver needs to provide)
// func (suite *DriverTestSuite) TestEventualConsistency() {
// 	if testing.Short() {
// 		suite.T().Skip("Skipping test in short mode")
// 	}
//
// 	filename := randomPath(1,32)
// 	defer suite.deletePath(suite.T(), firstPart(filename), false)
//
// 	var offset int64
// 	var misswrites int
// 	var chunkSize int64 = 32
//
// 	for i := 0; i < 1024; i++ {
// 		contents :=  s.blobberFactory.GetBlober(chunkSize).GetAllBytes()
// 		read, err := suite.StorageDriver.Writer(suite.ctx, filename, offset, bytes.NewReader(contents))
// 		require.NoError(suite.T(), err)
//
// 		fi, err := suite.StorageDriver.Stat(suite.ctx, filename)
// 		require.NoError(suite.T(), err)
//
// 		// We are most concerned with being able to read data as soon as Stat declares
// 		// it is uploaded. This is the strongest guarantee that some drivers (that guarantee
// 		// at best eventual consistency) absolutely need to provide.
// 		if fi.Size() == offset+chunkSize {
// 			reader, err := suite.StorageDriver.Reader(suite.ctx, filename, offset)
// 			require.NoError(suite.T(), err)
//
// 			readContents, err := io.ReadAll(reader)
// 			require.NoError(suite.T(), err)
//
//          require.Equal(suite.T(), contents, readContents)
//
// 			reader.Close()
// 			offset += read
// 		} else {
// 			misswrites++
// 		}
// 	}
//
// 	if misswrites > 0 {
//		c.Log("There were " + string(misswrites) + " occurrences of a write not being instantly available.")
// 	}
//
//  require.NotEqual(suite.T(), 1024, misswrites)
// }

// TestWalk ensures that all files are visted by WalkParallel.
func (s *DriverSuite) TestWalk() {
	rootDirectory := "/" + dtestutil.RandomFilenameRange(8, 8)
	defer s.deletePath(s.StorageDriver, rootDirectory)
	s.T().Logf("root directory used for testing: %s", rootDirectory)

	numWantedFiles := 10
	wantedFiles := dtestutil.RandomBranchingFiles(rootDirectory, numWantedFiles)
	wantedDirectoriesSet := make(map[string]struct{})

	for i := 0; i < numWantedFiles; i++ {
		// Gather unique directories from the full path, excluding the root directory.
		p := path.Dir(wantedFiles[i])
		for {
			// Guard against non-terminating loops: path.Dir returns "." if the path is empty.
			if p == rootDirectory || p == "." {
				break
			}
			wantedDirectoriesSet[p] = struct{}{}
			p = path.Dir(p)
		}

		/* #nosec G404 */
		err := s.StorageDriver.PutContent(s.ctx, wantedFiles[i], s.blobberFactory.GetBlobber(8+mrand.Int64N(8)).GetAllBytes())
		require.NoError(s.T(), err)
	}

	verifyResults := func(actualFiles, actualDirectories []string) {
		require.ElementsMatch(s.T(), wantedFiles, actualFiles)

		// Convert from a set of wanted directories into a slice.
		wantedDirectories := make([]string, len(wantedDirectoriesSet))

		var i int
		for k := range wantedDirectoriesSet {
			wantedDirectories[i] = k
			i++
		}

		require.ElementsMatch(s.T(), wantedDirectories, actualDirectories)
	}

	s.Run("PararellWalk", func() {
		fChan := make(chan string)
		dChan := make(chan string)

		var actualFiles []string
		var actualDirectories []string

		var wg sync.WaitGroup

		go func() {
			defer wg.Done()
			wg.Add(1)
			for f := range fChan {
				actualFiles = append(actualFiles, f)
			}
		}()
		go func() {
			defer wg.Done()
			wg.Add(1)
			for d := range dChan {
				actualDirectories = append(actualDirectories, d)
			}
		}()

		err := s.StorageDriver.WalkParallel(s.ctx, rootDirectory, func(fInfo storagedriver.FileInfo) error {
			// Use append here to prevent a panic if walk finds more than we expect.
			if fInfo.IsDir() {
				dChan <- fInfo.Path()
			} else {
				fChan <- fInfo.Path()
			}
			return nil
		})
		require.NoError(s.T(), err)

		close(fChan)
		close(dChan)

		wg.Wait()

		verifyResults(actualFiles, actualDirectories)
	})

	s.Run("PlainWalk", func() {
		var actualFiles []string
		var actualDirectories []string

		err := s.StorageDriver.Walk(s.ctx, rootDirectory, func(fInfo storagedriver.FileInfo) error {
			if fInfo.IsDir() {
				actualDirectories = append(actualDirectories, fInfo.Path())
			} else {
				actualFiles = append(actualFiles, fInfo.Path())
			}
			return nil
		})

		require.NoError(s.T(), err)
		verifyResults(actualFiles, actualDirectories)
	})
}

// TestWalkError ensures that walk reports WalkFn errors.
func (s *DriverSuite) TestWalkError() {
	rootDirectory := "/" + dtestutil.RandomFilenameRange(8, 8)
	defer s.deletePath(s.StorageDriver, rootDirectory)
	s.T().Logf("root directory used for testing: %s", rootDirectory)

	wantedFiles := dtestutil.RandomBranchingFiles(rootDirectory, 100)

	for _, file := range wantedFiles {
		/* #nosec G404 */
		err := s.StorageDriver.PutContent(s.ctx, file, s.blobberFactory.GetBlobber(8+mrand.Int64N(8)).GetAllBytes())
		require.NoError(s.T(), err)
	}

	innerErr := errors.New("walk: expected test error")
	errorFile := wantedFiles[0]

	s.Run("PararellWalk", func() {
		err := s.StorageDriver.WalkParallel(s.ctx, rootDirectory, func(fInfo storagedriver.FileInfo) error {
			if fInfo.Path() == errorFile {
				return innerErr
			}
			return nil
		})

		// Drivers may or may not return a multierror here, check that the innerError
		// is present in the error returned by walk.
		require.ErrorIs(s.T(), err, innerErr)
	})

	s.Run("PlainWalk", func() {
		err := s.StorageDriver.Walk(s.ctx, rootDirectory, func(fInfo storagedriver.FileInfo) error {
			if fInfo.Path() == errorFile {
				return innerErr
			}
			return nil
		})

		// Drivers may or may not return a multierror here, check that the innerError
		// is present in the error returned by walk.
		require.ErrorIs(s.T(), err, innerErr)
	})
}

// TestWalkSkipDir tests that the Walk and WalkParallel functions
// properly handle the ErrSkipDir special case.
func (s *DriverSuite) TestWalkSkipDir() {
	rootDirectory := "/" + dtestutil.RandomFilenameRange(8, 8)
	defer s.deletePath(s.StorageDriver, rootDirectory)
	s.T().Logf("root directory used for testing: %s", rootDirectory)

	// Create directories with a structure like:
	// rootDirectory/
	//   ├── dir1/
	//   │    ├── file1-1
	//   │    └── file1-2
	//   ├── dir2/  (this one will be skipped)
	//   │    ├── file2-1
	//   │    └── file2-2
	//   └── dir3/
	//        ├── file3-1
	//        └── file3-2

	dir1 := path.Join(rootDirectory, "dir1")
	dir2 := path.Join(rootDirectory, "dir2") // this will be skipped
	dir3 := path.Join(rootDirectory, "dir3")

	// Create map of all files to create with full paths
	fileStructure := map[string][]string{
		dir1: {
			path.Join(dir1, "file1-1"),
			path.Join(dir1, "file1-2"),
		},
		dir2: { // dir2 will be skipped during walk
			path.Join(dir2, "file2-1"),
			path.Join(dir2, "file2-2"),
		},
		dir3: {
			path.Join(dir3, "file3-1"),
			path.Join(dir3, "file3-2"),
		},
	}

	// Create all files in all directories
	for _, files := range fileStructure {
		for _, filePath := range files {
			err := s.StorageDriver.PutContent(s.ctx, filePath, s.blobberFactory.GetBlobber(32).GetAllBytes())
			require.NoError(s.T(), err)
		}
	}

	// Build expected and unexpected paths
	expectedPaths := []string{
		dir1,
		dir3,
		fileStructure[dir1][0],
		fileStructure[dir1][1],
		fileStructure[dir3][0],
		fileStructure[dir3][1],
	}

	// Unexpected paths are dir2 and all its files
	unexpectedPaths := []string{
		dir2,
		fileStructure[dir2][0],
		fileStructure[dir2][1],
	}

	// Helper function to create a walk function that skips dir2
	createWalkFunc := func(pathCollector *sync.Map) storagedriver.WalkFn {
		return func(fInfo storagedriver.FileInfo) error {
			path := fInfo.Path()

			// Skip dir2
			if path == dir2 {
				return storagedriver.ErrSkipDir
			}

			// Record this path was visited
			pathCollector.Store(path, struct{}{})
			return nil
		}
	}

	// Verify all expected paths were visited and unexpected were not
	verifyPaths := func(visitedPaths *sync.Map) {
		// Check expected paths were visited
		for _, expectedPath := range expectedPaths {
			_, found := visitedPaths.Load(expectedPath)
			assert.Truef(s.T(), found, "Path %s should have been visited", expectedPath)
		}

		// Check unexpected paths were not visited
		for _, unexpectedPath := range unexpectedPaths {
			_, found := visitedPaths.Load(unexpectedPath)
			assert.Falsef(s.T(), found, "Path %s should not have been visited", unexpectedPath)
		}
	}

	s.Run("PlainWalk", func() {
		var visitedPaths sync.Map
		err := s.StorageDriver.Walk(s.ctx, rootDirectory, createWalkFunc(&visitedPaths))
		require.NoError(s.T(), err)
		verifyPaths(&visitedPaths)
	})

	s.Run("ParallelWalk", func() {
		var visitedPaths sync.Map
		err := s.StorageDriver.WalkParallel(s.ctx, rootDirectory, createWalkFunc(&visitedPaths))
		require.NoError(s.T(), err)
		verifyPaths(&visitedPaths)
	})
}

// TestWalkErrorPathNotFound ensures that walk reports an error on a path not
// found.
func (s *DriverSuite) TestWalkErrorPathNotFound() {
	s.Run("PararellWalk", func() {
		err := s.StorageDriver.WalkParallel(s.ctx, "/maryna/boryna", func(_ storagedriver.FileInfo) error {
			return nil
		})

		// Drivers may or may not return a multierror here, check that the innerError
		// is present in the error returned by walk.
		require.ErrorAs(s.T(), err, new(storagedriver.PathNotFoundError))
	})

	s.Run("PlainWalk", func() {
		err := s.StorageDriver.Walk(s.ctx, "/maryna/boryna", func(_ storagedriver.FileInfo) error {
			return nil
		})

		// Drivers may or may not return a multierror here, check that the innerError
		// is present in the error returned by walk.
		require.ErrorAs(s.T(), err, new(storagedriver.PathNotFoundError))
	})
}

// TestWalkParallelStopsProcessingOnError ensures that walk stops processing when an error is encountered.
func (s *DriverSuite) TestWalkParallelStopsProcessingOnError() {
	d := s.StorageDriver.Name()
	switch d {
	case "filesystem", azure_v1.DriverName, azure_v2.DriverName:
		s.T().Skipf("%s driver does not support true WalkParallel", d)
	case "gcs":
		parallelWalk := os.Getenv("GCS_PARALLEL_WALK")
		var parallelWalkBool bool
		var err error
		if parallelWalk != "" {
			parallelWalkBool, err = strconv.ParseBool(parallelWalk)
			require.NoError(s.T(), err)
		}

		if !parallelWalkBool || parallelWalk == "" {
			s.T().Skipf("%s driver is not configured with parallelwalk", d)
		}
	}

	rootDirectory := "/" + dtestutil.RandomFilenameRange(8, 8)
	defer s.deletePath(s.StorageDriver, rootDirectory)
	s.T().Logf("root directory used for testing: %s", rootDirectory)

	numWantedFiles := 1000
	wantedFiles := dtestutil.RandomBranchingFiles(rootDirectory, numWantedFiles)

	// Add a file right under the root directory, so that processing is stopped
	// early in the walk cycle.
	errorFile := filepath.Join(rootDirectory, dtestutil.RandomFilenameRange(8, 8))
	wantedFiles = append(wantedFiles, errorFile)

	for _, file := range wantedFiles {
		/* #nosec G404 */
		err := s.StorageDriver.PutContent(s.ctx, file, s.blobberFactory.GetBlobber(8+mrand.Int64N(8)).GetAllBytes())
		require.NoError(s.T(), err)
	}

	processingTime := time.Second * 1
	// Rough limit that should scale with longer or shorter processing times. Shorter than full uncancled runtime.
	limit := time.Second * time.Duration(int64(processingTime)*4)

	start := time.Now()

	s.StorageDriver.WalkParallel(s.ctx, rootDirectory, func(fInfo storagedriver.FileInfo) error {
		if fInfo.Path() == errorFile {
			return errors.New("")
		}

		// Imitate workload.
		time.Sleep(processingTime)

		return nil
	})

	end := time.Now()

	require.Less(s.T(), end.Sub(start), limit)
}

// BenchmarkPutGetEmptyFiles benchmarks PutContent/GetContent for 0B files
func (s *DriverSuite) BenchmarkPutGetEmptyFiles(b *testing.B) {
	s.benchmarkPutGetFiles(b, 0)
}

// BenchmarkPutGet1KBFiles benchmarks PutContent/GetContent for 1KB files
func (s *DriverSuite) BenchmarkPutGet1KBFiles(b *testing.B) {
	s.benchmarkPutGetFiles(b, 1024)
}

// BenchmarkPutGet1MBFiles benchmarks PutContent/GetContent for 1MB files
func (s *DriverSuite) BenchmarkPutGet1MBFiles(b *testing.B) {
	s.benchmarkPutGetFiles(b, 1024*1024)
}

// BenchmarkPutGet1GBFiles benchmarks PutContent/GetContent for 1GB files
func (s *DriverSuite) BenchmarkPutGet1GBFiles(b *testing.B) {
	s.benchmarkPutGetFiles(b, 1024*1024*1024)
}

func (s *DriverSuite) benchmarkPutGetFiles(b *testing.B, size int64) {
	b.SetBytes(size)
	parentDir := dtestutil.RandomPath(1, 8)
	defer func() {
		b.StopTimer()
		s.StorageDriver.Delete(s.ctx, firstPart(parentDir))
	}()

	for i := 0; i < b.N; i++ {
		filename := path.Join(parentDir, dtestutil.RandomPath(4, 32))
		err := s.StorageDriver.PutContent(s.ctx, filename, s.blobberFactory.GetBlobber(size).GetAllBytes())
		require.NoError(b, err)

		_, err = s.StorageDriver.GetContent(s.ctx, filename)
		require.NoError(b, err)
	}
}

// BenchmarkStreamEmptyFiles benchmarks Writer/Reader for 0B files
func (s *DriverSuite) BenchmarkStreamEmptyFiles(b *testing.B) {
	if s.StorageDriver.Name() == "s3aws" {
		s.T().Skip("S3 multipart uploads require at least 1 chunk (>0B)")
	}
	s.benchmarkStreamFiles(b, 0)
}

// BenchmarkStream1KBFiles benchmarks Writer/Reader for 1KB files
func (s *DriverSuite) BenchmarkStream1KBFiles(b *testing.B) {
	s.benchmarkStreamFiles(b, 1024)
}

// BenchmarkStream1MBFiles benchmarks Writer/Reader for 1MB files
func (s *DriverSuite) BenchmarkStream1MBFiles(b *testing.B) {
	s.benchmarkStreamFiles(b, 1024*1024)
}

// BenchmarkStream1GBFiles benchmarks Writer/Reader for 1GB files
func (s *DriverSuite) BenchmarkStream1GBFiles(b *testing.B) {
	s.benchmarkStreamFiles(b, 1024*1024*1024)
}

func (s *DriverSuite) benchmarkStreamFiles(b *testing.B, size int64) {
	b.SetBytes(size)
	parentDir := dtestutil.RandomPath(1, 8)
	defer func() {
		b.StopTimer()
		s.StorageDriver.Delete(s.ctx, firstPart(parentDir))
	}()

	for i := 0; i < b.N; i++ {
		filename := path.Join(parentDir, dtestutil.RandomPath(4, 32))
		writer, err := s.StorageDriver.Writer(s.ctx, filename, false)
		require.NoError(b, err)
		written, err := io.Copy(writer, s.blobberFactory.GetBlobber(size).GetReader())
		require.NoError(b, err)
		require.Equal(b, size, written)

		err = writer.Commit()
		require.NoError(b, err)
		err = writer.Close()
		require.NoError(b, err)

		rc, err := s.StorageDriver.Reader(s.ctx, filename, 0)
		require.NoError(b, err)
		rc.Close()
	}
}

// BenchmarkList5Files benchmarks List for 5 small files
func (s *DriverSuite) BenchmarkList5Files(b *testing.B) {
	s.benchmarkListFiles(b, 5)
}

// BenchmarkList50Files benchmarks List for 50 small files
func (s *DriverSuite) BenchmarkList50Files(b *testing.B) {
	s.benchmarkListFiles(b, 50)
}

func (s *DriverSuite) benchmarkListFiles(b *testing.B, numFiles int64) {
	parentDir := dtestutil.RandomPath(1, 8)
	defer func() {
		b.StopTimer()
		s.StorageDriver.Delete(s.ctx, firstPart(parentDir))
	}()

	for i := int64(0); i < numFiles; i++ {
		err := s.StorageDriver.PutContent(s.ctx, path.Join(parentDir, dtestutil.RandomPath(4, 32)), nil)
		require.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		files, err := s.StorageDriver.List(s.ctx, parentDir)
		require.NoError(b, err)
		require.Equal(b, numFiles, int64(len(files)))
	}
}

// BenchmarkDelete5Files benchmarks Delete for 5 small files
func (s *DriverSuite) BenchmarkDelete5Files(b *testing.B) {
	s.benchmarkDelete(b, 5)
}

// BenchmarkDelete50Files benchmarks Delete for 50 small files
func (s *DriverSuite) BenchmarkDelete50Files(b *testing.B) {
	s.benchmarkDelete(b, 50)
}

func (s *DriverSuite) benchmarkDelete(b *testing.B, numFiles int64) {
	for i := 0; i < b.N; i++ {
		parentDir := dtestutil.RandomPath(4, 12)
		// nolint: revive// defer
		defer s.deletePath(s.StorageDriver, firstPart(parentDir))

		b.StopTimer()
		for j := int64(0); j < numFiles; j++ {
			err := s.StorageDriver.PutContent(s.ctx, path.Join(parentDir, dtestutil.RandomPath(4, 32)), nil)
			require.NoError(b, err)
		}
		b.StartTimer()

		// This is the operation we're benchmarking
		err := s.StorageDriver.Delete(s.ctx, firstPart(parentDir))
		require.NoError(b, err)
	}
}

// BenchmarkWalkParallelNop10Files benchmarks WalkParallel with a Nop function that visits 10 files
func (s *DriverSuite) BenchmarkWalkParallelNop10Files(b *testing.B) {
	s.benchmarkWalkParallel(b, 10, func(_ storagedriver.FileInfo) error {
		return nil
	})
}

// BenchmarkWalkParallelNop500Files benchmarks WalkParallel with a Nop function that visits 500 files
func (s *DriverSuite) BenchmarkWalkParallelNop500Files(b *testing.B) {
	s.benchmarkWalkParallel(b, 500, func(_ storagedriver.FileInfo) error {
		return nil
	})
}

func (s *DriverSuite) benchmarkWalkParallel(b *testing.B, numFiles int, f storagedriver.WalkFn) {
	for i := 0; i < b.N; i++ {
		rootDirectory := "/" + dtestutil.RandomFilenameRange(8, 8)
		// nolint: revive // defer
		defer s.deletePath(s.StorageDriver, rootDirectory)

		b.StopTimer()

		wantedFiles := dtestutil.RandomBranchingFiles(rootDirectory, numFiles)

		// NOTE(prozlach): We are creating random size blobs, so we
		// pre-allocate blobber that is able to accommodate the worst-case where
		// all blobs have maximum size.
		blobber := s.blobberFactory.GetBlobber(int64(numFiles * (8 + 8)))
		offset := 0
		for i := 0; i < numFiles; i++ {
			/* #nosec G404 */
			newOffset := 8 + mrand.IntN(8) + offset
			contents := blobber.GetAllBytes()[offset:newOffset]
			offset = newOffset
			err := s.StorageDriver.PutContent(s.ctx, wantedFiles[i], contents)
			require.NoError(b, err)
		}

		b.StartTimer()

		err := s.StorageDriver.WalkParallel(s.ctx, rootDirectory, f)
		require.NoError(b, err)
	}
}

func (s *DriverSuite) createRegistry(t require.TestingT) distribution.Namespace {
	k, err := libtrust.GenerateECP256PrivateKey()
	require.NoError(t, err)

	opts := []storage.RegistryOption{
		storage.EnableDelete,
		storage.Schema1SigningKey(k),
		storage.EnableSchema1,
	}

	registry, err := storage.NewRegistry(s.ctx, s.StorageDriver, opts...)
	require.NoError(t, err, "Failed to construct namespace")
	return registry
}

func (s *DriverSuite) makeRepository(t require.TestingT, registry distribution.Namespace, name string) distribution.Repository {
	named, err := reference.WithName(name)
	require.NoErrorf(t, err, "Failed to parse name %s", name)

	repo, err := registry.Repository(s.ctx, named)
	require.NoError(t, err, "Failed to construct repository")
	return repo
}

// BenchmarkMarkAndSweep10ImagesKeepUntagged uploads 10 images, deletes half
// and runs garbage collection on the registry without removing untaged images.
func (s *DriverSuite) BenchmarkMarkAndSweep10ImagesKeepUntagged(b *testing.B) {
	s.benchmarkMarkAndSweep(b, 10, false)
}

// BenchmarkMarkAndSweep50ImagesKeepUntagged uploads 50 images, deletes half
// and runs garbage collection on the registry without removing untaged images.
func (s *DriverSuite) BenchmarkMarkAndSweep50ImagesKeepUntagged(b *testing.B) {
	s.benchmarkMarkAndSweep(b, 50, false)
}

func (s *DriverSuite) benchmarkMarkAndSweep(b *testing.B, numImages int, removeUntagged bool) {
	// Setup for this test takes a long time, even with small numbers of images,
	// so keep the skip logic here in the sub test.
	defer s.deletePath(s.StorageDriver, firstPart("docker/"))

	for n := 0; n < b.N; n++ {
		b.StopTimer()

		registry := s.createRegistry(b)
		repo := s.makeRepository(b, registry, fmt.Sprintf("benchmarks-repo-%d", n))

		manifests, err := repo.Manifests(s.ctx)
		require.NoError(b, err)

		images := make([]testutil.Image, numImages)

		for i := 0; i < numImages; i++ {
			// Alternate between Schema1 and Schema2 images
			if i%2 == 0 {
				images[i], err = testutil.UploadRandomSchema1Image(repo)
				require.NoError(b, err)
			} else {
				images[i], err = testutil.UploadRandomSchema2Image(repo)
				require.NoError(b, err)
			}

			// Delete the manifests, so that their blobs can be garbage collected.
			manifests.Delete(s.ctx, images[i].ManifestDigest)
		}

		b.StartTimer()

		// Run GC
		err = storage.MarkAndSweep(context.Background(), s.StorageDriver, registry, storage.GCOpts{
			DryRun:         false,
			RemoveUntagged: removeUntagged,
		})
		require.NoError(b, err)
	}
}

func (*DriverSuite) buildBlobs(t require.TestingT, repo distribution.Repository, n int) []digest.Digest {
	dgsts := make([]digest.Digest, 0, n)

	// build and upload random layers
	layers, err := testutil.CreateRandomLayers(n)
	require.NoError(t, err, "failed to create random digest")
	err = testutil.UploadBlobs(repo, layers)
	require.NoError(t, err, "failed to upload blob")

	// collect digests from layers map
	for d := range layers {
		dgsts = append(dgsts, d)
	}

	return dgsts
}

// TestRemoveBlob checks that storage.Vacuum is able to delete a single blob.
func (s *DriverSuite) TestRemoveBlob() {
	defer s.deletePath(s.StorageDriver, firstPart("docker/"))

	registry := s.createRegistry(s.T())
	repoName := dtestutil.RandomFilename(5)
	repo := s.makeRepository(s.T(), registry, repoName)
	s.T().Logf("repo name used for testing: %s", repoName)
	v := storage.NewVacuum(s.StorageDriver)

	// build two blobs, one more than the number to delete, otherwise there will be no /docker/registry/v2/blobs path
	// for validation after delete
	blobs := s.buildBlobs(s.T(), repo, 2)
	blob := blobs[0]

	err := v.RemoveBlob(s.ctx, blob)
	require.NoError(s.T(), err)

	blobService := registry.Blobs()
	blobsLeft := newSyncDigestSet()
	err = blobService.Enumerate(s.ctx, func(desc distribution.Descriptor) error {
		blobsLeft.add(desc.Digest)
		return nil
	})
	require.NoError(s.T(), err, "error getting all blobs")

	assert.Equal(s.T(), 1, blobsLeft.len())
	assert.Falsef(s.T(), blobsLeft.contains(blob), "blob %q was not deleted", blob.String())
}

func (s *DriverSuite) benchmarkRemoveBlob(b *testing.B, numBlobs int) {
	defer s.deletePath(s.StorageDriver, firstPart("docker/"))

	registry := s.createRegistry(b)
	repoName := dtestutil.RandomFilename(5)
	repo := s.makeRepository(s.T(), registry, repoName)
	s.T().Logf("repo name used for testing: %s", repoName)
	v := storage.NewVacuum(s.StorageDriver)

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		blobs := s.buildBlobs(b, repo, numBlobs)
		b.StartTimer()

		for _, bl := range blobs {
			err := v.RemoveBlob(s.ctx, bl)
			require.NoError(b, err)
		}
	}
}

// BenchmarkRemoveBlob1Blob creates 1 blob and deletes it using the storage.Vacuum.RemoveBlob method.
func (s *DriverSuite) BenchmarkRemoveBlob1Blob(b *testing.B) {
	s.benchmarkRemoveBlob(b, 1)
}

// BenchmarkRemoveBlob10Blobs creates 10 blobs and deletes them using the storage.Vacuum.RemoveBlob method.
func (s *DriverSuite) BenchmarkRemoveBlob10Blobs(b *testing.B) {
	s.benchmarkRemoveBlob(b, 10)
}

// BenchmarkRemoveBlob100Blobs creates 100 blobs and deletes them using the storage.Vacuum.RemoveBlob method.
func (s *DriverSuite) BenchmarkRemoveBlob100Blobs(b *testing.B) {
	s.benchmarkRemoveBlob(b, 100)
}

// TestRemoveBlobs checks that storage.Vacuum is able to delete a set of blobs in bulk.
func (s *DriverSuite) TestRemoveBlobs() {
	defer s.deletePath(s.StorageDriver, firstPart("docker/"))

	registry := s.createRegistry(s.T())
	repoName := dtestutil.RandomFilename(5)
	repo := s.makeRepository(s.T(), registry, repoName)
	s.T().Logf("repo name used for testing: %s", repoName)
	v := storage.NewVacuum(s.StorageDriver)

	// build some blobs and remove half of them, otherwise there will be no /docker/registry/v2/blobs path to look at
	// for validation if there are no blobs left
	blobs := s.buildBlobs(s.T(), repo, 4)
	blobs = blobs[:2]

	err := v.RemoveBlobs(s.ctx, blobs)
	require.NoError(s.T(), err)

	// assert that blobs were deleted
	blobService := registry.Blobs()
	blobsLeft := newSyncDigestSet()
	err = blobService.Enumerate(s.ctx, func(desc distribution.Descriptor) error {
		blobsLeft.add(desc.Digest)
		return nil
	})
	require.NoError(s.T(), err, "error getting all blobs")

	require.Equal(s.T(), 2, blobsLeft.len())
	for _, b := range blobs {
		assert.Falsef(s.T(), blobsLeft.contains(b), "blob %q was not deleted", b.String())
	}
}

func (s *DriverSuite) benchmarkRemoveBlobs(b *testing.B, numBlobs int) {
	defer s.deletePath(s.StorageDriver, firstPart("docker/"))

	registry := s.createRegistry(b)
	repoName := dtestutil.RandomFilename(5)
	repo := s.makeRepository(s.T(), registry, repoName)
	s.T().Logf("repo name used for testing: %s", repoName)
	v := storage.NewVacuum(s.StorageDriver)

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		blobs := s.buildBlobs(b, repo, numBlobs)
		b.StartTimer()

		err := v.RemoveBlobs(s.ctx, blobs)
		assert.NoError(b, err)
	}
}

// BenchmarkRemoveBlobs1Blob creates 1 blob and deletes it using the storage.Vacuum.RemoveBlobs method.
func (s *DriverSuite) BenchmarkRemoveBlobs1Blob(b *testing.B) {
	s.benchmarkRemoveBlobs(b, 1)
}

// BenchmarkRemoveBlobs10Blobs creates 10 blobs and deletes them using the storage.Vacuum.RemoveBlobs method.
func (s *DriverSuite) BenchmarkRemoveBlobs10Blobs(b *testing.B) {
	s.benchmarkRemoveBlobs(b, 10)
}

// BenchmarkRemoveBlobs100Blobs creates 100 blobs and deletes them using the storage.Vacuum.RemoveBlobs method.
func (s *DriverSuite) BenchmarkRemoveBlobs100Blobs(b *testing.B) {
	s.benchmarkRemoveBlobs(b, 100)
}

// BenchmarkRemoveBlobs1000Blobs creates 1000 blobs and deletes them using the storage.Vacuum.RemoveBlobs method.
func (s *DriverSuite) BenchmarkRemoveBlobs1000Blobs(b *testing.B) {
	s.benchmarkRemoveBlobs(b, 1000)
}

func (s *DriverSuite) buildManifests(t require.TestingT, repo distribution.Repository, numManifests, numTagsPerManifest int) []storage.ManifestDel {
	images := make([]testutil.Image, numManifests)
	manifests := make([]storage.ManifestDel, 0)
	repoName := repo.Named().Name()

	var err error
	for i := 0; i < numManifests; i++ {
		// build images, alternating between Schema1 and Schema2 manifests
		if i%2 == 0 {
			images[i], err = testutil.UploadRandomSchema1Image(repo)
		} else {
			images[i], err = testutil.UploadRandomSchema2Image(repo)
		}
		require.NoError(t, err)

		// build numTags tags per manifest
		tags := make([]string, 0, numTagsPerManifest)
		for j := 0; j < numTagsPerManifest; j++ {
			rfn := dtestutil.RandomFilename(5)
			d := images[i].ManifestDigest
			err := repo.Tags(s.ctx).Tag(s.ctx, rfn, distribution.Descriptor{Digest: d})
			require.NoError(t, err)
			tags = append(tags, rfn)
		}

		manifests = append(manifests, storage.ManifestDel{
			Name:   repoName,
			Digest: images[i].ManifestDigest,
			Tags:   tags,
		})
	}

	return manifests
}

// TestRemoveManifests checks that storage.Vacuum is able to delete a set of manifests in bulk.
func (s *DriverSuite) TestRemoveManifests() {
	defer s.deletePath(s.StorageDriver, firstPart("docker/"))

	registry := s.createRegistry(s.T())
	repoName := dtestutil.RandomFilename(5)
	repo := s.makeRepository(s.T(), registry, repoName)
	s.T().Logf("repo name used for testing: %s", repoName)

	// build some manifests
	manifests := s.buildManifests(s.T(), repo, 3, 1)

	v := storage.NewVacuum(s.StorageDriver)

	// remove all manifests except one, otherwise there will be no `_manifests/revisions` folder to look at for
	// validation (empty "folders" are not preserved)
	numToDelete := len(manifests) - 1
	toDelete := manifests[:numToDelete]

	err := v.RemoveManifests(s.ctx, toDelete)
	require.NoError(s.T(), err)

	// assert that toDelete manifests were actually deleted
	manifestsLeft := newSyncDigestSet()
	manifestService, err := repo.Manifests(s.ctx)
	require.NoError(s.T(), err, "error building manifest service")
	manifestEnumerator, ok := manifestService.(distribution.ManifestEnumerator)
	require.True(s.T(), ok, "unable to convert ManifestService into ManifestEnumerator")
	err = manifestEnumerator.Enumerate(s.ctx, func(dgst digest.Digest) error {
		manifestsLeft.add(dgst)
		return nil
	})
	require.NoError(s.T(), err, "error getting all manifests")

	require.Equal(s.T(), len(manifests)-numToDelete, manifestsLeft.len())

	for _, m := range toDelete {
		assert.Falsef(s.T(), manifestsLeft.contains(m.Digest), "manifest %q was not deleted as expected", m.Digest)
	}
}

func (s *DriverSuite) testRemoveManifestsPathBuild(numManifests, numTagsPerManifest int) {
	v := storage.NewVacuum(s.StorageDriver)

	var tags []string
	for i := 0; i < numTagsPerManifest; i++ {
		tags = append(tags, "foo")
	}

	var toDelete []storage.ManifestDel
	for i := 0; i < numManifests; i++ {
		m := storage.ManifestDel{
			Name:   dtestutil.RandomFilename(10),
			Digest: digest.FromString(dtestutil.RandomFilename(20)),
			Tags:   tags,
		}
		toDelete = append(toDelete, m)
	}
	err := v.RemoveManifests(s.ctx, toDelete)
	require.NoError(s.T(), err)
}

// TestRemoveManifestsPathBuildLargeScale simulates the execution of vacuum.RemoveManifests for repositories with large
// numbers of manifests eligible for deletion. No files are created in this test, we only simulate their existence so
// that we can test and profile the execution of the path build process within vacuum.RemoveManifests. The storage
// drivers DeleteFiles method is idempotent, so no error will be raised by attempting to delete non-existing files.
// However, to avoid large number of HTTP requests against cloud storage backends, it's recommended to run this test
// against the filesystem storage backend only. For safety, the test is skipped when not using the filesystem storage
// backend. Tweak the method locally to test use cases with different sizes and/or storage drivers.
func (s *DriverSuite) TestRemoveManifestsPathBuildLargeScale() {
	if s.StorageDriver.Name() != "filesystem" {
		s.T().Skipf("Skipping test for the %s driver", s.StorageDriver.Name())
	}

	numManifests := 100
	numTagsPerManifest := 10

	s.testRemoveManifestsPathBuild(numManifests, numTagsPerManifest)
}

func (s *DriverSuite) benchmarkRemoveManifests(b *testing.B, numManifests, numTagsPerManifest int) {
	defer s.deletePath(s.StorageDriver, firstPart("docker/"))

	registry := s.createRegistry(b)
	repoName := dtestutil.RandomFilename(5)
	repo := s.makeRepository(s.T(), registry, repoName)
	s.T().Logf("repo name used for testing: %s", repoName)

	for n := 0; n < b.N; n++ {
		b.StopTimer()

		manifests := s.buildManifests(b, repo, numManifests, numTagsPerManifest)
		v := storage.NewVacuum(s.StorageDriver)

		b.StartTimer()

		err := v.RemoveManifests(s.ctx, manifests)
		require.NoError(b, err)
	}
}

// BenchmarkRemoveManifests1Manifest0Tags creates 1 manifest with no tags and deletes it using the
// storage.Vacuum.RemoveManifests method.
func (s *DriverSuite) BenchmarkRemoveManifests1Manifest0Tags(b *testing.B) {
	s.benchmarkRemoveManifests(b, 1, 0)
}

// BenchmarkRemoveManifests1Manifest1Tag creates 1 manifest with 1 tag and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (s *DriverSuite) BenchmarkRemoveManifests1Manifest1Tag(b *testing.B) {
	s.benchmarkRemoveManifests(b, 1, 1)
}

// BenchmarkRemoveManifests10Manifests0TagsEach creates 10 manifests with no tags and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (s *DriverSuite) BenchmarkRemoveManifests10Manifests0TagsEach(b *testing.B) {
	s.benchmarkRemoveManifests(b, 10, 0)
}

// BenchmarkRemoveManifests10Manifests1TagEach creates 10 manifests with 1 tag each and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (s *DriverSuite) BenchmarkRemoveManifests10Manifests1TagEach(b *testing.B) {
	s.benchmarkRemoveManifests(b, 10, 1)
}

// BenchmarkRemoveManifests100Manifests0TagsEach creates 100 manifests with no tags and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (s *DriverSuite) BenchmarkRemoveManifests100Manifests0TagsEach(b *testing.B) {
	s.benchmarkRemoveManifests(b, 100, 0)
}

// BenchmarkRemoveManifests100Manifests1TagEach creates 100 manifests with 1 tag each and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (s *DriverSuite) BenchmarkRemoveManifests100Manifests1TagEach(b *testing.B) {
	s.benchmarkRemoveManifests(b, 100, 1)
}

// BenchmarkRemoveManifests100Manifests20TagsEach creates 100 manifests with 20 tags each and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (s *DriverSuite) BenchmarkRemoveManifests100Manifests20TagsEach(b *testing.B) {
	s.benchmarkRemoveManifests(b, 100, 20)
}

// NOTE(prozlach) testFileStreams is used in a goroutine, we can't use
// `require` here
func (s *DriverSuite) testFileStreams(t *testing.T, size int64) {
	filename := dtestutil.RandomPath(4, 32)
	defer s.deletePath(s.StorageDriver, firstPart(filename))
	s.T().Logf("blob path for this stream: %s", filename)

	blobber := s.blobberFactory.GetBlobber(size)

	writer, err := s.StorageDriver.Writer(s.ctx, filename, false)
	// nolint: testifylint // require-error
	if !assert.NoError(t, err) {
		return
	}
	nn, err := io.Copy(writer, blobber.GetReader())
	// nolint: testifylint // require-error
	if !assert.NoError(t, err) {
		return
	}
	if !assert.EqualValues(t, size, nn) {
		return
	}

	// BUG(prozlach): See https://gitlab.com/gitlab-org/container-registry/-/issues/1500
	// This is just a workaround for now to enforce correct behavior on other
	// drivers. The TLDR is that gcs Writer object does not report correct size
	// until the Commit() is called. This is not the case for other drivers.
	if s.StorageDriver.Name() != "gcs" {
		if !assert.EqualValues(t, size, writer.Size()) {
			return
		}
	}

	// nolint: testifylint // require-error
	if !assert.NoError(t, writer.Commit()) {
		return
	}
	// nolint: testifylint // require-error
	if !assert.NoError(t, writer.Close()) {
		return
	}

	reader, err := s.StorageDriver.Reader(s.ctx, filename, 0)
	// nolint: testifylint // require-error
	if !assert.NoError(t, err) {
		return
	}
	defer reader.Close()

	streamID := fmt.Sprintf("file stream of size %d, path %s", size, filename)
	blobber.AssertStreamEqual(t, reader, 0, streamID)
}

func (s *DriverSuite) writeReadCompare(t *testing.T, filename string, contents []byte) {
	defer s.deletePath(s.StorageDriver, firstPart(filename))

	err := s.StorageDriver.PutContent(s.ctx, filename, contents)
	require.NoError(t, err)

	readContents, err := s.StorageDriver.GetContent(s.ctx, filename)
	require.NoError(t, err)

	require.Equal(t, contents, readContents)
}

func (s *DriverSuite) writeReadCompareStreams(t *testing.T, filename string, blobber *testutil.Blobber) {
	defer s.deletePath(s.StorageDriver, firstPart(filename))

	writer, err := s.StorageDriver.Writer(s.ctx, filename, false)
	require.NoError(t, err)
	nn, err := io.Copy(writer, blobber.GetReader())
	require.NoError(t, err)
	require.EqualValues(t, blobber.Size(), nn)

	err = writer.Commit()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	reader, err := s.StorageDriver.Reader(s.ctx, filename, 0)
	require.NoError(t, err)
	defer reader.Close()

	blobber.AssertStreamEqual(s.T(), reader, 0, fmt.Sprintf("filename: %s", filename))
}

func firstPart(filePath string) string {
	if filePath == "" {
		return "/"
	}
	for {
		if filePath[len(filePath)-1] == '/' {
			filePath = filePath[:len(filePath)-1]
		}

		dir, file := path.Split(filePath)
		if dir == "" && file == "" {
			return "/"
		}
		if dir == "/" || dir == "" {
			return "/" + file
		}
		if file == "" {
			return dir
		}
		filePath = dir
	}
}
