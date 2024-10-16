package testsuites

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	"github.com/docker/distribution/testutil"
)

// DriverConstructor is a function which returns a new
// storagedriver.StorageDriver.
type DriverConstructor func() (storagedriver.StorageDriver, error)

// DriverTeardown is a function which cleans up a suite's
// storagedriver.StorageDriver.
type DriverTeardown func() error

func NewDriverSuite(ctx context.Context, constructor DriverConstructor, destructor DriverTeardown) *DriverSuite {
	return &DriverSuite{
		ctx:         ctx,
		Constructor: constructor,
		Teardown:    destructor,
	}
}

// DriverSuite is a test suite designed to test a
// storagedriver.StorageDriver.
type DriverSuite struct {
	suite.Suite

	Constructor   DriverConstructor
	Teardown      DriverTeardown
	StorageDriver storagedriver.StorageDriver
	ctx           context.Context
}

// SetUpSuite sets up the test suite for tests.
func (suite *DriverSuite) SetupSuite() {
	suite.setupSuiteGeneric(suite.T())
}

// SetUpSuite sets up the test suite for benchmarks.
func (suite *DriverSuite) SetupSuiteWithB(b *testing.B) {
	suite.setupSuiteGeneric(b)
}

func (suite *DriverSuite) setupSuiteGeneric(t require.TestingT) {
	driver, err := suite.Constructor()
	require.NoError(t, err)
	suite.StorageDriver = driver
}

// TearDownSuite tears down the test suite when testing.
func (suite *DriverSuite) TearDownSuite() {
	suite.tearDownSuiteGeneric(suite.T())
}

// TearDownSuite tears down the test suite when benchmarking.
func (suite *DriverSuite) TearDownSuiteWithB(b *testing.B) {
	suite.tearDownSuiteGeneric(b)
}

func (suite *DriverSuite) tearDownSuiteGeneric(t require.TestingT) {
	if suite.Teardown == nil {
		return
	}

	err := suite.Teardown()
	require.NoError(t, err)
}

// TearDownTest tears down the test.
// This causes the suite to abort if any files are left around in the storage
// driver.
func (suite *DriverSuite) TearDownTest() {
	files, _ := suite.StorageDriver.List(suite.ctx, "/")
	require.Empty(suite.T(), files, "Storage driver did not clean up properly. Offending files: %#v", files)
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
// of benchmarks defined in the suite. This would require carefull
// maintainance/updating compared to simply automating it.
func (suite *DriverSuite) EnumerateBenchmarks() []BenchmarkFunc {
	benchmarks := []BenchmarkFunc{}

	st := reflect.TypeOf(suite)
	sv := reflect.ValueOf(suite)

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
func (suite *DriverSuite) TestRootExists() {
	_, err := suite.StorageDriver.List(suite.ctx, "/")
	require.NoError(suite.T(), err, `the root path "/" should always exist`)
}

// TestValidPaths checks that various valid file paths are accepted by the
// storage driver.
func (suite *DriverSuite) TestValidPaths() {
	contents := randomContents(64)
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
		err := suite.StorageDriver.PutContent(suite.ctx, filename, contents)
		defer suite.deletePath(suite.T(), firstPart(filename))
		require.NoError(suite.T(), err)

		received, err := suite.StorageDriver.GetContent(suite.ctx, filename)
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), contents, received)
	}
}

func (suite *DriverSuite) deletePath(t require.TestingT, path string) {
	// NOTE(prozlach): We want to make sure that we do not do an accidental
	// retry of the Delete call, hence it is outside of the
	// require.EventuallyWithT block.
	err := suite.StorageDriver.Delete(suite.ctx, path)
	if err != nil {
		if !errors.As(err, new(storagedriver.PathNotFoundError)) {
			// Handover the termination of the execution to require.NoError call
			require.NoError(t, err)
		}

		// Path does not exist, in theory we are done, but in practice
		// let's also confirm that with .List() call
	}

	require.EventuallyWithT(
		t,
		func(c *assert.CollectT) {
			paths, _ := suite.StorageDriver.List(suite.ctx, path)
			assert.Empty(c, paths)
		},
		4200*time.Millisecond,
		1*time.Second,
	)
}

// TestInvalidPaths checks that various invalid file paths are rejected by the
// storage driver.
func (suite *DriverSuite) TestInvalidPaths() {
	contents := randomContents(64)
	invalidFiles := []string{
		"",
		"/",
		"abc",
		"123.abc",
		"//bcd",
		"/abc_123/",
	}

	for _, filename := range invalidFiles {
		err := suite.StorageDriver.PutContent(suite.ctx, filename, contents)
		// only delete if file was successfully written
		if err == nil {
			defer suite.deletePath(suite.T(), firstPart(filename))
		}
		require.Error(suite.T(), err)
		require.ErrorIs(suite.T(), err, storagedriver.InvalidPathError{
			DriverName: suite.StorageDriver.Name(),
			Path:       filename,
		})

		_, err = suite.StorageDriver.GetContent(suite.ctx, filename)
		require.Error(suite.T(), err)
		require.ErrorIs(suite.T(), err, storagedriver.InvalidPathError{
			DriverName: suite.StorageDriver.Name(),
			Path:       filename,
		})
	}
}

// TestWriteRead1 tests a simple write-read workflow.
func (suite *DriverSuite) TestWriteRead1() {
	filename := randomPath(32)
	contents := []byte("a")
	suite.writeReadCompare(suite.T(), filename, contents)
}

// TestWriteRead2 tests a simple write-read workflow with unicode data.
func (suite *DriverSuite) TestWriteRead2() {
	filename := randomPath(32)
	contents := []byte("\xc3\x9f")
	suite.writeReadCompare(suite.T(), filename, contents)
}

// TestWriteRead3 tests a simple write-read workflow with a small string.
func (suite *DriverSuite) TestWriteRead3() {
	filename := randomPath(32)
	contents := randomContents(32)
	suite.writeReadCompare(suite.T(), filename, contents)
}

// TestWriteRead4 tests a simple write-read workflow with 1MB of data.
func (suite *DriverSuite) TestWriteRead4() {
	filename := randomPath(32)
	contents := randomContents(1024 * 1024)
	suite.writeReadCompare(suite.T(), filename, contents)
}

// TestWriteReadNonUTF8 tests that non-utf8 data may be written to the storage
// driver safely.
func (suite *DriverSuite) TestWriteReadNonUTF8() {
	filename := randomPath(32)
	contents := []byte{0x80, 0x80, 0x80, 0x80}
	suite.writeReadCompare(suite.T(), filename, contents)
}

// TestTruncate tests that putting smaller contents than an original file does
// remove the excess contents.
func (suite *DriverSuite) TestTruncate() {
	filename := randomPath(32)
	contents := randomContents(1024 * 1024)
	suite.writeReadCompare(suite.T(), filename, contents)

	contents = randomContents(1024)
	suite.writeReadCompare(suite.T(), filename, contents)
}

// TestReadNonexistent tests reading content from an empty path.
func (suite *DriverSuite) TestReadNonexistent() {
	filename := randomPath(32)
	_, err := suite.StorageDriver.GetContent(suite.ctx, filename)
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       filename,
	})
}

// TestWriteReadStreams1 tests a simple write-read streaming workflow.
func (suite *DriverSuite) TestWriteReadStreams1() {
	filename := randomPath(32)
	contents := []byte("a")
	suite.writeReadCompareStreams(suite.T(), filename, contents)
}

// TestWriteReadStreams2 tests a simple write-read streaming workflow with
// unicode data.
func (suite *DriverSuite) TestWriteReadStreams2() {
	filename := randomPath(32)
	contents := []byte("\xc3\x9f")
	suite.writeReadCompareStreams(suite.T(), filename, contents)
}

// TestWriteReadStreams3 tests a simple write-read streaming workflow with a
// small amount of data.
func (suite *DriverSuite) TestWriteReadStreams3() {
	filename := randomPath(32)
	contents := randomContents(32)
	suite.writeReadCompareStreams(suite.T(), filename, contents)
}

// TestWriteReadStreams4 tests a simple write-read streaming workflow with 1MB
// of data.
func (suite *DriverSuite) TestWriteReadStreams4() {
	filename := randomPath(32)
	contents := randomContents(1024 * 1024)
	suite.writeReadCompareStreams(suite.T(), filename, contents)
}

// TestWriteReadStreamsNonUTF8 tests that non-utf8 data may be written to the
// storage driver safely.
func (suite *DriverSuite) TestWriteReadStreamsNonUTF8() {
	filename := randomPath(32)
	contents := []byte{0x80, 0x80, 0x80, 0x80}
	suite.writeReadCompareStreams(suite.T(), filename, contents)
}

// TestWriteReadLargeStreams tests that a 5GB file may be written to the storage
// driver safely.
func (suite *DriverSuite) TestWriteReadLargeStreams() {
	if testing.Short() {
		suite.T().Skip("Skipping test in short mode")
	}

	filename := randomPath(32)
	defer suite.deletePath(suite.T(), firstPart(filename))

	checksum := sha256.New()
	var fileSize int64 = 2 * 1024 * 1024 * 1024

	contents := newRandReader(fileSize)

	writer, err := suite.StorageDriver.Writer(suite.ctx, filename, false)
	require.NoError(suite.T(), err)
	written, err := io.Copy(writer, io.TeeReader(contents, checksum))
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), fileSize, written)

	err = writer.Commit()
	require.NoError(suite.T(), err)
	err = writer.Close()
	require.NoError(suite.T(), err)

	reader, err := suite.StorageDriver.Reader(suite.ctx, filename, 0)
	require.NoError(suite.T(), err)
	defer reader.Close()

	writtenChecksum := sha256.New()
	_, err = io.Copy(writtenChecksum, reader)
	require.NoError(suite.T(), err)

	require.Equal(suite.T(), checksum.Sum(nil), writtenChecksum.Sum(nil))
}

// TestReaderWithOffset tests that the appropriate data is streamed when
// reading with a given offset.
func (suite *DriverSuite) TestReaderWithOffset() {
	filename := randomPath(32)
	defer suite.deletePath(suite.T(), firstPart(filename))

	chunkSize := int64(32)

	contentsChunk1 := randomContents(chunkSize)
	contentsChunk2 := randomContents(chunkSize)
	contentsChunk3 := randomContents(chunkSize)

	err := suite.StorageDriver.PutContent(suite.ctx, filename, append(append(contentsChunk1, contentsChunk2...), contentsChunk3...))
	require.NoError(suite.T(), err)

	reader, err := suite.StorageDriver.Reader(suite.ctx, filename, 0)
	require.NoError(suite.T(), err)
	defer reader.Close()

	readContents, err := io.ReadAll(reader)
	require.NoError(suite.T(), err)

	require.Equal(suite.T(), append(append(contentsChunk1, contentsChunk2...), contentsChunk3...), readContents)

	reader, err = suite.StorageDriver.Reader(suite.ctx, filename, chunkSize)
	require.NoError(suite.T(), err)
	defer reader.Close()

	readContents, err = io.ReadAll(reader)
	require.NoError(suite.T(), err)

	require.Equal(suite.T(), append(contentsChunk2, contentsChunk3...), readContents)

	reader, err = suite.StorageDriver.Reader(suite.ctx, filename, chunkSize*2)
	require.NoError(suite.T(), err)
	defer reader.Close()

	readContents, err = io.ReadAll(reader)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), contentsChunk3, readContents)

	// Ensure we get invalid offset for negative offsets.
	reader, err = suite.StorageDriver.Reader(suite.ctx, filename, -1)
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.InvalidOffsetError{
		DriverName: suite.StorageDriver.Name(),
		Path:       filename,
		Offset:     -1,
	})
	require.Nil(suite.T(), reader)

	// Read past the end of the content and make sure we get a reader that
	// returns 0 bytes and io.EOF
	reader, err = suite.StorageDriver.Reader(suite.ctx, filename, chunkSize*3)
	require.NoError(suite.T(), err)
	defer reader.Close()

	buf := make([]byte, chunkSize)
	n, err := reader.Read(buf)
	require.ErrorIs(suite.T(), err, io.EOF)
	require.Zero(suite.T(), n)

	// Check the N-1 boundary condition, ensuring we get 1 byte then io.EOF.
	reader, err = suite.StorageDriver.Reader(suite.ctx, filename, chunkSize*3-1)
	require.NoError(suite.T(), err)
	defer reader.Close()

	n, err = reader.Read(buf)
	require.Equal(suite.T(), 1, n)

	// We don't care whether the io.EOF comes on the this read or the first
	// zero read, but the only error acceptable here is io.EOF.
	if err != nil {
		require.ErrorIs(suite.T(), err, io.EOF)
	}

	// Any more reads should result in zero bytes and io.EOF
	n, err = reader.Read(buf)
	assert.Zero(suite.T(), n)
	assert.ErrorIs(suite.T(), err, io.EOF)
}

// TestContinueStreamAppendLarge tests that a stream write can be appended to without
// corrupting the data with a large chunk size.
func (suite *DriverSuite) TestContinueStreamAppendLarge() {
	suite.testContinueStreamAppend(suite.T(), int64(10*1024*1024))
}

// TestContinueStreamAppendSmall is the same as TestContinueStreamAppendLarge, but only
// with a tiny chunk size in order to test corner cases for some cloud storage drivers.
func (suite *DriverSuite) TestContinueStreamAppendSmall() {
	suite.testContinueStreamAppend(suite.T(), int64(32))
}

func (suite *DriverSuite) testContinueStreamAppend(t *testing.T, chunkSize int64) {
	filename := randomPath(32)
	defer suite.deletePath(t, firstPart(filename))

	contentsChunk1 := randomContents(chunkSize)
	contentsChunk2 := randomContents(chunkSize)
	contentsChunk3 := randomContents(chunkSize)

	fullContents := append(append(contentsChunk1, contentsChunk2...), contentsChunk3...)

	writer, err := suite.StorageDriver.Writer(suite.ctx, filename, false)
	require.NoError(t, err)
	nn, err := io.Copy(writer, bytes.NewReader(contentsChunk1))
	require.NoError(t, err)
	require.Equal(t, int64(len(contentsChunk1)), nn)

	err = writer.Close()
	require.NoError(t, err)

	curSize := writer.Size()
	require.Equal(t, int64(len(contentsChunk1)), curSize)

	writer, err = suite.StorageDriver.Writer(suite.ctx, filename, true)
	require.NoError(t, err)
	require.Equal(t, curSize, writer.Size())

	nn, err = io.Copy(writer, bytes.NewReader(contentsChunk2))
	require.NoError(t, err)
	require.Equal(t, int64(len(contentsChunk2)), nn)

	err = writer.Close()
	require.NoError(t, err)

	curSize = writer.Size()
	require.Equal(t, 2*chunkSize, curSize)

	writer, err = suite.StorageDriver.Writer(suite.ctx, filename, true)
	require.NoError(t, err)
	require.Equal(t, curSize, writer.Size())

	nn, err = io.Copy(writer, bytes.NewReader(fullContents[curSize:]))
	require.NoError(t, err)
	require.Equal(t, int64(len(fullContents[curSize:])), nn)

	err = writer.Commit()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	received, err := suite.StorageDriver.GetContent(suite.ctx, filename)
	require.NoError(t, err)
	require.Equal(t, fullContents, received)
}

// TestReadNonexistentStream tests that reading a stream for a nonexistent path
// fails.
func (suite *DriverSuite) TestReadNonexistentStream() {
	filename := randomPath(32)

	_, err := suite.StorageDriver.Reader(suite.ctx, filename, 0)
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       filename,
	})

	_, err = suite.StorageDriver.Reader(suite.ctx, filename, 64)
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       filename,
	})
}

// TestList1File tests the validity of List calls for 1 file.
func (suite *DriverSuite) TestList1File() {
	suite.testList(suite.T(), 1)
}

// TestList1200Files tests the validity of List calls for 1200 files.
func (suite *DriverSuite) TestList1200Files() {
	suite.testList(suite.T(), 1200)
}

// testList checks the returned list of keys after populating a directory tree.
func (suite *DriverSuite) testList(t *testing.T, numFiles int) {
	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer suite.deletePath(t, rootDirectory)

	doesnotexist := path.Join(rootDirectory, "nonexistent")
	_, err := suite.StorageDriver.List(suite.ctx, doesnotexist)
	require.ErrorIs(t, err, storagedriver.PathNotFoundError{
		Path:       doesnotexist,
		DriverName: suite.StorageDriver.Name(),
	})

	parentDirectory := rootDirectory + "/" + randomFilenameRange(8, 8)
	childFiles := make([]string, numFiles)
	for i := range childFiles {
		childFile := parentDirectory + "/" + randomFilenameRange(8, 8)
		childFiles[i] = childFile
		err := suite.StorageDriver.PutContent(suite.ctx, childFile, randomContents(8))
		require.NoError(t, err)
	}
	sort.Strings(childFiles)

	keys, err := suite.StorageDriver.List(suite.ctx, "/")
	require.NoError(t, err)
	require.Equal(t, []string{rootDirectory}, keys)

	keys, err = suite.StorageDriver.List(suite.ctx, rootDirectory)
	require.NoError(t, err)
	require.Equal(t, []string{parentDirectory}, keys)

	keys, err = suite.StorageDriver.List(suite.ctx, parentDirectory)
	require.NoError(t, err)

	sort.Strings(keys)
	require.Equal(t, childFiles, keys)

	// A few checks to add here (check out #819 for more discussion on this):
	// 1. Ensure that all paths are absolute.
	// 2. Ensure that listings only include direct children.
	// 3. Ensure that we only respond to directory listings that end with a slash (maybe?).
}

// TestMove checks that a moved object no longer exists at the source path and
// does exist at the destination.
func (suite *DriverSuite) TestMove() {
	contents := randomContents(32)
	sourcePath := randomPath(32)
	destPath := randomPath(32)

	defer suite.deletePath(suite.T(), firstPart(sourcePath))
	defer suite.deletePath(suite.T(), firstPart(destPath))

	err := suite.StorageDriver.PutContent(suite.ctx, sourcePath, contents)
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.Move(suite.ctx, sourcePath, destPath)
	require.NoError(suite.T(), err)

	received, err := suite.StorageDriver.GetContent(suite.ctx, destPath)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), contents, received)

	_, err = suite.StorageDriver.GetContent(suite.ctx, sourcePath)
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       sourcePath,
	})
}

// TestMoveOverwrite checks that a moved object no longer exists at the source
// path and overwrites the contents at the destination.
func (suite *DriverSuite) TestMoveOverwrite() {
	sourcePath := randomPath(32)
	destPath := randomPath(32)
	sourceContents := randomContents(32)
	destContents := randomContents(64)

	defer suite.deletePath(suite.T(), firstPart(sourcePath))
	defer suite.deletePath(suite.T(), firstPart(destPath))

	err := suite.StorageDriver.PutContent(suite.ctx, sourcePath, sourceContents)
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.PutContent(suite.ctx, destPath, destContents)
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.Move(suite.ctx, sourcePath, destPath)
	require.NoError(suite.T(), err)

	received, err := suite.StorageDriver.GetContent(suite.ctx, destPath)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), sourceContents, received)

	_, err = suite.StorageDriver.GetContent(suite.ctx, sourcePath)
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       sourcePath,
	})
}

// TestMoveNonexistent checks that moving a nonexistent key fails and does not
// delete the data at the destination path.
func (suite *DriverSuite) TestMoveNonexistent() {
	contents := randomContents(32)
	sourcePath := randomPath(32)
	destPath := randomPath(32)

	defer suite.deletePath(suite.T(), firstPart(destPath))

	err := suite.StorageDriver.PutContent(suite.ctx, destPath, contents)
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.Move(suite.ctx, sourcePath, destPath)
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       sourcePath,
	})

	received, err := suite.StorageDriver.GetContent(suite.ctx, destPath)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), contents, received)
}

// TestMoveInvalid provides various checks for invalid moves.
func (suite *DriverSuite) TestMoveInvalid() {
	contents := randomContents(32)

	// Create a regular file.
	err := suite.StorageDriver.PutContent(suite.ctx, "/notadir", contents)
	require.NoError(suite.T(), err)
	defer suite.deletePath(suite.T(), "/notadir")

	// Now try to move a non-existent file under it.
	err = suite.StorageDriver.Move(suite.ctx, "/notadir/foo", "/notadir/bar")
	require.Error(suite.T(), err)
}

// TestDelete checks that the delete operation removes data from the storage
// driver
func (suite *DriverSuite) TestDelete() {
	filename := randomPath(32)
	contents := randomContents(32)

	defer suite.deletePath(suite.T(), firstPart(filename))

	err := suite.StorageDriver.PutContent(suite.ctx, filename, contents)
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.Delete(suite.ctx, filename)
	require.NoError(suite.T(), err)

	_, err = suite.StorageDriver.GetContent(suite.ctx, filename)
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       filename,
	})
}

// TestDeleteDir1File ensures the driver is able to delete all objects in a
// directory with 1 file.
func (suite *DriverSuite) TestDeleteDir1File() {
	suite.testDeleteDir(suite.T(), 1)
}

// TestDeleteDir1200Files ensures the driver is able to delete all objects in a
// directory with 1200 files.
func (suite *DriverSuite) TestDeleteDir1200Files() {
	suite.testDeleteDir(suite.T(), 1200)
}

func (suite *DriverSuite) testDeleteDir(t *testing.T, numFiles int) {
	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer suite.deletePath(t, rootDirectory)

	parentDirectory := rootDirectory + "/" + randomFilenameRange(8, 8)
	childFiles := make([]string, numFiles)
	for i := range childFiles {
		childFile := parentDirectory + "/" + randomFilenameRange(8, 8)
		childFiles[i] = childFile
		err := suite.StorageDriver.PutContent(suite.ctx, childFile, randomContents(8))
		require.NoError(t, err)
	}

	err := suite.StorageDriver.Delete(suite.ctx, parentDirectory)
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
		if _, err = suite.StorageDriver.GetContent(suite.ctx, f); err == nil {
			filesRemaining = true
			t.Logf("able to access file %d after deletion", i)
		} else {
			require.Error(t, err)
			require.ErrorIs(t, err, storagedriver.PathNotFoundError{
				DriverName: suite.StorageDriver.Name(),
				Path:       f,
			})
		}
	}

	require.False(t, filesRemaining, "Encountered files remaining after deletion")
}

// buildFiles builds a num amount of test files with a size of size under parentDir. Returns a slice with the path of
// the created files.
func (suite *DriverSuite) buildFiles(t require.TestingT, parentDir string, num, size int64) []string {
	paths := make([]string, 0, num)

	for i := int64(0); i < num; i++ {
		p := path.Join(parentDir, randomPath(32))
		paths = append(paths, p)

		err := suite.StorageDriver.PutContent(suite.ctx, p, randomContents(size))
		require.NoError(t, err)
	}

	return paths
}

// assertPathNotFound asserts that path does not exist in the storage driver filesystem.
func (suite *DriverSuite) assertPathNotFound(t require.TestingT, path ...string) {
	for _, p := range path {
		_, err := suite.StorageDriver.GetContent(suite.ctx, p)
		require.Errorf(t, err, "path %q expected to be not found but it is still there", p)
		require.ErrorIs(t, err, storagedriver.PathNotFoundError{
			DriverName: suite.StorageDriver.Name(),
			Path:       p,
		})
	}
}

// TestDeleteFiles checks that DeleteFiles removes data from the storage driver for a random (<10) number of files.
func (suite *DriverSuite) TestDeleteFiles() {
	parentDir := randomPath(8)
	defer suite.deletePath(suite.T(), firstPart(parentDir))

	/* #nosec G404 */
	blobPaths := suite.buildFiles(suite.T(), parentDir, rand.Int63n(10), 32)

	count, err := suite.StorageDriver.DeleteFiles(suite.ctx, blobPaths)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), len(blobPaths), count)

	suite.assertPathNotFound(suite.T(), blobPaths...)
}

// TestDeleteFiles is a regression test for deleting files where the file name and folder where the file resides have the same names
func (suite *DriverSuite) TestDeleteFileEqualFolderFileName() {
	parentDir := randomPath(8)
	fileName := "Maryna"
	path := path.Join(parentDir, fileName, fileName)
	defer suite.deletePath(suite.T(), firstPart(parentDir))

	err := suite.StorageDriver.PutContent(suite.ctx, path, randomContents(32))
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.Delete(suite.ctx, path)
	require.NoError(suite.T(), err)

	suite.assertPathNotFound(suite.T(), path)
}

// TestDeleteFilesNotFound checks that DeleteFiles is idempotent and doesn't return an error if a file was not found.
func (suite *DriverSuite) TestDeleteFilesNotFound() {
	parentDir := randomPath(8)
	defer suite.deletePath(suite.T(), firstPart(parentDir))

	blobPaths := suite.buildFiles(suite.T(), parentDir, 5, 32)
	// delete the 1st, 3rd and last file so that they don't exist anymore
	suite.deletePath(suite.T(), blobPaths[0])
	suite.deletePath(suite.T(), blobPaths[2])
	suite.deletePath(suite.T(), blobPaths[4])

	count, err := suite.StorageDriver.DeleteFiles(suite.ctx, blobPaths)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), len(blobPaths), count)

	suite.assertPathNotFound(suite.T(), blobPaths...)
}

// benchmarkDeleteFiles benchmarks DeleteFiles for an amount of num files.
func (suite *DriverSuite) benchmarkDeleteFiles(b *testing.B, num int64) {
	parentDir := randomPath(8)
	defer suite.deletePath(b, firstPart(parentDir))

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		paths := suite.buildFiles(b, parentDir, num, 32)
		b.StartTimer()
		count, err := suite.StorageDriver.DeleteFiles(suite.ctx, paths)
		b.StopTimer()
		require.NoError(b, err)
		require.Len(b, paths, count)
		suite.assertPathNotFound(b, paths...)
	}
}

// BenchmarkDeleteFiles1File benchmarks DeleteFiles for 1 file.
func (suite *DriverSuite) BenchmarkDeleteFiles1File(b *testing.B) {
	suite.benchmarkDeleteFiles(b, 1)
}

// BenchmarkDeleteFiles100Files benchmarks DeleteFiles for 100 files.
func (suite *DriverSuite) BenchmarkDeleteFiles100Files(b *testing.B) {
	suite.benchmarkDeleteFiles(b, 100)
}

// TestURLFor checks that the URLFor method functions properly, but only if it
// is implemented
func (suite *DriverSuite) TestURLFor() {
	filename := randomPath(32)
	contents := randomContents(32)

	defer suite.deletePath(suite.T(), firstPart(filename))

	err := suite.StorageDriver.PutContent(suite.ctx, filename, contents)
	require.NoError(suite.T(), err)

	url, err := suite.StorageDriver.URLFor(suite.ctx, filename, nil)
	if errors.As(err, new(storagedriver.ErrUnsupportedMethod)) {
		return
	}
	require.NoError(suite.T(), err)

	response, err := http.Get(url)
	require.NoError(suite.T(), err)
	defer response.Body.Close()

	read, err := io.ReadAll(response.Body)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), contents, read)

	url, err = suite.StorageDriver.URLFor(suite.ctx, filename, map[string]interface{}{"method": http.MethodHead})
	if errors.As(err, new(storagedriver.ErrUnsupportedMethod)) {
		return
	}
	require.NoError(suite.T(), err)

	response, err = http.Head(url)
	require.NoError(suite.T(), err)
	response.Body.Close()
	assert.Equal(suite.T(), http.StatusOK, response.StatusCode)
	assert.Equal(suite.T(), int64(32), response.ContentLength)
}

// TestDeleteNonexistent checks that removing a nonexistent key fails.
func (suite *DriverSuite) TestDeleteNonexistent() {
	filename := randomPath(32)
	err := suite.StorageDriver.Delete(suite.ctx, filename)
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       filename,
	})
}

// TestDeleteFolder checks that deleting a folder removes all child elements.
func (suite *DriverSuite) TestDeleteFolder() {
	dirname := randomPath(32)
	filename1 := randomPath(32)
	filename2 := randomPath(32)
	filename3 := randomPath(32)
	contents := randomContents(32)

	defer suite.deletePath(suite.T(), firstPart(dirname))

	err := suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, filename1), contents)
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, filename2), contents)
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, filename3), contents)
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.Delete(suite.ctx, path.Join(dirname, filename1))
	require.NoError(suite.T(), err)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename1))
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       path.Join(dirname, filename1),
	})

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename2))
	require.NoError(suite.T(), err)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename3))
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.Delete(suite.ctx, dirname)
	require.NoError(suite.T(), err)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename1))
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       path.Join(dirname, filename1),
	})

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename2))
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       path.Join(dirname, filename2),
	})

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename3))
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       path.Join(dirname, filename3),
	})
}

// TestDeleteOnlyDeletesSubpaths checks that deleting path A does not
// delete path B when A is a prefix of B but B is not a subpath of A (so that
// deleting "/a" does not delete "/ab").  This matters for services like S3 that
// do not implement directories.
func (suite *DriverSuite) TestDeleteOnlyDeletesSubpaths() {
	dirname := randomPath(32)
	filename := randomPath(32)
	contents := randomContents(32)

	defer suite.deletePath(suite.T(), firstPart(dirname))

	err := suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, filename), contents)
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, filename+"suffix"), contents)
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, dirname, filename), contents)
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, dirname+"suffix", filename), contents)
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.Delete(suite.ctx, path.Join(dirname, filename))
	require.NoError(suite.T(), err)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename))
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       path.Join(dirname, filename),
	})

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename+"suffix"))
	require.NoError(suite.T(), err)

	err = suite.StorageDriver.Delete(suite.ctx, path.Join(dirname, dirname))
	require.NoError(suite.T(), err)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, dirname, filename))
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       path.Join(dirname, dirname, filename),
	})

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, dirname+"suffix", filename))
	require.NoError(suite.T(), err)
}

// TestStatCall runs verifies the implementation of the storagedriver's Stat call.
func (suite *DriverSuite) TestStatCall() {
	content := randomContents(4096)
	dirPath := randomPath(32)
	fileName := randomFilename(32)
	filePath := path.Join(dirPath, fileName)

	defer suite.deletePath(suite.T(), firstPart(dirPath))

	// Call on non-existent file/dir, check error.
	fi, err := suite.StorageDriver.Stat(suite.ctx, dirPath)
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       dirPath,
	})
	require.Nil(suite.T(), fi)

	fi, err = suite.StorageDriver.Stat(suite.ctx, filePath)
	require.Error(suite.T(), err)
	require.ErrorIs(suite.T(), err, storagedriver.PathNotFoundError{
		DriverName: suite.StorageDriver.Name(),
		Path:       filePath,
	})
	require.Nil(suite.T(), fi)

	err = suite.StorageDriver.PutContent(suite.ctx, filePath, content)
	require.NoError(suite.T(), err)

	// Call on regular file, check results
	fi, err = suite.StorageDriver.Stat(suite.ctx, filePath)
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), fi)
	require.Equal(suite.T(), filePath, fi.Path())
	require.Equal(suite.T(), int64(len(content)), fi.Size())
	require.False(suite.T(), fi.IsDir())
	createdTime := fi.ModTime()

	// Sleep and modify the file
	time.Sleep(time.Second * 10)
	content = randomContents(4096)
	err = suite.StorageDriver.PutContent(suite.ctx, filePath, content)
	require.NoError(suite.T(), err)
	fi, err = suite.StorageDriver.Stat(suite.ctx, filePath)
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), fi)
	time.Sleep(time.Second * 5) // allow changes to propagate (eventual consistency)

	// Check if the modification time is after the creation time.
	// In case of cloud storage services, storage frontend nodes might have
	// time drift between them, however that should be solved with sleeping
	// before update.
	modTime := fi.ModTime()
	require.Greaterf(
		suite.T(),
		modTime,
		createdTime,
		"modtime (%s) is before the creation time (%s)", modTime, createdTime,
	)

	// Call on directory (do not check ModTime as dirs don't need to support it)
	fi, err = suite.StorageDriver.Stat(suite.ctx, dirPath)
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), fi)
	assert.Equal(suite.T(), dirPath, fi.Path())
	assert.Zero(suite.T(), fi.Size())
	assert.True(suite.T(), fi.IsDir())
}

// TestPutContentMultipleTimes checks that if storage driver can overwrite the content
// in the subsequent puts. Validates that PutContent does not have to work
// with an offset like Writer does and overwrites the file entirely
// rather than writing the data to the [0,len(data)) of the file.
func (suite *DriverSuite) TestPutContentMultipleTimes() {
	filename := randomPath(32)
	contents := randomContents(4096)

	defer suite.deletePath(suite.T(), firstPart(filename))
	err := suite.StorageDriver.PutContent(suite.ctx, filename, contents)
	require.NoError(suite.T(), err)

	contents = randomContents(2048) // upload a different, smaller file
	err = suite.StorageDriver.PutContent(suite.ctx, filename, contents)
	require.NoError(suite.T(), err)

	readContents, err := suite.StorageDriver.GetContent(suite.ctx, filename)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), contents, readContents)
}

// TestConcurrentStreamReads checks that multiple clients can safely read from
// the same file simultaneously with various offsets.
func (suite *DriverSuite) TestConcurrentStreamReads() {
	var filesize int64 = 128 * 1024 * 1024

	if testing.Short() {
		filesize = 10 * 1024 * 1024
		suite.T().Log("Reducing file size to 10MB for short mode")
	}

	filename := randomPath(32)
	contents := randomContents(filesize)

	defer suite.deletePath(suite.T(), firstPart(filename))

	err := suite.StorageDriver.PutContent(suite.ctx, filename, contents)
	require.NoError(suite.T(), err)

	var wg sync.WaitGroup

	readContents := func() {
		defer wg.Done()
		/* #nosec G404 */
		offset := rand.Int63n(int64(len(contents)))
		reader, err := suite.StorageDriver.Reader(suite.ctx, filename, offset)
		require.NoError(suite.T(), err)
		defer reader.Close()

		readContents, err := io.ReadAll(reader)
		require.NoError(suite.T(), err)
		require.Equal(suite.T(), contents[offset:], readContents)
	}

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go readContents()
	}
	wg.Wait()
}

// TestConcurrentFileStreams checks that multiple *os.File objects can be passed
// in to Writer concurrently without hanging.
func (suite *DriverSuite) TestConcurrentFileStreams() {
	numStreams := 32

	if testing.Short() {
		numStreams = 8
		suite.T().Log("Reducing number of streams to 8 for short mode")
	}

	var wg sync.WaitGroup

	testStream := func(size int64) {
		defer wg.Done()
		suite.testFileStreams(suite.T(), size)
	}

	wg.Add(numStreams)
	for i := numStreams; i > 0; i-- {
		go testStream(int64(numStreams) * 1024 * 1024)
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
// 	filename := randomPath(32)
// 	defer suite.deletePath(suite.T(), firstPart(filename))
//
// 	var offset int64
// 	var misswrites int
// 	var chunkSize int64 = 32
//
// 	for i := 0; i < 1024; i++ {
// 		contents := randomContents(chunkSize)
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

// TestWalkParallel ensures that all files are visted by WalkParallel.
func (suite *DriverSuite) TestWalkParallel() {
	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer suite.deletePath(suite.T(), rootDirectory)

	numWantedFiles := 10
	wantedFiles := randomBranchingFiles(rootDirectory, numWantedFiles)
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
		err := suite.StorageDriver.PutContent(suite.ctx, wantedFiles[i], randomContents(int64(8+rand.Intn(8))))
		require.NoError(suite.T(), err)
	}

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

	err := suite.StorageDriver.WalkParallel(suite.ctx, rootDirectory, func(fInfo storagedriver.FileInfo) error {
		// Use append here to prevent a panic if walk finds more than we expect.
		if fInfo.IsDir() {
			dChan <- fInfo.Path()
		} else {
			fChan <- fInfo.Path()
		}
		return nil
	})
	require.NoError(suite.T(), err)

	close(fChan)
	close(dChan)

	wg.Wait()

	sort.Strings(actualFiles)
	sort.Strings(wantedFiles)
	require.Equal(suite.T(), wantedFiles, actualFiles)

	// Convert from a set of wanted directories into a slice.
	wantedDirectories := make([]string, len(wantedDirectoriesSet))

	var i int
	for k := range wantedDirectoriesSet {
		wantedDirectories[i] = k
		i++
	}

	require.ElementsMatch(suite.T(), wantedDirectories, actualDirectories)
}

// TestWalkParallelError ensures that walk reports WalkFn errors.
func (suite *DriverSuite) TestWalkParallelError() {
	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer suite.deletePath(suite.T(), rootDirectory)

	wantedFiles := randomBranchingFiles(rootDirectory, 100)

	for _, file := range wantedFiles {
		/* #nosec G404 */
		err := suite.StorageDriver.PutContent(suite.ctx, file, randomContents(int64(8+rand.Intn(8))))
		require.NoError(suite.T(), err)
	}

	innerErr := errors.New("walk: expected test error")
	errorFile := wantedFiles[0]

	err := suite.StorageDriver.WalkParallel(suite.ctx, rootDirectory, func(fInfo storagedriver.FileInfo) error {
		if fInfo.Path() == errorFile {
			return innerErr
		}

		return nil
	})

	// Drivers may or may not return a multierror here, check that the innerError
	// is present in the error returned by walk.
	require.ErrorContains(suite.T(), err, innerErr.Error())
}

// TestWalkParallelStopsProcessingOnError ensures that walk stops processing when an error is encountered.
func (suite *DriverSuite) TestWalkParallelStopsProcessingOnError() {
	d := suite.StorageDriver.Name()
	switch d {
	case "filesystem", "azure":
		suite.T().Skip(fmt.Sprintf("%s driver does not support true WalkParallel", d))
	case "gcs":
		parallelWalk := os.Getenv("GCS_PARALLEL_WALK")
		var parallelWalkBool bool
		var err error
		if parallelWalk != "" {
			parallelWalkBool, err = strconv.ParseBool(parallelWalk)
			require.NoError(suite.T(), err)
		}

		if !parallelWalkBool || parallelWalk == "" {
			suite.T().Skip(fmt.Sprintf("%s driver is not configured with parallelwalk", d))
		}
	}

	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer suite.deletePath(suite.T(), rootDirectory)

	numWantedFiles := 1000
	wantedFiles := randomBranchingFiles(rootDirectory, numWantedFiles)

	// Add a file right under the root directory, so that processing is stopped
	// early in the walk cycle.
	errorFile := filepath.Join(rootDirectory, randomFilenameRange(8, 8))
	wantedFiles = append(wantedFiles, errorFile)

	for _, file := range wantedFiles {
		/* #nosec G404 */
		err := suite.StorageDriver.PutContent(suite.ctx, file, randomContents(int64(8+rand.Intn(8))))
		require.NoError(suite.T(), err)
	}

	processingTime := time.Second * 1
	// Rough limit that should scale with longer or shorter processing times. Shorter than full uncancled runtime.
	limit := time.Second * time.Duration(int64(processingTime)*4)

	start := time.Now()

	suite.StorageDriver.WalkParallel(suite.ctx, rootDirectory, func(fInfo storagedriver.FileInfo) error {
		if fInfo.Path() == errorFile {
			return errors.New("")
		}

		// Imitate workload.
		time.Sleep(processingTime)

		return nil
	})

	end := time.Now()

	require.Less(suite.T(), end.Sub(start), limit)
}

// BenchmarkPutGetEmptyFiles benchmarks PutContent/GetContent for 0B files
func (suite *DriverSuite) BenchmarkPutGetEmptyFiles(b *testing.B) {
	suite.benchmarkPutGetFiles(b, 0)
}

// BenchmarkPutGet1KBFiles benchmarks PutContent/GetContent for 1KB files
func (suite *DriverSuite) BenchmarkPutGet1KBFiles(b *testing.B) {
	suite.benchmarkPutGetFiles(b, 1024)
}

// BenchmarkPutGet1MBFiles benchmarks PutContent/GetContent for 1MB files
func (suite *DriverSuite) BenchmarkPutGet1MBFiles(b *testing.B) {
	suite.benchmarkPutGetFiles(b, 1024*1024)
}

// BenchmarkPutGet1GBFiles benchmarks PutContent/GetContent for 1GB files
func (suite *DriverSuite) BenchmarkPutGet1GBFiles(b *testing.B) {
	suite.benchmarkPutGetFiles(b, 1024*1024*1024)
}

func (suite *DriverSuite) benchmarkPutGetFiles(b *testing.B, size int64) {
	b.SetBytes(size)
	parentDir := randomPath(8)
	defer func() {
		b.StopTimer()
		suite.StorageDriver.Delete(suite.ctx, firstPart(parentDir))
	}()

	for i := 0; i < b.N; i++ {
		filename := path.Join(parentDir, randomPath(32))
		err := suite.StorageDriver.PutContent(suite.ctx, filename, randomContents(size))
		require.NoError(b, err)

		_, err = suite.StorageDriver.GetContent(suite.ctx, filename)
		require.NoError(b, err)
	}
}

// BenchmarkStreamEmptyFiles benchmarks Writer/Reader for 0B files
func (suite *DriverSuite) BenchmarkStreamEmptyFiles(b *testing.B) {
	if suite.StorageDriver.Name() == "s3aws" {
		suite.T().Skip("S3 multipart uploads require at least 1 chunk (>0B)")
	}
	suite.benchmarkStreamFiles(b, 0)
}

// BenchmarkStream1KBFiles benchmarks Writer/Reader for 1KB files
func (suite *DriverSuite) BenchmarkStream1KBFiles(b *testing.B) {
	suite.benchmarkStreamFiles(b, 1024)
}

// BenchmarkStream1MBFiles benchmarks Writer/Reader for 1MB files
func (suite *DriverSuite) BenchmarkStream1MBFiles(b *testing.B) {
	suite.benchmarkStreamFiles(b, 1024*1024)
}

// BenchmarkStream1GBFiles benchmarks Writer/Reader for 1GB files
func (suite *DriverSuite) BenchmarkStream1GBFiles(b *testing.B) {
	suite.benchmarkStreamFiles(b, 1024*1024*1024)
}

func (suite *DriverSuite) benchmarkStreamFiles(b *testing.B, size int64) {
	b.SetBytes(size)
	parentDir := randomPath(8)
	defer func() {
		b.StopTimer()
		suite.StorageDriver.Delete(suite.ctx, firstPart(parentDir))
	}()

	for i := 0; i < b.N; i++ {
		filename := path.Join(parentDir, randomPath(32))
		writer, err := suite.StorageDriver.Writer(suite.ctx, filename, false)
		require.NoError(b, err)
		written, err := io.Copy(writer, bytes.NewReader(randomContents(size)))
		require.NoError(b, err)
		require.Equal(b, size, written)

		err = writer.Commit()
		require.NoError(b, err)
		err = writer.Close()
		require.NoError(b, err)

		rc, err := suite.StorageDriver.Reader(suite.ctx, filename, 0)
		require.NoError(b, err)
		rc.Close()
	}
}

// BenchmarkList5Files benchmarks List for 5 small files
func (suite *DriverSuite) BenchmarkList5Files(b *testing.B) {
	suite.benchmarkListFiles(b, 5)
}

// BenchmarkList50Files benchmarks List for 50 small files
func (suite *DriverSuite) BenchmarkList50Files(b *testing.B) {
	suite.benchmarkListFiles(b, 50)
}

func (suite *DriverSuite) benchmarkListFiles(b *testing.B, numFiles int64) {
	parentDir := randomPath(8)
	defer func() {
		b.StopTimer()
		suite.StorageDriver.Delete(suite.ctx, firstPart(parentDir))
	}()

	for i := int64(0); i < numFiles; i++ {
		err := suite.StorageDriver.PutContent(suite.ctx, path.Join(parentDir, randomPath(32)), nil)
		require.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		files, err := suite.StorageDriver.List(suite.ctx, parentDir)
		require.NoError(b, err)
		require.Equal(b, numFiles, int64(len(files)))
	}
}

// BenchmarkDelete5Files benchmarks Delete for 5 small files
func (suite *DriverSuite) BenchmarkDelete5Files(b *testing.B) {
	suite.benchmarkDelete(b, 5)
}

// BenchmarkDelete50Files benchmarks Delete for 50 small files
func (suite *DriverSuite) BenchmarkDelete50Files(b *testing.B) {
	suite.benchmarkDelete(b, 50)
}

func (suite *DriverSuite) benchmarkDelete(b *testing.B, numFiles int64) {
	for i := 0; i < b.N; i++ {
		parentDir := randomPath(8)
		defer suite.deletePath(b, firstPart(parentDir))

		b.StopTimer()
		for j := int64(0); j < numFiles; j++ {
			err := suite.StorageDriver.PutContent(suite.ctx, path.Join(parentDir, randomPath(32)), nil)
			require.NoError(b, err)
		}
		b.StartTimer()

		// This is the operation we're benchmarking
		err := suite.StorageDriver.Delete(suite.ctx, firstPart(parentDir))
		require.NoError(b, err)
	}
}

// BenchmarkWalkParallelNop10Files benchmarks WalkParallel with a Nop function that visits 10 files
func (suite *DriverSuite) BenchmarkWalkParallelNop10Files(b *testing.B) {
	suite.benchmarkWalkParallel(b, 10, func(fInfo storagedriver.FileInfo) error {
		return nil
	})
}

// BenchmarkWalkParallelNop500Files benchmarks WalkParallel with a Nop function that visits 500 files
func (suite *DriverSuite) BenchmarkWalkParallelNop500Files(b *testing.B) {
	suite.benchmarkWalkParallel(b, 500, func(fInfo storagedriver.FileInfo) error {
		return nil
	})
}

func (suite *DriverSuite) benchmarkWalkParallel(b *testing.B, numFiles int, f storagedriver.WalkFn) {
	for i := 0; i < b.N; i++ {
		rootDirectory := "/" + randomFilenameRange(8, 8)
		defer suite.deletePath(b, rootDirectory)

		b.StopTimer()

		wantedFiles := randomBranchingFiles(rootDirectory, numFiles)

		for i := 0; i < numFiles; i++ {
			/* #nosec G404 */
			err := suite.StorageDriver.PutContent(suite.ctx, wantedFiles[i], randomContents(int64(8+rand.Intn(8))))
			require.NoError(b, err)
		}

		b.StartTimer()

		err := suite.StorageDriver.WalkParallel(suite.ctx, rootDirectory, f)
		require.NoError(b, err)
	}
}

func (suite *DriverSuite) createRegistry(t require.TestingT, options ...storage.RegistryOption) distribution.Namespace {
	k, err := libtrust.GenerateECP256PrivateKey()
	require.NoError(t, err)

	options = append([]storage.RegistryOption{storage.EnableDelete, storage.Schema1SigningKey(k), storage.EnableSchema1}, options...)
	registry, err := storage.NewRegistry(suite.ctx, suite.StorageDriver, options...)
	require.NoError(t, err, "Failed to construct namespace")
	return registry
}

func (suite *DriverSuite) makeRepository(t require.TestingT, registry distribution.Namespace, name string) distribution.Repository {
	named, err := reference.WithName(name)
	require.NoErrorf(t, err, "Failed to parse name %s", name)

	repo, err := registry.Repository(suite.ctx, named)
	require.NoError(t, err, "Failed to construct repository")
	return repo
}

// BenchmarkMarkAndSweep10ImagesKeepUntagged uploads 10 images, deletes half
// and runs garbage collection on the registry without removing untaged images.
func (suite *DriverSuite) BenchmarkMarkAndSweep10ImagesKeepUntagged(b *testing.B) {
	suite.benchmarkMarkAndSweep(b, 10, false)
}

// BenchmarkMarkAndSweep50ImagesKeepUntagged uploads 50 images, deletes half
// and runs garbage collection on the registry without removing untaged images.
func (suite *DriverSuite) BenchmarkMarkAndSweep50ImagesKeepUntagged(b *testing.B) {
	suite.benchmarkMarkAndSweep(b, 50, false)
}

func (suite *DriverSuite) benchmarkMarkAndSweep(b *testing.B, numImages int, removeUntagged bool) {
	// Setup for this test takes a long time, even with small numbers of images,
	// so keep the skip logic here in the sub test.
	if testing.Short() {
		b.Skip("Skipping test in short mode")
	}

	defer suite.deletePath(b, firstPart("docker/"))

	for n := 0; n < b.N; n++ {
		b.StopTimer()

		registry := suite.createRegistry(b)
		repo := suite.makeRepository(b, registry, fmt.Sprintf("benchmarks-repo-%d", n))

		manifests, err := repo.Manifests(suite.ctx)
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
			manifests.Delete(suite.ctx, images[i].ManifestDigest)
		}

		b.StartTimer()

		// Run GC
		err = storage.MarkAndSweep(context.Background(), suite.StorageDriver, registry, storage.GCOpts{
			DryRun:         false,
			RemoveUntagged: removeUntagged,
		})
		require.NoError(b, err)
	}
}

func (suite *DriverSuite) buildBlobs(t require.TestingT, repo distribution.Repository, n int) []digest.Digest {
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
func (suite *DriverSuite) TestRemoveBlob() {
	defer suite.deletePath(suite.T(), firstPart("docker/"))

	registry := suite.createRegistry(suite.T())
	repo := suite.makeRepository(suite.T(), registry, randomFilename(5))
	v := storage.NewVacuum(suite.StorageDriver)

	// build two blobs, one more than the number to delete, otherwise there will be no /docker/registry/v2/blobs path
	// for validation after delete
	blobs := suite.buildBlobs(suite.T(), repo, 2)
	blob := blobs[0]

	err := v.RemoveBlob(suite.ctx, blob)
	require.NoError(suite.T(), err)

	blobService := registry.Blobs()
	blobsLeft := newSyncDigestSet()
	err = blobService.Enumerate(suite.ctx, func(desc distribution.Descriptor) error {
		blobsLeft.add(desc.Digest)
		return nil
	})
	require.NoError(suite.T(), err, "error getting all blobs")

	assert.Equal(suite.T(), 1, blobsLeft.len())
	assert.Falsef(suite.T(), blobsLeft.contains(blob), "blob %q was not deleted", blob.String())
}

func (suite *DriverSuite) benchmarkRemoveBlob(b *testing.B, numBlobs int) {
	defer suite.deletePath(b, firstPart("docker/"))

	registry := suite.createRegistry(b)
	repo := suite.makeRepository(b, registry, randomFilename(5))
	v := storage.NewVacuum(suite.StorageDriver)

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		blobs := suite.buildBlobs(b, repo, numBlobs)
		b.StartTimer()

		for _, bl := range blobs {
			err := v.RemoveBlob(suite.ctx, bl)
			require.NoError(b, err)
		}
	}
}

// BenchmarkRemoveBlob1Blob creates 1 blob and deletes it using the storage.Vacuum.RemoveBlob method.
func (suite *DriverSuite) BenchmarkRemoveBlob1Blob(b *testing.B) {
	suite.benchmarkRemoveBlob(b, 1)
}

// BenchmarkRemoveBlob10Blobs creates 10 blobs and deletes them using the storage.Vacuum.RemoveBlob method.
func (suite *DriverSuite) BenchmarkRemoveBlob10Blobs(b *testing.B) {
	suite.benchmarkRemoveBlob(b, 10)
}

// BenchmarkRemoveBlob100Blobs creates 100 blobs and deletes them using the storage.Vacuum.RemoveBlob method.
func (suite *DriverSuite) BenchmarkRemoveBlob100Blobs(b *testing.B) {
	suite.benchmarkRemoveBlob(b, 100)
}

// TestRemoveBlobs checks that storage.Vacuum is able to delete a set of blobs in bulk.
func (suite *DriverSuite) TestRemoveBlobs() {
	defer suite.deletePath(suite.T(), firstPart("docker/"))

	registry := suite.createRegistry(suite.T())
	repo := suite.makeRepository(suite.T(), registry, randomFilename(5))
	v := storage.NewVacuum(suite.StorageDriver)

	// build some blobs and remove half of them, otherwise there will be no /docker/registry/v2/blobs path to look at
	// for validation if there are no blobs left
	blobs := suite.buildBlobs(suite.T(), repo, 4)
	blobs = blobs[:2]

	err := v.RemoveBlobs(suite.ctx, blobs)
	require.NoError(suite.T(), err)

	// assert that blobs were deleted
	blobService := registry.Blobs()
	blobsLeft := newSyncDigestSet()
	err = blobService.Enumerate(suite.ctx, func(desc distribution.Descriptor) error {
		blobsLeft.add(desc.Digest)
		return nil
	})
	require.NoError(suite.T(), err, "error getting all blobs")

	require.Equal(suite.T(), 2, blobsLeft.len())
	for _, b := range blobs {
		assert.Falsef(suite.T(), blobsLeft.contains(b), "blob %q was not deleted", b.String())
	}
}

func (suite *DriverSuite) benchmarkRemoveBlobs(b *testing.B, numBlobs int) {
	defer suite.deletePath(b, firstPart("docker/"))

	registry := suite.createRegistry(b)
	repo := suite.makeRepository(b, registry, randomFilename(5))
	v := storage.NewVacuum(suite.StorageDriver)

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		blobs := suite.buildBlobs(b, repo, numBlobs)
		b.StartTimer()

		err := v.RemoveBlobs(suite.ctx, blobs)
		assert.NoError(b, err)
	}
}

// BenchmarkRemoveBlobs1Blob creates 1 blob and deletes it using the storage.Vacuum.RemoveBlobs method.
func (suite *DriverSuite) BenchmarkRemoveBlobs1Blob(b *testing.B) {
	suite.benchmarkRemoveBlobs(b, 1)
}

// BenchmarkRemoveBlobs10Blobs creates 10 blobs and deletes them using the storage.Vacuum.RemoveBlobs method.
func (suite *DriverSuite) BenchmarkRemoveBlobs10Blobs(b *testing.B) {
	suite.benchmarkRemoveBlobs(b, 10)
}

// BenchmarkRemoveBlobs100Blobs creates 100 blobs and deletes them using the storage.Vacuum.RemoveBlobs method.
func (suite *DriverSuite) BenchmarkRemoveBlobs100Blobs(b *testing.B) {
	suite.benchmarkRemoveBlobs(b, 100)
}

// BenchmarkRemoveBlobs1000Blobs creates 1000 blobs and deletes them using the storage.Vacuum.RemoveBlobs method.
func (suite *DriverSuite) BenchmarkRemoveBlobs1000Blobs(b *testing.B) {
	suite.benchmarkRemoveBlobs(b, 1000)
}

func (suite *DriverSuite) buildManifests(t require.TestingT, repo distribution.Repository, numManifests, numTagsPerManifest int) []storage.ManifestDel {
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
			rfn := randomFilename(5)
			d := images[i].ManifestDigest
			err := repo.Tags(suite.ctx).Tag(suite.ctx, rfn, distribution.Descriptor{Digest: d})
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
func (suite *DriverSuite) TestRemoveManifests() {
	defer suite.deletePath(suite.T(), firstPart("docker/"))

	registry := suite.createRegistry(suite.T())
	repo := suite.makeRepository(suite.T(), registry, randomFilename(5))

	// build some manifests
	manifests := suite.buildManifests(suite.T(), repo, 3, 1)

	v := storage.NewVacuum(suite.StorageDriver)

	// remove all manifests except one, otherwise there will be no `_manifests/revisions` folder to look at for
	// validation (empty "folders" are not preserved)
	numToDelete := len(manifests) - 1
	toDelete := manifests[:numToDelete]

	err := v.RemoveManifests(suite.ctx, toDelete)
	require.NoError(suite.T(), err)

	// assert that toDelete manifests were actually deleted
	manifestsLeft := newSyncDigestSet()
	manifestService, err := repo.Manifests(suite.ctx)
	require.NoError(suite.T(), err, "error building manifest service")
	manifestEnumerator, ok := manifestService.(distribution.ManifestEnumerator)
	require.True(suite.T(), ok, "unable to convert ManifestService into ManifestEnumerator")
	err = manifestEnumerator.Enumerate(suite.ctx, func(dgst digest.Digest) error {
		manifestsLeft.add(dgst)
		return nil
	})
	require.NoError(suite.T(), err, "error getting all manifests")

	require.Equal(suite.T(), len(manifests)-numToDelete, manifestsLeft.len())

	for _, m := range toDelete {
		assert.Falsef(suite.T(), manifestsLeft.contains(m.Digest), "manifest %q was not deleted as expected", m.Digest)
	}
}

func (suite *DriverSuite) testRemoveManifestsPathBuild(numManifests, numTagsPerManifest int) {
	v := storage.NewVacuum(suite.StorageDriver)

	var tags []string
	for i := 0; i < numTagsPerManifest; i++ {
		tags = append(tags, "foo")
	}

	var toDelete []storage.ManifestDel
	for i := 0; i < numManifests; i++ {
		m := storage.ManifestDel{
			Name:   randomFilename(10),
			Digest: digest.FromString(randomFilename(20)),
			Tags:   tags,
		}
		toDelete = append(toDelete, m)
	}
	err := v.RemoveManifests(suite.ctx, toDelete)
	require.NoError(suite.T(), err)
}

// TestRemoveManifestsPathBuildLargeScale simulates the execution of vacuum.RemoveManifests for repositories with large
// numbers of manifests eligible for deletion. No files are created in this test, we only simulate their existence so
// that we can test and profile the execution of the path build process within vacuum.RemoveManifests. The storage
// drivers DeleteFiles method is idempotent, so no error will be raised by attempting to delete non-existing files.
// However, to avoid large number of HTTP requests against cloud storage backends, it's recommended to run this test
// against the filesystem storage backend only. For safety, the test is skipped when not using the filesystem storage
// backend. Tweak the method locally to test use cases with different sizes and/or storage drivers.
func (suite *DriverSuite) TestRemoveManifestsPathBuildLargeScale() {
	if testing.Short() {
		suite.T().Skip("Skipping test in short mode")
	}
	if suite.StorageDriver.Name() != "filesystem" {
		suite.T().Skip(fmt.Sprintf("Skipping test for the %s driver", suite.StorageDriver.Name()))
	}

	numManifests := 100
	numTagsPerManifest := 10

	suite.testRemoveManifestsPathBuild(numManifests, numTagsPerManifest)
}

func (suite *DriverSuite) benchmarkRemoveManifests(b *testing.B, numManifests, numTagsPerManifest int) {
	if testing.Short() {
		b.Skip("Skipping test in short mode")
	}

	defer suite.deletePath(b, firstPart("docker/"))

	registry := suite.createRegistry(b)
	repo := suite.makeRepository(b, registry, randomFilename(5))

	for n := 0; n < b.N; n++ {
		b.StopTimer()

		manifests := suite.buildManifests(b, repo, numManifests, numTagsPerManifest)
		v := storage.NewVacuum(suite.StorageDriver)

		b.StartTimer()

		err := v.RemoveManifests(suite.ctx, manifests)
		require.NoError(b, err)
	}
}

// BenchmarkRemoveManifests1Manifest0Tags creates 1 manifest with no tags and deletes it using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests1Manifest0Tags(b *testing.B) {
	suite.benchmarkRemoveManifests(b, 1, 0)
}

// BenchmarkRemoveManifests1Manifest1Tag creates 1 manifest with 1 tag and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests1Manifest1Tag(b *testing.B) {
	suite.benchmarkRemoveManifests(b, 1, 1)
}

// BenchmarkRemoveManifests10Manifests0TagsEach creates 10 manifests with no tags and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests10Manifests0TagsEach(b *testing.B) {
	suite.benchmarkRemoveManifests(b, 10, 0)
}

// BenchmarkRemoveManifests10Manifests1TagEach creates 10 manifests with 1 tag each and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests10Manifests1TagEach(b *testing.B) {
	suite.benchmarkRemoveManifests(b, 10, 1)
}

// BenchmarkRemoveManifests100Manifests0TagsEach creates 100 manifests with no tags and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests100Manifests0TagsEach(b *testing.B) {
	suite.benchmarkRemoveManifests(b, 100, 0)
}

// BenchmarkRemoveManifests100Manifests1TagEach creates 100 manifests with 1 tag each and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests100Manifests1TagEach(b *testing.B) {
	suite.benchmarkRemoveManifests(b, 100, 1)
}

// BenchmarkRemoveManifests100Manifests20TagsEach creates 100 manifests with 20 tags each and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests100Manifests20TagsEach(b *testing.B) {
	suite.benchmarkRemoveManifests(b, 100, 20)
}

func (suite *DriverSuite) testFileStreams(t *testing.T, size int64) {
	tf, err := os.CreateTemp("", "tf")
	require.NoError(t, err)
	defer os.Remove(tf.Name())
	defer tf.Close()

	filename := randomPath(32)
	defer suite.deletePath(t, firstPart(filename))

	contents := randomContents(size)

	_, err = tf.Write(contents)
	require.NoError(t, err)

	tf.Sync()
	tf.Seek(0, io.SeekStart)

	writer, err := suite.StorageDriver.Writer(suite.ctx, filename, false)
	require.NoError(t, err)
	nn, err := io.Copy(writer, tf)
	require.NoError(t, err)
	require.Equal(t, size, nn)

	err = writer.Commit()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	reader, err := suite.StorageDriver.Reader(suite.ctx, filename, 0)
	require.NoError(t, err)
	defer reader.Close()

	readContents, err := io.ReadAll(reader)
	require.NoError(t, err)

	require.Equal(t, contents, readContents)
}

func (suite *DriverSuite) writeReadCompare(t *testing.T, filename string, contents []byte) {
	defer suite.deletePath(t, firstPart(filename))

	err := suite.StorageDriver.PutContent(suite.ctx, filename, contents)
	require.NoError(t, err)

	readContents, err := suite.StorageDriver.GetContent(suite.ctx, filename)
	require.NoError(t, err)

	require.Equal(t, contents, readContents)
}

func (suite *DriverSuite) writeReadCompareStreams(t *testing.T, filename string, contents []byte) {
	defer suite.deletePath(t, firstPart(filename))

	writer, err := suite.StorageDriver.Writer(suite.ctx, filename, false)
	require.NoError(t, err)
	nn, err := io.Copy(writer, bytes.NewReader(contents))
	require.NoError(t, err)
	require.Equal(t, int64(len(contents)), nn)

	err = writer.Commit()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	reader, err := suite.StorageDriver.Reader(suite.ctx, filename, 0)
	require.NoError(t, err)
	defer reader.Close()

	readContents, err := io.ReadAll(reader)
	require.NoError(t, err)

	require.Equal(t, contents, readContents)
}

var (
	filenameChars  = []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	separatorChars = []byte("._-")
)

func randomPath(length int64) string {
	path := "/"
	for int64(len(path)) < length {
		/* #nosec G404 */
		chunkLength := rand.Int63n(length-int64(len(path))) + 1
		chunk := randomFilename(chunkLength)
		path += chunk
		remaining := length - int64(len(path))
		if remaining == 1 {
			path += randomFilename(1)
		} else if remaining > 1 {
			path += "/"
		}
	}
	return path
}

func randomFilename(length int64) string {
	b := make([]byte, length)
	wasSeparator := true
	for i := range b {
		/* #nosec G404 */
		if !wasSeparator && i < len(b)-1 && rand.Intn(4) == 0 {
			b[i] = separatorChars[rand.Intn(len(separatorChars))]
			wasSeparator = true
		} else {
			b[i] = filenameChars[rand.Intn(len(filenameChars))]
			wasSeparator = false
		}
	}
	return string(b)
}

// randomFilenameRange returns a random file with a length between min and max
// chars long inclusive.
func randomFilenameRange(min, max int) string {
	/* #nosec G404 */
	return randomFilename(int64(min + (rand.Intn(max + 1))))
}

// randomBranchingFiles creates n number of randomly named files at the end of
// a binary tree of randomly named directories.
func randomBranchingFiles(root string, n int) []string {
	var files []string

	subDirectory := path.Join(root, randomFilenameRange(8, 8))

	if n <= 1 {
		files = append(files, path.Join(subDirectory, randomFilenameRange(8, 8)))
		return files
	}

	half := n / 2
	remainder := n % 2

	files = append(files, randomBranchingFiles(subDirectory, half+remainder)...)
	files = append(files, randomBranchingFiles(subDirectory, half)...)

	return files
}

// randomBytes pre-allocates all of the memory sizes needed for the test. If
// anything panics while accessing randomBytes, just make this number bigger.
var randomBytes []byte

var once sync.Once

func randomContents(length int64) []byte {
	once.Do(func() {
		if testing.Short() {
			randomBytes = make([]byte, 10*1024*1024)
		} else {
			randomBytes = make([]byte, 128<<23)
		}

		/* #nosec G404*/
		_, _ = rand.Read(randomBytes) // always returns len(randomBytes) and nil error
	})

	if length > int64(len(randomBytes)) {
		message := fmt.Sprintf("out of bounds: please bump randomBytes size to at least %d bytes", length)
		panic(message)
	}

	return randomBytes[:length]
}

type randReader struct {
	r int64
	m sync.Mutex
}

func (rr *randReader) Read(p []byte) (n int, err error) {
	rr.m.Lock()
	defer rr.m.Unlock()

	toread := int64(len(p))
	if toread > rr.r {
		toread = rr.r
	}
	n = copy(p, randomContents(toread))
	rr.r -= int64(n)

	if rr.r <= 0 {
		err = io.EOF
	}

	return
}

func newRandReader(n int64) *randReader {
	return &randReader{r: n}
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
