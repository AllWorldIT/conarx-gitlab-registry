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

// SetupSuite sets up the test suite for tests.
func (s *DriverSuite) SetupSuite() {
	s.setupSuiteGeneric(s.T())
}

// SetupSuiteWithB sets up the test suite for benchmarks.
func (s *DriverSuite) SetupSuiteWithB(b *testing.B) {
	s.setupSuiteGeneric(b)
}

func (s *DriverSuite) setupSuiteGeneric(t require.TestingT) {
	driver, err := s.Constructor()
	require.NoError(t, err)
	s.StorageDriver = driver
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
	files, _ := s.StorageDriver.List(s.ctx, "/")
	require.Empty(s.T(), files, "Storage driver did not clean up properly. Offending files: %#v", files)
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
		err := s.StorageDriver.PutContent(s.ctx, filename, contents)
		// nolint: revive // defer
		defer s.deletePath(s.T(), firstPart(filename))
		require.NoError(s.T(), err)

		received, err := s.StorageDriver.GetContent(s.ctx, filename)
		require.NoError(s.T(), err)
		assert.Equal(s.T(), contents, received)
	}
}

func (s *DriverSuite) deletePath(t require.TestingT, targetPath string) {
	// NOTE(prozlach): We want to make sure that we do not do an accidental
	// retry of the Delete call, hence it is outside of the
	// require.EventuallyWithT block.
	err := s.StorageDriver.Delete(s.ctx, targetPath)
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
			paths, _ := s.StorageDriver.List(s.ctx, targetPath)
			assert.Empty(c, paths)
		},
		4200*time.Millisecond,
		1*time.Second,
	)
}

// TestInvalidPaths checks that various invalid file paths are rejected by the
// storage driver.
func (s *DriverSuite) TestInvalidPaths() {
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
		err := s.StorageDriver.PutContent(s.ctx, filename, contents)
		// only delete if file was successfully written
		if err == nil {
			// nolint: revive // defer
			defer s.deletePath(s.T(), firstPart(filename))
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
	filename := randomPath(1, 32)
	contents := []byte("a")
	s.writeReadCompare(s.T(), filename, contents)
}

// TestWriteRead2 tests a simple write-read workflow with unicode data.
func (s *DriverSuite) TestWriteRead2() {
	filename := randomPath(1, 32)
	contents := []byte("\xc3\x9f")
	s.writeReadCompare(s.T(), filename, contents)
}

// TestWriteRead3 tests a simple write-read workflow with a small string.
func (s *DriverSuite) TestWriteRead3() {
	filename := randomPath(1, 32)
	contents := randomContents(32)
	s.writeReadCompare(s.T(), filename, contents)
}

// TestWriteRead4 tests a simple write-read workflow with 1MB of data.
func (s *DriverSuite) TestWriteRead4() {
	filename := randomPath(1, 32)
	contents := randomContents(1024 * 1024)
	s.writeReadCompare(s.T(), filename, contents)
}

// TestWriteReadNonUTF8 tests that non-utf8 data may be written to the storage
// driver safely.
func (s *DriverSuite) TestWriteReadNonUTF8() {
	filename := randomPath(1, 32)
	contents := []byte{0x80, 0x80, 0x80, 0x80}
	s.writeReadCompare(s.T(), filename, contents)
}

// TestTruncate tests that putting smaller contents than an original file does
// remove the excess contents.
func (s *DriverSuite) TestTruncate() {
	filename := randomPath(1, 32)
	contents := randomContents(1024 * 1024)
	s.writeReadCompare(s.T(), filename, contents)

	contents = randomContents(1024)
	s.writeReadCompare(s.T(), filename, contents)
}

// TestReadNonexistent tests reading content from an empty path.
func (s *DriverSuite) TestReadNonexistent() {
	filename := randomPath(1, 32)
	_, err := s.StorageDriver.GetContent(s.ctx, filename)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       filename,
	})
}

// TestWriteReadStreams1 tests a simple write-read streaming workflow.
func (s *DriverSuite) TestWriteReadStreams1() {
	filename := randomPath(1, 32)
	contents := []byte("a")
	s.writeReadCompareStreams(s.T(), filename, contents)
}

// TestWriteReadStreams2 tests a simple write-read streaming workflow with
// unicode data.
func (s *DriverSuite) TestWriteReadStreams2() {
	filename := randomPath(1, 32)
	contents := []byte("\xc3\x9f")
	s.writeReadCompareStreams(s.T(), filename, contents)
}

// TestWriteReadStreams3 tests a simple write-read streaming workflow with a
// small amount of data.
func (s *DriverSuite) TestWriteReadStreams3() {
	filename := randomPath(1, 32)
	contents := randomContents(32)
	s.writeReadCompareStreams(s.T(), filename, contents)
}

// TestWriteReadStreams4 tests a simple write-read streaming workflow with 1MB
// of data.
func (s *DriverSuite) TestWriteReadStreams4() {
	filename := randomPath(1, 32)
	contents := randomContents(1024 * 1024)
	s.writeReadCompareStreams(s.T(), filename, contents)
}

// TestWriteReadStreamsNonUTF8 tests that non-utf8 data may be written to the
// storage driver safely.
func (s *DriverSuite) TestWriteReadStreamsNonUTF8() {
	filename := randomPath(1, 32)
	contents := []byte{0x80, 0x80, 0x80, 0x80}
	s.writeReadCompareStreams(s.T(), filename, contents)
}

// TestWriteReadLargeStreams tests that a 2GB file may be written to the storage
// driver safely.
func (s *DriverSuite) TestWriteReadLargeStreams() {
	filename := randomPath(1, 32)
	defer s.deletePath(s.T(), firstPart(filename))

	checksum := sha256.New()

	var fileSize int64 = 2 * 1024 * 1024 * 1024
	if testing.Short() {
		fileSize = 256 * 1024 * 1024
		s.T().Log("Reducing file size to 256MiB for short mode")
	}

	contents := newRandReader(fileSize)

	writer, err := s.StorageDriver.Writer(s.ctx, filename, false)
	require.NoError(s.T(), err)
	written, err := io.Copy(writer, io.TeeReader(contents, checksum))
	require.NoError(s.T(), err)
	require.Equal(s.T(), fileSize, written)

	err = writer.Commit()
	require.NoError(s.T(), err)
	err = writer.Close()
	require.NoError(s.T(), err)

	reader, err := s.StorageDriver.Reader(s.ctx, filename, 0)
	require.NoError(s.T(), err)
	defer reader.Close()

	writtenChecksum := sha256.New()
	_, err = io.Copy(writtenChecksum, reader)
	require.NoError(s.T(), err)

	require.Equal(s.T(), checksum.Sum(nil), writtenChecksum.Sum(nil))
}

// TestReaderWithOffset tests that the appropriate data is streamed when
// reading with a given offset.
func (s *DriverSuite) TestReaderWithOffset() {
	filename := randomPath(1, 32)
	defer s.deletePath(s.T(), firstPart(filename))

	chunkSize := int64(32)

	contentsChunk1 := randomContents(chunkSize)
	contentsChunk2 := randomContents(chunkSize)
	contentsChunk3 := randomContents(chunkSize)

	err := s.StorageDriver.PutContent(s.ctx, filename, append(append(contentsChunk1, contentsChunk2...), contentsChunk3...))
	require.NoError(s.T(), err)

	reader, err := s.StorageDriver.Reader(s.ctx, filename, 0)
	require.NoError(s.T(), err)
	defer reader.Close()

	readContents, err := io.ReadAll(reader)
	require.NoError(s.T(), err)

	require.Equal(s.T(), append(append(contentsChunk1, contentsChunk2...), contentsChunk3...), readContents)

	reader, err = s.StorageDriver.Reader(s.ctx, filename, chunkSize)
	require.NoError(s.T(), err)
	defer reader.Close()

	readContents, err = io.ReadAll(reader)
	require.NoError(s.T(), err)

	require.Equal(s.T(), append(contentsChunk2, contentsChunk3...), readContents)

	reader, err = s.StorageDriver.Reader(s.ctx, filename, chunkSize*2)
	require.NoError(s.T(), err)
	defer reader.Close()

	readContents, err = io.ReadAll(reader)
	require.NoError(s.T(), err)
	require.Equal(s.T(), contentsChunk3, readContents)

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
	s.testContinueStreamAppend(s.T(), int64(10*1024*1024))
}

// TestContinueStreamAppendSmall is the same as TestContinueStreamAppendLarge, but only
// with a tiny chunk size in order to test corner cases for some cloud storage drivers.
func (s *DriverSuite) TestContinueStreamAppendSmall() {
	s.testContinueStreamAppend(s.T(), int64(32))
}

func (s *DriverSuite) testContinueStreamAppend(t *testing.T, chunkSize int64) {
	filename := randomPath(1, 32)
	defer s.deletePath(t, firstPart(filename))

	contentsChunk1 := randomContents(chunkSize)
	contentsChunk2 := randomContents(chunkSize)
	contentsChunk3 := randomContents(chunkSize)

	fullContents := append(append(contentsChunk1, contentsChunk2...), contentsChunk3...)

	writer, err := s.StorageDriver.Writer(s.ctx, filename, false)
	require.NoError(t, err)
	nn, err := io.Copy(writer, bytes.NewReader(contentsChunk1))
	require.NoError(t, err)
	require.Equal(t, int64(len(contentsChunk1)), nn)

	err = writer.Close()
	require.NoError(t, err)

	curSize := writer.Size()
	require.Equal(t, int64(len(contentsChunk1)), curSize)

	writer, err = s.StorageDriver.Writer(s.ctx, filename, true)
	require.NoError(t, err)
	require.Equal(t, curSize, writer.Size())

	nn, err = io.Copy(writer, bytes.NewReader(contentsChunk2))
	require.NoError(t, err)
	require.Equal(t, int64(len(contentsChunk2)), nn)

	err = writer.Close()
	require.NoError(t, err)

	curSize = writer.Size()
	require.Equal(t, 2*chunkSize, curSize)

	writer, err = s.StorageDriver.Writer(s.ctx, filename, true)
	require.NoError(t, err)
	require.Equal(t, curSize, writer.Size())

	nn, err = io.Copy(writer, bytes.NewReader(fullContents[curSize:]))
	require.NoError(t, err)
	require.Equal(t, int64(len(fullContents[curSize:])), nn)

	err = writer.Commit()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	received, err := s.StorageDriver.GetContent(s.ctx, filename)
	require.NoError(t, err)
	require.Equal(t, fullContents, received)
}

// TestReadNonexistentStream tests that reading a stream for a nonexistent path
// fails.
func (s *DriverSuite) TestReadNonexistentStream() {
	filename := randomPath(1, 32)

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
	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer s.deletePath(t, rootDirectory)

	doesnotexist := path.Join(rootDirectory, "nonexistent")
	_, err := s.StorageDriver.List(s.ctx, doesnotexist)
	require.ErrorIs(t, err, storagedriver.PathNotFoundError{
		Path:       doesnotexist,
		DriverName: s.StorageDriver.Name(),
	})

	parentDirectory := rootDirectory + "/" + randomFilenameRange(8, 8)
	childFiles := make([]string, numFiles)
	for i := range childFiles {
		childFile := parentDirectory + "/" + randomFilenameRange(8, 8)
		childFiles[i] = childFile
		err := s.StorageDriver.PutContent(s.ctx, childFile, randomContents(8))
		require.NoError(t, err)
	}
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

// TestMove checks that a moved object no longer exists at the source path and
// does exist at the destination.
func (s *DriverSuite) TestMove() {
	contents := randomContents(32)
	sourcePath := randomPath(1, 32)
	destPath := randomPath(1, 32)

	defer s.deletePath(s.T(), firstPart(sourcePath))
	defer s.deletePath(s.T(), firstPart(destPath))

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

// TestMoveOverwrite checks that a moved object no longer exists at the source
// path and overwrites the contents at the destination.
func (s *DriverSuite) TestMoveOverwrite() {
	sourcePath := randomPath(1, 32)
	destPath := randomPath(1, 32)
	sourceContents := randomContents(32)
	destContents := randomContents(64)

	defer s.deletePath(s.T(), firstPart(sourcePath))
	defer s.deletePath(s.T(), firstPart(destPath))

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
	contents := randomContents(32)
	sourcePath := randomPath(1, 32)
	destPath := randomPath(1, 32)

	defer s.deletePath(s.T(), firstPart(destPath))

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
	contents := randomContents(32)

	// Create a regular file.
	err := s.StorageDriver.PutContent(s.ctx, "/notadir", contents)
	require.NoError(s.T(), err)
	defer s.deletePath(s.T(), "/notadir")

	// Now try to move a non-existent file under it.
	err = s.StorageDriver.Move(s.ctx, "/notadir/foo", "/notadir/bar")
	require.Error(s.T(), err)
}

// TestDelete checks that the delete operation removes data from the storage
// driver
func (s *DriverSuite) TestDelete() {
	filename := randomPath(1, 32)
	contents := randomContents(32)

	defer s.deletePath(s.T(), firstPart(filename))

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
	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer s.deletePath(t, rootDirectory)

	parentDirectory := rootDirectory + "/" + randomFilenameRange(8, 8)
	childFiles := make([]string, numFiles)
	for i := range childFiles {
		childFile := parentDirectory + "/" + randomFilenameRange(8, 8)
		childFiles[i] = childFile
		err := s.StorageDriver.PutContent(s.ctx, childFile, randomContents(8))
		require.NoError(t, err)
	}

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

// buildFiles builds a num amount of test files with a size of size under parentDir. Returns a slice with the path of
// the created files.
func (s *DriverSuite) buildFiles(t require.TestingT, parentDir string, num, size int64) []string {
	paths := make([]string, 0, num)

	for i := int64(0); i < num; i++ {
		p := path.Join(parentDir, randomPath(4, 32))
		paths = append(paths, p)

		err := s.StorageDriver.PutContent(s.ctx, p, randomContents(size))
		require.NoError(t, err)
	}

	return paths
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
	parentDir := randomPath(1, 8)
	defer s.deletePath(s.T(), firstPart(parentDir))

	/* #nosec G404 */
	blobPaths := s.buildFiles(s.T(), parentDir, rand.Int63n(10), 32)

	count, err := s.StorageDriver.DeleteFiles(s.ctx, blobPaths)
	require.NoError(s.T(), err)
	require.Equal(s.T(), len(blobPaths), count)

	s.assertPathNotFound(s.T(), blobPaths...)
}

// TestDeleteFileEqualFolderFileName is a regression test for deleting files
// where the file name and folder where the file resides have the same names
func (s *DriverSuite) TestDeleteFileEqualFolderFileName() {
	parentDir := randomPath(1, 8)
	fileName := "Maryna"
	p := path.Join(parentDir, fileName, fileName)
	defer s.deletePath(s.T(), firstPart(parentDir))

	err := s.StorageDriver.PutContent(s.ctx, p, randomContents(32))
	require.NoError(s.T(), err)

	err = s.StorageDriver.Delete(s.ctx, p)
	require.NoError(s.T(), err)

	s.assertPathNotFound(s.T(), p)
}

// TestDeleteFilesNotFound checks that DeleteFiles is idempotent and doesn't return an error if a file was not found.
func (s *DriverSuite) TestDeleteFilesNotFound() {
	parentDir := randomPath(1, 8)
	defer s.deletePath(s.T(), firstPart(parentDir))

	blobPaths := s.buildFiles(s.T(), parentDir, 5, 32)
	// delete the 1st, 3rd and last file so that they don't exist anymore
	s.deletePath(s.T(), blobPaths[0])
	s.deletePath(s.T(), blobPaths[2])
	s.deletePath(s.T(), blobPaths[4])

	count, err := s.StorageDriver.DeleteFiles(s.ctx, blobPaths)
	require.NoError(s.T(), err)
	require.Equal(s.T(), len(blobPaths), count)

	s.assertPathNotFound(s.T(), blobPaths...)
}

// benchmarkDeleteFiles benchmarks DeleteFiles for an amount of num files.
func (s *DriverSuite) benchmarkDeleteFiles(b *testing.B, num int64) {
	parentDir := randomPath(1, 8)
	defer s.deletePath(b, firstPart(parentDir))

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
	filename := randomPath(1, 32)
	contents := randomContents(32)

	defer s.deletePath(s.T(), firstPart(filename))

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
	filename := randomPath(1, 32)
	err := s.StorageDriver.Delete(s.ctx, filename)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       filename,
	})
}

// TestDeleteFolder checks that deleting a folder removes all child elements.
func (s *DriverSuite) TestDeleteFolder() {
	dirname := randomPath(1, 32)
	filename1 := randomPath(1, 32)
	filename2 := randomPath(1, 32)
	filename3 := randomPath(1, 32)
	contents := randomContents(32)

	defer s.deletePath(s.T(), firstPart(dirname))

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
	dirname := randomPath(1, 32)
	filename := randomPath(1, 32)
	contents := randomContents(32)

	defer s.deletePath(s.T(), firstPart(dirname))

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
	content := randomContents(4096)
	dirPath := randomPath(1, 32)
	fileName := randomFilename(32)
	filePath := path.Join(dirPath, fileName)

	defer s.deletePath(s.T(), firstPart(dirPath))

	// Call on non-existent file/dir, check error.
	fi, err := s.StorageDriver.Stat(s.ctx, dirPath)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       dirPath,
	})
	require.Nil(s.T(), fi)

	fi, err = s.StorageDriver.Stat(s.ctx, filePath)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, storagedriver.PathNotFoundError{
		DriverName: s.StorageDriver.Name(),
		Path:       filePath,
	})
	require.Nil(s.T(), fi)

	err = s.StorageDriver.PutContent(s.ctx, filePath, content)
	require.NoError(s.T(), err)

	// Call on regular file, check results
	fi, err = s.StorageDriver.Stat(s.ctx, filePath)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), fi)
	require.Equal(s.T(), filePath, fi.Path())
	require.Equal(s.T(), int64(len(content)), fi.Size())
	require.False(s.T(), fi.IsDir())
	createdTime := fi.ModTime()

	// Sleep and modify the file
	time.Sleep(time.Second * 10)
	content = randomContents(4096)
	err = s.StorageDriver.PutContent(s.ctx, filePath, content)
	require.NoError(s.T(), err)
	fi, err = s.StorageDriver.Stat(s.ctx, filePath)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), fi)
	time.Sleep(time.Second * 5) // allow changes to propagate (eventual consistency)

	// Check if the modification time is after the creation time.
	// In case of cloud storage services, storage frontend nodes might have
	// time drift between them, however that should be solved with sleeping
	// before update.
	modTime := fi.ModTime()
	require.Greaterf(
		s.T(),
		modTime,
		createdTime,
		"modtime (%s) is before the creation time (%s)", modTime, createdTime,
	)

	// Call on directory (do not check ModTime as dirs don't need to support it)
	fi, err = s.StorageDriver.Stat(s.ctx, dirPath)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), fi)
	assert.Equal(s.T(), dirPath, fi.Path())
	assert.Zero(s.T(), fi.Size())
	assert.True(s.T(), fi.IsDir())
}

// TestPutContentMultipleTimes checks that if storage driver can overwrite the content
// in the subsequent puts. Validates that PutContent does not have to work
// with an offset like Writer does and overwrites the file entirely
// rather than writing the data to the [0,len(data)) of the file.
func (s *DriverSuite) TestPutContentMultipleTimes() {
	filename := randomPath(1, 32)
	contents := randomContents(4096)

	defer s.deletePath(s.T(), firstPart(filename))
	err := s.StorageDriver.PutContent(s.ctx, filename, contents)
	require.NoError(s.T(), err)

	contents = randomContents(2048) // upload a different, smaller file
	err = s.StorageDriver.PutContent(s.ctx, filename, contents)
	require.NoError(s.T(), err)

	readContents, err := s.StorageDriver.GetContent(s.ctx, filename)
	require.NoError(s.T(), err)
	require.Equal(s.T(), contents, readContents)
}

// TestConcurrentStreamReads checks that multiple clients can safely read from
// the same file simultaneously with various offsets.
func (s *DriverSuite) TestConcurrentStreamReads() {
	var filesize int64 = 128 * 1024 * 1024

	if testing.Short() {
		filesize = 10 * 1024 * 1024
		s.T().Log("Reducing file size to 10MB for short mode")
	}

	filename := randomPath(1, 32)
	contents := randomContents(filesize)

	defer s.deletePath(s.T(), firstPart(filename))

	err := s.StorageDriver.PutContent(s.ctx, filename, contents)
	require.NoError(s.T(), err)

	var wg sync.WaitGroup

	readContents := func() {
		defer wg.Done()
		/* #nosec G404 */
		offset := rand.Int63n(int64(len(contents)))
		reader, err := s.StorageDriver.Reader(s.ctx, filename, offset)
		require.NoError(s.T(), err)
		defer reader.Close()

		readContents, err := io.ReadAll(reader)
		require.NoError(s.T(), err)
		require.Equal(s.T(), contents[offset:], readContents)
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
// 	filename := randomPath(1,32)
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
func (s *DriverSuite) TestWalkParallel() {
	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer s.deletePath(s.T(), rootDirectory)

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
		err := s.StorageDriver.PutContent(s.ctx, wantedFiles[i], randomContents(int64(8+rand.Intn(8))))
		require.NoError(s.T(), err)
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

	sort.Strings(actualFiles)
	sort.Strings(wantedFiles)
	require.Equal(s.T(), wantedFiles, actualFiles)

	// Convert from a set of wanted directories into a slice.
	wantedDirectories := make([]string, len(wantedDirectoriesSet))

	var i int
	for k := range wantedDirectoriesSet {
		wantedDirectories[i] = k
		i++
	}

	require.ElementsMatch(s.T(), wantedDirectories, actualDirectories)
}

// TestWalkParallelError ensures that walk reports WalkFn errors.
func (s *DriverSuite) TestWalkParallelError() {
	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer s.deletePath(s.T(), rootDirectory)

	wantedFiles := randomBranchingFiles(rootDirectory, 100)

	for _, file := range wantedFiles {
		/* #nosec G404 */
		err := s.StorageDriver.PutContent(s.ctx, file, randomContents(int64(8+rand.Intn(8))))
		require.NoError(s.T(), err)
	}

	innerErr := errors.New("walk: expected test error")
	errorFile := wantedFiles[0]

	err := s.StorageDriver.WalkParallel(s.ctx, rootDirectory, func(fInfo storagedriver.FileInfo) error {
		if fInfo.Path() == errorFile {
			return innerErr
		}

		return nil
	})

	// Drivers may or may not return a multierror here, check that the innerError
	// is present in the error returned by walk.
	require.ErrorContains(s.T(), err, innerErr.Error())
}

// TestWalkParallelStopsProcessingOnError ensures that walk stops processing when an error is encountered.
func (s *DriverSuite) TestWalkParallelStopsProcessingOnError() {
	d := s.StorageDriver.Name()
	switch d {
	case "filesystem", "azure":
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

	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer s.deletePath(s.T(), rootDirectory)

	numWantedFiles := 1000
	wantedFiles := randomBranchingFiles(rootDirectory, numWantedFiles)

	// Add a file right under the root directory, so that processing is stopped
	// early in the walk cycle.
	errorFile := filepath.Join(rootDirectory, randomFilenameRange(8, 8))
	wantedFiles = append(wantedFiles, errorFile)

	for _, file := range wantedFiles {
		/* #nosec G404 */
		err := s.StorageDriver.PutContent(s.ctx, file, randomContents(int64(8+rand.Intn(8))))
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
	parentDir := randomPath(1, 8)
	defer func() {
		b.StopTimer()
		s.StorageDriver.Delete(s.ctx, firstPart(parentDir))
	}()

	for i := 0; i < b.N; i++ {
		filename := path.Join(parentDir, randomPath(4, 32))
		err := s.StorageDriver.PutContent(s.ctx, filename, randomContents(size))
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
	parentDir := randomPath(1, 8)
	defer func() {
		b.StopTimer()
		s.StorageDriver.Delete(s.ctx, firstPart(parentDir))
	}()

	for i := 0; i < b.N; i++ {
		filename := path.Join(parentDir, randomPath(4, 32))
		writer, err := s.StorageDriver.Writer(s.ctx, filename, false)
		require.NoError(b, err)
		written, err := io.Copy(writer, bytes.NewReader(randomContents(size)))
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
	parentDir := randomPath(1, 8)
	defer func() {
		b.StopTimer()
		s.StorageDriver.Delete(s.ctx, firstPart(parentDir))
	}()

	for i := int64(0); i < numFiles; i++ {
		err := s.StorageDriver.PutContent(s.ctx, path.Join(parentDir, randomPath(4, 32)), nil)
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
		parentDir := randomPath(4, 12)
		// nolint: revive// defer
		defer s.deletePath(b, firstPart(parentDir))

		b.StopTimer()
		for j := int64(0); j < numFiles; j++ {
			err := s.StorageDriver.PutContent(s.ctx, path.Join(parentDir, randomPath(4, 32)), nil)
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
		rootDirectory := "/" + randomFilenameRange(8, 8)
		// nolint: revive // defer
		defer s.deletePath(b, rootDirectory)

		b.StopTimer()

		wantedFiles := randomBranchingFiles(rootDirectory, numFiles)

		for i := 0; i < numFiles; i++ {
			/* #nosec G404 */
			err := s.StorageDriver.PutContent(s.ctx, wantedFiles[i], randomContents(int64(8+rand.Intn(8))))
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
	defer s.deletePath(b, firstPart("docker/"))

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
	defer s.deletePath(s.T(), firstPart("docker/"))

	registry := s.createRegistry(s.T())
	repo := s.makeRepository(s.T(), registry, randomFilename(5))
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
	defer s.deletePath(b, firstPart("docker/"))

	registry := s.createRegistry(b)
	repo := s.makeRepository(b, registry, randomFilename(5))
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
	defer s.deletePath(s.T(), firstPart("docker/"))

	registry := s.createRegistry(s.T())
	repo := s.makeRepository(s.T(), registry, randomFilename(5))
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
	defer s.deletePath(b, firstPart("docker/"))

	registry := s.createRegistry(b)
	repo := s.makeRepository(b, registry, randomFilename(5))
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
			rfn := randomFilename(5)
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
	defer s.deletePath(s.T(), firstPart("docker/"))

	registry := s.createRegistry(s.T())
	repo := s.makeRepository(s.T(), registry, randomFilename(5))

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
			Name:   randomFilename(10),
			Digest: digest.FromString(randomFilename(20)),
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
	defer s.deletePath(b, firstPart("docker/"))

	registry := s.createRegistry(b)
	repo := s.makeRepository(b, registry, randomFilename(5))

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

func (s *DriverSuite) testFileStreams(t *testing.T, size int64) {
	tf, err := os.CreateTemp("", "tf")
	require.NoError(t, err)
	defer os.Remove(tf.Name())
	defer tf.Close()

	filename := randomPath(4, 32)
	defer s.deletePath(t, firstPart(filename))

	contents := randomContents(size)

	_, err = tf.Write(contents)
	require.NoError(t, err)

	err = tf.Sync()
	require.NoError(t, err)
	_, err = tf.Seek(0, io.SeekStart)
	require.NoError(t, err)

	writer, err := s.StorageDriver.Writer(s.ctx, filename, false)
	require.NoError(t, err)
	nn, err := io.Copy(writer, tf)
	require.NoError(t, err)
	require.Equal(t, size, nn)

	err = writer.Commit()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	reader, err := s.StorageDriver.Reader(s.ctx, filename, 0)
	require.NoError(t, err)
	defer reader.Close()

	readContents, err := io.ReadAll(reader)
	require.NoError(t, err)

	require.Equal(t, contents, readContents)
}

func (s *DriverSuite) writeReadCompare(t *testing.T, filename string, contents []byte) {
	defer s.deletePath(t, firstPart(filename))

	err := s.StorageDriver.PutContent(s.ctx, filename, contents)
	require.NoError(t, err)

	readContents, err := s.StorageDriver.GetContent(s.ctx, filename)
	require.NoError(t, err)

	require.Equal(t, contents, readContents)
}

func (s *DriverSuite) writeReadCompareStreams(t *testing.T, filename string, contents []byte) {
	defer s.deletePath(t, firstPart(filename))

	writer, err := s.StorageDriver.Writer(s.ctx, filename, false)
	require.NoError(t, err)
	nn, err := io.Copy(writer, bytes.NewReader(contents))
	require.NoError(t, err)
	require.Equal(t, int64(len(contents)), nn)

	err = writer.Commit()
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	reader, err := s.StorageDriver.Reader(s.ctx, filename, 0)
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

func randomPath(minTldLen, length int) string {
	// NOTE(prozlach): randomPath() is called in some tests concurrently and it
	// may happen that the top-level directory of the path returned is not
	// unique given enough calls to it. This leads to wonky race conditions as
	// the tests are running concurrently and are trying to remove top level
	// directory to clean up after themselves while others are still writing to
	// it, leading to errors where paths are not being cleaned up.
	//
	// The solution is simple - enforce the minimal length of the top level dir
	// enough so that the chance that there are collisions(i.e. same top-level
	// directories) are very low for tests that require it.
	//
	// In my test script doing one million iterations I was not able to get a
	// collision anymore for directories with minimal length of 4 and 32 calls
	// to randomPath() function.
	p := "/"
	for len(p) < length {
		/* #nosec G404 */
		chunkLength := rand.Intn(length-len(p)) + 1
		if len(p) == 1 && chunkLength < minTldLen {
			// First component is too short - retry
			continue
		}
		chunk := randomFilename(chunkLength)
		p += chunk
		remaining := length - len(p)
		if remaining == 1 {
			p += randomFilename(1)
		} else if remaining > 1 {
			p += "/"
		}
	}
	return p
}

func randomFilename(length int) string {
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
func randomFilenameRange(minimum, maximum int) string { // nolint:unparam //(min always receives 8)
	/* #nosec G404 */
	return randomFilename(minimum + (rand.Intn(maximum + 1)))
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

func (rr *randReader) Read(p []byte) (int, error) {
	rr.m.Lock()
	defer rr.m.Unlock()

	toread := int64(len(p))
	if toread > rr.r {
		toread = rr.r
	}
	n := copy(p, randomContents(toread))
	rr.r -= int64(n)

	if rr.r <= 0 {
		return n, io.EOF
	}

	return n, nil
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
