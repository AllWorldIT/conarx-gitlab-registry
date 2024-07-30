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
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/libtrust"

	"github.com/docker/distribution/registry/storage"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/testutil"
	"gopkg.in/check.v1"
)

// Test hooks up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

// RegisterSuite registers an in-process storage driver test suite with
// the go test runner.
func RegisterSuite(driverConstructor DriverConstructor, skipCheck SkipCheck) {
	check.Suite(&DriverSuite{
		Constructor: driverConstructor,
		SkipCheck:   skipCheck,
		ctx:         context.Background(),
	})
}

// SkipCheck is a function used to determine if a test suite should be skipped.
// If a SkipCheck returns a non-empty skip reason, the suite is skipped with
// the given reason.
type SkipCheck func() (reason string)

// NeverSkip is a default SkipCheck which never skips the suite.
var NeverSkip SkipCheck = func() string { return "" }

// DriverConstructor is a function which returns a new
// storagedriver.StorageDriver.
type DriverConstructor func() (storagedriver.StorageDriver, error)

// DriverTeardown is a function which cleans up a suite's
// storagedriver.StorageDriver.
type DriverTeardown func() error

// DriverSuite is a gocheck test suite designed to test a
// storagedriver.StorageDriver. The intended way to create a DriverSuite is
// with RegisterSuite.
type DriverSuite struct {
	Constructor DriverConstructor
	Teardown    DriverTeardown
	SkipCheck
	storagedriver.StorageDriver
	ctx context.Context
}

// SetUpSuite sets up the gocheck test suite.
func (suite *DriverSuite) SetUpSuite(c *check.C) {
	if reason := suite.SkipCheck(); reason != "" {
		c.Skip(reason)
	}
	d, err := suite.Constructor()
	c.Assert(err, check.IsNil)
	suite.StorageDriver = d
}

// TearDownSuite tears down the gocheck test suite.
func (suite *DriverSuite) TearDownSuite(c *check.C) {
	if suite.Teardown != nil {
		err := suite.Teardown()
		c.Assert(err, check.IsNil)
	}
}

// TearDownTest tears down the gocheck test.
// This causes the suite to abort if any files are left around in the storage
// driver.
func (suite *DriverSuite) TearDownTest(c *check.C) {
	files, _ := suite.StorageDriver.List(suite.ctx, "/")
	if len(files) > 0 {
		c.Fatalf("Storage driver did not clean up properly. Offending files: %#v", files)
	}
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

// TestRootExists ensures that all storage drivers have a root path by default.
func (suite *DriverSuite) TestRootExists(c *check.C) {
	_, err := suite.StorageDriver.List(suite.ctx, "/")
	if err != nil {
		c.Fatalf(`the root path "/" should always exist: %v`, err)
	}
}

// TestValidPaths checks that various valid file paths are accepted by the
// storage driver.
func (suite *DriverSuite) TestValidPaths(c *check.C) {
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
		defer suite.deletePath(c, firstPart(filename))
		c.Assert(err, check.IsNil)

		received, err := suite.StorageDriver.GetContent(suite.ctx, filename)
		c.Assert(err, check.IsNil)
		c.Assert(received, check.DeepEquals, contents)
	}
}

func (suite *DriverSuite) deletePath(c *check.C, path string) {
	for tries := 2; tries > 0; tries-- {
		err := suite.StorageDriver.Delete(suite.ctx, path)
		if _, ok := err.(storagedriver.PathNotFoundError); ok {
			err = nil
		}
		c.Assert(err, check.IsNil)
		paths, _ := suite.StorageDriver.List(suite.ctx, path)
		if len(paths) == 0 {
			break
		}
		time.Sleep(time.Second * 2)
	}
}

// TestInvalidPaths checks that various invalid file paths are rejected by the
// storage driver.
func (suite *DriverSuite) TestInvalidPaths(c *check.C) {
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
			defer suite.deletePath(c, firstPart(filename))
		}
		c.Assert(err, check.NotNil)
		c.Assert(err, check.FitsTypeOf, storagedriver.InvalidPathError{})
		c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)

		_, err = suite.StorageDriver.GetContent(suite.ctx, filename)
		c.Assert(err, check.NotNil)
		c.Assert(err, check.FitsTypeOf, storagedriver.InvalidPathError{})
		c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)
	}
}

// TestWriteRead1 tests a simple write-read workflow.
func (suite *DriverSuite) TestWriteRead1(c *check.C) {
	filename := randomPath(32)
	contents := []byte("a")
	suite.writeReadCompare(c, filename, contents)
}

// TestWriteRead2 tests a simple write-read workflow with unicode data.
func (suite *DriverSuite) TestWriteRead2(c *check.C) {
	filename := randomPath(32)
	contents := []byte("\xc3\x9f")
	suite.writeReadCompare(c, filename, contents)
}

// TestWriteRead3 tests a simple write-read workflow with a small string.
func (suite *DriverSuite) TestWriteRead3(c *check.C) {
	filename := randomPath(32)
	contents := randomContents(32)
	suite.writeReadCompare(c, filename, contents)
}

// TestWriteRead4 tests a simple write-read workflow with 1MB of data.
func (suite *DriverSuite) TestWriteRead4(c *check.C) {
	filename := randomPath(32)
	contents := randomContents(1024 * 1024)
	suite.writeReadCompare(c, filename, contents)
}

// TestWriteReadNonUTF8 tests that non-utf8 data may be written to the storage
// driver safely.
func (suite *DriverSuite) TestWriteReadNonUTF8(c *check.C) {
	filename := randomPath(32)
	contents := []byte{0x80, 0x80, 0x80, 0x80}
	suite.writeReadCompare(c, filename, contents)
}

// TestTruncate tests that putting smaller contents than an original file does
// remove the excess contents.
func (suite *DriverSuite) TestTruncate(c *check.C) {
	filename := randomPath(32)
	contents := randomContents(1024 * 1024)
	suite.writeReadCompare(c, filename, contents)

	contents = randomContents(1024)
	suite.writeReadCompare(c, filename, contents)
}

// TestReadNonexistent tests reading content from an empty path.
func (suite *DriverSuite) TestReadNonexistent(c *check.C) {
	filename := randomPath(32)
	_, err := suite.StorageDriver.GetContent(suite.ctx, filename)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)
}

// TestWriteReadStreams1 tests a simple write-read streaming workflow.
func (suite *DriverSuite) TestWriteReadStreams1(c *check.C) {
	filename := randomPath(32)
	contents := []byte("a")
	suite.writeReadCompareStreams(c, filename, contents)
}

// TestWriteReadStreams2 tests a simple write-read streaming workflow with
// unicode data.
func (suite *DriverSuite) TestWriteReadStreams2(c *check.C) {
	filename := randomPath(32)
	contents := []byte("\xc3\x9f")
	suite.writeReadCompareStreams(c, filename, contents)
}

// TestWriteReadStreams3 tests a simple write-read streaming workflow with a
// small amount of data.
func (suite *DriverSuite) TestWriteReadStreams3(c *check.C) {
	filename := randomPath(32)
	contents := randomContents(32)
	suite.writeReadCompareStreams(c, filename, contents)
}

// TestWriteReadStreams4 tests a simple write-read streaming workflow with 1MB
// of data.
func (suite *DriverSuite) TestWriteReadStreams4(c *check.C) {
	filename := randomPath(32)
	contents := randomContents(1024 * 1024)
	suite.writeReadCompareStreams(c, filename, contents)
}

// TestWriteReadStreamsNonUTF8 tests that non-utf8 data may be written to the
// storage driver safely.
func (suite *DriverSuite) TestWriteReadStreamsNonUTF8(c *check.C) {
	filename := randomPath(32)
	contents := []byte{0x80, 0x80, 0x80, 0x80}
	suite.writeReadCompareStreams(c, filename, contents)
}

// TestWriteReadLargeStreams tests that a 5GB file may be written to the storage
// driver safely.
func (suite *DriverSuite) TestWriteReadLargeStreams(c *check.C) {
	if testing.Short() {
		c.Skip("Skipping test in short mode")
	}

	filename := randomPath(32)
	defer suite.deletePath(c, firstPart(filename))

	checksum := sha256.New()
	var fileSize int64 = 2 * 1024 * 1024 * 1024

	contents := newRandReader(fileSize)

	writer, err := suite.StorageDriver.Writer(suite.ctx, filename, false)
	c.Assert(err, check.IsNil)
	written, err := io.Copy(writer, io.TeeReader(contents, checksum))
	c.Assert(err, check.IsNil)
	c.Assert(written, check.Equals, fileSize)

	err = writer.Commit()
	c.Assert(err, check.IsNil)
	err = writer.Close()
	c.Assert(err, check.IsNil)

	reader, err := suite.StorageDriver.Reader(suite.ctx, filename, 0)
	c.Assert(err, check.IsNil)
	defer reader.Close()

	writtenChecksum := sha256.New()
	io.Copy(writtenChecksum, reader)

	c.Assert(writtenChecksum.Sum(nil), check.DeepEquals, checksum.Sum(nil))
}

// TestReaderWithOffset tests that the appropriate data is streamed when
// reading with a given offset.
func (suite *DriverSuite) TestReaderWithOffset(c *check.C) {
	filename := randomPath(32)
	defer suite.deletePath(c, firstPart(filename))

	chunkSize := int64(32)

	contentsChunk1 := randomContents(chunkSize)
	contentsChunk2 := randomContents(chunkSize)
	contentsChunk3 := randomContents(chunkSize)

	err := suite.StorageDriver.PutContent(suite.ctx, filename, append(append(contentsChunk1, contentsChunk2...), contentsChunk3...))
	c.Assert(err, check.IsNil)

	reader, err := suite.StorageDriver.Reader(suite.ctx, filename, 0)
	c.Assert(err, check.IsNil)
	defer reader.Close()

	readContents, err := io.ReadAll(reader)
	c.Assert(err, check.IsNil)

	c.Assert(readContents, check.DeepEquals, append(append(contentsChunk1, contentsChunk2...), contentsChunk3...))

	reader, err = suite.StorageDriver.Reader(suite.ctx, filename, chunkSize)
	c.Assert(err, check.IsNil)
	defer reader.Close()

	readContents, err = io.ReadAll(reader)
	c.Assert(err, check.IsNil)

	c.Assert(readContents, check.DeepEquals, append(contentsChunk2, contentsChunk3...))

	reader, err = suite.StorageDriver.Reader(suite.ctx, filename, chunkSize*2)
	c.Assert(err, check.IsNil)
	defer reader.Close()

	readContents, err = io.ReadAll(reader)
	c.Assert(err, check.IsNil)
	c.Assert(readContents, check.DeepEquals, contentsChunk3)

	// Ensure we get invalid offset for negative offsets.
	reader, err = suite.StorageDriver.Reader(suite.ctx, filename, -1)
	c.Assert(err, check.FitsTypeOf, storagedriver.InvalidOffsetError{})
	c.Assert(err.(storagedriver.InvalidOffsetError).Offset, check.Equals, int64(-1))
	c.Assert(err.(storagedriver.InvalidOffsetError).Path, check.Equals, filename)
	c.Assert(reader, check.IsNil)
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)

	// Read past the end of the content and make sure we get a reader that
	// returns 0 bytes and io.EOF
	reader, err = suite.StorageDriver.Reader(suite.ctx, filename, chunkSize*3)
	c.Assert(err, check.IsNil)
	defer reader.Close()

	buf := make([]byte, chunkSize)
	n, err := reader.Read(buf)
	c.Assert(err, check.Equals, io.EOF)
	c.Assert(n, check.Equals, 0)

	// Check the N-1 boundary condition, ensuring we get 1 byte then io.EOF.
	reader, err = suite.StorageDriver.Reader(suite.ctx, filename, chunkSize*3-1)
	c.Assert(err, check.IsNil)
	defer reader.Close()

	n, err = reader.Read(buf)
	c.Assert(n, check.Equals, 1)

	// We don't care whether the io.EOF comes on the this read or the first
	// zero read, but the only error acceptable here is io.EOF.
	if err != nil {
		c.Assert(err, check.Equals, io.EOF)
	}

	// Any more reads should result in zero bytes and io.EOF
	n, err = reader.Read(buf)
	c.Assert(n, check.Equals, 0)
	c.Assert(err, check.Equals, io.EOF)
}

// TestContinueStreamAppendLarge tests that a stream write can be appended to without
// corrupting the data with a large chunk size.
func (suite *DriverSuite) TestContinueStreamAppendLarge(c *check.C) {
	suite.testContinueStreamAppend(c, int64(10*1024*1024))
}

// TestContinueStreamAppendSmall is the same as TestContinueStreamAppendLarge, but only
// with a tiny chunk size in order to test corner cases for some cloud storage drivers.
func (suite *DriverSuite) TestContinueStreamAppendSmall(c *check.C) {
	suite.testContinueStreamAppend(c, int64(32))
}

func (suite *DriverSuite) testContinueStreamAppend(c *check.C, chunkSize int64) {
	filename := randomPath(32)
	defer suite.deletePath(c, firstPart(filename))

	contentsChunk1 := randomContents(chunkSize)
	contentsChunk2 := randomContents(chunkSize)
	contentsChunk3 := randomContents(chunkSize)

	fullContents := append(append(contentsChunk1, contentsChunk2...), contentsChunk3...)

	writer, err := suite.StorageDriver.Writer(suite.ctx, filename, false)
	c.Assert(err, check.IsNil)
	nn, err := io.Copy(writer, bytes.NewReader(contentsChunk1))
	c.Assert(err, check.IsNil)
	c.Assert(nn, check.Equals, int64(len(contentsChunk1)))

	err = writer.Close()
	c.Assert(err, check.IsNil)

	curSize := writer.Size()
	c.Assert(curSize, check.Equals, int64(len(contentsChunk1)))

	writer, err = suite.StorageDriver.Writer(suite.ctx, filename, true)
	c.Assert(err, check.IsNil)
	c.Assert(writer.Size(), check.Equals, curSize)

	nn, err = io.Copy(writer, bytes.NewReader(contentsChunk2))
	c.Assert(err, check.IsNil)
	c.Assert(nn, check.Equals, int64(len(contentsChunk2)))

	err = writer.Close()
	c.Assert(err, check.IsNil)

	curSize = writer.Size()
	c.Assert(curSize, check.Equals, 2*chunkSize)

	writer, err = suite.StorageDriver.Writer(suite.ctx, filename, true)
	c.Assert(err, check.IsNil)
	c.Assert(writer.Size(), check.Equals, curSize)

	nn, err = io.Copy(writer, bytes.NewReader(fullContents[curSize:]))
	c.Assert(err, check.IsNil)
	c.Assert(nn, check.Equals, int64(len(fullContents[curSize:])))

	err = writer.Commit()
	c.Assert(err, check.IsNil)
	err = writer.Close()
	c.Assert(err, check.IsNil)

	received, err := suite.StorageDriver.GetContent(suite.ctx, filename)
	c.Assert(err, check.IsNil)
	c.Assert(received, check.DeepEquals, fullContents)
}

// TestReadNonexistentStream tests that reading a stream for a nonexistent path
// fails.
func (suite *DriverSuite) TestReadNonexistentStream(c *check.C) {
	filename := randomPath(32)

	_, err := suite.StorageDriver.Reader(suite.ctx, filename, 0)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)

	_, err = suite.StorageDriver.Reader(suite.ctx, filename, 64)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)
}

// TestList1File tests the validity of List calls for 1 file.
func (suite *DriverSuite) TestList1File(c *check.C) {
	suite.testList(c, 1)
}

// TestList1200Files tests the validity of List calls for 1200 files.
func (suite *DriverSuite) TestList1200Files(c *check.C) {
	suite.testList(c, 1200)
}

// testList checks the returned list of keys after populating a directory tree.
func (suite *DriverSuite) testList(c *check.C, numFiles int) {
	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer suite.deletePath(c, rootDirectory)

	doesnotexist := path.Join(rootDirectory, "nonexistent")
	_, err := suite.StorageDriver.List(suite.ctx, doesnotexist)
	c.Assert(err, check.Equals, storagedriver.PathNotFoundError{
		Path:       doesnotexist,
		DriverName: suite.StorageDriver.Name(),
	})

	parentDirectory := rootDirectory + "/" + randomFilenameRange(8, 8)
	childFiles := make([]string, numFiles)
	for i := range childFiles {
		childFile := parentDirectory + "/" + randomFilenameRange(8, 8)
		childFiles[i] = childFile
		err := suite.StorageDriver.PutContent(suite.ctx, childFile, randomContents(8))
		c.Assert(err, check.IsNil)
	}
	sort.Strings(childFiles)

	keys, err := suite.StorageDriver.List(suite.ctx, "/")
	c.Assert(err, check.IsNil)
	c.Assert(keys, check.DeepEquals, []string{rootDirectory})

	keys, err = suite.StorageDriver.List(suite.ctx, rootDirectory)
	c.Assert(err, check.IsNil)
	c.Assert(keys, check.DeepEquals, []string{parentDirectory})

	keys, err = suite.StorageDriver.List(suite.ctx, parentDirectory)
	c.Assert(err, check.IsNil)

	sort.Strings(keys)
	c.Assert(keys, check.DeepEquals, childFiles)

	// A few checks to add here (check out #819 for more discussion on this):
	// 1. Ensure that all paths are absolute.
	// 2. Ensure that listings only include direct children.
	// 3. Ensure that we only respond to directory listings that end with a slash (maybe?).
}

// TestMove checks that a moved object no longer exists at the source path and
// does exist at the destination.
func (suite *DriverSuite) TestMove(c *check.C) {
	contents := randomContents(32)
	sourcePath := randomPath(32)
	destPath := randomPath(32)

	defer suite.deletePath(c, firstPart(sourcePath))
	defer suite.deletePath(c, firstPart(destPath))

	err := suite.StorageDriver.PutContent(suite.ctx, sourcePath, contents)
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.Move(suite.ctx, sourcePath, destPath)
	c.Assert(err, check.IsNil)

	received, err := suite.StorageDriver.GetContent(suite.ctx, destPath)
	c.Assert(err, check.IsNil)
	c.Assert(received, check.DeepEquals, contents)

	_, err = suite.StorageDriver.GetContent(suite.ctx, sourcePath)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)
}

// TestMoveOverwrite checks that a moved object no longer exists at the source
// path and overwrites the contents at the destination.
func (suite *DriverSuite) TestMoveOverwrite(c *check.C) {
	sourcePath := randomPath(32)
	destPath := randomPath(32)
	sourceContents := randomContents(32)
	destContents := randomContents(64)

	defer suite.deletePath(c, firstPart(sourcePath))
	defer suite.deletePath(c, firstPart(destPath))

	err := suite.StorageDriver.PutContent(suite.ctx, sourcePath, sourceContents)
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.PutContent(suite.ctx, destPath, destContents)
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.Move(suite.ctx, sourcePath, destPath)
	c.Assert(err, check.IsNil)

	received, err := suite.StorageDriver.GetContent(suite.ctx, destPath)
	c.Assert(err, check.IsNil)
	c.Assert(received, check.DeepEquals, sourceContents)

	_, err = suite.StorageDriver.GetContent(suite.ctx, sourcePath)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)
}

// TestMoveNonexistent checks that moving a nonexistent key fails and does not
// delete the data at the destination path.
func (suite *DriverSuite) TestMoveNonexistent(c *check.C) {
	contents := randomContents(32)
	sourcePath := randomPath(32)
	destPath := randomPath(32)

	defer suite.deletePath(c, firstPart(destPath))

	err := suite.StorageDriver.PutContent(suite.ctx, destPath, contents)
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.Move(suite.ctx, sourcePath, destPath)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)

	received, err := suite.StorageDriver.GetContent(suite.ctx, destPath)
	c.Assert(err, check.IsNil)
	c.Assert(received, check.DeepEquals, contents)
}

// TestMoveInvalid provides various checks for invalid moves.
func (suite *DriverSuite) TestMoveInvalid(c *check.C) {
	contents := randomContents(32)

	// Create a regular file.
	err := suite.StorageDriver.PutContent(suite.ctx, "/notadir", contents)
	c.Assert(err, check.IsNil)
	defer suite.deletePath(c, "/notadir")

	// Now try to move a non-existent file under it.
	err = suite.StorageDriver.Move(suite.ctx, "/notadir/foo", "/notadir/bar")
	c.Assert(err, check.NotNil) // non-nil error
}

// TestDelete checks that the delete operation removes data from the storage
// driver
func (suite *DriverSuite) TestDelete(c *check.C) {
	filename := randomPath(32)
	contents := randomContents(32)

	defer suite.deletePath(c, firstPart(filename))

	err := suite.StorageDriver.PutContent(suite.ctx, filename, contents)
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.Delete(suite.ctx, filename)
	c.Assert(err, check.IsNil)

	_, err = suite.StorageDriver.GetContent(suite.ctx, filename)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)
}

// TestDeleteDir1File ensures the driver is able to delete all objects in a
// directory with 1 file.
func (suite *DriverSuite) TestDeleteDir1File(c *check.C) {
	suite.testDeleteDir(c, 1)
}

// TestDeleteDir1200Files ensures the driver is able to delete all objects in a
// directory with 1200 files.
func (suite *DriverSuite) TestDeleteDir1200Files(c *check.C) {
	suite.testDeleteDir(c, 1200)
}

func (suite *DriverSuite) testDeleteDir(c *check.C, numFiles int) {
	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer suite.deletePath(c, rootDirectory)

	parentDirectory := rootDirectory + "/" + randomFilenameRange(8, 8)
	childFiles := make([]string, numFiles)
	for i := range childFiles {
		childFile := parentDirectory + "/" + randomFilenameRange(8, 8)
		childFiles[i] = childFile
		err := suite.StorageDriver.PutContent(suite.ctx, childFile, randomContents(8))
		c.Assert(err, check.IsNil)
	}

	err := suite.StorageDriver.Delete(suite.ctx, parentDirectory)
	c.Assert(err, check.IsNil)

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
			c.Logf("able to access file %d after deletion", i)
		} else {
			c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
			c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)
		}
	}

	if filesRemaining {
		c.Fatal("Encountered files remaining after deletion")
	}
}

// buildFiles builds a num amount of test files with a size of size under parentDir. Returns a slice with the path of
// the created files.
func (suite *DriverSuite) buildFiles(c *check.C, parentDir string, num int64, size int64) []string {
	paths := make([]string, 0, num)

	for i := int64(0); i < num; i++ {
		p := path.Join(parentDir, randomPath(32))
		paths = append(paths, p)

		err := suite.StorageDriver.PutContent(suite.ctx, p, randomContents(size))
		c.Assert(err, check.IsNil)
	}

	return paths
}

// assertPathNotFound asserts that path does not exist in the storage driver filesystem.
func (suite *DriverSuite) assertPathNotFound(c *check.C, path ...string) {
	for _, p := range path {
		_, err := suite.StorageDriver.GetContent(suite.ctx, p)
		c.Assert(err, check.NotNil)

		c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
		c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)
	}
}

// TestDeleteFiles checks that DeleteFiles removes data from the storage driver for a random (<10) number of files.
func (suite *DriverSuite) TestDeleteFiles(c *check.C) {
	parentDir := randomPath(8)
	defer suite.deletePath(c, firstPart(parentDir))

	/* #nosec G404 */
	blobPaths := suite.buildFiles(c, parentDir, rand.Int63n(10), 32)

	count, err := suite.StorageDriver.DeleteFiles(suite.ctx, blobPaths)
	c.Assert(err, check.IsNil)
	c.Assert(count, check.Equals, len(blobPaths))

	suite.assertPathNotFound(c, blobPaths...)
}

// TestDeleteFilesNotFound checks that DeleteFiles is idempotent and doesn't return an error if a file was not found.
func (suite *DriverSuite) TestDeleteFilesNotFound(c *check.C) {
	parentDir := randomPath(8)
	defer suite.deletePath(c, firstPart(parentDir))

	blobPaths := suite.buildFiles(c, parentDir, 5, 32)
	// delete the 1st, 3rd and last file so that they don't exist anymore
	suite.deletePath(c, blobPaths[0])
	suite.deletePath(c, blobPaths[2])
	suite.deletePath(c, blobPaths[4])

	count, err := suite.StorageDriver.DeleteFiles(suite.ctx, blobPaths)
	c.Assert(err, check.IsNil)
	c.Assert(count, check.Equals, len(blobPaths))

	suite.assertPathNotFound(c, blobPaths...)
}

// benchmarkDeleteFiles benchmarks DeleteFiles for an amount of num files.
func (suite *DriverSuite) benchmarkDeleteFiles(c *check.C, num int64) {
	parentDir := randomPath(8)
	defer suite.deletePath(c, firstPart(parentDir))

	for i := 0; i < c.N; i++ {
		c.StopTimer()
		paths := suite.buildFiles(c, parentDir, num, 32)
		c.StartTimer()
		count, err := suite.StorageDriver.DeleteFiles(suite.ctx, paths)
		c.StopTimer()
		c.Assert(err, check.IsNil)
		c.Assert(count, check.Equals, len(paths))
		suite.assertPathNotFound(c, paths...)
	}
}

// BenchmarkDeleteFiles1File benchmarks DeleteFiles for 1 file.
func (suite *DriverSuite) BenchmarkDeleteFiles1File(c *check.C) {
	suite.benchmarkDeleteFiles(c, 1)
}

// BenchmarkDeleteFiles100Files benchmarks DeleteFiles for 100 files.
func (suite *DriverSuite) BenchmarkDeleteFiles100Files(c *check.C) {
	suite.benchmarkDeleteFiles(c, 100)
}

// TestURLFor checks that the URLFor method functions properly, but only if it
// is implemented
func (suite *DriverSuite) TestURLFor(c *check.C) {
	filename := randomPath(32)
	contents := randomContents(32)

	defer suite.deletePath(c, firstPart(filename))

	err := suite.StorageDriver.PutContent(suite.ctx, filename, contents)
	c.Assert(err, check.IsNil)

	url, err := suite.StorageDriver.URLFor(suite.ctx, filename, nil)
	if _, ok := err.(storagedriver.ErrUnsupportedMethod); ok {
		return
	}
	c.Assert(err, check.IsNil)

	response, err := http.Get(url)
	c.Assert(err, check.IsNil)
	defer response.Body.Close()

	read, err := io.ReadAll(response.Body)
	c.Assert(err, check.IsNil)
	c.Assert(read, check.DeepEquals, contents)

	url, err = suite.StorageDriver.URLFor(suite.ctx, filename, map[string]interface{}{"method": http.MethodHead})
	if _, ok := err.(storagedriver.ErrUnsupportedMethod); ok {
		return
	}
	c.Assert(err, check.IsNil)

	response, err = http.Head(url)
	c.Assert(err, check.IsNil)
	response.Body.Close()
	c.Assert(response.StatusCode, check.Equals, 200)
	c.Assert(response.ContentLength, check.Equals, int64(32))
}

// TestDeleteNonexistent checks that removing a nonexistent key fails.
func (suite *DriverSuite) TestDeleteNonexistent(c *check.C) {
	filename := randomPath(32)
	err := suite.StorageDriver.Delete(suite.ctx, filename)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)
}

// TestDeleteFolder checks that deleting a folder removes all child elements.
func (suite *DriverSuite) TestDeleteFolder(c *check.C) {
	dirname := randomPath(32)
	filename1 := randomPath(32)
	filename2 := randomPath(32)
	filename3 := randomPath(32)
	contents := randomContents(32)

	defer suite.deletePath(c, firstPart(dirname))

	err := suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, filename1), contents)
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, filename2), contents)
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, filename3), contents)
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.Delete(suite.ctx, path.Join(dirname, filename1))
	c.Assert(err, check.IsNil)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename1))
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename2))
	c.Assert(err, check.IsNil)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename3))
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.Delete(suite.ctx, dirname)
	c.Assert(err, check.IsNil)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename1))
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename2))
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename3))
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)
}

// TestDeleteOnlyDeletesSubpaths checks that deleting path A does not
// delete path B when A is a prefix of B but B is not a subpath of A (so that
// deleting "/a" does not delete "/ab").  This matters for services like S3 that
// do not implement directories.
func (suite *DriverSuite) TestDeleteOnlyDeletesSubpaths(c *check.C) {
	dirname := randomPath(32)
	filename := randomPath(32)
	contents := randomContents(32)

	defer suite.deletePath(c, firstPart(dirname))

	err := suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, filename), contents)
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, filename+"suffix"), contents)
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, dirname, filename), contents)
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.PutContent(suite.ctx, path.Join(dirname, dirname+"suffix", filename), contents)
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.Delete(suite.ctx, path.Join(dirname, filename))
	c.Assert(err, check.IsNil)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename))
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, filename+"suffix"))
	c.Assert(err, check.IsNil)

	err = suite.StorageDriver.Delete(suite.ctx, path.Join(dirname, dirname))
	c.Assert(err, check.IsNil)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, dirname, filename))
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)

	_, err = suite.StorageDriver.GetContent(suite.ctx, path.Join(dirname, dirname+"suffix", filename))
	c.Assert(err, check.IsNil)
}

// TestStatCall runs verifies the implementation of the storagedriver's Stat call.
func (suite *DriverSuite) TestStatCall(c *check.C) {
	content := randomContents(4096)
	dirPath := randomPath(32)
	fileName := randomFilename(32)
	filePath := path.Join(dirPath, fileName)

	defer suite.deletePath(c, firstPart(dirPath))

	// Call on non-existent file/dir, check error.
	fi, err := suite.StorageDriver.Stat(suite.ctx, dirPath)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)
	c.Assert(fi, check.IsNil)

	fi, err = suite.StorageDriver.Stat(suite.ctx, filePath)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.FitsTypeOf, storagedriver.PathNotFoundError{})
	c.Assert(strings.Contains(err.Error(), suite.Name()), check.Equals, true)
	c.Assert(fi, check.IsNil)

	err = suite.StorageDriver.PutContent(suite.ctx, filePath, content)
	c.Assert(err, check.IsNil)

	// Call on regular file, check results
	fi, err = suite.StorageDriver.Stat(suite.ctx, filePath)
	c.Assert(err, check.IsNil)
	c.Assert(fi, check.NotNil)
	c.Assert(fi.Path(), check.Equals, filePath)
	c.Assert(fi.Size(), check.Equals, int64(len(content)))
	c.Assert(fi.IsDir(), check.Equals, false)
	createdTime := fi.ModTime()

	// Sleep and modify the file
	time.Sleep(time.Second * 10)
	content = randomContents(4096)
	err = suite.StorageDriver.PutContent(suite.ctx, filePath, content)
	c.Assert(err, check.IsNil)
	fi, err = suite.StorageDriver.Stat(suite.ctx, filePath)
	c.Assert(err, check.IsNil)
	c.Assert(fi, check.NotNil)
	time.Sleep(time.Second * 5) // allow changes to propagate (eventual consistency)

	// Check if the modification time is after the creation time.
	// In case of cloud storage services, storage frontend nodes might have
	// time drift between them, however that should be solved with sleeping
	// before update.
	modTime := fi.ModTime()
	if !modTime.After(createdTime) {
		c.Errorf("modtime (%s) is before the creation time (%s)", modTime, createdTime)
	}

	// Call on directory (do not check ModTime as dirs don't need to support it)
	fi, err = suite.StorageDriver.Stat(suite.ctx, dirPath)
	c.Assert(err, check.IsNil)
	c.Assert(fi, check.NotNil)
	c.Assert(fi.Path(), check.Equals, dirPath)
	c.Assert(fi.Size(), check.Equals, int64(0))
	c.Assert(fi.IsDir(), check.Equals, true)
}

// TestPutContentMultipleTimes checks that if storage driver can overwrite the content
// in the subsequent puts. Validates that PutContent does not have to work
// with an offset like Writer does and overwrites the file entirely
// rather than writing the data to the [0,len(data)) of the file.
func (suite *DriverSuite) TestPutContentMultipleTimes(c *check.C) {
	filename := randomPath(32)
	contents := randomContents(4096)

	defer suite.deletePath(c, firstPart(filename))
	err := suite.StorageDriver.PutContent(suite.ctx, filename, contents)
	c.Assert(err, check.IsNil)

	contents = randomContents(2048) // upload a different, smaller file
	err = suite.StorageDriver.PutContent(suite.ctx, filename, contents)
	c.Assert(err, check.IsNil)

	readContents, err := suite.StorageDriver.GetContent(suite.ctx, filename)
	c.Assert(err, check.IsNil)
	c.Assert(readContents, check.DeepEquals, contents)
}

// TestConcurrentStreamReads checks that multiple clients can safely read from
// the same file simultaneously with various offsets.
func (suite *DriverSuite) TestConcurrentStreamReads(c *check.C) {
	var filesize int64 = 128 * 1024 * 1024

	if testing.Short() {
		filesize = 10 * 1024 * 1024
		c.Log("Reducing file size to 10MB for short mode")
	}

	filename := randomPath(32)
	contents := randomContents(filesize)

	defer suite.deletePath(c, firstPart(filename))

	err := suite.StorageDriver.PutContent(suite.ctx, filename, contents)
	c.Assert(err, check.IsNil)

	var wg sync.WaitGroup

	readContents := func() {
		defer wg.Done()
		/* #nosec G404 */
		offset := rand.Int63n(int64(len(contents)))
		reader, err := suite.StorageDriver.Reader(suite.ctx, filename, offset)
		c.Assert(err, check.IsNil)
		defer reader.Close()

		readContents, err := io.ReadAll(reader)
		c.Assert(err, check.IsNil)
		c.Assert(readContents, check.DeepEquals, contents[offset:])
	}

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go readContents()
	}
	wg.Wait()
}

// TestConcurrentFileStreams checks that multiple *os.File objects can be passed
// in to Writer concurrently without hanging.
func (suite *DriverSuite) TestConcurrentFileStreams(c *check.C) {
	numStreams := 32

	if testing.Short() {
		numStreams = 8
		c.Log("Reducing number of streams to 8 for short mode")
	}

	var wg sync.WaitGroup

	testStream := func(size int64) {
		defer wg.Done()
		suite.testFileStreams(c, size)
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
// func (suite *DriverSuite) TestEventualConsistency(c *check.C) {
// 	if testing.Short() {
// 		c.Skip("Skipping test in short mode")
// 	}
//
// 	filename := randomPath(32)
// 	defer suite.deletePath(c, firstPart(filename))
//
// 	var offset int64
// 	var misswrites int
// 	var chunkSize int64 = 32
//
// 	for i := 0; i < 1024; i++ {
// 		contents := randomContents(chunkSize)
// 		read, err := suite.StorageDriver.Writer(suite.ctx, filename, offset, bytes.NewReader(contents))
// 		c.Assert(err, check.IsNil)
//
// 		fi, err := suite.StorageDriver.Stat(suite.ctx, filename)
// 		c.Assert(err, check.IsNil)
//
// 		// We are most concerned with being able to read data as soon as Stat declares
// 		// it is uploaded. This is the strongest guarantee that some drivers (that guarantee
// 		// at best eventual consistency) absolutely need to provide.
// 		if fi.Size() == offset+chunkSize {
// 			reader, err := suite.StorageDriver.Reader(suite.ctx, filename, offset)
// 			c.Assert(err, check.IsNil)
//
// 			readContents, err := io.ReadAll(reader)
// 			c.Assert(err, check.IsNil)
//
// 			c.Assert(readContents, check.DeepEquals, contents)
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
// 	c.Assert(misswrites, check.Not(check.Equals), 1024)
// }

// TestWalkParallel ensures that all files are visted by WalkParallel.
func (suite *DriverSuite) TestWalkParallel(c *check.C) {
	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer suite.deletePath(c, rootDirectory)

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
		c.Assert(err, check.IsNil)
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
	c.Assert(err, check.IsNil)

	close(fChan)
	close(dChan)

	wg.Wait()

	sort.Strings(actualFiles)
	sort.Strings(wantedFiles)
	c.Assert(actualFiles, check.DeepEquals, wantedFiles)

	// Convert from a set of wanted directories into a slice.
	wantedDirectories := make([]string, len(wantedDirectoriesSet))

	var i int
	for k := range wantedDirectoriesSet {
		wantedDirectories[i] = k
		i++
	}

	sort.Strings(actualDirectories)
	sort.Strings(wantedDirectories)
	c.Assert(actualDirectories, check.DeepEquals, wantedDirectories)
}

// TestWalkParallelError ensures that walk reports WalkFn errors.
func (suite *DriverSuite) TestWalkParallelError(c *check.C) {
	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer suite.deletePath(c, rootDirectory)

	wantedFiles := randomBranchingFiles(rootDirectory, 100)

	for _, file := range wantedFiles {
		/* #nosec G404 */
		err := suite.StorageDriver.PutContent(suite.ctx, file, randomContents(int64(8+rand.Intn(8))))
		c.Assert(err, check.IsNil)
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
	c.Assert(strings.Contains(err.Error(), innerErr.Error()), check.Equals, true)
}

// TestWalkParallelStopsProcessingOnError ensures that walk stops processing when an error is encountered.
func (suite *DriverSuite) TestWalkParallelStopsProcessingOnError(c *check.C) {
	d := suite.StorageDriver.Name()
	switch d {
	case "filesystem", "azure":
		c.Skip(fmt.Sprintf("%s driver does not support true WalkParallel", d))
	case "gcs":
		parallelWalk := os.Getenv("GCS_PARALLEL_WALK")
		var parallelWalkBool bool
		var err error
		if parallelWalk != "" {
			parallelWalkBool, err = strconv.ParseBool(parallelWalk)
			c.Assert(err, check.IsNil)
		}

		if !parallelWalkBool || parallelWalk == "" {
			c.Skip(fmt.Sprintf("%s driver is not configured with parallelwalk", d))
		}
	}

	rootDirectory := "/" + randomFilenameRange(8, 8)
	defer suite.deletePath(c, rootDirectory)

	numWantedFiles := 1000
	wantedFiles := randomBranchingFiles(rootDirectory, numWantedFiles)

	// Add a file right under the root directory, so that processing is stopped
	// early in the walk cycle.
	errorFile := filepath.Join(rootDirectory, randomFilenameRange(8, 8))
	wantedFiles = append(wantedFiles, errorFile)

	for _, file := range wantedFiles {
		/* #nosec G404 */
		err := suite.StorageDriver.PutContent(suite.ctx, file, randomContents(int64(8+rand.Intn(8))))
		c.Assert(err, check.IsNil)
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

	c.Assert(end.Sub(start) < limit, check.Equals, true)
}

// BenchmarkPutGetEmptyFiles benchmarks PutContent/GetContent for 0B files
func (suite *DriverSuite) BenchmarkPutGetEmptyFiles(c *check.C) {
	suite.benchmarkPutGetFiles(c, 0)
}

// BenchmarkPutGet1KBFiles benchmarks PutContent/GetContent for 1KB files
func (suite *DriverSuite) BenchmarkPutGet1KBFiles(c *check.C) {
	suite.benchmarkPutGetFiles(c, 1024)
}

// BenchmarkPutGet1MBFiles benchmarks PutContent/GetContent for 1MB files
func (suite *DriverSuite) BenchmarkPutGet1MBFiles(c *check.C) {
	suite.benchmarkPutGetFiles(c, 1024*1024)
}

// BenchmarkPutGet1GBFiles benchmarks PutContent/GetContent for 1GB files
func (suite *DriverSuite) BenchmarkPutGet1GBFiles(c *check.C) {
	suite.benchmarkPutGetFiles(c, 1024*1024*1024)
}

func (suite *DriverSuite) benchmarkPutGetFiles(c *check.C, size int64) {
	c.SetBytes(size)
	parentDir := randomPath(8)
	defer func() {
		c.StopTimer()
		suite.StorageDriver.Delete(suite.ctx, firstPart(parentDir))
	}()

	for i := 0; i < c.N; i++ {
		filename := path.Join(parentDir, randomPath(32))
		err := suite.StorageDriver.PutContent(suite.ctx, filename, randomContents(size))
		c.Assert(err, check.IsNil)

		_, err = suite.StorageDriver.GetContent(suite.ctx, filename)
		c.Assert(err, check.IsNil)
	}
}

// BenchmarkStreamEmptyFiles benchmarks Writer/Reader for 0B files
func (suite *DriverSuite) BenchmarkStreamEmptyFiles(c *check.C) {
	if suite.StorageDriver.Name() == "s3aws" {
		c.Skip("S3 multipart uploads require at least 1 chunk (>0B)")
	}
	suite.benchmarkStreamFiles(c, 0)
}

// BenchmarkStream1KBFiles benchmarks Writer/Reader for 1KB files
func (suite *DriverSuite) BenchmarkStream1KBFiles(c *check.C) {
	suite.benchmarkStreamFiles(c, 1024)
}

// BenchmarkStream1MBFiles benchmarks Writer/Reader for 1MB files
func (suite *DriverSuite) BenchmarkStream1MBFiles(c *check.C) {
	suite.benchmarkStreamFiles(c, 1024*1024)
}

// BenchmarkStream1GBFiles benchmarks Writer/Reader for 1GB files
func (suite *DriverSuite) BenchmarkStream1GBFiles(c *check.C) {
	suite.benchmarkStreamFiles(c, 1024*1024*1024)
}

func (suite *DriverSuite) benchmarkStreamFiles(c *check.C, size int64) {
	c.SetBytes(size)
	parentDir := randomPath(8)
	defer func() {
		c.StopTimer()
		suite.StorageDriver.Delete(suite.ctx, firstPart(parentDir))
	}()

	for i := 0; i < c.N; i++ {
		filename := path.Join(parentDir, randomPath(32))
		writer, err := suite.StorageDriver.Writer(suite.ctx, filename, false)
		c.Assert(err, check.IsNil)
		written, err := io.Copy(writer, bytes.NewReader(randomContents(size)))
		c.Assert(err, check.IsNil)
		c.Assert(written, check.Equals, size)

		err = writer.Commit()
		c.Assert(err, check.IsNil)
		err = writer.Close()
		c.Assert(err, check.IsNil)

		rc, err := suite.StorageDriver.Reader(suite.ctx, filename, 0)
		c.Assert(err, check.IsNil)
		rc.Close()
	}
}

// BenchmarkList5Files benchmarks List for 5 small files
func (suite *DriverSuite) BenchmarkList5Files(c *check.C) {
	suite.benchmarkListFiles(c, 5)
}

// BenchmarkList50Files benchmarks List for 50 small files
func (suite *DriverSuite) BenchmarkList50Files(c *check.C) {
	suite.benchmarkListFiles(c, 50)
}

func (suite *DriverSuite) benchmarkListFiles(c *check.C, numFiles int64) {
	parentDir := randomPath(8)
	defer func() {
		c.StopTimer()
		suite.StorageDriver.Delete(suite.ctx, firstPart(parentDir))
	}()

	for i := int64(0); i < numFiles; i++ {
		err := suite.StorageDriver.PutContent(suite.ctx, path.Join(parentDir, randomPath(32)), nil)
		c.Assert(err, check.IsNil)
	}

	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		files, err := suite.StorageDriver.List(suite.ctx, parentDir)
		c.Assert(err, check.IsNil)
		c.Assert(int64(len(files)), check.Equals, numFiles)
	}
}

// BenchmarkDelete5Files benchmarks Delete for 5 small files
func (suite *DriverSuite) BenchmarkDelete5Files(c *check.C) {
	suite.benchmarkDelete(c, 5)
}

// BenchmarkDelete50Files benchmarks Delete for 50 small files
func (suite *DriverSuite) BenchmarkDelete50Files(c *check.C) {
	suite.benchmarkDelete(c, 50)
}

func (suite *DriverSuite) benchmarkDelete(c *check.C, numFiles int64) {
	for i := 0; i < c.N; i++ {
		parentDir := randomPath(8)
		defer suite.deletePath(c, firstPart(parentDir))

		c.StopTimer()
		for j := int64(0); j < numFiles; j++ {
			err := suite.StorageDriver.PutContent(suite.ctx, path.Join(parentDir, randomPath(32)), nil)
			c.Assert(err, check.IsNil)
		}
		c.StartTimer()

		// This is the operation we're benchmarking
		err := suite.StorageDriver.Delete(suite.ctx, firstPart(parentDir))
		c.Assert(err, check.IsNil)
	}
}

// BenchmarkWalkParallelNop10Files benchmarks WalkParallel with a Nop function that visits 10 files
func (suite *DriverSuite) BenchmarkWalkParallelNop10Files(c *check.C) {
	suite.benchmarkWalkParallel(c, 10, func(fInfo storagedriver.FileInfo) error {
		return nil
	})
}

// BenchmarkWalkParallelNop500Files benchmarks WalkParallel with a Nop function that visits 500 files
func (suite *DriverSuite) BenchmarkWalkParallelNop500Files(c *check.C) {
	suite.benchmarkWalkParallel(c, 500, func(fInfo storagedriver.FileInfo) error {
		return nil
	})
}

func (suite *DriverSuite) benchmarkWalkParallel(c *check.C, numFiles int, f storagedriver.WalkFn) {
	for i := 0; i < c.N; i++ {
		rootDirectory := "/" + randomFilenameRange(8, 8)
		defer suite.deletePath(c, rootDirectory)

		c.StopTimer()

		wantedFiles := randomBranchingFiles(rootDirectory, numFiles)

		for i := 0; i < numFiles; i++ {
			/* #nosec G404 */
			err := suite.StorageDriver.PutContent(suite.ctx, wantedFiles[i], randomContents(int64(8+rand.Intn(8))))
			c.Assert(err, check.IsNil)
		}

		c.StartTimer()

		err := suite.StorageDriver.WalkParallel(suite.ctx, rootDirectory, f)
		c.Assert(err, check.IsNil)
	}
}

func (suite *DriverSuite) createRegistry(c *check.C, options ...storage.RegistryOption) distribution.Namespace {
	k, err := libtrust.GenerateECP256PrivateKey()
	if err != nil {
		c.Fatal(err)
	}
	options = append([]storage.RegistryOption{storage.EnableDelete, storage.Schema1SigningKey(k), storage.EnableSchema1}, options...)
	registry, err := storage.NewRegistry(suite.ctx, suite.StorageDriver, options...)
	if err != nil {
		c.Fatalf("Failed to construct namespace")
	}
	return registry
}

func (suite *DriverSuite) makeRepository(c *check.C, registry distribution.Namespace, name string) distribution.Repository {
	named, err := reference.WithName(name)
	if err != nil {
		c.Fatalf("Failed to parse name %s:  %v", name, err)
	}

	repo, err := registry.Repository(suite.ctx, named)
	if err != nil {
		c.Fatalf("Failed to construct repository: %v", err)
	}
	return repo
}

// BenchmarkMarkAndSweep10ImagesKeepUntagged uploads 10 images, deletes half
// and runs garbage collection on the registry without removing untaged images.
func (suite *DriverSuite) BenchmarkMarkAndSweep10ImagesKeepUntagged(c *check.C) {
	suite.benchmarkMarkAndSweep(c, 10, false)
}

// BenchmarkMarkAndSweep50ImagesKeepUntagged uploads 50 images, deletes half
// and runs garbage collection on the registry without removing untaged images.
func (suite *DriverSuite) BenchmarkMarkAndSweep50ImagesKeepUntagged(c *check.C) {
	suite.benchmarkMarkAndSweep(c, 50, false)
}

func (suite *DriverSuite) benchmarkMarkAndSweep(c *check.C, numImages int, removeUntagged bool) {
	// Setup for this test takes a long time, even with small numbers of images,
	// so keep the skip logic here in the sub test.
	if testing.Short() {
		c.Skip("Skipping test in short mode")
	}

	defer suite.deletePath(c, firstPart("docker/"))

	for n := 0; n < c.N; n++ {
		c.StopTimer()

		registry := suite.createRegistry(c)
		repo := suite.makeRepository(c, registry, fmt.Sprintf("benchmarks-repo-%d", n))

		manifests, err := repo.Manifests(suite.ctx)
		c.Assert(err, check.IsNil)

		images := make([]testutil.Image, numImages)

		for i := 0; i < numImages; i++ {
			// Alternate between Schema1 and Schema2 images
			if i%2 == 0 {
				images[i], err = testutil.UploadRandomSchema1Image(repo)
				c.Assert(err, check.IsNil)
			} else {
				images[i], err = testutil.UploadRandomSchema2Image(repo)
				c.Assert(err, check.IsNil)
			}

			// Delete the manifests, so that their blobs can be garbage collected.
			manifests.Delete(suite.ctx, images[i].ManifestDigest)
		}

		c.StartTimer()

		// Run GC
		err = storage.MarkAndSweep(context.Background(), suite.StorageDriver, registry, storage.GCOpts{
			DryRun:         false,
			RemoveUntagged: removeUntagged,
		})
		c.Assert(err, check.IsNil)
	}
}

func (suite *DriverSuite) buildBlobs(c *check.C, repo distribution.Repository, n int) []digest.Digest {
	dgsts := make([]digest.Digest, 0, n)

	// build and upload random layers
	layers, err := testutil.CreateRandomLayers(n)
	if err != nil {
		c.Fatalf("failed to create random digest: %v", err)
	}
	if err = testutil.UploadBlobs(repo, layers); err != nil {
		c.Fatalf("failed to upload blob: %v", err)
	}

	// collect digests from layers map
	for d := range layers {
		dgsts = append(dgsts, d)
	}

	return dgsts
}

// TestRemoveBlob checks that storage.Vacuum is able to delete a single blob.
func (suite *DriverSuite) TestRemoveBlob(c *check.C) {
	defer suite.deletePath(c, firstPart("docker/"))

	registry := suite.createRegistry(c)
	repo := suite.makeRepository(c, registry, randomFilename(5))
	v := storage.NewVacuum(suite.StorageDriver)

	// build two blobs, one more than the number to delete, otherwise there will be no /docker/registry/v2/blobs path
	// for validation after delete
	blobs := suite.buildBlobs(c, repo, 2)
	blob := blobs[0]

	err := v.RemoveBlob(suite.ctx, blob)
	c.Assert(err, check.IsNil)

	blobService := registry.Blobs()
	blobsLeft := newSyncDigestSet()
	err = blobService.Enumerate(suite.ctx, func(desc distribution.Descriptor) error {
		blobsLeft.add(desc.Digest)
		return nil
	})
	if err != nil {
		c.Fatalf("error getting all blobs: %v", err)
	}

	c.Assert(blobsLeft.len(), check.Equals, 1)
	if blobsLeft.contains(blob) {
		c.Errorf("blob %q was not deleted", blob.String())
	}
}

func (suite *DriverSuite) benchmarkRemoveBlob(c *check.C, numBlobs int) {
	defer suite.deletePath(c, firstPart("docker/"))

	registry := suite.createRegistry(c)
	repo := suite.makeRepository(c, registry, randomFilename(5))
	v := storage.NewVacuum(suite.StorageDriver)

	for n := 0; n < c.N; n++ {
		c.StopTimer()
		blobs := suite.buildBlobs(c, repo, numBlobs)
		c.StartTimer()

		for _, b := range blobs {
			err := v.RemoveBlob(suite.ctx, b)
			c.Assert(err, check.IsNil)
		}
	}
}

// BenchmarkRemoveBlob1Blob creates 1 blob and deletes it using the storage.Vacuum.RemoveBlob method.
func (suite *DriverSuite) BenchmarkRemoveBlob1Blob(c *check.C) {
	suite.benchmarkRemoveBlob(c, 1)
}

// BenchmarkRemoveBlob10Blobs creates 10 blobs and deletes them using the storage.Vacuum.RemoveBlob method.
func (suite *DriverSuite) BenchmarkRemoveBlob10Blobs(c *check.C) {
	suite.benchmarkRemoveBlob(c, 10)
}

// BenchmarkRemoveBlob100Blobs creates 100 blobs and deletes them using the storage.Vacuum.RemoveBlob method.
func (suite *DriverSuite) BenchmarkRemoveBlob100Blobs(c *check.C) {
	suite.benchmarkRemoveBlob(c, 100)
}

// TestRemoveBlobs checks that storage.Vacuum is able to delete a set of blobs in bulk.
func (suite *DriverSuite) TestRemoveBlobs(c *check.C) {
	defer suite.deletePath(c, firstPart("docker/"))

	registry := suite.createRegistry(c)
	repo := suite.makeRepository(c, registry, randomFilename(5))
	v := storage.NewVacuum(suite.StorageDriver)

	// build some blobs and remove half of them, otherwise there will be no /docker/registry/v2/blobs path to look at
	// for validation if there are no blobs left
	blobs := suite.buildBlobs(c, repo, 4)
	blobs = blobs[:2]

	err := v.RemoveBlobs(suite.ctx, blobs)
	c.Assert(err, check.IsNil)

	// assert that blobs were deleted
	blobService := registry.Blobs()
	blobsLeft := newSyncDigestSet()
	err = blobService.Enumerate(suite.ctx, func(desc distribution.Descriptor) error {
		blobsLeft.add(desc.Digest)
		return nil
	})
	if err != nil {
		c.Fatalf("error getting all blobs: %v", err)
	}

	c.Assert(blobsLeft.len(), check.Equals, 2)
	for _, b := range blobs {
		if blobsLeft.contains(b) {
			c.Errorf("blob %q was not deleted", b.String())
		}
	}
}

func (suite *DriverSuite) benchmarkRemoveBlobs(c *check.C, numBlobs int) {
	defer suite.deletePath(c, firstPart("docker/"))

	registry := suite.createRegistry(c)
	repo := suite.makeRepository(c, registry, randomFilename(5))
	v := storage.NewVacuum(suite.StorageDriver)

	for n := 0; n < c.N; n++ {
		c.StopTimer()
		blobs := suite.buildBlobs(c, repo, numBlobs)
		c.StartTimer()

		err := v.RemoveBlobs(suite.ctx, blobs)
		c.Assert(err, check.IsNil)
	}
}

// BenchmarkRemoveBlobs1Blob creates 1 blob and deletes it using the storage.Vacuum.RemoveBlobs method.
func (suite *DriverSuite) BenchmarkRemoveBlobs1Blob(c *check.C) {
	suite.benchmarkRemoveBlobs(c, 1)
}

// BenchmarkRemoveBlobs10Blobs creates 10 blobs and deletes them using the storage.Vacuum.RemoveBlobs method.
func (suite *DriverSuite) BenchmarkRemoveBlobs10Blobs(c *check.C) {
	suite.benchmarkRemoveBlobs(c, 10)
}

// BenchmarkRemoveBlobs100Blobs creates 100 blobs and deletes them using the storage.Vacuum.RemoveBlobs method.
func (suite *DriverSuite) BenchmarkRemoveBlobs100Blobs(c *check.C) {
	suite.benchmarkRemoveBlobs(c, 100)
}

// BenchmarkRemoveBlobs1000Blobs creates 1000 blobs and deletes them using the storage.Vacuum.RemoveBlobs method.
func (suite *DriverSuite) BenchmarkRemoveBlobs1000Blobs(c *check.C) {
	suite.benchmarkRemoveBlobs(c, 1000)
}

func (suite *DriverSuite) buildManifests(c *check.C, repo distribution.Repository, numManifests, numTagsPerManifest int) []storage.ManifestDel {
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
		c.Assert(err, check.IsNil)

		// build numTags tags per manifest
		tags := make([]string, 0, numTagsPerManifest)
		for j := 0; j < numTagsPerManifest; j++ {
			t := randomFilename(5)
			d := images[i].ManifestDigest
			err := repo.Tags(suite.ctx).Tag(suite.ctx, t, distribution.Descriptor{Digest: d})
			c.Assert(err, check.IsNil)
			tags = append(tags, t)
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
func (suite *DriverSuite) TestRemoveManifests(c *check.C) {
	defer suite.deletePath(c, firstPart("docker/"))

	registry := suite.createRegistry(c)
	repo := suite.makeRepository(c, registry, randomFilename(5))

	// build some manifests
	manifests := suite.buildManifests(c, repo, 3, 1)

	v := storage.NewVacuum(suite.StorageDriver)

	// remove all manifests except one, otherwise there will be no `_manifests/revisions` folder to look at for
	// validation (empty "folders" are not preserved)
	numToDelete := len(manifests) - 1
	toDelete := manifests[:numToDelete]

	err := v.RemoveManifests(suite.ctx, toDelete)
	c.Assert(err, check.IsNil)

	// assert that toDelete manifests were actually deleted
	manifestsLeft := newSyncDigestSet()
	manifestService, err := repo.Manifests(suite.ctx)
	if err != nil {
		c.Fatalf("error building manifest service: %v", err)
	}
	manifestEnumerator, ok := manifestService.(distribution.ManifestEnumerator)
	if !ok {
		c.Fatalf("unable to convert ManifestService into ManifestEnumerator")
	}
	err = manifestEnumerator.Enumerate(suite.ctx, func(dgst digest.Digest) error {
		manifestsLeft.add(dgst)
		return nil
	})
	if err != nil {
		c.Fatalf("error getting all manifests: %v", err)
	}

	c.Assert(manifestsLeft.len(), check.Equals, len(manifests)-numToDelete)

	for _, m := range toDelete {
		if manifestsLeft.contains(m.Digest) {
			c.Errorf("manifest %q was not deleted as expected", m.Digest)
		}
	}
}

func (suite *DriverSuite) testRemoveManifestsPathBuild(c *check.C, numManifests, numTagsPerManifest int) {
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
	c.Assert(err, check.IsNil)
}

// TestRemoveManifestsPathBuildLargeScale simulates the execution of vacuum.RemoveManifests for repositories with large
// numbers of manifests eligible for deletion. No files are created in this test, we only simulate their existence so
// that we can test and profile the execution of the path build process within vacuum.RemoveManifests. The storage
// drivers DeleteFiles method is idempotent, so no error will be raised by attempting to delete non-existing files.
// However, to avoid large number of HTTP requests against cloud storage backends, it's recommended to run this test
// against the filesystem storage backend only. For safety, the test is skipped when not using the filesystem storage
// backend. Tweak the method locally to test use cases with different sizes and/or storage drivers.
func (suite *DriverSuite) TestRemoveManifestsPathBuildLargeScale(c *check.C) {
	if testing.Short() {
		c.Skip("Skipping test in short mode")
	}
	if suite.StorageDriver.Name() != "filesystem" {
		c.Skip(fmt.Sprintf("Skipping test for the %s driver", suite.StorageDriver.Name()))
	}

	numManifests := 100
	numTagsPerManifest := 10

	suite.testRemoveManifestsPathBuild(c, numManifests, numTagsPerManifest)
}

func (suite *DriverSuite) benchmarkRemoveManifests(c *check.C, numManifests, numTagsPerManifest int) {
	if testing.Short() {
		c.Skip("Skipping test in short mode")
	}

	defer suite.deletePath(c, firstPart("docker/"))

	registry := suite.createRegistry(c)
	repo := suite.makeRepository(c, registry, randomFilename(5))

	for n := 0; n < c.N; n++ {
		c.StopTimer()

		manifests := suite.buildManifests(c, repo, numManifests, numTagsPerManifest)
		v := storage.NewVacuum(suite.StorageDriver)

		c.StartTimer()

		err := v.RemoveManifests(suite.ctx, manifests)
		c.Assert(err, check.IsNil)
	}
}

// BenchmarkRemoveManifests1Manifest0Tags creates 1 manifest with no tags and deletes it using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests1Manifest0Tags(c *check.C) {
	suite.benchmarkRemoveManifests(c, 1, 0)
}

// BenchmarkRemoveManifests1Manifest1Tag creates 1 manifest with 1 tag and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests1Manifest1Tag(c *check.C) {
	suite.benchmarkRemoveManifests(c, 1, 1)
}

// BenchmarkRemoveManifests10Manifests0TagsEach creates 10 manifests with no tags and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests10Manifests0TagsEach(c *check.C) {
	suite.benchmarkRemoveManifests(c, 10, 0)
}

// BenchmarkRemoveManifests10Manifests1TagEach creates 10 manifests with 1 tag each and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests10Manifests1TagEach(c *check.C) {
	suite.benchmarkRemoveManifests(c, 10, 1)
}

// BenchmarkRemoveManifests100Manifests0TagsEach creates 100 manifests with no tags and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests100Manifests0TagsEach(c *check.C) {
	suite.benchmarkRemoveManifests(c, 100, 0)
}

// BenchmarkRemoveManifests100Manifests1TagEach creates 100 manifests with 1 tag each and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests100Manifests1TagEach(c *check.C) {
	suite.benchmarkRemoveManifests(c, 100, 1)
}

// BenchmarkRemoveManifests100Manifests20TagsEach creates 100 manifests with 20 tags each and deletes them using the
// storage.Vacuum.RemoveManifests method.
func (suite *DriverSuite) BenchmarkRemoveManifests100Manifests20TagsEach(c *check.C) {
	suite.benchmarkRemoveManifests(c, 100, 20)
}

func (suite *DriverSuite) testFileStreams(c *check.C, size int64) {
	tf, err := os.CreateTemp("", "tf")
	c.Assert(err, check.IsNil)
	defer os.Remove(tf.Name())
	defer tf.Close()

	filename := randomPath(32)
	defer suite.deletePath(c, firstPart(filename))

	contents := randomContents(size)

	_, err = tf.Write(contents)
	c.Assert(err, check.IsNil)

	tf.Sync()
	tf.Seek(0, io.SeekStart)

	writer, err := suite.StorageDriver.Writer(suite.ctx, filename, false)
	c.Assert(err, check.IsNil)
	nn, err := io.Copy(writer, tf)
	c.Assert(err, check.IsNil)
	c.Assert(nn, check.Equals, size)

	err = writer.Commit()
	c.Assert(err, check.IsNil)
	err = writer.Close()
	c.Assert(err, check.IsNil)

	reader, err := suite.StorageDriver.Reader(suite.ctx, filename, 0)
	c.Assert(err, check.IsNil)
	defer reader.Close()

	readContents, err := io.ReadAll(reader)
	c.Assert(err, check.IsNil)

	c.Assert(readContents, check.DeepEquals, contents)
}

func (suite *DriverSuite) writeReadCompare(c *check.C, filename string, contents []byte) {
	defer suite.deletePath(c, firstPart(filename))

	err := suite.StorageDriver.PutContent(suite.ctx, filename, contents)
	c.Assert(err, check.IsNil)

	readContents, err := suite.StorageDriver.GetContent(suite.ctx, filename)
	c.Assert(err, check.IsNil)

	c.Assert(readContents, check.DeepEquals, contents)
}

func (suite *DriverSuite) writeReadCompareStreams(c *check.C, filename string, contents []byte) {
	defer suite.deletePath(c, firstPart(filename))

	writer, err := suite.StorageDriver.Writer(suite.ctx, filename, false)
	c.Assert(err, check.IsNil)
	nn, err := io.Copy(writer, bytes.NewReader(contents))
	c.Assert(err, check.IsNil)
	c.Assert(nn, check.Equals, int64(len(contents)))

	err = writer.Commit()
	c.Assert(err, check.IsNil)
	err = writer.Close()
	c.Assert(err, check.IsNil)

	reader, err := suite.StorageDriver.Reader(suite.ctx, filename, 0)
	c.Assert(err, check.IsNil)
	defer reader.Close()

	readContents, err := io.ReadAll(reader)
	c.Assert(err, check.IsNil)

	c.Assert(readContents, check.DeepEquals, contents)
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
