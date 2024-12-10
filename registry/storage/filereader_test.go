package storage

import (
	"bytes"
	"io"
	mrand "math/rand"
	"testing"

	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage/driver/filesystem"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestSimpleRead(t *testing.T) {
	ctx := context.Background()
	content := make([]byte, 1<<20)
	n, err := mrand.Read(content)
	require.NoError(t, err, "unexpected error building random data")

	require.Equal(t, len(content), n, "random read didn't fill buffer")

	dgst, err := digest.FromReader(bytes.NewReader(content))
	require.NoError(t, err, "unexpected error digesting random content")

	driver := inmemory.New()
	path := "/random"

	require.NoError(t, driver.PutContent(ctx, path, content), "error putting patterned content")

	fr := newFileReader(ctx, driver, path, int64(len(content)))
	require.NoError(t, err, "error allocating file reader")
	defer fr.Close()

	verifier := dgst.Verifier()
	_, err = io.Copy(verifier, fr)
	require.NoError(t, err)

	require.True(t, verifier.Verified(), "unable to verify read data")
}

func TestFileReaderSeek(t *testing.T) {
	// With Go 1.18, the inmemory driver fails due to issues with changes to the
	// implementation of io.SectionReader:
	// https://github.com/golang/go/commit/12e8ffc18e84a76f8e01457852c456a3b28ec55a#diff-b8f7b0d27cd167384fb8c925c93ab5340faa29a00ac39746e2d5b527476e81c7R503
	// While switching to another driver implementation is not ideal, inmemory
	// driver should only be used for tests, and the only issue caused by this
	// change the core library is this test failing.
	driver, err := filesystem.FromParameters(map[string]any{"rootdirectory": t.TempDir()})
	require.NoError(t, err)

	pattern := "01234567890ab" // prime length block
	repititions := 1024
	path := "/patterned"
	content := bytes.Repeat([]byte(pattern), repititions)
	ctx := context.Background()

	require.NoError(t, driver.PutContent(ctx, path, content), "error putting patterned content")

	fr := newFileReader(ctx, driver, path, int64(len(content)))
	require.NoError(t, err, "unexpected error creating file reader")

	defer fr.Close()

	// Seek all over the place, in blocks of pattern size and make sure we get
	// the right data.
	for _, repitition := range mrand.Perm(repititions - 1) {
		targetOffset := int64(len(pattern) * repitition)
		// Seek to a multiple of pattern size and read pattern size bytes
		offset, err := fr.Seek(targetOffset, io.SeekStart)
		require.NoError(t, err, "unexpected error seeking")

		require.Equal(t, targetOffset, offset, "did not seek to correct offset")

		p := make([]byte, len(pattern))

		n, err := fr.Read(p)
		require.NoError(t, err, "error reading pattern")

		require.Len(t, pattern, n, "incorrect read length")

		require.Equal(t, pattern, string(p), "incorrect read content")

		// Check offset
		current, err := fr.Seek(0, io.SeekCurrent)
		require.NoError(t, err, "error checking current offset")

		require.Equal(t, targetOffset+int64(len(pattern)), current, "unexpected offset after read")
	}

	start, err := fr.Seek(0, io.SeekStart)
	require.NoError(t, err, "error seeking to start")

	require.Zero(t, start, "expected to seek to start")

	end, err := fr.Seek(0, io.SeekEnd)
	require.NoError(t, err, "error checking current offset")

	require.Equal(t, int64(len(content)), end, "expected to seek to end")
	// 4. Seek before start, ensure error.

	// seek before start
	before, err := fr.Seek(-1, io.SeekStart)
	require.Error(t, err, "error expected, returned offset=%v", before)

	// 5. Seek after end,
	after, err := fr.Seek(1, io.SeekEnd)
	require.NoError(t, err, "unexpected error expected, returned offset=%v", after)

	p := make([]byte, 16)
	n, err := fr.Read(p)

	require.Zero(t, n, "bytes reads")

	require.ErrorIs(t, err, io.EOF, "expected io.EOF")
}

// TestFileReaderNonExistentFile ensures the reader behaves as expected with a
// missing or zero-length remote file. While the file may not exist, the
// reader should not error out on creation and should return 0-bytes from the
// read method, with an io.EOF error.
func TestFileReaderNonExistentFile(t *testing.T) {
	driver := inmemory.New()
	fr := newFileReader(context.Background(), driver, "/doesnotexist", 10)
	defer fr.Close()

	var buf [1024]byte

	n, err := fr.Read(buf[:])
	require.Zero(t, n, "non-zero byte read reported")

	require.ErrorIs(t, err, io.EOF, "read on missing file should return io.EOF")
}

// TestLayerReadErrors covers the various error return type for different
// conditions that can arise when reading a layer.
func TestFileReaderErrors(_ *testing.T) {
	// TODO(stevvooe): We need to cover error return types, driven by the
	// errors returned via the HTTP API. For now, here is an incomplete list:
	//
	// 	1. Layer Not Found: returned when layer is not found or access is
	//        denied.
	//	2. Layer Unavailable: returned when link references are unresolved,
	//     but layer is known to the registry.
	//  3. Layer Invalid: This may more split into more errors, but should be
	//     returned when name or tarsum does not reference a valid error. We
	//     may also need something to communication layer verification errors
	//     for the inline tarsum check.
	//	4. Timeout: timeouts to backend. Need to better understand these
	//     failure cases and how the storage driver propagates these errors
	//     up the stack.
}
