package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"path"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/cache/memory"
	"github.com/docker/distribution/registry/storage/driver/testdriver"
	"github.com/docker/distribution/testutil"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

// TestWriteSeek tests that the current file size can be
// obtained using Seek
func TestWriteSeek(t *testing.T) {
	ctx := context.Background()
	imageName, _ := reference.WithName("foo/bar")
	driver := testdriver.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), EnableDelete, EnableRedirect)
	require.NoError(t, err, "error creating registry")
	repository, err := registry.Repository(ctx, imageName)
	require.NoError(t, err, "unexpected error getting repo")
	bs := repository.Blobs(ctx)

	blobUpload, err := bs.Create(ctx)
	require.NoError(t, err, "unexpected error starting layer upload")
	contents := []byte{1, 2, 3}
	blobUpload.Write(contents)
	blobUpload.Close()
	offset := blobUpload.Size()
	require.Equal(t, int64(len(contents)), offset, "unexpected value for blobUpload offset")
}

// TestSimpleBlobUpload covers the blob upload process, exercising common
// error paths that might be seen during an upload.
func TestSimpleBlobUpload(t *testing.T) {
	randomDataReader, dgst, err := testutil.CreateRandomTarFile()
	require.NoError(t, err, "error creating random reader")

	ctx := context.Background()
	imageName, _ := reference.WithName("foo/bar")
	driver := testdriver.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), EnableDelete, EnableRedirect)
	require.NoError(t, err, "error creating registry")
	repository, err := registry.Repository(ctx, imageName)
	require.NoError(t, err, "unexpected error getting repo")
	bs := repository.Blobs(ctx)

	h := sha256.New()
	rd := io.TeeReader(randomDataReader, h)

	blobUpload, err := bs.Create(ctx)
	require.NoError(t, err, "unexpected error starting layer upload")

	// Cancel the upload then restart it
	require.NoError(t, blobUpload.Cancel(ctx), "unexpected error during upload cancellation")

	// get the enclosing directory
	uploadPath := path.Dir(blobUpload.(*blobWriter).path)

	// ensure state was cleaned up
	_, err = driver.List(ctx, uploadPath)
	require.Error(t, err, "files in upload path after cleanup")

	// Do a resume, get unknown upload
	_, err = bs.Resume(ctx, blobUpload.ID())
	require.ErrorIs(t, err, distribution.ErrBlobUploadUnknown)

	// Restart!
	blobUpload, err = bs.Create(ctx)
	require.NoError(t, err, "unexpected error starting layer upload")

	// Get the size of our random tarfile
	randomDataSize, err := seekerSize(randomDataReader)
	require.NoError(t, err, "error getting seeker size of random data")

	nn, err := io.Copy(blobUpload, rd)
	require.NoError(t, err, "unexpected error uploading layer data")

	require.Equal(t, randomDataSize, nn, "layer data write incomplete")

	blobUpload.Close()

	offset := blobUpload.Size()
	require.Equal(t, nn, offset, "blobUpload not updated with correct offset")

	// Do a resume, for good fun
	blobUpload, err = bs.Resume(ctx, blobUpload.ID())
	require.NoError(t, err, "unexpected error resuming upload")

	sha256Digest := digest.NewDigest("sha256", h)
	desc, err := blobUpload.Commit(ctx, distribution.Descriptor{Digest: dgst})
	require.NoError(t, err, "unexpected error finishing layer upload")

	// ensure state was cleaned up
	uploadPath = path.Dir(blobUpload.(*blobWriter).path)
	_, err = driver.List(ctx, uploadPath)
	require.Error(t, err, "files in upload path after commit")

	// After finishing an upload, it should no longer exist.
	_, err = bs.Resume(ctx, blobUpload.ID())
	require.ErrorIs(t, err, distribution.ErrBlobUploadUnknown, "expected layer upload to be unknown")

	// Test for existence.
	statDesc, err := bs.Stat(ctx, desc.Digest)
	require.NoError(t, err, "unexpected error checking for existence")

	require.Equal(t, statDesc, desc, "descriptors not equal")

	rc, err := bs.Open(ctx, desc.Digest)
	require.NoError(t, err, "unexpected error opening blob for read")
	defer rc.Close()

	h.Reset()
	nn, err = io.Copy(h, rc)
	require.NoError(t, err, "error reading layer")

	require.Equal(t, randomDataSize, nn, "incorrect read length")

	require.Equal(t, sha256Digest, digest.NewDigest("sha256", h), "unexpected digest from uploaded layer")

	// Delete a blob
	err = bs.Delete(ctx, desc.Digest)
	require.NoError(t, err, "unexpected error deleting blob")

	d, err := bs.Stat(ctx, desc.Digest)
	require.Errorf(t, err, "unexpected non-error stating deleted blob: %v", d)

	require.ErrorIs(t, err, distribution.ErrBlobUnknown)

	_, err = bs.Open(ctx, desc.Digest)
	require.Error(t, err, "unexpected success opening deleted blob for read")

	require.ErrorIs(t, err, distribution.ErrBlobUnknown)

	// Re-upload the blob
	randomBlob, err := io.ReadAll(randomDataReader)
	require.NoError(t, err, "error reading all of blob")
	expectedDigest := digest.FromBytes(randomBlob)
	simpleUpload(t, bs, randomBlob, expectedDigest)

	d, err = bs.Stat(ctx, expectedDigest)
	require.NoError(t, err, "unexpected error stat-ing blob")
	require.Equal(t, expectedDigest, d.Digest, "mismatching digest with restored blob")

	br, err := bs.Open(ctx, expectedDigest)
	require.NoError(t, err, "unexpected error opening blob")
	defer br.Close()

	// Reuse state to test delete with a delete-disabled registry
	registry, err = NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), EnableRedirect)
	require.NoError(t, err, "error creating registry")
	repository, err = registry.Repository(ctx, imageName)
	require.NoError(t, err, "unexpected error getting repo")
	bs = repository.Blobs(ctx)
	err = bs.Delete(ctx, desc.Digest)
	require.Error(t, err, "unexpected success deleting while disabled")
}

// TestSimpleBlobRead just creates a simple blob file and ensures that basic
// open, read, seek, read works. More specific edge cases should be covered in
// other tests.
func TestSimpleBlobRead(t *testing.T) {
	ctx := context.Background()
	imageName, _ := reference.WithName("foo/bar")
	driver := testdriver.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), EnableDelete, EnableRedirect)
	require.NoError(t, err, "error creating registry")
	repository, err := registry.Repository(ctx, imageName)
	require.NoError(t, err, "unexpected error getting repo")
	bs := repository.Blobs(ctx)

	randomLayerReader, dgst, err := testutil.CreateRandomTarFile()
	require.NoError(t, err, "error creating random data")

	// Test for existence.
	_, err = bs.Stat(ctx, dgst)
	require.ErrorIs(t, err, distribution.ErrBlobUnknown)

	_, err = bs.Open(ctx, dgst)
	require.ErrorIs(t, err, distribution.ErrBlobUnknown)

	randomLayerSize, err := seekerSize(randomLayerReader)
	require.NoError(t, err, "error getting seeker size for random layer")

	descBefore := distribution.Descriptor{Digest: dgst, MediaType: "application/octet-stream", Size: randomLayerSize}
	t.Logf("desc: %v", descBefore)

	desc, err := addBlob(ctx, bs, descBefore, randomLayerReader)
	require.NoError(t, err, "error adding blob to blobservice")

	require.Equal(t, desc.Size, randomLayerSize, "committed blob has incorrect length")

	rc, err := bs.Open(ctx, desc.Digest) // note that we are opening with original digest.
	require.NoError(t, err, "error opening blob with %v", dgst)
	defer rc.Close()

	// Now check the sha digest and ensure its the same
	h := sha256.New()
	nn, err := io.Copy(h, rc)
	require.NoError(t, err, "unexpected error copying to hash")

	require.Equal(t, nn, randomLayerSize, "stored incorrect number of bytes in blob")

	sha256Digest := digest.NewDigest("sha256", h)
	require.Equal(t, sha256Digest, desc.Digest, "fetched digest does not match")

	// Now seek back the blob, read the whole thing and check against randomLayerData
	offset, err := rc.Seek(0, io.SeekStart)
	require.NoError(t, err, "error seeking blob")

	require.Zerof(t, offset, "seek failed: expected 0 offset, got %d", offset)

	p, err := io.ReadAll(rc)
	require.NoError(t, err, "error reading all of blob")

	require.Len(t, p, int(randomLayerSize), "blob data read has different length")

	// Reset the randomLayerReader and read back the buffer
	_, err = randomLayerReader.Seek(0, io.SeekStart)
	require.NoError(t, err, "error resetting layer reader")

	randomLayerData, err := io.ReadAll(randomLayerReader)
	require.NoError(t, err, "random layer read failed")

	require.Equal(t, p, randomLayerData, "layer data not equal")
}

// TestBlobMount covers the blob mount process, exercising common
// error paths that might be seen during a mount.
func TestBlobMount(t *testing.T) {
	randomDataReader, dgst, err := testutil.CreateRandomTarFile()
	require.NoError(t, err, "error creating random reader")

	ctx := context.Background()
	imageName, _ := reference.WithName("foo/bar")
	sourceImageName, _ := reference.WithName("foo/source")
	driver := testdriver.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), EnableDelete, EnableRedirect)
	require.NoError(t, err, "error creating registry")

	repository, err := registry.Repository(ctx, imageName)
	require.NoError(t, err, "unexpected error getting repo")
	sourceRepository, err := registry.Repository(ctx, sourceImageName)
	require.NoError(t, err, "unexpected error getting repo")

	sbs := sourceRepository.Blobs(ctx)

	blobUpload, err := sbs.Create(ctx)
	require.NoError(t, err, "unexpected error starting layer upload")

	// Get the size of our random tarfile
	randomDataSize, err := seekerSize(randomDataReader)
	require.NoError(t, err, "error getting seeker size of random data")

	_, err = io.Copy(blobUpload, randomDataReader)
	require.NoError(t, err, "unexpected error uploading layer data")

	desc, err := blobUpload.Commit(ctx, distribution.Descriptor{Digest: dgst})
	require.NoError(t, err, "unexpected error finishing layer upload")

	// Test for existence.
	statDesc, err := sbs.Stat(ctx, desc.Digest)
	require.NoErrorf(t, err, "unexpected error checking for existence (sbs: %#v)", sbs)

	require.Equal(t, statDesc, desc, "descriptors not equal")

	bs := repository.Blobs(ctx)
	// Test destination for existence.
	_, err = bs.Stat(ctx, desc.Digest)
	require.Errorf(t, err, "unexpected non-error stating unmounted blob: %v", desc)

	canonicalRef, err := reference.WithDigest(sourceRepository.Named(), desc.Digest)
	require.NoError(t, err)

	bw, err := bs.Create(ctx, WithMountFrom(canonicalRef))
	require.Nil(t, bw, "unexpected blobwriter returned from Create call, should mount instead")

	var ebm distribution.ErrBlobMounted
	require.ErrorAs(t, err, &ebm)

	require.Equal(t, ebm.Descriptor, desc, "descriptors not equal")

	// Test for existence.
	statDesc, err = bs.Stat(ctx, desc.Digest)
	require.NoErrorf(t, err, "unexpected error checking for existence (bs: %#v)", bs)

	require.Equal(t, statDesc, desc, "descriptors not equal")

	rc, err := bs.Open(ctx, desc.Digest)
	require.NoError(t, err, "unexpected error opening blob for read")
	defer rc.Close()

	h := sha256.New()
	nn, err := io.Copy(h, rc)
	require.NoError(t, err, "error reading layer")

	require.Equal(t, nn, randomDataSize, "incorrect read length")

	require.Equal(t, digest.NewDigest("sha256", h), dgst, "unexpected digest from uploaded layer")

	// Delete the blob from the source repo
	err = sbs.Delete(ctx, desc.Digest)
	require.NoError(t, err, "Unexpected error deleting blob")

	_, err = bs.Stat(ctx, desc.Digest)
	require.NoError(t, err, "unexpected error stating blob deleted from source repository")

	d, err := sbs.Stat(ctx, desc.Digest)
	require.Errorf(t, err, "unexpected non-error stating deleted blob: %v", d)

	require.ErrorIs(t, err, distribution.ErrBlobUnknown, "Unexpected error type stat-ing deleted manifest")

	// Delete the blob from the dest repo
	err = bs.Delete(ctx, desc.Digest)
	require.NoError(t, err, "Unexpected error deleting blob")

	d, err = bs.Stat(ctx, desc.Digest)
	require.Errorf(t, err, "unexpected non-error stating deleted blob: %v", d)

	require.ErrorIs(t, err, distribution.ErrBlobUnknown, "Unexpected error type stat-ing deleted manifest")
}

// TestLayerUploadZeroLength uploads zero-length
func TestLayerUploadZeroLength(t *testing.T) {
	ctx := context.Background()
	imageName, _ := reference.WithName("foo/bar")
	driver := testdriver.New()
	registry, err := NewRegistry(ctx, driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), EnableDelete, EnableRedirect)
	require.NoError(t, err, "error creating registry")
	repository, err := registry.Repository(ctx, imageName)
	require.NoError(t, err, "unexpected error getting repo")
	bs := repository.Blobs(ctx)

	simpleUpload(t, bs, make([]byte, 0), digestSha256Empty)
}

func simpleUpload(t *testing.T, bs distribution.BlobIngester, blob []byte, expectedDigest digest.Digest) {
	ctx := context.Background()
	wr, err := bs.Create(ctx)
	require.NoError(t, err, "unexpected error starting upload")

	nn, err := io.Copy(wr, bytes.NewReader(blob))
	require.NoError(t, err, "error copying into blob writer")

	require.Zero(t, nn, "unexpected number of bytes copied")

	dgst, err := digest.FromReader(bytes.NewReader(blob))
	require.NoError(t, err, "error getting digest")

	// sanity check on zero digest
	require.Equal(t, expectedDigest, dgst)

	desc, err := wr.Commit(ctx, distribution.Descriptor{Digest: dgst})
	require.NoError(t, err, "unexpected error committing write")

	require.Equal(t, desc.Digest, dgst)
}

// seekerSize seeks to the end of seeker, checks the size and returns it to
// the original state, returning the size. The state of the seeker should be
// treated as unknown if an error is returned.
func seekerSize(seeker io.ReadSeeker) (int64, error) {
	current, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	end, err := seeker.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	resumed, err := seeker.Seek(current, io.SeekStart)
	if err != nil {
		return 0, err
	}

	if resumed != current {
		return 0, fmt.Errorf("error returning seeker to original state, could not seek back to original location")
	}

	return end, nil
}

// addBlob simply consumes the reader and inserts into the blob service,
// returning a descriptor on success.
func addBlob(ctx context.Context, bs distribution.BlobIngester, desc distribution.Descriptor, rd io.Reader) (distribution.Descriptor, error) {
	wr, err := bs.Create(ctx)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	defer wr.Cancel(ctx)

	if nn, err := io.Copy(wr, rd); err != nil {
		return distribution.Descriptor{}, err
	} else if nn != desc.Size {
		return distribution.Descriptor{}, fmt.Errorf("incorrect number of bytes copied: %v != %v", nn, desc.Size)
	}

	return wr.Commit(ctx, desc)
}
