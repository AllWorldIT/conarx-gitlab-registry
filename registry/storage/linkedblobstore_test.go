package storage

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/testutil"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLinkedBlobStoreCreateWithMountFrom(t *testing.T) {
	fooRepoName, _ := reference.WithName("nm/foo")
	fooEnv := newManifestStoreTestEnv(t, fooRepoName)
	ctx := context.Background()
	stats, err := mockRegistry(t, fooEnv.registry)
	require.NoError(t, err)

	// Build up some test layers and add them to the manifest, saving the
	// readseekers for upload later.
	testLayers := make(map[digest.Digest]io.ReadSeeker)
	for i := 0; i < 2; i++ {
		rs, dgst, err := testutil.CreateRandomTarFile()
		require.NoError(t, err, "unexpected error generating test layer file")
		testLayers[dgst] = rs
	}

	// upload the layers to foo/bar
	for dgst, rs := range testLayers {
		wr, err := fooEnv.repository.Blobs(fooEnv.ctx).Create(fooEnv.ctx)
		require.NoError(t, err, "unexpected error creating test upload")

		_, err = io.Copy(wr, rs)
		require.NoError(t, err, "unexpected error copying to upload")

		_, err = wr.Commit(fooEnv.ctx, distribution.Descriptor{Digest: dgst})
		require.NoError(t, err, "unexpected error finishing upload")
	}

	// create another repository nm/bar
	barRepoName, _ := reference.WithName("nm/bar")
	barRepo, err := fooEnv.registry.Repository(ctx, barRepoName)
	require.NoError(t, err, "unexpected error getting repo")

	// cross-repo mount the test layers into a nm/bar
	for dgst := range testLayers {
		fooCanonical, _ := reference.WithDigest(fooRepoName, dgst)
		option := WithMountFrom(fooCanonical)
		// ensure we can instrospect it
		createOpts := distribution.CreateOptions{}
		require.NoError(t, option.Apply(&createOpts), "failed to apply MountFrom option")
		require.True(t, createOpts.Mount.ShouldMount)
		require.Equal(t, fooCanonical.String(), createOpts.Mount.From.String())

		_, err := barRepo.Blobs(ctx).Create(ctx, WithMountFrom(fooCanonical))
		require.Error(t, err)
		_, ok := err.(distribution.ErrBlobMounted)
		require.True(t, ok, "expected ErrMountFrom error, not %T", err)
	}
	for dgst := range testLayers {
		fooCanonical, _ := reference.WithDigest(fooRepoName, dgst)
		count, exists := stats[fooCanonical.String()]
		require.True(t, exists, "expected entry %q not found among handled stat calls", fooCanonical.String())
		require.Equal(t, 1, count, "expected exactly one stat call for entry %q, not %d", fooCanonical.String(), count)
	}

	clearStats(stats)

	// create yet another repository nm/baz
	bazRepoName, _ := reference.WithName("nm/baz")
	bazRepo, err := fooEnv.registry.Repository(ctx, bazRepoName)
	require.NoError(t, err, "unexpected error getting repo")

	// cross-repo mount them into a nm/baz and provide a prepopulated blob descriptor
	for dgst := range testLayers {
		fooCanonical, _ := reference.WithDigest(fooRepoName, dgst)
		size, err := strconv.ParseInt("0x"+dgst.Hex()[:8], 0, 64)
		require.NoError(t, err)
		prepolutatedDescriptor := distribution.Descriptor{
			Digest:    dgst,
			Size:      size,
			MediaType: "application/octet-stream",
		}
		_, err = bazRepo.Blobs(ctx).Create(ctx, WithMountFrom(fooCanonical), &statCrossMountCreateOption{
			desc: prepolutatedDescriptor,
		})
		blobMounted, ok := err.(distribution.ErrBlobMounted)
		assert.True(t, ok, "expected ErrMountFrom error, not %T", err)
		if !ok {
			continue
		}
		assert.Equal(t, blobMounted.Descriptor, prepolutatedDescriptor, "unexpected descriptor")
	}
	// this time no stat calls will be made
	assert.Empty(t, stats, "unexpected number of stats made")
}

func clearStats(stats map[string]int) {
	for k := range stats {
		delete(stats, k)
	}
}

// mockRegistry sets a mock blob descriptor service factory that overrides
// statter's Stat method to note each attempt to stat a blob in any repository.
// Returned stats map contains canonical references to blobs with a number of
// attempts.
func mockRegistry(t *testing.T, nm distribution.Namespace) (map[string]int, error) {
	registry, ok := nm.(*registry)
	if !ok {
		return nil, fmt.Errorf("not an expected type of registry: %T", nm)
	}
	stats := make(map[string]int)

	registry.blobDescriptorServiceFactory = &mockBlobDescriptorServiceFactory{
		t:     t,
		stats: stats,
	}

	return stats, nil
}

type mockBlobDescriptorServiceFactory struct {
	t     *testing.T
	stats map[string]int
}

func (f *mockBlobDescriptorServiceFactory) BlobAccessController(svc distribution.BlobDescriptorService) distribution.BlobDescriptorService {
	return &mockBlobDescriptorService{
		BlobDescriptorService: svc,
		t:                     f.t,
		stats:                 f.stats,
	}
}

type mockBlobDescriptorService struct {
	distribution.BlobDescriptorService
	t     *testing.T
	stats map[string]int
}

var _ distribution.BlobDescriptorService = &mockBlobDescriptorService{}

func (bs *mockBlobDescriptorService) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	statter, ok := bs.BlobDescriptorService.(*linkedBlobStatter)
	if !ok {
		return distribution.Descriptor{}, fmt.Errorf("unexpected blob descriptor service: %T", bs.BlobDescriptorService)
	}

	name := statter.repository.Named()
	canonical, err := reference.WithDigest(name, dgst)
	if err != nil {
		return distribution.Descriptor{}, fmt.Errorf("failed to make canonical reference: %v", err)
	}

	bs.stats[canonical.String()]++
	bs.t.Logf("calling Stat on %s", canonical.String())

	return bs.BlobDescriptorService.Stat(ctx, dgst)
}

// statCrossMountCreateOptions ensures the expected options type is passed, and optionally pre-fills the cross-mount stat info
type statCrossMountCreateOption struct {
	desc distribution.Descriptor
}

var _ distribution.BlobCreateOption = statCrossMountCreateOption{}

func (f statCrossMountCreateOption) Apply(v any) error {
	opts, ok := v.(*distribution.CreateOptions)
	if !ok {
		return fmt.Errorf("Unexpected create options: %#v", v)
	}

	if !opts.Mount.ShouldMount {
		return nil
	}

	opts.Mount.Stat = &f.desc

	return nil
}
