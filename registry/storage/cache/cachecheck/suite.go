package cachecheck

import (
	"context"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/registry/storage/cache"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

// CheckBlobDescriptorCache takes a cache implementation through a common set
// of operations. If adding new tests, please add them here so new
// implementations get the benefit. This should be used for unit tests.
func CheckBlobDescriptorCache(t *testing.T, provider cache.BlobDescriptorCacheProvider) {
	ctx := context.Background()

	checkBlobDescriptorCacheEmptyRepository(ctx, t, provider)
	checkBlobDescriptorCacheSetAndRead(ctx, t, provider)
	checkBlobDescriptorCacheClear(ctx, t, provider)
}

func checkBlobDescriptorCacheEmptyRepository(ctx context.Context, t *testing.T, provider cache.BlobDescriptorCacheProvider) {
	_, err := provider.Stat(ctx, "sha384:abc111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111")
	require.ErrorIs(t, err, distribution.ErrBlobUnknown, "expected unknown blob error with empty store")

	_, err = provider.RepositoryScoped("")
	require.Error(t, err, "expected an error when asking for invalid repo")

	repoCache, err := provider.RepositoryScoped("foo/bar")
	require.NoError(t, err, "unexpected error getting repository")

	err = repoCache.SetDescriptor(
		ctx,
		"",
		distribution.Descriptor{
			Digest:    "sha384:abc",
			Size:      10,
			MediaType: "application/octet-stream",
		},
	)
	require.ErrorIs(t, err, digest.ErrDigestInvalidFormat, "expected error with invalid digest")

	err = repoCache.SetDescriptor(
		ctx,
		"sha384:abc111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111",
		distribution.Descriptor{
			Digest:    "",
			Size:      10,
			MediaType: "application/octet-stream",
		},
	)
	require.Error(t, err, "expected error setting value on invalid descriptor")

	_, err = repoCache.Stat(ctx, "")
	require.ErrorIs(t, err, digest.ErrDigestInvalidFormat, "expected error checking for cache item with empty digest")

	_, err = repoCache.Stat(ctx, "sha384:cba111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111")
	require.ErrorIs(t, err, distribution.ErrBlobUnknown, "expected unknown blob error with uncached repo")

	_, err = repoCache.Stat(ctx, "sha384:abc111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111")
	require.ErrorIs(t, err, distribution.ErrBlobUnknown, "expected unknown blob error with empty repo")
}

func checkBlobDescriptorCacheSetAndRead(ctx context.Context, t *testing.T, provider cache.BlobDescriptorCacheProvider) {
	localDigest := digest.Digest("sha384:abc111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111")
	expected := distribution.Descriptor{
		Digest:    "sha256:abc1111111111111111111111111111111111111111111111111111111111111",
		Size:      10,
		MediaType: "application/octet-stream",
	}

	repoCache, err := provider.RepositoryScoped("foo/bar")
	require.NoError(t, err, "unexpected error getting scoped cache")

	err = repoCache.SetDescriptor(ctx, localDigest, expected)
	require.NoError(t, err, "error setting descriptor")

	desc, err := repoCache.Stat(ctx, localDigest)
	require.NoError(t, err, "unexpected error statting fake2:abc")

	require.Equal(t, expected, desc)

	// also check that we set the canonical key ("fake:abc")
	desc, err = repoCache.Stat(ctx, localDigest)
	require.NoError(t, err, "descriptor not returned for canonical key")

	require.Equal(t, expected, desc)

	// ensure that global gets extra descriptor mapping
	desc, err = provider.Stat(ctx, localDigest)
	require.NoErrorf(t, err, "expected blob unknown in global cache: %v", desc)

	require.Equal(t, expected, desc, "unexpected descriptor")

	// get at it through canonical descriptor
	desc, err = provider.Stat(ctx, expected.Digest)
	require.NoError(t, err, "unexpected error checking glboal descriptor")

	require.Equal(t, expected, desc, "unexpected descriptor: %#v != %#v", expected, desc)

	// now, we set the repo local mediatype to something else and ensure it
	// doesn't get changed in the provider cache.
	expected.MediaType = "application/json"

	err = repoCache.SetDescriptor(ctx, localDigest, expected)
	require.NoError(t, err, "unexpected error setting descriptor")

	desc, err = repoCache.Stat(ctx, localDigest)
	require.NoError(t, err, "unexpected error getting descriptor")

	require.Equal(t, expected, desc)

	desc, err = provider.Stat(ctx, localDigest)
	require.NoError(t, err, "unexpected error getting global descriptor")

	expected.MediaType = "application/octet-stream" // expect original mediatype in global

	require.Equal(t, expected, desc)
}

func checkBlobDescriptorCacheClear(ctx context.Context, t *testing.T, provider cache.BlobDescriptorCacheProvider) {
	localDigest := digest.Digest("sha384:def111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111")
	expected := distribution.Descriptor{
		Digest:    "sha256:def1111111111111111111111111111111111111111111111111111111111111",
		Size:      10,
		MediaType: "application/octet-stream",
	}

	repoCache, err := provider.RepositoryScoped("foo/bar")
	require.NoError(t, err, "unexpected error getting scoped cache")

	err = repoCache.SetDescriptor(ctx, localDigest, expected)
	require.NoError(t, err, "error setting descriptor")

	desc, err := repoCache.Stat(ctx, localDigest)
	require.NoError(t, err, "unexpected error statting fake2:abc")

	require.Equal(t, expected, desc)

	err = repoCache.Clear(ctx, localDigest)
	require.NoError(t, err)

	_, err = repoCache.Stat(ctx, localDigest)
	require.Error(t, err, "expected error statting deleted blob")
}
