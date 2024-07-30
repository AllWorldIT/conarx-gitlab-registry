package storage

import (
	"context"
	"testing"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	"github.com/docker/distribution/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewRegistry_RedirectException(t *testing.T) {
	redirectTests := []struct {
		exceptions                  []string
		wantMatchingRepositories    []string
		wantNonMatchingRepositories []string
	}{
		{
			exceptions:                  []string{"^gitlab-org"},
			wantMatchingRepositories:    []string{"gitlab-org/gitlab-build-images", "gitlab-org/gitlab"},
			wantNonMatchingRepositories: []string{"example-com/alpine", "containers-gov/cool"},
		},
		{
			exceptions:                  []string{},
			wantMatchingRepositories:    []string{},
			wantNonMatchingRepositories: []string{"example-com/alpine", "gitlab-org/gitlab-build-images", "example-com/best-app", "cloud-internet/rockstar"},
		},
	}

	for _, tt := range redirectTests {
		reg, err := NewRegistry(context.Background(), inmemory.New(), EnableRedirectWithExceptions(tt.exceptions))
		require.NoError(t, err)

		// Redirects are enabled in general.
		r, ok := reg.(*registry)
		require.True(t, ok)

		require.True(t, r.blobServer.redirect.enabled)

		// All exceptions strings are compiled to regular expressions.
		require.Len(t, r.redirectExceptions, len(tt.exceptions))

		for _, match := range tt.wantMatchingRepositories {
			expectRedirect(t, r, match, false)
		}

		for _, nonMatch := range tt.wantNonMatchingRepositories {
			expectRedirect(t, r, nonMatch, true)
		}

		// Global direction is not effected by repository specific exceptions.
		require.True(t, r.blobServer.redirect.enabled)
	}
}

func expectRedirect(t *testing.T, reg *registry, repoPath string, redirect bool) {
	ctx := context.Background()

	// Repositories which do not match any of the exceptions continue to redirect.
	named, err := reference.WithName(repoPath)
	require.NoError(t, err)

	repo, err := reg.Repository(ctx, named)
	require.NoError(t, err)

	rep, ok := repo.(*repository)
	require.True(t, ok)

	blobStore := rep.Blobs(ctx)

	lbs, ok := blobStore.(*linkedBlobStore)
	require.True(t, ok)

	bs, ok := lbs.blobServer.(*blobServer)
	require.True(t, ok)

	require.Equalf(t, redirect, bs.redirect.enabled, "\n\tregexes: %+v\n\trepo path: %q", reg.redirectExceptions, repoPath)
}

func TestNewRegistry_RedirectException_InvalidRegex(t *testing.T) {
	_, err := NewRegistry(context.Background(), inmemory.New(), EnableRedirectWithExceptions([]string{"><(((('>"}))
	require.EqualError(t, err, "configuring storage redirect exception: error parsing regexp: missing closing ): `><(((('>`")
}

func TestNewRegistry_RedirectExpiryDelay(t *testing.T) {
	expiryDelay := 2 * time.Minute
	reg, err := NewRegistry(context.Background(), inmemory.New(), WithRedirectExpiryDelay(expiryDelay))
	require.NoError(t, err)
	r, ok := reg.(*registry)
	require.True(t, ok)

	require.Equal(t, expiryDelay, r.blobServer.redirect.expiryDelay)
}

func TestRepositoryExists(t *testing.T) {
	ctx := context.Background()

	registry, err := NewRegistry(ctx, inmemory.New())
	require.NoError(t, err)

	named, err := reference.WithName("test/repo/with/parent")
	require.NoError(t, err)

	repo, err := registry.Repository(ctx, named)
	require.NoError(t, err)

	r, ok := repo.(*repository)
	require.True(t, ok)

	// check non existing test repo
	exists, err := r.Exists(ctx)
	require.NoError(t, err)
	require.False(t, exists)

	// upload a manifest to test repo (will create the repository path)
	manifest, err := testutil.UploadRandomSchema2Image(repo)
	require.NoError(t, err)

	// Check the child repo, since there are no tags, it should not count as existing.
	exists, err = r.Exists(ctx)
	require.NoError(t, err)
	require.False(t, exists)

	// Tag the manifest we just uploaded, now the repo should count as existing.
	err = r.Tags(ctx).Tag(ctx, "test", distribution.Descriptor{Digest: manifest.ManifestDigest})
	require.NoError(t, err)

	exists, err = r.Exists(ctx)
	require.NoError(t, err)
	require.True(t, exists)

	// Check a parent repo, since there are no tags, it should not count as existing.
	named, err = reference.WithName("test/repo")
	require.NoError(t, err)

	repo, err = registry.Repository(ctx, named)
	require.NoError(t, err)

	r, ok = repo.(*repository)
	require.True(t, ok)

	exists, err = r.Exists(ctx)
	require.NoError(t, err)
	require.False(t, exists)
}
