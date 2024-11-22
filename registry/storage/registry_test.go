package storage

import (
	"context"
	"testing"
	"time"

	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
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
