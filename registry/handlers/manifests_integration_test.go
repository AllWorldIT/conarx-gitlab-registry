//go:build integration && handlers_test

package handlers

import (
	"testing"

	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/internal/testutil"

	"github.com/stretchr/testify/require"
)

func TestDBTagManifest_WithCentralRepositoryCache_InvalidateRootSizeWithDescendants(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	cache := datastore.NewCentralRepositoryCache(testutil.RedisCache(t, 0))
	rStore := datastore.NewRepositoryStore(env.db, datastore.WithRepositoryCache(cache))

	// build and populate test repository
	r, err := rStore.CreateByPath(env.ctx, "foo/bar")
	require.NoError(t, err)
	require.NotNil(t, r)

	mStore := datastore.NewManifestStore(env.db)
	m := &models.Manifest{
		NamespaceID:   r.NamespaceID,
		RepositoryID:  r.ID,
		SchemaVersion: 2,
		MediaType:     schema2.MediaTypeManifest,
		Digest:        "sha256:bca3c0bf2ca0cde987ad9cab2dac986047a0ccff282f1b23df282ef05e3a10a6",
		Payload:       models.Payload{},
	}
	err = mStore.Create(env.ctx, m)
	require.NoError(t, err)

	// ensure the cache key does not exist
	root := &models.Repository{NamespaceID: r.NamespaceID, Path: r.TopLevelPathSegment()}
	found, _ := cache.GetSizeWithDescendants(env.ctx, root)
	require.False(t, found)

	// fetch size to force populate the cache
	_, err = rStore.SizeWithDescendants(env.ctx, root)
	require.NoError(t, err)

	// ensure the cache key now exists
	found, _ = cache.GetSizeWithDescendants(env.ctx, root)
	require.True(t, found)

	// create tag
	err = dbTagManifest(env.ctx, env.db, cache, m.Digest, "latest", r.Path)
	require.NoError(t, err)

	// the cache key should be gone
	found, _ = cache.GetSizeWithDescendants(env.ctx, root)
	require.False(t, found)
}

func TestDBDeleteManifest_WithCentralRepositoryCache_InvalidateRootSizeWithDescendants(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	cache := datastore.NewCentralRepositoryCache(testutil.RedisCache(t, 0))
	rStore := datastore.NewRepositoryStore(env.db, datastore.WithRepositoryCache(cache))

	// build and populate test repository
	r, err := rStore.CreateByPath(env.ctx, "foo/bar")
	require.NoError(t, err)
	require.NotNil(t, r)

	mStore := datastore.NewManifestStore(env.db)
	m := &models.Manifest{
		NamespaceID:   r.NamespaceID,
		RepositoryID:  r.ID,
		SchemaVersion: 2,
		MediaType:     schema2.MediaTypeManifest,
		Digest:        "sha256:bca3c0bf2ca0cde987ad9cab2dac986047a0ccff282f1b23df282ef05e3a10a6",
		Payload:       models.Payload{},
	}
	err = mStore.Create(env.ctx, m)
	require.NoError(t, err)

	// ensure the cache key does not exist
	root := &models.Repository{NamespaceID: r.NamespaceID, Path: r.TopLevelPathSegment()}
	found, _ := cache.GetSizeWithDescendants(env.ctx, root)
	require.False(t, found)

	// fetch size to force populate the cache
	_, err = rStore.SizeWithDescendants(env.ctx, root)
	require.NoError(t, err)

	// ensure the cache key now exists
	found, _ = cache.GetSizeWithDescendants(env.ctx, root)
	require.True(t, found)

	// delete manifest
	err = dbDeleteManifest(env.ctx, env.db, cache, r.Path, m.Digest)
	require.NoError(t, err)

	// the cache key should be gone
	found, _ = cache.GetSizeWithDescendants(env.ctx, root)
	require.False(t, found)
}
