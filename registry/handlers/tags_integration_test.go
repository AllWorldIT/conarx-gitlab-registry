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

func TestDeleteTagDB(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	// Setup

	// build test repository
	rStore := datastore.NewRepositoryStore(env.db)
	r, err := rStore.CreateByPath(env.ctx, "foo")
	require.NoError(t, err)
	require.NotNil(t, r)

	// add a manifest
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

	// tag manifest
	tStore := datastore.NewTagStore(env.db)
	tag := &models.Tag{
		Name:         "latest",
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m.ID,
	}
	err = tStore.CreateOrUpdate(env.ctx, tag)
	require.NoError(t, err)

	// Test

	err = dbDeleteTag(env.ctx, env.db, datastore.NewNoOpRepositoryCache(), r.Path, tag.Name)
	require.NoError(t, err)

	// the tag shouldn't be there
	tag, err = tStore.FindByID(env.ctx, tag.ID)
	require.NoError(t, err)
	require.Nil(t, tag)
}

func TestDeleteTagDB_SizeInvalidated_WithCentralRepositoryCache(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	// Setup

	// build a test repository and add it to the db & cache
	cache := datastore.NewCentralRepositoryCache(testutil.RedisCache(t, 0))
	rStore := datastore.NewRepositoryStore(env.db, datastore.WithRepositoryCache(cache))
	r, err := rStore.CreateByPath(env.ctx, "foo")
	require.NoError(t, err)
	require.NotNil(t, r)

	// add the size attribute to the already cached repository object
	expectedRepoSizeFromDB, err := rStore.Size(env.ctx, r)
	require.NoError(t, err)
	require.Equal(t, expectedRepoSizeFromDB.Bytes(), *cache.Get(env.ctx, r.Path).Size)

	// add a manifest
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

	// tag manifest
	tStore := datastore.NewTagStore(env.db)
	tag := &models.Tag{
		Name:         "latest",
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m.ID,
	}
	err = tStore.CreateOrUpdate(env.ctx, tag)
	require.NoError(t, err)

	// delete the tag
	err = dbDeleteTag(env.ctx, env.db, cache, r.Path, tag.Name)
	require.NoError(t, err)

	// the size attribute of the cached repository object is also removed
	require.Nil(t, cache.Get(env.ctx, r.Path).Size)
}

func TestDeleteTagDB_SizeInvalidated_WithSingleRepositoryCache(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	// Setup

	// build a test repository and add it to the db & cache
	cache := datastore.NewCentralRepositoryCache(testutil.RedisCache(t, 0))
	rStore := datastore.NewRepositoryStore(env.db, datastore.WithRepositoryCache(cache))
	r, err := rStore.CreateByPath(env.ctx, "foo")
	require.NoError(t, err)
	require.NotNil(t, r)

	// add the size attribute to the already cached repository object
	expectedRepoSizeFromDB, err := rStore.Size(env.ctx, r)
	require.NoError(t, err)
	require.Equal(t, expectedRepoSizeFromDB.Bytes(), *cache.Get(env.ctx, r.Path).Size)

	// add a manifest
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

	// tag manifest
	tStore := datastore.NewTagStore(env.db)
	tag := &models.Tag{
		Name:         "latest",
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m.ID,
	}
	err = tStore.CreateOrUpdate(env.ctx, tag)
	require.NoError(t, err)

	// delete the tag
	err = dbDeleteTag(env.ctx, env.db, cache, r.Path, tag.Name)
	require.NoError(t, err)

	// the size attribute of the cached repository object is also removed
	require.Nil(t, cache.Get(env.ctx, r.Path).Size)
}

func TestDeleteTagDB_RepositoryNotFound(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	err := dbDeleteTag(env.ctx, env.db, datastore.NewNoOpRepositoryCache(), "foo", "bar")
	require.Error(t, err, "repository not found in database")
}

func TestDeleteTagDB_TagNotFound(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	// build test repository
	rStore := datastore.NewRepositoryStore(env.db)
	r, err := rStore.CreateByPath(env.ctx, "foo")
	require.NoError(t, err)
	require.NotNil(t, r)

	err = dbDeleteTag(env.ctx, env.db, datastore.NewNoOpRepositoryCache(), r.Path, "bar")
	require.Error(t, err, "repository not found in database")
}
