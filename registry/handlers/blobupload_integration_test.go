//go:build integration && handlers_test

package handlers

import (
	"math/rand"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func buildRepository(t *testing.T, env *env, path string) *models.Repository {
	t.Helper()

	r, err := env.rStore.CreateByPath(env.ctx, path)
	require.NoError(t, err)
	require.NotNil(t, r)

	return r
}

func randomDigest(t *testing.T) digest.Digest {
	t.Helper()

	bytes := make([]byte, rand.Intn(10000))
	_, err := rand.Read(bytes)
	require.NoError(t, err)

	return digest.FromBytes(bytes)
}

func buildRandomBlob(t *testing.T, env *env) *models.Blob {
	t.Helper()

	bStore := datastore.NewBlobStore(env.db)

	b := &models.Blob{
		MediaType: "application/octet-stream",
		Digest:    randomDigest(t),
		Size:      rand.Int63n(10000),
	}
	err := bStore.Create(env.ctx, b)
	require.NoError(t, err)

	return b
}

func randomBlobDescriptor(t *testing.T) distribution.Descriptor {
	t.Helper()

	return distribution.Descriptor{
		MediaType: "application/octet-stream",
		Digest:    randomDigest(t),
		Size:      rand.Int63n(10000),
	}
}

func descriptorFromBlob(t *testing.T, b *models.Blob) distribution.Descriptor {
	t.Helper()

	return distribution.Descriptor{
		MediaType: b.MediaType,
		Digest:    b.Digest,
		Size:      b.Size,
	}
}

func linkBlob(t *testing.T, env *env, r *models.Repository, d digest.Digest) {
	t.Helper()

	err := env.rStore.LinkBlob(env.ctx, r, d)
	require.NoError(t, err)
}

func isBlobLinked(t *testing.T, env *env, r *models.Repository, d digest.Digest) bool {
	t.Helper()

	linked, err := env.rStore.ExistsBlob(env.ctx, r, d)
	require.NoError(t, err)

	return linked
}

func findRepository(t *testing.T, env *env, path string) *models.Repository {
	t.Helper()

	r, err := env.rStore.FindByPath(env.ctx, path)
	require.NoError(t, err)
	require.NotNil(t, r)

	return r
}

func findBlob(t *testing.T, env *env, d digest.Digest) *models.Blob {
	t.Helper()

	bStore := datastore.NewBlobStore(env.db)
	b, err := bStore.FindByDigest(env.ctx, d)
	require.NoError(t, err)
	require.NotNil(t, b)

	return b
}

func TestDBMountBlob_NonExistentSourceRepo(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	// Test for cases where only the source repo does not exist.
	buildRepository(t, env, "to")

	b := buildRandomBlob(t, env)

	err := dbMountBlob(env.ctx, env.rStore, "from", "to", b.Digest)
	require.Error(t, err)
	require.Equal(t, "source repository not found in database", err.Error())

}

func TestDBMountBlob_NonExistentBlob(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	fromRepo := buildRepository(t, env, "from")

	err := dbMountBlob(env.ctx, env.rStore, fromRepo.Path, "to", randomDigest(t))
	require.Error(t, err)
	require.Equal(t, "blob not found in database", err.Error())
}

func TestDBMountBlob_NonExistentBlobLinkInSourceRepo(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	fromRepo := buildRepository(t, env, "from")
	b := buildRandomBlob(t, env) // not linked in fromRepo

	err := dbMountBlob(env.ctx, env.rStore, fromRepo.Path, "to", b.Digest)
	require.Error(t, err)
	require.Equal(t, "blob not found in database", err.Error())
}

func TestDBMountBlob_NonExistentDestinationRepo(t *testing.T) {
	tcs := map[string]struct {
		useCache bool
	}{
		"non existent dest repo":            {},
		"non existent dest repo with cache": {useCache: true},
	}

	for tn, tc := range tcs {
		t.Run(tn, func(t *testing.T) {
			var opts []envOpt
			if tc.useCache {
				opts = append(opts, witCachedRepositoryStore(t))
			}

			env := newEnv(t, opts...)
			defer env.shutdown(t)

			fromRepo := buildRepository(t, env, "from")
			b := buildRandomBlob(t, env)
			linkBlob(t, env, fromRepo, b.Digest)
			err := dbMountBlob(env.ctx, env.rStore, fromRepo.Path, "to", b.Digest)
			require.NoError(t, err)

			destRepo := findRepository(t, env, "to")
			require.True(t, isBlobLinked(t, env, destRepo, b.Digest))
		})
	}
}

func TestDBMountBlob_AlreadyLinked(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	b := buildRandomBlob(t, env)

	fromRepo := buildRepository(t, env, "from")
	linkBlob(t, env, fromRepo, b.Digest)

	destRepo := buildRepository(t, env, "to")
	linkBlob(t, env, destRepo, b.Digest)

	err := dbMountBlob(env.ctx, env.rStore, fromRepo.Path, destRepo.Path, b.Digest)
	require.NoError(t, err)

	require.True(t, isBlobLinked(t, env, destRepo, b.Digest))
}

func TestDBPutBlobUploadComplete_NonExistentRepoAndBlob_WithCache(t *testing.T) {
	testDBPutBlobUploadComplete_NonExistentRepoAndBlob(t, witCachedRepositoryStore(t))
}

func TestDBPutBlobUploadComplete_NonExistentRepoAndBlob_WithoutCache(t *testing.T) {
	testDBPutBlobUploadComplete_NonExistentRepoAndBlob(t)
}

func testDBPutBlobUploadComplete_NonExistentRepoAndBlob(t *testing.T, envOpts ...envOpt) {
	env := newEnv(t, envOpts...)
	defer env.shutdown(t)

	desc := randomBlobDescriptor(t)

	var repoStoreOpts []datastore.RepositoryStoreOption
	if env.cache != nil {
		repoStoreOpts = append(repoStoreOpts, datastore.WithRepositoryCache(datastore.NewCentralRepositoryCache(env.cache)))
	}
	err := dbPutBlobUploadComplete(env.ctx, env.db, "foo", desc, repoStoreOpts)
	require.NoError(t, err)

	// the blob should have been created
	b := findBlob(t, env, desc.Digest)
	// and so does the repository
	r := findRepository(t, env, "foo")
	// and the link between blob and repository
	require.True(t, isBlobLinked(t, env, r, b.Digest))
}

func TestDBPutBlobUploadComplete_NonExistentRepoAndExistentBlob_WithCache(t *testing.T) {
	testDBPutBlobUploadComplete_NonExistentRepoAndExistentBlob(t, witCachedRepositoryStore(t))
}

func TestDBPutBlobUploadComplete_NonExistentRepoAndExistentBlob_WithoutCache(t *testing.T) {
	testDBPutBlobUploadComplete_NonExistentRepoAndExistentBlob(t)
}

func testDBPutBlobUploadComplete_NonExistentRepoAndExistentBlob(t *testing.T, envOpts ...envOpt) {
	env := newEnv(t, envOpts...)
	defer env.shutdown(t)

	b := buildRandomBlob(t, env)
	desc := descriptorFromBlob(t, b)

	var repoStoreOpts []datastore.RepositoryStoreOption
	if env.cache != nil {
		repoStoreOpts = append(repoStoreOpts, datastore.WithRepositoryCache(datastore.NewCentralRepositoryCache(env.cache)))
	}
	err := dbPutBlobUploadComplete(env.ctx, env.db, "foo", desc, repoStoreOpts)
	require.NoError(t, err)

	// the repository should have been created
	r := findRepository(t, env, "foo")
	// and so does the link between blob and repository
	require.True(t, isBlobLinked(t, env, r, b.Digest))
}

func TestDBPutBlobUploadComplete_ExistentRepoAndNonExistentBlob_WithCache(t *testing.T) {
	testDBPutBlobUploadComplete_ExistentRepoAndNonExistentBlob(t, witCachedRepositoryStore(t))
}

func TestDBPutBlobUploadComplete_ExistentRepoAndNonExistentBlob_WithoutCache(t *testing.T) {
	testDBPutBlobUploadComplete_ExistentRepoAndNonExistentBlob(t)
}

func testDBPutBlobUploadComplete_ExistentRepoAndNonExistentBlob(t *testing.T, envOpts ...envOpt) {
	env := newEnv(t, envOpts...)
	defer env.shutdown(t)

	r := buildRepository(t, env, "foo")
	desc := randomBlobDescriptor(t)

	var repoStoreOpts []datastore.RepositoryStoreOption
	if env.cache != nil {
		repoStoreOpts = append(repoStoreOpts, datastore.WithRepositoryCache(datastore.NewCentralRepositoryCache(env.cache)))
	}
	err := dbPutBlobUploadComplete(env.ctx, env.db, r.Path, desc, repoStoreOpts)
	require.NoError(t, err)

	// the blob should have been created
	b := findBlob(t, env, desc.Digest)
	// and so does the link between blob and repository
	require.True(t, isBlobLinked(t, env, r, b.Digest))
}

func TestDBPutBlobUploadComplete_ExistentRepoAndBlobButNotLinked_WithCache(t *testing.T) {
	testDBPutBlobUploadComplete_ExistentRepoAndBlobButNotLinked(t, witCachedRepositoryStore(t))
}

func TestDBPutBlobUploadComplete_ExistentRepoAndBlobButNotLinked_WithoutCache(t *testing.T) {
	testDBPutBlobUploadComplete_ExistentRepoAndBlobButNotLinked(t)
}

func testDBPutBlobUploadComplete_ExistentRepoAndBlobButNotLinked(t *testing.T, envOpts ...envOpt) {
	env := newEnv(t, envOpts...)
	defer env.shutdown(t)

	r := buildRepository(t, env, "foo")
	b := buildRandomBlob(t, env)
	desc := descriptorFromBlob(t, b)

	var repoStoreOpts []datastore.RepositoryStoreOption
	if env.cache != nil {
		repoStoreOpts = append(repoStoreOpts, datastore.WithRepositoryCache(datastore.NewCentralRepositoryCache(env.cache)))
	}
	err := dbPutBlobUploadComplete(env.ctx, env.db, r.Path, desc, repoStoreOpts)
	require.NoError(t, err)

	// the link between blob and repository should have been created
	require.True(t, isBlobLinked(t, env, r, b.Digest))
}

func TestDBPutBlobUploadComplete_ExistentRepoAndBlobAlreadyLinked_WithCache(t *testing.T) {
	testDBPutBlobUploadComplete_ExistentRepoAndBlobAlreadyLinked(t, witCachedRepositoryStore(t))
}

func TestDBPutBlobUploadComplete_ExistentRepoAndBlobAlreadyLinked_WithoutCache(t *testing.T) {
	testDBPutBlobUploadComplete_ExistentRepoAndBlobAlreadyLinked(t)
}

func testDBPutBlobUploadComplete_ExistentRepoAndBlobAlreadyLinked(t *testing.T, envOpts ...envOpt) {
	env := newEnv(t, envOpts...)
	defer env.shutdown(t)

	r := buildRepository(t, env, "foo")
	b := buildRandomBlob(t, env)
	linkBlob(t, env, r, b.Digest)
	desc := descriptorFromBlob(t, b)

	var repoStoreOpts []datastore.RepositoryStoreOption
	if env.cache != nil {
		repoStoreOpts = append(repoStoreOpts, datastore.WithRepositoryCache(datastore.NewCentralRepositoryCache(env.cache)))
	}
	err := dbPutBlobUploadComplete(env.ctx, env.db, r.Path, desc, repoStoreOpts)
	require.NoError(t, err)

	// the link between blob and repository should remain
	require.True(t, isBlobLinked(t, env, r, b.Digest))
}
