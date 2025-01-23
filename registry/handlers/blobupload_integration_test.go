//go:build integration && handlers_test

package handlers

import (
	"io"
	"math/rand/v2"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/mocks"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/testutil"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
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

	rng := rand.NewChaCha8([32]byte(testutil.MustChaChaSeed(t)))
	dgst, _ := digest.FromReader(io.LimitReader(rng, rand.Int64N(10000)))
	return dgst
}

func buildRandomBlob(t *testing.T, env *env) *models.Blob {
	t.Helper()

	bStore := datastore.NewBlobStore(env.db)

	b := &models.Blob{
		MediaType: "application/octet-stream",
		Digest:    randomDigest(t),
		Size:      rand.Int64N(10000),
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
		Size:      rand.Int64N(10000),
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

// gomockMatchRepoFn is a function used in tests that use Gomock to assert calls
// to RepositoryCache object to validate that passed repository object matches
// given name.
func gomockMatchRepoFn(repoName string) func(x any) bool {
	return func(x any) bool {
		repoArg := x.(*models.Repository)
		return repoArg != nil && repoArg.Name == repoName && repoArg.Path == repoName
	}
}

func TestDBMountBlob_NonExistentSourceRepo(t *testing.T) {
	fromRepoName := "from"
	toRepoName := "to"

	env := newEnv(t)
	defer env.shutdown(t)

	ctrl := gomock.NewController(t)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, fromRepoName).Return(nil).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, (*models.Repository)(nil)).Times(1),
	)
	rStore := datastore.NewRepositoryStore(env.db, datastore.WithRepositoryCache(repoCacheMock))

	b := buildRandomBlob(t, env)

	destRepo := buildRepository(t, env, toRepoName)

	err := dbMountBlob(env.ctx, rStore, fromRepoName, destRepo.Path, b.Digest)
	require.Error(t, err)
	require.Equal(t, "source repository not found in database", err.Error())
}

func TestDBMountBlob_NonExistantBlob(t *testing.T) {
	fromRepoName := "from"
	toRepoName := "to"

	env := newEnv(t)
	defer env.shutdown(t)

	ctrl := gomock.NewController(t)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, fromRepoName).Return(nil).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(fromRepoName))).Times(1),
	)
	rStore := datastore.NewRepositoryStore(env.db, datastore.WithRepositoryCache(repoCacheMock))

	bDigest := randomDigest(t)
	fromRepo := buildRepository(t, env, fromRepoName)
	destRepo := buildRepository(t, env, toRepoName)

	err := dbMountBlob(env.ctx, rStore, fromRepo.Path, destRepo.Path, bDigest)
	require.Error(t, err)
	require.Equal(t, "blob not found in database", err.Error())
}

func TestDBMountBlob_NonExistentBlobLinkInSourceRepo(t *testing.T) {
	fromRepoName := "from"
	toRepoName := "to"

	env := newEnv(t)
	defer env.shutdown(t)

	ctrl := gomock.NewController(t)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, fromRepoName).Return(nil).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(fromRepoName))).Times(1),
	)
	rStore := datastore.NewRepositoryStore(env.db, datastore.WithRepositoryCache(repoCacheMock))

	b := buildRandomBlob(t, env)

	fromRepo := buildRepository(t, env, fromRepoName)
	destRepo := buildRepository(t, env, toRepoName)

	err := dbMountBlob(env.ctx, rStore, fromRepo.Path, destRepo.Path, b.Digest)
	require.Error(t, err)
	require.Equal(t, "blob not found in database", err.Error())
}

func TestDBMountBlob_NonExistentDestinationRepo(t *testing.T) {
	fromRepoName := "from"
	toRepoName := "to"

	env := newEnv(t)
	defer env.shutdown(t)

	ctrl := gomock.NewController(t)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, fromRepoName).Return(nil).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(fromRepoName))).Times(1),
		repoCacheMock.EXPECT().Get(env.ctx, toRepoName).Return(nil).Times(3),
		repoCacheMock.EXPECT().Set(env.ctx, (*models.Repository)(nil)).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(toRepoName))).Times(1),
	)
	rStore := datastore.NewRepositoryStore(env.db, datastore.WithRepositoryCache(repoCacheMock))

	b := buildRandomBlob(t, env)

	fromRepo := buildRepository(t, env, fromRepoName)

	linkBlob(t, env, fromRepo, b.Digest)

	err := dbMountBlob(env.ctx, rStore, fromRepo.Path, toRepoName, b.Digest)
	require.NoError(t, err)
	destRepo := findRepository(t, env, toRepoName)
	require.True(t, isBlobLinked(t, env, destRepo, b.Digest))
}

func TestDBMountBlob_BlobAlreadyLinked(t *testing.T) {
	fromRepoName := "from"
	toRepoName := "to"

	env := newEnv(t)
	defer env.shutdown(t)

	ctrl := gomock.NewController(t)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, fromRepoName).Return(nil).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(fromRepoName))).Times(1),
		repoCacheMock.EXPECT().Get(env.ctx, toRepoName).Return(nil).Times(3),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(toRepoName))).Times(2),
	)
	rStore := datastore.NewRepositoryStore(env.db, datastore.WithRepositoryCache(repoCacheMock))

	b := buildRandomBlob(t, env)

	fromRepo := buildRepository(t, env, fromRepoName)

	linkBlob(t, env, fromRepo, b.Digest)

	destRepo := buildRepository(t, env, toRepoName)

	linkBlob(t, env, destRepo, b.Digest)

	err := dbMountBlob(env.ctx, rStore, fromRepo.Path, destRepo.Path, b.Digest)
	require.NoError(t, err)
	require.True(t, isBlobLinked(t, env, destRepo, b.Digest))
}

func TestDBPutBlobUploadComplete_NonExistantRepoAndBlob(t *testing.T) {
	repoName := "foo"

	env := newEnv(t)
	defer env.shutdown(t)

	desc := randomBlobDescriptor(t)

	ctrl := gomock.NewController(t)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, repoName).Return(nil).Times(3),
		repoCacheMock.EXPECT().Set(env.ctx, (*models.Repository)(nil)).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(repoName))).Times(1),
	)

	err := dbPutBlobUploadComplete(env.ctx, env.db, repoName, desc, repoCacheMock)
	require.NoError(t, err)

	// the blob should have been created
	b := findBlob(t, env, desc.Digest)
	// and so does the repository
	r := findRepository(t, env, repoName)

	// and the link between blob and repository
	require.True(t, isBlobLinked(t, env, r, b.Digest))
}

func TestDBPutBlobUploadComplete_BlobExistsAndNonExistentRepo(t *testing.T) {
	repoName := "foo"

	env := newEnv(t)
	defer env.shutdown(t)

	b := buildRandomBlob(t, env)
	desc := descriptorFromBlob(t, b)

	ctrl := gomock.NewController(t)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, repoName).Return(nil).Times(3),
		repoCacheMock.EXPECT().Set(env.ctx, (*models.Repository)(nil)).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(repoName))).Times(1),
	)

	err := dbPutBlobUploadComplete(env.ctx, env.db, repoName, desc, repoCacheMock)
	require.NoError(t, err)

	r := findRepository(t, env, repoName)

	// and the link between blob and repository
	require.True(t, isBlobLinked(t, env, r, b.Digest))
}

func TestDBPutBlobUploadComplete_RepoExistsAndBlobDoesNot(t *testing.T) {
	repoName := "foo"

	env := newEnv(t)
	defer env.shutdown(t)

	desc := randomBlobDescriptor(t)

	r := buildRepository(t, env, repoName)

	ctrl := gomock.NewController(t)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, repoName).Return(nil).Times(3),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(repoName))).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(repoName))).Times(1),
	)

	err := dbPutBlobUploadComplete(env.ctx, env.db, repoName, desc, repoCacheMock)
	require.NoError(t, err)

	// the blob should have been created
	b := findBlob(t, env, desc.Digest)

	// and the link between blob and repository
	require.True(t, isBlobLinked(t, env, r, b.Digest))
}

func TestDBPutBlobUploadComplete_BothBlobAndRepoExistsButNotLinked(t *testing.T) {
	repoName := "foo"

	env := newEnv(t)
	defer env.shutdown(t)

	b := buildRandomBlob(t, env)
	desc := descriptorFromBlob(t, b)

	r := buildRepository(t, env, repoName)

	ctrl := gomock.NewController(t)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, repoName).Return(nil).Times(3),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(repoName))).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(repoName))).Times(1),
	)

	err := dbPutBlobUploadComplete(env.ctx, env.db, repoName, desc, repoCacheMock)
	require.NoError(t, err)

	// and the link between blob and repository
	require.True(t, isBlobLinked(t, env, r, b.Digest))
}

func TestDBPutBlobUploadComplete_BothBlobAndRepoExistsAndLinked(t *testing.T) {
	repoName := "foo"

	env := newEnv(t)
	defer env.shutdown(t)

	b := buildRandomBlob(t, env)
	desc := descriptorFromBlob(t, b)

	r := buildRepository(t, env, repoName)

	linkBlob(t, env, r, b.Digest)

	ctrl := gomock.NewController(t)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, repoName).Return(nil).Times(3),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(repoName))).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(gomockMatchRepoFn(repoName))).Times(1),
	)

	err := dbPutBlobUploadComplete(env.ctx, env.db, repoName, desc, repoCacheMock)
	require.NoError(t, err)

	// and the link between blob and repository
	require.True(t, isBlobLinked(t, env, r, b.Digest))
}
