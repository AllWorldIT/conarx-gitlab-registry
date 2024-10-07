//go:build integration && handlers_test

package handlers

import (
	"context"
	iredis "github.com/docker/distribution/registry/internal/redis"
	"os"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/migrations"
	"github.com/docker/distribution/registry/datastore/mocks"
	"github.com/docker/distribution/registry/datastore/models"
	dbtestutil "github.com/docker/distribution/registry/datastore/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type env struct {
	ctx    context.Context
	db     *datastore.DB
	cache  *iredis.Cache
	config *configuration.Configuration
	rStore datastore.RepositoryStore

	// isShutdown helps ensure that tests do not try to access the db after the
	// connection has been closed.
	isShutdown bool
}

func (e *env) isDatabaseEnabled() bool {
	return !e.isShutdown && os.Getenv("REGISTRY_DATABASE_ENABLED") == "true"
}

func (e *env) shutdown(t *testing.T) {
	t.Helper()

	if !e.isDatabaseEnabled() {
		return
	}

	err := dbtestutil.TruncateAllTables(e.db)
	require.NoError(t, err)

	err = e.db.Close()
	require.NoError(t, err)

	e.isShutdown = true
}

func initDatabase(t *testing.T, env *env) {
	t.Helper()

	if !env.isDatabaseEnabled() {
		t.Skip("database connection is required for this test")
	}

	db, err := dbtestutil.NewDBFromEnv()
	require.NoError(t, err)

	env.db = db

	m := migrations.NewMigrator(db)
	_, err = m.Up()
	require.NoError(t, err)
}

type envOpt func(*env)

func newEnv(t *testing.T) *env {
	t.Helper()

	env := &env{
		ctx: context.Background(),
		config: &configuration.Configuration{
			Storage: map[string]configuration.Parameters{
				"delete": map[string]interface{}{
					"enabled": true,
				},
			},
		},
	}

	initDatabase(t, env)

	// set a default repository store if not set by options
	env.rStore = datastore.NewRepositoryStore(env.db)

	return env
}

func setupBlob(t *testing.T, path string, env *env) (*models.Blob, *models.Repository, datastore.BlobStore) {
	t.Helper()

	// build test repository
	rStore := datastore.NewRepositoryStore(env.db)
	r, err := rStore.CreateByPath(env.ctx, path)
	require.NoError(t, err)
	require.NotNil(t, r)

	// add layer blob
	bStore := datastore.NewBlobStore(env.db)
	b := &models.Blob{
		MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
		Digest:    "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9",
		Size:      2802957,
	}
	err = bStore.Create(env.ctx, b)
	require.NoError(t, err)
	require.NotEmpty(t, r.ID)

	// link blob to repository
	err = rStore.LinkBlob(env.ctx, r, b.Digest)
	require.NoError(t, err)

	// make sure it's linked
	require.True(t, isBlobLinked(t, env, r, b.Digest))

	return b, r, bStore
}

func TestDeleteBlobDB(t *testing.T) {
	repoName := "bar"

	env := newEnv(t)
	defer env.shutdown(t)

	// Setup
	ctrl := gomock.NewController(t)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)

	b, r, bStore := setupBlob(t, repoName, env)

	matchFn := func(x any) bool {
		repoArg := x.(*models.Repository)
		return repoArg.Name == repoName && repoArg.Path == repoName
	}
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, repoName).Return(nil).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(matchFn)).Times(1),
	)

	// Test
	err := dbDeleteBlob(env.ctx, env.config, env.db, repoCacheMock, r.Path, b.Digest)
	require.NoError(t, err)

	// the layer blob should still be there
	b2, err := bStore.FindByDigest(env.ctx, b.Digest)
	require.NoError(t, err)
	require.NotNil(t, b2)

	// but not the link for the repository
	require.False(t, isBlobLinked(t, env, r, b.Digest))
}

func TestDeleteBlobDB_RepositoryNotFound(t *testing.T) {
	repoName := "foo"

	env := newEnv(t)
	defer env.shutdown(t)

	ctrl := gomock.NewController(t)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)

	repoCacheMock.EXPECT().Get(env.ctx, repoName).Return(nil).Times(1)
	repoCacheMock.EXPECT().Set(env.ctx, nil).Times(1)

	err := dbDeleteBlob(env.ctx, env.config, env.db, repoCacheMock, repoName, "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9")
	require.ErrorIs(t, err, distribution.ErrRepositoryUnknown{Name: repoName})
}

func TestExistsBlobDB_Exists(t *testing.T) {
	repoName := "bar"

	env := newEnv(t)
	defer env.shutdown(t)

	// Setup
	ctrl := gomock.NewController(t)

	b, r, _ := setupBlob(t, repoName, env)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)
	matchFn := func(x any) bool {
		repoArg := x.(*models.Repository)
		return repoArg.Name == repoName && repoArg.Path == repoName
	}
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, repoName).Return(nil).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(matchFn)).Times(1),
	)

	// Test
	err := dbBlobLinkExists(env.ctx, env.db, r.Path, b.Digest, repoCacheMock)
	require.NoError(t, err)
}

func TestExistsBlobDB_NotExists(t *testing.T) {
	repoName := "bar"

	env := newEnv(t)
	defer env.shutdown(t)

	// Setup
	ctrl := gomock.NewController(t)

	_, r, _ := setupBlob(t, repoName, env)

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)
	matchFn := func(x any) bool {
		repoArg := x.(*models.Repository)
		return repoArg.Name == repoName && repoArg.Path == repoName
	}
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, repoName).Return(nil).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(matchFn)).Times(1),
	)

	// Test
	err := dbBlobLinkExists(env.ctx, env.db, r.Path, "sha256:297e345743c4708ac4c9c68f9a9f0ead1fcfccc660718b5ebcd3452e202bc2c2", repoCacheMock)
	require.ErrorContains(t, err, "blob unknown to registry")
}
