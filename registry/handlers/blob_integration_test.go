//go:build integration && handlers_test

package handlers

import (
	"context"
	"os"
	"testing"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/migrations"
	"github.com/docker/distribution/registry/datastore/models"
	dbtestutil "github.com/docker/distribution/registry/datastore/testutil"
	gocache "github.com/eko/gocache/lib/v4/cache"
	"github.com/stretchr/testify/require"
)

type env struct {
	ctx    context.Context
	db     *datastore.DB
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

	m := migrations.NewMigrator(db.DB)
	_, err = m.Up()
	require.NoError(t, err)
}

type envOpt func(*env)

func witCachedRepositoryStore(cache *gocache.Cache[any]) envOpt {
	return func(e *env) {
		e.rStore = datastore.NewRepositoryStore(e.db, datastore.WithRepositoryCache(datastore.NewCentralRepositoryCache(cache)))
	}
}

func newEnv(t *testing.T, opts ...envOpt) *env {
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

	for _, o := range opts {
		o(env)
	}

	// set a default repository store if not set by options
	if env.rStore == nil {
		env.rStore = datastore.NewRepositoryStore(env.db)
	}

	return env
}

func TestDeleteBlobDB(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	// Setup

	// build test repository
	rStore := datastore.NewRepositoryStore(env.db)
	r, err := rStore.CreateByPath(env.ctx, "bar")
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

	// Test

	err = dbDeleteBlob(env.ctx, env.config, env.db, datastore.NewNoOpRepositoryCache(), r.Path, b.Digest)
	require.NoError(t, err)

	// the layer blob should still be there
	b2, err := bStore.FindByDigest(env.ctx, b.Digest)
	require.NoError(t, err)
	require.NotNil(t, b2)

	// but not the link for the repository
	require.False(t, isBlobLinked(t, env, r, b.Digest))
}

func TestDeleteBlobDB_RepositoryNotFound(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	err := dbDeleteBlob(env.ctx, env.config, env.db, datastore.NewNoOpRepositoryCache(), "foo", "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9")
	require.Error(t, err)
}
