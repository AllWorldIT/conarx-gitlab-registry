//go:build integration && handlers_test

package handlers

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/migrations"
	"github.com/docker/distribution/registry/datastore/models"
	dbtestutil "github.com/docker/distribution/registry/datastore/testutil"
	"github.com/docker/distribution/registry/internal/testutil"
	gocache "github.com/eko/gocache/lib/v4/cache"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

type env struct {
	ctx    context.Context
	db     *datastore.DB
	cache  *gocache.Cache[any]
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

func witCachedRepositoryStore(t *testing.T) envOpt {
	return func(e *env) {
		if e.cache == nil {
			e.cache = testutil.RedisCache(t, testutil.RedisCacheTTL)
		}
		e.rStore = datastore.NewRepositoryStore(e.db, datastore.WithRepositoryCache(datastore.NewCentralRepositoryCache(e.cache)))
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

func setupBlob(t *testing.T, path string, e *env) (*models.Blob, *models.Repository, datastore.BlobStore) {
	t.Helper()

	// build test repository
	rStore := datastore.NewRepositoryStore(e.db)
	r, err := rStore.CreateByPath(e.ctx, "bar")
	require.NoError(t, err)
	require.NotNil(t, r)

	// add layer blob
	bStore := datastore.NewBlobStore(e.db)
	b := &models.Blob{
		MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
		Digest:    "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9",
		Size:      2802957,
	}
	err = bStore.Create(e.ctx, b)
	require.NoError(t, err)
	require.NotEmpty(t, r.ID)

	// link blob to repository
	err = rStore.LinkBlob(e.ctx, r, b.Digest)
	require.NoError(t, err)

	// make sure it's linked
	require.True(t, isBlobLinked(t, e, r, b.Digest))

	return b, r, bStore
}

func TestDeleteBlobDB(t *testing.T) {
	repoName := "bar"

	env := newEnv(t)
	defer env.shutdown(t)

	// Setup
	ttl := 30 * time.Minute
	redisCache, redisMock := testutil.RedisCacheMock(t, ttl)
	cache := datastore.NewCentralRepositoryCache(redisCache)

	key := "registry:db:{repository:" + repoName + ":fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9}"
	redisMock.ExpectGet(key).RedisNil()
	redisMock.CustomMatch(func(expected, actual []interface{}) error {
		var actDecoded models.Repository
		err := msgpack.Unmarshal((actual[2]).([]byte), &actDecoded)
		if err != nil {
			return err
		}
		if actDecoded.Name != repoName || actDecoded.Path != repoName {
			return fmt.Errorf("Bad data was set: %+v", actDecoded)
		}
		return nil
	}).ExpectSet(key, nil, ttl).SetVal("OK")

	b, r, bStore := setupBlob(t, repoName, env)

	// Test
	err := dbDeleteBlob(env.ctx, env.config, env.db, cache, r.Path, b.Digest)
	require.NoError(t, err)

	// the layer blob should still be there
	b2, err := bStore.FindByDigest(env.ctx, b.Digest)
	require.NoError(t, err)
	require.NotNil(t, b2)

	// but not the link for the repository
	require.False(t, isBlobLinked(t, env, r, b.Digest))

	require.NoError(t, redisMock.ExpectationsWereMet())
}

func TestDeleteBlobDB_RepositoryNotFound(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	ttl := 30 * time.Minute
	redisCache, redisMock := testutil.RedisCacheMock(t, ttl)
	cache := datastore.NewCentralRepositoryCache(redisCache)

	key := "registry:db:{repository:foo:2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae}"
	redisMock.ExpectGet(key).RedisNil()

	err := dbDeleteBlob(env.ctx, env.config, env.db, cache, "foo", "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9")
	require.ErrorIs(t, err, distribution.ErrRepositoryUnknown{Name: "foo"})

	require.NoError(t, redisMock.ExpectationsWereMet())
}

func TestExistsBlobDB_Exists(t *testing.T) {
	repoName := "bar"

	env := newEnv(t)
	defer env.shutdown(t)

	// Setup
	ttl := 30 * time.Minute
	redisCache, redisMock := testutil.RedisCacheMock(t, ttl)

	key := "registry:db:{repository:" + repoName + ":fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9}"
	redisMock.ExpectGet(key).RedisNil()
	redisMock.CustomMatch(func(expected, actual []interface{}) error {
		var actDecoded models.Repository
		err := msgpack.Unmarshal((actual[2]).([]byte), &actDecoded)
		if err != nil {
			return err
		}
		if actDecoded.Name != repoName || actDecoded.Path != repoName {
			return fmt.Errorf("Bad data was set: %+v", actDecoded)
		}
		return nil
	}).ExpectSet(key, nil, ttl).SetVal("OK")

	b, r, _ := setupBlob(t, repoName, env)

	// Test
	err := dbBlobLinkExists(env.ctx, env.db, r.Path, b.Digest, redisCache)
	require.NoError(t, err)

	require.NoError(t, redisMock.ExpectationsWereMet())
}

func TestExistsBlobDB_NotExists(t *testing.T) {
	repoName := "bar"

	env := newEnv(t)
	defer env.shutdown(t)

	// Setup
	ttl := 30 * time.Minute
	redisCache, redisMock := testutil.RedisCacheMock(t, ttl)

	key := "registry:db:{repository:" + repoName + ":fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9}"
	redisMock.ExpectGet(key).RedisNil()
	redisMock.CustomMatch(func(expected, actual []interface{}) error {
		var actDecoded models.Repository
		err := msgpack.Unmarshal((actual[2]).([]byte), &actDecoded)
		if err != nil {
			return err
		}
		if actDecoded.Name != repoName || actDecoded.Path != repoName {
			return fmt.Errorf("Bad data was set: %+v", actDecoded)
		}
		return nil
	}).ExpectSet(key, nil, ttl).SetVal("OK")

	_, r, _ := setupBlob(t, repoName, env)

	// Test
	err := dbBlobLinkExists(env.ctx, env.db, r.Path, "sha256:297e345743c4708ac4c9c68f9a9f0ead1fcfccc660718b5ebcd3452e202bc2c2", redisCache)
	require.ErrorContains(t, err, "blob unknown to registry")

	require.NoError(t, redisMock.ExpectationsWereMet())
}
