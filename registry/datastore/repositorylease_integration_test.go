//go:build integration

package datastore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	itestutil "github.com/docker/distribution/registry/internal/testutil"
	"github.com/opencontainers/go-digest"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestCentralRepositoryLeaseCache(t *testing.T) {
	repoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.RenameLease,
	}

	// setup
	ttl := 30 * time.Minute
	redisCache, redisMock := itestutil.RedisCacheMock(t, ttl)

	// assert `Get` makes expected call to rediscache
	cache := datastore.NewCentralRepositoryLeaseCache(redisCache)
	ctx := context.Background()
	hex := digest.FromString(repoLease.Path).Hex()
	key := "registry:api:{repository-lease:gitlab-org:" + hex + "}"
	redisMock.ExpectGet(key).RedisNil()
	r, err := cache.Get(ctx, repoLease.Path, repoLease.Type)
	require.NoError(t, err)
	require.Empty(t, r)

	// assert `Set` makes expected call to rediscache
	bytes, err := msgpack.Marshal(repoLease)
	require.NoError(t, err)
	redisMock.ExpectSet(key, bytes, ttl).SetVal("OK")
	err = cache.Set(ctx, repoLease, ttl)
	require.NoError(t, err)

	// assert `Get` makes expected call to rediscache
	redisMock.ExpectGet(key).SetVal(string(bytes))
	r, err = cache.Get(ctx, repoLease.Path, repoLease.Type)
	require.NoError(t, err)
	require.Equal(t, repoLease, r)

	// assert `TTL` makes expected call to rediscache
	redisMock.ExpectGet(key).SetVal(string(bytes))
	redisMock.ExpectTTL(key).SetVal(ttl)
	returnedTTL, err := cache.TTL(ctx, repoLease)
	require.NoError(t, err)
	require.Equal(t, ttl, returnedTTL)

	// assert `Invalidate` makes expected call to rediscache
	redisMock.ExpectDel(key).SetVal(0)
	err = cache.Invalidate(ctx, repoLease.Path)
	require.NoError(t, err)

	require.NoError(t, redisMock.ExpectationsWereMet())
}

func TestRepositoryLeaseStore_RenameLease_FindRenameLeaseByPath_Empty(t *testing.T) {

	path := "a-test-group/foo"

	// create a store and try fetching the rename lease
	cache := datastore.NewCentralRepositoryLeaseCache(itestutil.RedisCache(t, 0))
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))
	expectedRenameLease, err := s.FindRenameByPath(suite.ctx, path)
	require.NoError(t, err)

	// verify the repo lease object in the cache does not exist
	require.Empty(t, expectedRenameLease)
}

func TestRepositoryLeaseStore_RenameLease_FindRenameLeaseByPath(t *testing.T) {
	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	expectedRepoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.RenameLease,
	}

	bytes, err := msgpack.Marshal(expectedRepoLease)
	require.NoError(t, err)
	hex := digest.FromString(expectedRepoLease.Path).Hex()
	key := "registry:api:{repository-lease:gitlab-org:" + hex + "}"
	redisMock.ExpectGet(key).SetVal(string(bytes))

	// create a store and try fetching the rename lease
	cache := datastore.NewCentralRepositoryLeaseCache(redisCache)
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))
	actualRenameLease, err := s.FindRenameByPath(suite.ctx, expectedRepoLease.Path)
	require.NoError(t, err)

	// verify the repo lease object in the cache is identical to the one the mock redis server was setup with:
	require.Equal(t, actualRenameLease, actualRenameLease)
}

func TestRepositoryLeaseStore_RenameLease_GetLeaseTTL(t *testing.T) {
	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	repoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.RenameLease,
	}

	bytes, err := msgpack.Marshal(repoLease)
	expectedTTL := 60 * time.Second
	require.NoError(t, err)
	hex := digest.FromString(repoLease.Path).Hex()
	key := "registry:api:{repository-lease:gitlab-org:" + hex + "}"
	redisMock.ExpectGet(key).SetVal(string(bytes))
	redisMock.ExpectTTL(key).SetVal(expectedTTL)

	// create a store and try fetching the rename lease TTL
	cache := datastore.NewCentralRepositoryLeaseCache(redisCache)
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))
	ttl, err := s.GetTTL(suite.ctx, repoLease)
	require.NoError(t, err)

	// verify the repo lease TTL in the cache is identical to the one the mock redis server was setup with:
	require.Equal(t, ttl, expectedTTL)
}

func TestRepositoryLeaseStore_RenameLease_GetLeaseTTL_Fails(t *testing.T) {
	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	repoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.RenameLease,
	}

	bytes, err := msgpack.Marshal(repoLease)
	require.NoError(t, err)
	hex := digest.FromString(repoLease.Path).Hex()
	key := "registry:api:{repository-lease:gitlab-org:" + hex + "}"
	redisMock.ExpectGet(key).SetVal(string(bytes))
	redisMock.ExpectTTL(key).SetErr(fmt.Errorf("an error"))

	// create a store and try fetching the rename lease TTL
	cache := datastore.NewCentralRepositoryLeaseCache(redisCache)
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))
	ttl, err := s.GetTTL(suite.ctx, repoLease)

	// verify an errors is returned as configured by the redis server mocks
	require.Error(t, err)
	require.Empty(t, ttl)
}

func TestRepositoryLeaseStore_RenameLease_GetLeaseTTL_LeaseExtractionFails(t *testing.T) {
	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	repoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.RenameLease,
	}
	expectedTTL := 60 * time.Second

	hex := digest.FromString(repoLease.Path).Hex()
	key := "registry:api:{repository-lease:gitlab-org:" + hex + "}"
	redisMock.ExpectGet(key).SetVal("not-a-lease-object")
	redisMock.ExpectTTL(key).SetVal(expectedTTL)

	// create a store and try fetching the rename lease TTL
	cache := datastore.NewCentralRepositoryLeaseCache(redisCache)
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))
	ttl, err := s.GetTTL(suite.ctx, repoLease)

	// assert "not-a-lease-object" returns an error when marshaling into a repository lease object
	require.Error(t, err)
	require.Empty(t, ttl)
}

func TestRepositoryLeaseStore_RenameLease_GetLeaseTTL_ConflictingGrantor(t *testing.T) {
	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	repoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.RenameLease,
	}

	bytes, err := msgpack.Marshal(repoLease)
	expectedTTL := 60 * time.Second
	require.NoError(t, err)
	hex := digest.FromString(repoLease.Path).Hex()
	key := "registry:api:{repository-lease:gitlab-org:" + hex + "}"
	redisMock.ExpectGet(key).SetVal(string(bytes))
	redisMock.ExpectTTL(key).SetVal(expectedTTL)

	// create a store and try fetching the rename lease TTL
	cache := datastore.NewCentralRepositoryLeaseCache(redisCache)
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))
	conflictingGrantorLease := *repoLease
	conflictingGrantorLease.GrantedTo = "gitlab-org/another-gitlab-name"
	ttl, err := s.GetTTL(suite.ctx, &conflictingGrantorLease)

	// verify the repo lease TTL in the cache is 0 for the wrong grantor and there is no error
	require.Empty(t, ttl, 0)
	require.NoError(t, err)
}

func TestRepositoryLeaseStore_FindRenameLeaseByPath_UnknownLeaseType(t *testing.T) {
	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	repoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.LeaseType("unknownLeaseType"),
	}

	bytes, err := msgpack.Marshal(repoLease)
	require.NoError(t, err)
	hex := digest.FromString(repoLease.Path).Hex()
	key := "registry:api:{repository-lease:gitlab-org:" + hex + "}"
	redisMock.ExpectGet(key).SetVal(string(bytes))

	// Create a store and try fetching the rename lease
	cache := datastore.NewCentralRepositoryLeaseCache(redisCache)
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))
	actualRepoLease, err := s.FindRenameByPath(suite.ctx, repoLease.Path)

	// assert that the "unknownLeaseType" returns an error when marshaling into a repository lease object
	require.NoError(t, err)
	// assert that the returned lease is empty
	require.Empty(t, actualRepoLease)
}

func TestRepositoryLeaseStore_FindRenameLeaseByPath_Empty(t *testing.T) {
	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	repoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.RenameLease,
	}

	hex := digest.FromString(repoLease.Path).Hex()
	key := "registry:api:{repository-lease:gitlab-org:" + hex + "}"
	redisMock.ExpectGet(key).SetErr(redis.Nil)

	// create a store and try fetching the rename lease TTL
	cache := datastore.NewCentralRepositoryLeaseCache(redisCache)
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))
	actualRepoLease, err := s.FindRenameByPath(suite.ctx, repoLease.Path)

	// assert there is no error returned when the requested lease does not exist (i.e redis.Nil)
	require.NoError(t, err)
	// assert a nil lease is returned
	require.Empty(t, actualRepoLease)
}

func TestRepositoryLeaseStore_FindRenameLeaseByPath_Fails(t *testing.T) {
	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	repoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.RenameLease,
	}

	hex := digest.FromString(repoLease.Path).Hex()
	key := "registry:api:{repository-lease:gitlab-org:" + hex + "}"
	redisMock.ExpectGet(key).SetErr(fmt.Errorf("an error"))

	// create a store and try fetching the rename lease TTL
	cache := datastore.NewCentralRepositoryLeaseCache(redisCache)
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))
	actualRepoLease, err := s.FindRenameByPath(suite.ctx, repoLease.Path)

	// assert an expected configured error is returned
	require.Error(t, err)
	require.Empty(t, actualRepoLease)
}

func TestRepositoryLeaseStore_FindRenameLeaseByPath_LeaseExtractionFails(t *testing.T) {
	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	repoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.RenameLease,
	}

	hex := digest.FromString(repoLease.Path).Hex()
	key := "registry:api:{repository-lease:gitlab-org:" + hex + "}"
	redisMock.ExpectGet(key).SetVal("not-a-lease-object")

	// create a store and try fetching the rename lease TTL
	cache := datastore.NewCentralRepositoryLeaseCache(redisCache)
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))
	actualRepoLease, err := s.FindRenameByPath(suite.ctx, repoLease.Path)

	// assert "not-a-lease-object" returns an error when marshaling into a repository lease object
	require.Error(t, err)
	require.Empty(t, actualRepoLease)
}

func TestRepositoryLeaseStore_RenameLease_CreateLease(t *testing.T) {
	repoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.RenameLease,
	}

	ttl := 60 * time.Minute
	// create a store
	cache := datastore.NewCentralRepositoryLeaseCache(itestutil.RedisCache(t, ttl))
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))

	// create a lease
	createdLease, err := s.UpsertRename(suite.ctx, repoLease)
	require.NoError(t, err)
	require.Equal(t, repoLease, createdLease)

	// verify the lease exists
	foundLease, err := s.FindRenameByPath(suite.ctx, repoLease.Path)
	require.NoError(t, err)
	require.Equal(t, repoLease, foundLease)
}

func TestRepositoryLeaseStore_RenameLease_CreateLease_Empty(t *testing.T) {

	ttl := 60 * time.Minute
	// create a store
	cache := datastore.NewCentralRepositoryLeaseCache(itestutil.RedisCache(t, ttl))
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))

	// create a lease
	createdLease, err := s.UpsertRename(suite.ctx, nil)

	// verify a lease is not created and an error is returned
	require.Error(t, err)
	require.Empty(t, createdLease)
}

func TestRepositoryLeaseStore_RenameLease_CreateLease_Fails(t *testing.T) {
	repoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.RenameLease,
	}

	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	hex := digest.FromString(repoLease.Path).Hex()
	key := "registry:api:{repository-lease:gitlab-org:" + hex + "}"
	bytes, err := msgpack.Marshal(repoLease)
	require.NoError(t, err)
	ttl := 30 * time.Minute
	redisMock.ExpectSet(key, bytes, ttl).SetErr(fmt.Errorf("an error"))

	// create a store
	cache := datastore.NewCentralRepositoryLeaseCache(redisCache)
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))

	// create a lease
	createdLease, err := s.UpsertRename(suite.ctx, repoLease)

	// verify the lease is created successfully
	require.Error(t, err)
	require.Empty(t, createdLease)
}

func TestRepositoryLeaseStore_DestroyLease(t *testing.T) {
	repoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.RenameLease,
	}

	ttl := 60 * time.Minute
	// Create a store
	cache := datastore.NewCentralRepositoryLeaseCache(itestutil.RedisCache(t, ttl))
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))

	// create a lease
	createdLease, err := s.UpsertRename(suite.ctx, repoLease)
	require.NoError(t, err)
	require.Equal(t, repoLease, createdLease)

	// verify the lease exists
	foundLease, err := s.FindRenameByPath(suite.ctx, repoLease.Path)
	require.NoError(t, err)
	require.Equal(t, repoLease, foundLease)

	// destroy the lease
	err = s.Destroy(suite.ctx, repoLease)
	require.NoError(t, err)

	// verify the lease is destroyed
	foundLease, err = s.FindRenameByPath(suite.ctx, repoLease.Path)
	require.NoError(t, err)
	require.Empty(t, foundLease)
}

func TestRepositoryLeaseStore_DestroyLease_Idempotent(t *testing.T) {
	repoLease := &models.RepositoryLease{
		GrantedTo: "gitlab-org/old-gitlab-name",
		Path:      "gitlab-org/new-gitlab-name",
		Type:      models.RenameLease,
	}

	ttl := 60 * time.Minute
	// Create a store
	cache := datastore.NewCentralRepositoryLeaseCache(itestutil.RedisCache(t, ttl))
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))

	// create a lease
	createdLease, err := s.UpsertRename(suite.ctx, repoLease)
	require.NoError(t, err)
	require.Equal(t, repoLease, createdLease)

	// verify the lease exists
	foundLease, err := s.FindRenameByPath(suite.ctx, repoLease.Path)
	require.NoError(t, err)
	require.Equal(t, repoLease, foundLease)

	// destroy the lease
	err = s.Destroy(suite.ctx, repoLease)
	require.NoError(t, err)

	// destroy the lease again
	err = s.Destroy(suite.ctx, repoLease)

	// assert destroying a lease never fails no matter how much it is called (if redis is running)
	require.NoError(t, err)
}

func TestRepositoryLeaseStore_DestroyLease_Fails(t *testing.T) {
	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	path := "gitlab-org/new-gitlab-name"

	hex := digest.FromString(path).Hex()
	key := "registry:api:{repository-lease:gitlab-org:" + hex + "}"
	redisMock.ExpectDel(key).SetErr(fmt.Errorf("an error"))

	// Create a store and try fetching the rename lease
	cache := datastore.NewCentralRepositoryLeaseCache(redisCache)
	s := datastore.NewRepositoryLeaseStore(datastore.WithRepositoryLeaseCache(cache))
	err := s.Destroy(suite.ctx, &models.RepositoryLease{Path: path})
	// assert destroying a lease returns a failure if redis fails
	require.Error(t, err)
}
