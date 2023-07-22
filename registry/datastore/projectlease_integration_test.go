//go:build integration

package datastore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/docker/distribution/registry/datastore"
	itestutil "github.com/docker/distribution/registry/internal/testutil"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestNewProjectLeaseStore_Fails(t *testing.T) {
	s, err := datastore.NewProjectLeaseStore(nil)
	require.Empty(t, s)
	require.Error(t, err)
	require.EqualError(t, err, "cache can not be empty")
}

func TestCentralProjectLeaseCache(t *testing.T) {
	path := "group-name/project-name"

	// setup
	ttl := 30 * time.Minute
	redisCache, redisMock := itestutil.RedisCacheMock(t, ttl)

	// assert `Exists` makes expected call to rediscache
	cache := datastore.NewCentralProjectLeaseCache(redisCache)
	ctx := context.Background()
	hex := digest.FromString(path).Hex()
	key := "registry:api:{project-lease:group-name:" + hex + "}"
	redisMock.ExpectGet(key).RedisNil()
	isExist, err := cache.Exists(ctx, path)
	require.NoError(t, err)
	require.False(t, isExist)

	// assert `Set` makes expected call to rediscache
	redisMock.ExpectSet(key, path, ttl).SetVal("OK")
	err = cache.Set(ctx, path, ttl)
	require.NoError(t, err)

	// assert `Exists` makes expected call to rediscache
	redisMock.ExpectGet(key).SetVal(path)
	isExist, err = cache.Exists(ctx, path)
	require.NoError(t, err)
	require.True(t, isExist)

	// assert `Invalidate` makes expected call to rediscache
	redisMock.ExpectDel(key).SetVal(0)
	err = cache.Invalidate(ctx, path)
	require.NoError(t, err)

	require.NoError(t, redisMock.ExpectationsWereMet())
}

func TestProjectLeaseStore_Exists_Empty(t *testing.T) {

	path := "a-test-group/foo"

	// create a store and try checking the existence of the project lease
	// associated with `path`
	cache := datastore.NewCentralProjectLeaseCache(itestutil.RedisCache(t, 0))
	s, err := datastore.NewProjectLeaseStore(cache)
	require.NoError(t, err)

	isExist, err := s.Exists(suite.ctx, path)
	require.NoError(t, err)

	// verify the project lease does not exist in the cache
	require.False(t, isExist)
}

func TestProjectLeaseStore_Exists(t *testing.T) {
	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	path := "gitlab-group/gitlab-project"

	hex := digest.FromString(path).Hex()
	key := "registry:api:{project-lease:gitlab-group:" + hex + "}"
	redisMock.ExpectGet(key).SetVal(path)

	// create a store and try fetching the project lease
	cache := datastore.NewCentralProjectLeaseCache(redisCache)
	s, err := datastore.NewProjectLeaseStore(cache)
	require.NoError(t, err)

	exists, err := s.Exists(suite.ctx, path)
	require.NoError(t, err)

	// verify the project lease object exists in the cache
	require.True(t, exists)
}

func TestProjectLeaseStore_Exists_Fails(t *testing.T) {
	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	path := "gitlab-group/gitlab-project"

	hex := digest.FromString(path).Hex()
	key := "registry:api:{project-lease:gitlab-group:" + hex + "}"
	expectedErr := errors.New("an error")
	redisMock.ExpectGet(key).SetErr(expectedErr)

	// create a store and try fetching the project lease
	cache := datastore.NewCentralProjectLeaseCache(redisCache)
	s, err := datastore.NewProjectLeaseStore(cache)
	require.NoError(t, err)

	exists, err := s.Exists(suite.ctx, path)

	// assert an expected error is returned
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)

	// verify the project lease object exists in the cache
	require.False(t, exists)
}

func TestProjectLeaseStore_Set(t *testing.T) {
	path := "gitlab-group/gitlab-project"
	ttl := 60 * time.Minute

	// create a store
	cache := datastore.NewCentralProjectLeaseCache(itestutil.RedisCache(t, ttl))
	s, err := datastore.NewProjectLeaseStore(cache)
	require.NoError(t, err)

	// create a lease
	err = s.Set(suite.ctx, path, ttl)
	require.NoError(t, err)

	// verify the lease exists
	isExist, err := s.Exists(suite.ctx, path)
	require.NoError(t, err)
	require.True(t, isExist)
}

func TestProjectLeaseStore_Set_Empty(t *testing.T) {

	ttl := 60 * time.Minute

	// create a store
	cache := datastore.NewCentralProjectLeaseCache(itestutil.RedisCache(t, ttl))
	s, err := datastore.NewProjectLeaseStore(cache)
	require.NoError(t, err)

	// create a lease
	err = s.Set(suite.ctx, "", ttl)

	// verify a lease is not created and an error is returned
	require.Error(t, err)
	require.EqualError(t, err, "project lease path can not be empty")
}

func TestProjectLeaseStore_Set_Fails(t *testing.T) {
	path := "gitlab-group/gitlab-project"

	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	hex := digest.FromString(path).Hex()
	key := "registry:api:{project-lease:gitlab-group:" + hex + "}"
	ttl := 30 * time.Minute
	expectedErr := errors.New("an error")
	redisMock.ExpectSet(key, path, ttl).SetErr(expectedErr)

	// create a store
	cache := datastore.NewCentralProjectLeaseCache(redisCache)
	s, err := datastore.NewProjectLeaseStore(cache)
	require.NoError(t, err)

	// create a lease
	err = s.Set(suite.ctx, path, ttl)

	// verify the lease is created succesfully
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
}

func TestProjectLeaseStore_InvalidateLease(t *testing.T) {
	path := "gitlab-group/gitlab-project"

	ttl := 60 * time.Minute
	// Create a store
	cache := datastore.NewCentralProjectLeaseCache(itestutil.RedisCache(t, ttl))
	s, err := datastore.NewProjectLeaseStore(cache)
	require.NoError(t, err)

	// create a lease
	err = s.Set(suite.ctx, path, ttl)
	require.NoError(t, err)

	// verify the lease exists
	isExist, err := s.Exists(suite.ctx, path)
	require.NoError(t, err)
	require.True(t, isExist)

	// destroy the lease
	err = s.Invalidate(suite.ctx, path)
	require.NoError(t, err)

	// verify the lease is invalidated
	isExist, err = s.Exists(suite.ctx, path)
	require.NoError(t, err)
	require.False(t, isExist)

}

func TestProjectLeaseStore_InvalidateLease_Idempotent(t *testing.T) {
	path := "gitlab-group/gitlab-project"

	ttl := 60 * time.Minute
	// Create a store
	cache := datastore.NewCentralProjectLeaseCache(itestutil.RedisCache(t, ttl))
	s, err := datastore.NewProjectLeaseStore(cache)
	require.NoError(t, err)

	// create a lease
	err = s.Set(suite.ctx, path, ttl)
	require.NoError(t, err)

	// verify the lease exists
	isExist, err := s.Exists(suite.ctx, path)
	require.NoError(t, err)
	require.True(t, isExist)

	// invalidate the lease
	err = s.Invalidate(suite.ctx, path)
	require.NoError(t, err)

	// invalidate the lease again
	err = s.Invalidate(suite.ctx, path)
	// assert destroying a lease never fails no matter how much it is called (if redis is running)
	require.NoError(t, err)
}

func TestProjectLeaseStore_InvalidateLease_Fails(t *testing.T) {
	// setup mock responses from redis
	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	path := "gitlab-org/new-gitlab-name"

	hex := digest.FromString(path).Hex()
	key := "registry:api:{project-lease:gitlab-org:" + hex + "}"
	expectedErr := errors.New("an error")
	redisMock.ExpectDel(key).SetErr(expectedErr)

	// Create a store and try fetching the project lease
	cache := datastore.NewCentralProjectLeaseCache(redisCache)
	s, err := datastore.NewProjectLeaseStore(cache)
	require.NoError(t, err)

	err = s.Invalidate(suite.ctx, path)

	// assert destroying a lease returns a failure if redis fails
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
}
