package datastore_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/internal/testutil"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestCentralRepositoryCache(t *testing.T) {
	var size int64 = 1
	repo := &models.Repository{
		ID:          1,
		NamespaceID: 1,
		Name:        "gitlab",
		Path:        "gitlab-org/gitlab",
		CreatedAt:   time.Now().Local(),
		UpdatedAt:   sql.NullTime{Time: time.Now().Local(), Valid: true},
		Size:        &size,
	}

	ttl := 30 * time.Minute
	redisCache, redisMock := testutil.RedisCacheMock(t, ttl)
	cache := datastore.NewCentralRepositoryCache(redisCache)
	ctx := context.Background()

	key := "registry:db:{repository:gitlab-org:6fc8277be731c24196adfdfbbf4fab5a760941f1808efc8e2f37d1fae8b44ac3}"
	redisMock.ExpectGet(key).RedisNil()
	r := cache.Get(ctx, repo.Path)
	require.Nil(t, r)

	bytes, err := msgpack.Marshal(repo)
	require.NoError(t, err)
	redisMock.ExpectSet(key, bytes, ttl).SetVal("OK")
	cache.Set(ctx, repo)

	redisMock.ExpectGet(key).SetVal(string(bytes))
	r = cache.Get(ctx, repo.Path)
	// msgpack uses time.Local as the Location for time.Time, but we expect UTC.
	// This is irrelevant for this test as r.DeletedAt.Valid = false so we can clear the value.
	// This is related to https://github.com/vmihailenco/msgpack/issues/332
	r.DeletedAt = sql.NullTime{}
	require.Equal(t, repo, r)

	nilSizeRepo := repo
	nilSizeRepo.Size = nil
	bytes, err = msgpack.Marshal(nilSizeRepo)
	redisMock.ExpectSet(key, bytes, ttl).SetVal("OK")
	require.NoError(t, err)
	cache.InvalidateSize(ctx, repo)

	require.NoError(t, redisMock.ExpectationsWereMet())
}

func TestCentralRepositoryCache_LSN(t *testing.T) {
	actualLSN := "0/16B3748"
	ttl := 1 * time.Hour
	repo := &models.Repository{
		Path: "gitlab-org/gitlab",
	}

	redisCache, redisMock := testutil.RedisCacheMock(t, ttl)
	cache := datastore.NewCentralRepositoryCache(redisCache)
	ctx := context.Background()

	hex := digest.FromString(repo.Path).Hex()
	key := fmt.Sprintf("registry:db:{repository:%s:%s}:lsn", repo.TopLevelPathSegment(), hex)

	redisMock.ExpectGet(key).RedisNil()
	lsn := cache.GetLSN(ctx, repo)
	require.Empty(t, lsn)

	redisMock.ExpectSet(key, actualLSN, ttl).SetVal("OK")
	cache.SetLSN(ctx, repo, actualLSN)

	redisMock.ExpectGet(key).SetVal(actualLSN)
	lsn = cache.GetLSN(ctx, repo)
	require.Equal(t, actualLSN, lsn)

	require.NoError(t, redisMock.ExpectationsWereMet())
}

func TestCentralRepositoryCache_LSN_Error(t *testing.T) {
	lsn := "0/16B3748"
	ttl := 1 * time.Hour
	err := errors.New("foo")
	repo := &models.Repository{
		Path: "gitlab-org/gitlab",
	}

	redisCache, redisMock := testutil.RedisCacheMock(t, ttl)
	cache := datastore.NewCentralRepositoryCache(redisCache)
	ctx := context.Background()

	hex := digest.FromString(repo.Path).Hex()
	key := fmt.Sprintf("registry:db:{repository:%s:%s}:lsn", repo.TopLevelPathSegment(), hex)

	redisMock.ExpectGet(key).SetErr(err)
	require.Empty(t, cache.GetLSN(ctx, repo))

	redisMock.ExpectSet(key, lsn, ttl).SetErr(err)
	cache.SetLSN(ctx, repo, lsn)

	require.NoError(t, redisMock.ExpectationsWereMet())
}
