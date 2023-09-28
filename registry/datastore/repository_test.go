package datastore_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/internal/testutil"

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
	require.Equal(t, repo, r)

	nilSizeRepo := repo
	nilSizeRepo.Size = nil
	bytes, err = msgpack.Marshal(nilSizeRepo)
	redisMock.ExpectSet(key, bytes, ttl).SetVal("OK")
	require.NoError(t, err)
	cache.InvalidateSize(ctx, repo)

	require.NoError(t, redisMock.ExpectationsWereMet())
}
