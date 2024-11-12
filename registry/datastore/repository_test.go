package datastore_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"sync"
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

// Why the SHA1 and not the actual lsnUpdateScript script source: Redis can cache the source of scripts so that clients
// don't have to re-send the script source with every invocation. Upon a first script EVAL, a script is hashed and then
// clients can use that SHA1 to invoke the same command with EVALSHA without transmitting its source. The
// github.com/redis/go-redis Redis client "optimistically uses EVALSHA to run the script. If script does not exist it
// is retried using EVAL".
const lsnUpdateScriptSha1 = "d01c935df18ece7f4483a3090060896c632e4d60"

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
	lsn, err := cache.GetLSN(ctx, repo)
	require.NoError(t, err)
	require.Empty(t, lsn)

	redisMock.ExpectEvalSha(lsnUpdateScriptSha1, []string{key}, []any{actualLSN, ttl.Seconds()}).SetVal("OK")
	err = cache.SetLSN(ctx, repo, actualLSN)
	require.NoError(t, err)

	redisMock.ExpectGet(key).SetVal(actualLSN)
	lsn, err = cache.GetLSN(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, actualLSN, lsn)

	require.NoError(t, redisMock.ExpectationsWereMet())
}

func TestCentralRepositoryCache_LSN_Error(t *testing.T) {
	ttl := 1 * time.Hour
	repo := &models.Repository{
		Path: "gitlab-org/gitlab",
	}

	redisCache, redisMock := testutil.RedisCacheMock(t, ttl)
	cache := datastore.NewCentralRepositoryCache(redisCache)
	ctx := context.Background()

	hex := digest.FromString(repo.Path).Hex()
	key := fmt.Sprintf("registry:db:{repository:%s:%s}:lsn", repo.TopLevelPathSegment(), hex)

	redisMock.ExpectGet(key).SetErr(errors.New("foo"))
	lsn, err := cache.GetLSN(ctx, repo)
	require.EqualError(t, err, "failed to read LSN key from cache: foo")
	require.Empty(t, lsn)

	redisMock.ExpectEvalSha(lsnUpdateScriptSha1, []string{key}, []any{lsn, ttl.Seconds()}).SetErr(errors.New("bar"))
	err = cache.SetLSN(ctx, repo, lsn)
	require.EqualError(t, err, "bar")

	require.NoError(t, redisMock.ExpectationsWereMet())
}

func TestCentralRepositoryCache_SetLSN_IsAtomic(t *testing.T) {
	redisCache := testutil.RedisCache(t, 0)
	cache := datastore.NewCentralRepositoryCache(redisCache)
	ctx := context.Background()

	currentLSN := "0/16B3748"
	higherLSN := "0/16B3750"
	lowerLSN := "0/16B3730"
	repo := &models.Repository{Path: "gitlab-org/gitlab"}

	// Ensure no LSN exists yet
	lsn, err := cache.GetLSN(ctx, repo)
	require.NoError(t, err)
	require.Empty(t, lsn)

	// Simulate setting the initial LSN in Redis
	err = cache.SetLSN(ctx, repo, currentLSN)
	require.NoError(t, err)
	lsn, err = cache.GetLSN(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, currentLSN, lsn)

	// Attempt to set a lower LSN, should not update
	err = cache.SetLSN(ctx, repo, lowerLSN)
	require.NoError(t, err)
	lsn, err = cache.GetLSN(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, currentLSN, lsn)

	// Attempt to set a higher LSN, should update
	err = cache.SetLSN(ctx, repo, higherLSN)
	require.NoError(t, err)
	lsn, err = cache.GetLSN(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, higherLSN, lsn)

	// Ensure atomic behavior by checking that the LSN is set to the highest value when concurrent updates occur
	lsnValues := []string{
		"0/16B3730",         // Lower LSN (Major part 0, lower minor)
		"0/16B3750",         // Higher LSN (Major part 0, higher minor)
		"1/16B3749",         // Same major, higher minor
		"2/16B3760",         // Higher major
		"1/16B3740",         // Same major, lower minor
		"2/16B3761",         // Higher major, slightly higher minor
		"3/16B3770",         // Even higher major
		"1/16B3755",         // Same major, in-between minor
		"0/16B3725",         // Lower major and lower minor
		"FFFFFFFF/FFFFFFFF", // Highest major and minor
	}
	rand.Shuffle(len(lsnValues), func(i, j int) { lsnValues[i], lsnValues[j] = lsnValues[j], lsnValues[i] })

	var wg sync.WaitGroup
	wg.Add(len(lsnValues))

	for _, lsn := range lsnValues {
		go func(lsn string) {
			defer wg.Done()
			require.NoError(t, cache.SetLSN(ctx, repo, lsn))
		}(lsn)
	}
	wg.Wait()

	finalLSN, err := cache.GetLSN(ctx, repo)
	require.NoError(t, err)
	require.Equal(t, "FFFFFFFF/FFFFFFFF", finalLSN)

	// Ensure that the key has a TTL (we constructed the cache with no default TTL, but the SetLSN script should have
	// set one for this key
	key := fmt.Sprintf("registry:db:{repository:%s:%s}:lsn", repo.TopLevelPathSegment(), digest.FromString(repo.Path).Hex())
	value, ttl, err := redisCache.GetWithTTL(ctx, key)
	require.NoError(t, err)
	require.Equal(t, finalLSN, value)
	require.NotZero(t, ttl)
}
