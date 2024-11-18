package handlers

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/handlers/mocks"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestRepositoryStats_IncrementPullCount(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockCache := mocks.NewMockRepositoryStatsCache(ctrl)
	repoStats := NewRepositoryStats(mockCache)

	ctx := context.Background()
	repo := &models.Repository{
		ID:   1,
		Path: "test/repo",
	}

	mockCache.EXPECT().Incr(ctx, repoStats.key(repo.Path, statsOperationPull)).Return(nil).Times(2)

	err := repoStats.IncrementPullCount(ctx, repo)
	assert.NoError(t, err)

	// Increase the stats again
	err = repoStats.IncrementPullCount(ctx, repo)
	assert.NoError(t, err)
}

func TestRepositoryStats_IncrementPushCount(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockCache := mocks.NewMockRepositoryStatsCache(ctrl)
	repoStats := NewRepositoryStats(mockCache)

	ctx := context.Background()
	repo := &models.Repository{
		ID:   1,
		Path: "test/repo",
	}

	mockCache.EXPECT().Incr(ctx, repoStats.key(repo.Path, statsOperationPush)).Return(nil).Times(2)

	err := repoStats.IncrementPushCount(ctx, repo)
	assert.NoError(t, err)

	err = repoStats.IncrementPushCount(ctx, repo)
	assert.NoError(t, err)
}

func TestRepositoryStats_CacheError(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockCache := mocks.NewMockRepositoryStatsCache(ctrl)
	repoStats := NewRepositoryStats(mockCache)

	ctx := context.Background()
	repo := &models.Repository{
		ID:   1,
		Path: "test/repo",
	}

	cacheErr := errors.New("cache error")

	t.Run("increase pull count", func(t *testing.T) {
		mockCache.EXPECT().Incr(ctx, repoStats.key(repo.Path, statsOperationPull)).Return(cacheErr).Times(1)

		err := repoStats.IncrementPullCount(ctx, repo)
		require.EqualError(t, err, "incrementing pull count in cache: cache error")
	})

	t.Run("increase push count", func(t *testing.T) {
		mockCache.EXPECT().Incr(ctx, repoStats.key(repo.Path, statsOperationPush)).Return(cacheErr).Times(1)

		err := repoStats.IncrementPushCount(ctx, repo)
		require.EqualError(t, err, "incrementing push count in cache: cache error")
	})
}

func TestRepositoryStats_key(t *testing.T) {
	tcs := map[string]struct {
		path        string
		op          string
		expectedKey string
	}{
		statsOperationPush: {
			path:        "test/repo",
			op:          statsOperationPush,
			expectedKey: "registry:api:{repository:test:c3ecf330c6173bf445635647db26f09843444527b55b3a0f5d5223d64045d378}:push",
		},
		statsOperationPull: {
			path:        "test/repo",
			op:          statsOperationPull,
			expectedKey: "registry:api:{repository:test:c3ecf330c6173bf445635647db26f09843444527b55b3a0f5d5223d64045d378}:pull",
		},
	}

	rs := NewRepositoryStats(nil)
	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			out := rs.key(tc.path, tc.op)
			require.Equal(t, tc.expectedKey, out)
		})
	}
}
