//go:generate mockgen -package mocks -destination mocks/stats.go . RepositoryStatsCache

package handlers

import (
	"context"
	"fmt"
	"strings"

	"github.com/docker/distribution/registry/datastore/models"

	"github.com/opencontainers/go-digest"
)

const (
	statsOperationPush = "push"
	statsOperationPull = "pull"
)

// RepositoryStatsCache describes the behavior of the repository statistics cache
// that can be used to temporarily save the stats before flushing into a
// storage
type RepositoryStatsCache interface {
	Incr(context.Context, string) error
}

// RepositoryStats knows how to increase the download and upload counts for a
// repository in the cache.
// TODO: attach a repository store to upsert records in the DB in bulk.
// TODO: create worker that will flush the cache to the DB periodically.
type RepositoryStats struct {
	cache RepositoryStatsCache
}

func NewRepositoryStats(cache RepositoryStatsCache) *RepositoryStats {
	return &RepositoryStats{
		cache: cache,
	}
}

// IncrementPullCount upserts the value of the models.Repository count in the cache
func (rs *RepositoryStats) IncrementPullCount(ctx context.Context, r *models.Repository) error {
	if err := rs.cache.Incr(ctx, rs.key(r.Path, statsOperationPull)); err != nil {
		return fmt.Errorf("incrementing pull count in cache: %w", err)
	}

	return nil
}

// IncrementPushCount upserts the value of the models.Repository count in the cache
func (rs *RepositoryStats) IncrementPushCount(ctx context.Context, r *models.Repository) error {
	if err := rs.cache.Incr(ctx, rs.key(r.Path, statsOperationPush)); err != nil {
		return fmt.Errorf("incrementing push count in cache: %w", err)
	}

	return nil
}

// key generates a valid Redis key string for a given repository stats object. The used key format is described in
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/redis-dev-guidelines.md#key-format.
func (*RepositoryStats) key(path, op string) string {
	nsPrefix := strings.Split(path, "/")[0]
	hex := digest.FromString(path).Hex()
	return fmt.Sprintf("registry:api:{repository:%s:%s}:%s", nsPrefix, hex, op)
}
