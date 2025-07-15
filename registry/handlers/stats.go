//go:generate mockgen -package mocks -destination mocks/stats.go . RepositoryStatsCache

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/docker/distribution/registry/api/errcode"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/version"
	"github.com/gorilla/handlers"

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

// StatisticsAPIResponse is the top-level statistics API response.
type StatisticsAPIResponse struct {
	Release  ReleaseStats               `json:"release"`
	Database DatabaseStats              `json:"database"`
	Imports  []*models.ImportStatistics `json:"import,omitempty"`
}

// ReleaseStats contain information associated with the registry binary itself,
// such as the version number, supported features, and other things not expected
// to change based on different configurations or runtime activity.
type ReleaseStats struct {
	ExtFeatures string `json:"ext_features"`
	Version     string `json:"version"`
}

// DatabaseStats contain information related to the metadata database.
type DatabaseStats struct {
	Enabled bool `json:"enabled"`
}

type statisticsHandler struct {
	*Context
}

func statisticsDispatcher(ctx *Context, _ *http.Request) http.Handler {
	statsHandler := &statisticsHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		http.MethodGet: http.HandlerFunc(statsHandler.HandleGetStatistics),
	}
}

func (h *statisticsHandler) HandleGetStatistics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	resp := StatisticsAPIResponse{
		Release: ReleaseStats{
			ExtFeatures: version.ExtFeatures,
			Version:     version.Version,
		},
		Database: DatabaseStats{
			Enabled: h.Config.Database.Enabled,
		},
	}

	imports, err := datastore.NewImportStatisticsStore(h.db.Primary()).FindAll(h)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	if len(imports) != 0 {
		resp.Imports = imports
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
}
