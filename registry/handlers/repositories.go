package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	v1 "github.com/docker/distribution/registry/api/gitlab/v1"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"

	"github.com/gorilla/handlers"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
)

type repositoryHandler struct {
	*Context
}

func repositoryDispatcher(ctx *Context, _ *http.Request) http.Handler {
	repositoryHandler := &repositoryHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		http.MethodGet: http.HandlerFunc(repositoryHandler.GetRepository),
	}
}

type RepositoryAPIResponse struct {
	Name          string `json:"name"`
	Path          string `json:"path"`
	Size          *int64 `json:"size_bytes,omitempty"`
	SizePrecision string `json:"size_precision,omitempty"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at,omitempty"`
}

const (
	sizeQueryParamKey                      = "size"
	sizeQueryParamSelfValue                = "self"
	sizeQueryParamSelfWithDescendantsValue = "self_with_descendants"
	nQueryParamKey                         = "n"
	nQueryParamValueMin                    = 1
	nQueryParamValueMax                    = 1000
	lastQueryParamKey                      = "last"
)

var (
	nQueryParamValidTypes = []reflect.Kind{reflect.Int}

	sizeQueryParamValidValues = []string{
		sizeQueryParamSelfValue,
		sizeQueryParamSelfWithDescendantsValue,
	}

	lastTagQueryParamPattern  = reference.TagRegexp
	lastPathQueryParamPattern = reference.NameRegexp
)

func isQueryParamValueValid(value string, validValues []string) bool {
	for _, v := range validValues {
		if value == v {
			return true
		}
	}
	return false
}

func isQueryParamTypeInt(value string) (int, bool) {
	i, err := strconv.Atoi(value)
	return i, err == nil
}

func isQueryParamIntValueInBetween(value, min, max int) bool {
	return value >= min && value <= max
}

func queryParamValueMatchesPattern(value string, pattern *regexp.Regexp) bool {
	return pattern.MatchString(value)
}

func sizeQueryParamValue(r *http.Request) string {
	return r.URL.Query().Get(sizeQueryParamKey)
}

// timeToString converts a time.Time to a ISO 8601 with millisecond precision string. This is the standard format used
// across GitLab applications.
func timeToString(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

const (
	// sizePrecisionDefault is used for repository size measurements with default precision, i.e., only tagged (directly
	// or indirectly) layers are taken into account.
	sizePrecisionDefault = "default"
	// sizePrecisionUntagged is used for repository size measurements where full precision is not possible, and instead
	// we fall back to an estimate that also accounts for untagged layers (if any).
	sizePrecisionUntagged = "untagged"
)

func (h *repositoryHandler) GetRepository(w http.ResponseWriter, r *http.Request) {
	l := log.GetLogger(log.WithContext(h)).WithFields(log.Fields{"path": h.Repository.Named().Name()})
	l.Debug("GetRepository")

	var withSize bool
	sizeVal := sizeQueryParamValue(r)
	if sizeVal != "" {
		if !isQueryParamValueValid(sizeVal, sizeQueryParamValidValues) {
			detail := v1.InvalidQueryParamValueErrorDetail(sizeQueryParamKey, sizeQueryParamValidValues)
			h.Errors = append(h.Errors, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail))
			return
		}
		withSize = true
	}

	var opts []datastore.RepositoryStoreOption
	if h.App.redisCache != nil {
		opts = append(opts, datastore.WithRepositoryCache(datastore.NewCentralRepositoryCache(h.App.redisCache)))
	}
	store := datastore.NewRepositoryStore(h.db, opts...)

	repo, err := store.FindByPath(h.Context, h.Repository.Named().Name())
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
	if repo == nil {
		// If the caller is requesting the aggregated size of repository `foo/bar` including descendants, it might be
		// the case that `foo/bar` does not exist but there is an e.g. `foo/bar/car`. In such case we should not raise a
		// 404 if the base repository (`foo/bar`) does not exist. This is required to allow retrieving the Project level
		// usage when there is no "root" repository for such project but there is at least one sub-repository.
		if !withSize || sizeVal != sizeQueryParamSelfWithDescendantsValue {
			h.Errors = append(h.Errors, v2.ErrorCodeNameUnknown)
			return
		}
		// If this is the case, we need to find the corresponding top-level namespace. That must exist. If not we
		// throw a 404 Not Found here.
		repo = &models.Repository{Path: h.Repository.Named().Name()}
		ns := datastore.NewNamespaceStore(h.db)
		n, err := ns.FindByName(h.Context, repo.TopLevelPathSegment())
		if err != nil {
			h.Errors = append(h.Errors, errcode.FromUnknownError(err))
			return
		}
		if n == nil {
			h.Errors = append(h.Errors, v2.ErrorCodeNameUnknown)
			return
		}
		// path and namespace ID are the two required parameters for the queries in repositoryStore.SizeWithDescendants,
		// so we must fill those. We also fill the name for consistency on the response.
		repo.NamespaceID = n.ID
		repo.Name = repo.Path[strings.LastIndex(repo.Path, "/")+1:]
	}

	resp := RepositoryAPIResponse{
		Name: repo.Name,
		Path: repo.Path,
	}
	if !repo.CreatedAt.IsZero() {
		resp.CreatedAt = timeToString(repo.CreatedAt)
	}
	if repo.UpdatedAt.Valid {
		resp.UpdatedAt = timeToString(repo.UpdatedAt.Time)
	}

	if withSize {
		var size int64
		precision := sizePrecisionDefault

		t := time.Now()
		ctx := h.Context.Context

		switch sizeVal {
		case sizeQueryParamSelfValue:
			size, err = store.Size(ctx, repo)
		case sizeQueryParamSelfWithDescendantsValue:
			size, err = store.SizeWithDescendants(ctx, repo)
			if err != nil {
				var pgErr *pgconn.PgError
				// if this same query has timed out in the last 24h OR times out now, fallback to estimation
				if errors.Is(err, datastore.ErrSizeHasTimedOut) || (errors.As(err, &pgErr) && pgErr.Code == pgerrcode.QueryCanceled) {
					size, err = store.EstimatedSizeWithDescendants(ctx, repo)
					precision = sizePrecisionUntagged
				}
			}
		}
		l.WithError(err).WithFields(log.Fields{
			"size_bytes":   size,
			"size_type":    sizeVal,
			"duration_ms":  time.Since(t).Milliseconds(),
			"is_top_level": repo.IsTopLevel(),
			"root_repo":    repo.TopLevelPathSegment(),
			"precision":    precision,
		}).Info("repository size measurement")
		if err != nil {
			h.Errors = append(h.Errors, errcode.FromUnknownError(err))
			return
		}
		resp.Size = &size
		resp.SizePrecision = precision
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)

	if err := enc.Encode(resp); err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
}

type repositoryTagsHandler struct {
	*Context
}

func repositoryTagsDispatcher(ctx *Context, _ *http.Request) http.Handler {
	repositoryTagsHandler := &repositoryTagsHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		http.MethodGet: http.HandlerFunc(repositoryTagsHandler.GetTags),
	}
}

// RepositoryTagResponse is the API counterpart for models.TagDetail. This allows us to abstract the datastore-specific
// implementation details (such as sql.NullTime) without having to implement custom JSON serializers (and having to use
// our own implementations) for these types. This is therefore a precise representation of the API response structure.
type RepositoryTagResponse struct {
	Name      string `json:"name"`
	Digest    string `json:"digest"`
	MediaType string `json:"media_type"`
	Size      int64  `json:"size_bytes"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

// GetTags retrieves a list of tag details for a given repository. This includes support for marker-based pagination
// using limit (`n`) and last (`last`) query parameters, as in the Docker/OCI Distribution tags list API. `n` is capped
// to 100 entries by default.
func (h *repositoryTagsHandler) GetTags(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	maxEntries := maximumReturnedEntries
	if q.Has(nQueryParamKey) {
		val, valid := isQueryParamTypeInt(q.Get(nQueryParamKey))
		if !valid {
			detail := v1.InvalidQueryParamTypeErrorDetail(nQueryParamKey, nQueryParamValidTypes)
			h.Errors = append(h.Errors, v1.ErrorCodeInvalidQueryParamType.WithDetail(detail))
			return
		}
		if !isQueryParamIntValueInBetween(val, nQueryParamValueMin, nQueryParamValueMax) {
			detail := v1.InvalidQueryParamValueRangeErrorDetail(nQueryParamKey, nQueryParamValueMin, nQueryParamValueMax)
			h.Errors = append(h.Errors, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail))
			return
		}
		maxEntries = val
	}

	// `lastEntry` must conform to the tag name regexp
	var lastEntry string
	if q.Has(lastQueryParamKey) {
		lastEntry = q.Get(lastQueryParamKey)
		if !queryParamValueMatchesPattern(lastEntry, lastTagQueryParamPattern) {
			detail := v1.InvalidQueryParamValuePatternErrorDetail(lastQueryParamKey, lastTagQueryParamPattern)
			h.Errors = append(h.Errors, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail))
			return
		}
	}

	path := h.Repository.Named().Name()
	rStore := datastore.NewRepositoryStore(h.db)
	repo, err := rStore.FindByPath(h.Context, path)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
	if repo == nil {
		h.Errors = append(h.Errors, v2.ErrorCodeNameUnknown.WithDetail(map[string]string{"name": path}))
		return
	}

	tagsList, err := rStore.TagsDetailPaginated(h.Context, repo, maxEntries, lastEntry)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	// Add a link header if there are more entries to retrieve
	if len(tagsList) > 0 {
		n, err := rStore.TagsCountAfterName(h.Context, repo, tagsList[len(tagsList)-1].Name)
		if err != nil {
			h.Errors = append(h.Errors, errcode.FromUnknownError(err))
			return
		}
		if n > 0 {
			lastEntry = tagsList[len(tagsList)-1].Name
			urlStr, err := createLinkEntry(r.URL.String(), maxEntries, lastEntry)
			if err != nil {
				h.Errors = append(h.Errors, errcode.FromUnknownError(err))
				return
			}
			w.Header().Set("Link", urlStr)
		}
	}

	w.Header().Set("Content-Type", "application/json")

	resp := make([]RepositoryTagResponse, 0, len(tagsList))
	for _, t := range tagsList {
		d := RepositoryTagResponse{
			Name:      t.Name,
			Digest:    t.Digest.String(),
			MediaType: t.MediaType,
			Size:      t.Size,
			CreatedAt: timeToString(t.CreatedAt),
		}
		if t.UpdatedAt.Valid {
			d.UpdatedAt = timeToString(t.UpdatedAt.Time)
		}
		resp = append(resp, d)
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
}

type subRepositoriesHandler struct {
	*Context
}

func subRepositoriesDispatcher(ctx *Context, _ *http.Request) http.Handler {
	subRepositoriesHandler := &subRepositoriesHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		http.MethodGet: http.HandlerFunc(subRepositoriesHandler.GetSubRepositories),
	}
}

// GetSubRepositories retrieves a list of repositories for a given repository base path. This includes support for marker-based pagination
// using limit (`n`) and last (`last`) query parameters, as in the Docker/OCI Distribution catalog list API. `n` can not exceed 1000.
// if no `n` query parameter is specified the default of `100` is used.
func (h *subRepositoriesHandler) GetSubRepositories(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	maxEntries := maximumReturnedEntries
	if q.Has(nQueryParamKey) {
		val, valid := isQueryParamTypeInt(q.Get(nQueryParamKey))
		if !valid {
			detail := v1.InvalidQueryParamTypeErrorDetail(nQueryParamKey, nQueryParamValidTypes)
			h.Errors = append(h.Errors, v1.ErrorCodeInvalidQueryParamType.WithDetail(detail))
			return
		}
		if !isQueryParamIntValueInBetween(val, nQueryParamValueMin, nQueryParamValueMax) {
			detail := v1.InvalidQueryParamValueRangeErrorDetail(nQueryParamKey, nQueryParamValueMin, nQueryParamValueMax)
			h.Errors = append(h.Errors, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail))
			return
		}
		maxEntries = val
	}

	// `lastEntry` must conform to the repository name regexp
	var lastEntry string
	if q.Has(lastQueryParamKey) {
		lastEntry = q.Get(lastQueryParamKey)
		if !queryParamValueMatchesPattern(lastEntry, lastPathQueryParamPattern) {
			detail := v1.InvalidQueryParamValuePatternErrorDetail(lastQueryParamKey, lastPathQueryParamPattern)
			h.Errors = append(h.Errors, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail))
			return
		}
	}

	path := h.Repository.Named().Name()
	rStore := datastore.NewRepositoryStore(h.db)
	repoList, err := rStore.FindPagingatedRepositoriesForPath(h.Context, path, lastEntry, maxEntries)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	// Add a link header if there might be more entries to retrieve
	if len(repoList) == maxEntries {
		lastEntry = repoList[len(repoList)-1].Path
		urlStr, err := createLinkEntry(r.URL.String(), maxEntries, lastEntry)
		if err != nil {
			h.Errors = append(h.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			return
		}
		w.Header().Set("Link", urlStr)
	}

	w.Header().Set("Content-Type", "application/json")

	resp := make([]RepositoryAPIResponse, 0, len(repoList))
	for _, r := range repoList {
		d := RepositoryAPIResponse{
			Name:      r.Name,
			Path:      r.Path,
			Size:      r.Size,
			CreatedAt: timeToString(r.CreatedAt),
		}
		if r.UpdatedAt.Valid {
			d.UpdatedAt = timeToString(r.UpdatedAt.Time)
		}
		resp = append(resp, d)
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(resp); err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
}
