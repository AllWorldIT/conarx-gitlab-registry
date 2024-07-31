package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/docker/distribution/internal/feature"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	v1 "github.com/docker/distribution/registry/api/gitlab/v1"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/auth"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	gocache "github.com/eko/gocache/lib/v4/cache"

	"github.com/gorilla/handlers"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"gitlab.com/gitlab-org/labkit/errortracking"
)

type repositoryHandler struct {
	*Context
}

func repositoryDispatcher(ctx *Context, _ *http.Request) http.Handler {
	repositoryHandler := &repositoryHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		http.MethodGet:   http.HandlerFunc(repositoryHandler.GetRepository),
		http.MethodPatch: http.HandlerFunc(repositoryHandler.RenameRepository),
	}
}

type RepositoryAPIResponse struct {
	Name            string `json:"name"`
	Path            string `json:"path"`
	Size            *int64 `json:"size_bytes,omitempty"`
	SizePrecision   string `json:"size_precision,omitempty"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at,omitempty"`
	LastPublishedAt string `json:"last_published_at,omitempty"`
}

type RenameRepositoryAPIResponse struct {
	TTL time.Time `json:"ttl"`
}

type RenameRepositoryAPIRequest struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type validatedRenameRequest struct {
	isDryRun                 bool
	isRenameNamespaceRequest bool
	renameObject             *RenameRepositoryAPIRequest
}

const (
	sizeQueryParamKey                      = "size"
	sizeQueryParamSelfValue                = "self"
	sizeQueryParamSelfWithDescendantsValue = "self_with_descendants"
	nQueryParamKey                         = "n"
	nQueryParamValueMin                    = 1
	nQueryParamValueMax                    = 1000
	beforeQueryParamKey                    = "before"
	lastQueryParamKey                      = "last"
	dryRunParamKey                         = "dry_run"
	tagNameQueryParamKey                   = "name"
	tagExactNameQueryParamKey              = "name_exact"
	sortQueryParamKey                      = "sort"
	publishedAtQueryParamKey               = "published_at"
	sortOrderDescPrefix                    = "-"
	referrersQueryParamKey                 = "referrers"
	referrerTypeQueryParamKey              = "referrer_type"
	defaultDryRunRenameOperationTimeout    = 5 * time.Second
	maxRepositoriesToRename                = 1000
)

var (
	nQueryParamValidTypes = []reflect.Kind{reflect.Int}

	sizeQueryParamValidValues = []string{
		sizeQueryParamSelfValue,
		sizeQueryParamSelfWithDescendantsValue,
	}

	// sortQueryParamValidValues is a list of accepted values to sort by.
	// Using  `-` means values in descending order
	sortQueryParamValidValues = []string{
		fmt.Sprintf("-%s", tagNameQueryParamKey),
		tagNameQueryParamKey,
		fmt.Sprintf("-%s", publishedAtQueryParamKey),
		publishedAtQueryParamKey,
	}

	tagQueryParamPattern = reference.TagRegexp

	// tagNameQueryParamPattern is a modified version of the OCI Distribution tag name regexp pattern (described in
	// https://github.com/opencontainers/distribution-spec/blob/main/spec.md#pulling-manifests) to allow the tag name
	// filter string to start with `.` and `-` characters (not just `_`). This is required to support a partial match.
	tagNameQueryParamPattern = regexp.MustCompile("^[a-zA-Z0-9._-]{1,128}$")

	timestampRegexParam = regexp.MustCompile(`^\d{1,}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$`)

	// writeMethods are a map of http methods that modify registry data
	writeMethods = map[string]struct{}{
		http.MethodPost:   {},
		http.MethodDelete: {},
		http.MethodPut:    {},
		http.MethodPatch:  {},
	}
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

func queryParamDryRunValue(value string) (bool, error) {
	switch value {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("unknown value: %s", value)
	}
}

func sizeQueryParamValue(r *http.Request) string {
	return r.URL.Query().Get(sizeQueryParamKey)
}

// timeToString converts a time.Time to a ISO 8601 with millisecond precision string. This is the standard format used
// across GitLab applications.
func timeToString(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

// timeToStringMicroPrecision converts a time.Time to a ISO 8601 with microsecond precision string.
func timeToStringMicroPrecision(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000Z07:00")
}

// replacePathName removes the last part (i.e the name) of `originPath` and replaces it with `newName`
func replacePathName(originPath, newName string) string {
	dir := path.Dir(originPath)
	return path.Join(dir, newName)
}

// extractDryRunQueryParamValue extracts a valid `dry_run` query parameter value from `url`.
// when no `dry_run` key is found it returns flase by default, when a key is found the function
// returns the value of the key or returns an error if the vaues are neither "true" or "false".
func extractDryRunQueryParamValue(url url.Values) (dryRun bool, err error) {
	if url.Has(dryRunParamKey) {
		dryRun, err = queryParamDryRunValue(url.Get(dryRunParamKey))
	}
	return dryRun, err
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
	// TODO: remove as part of https://gitlab.com/gitlab-org/container-registry/-/issues/1056
	if h.App.redisCache != nil {
		opts = append(opts, datastore.WithRepositoryCache(datastore.NewCentralRepositoryCache(h.App.redisCache)))
	}
	store := datastore.NewRepositoryStore(h.db.Primary(), opts...)

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
		ns := datastore.NewNamespaceStore(h.db.Primary())
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
	if repo.LastPublishedAt.Valid {
		resp.LastPublishedAt = timeToString(repo.LastPublishedAt.Time)
	}

	if withSize {
		var size datastore.RepositorySize
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
			"size_bytes":   size.Bytes(),
			"size_cached":  size.Cached(),
			"size_type":    sizeVal,
			"duration_ms":  time.Since(t).Milliseconds(),
			"is_top_level": repo.IsTopLevel(),
			"root_repo":    repo.TopLevelPathSegment(),
			"precision":    precision,
			"failure":      err != nil,
		}).Info("repository size measurement")
		if err != nil {
			h.Errors = append(h.Errors, errcode.FromUnknownError(err))
			return
		}

		b := size.Bytes()
		resp.Size = &b
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
	Name         string                          `json:"name"`
	Digest       string                          `json:"digest"`
	ConfigDigest string                          `json:"config_digest,omitempty"`
	MediaType    string                          `json:"media_type"`
	Size         int64                           `json:"size_bytes"`
	CreatedAt    string                          `json:"created_at"`
	UpdatedAt    string                          `json:"updated_at,omitempty"`
	PublishedAt  string                          `json:"published_at,omitempty"`
	Referrers    []RepositoryTagReferrerResponse `json:"referrers,omitempty"`
}

type RepositoryTagReferrerResponse struct {
	ArtifactType string `json:"artifactType"`
	Digest       string `json:"digest"`
}

func tagNameQueryParamValue(r *http.Request) string {
	return r.URL.Query().Get(tagNameQueryParamKey)
}

func tagExactNameQueryParamValue(r *http.Request) string {
	return r.URL.Query().Get(tagExactNameQueryParamKey)
}

func sortQueryParamValue(q url.Values) string {
	return strings.ToLower(strings.TrimSpace(q.Get(sortQueryParamKey)))
}

func referrerTypeQueryParamValue(q url.Values) []string {
	if len(q[referrerTypeQueryParamKey]) == 0 {
		return nil
	}

	types := make([]string, len(q[referrerTypeQueryParamKey]))
	for _, t := range q[referrerTypeQueryParamKey] {
		types = append(types, strings.Split(strings.ToLower(strings.TrimSpace(t)), ",")...)
	}

	return types
}

func filterParamsFromRequest(r *http.Request) (datastore.FilterParams, error) {
	var filters datastore.FilterParams

	q := r.URL.Query()
	maxEntries := defaultMaximumReturnedEntries
	if q.Has(nQueryParamKey) {
		val, valid := isQueryParamTypeInt(q.Get(nQueryParamKey))
		if !valid {
			detail := v1.InvalidQueryParamTypeErrorDetail(nQueryParamKey, nQueryParamValidTypes)
			return filters, v1.ErrorCodeInvalidQueryParamType.WithDetail(detail)
		}
		if !isQueryParamIntValueInBetween(val, nQueryParamValueMin, nQueryParamValueMax) {
			detail := v1.InvalidQueryParamValueRangeErrorDetail(nQueryParamKey, nQueryParamValueMin, nQueryParamValueMax)
			return filters, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail)
		}

		maxEntries = val
	}
	filters.MaxEntries = maxEntries

	// `last` and `before` are mutually exclusive
	if q.Has(lastQueryParamKey) && q.Has(beforeQueryParamKey) {
		detail := v1.MutuallyExclusiveParametersErrorDetail(lastQueryParamKey, beforeQueryParamKey)
		return filters, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail)
	}

	var beforeEntry string
	if q.Has(beforeQueryParamKey) {
		beforeEntry = q.Get(beforeQueryParamKey)
		// check if entry is base64 encoded, when is not, an error will be returned so we can ignore and continue
		publishedAt, tagName, err := DecodeFilter(beforeEntry)
		// if we assume we decoded the beforeEntry value successfully then the resulting publishedAt parameter
		// must be a date, otherwise the value of beforeEntry should be used in its raw form (i.e as a tag name)
		if err == nil && queryParamValueMatchesPattern(publishedAt, timestampRegexParam) {
			beforeEntry = tagName
			filters.PublishedAt = publishedAt
		}

		if !queryParamValueMatchesPattern(beforeEntry, tagQueryParamPattern) {
			detail := v1.InvalidQueryParamValuePatternErrorDetail(beforeQueryParamKey, tagQueryParamPattern)
			return filters, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail)
		}
	}
	filters.BeforeEntry = beforeEntry

	// `lastEntry` must conform to the tag name regexp
	var lastEntry string
	if q.Has(lastQueryParamKey) {
		lastEntry = q.Get(lastQueryParamKey)
		// check if entry is base64 encoded, when is not, an error will be returned so we can ignore and continue
		publishedAt, tagName, err := DecodeFilter(lastEntry)
		// if we assume we decoded the lastEntry value successfully then the resulting publishedAt parameter
		// must be a date, otherwise the value of lastEntry should be used in its raw form (i.e as a tag name)
		if err == nil && queryParamValueMatchesPattern(publishedAt, timestampRegexParam) {
			lastEntry = tagName
			filters.PublishedAt = publishedAt
		}

		if !queryParamValueMatchesPattern(lastEntry, tagQueryParamPattern) {
			detail := v1.InvalidQueryParamValuePatternErrorDetail(lastQueryParamKey, tagQueryParamPattern)
			return filters, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail)
		}
	}
	filters.LastEntry = lastEntry

	// `name` and `name_exact` are mutually exclusive
	if q.Has(tagNameQueryParamKey) && q.Has(tagExactNameQueryParamKey) {
		detail := v1.MutuallyExclusiveParametersErrorDetail(tagNameQueryParamKey, tagExactNameQueryParamKey)
		return filters, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail)
	}

	nameFilter := tagNameQueryParamValue(r)
	if nameFilter != "" {
		if !queryParamValueMatchesPattern(nameFilter, tagNameQueryParamPattern) {
			detail := v1.InvalidQueryParamValuePatternErrorDetail(tagNameQueryParamKey, tagNameQueryParamPattern)
			return filters, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail)
		}
	}
	filters.Name = nameFilter

	exactNameFilter := tagExactNameQueryParamValue(r)
	if exactNameFilter != "" {
		if !queryParamValueMatchesPattern(exactNameFilter, tagQueryParamPattern) {
			detail := v1.InvalidQueryParamValuePatternErrorDetail(tagExactNameQueryParamKey, tagQueryParamPattern)
			return filters, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail)
		}
	}
	filters.ExactName = exactNameFilter

	sort := sortQueryParamValue(q)
	if sort != "" {
		if !isQueryParamValueValid(sort, sortQueryParamValidValues) {
			detail := v1.InvalidQueryParamValueErrorDetail(sortQueryParamKey, sortQueryParamValidValues)
			return filters, v1.ErrorCodeInvalidQueryParamValue.WithDetail(detail)
		}

		filters.OrderBy, filters.SortOrder = getSortOrderParams(sort)
	}

	if q.Has(referrersQueryParamKey) && q.Get(referrersQueryParamKey) == "true" {
		filters.IncludeReferrers = true
	}

	filters.ReferrerTypes = referrerTypeQueryParamValue(q)

	return filters, nil
}

func getSortOrderParams(sort string) (string, datastore.SortOrder) {
	orderBy := tagNameQueryParamKey
	sortOrder := datastore.OrderAsc

	values := strings.Split(sort, sortOrderDescPrefix)
	if len(values) == 2 {
		sortOrder = datastore.OrderDesc
		orderBy = values[1]
	} else {
		orderBy = sort
	}

	return orderBy, sortOrder
}

// GetTags retrieves a list of tag details for a given repository. This includes support for marker-based pagination
// using limit (`n`) and last (`last`) query parameters, as in the Docker/OCI Distribution tags list API. `n` is capped
// to 100 entries by default.
func (h *repositoryTagsHandler) GetTags(w http.ResponseWriter, r *http.Request) {
	filters, err := filterParamsFromRequest(r)
	if err != nil {
		h.Errors = append(h.Errors, err)
		return
	}

	var opts []datastore.RepositoryStoreOption
	// TODO: remove as part of https://gitlab.com/gitlab-org/container-registry/-/issues/1056
	if h.App.redisCache != nil {
		opts = append(opts, datastore.WithRepositoryCache(datastore.NewCentralRepositoryCache(h.App.redisCache)))
	}
	rStore := datastore.NewRepositoryStore(h.db.Primary(), opts...)
	path := h.Repository.Named().Name()
	repo, err := rStore.FindByPath(h.Context, path)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
	if repo == nil {
		h.Errors = append(h.Errors, v2.ErrorCodeNameUnknown.WithDetail(map[string]string{"name": path}))
		return
	}

	tagsList, err := rStore.TagsDetailPaginated(h.Context, repo, filters)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	// Add a link header if there are more entries to retrieve
	// NOTE(prozlach): with exact-match filter we get only one or no entries,
	// so pagination is not needed.
	if len(tagsList) > 0 && filters.ExactName == "" {
		filters.LastEntry = tagsList[len(tagsList)-1].Name
		filters.PublishedAt = timeToStringMicroPrecision(tagsList[len(tagsList)-1].PublishedAt)
		publishedLast := filters.PublishedAt
		hasTagsAfter, err := rStore.HasTagsAfterName(h.Context, repo, filters)
		if err != nil {
			h.Errors = append(h.Errors, errcode.FromUnknownError(err))
			return
		}

		if !hasTagsAfter {
			filters.LastEntry = ""
			publishedLast = ""
		}

		filters.BeforeEntry = tagsList[0].Name
		filters.PublishedAt = timeToStringMicroPrecision(tagsList[0].PublishedAt)
		publishedBefore := filters.PublishedAt
		hasTagsBefore, err := rStore.HasTagsBeforeName(h.Context, repo, filters)
		if err != nil {
			h.Errors = append(h.Errors, errcode.FromUnknownError(err))
			return
		}

		if !hasTagsBefore {
			filters.BeforeEntry = ""
			publishedBefore = ""
		}

		urlStr, err := createLinkEntry(r.URL.String(), filters, publishedBefore, publishedLast)
		if err != nil {
			h.Errors = append(h.Errors, errcode.FromUnknownError(err))
			return
		}
		if urlStr != "" {
			w.Header().Set("Link", urlStr)
		}
	}

	w.Header().Set("Content-Type", "application/json")

	resp := make([]RepositoryTagResponse, 0, len(tagsList))
	for _, t := range tagsList {
		d := RepositoryTagResponse{
			Name:        t.Name,
			Digest:      t.Digest.String(),
			MediaType:   t.MediaType,
			Size:        t.Size,
			CreatedAt:   timeToString(t.CreatedAt),
			PublishedAt: timeToString(t.PublishedAt),
		}
		if t.ConfigDigest.Valid {
			d.ConfigDigest = t.ConfigDigest.Digest.String()
		}
		if t.UpdatedAt.Valid {
			d.UpdatedAt = timeToString(t.UpdatedAt.Time)
		}
		if t.Referrers != nil {
			d.Referrers = make([]RepositoryTagReferrerResponse, 0, len(t.Referrers))
			for _, td := range t.Referrers {
				d.Referrers = append(d.Referrers, RepositoryTagReferrerResponse{
					Digest:       td.Digest,
					ArtifactType: td.ArtifactType,
				})
			}
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
	filters, err := filterParamsFromRequest(r)
	if err != nil {
		h.Errors = append(h.Errors, err)
		return
	}

	// extract the repository name to create the a preliminary repository
	path := h.Repository.Named().Name()
	repo := &models.Repository{Path: path}

	// try to find the namespaceid for the repo if it exists
	topLevelPathSegment := repo.TopLevelPathSegment()
	nStore := datastore.NewNamespaceStore(h.db.Primary())
	namespace, err := nStore.FindByName(h.Context, topLevelPathSegment)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
	if namespace == nil {
		h.Errors = append(h.Errors, v2.ErrorCodeNameUnknown.WithDetail(map[string]string{"namespace": topLevelPathSegment}))
		return
	}

	// path and namespace ID are the two required parameters for the queries in repositoryStore.FindPaginatedRepositoriesForPath,
	// so we must fill those. We also fill the name for consistency on the response.
	repo.NamespaceID = namespace.ID
	repo.Name = repo.Path[strings.LastIndex(repo.Path, "/")+1:]

	rStore := datastore.NewRepositoryStore(h.db.Primary())
	repoList, err := rStore.FindPaginatedRepositoriesForPath(h.Context, repo, filters)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	// Add a link header if there might be more entries to retrieve
	if len(repoList) == filters.MaxEntries {
		filters.LastEntry = repoList[len(repoList)-1].Path
		urlStr, err := createLinkEntry(r.URL.String(), filters, "", "")
		if err != nil {
			h.Errors = append(h.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			return
		}

		if urlStr != "" {
			w.Header().Set("Link", urlStr)
		}
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

// RenameRepository renames a given base repository (name and path - if exist) and updates the paths of all sub-repositories originating
// from the refrenced base repository path. If the query param: `dry_run` is set to true, then this operation
// only attempts to verify that a rename is possible for a provided repository and name.
// When no `dry_run` option is provided, this function defaults to `dry_run=false`.
func (h *repositoryHandler) RenameRepository(w http.ResponseWriter, r *http.Request) {
	l := log.GetLogger(log.WithContext(h)).WithFields(log.Fields{"path": h.Repository.Named().Name()})

	// this endpoint is only available on a registry that utilizes the redis cache,
	// we make sure we fail with a 404 and detailing a missing dependecy error if no redis cache is found
	if h.App.redisCache == nil {
		detail := v1.MissingServerDependencyTypeErrorDetail("redis")
		h.Errors = append(h.Errors, v1.ErrorCodeNotImplemented.WithDetail(detail))
		return
	}

	// verify that the parameters of the rename request are valid
	validatedRenameRequest, err := validateRenameRequest(h, r, h.Repository.Named().Name())
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	rStore := datastore.NewRepositoryStore(h.db.Primary())
	nStore := datastore.NewNamespaceStore(h.db.Primary())

	// find the origin repository for the path to be renamed (if it exists), if the origin path does not exist
	// we still need to check and rename the sub-repositories of the provided path (if they exist)
	repo, renameOriginRepo, err := inferRepository(h.Context, h.Repository.Named().Name(), rStore, nStore)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	// count the number of repositories under the origin path
	repoCount, err := rStore.CountPathSubRepositories(h.Context, repo.NamespaceID, repo.Path)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	err = assertRenameRepositoryCount(h, repo, repoCount)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	// extract the new path and/or new name of the origin repository
	var (
		newPath                  string
		newName                  = validatedRenameRequest.renameObject.Name
		isRenameNamespaceRequest = validatedRenameRequest.isRenameNamespaceRequest
	)
	if isRenameNamespaceRequest {
		// bottomLevelPathSegment is the base repository name
		pathSegments := strings.Split(h.Repository.Named().Name(), "/")
		bottomLevelPathSegment := pathSegments[len(pathSegments)-1]
		newPath = validatedRenameRequest.renameObject.Namespace + "/" + bottomLevelPathSegment
		newName = bottomLevelPathSegment
	} else {
		newPath = replacePathName(repo.Path, newName)
	}

	// check that no base repository or sub repository exists for the new target path
	nameTaken, err := isRepositoryNameTaken(h.Context, rStore, repo.NamespaceID, newName, newPath)
	if nameTaken {
		l.WithError(err).WithFields(log.Fields{
			"rename_path": newPath,
		}).Info("repository rename conflicts with existing repository")
	}
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	// at this point everything checks out with the request and there are no conflicts
	// with existing repositories/sub-repositories within the registry. we proceed
	// with procuring a lease for the rename operation and/or executing the rename in the datastore.

	rsp := renameStoreParams{
		source:           repo,
		newPath:          newPath,
		newName:          newName,
		isDryRun:         validatedRenameRequest.isDryRun,
		isPathOriginRepo: renameOriginRepo,
	}

	err = handleRenameStoreOperation(h.Context, w, rsp, h.App.redisCache, h.db.Primary())
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
}

// enforceRenameLease makes sure a conflicting rename lease does not already exist for `forPath` that is not granted to `grantedToPath`
// if a rename lease exist for the `forPath` that is not granted to `grantedToPath` it returns an errcode.Error.
// if a rename lease exist for the `forPath` with the same `grantedToPath` it refreshes the TTL the lease.
// if no rename lease exist for the `forPath` whatsoever it allocates a new lease for `forPath` to `grantedToPath`.
func enforceRenameLease(ctx context.Context, rlstore datastore.RepositoryLeaseStore, forPath, grantedToPath string) (*models.RepositoryLease, error) {
	// Check if an existing lease exist for the rename path
	rlease, err := rlstore.FindRenameByPath(ctx, forPath)
	if err != nil {
		return nil, err
	}

	// if no leases exist then create one immediately
	if rlease == nil {
		rlease, err = rlstore.UpsertRename(ctx, &models.RepositoryLease{
			GrantedTo: grantedToPath,
			Path:      forPath,
		})
		if err != nil {
			return nil, err
		}
	} else {
		// verify the current repository path owns the lease or if it is owned by a different repository path
		if grantedToPath != rlease.GrantedTo {
			detail := v1.ConflictWithOngoingRename(forPath)
			return nil, v1.ErrorCodeRenameConflict.WithDetail(detail)
		}

		// if the current user owns the lease, refresh it so it lives longer
		rlease, err = rlstore.UpsertRename(ctx, &models.RepositoryLease{
			GrantedTo: grantedToPath,
			Path:      forPath,
		})
		if err != nil {
			return nil, err
		}
	}
	return rlease, nil
}

// extractRenameRequestParams retrieves the necessary parameters for a rename operation from a request
func extractRenameRequestParams(r *http.Request) (bool, *RenameRepositoryAPIRequest, error) {
	// extract the requests `dry_run` param and validate it
	dryRun, err := extractDryRunQueryParamValue(r.URL.Query())
	if err != nil {
		detail := v1.InvalidQueryParamValueErrorDetail(dryRunParamKey, []string{"true", "false"})
		return dryRun, nil, v1.ErrorCodeInvalidQueryParamType.WithDetail(detail)
	}

	// parse request body
	var renameObject RenameRepositoryAPIRequest
	err = json.NewDecoder(r.Body).Decode(&renameObject)
	if err != nil {
		return dryRun, nil, v1.ErrorCodeInvalidJSONBody.WithDetail("invalid json")
	}
	return dryRun, &renameObject, nil
}

// getDynamicRenameOperationTTL selects the greater of `defaultDryRunRenameOperationTimeout` and a lease's TTL
func getDynamicRenameOperationTTL(ctx context.Context, rlstore datastore.RepositoryLeaseStore, lease *models.RepositoryLease) (time.Duration, error) {
	repositoryRenameOperationTTL, err := rlstore.GetTTL(ctx, lease)
	if err != nil {
		return 0, err
	}

	// the rename query should only take at most the minimum time dedicated to database queries
	if repositoryRenameOperationTTL > defaultDryRunRenameOperationTimeout {
		repositoryRenameOperationTTL = defaultDryRunRenameOperationTimeout
	}
	return repositoryRenameOperationTTL, nil
}

// executeRenameOperation executes a rename operation to `newPath` and `newName` on the provided `repo` using a share transaction `tx`
// and a shared context `ctx`
func executeRenameOperation(ctx context.Context, tx datastore.Transactor, repo *models.Repository, renameOriginRepo bool, newPath, newName string) error {
	rStoreTx := datastore.NewRepositoryStore(tx)
	oldpath := repo.Path
	if renameOriginRepo {
		if err := rStoreTx.Rename(ctx, repo, newPath, newName); err != nil {
			return err
		}
	}
	return rStoreTx.RenamePathForSubRepositories(ctx, repo.NamespaceID, oldpath, newPath)
}

// inferRepository infers a repository object (using the `path` argument) from either the repository store or the namesapce store
func inferRepository(context context.Context, path string, rStore datastore.RepositoryStore, nStore datastore.NamespaceStore) (*models.Repository, bool, error) {
	// find the base repository for the path to be renamed (if it exists)
	// if the base path does not exist we still need to update the subrepositories
	// of the path (if they exist)
	var renameOriginRepo bool
	repo, err := rStore.FindByPath(context, path)
	if err != nil {
		return nil, renameOriginRepo, err
	}

	if repo != nil {
		renameOriginRepo = true
	}

	// if a base repository was not found we infer a repository using the paths namespace
	if repo == nil {
		// build a preliminary repository object
		repo = &models.Repository{Path: path}
		topLevelPathSegment := repo.TopLevelPathSegment()

		// find the repository namespace and update the preliminary repository object
		namespace, err := nStore.FindByName(context, topLevelPathSegment)
		if err != nil {
			return nil, renameOriginRepo, err
		}
		if namespace == nil {
			return nil, renameOriginRepo, v2.ErrorCodeNameUnknown.WithDetail(map[string]string{"namespace": topLevelPathSegment})
		}
		repo.NamespaceID = namespace.ID
	}
	return repo, renameOriginRepo, nil
}

// isRepositoryNameTaken checks if the `name` and `path` provided in the arguments are used by
// any base repositories or sub-repositories within a given namespace with `namespaceId`
func isRepositoryNameTaken(ctx context.Context, rStore datastore.RepositoryStore, namespaceId int64, newName, newPath string) (bool, error) {
	newRepo, err := rStore.FindByPath(ctx, newPath)
	if err != nil {
		return false, err
	}

	// fail if new base path already exist in the registry
	if newRepo != nil {
		detail := v1.ConflictWithExistingRepository(newName)
		return true, v1.ErrorCodeRenameConflict.WithDetail(detail)
	}

	// if a base path does not contain a repository, we still need to check
	// that no sub-repositories potentially exist within the nested path
	if newRepo == nil {
		// check that no sub-repositories exist for the path
		subrepositories, err := rStore.CountPathSubRepositories(ctx, namespaceId, newPath)
		if err != nil {
			return false, err
		}
		if subrepositories > 0 {
			detail := v1.ConflictWithExistingRepository(newName)
			return true, v1.ErrorCodeRenameConflict.WithDetail(detail)
		}
	}
	return false, nil
}

// findProjectPath extracts the project path from the auth token resource
// for a repository that matches the resource repository name
func findProjectPath(repoName string, resources []auth.Resource) (string, error) {
	for _, r := range resources {
		// once the repo name in the request url matches the one in the token, then extract the project path
		if r.Name == repoName {
			if r.ProjectPath != "" {
				return r.ProjectPath, nil
			}
			return r.ProjectPath, v1.ErrorCodeUnknownProjectPath.WithDetail("project path not found")
		}
	}
	return "", v1.ErrorCodeUnknownProjectPath.WithDetail("requested repository does not match authorized repository in token")
}

// checkOngoingRename is a wrappper around http request handlers. It checks if write or delete requests can be made to a repository at the time
// and prevents the request from proceeding to the wrapped handler if so.
// It determines blocked vs allowed request based on if the repository in question is "undergoing a rename operation".
func checkOngoingRename(handler http.Handler, h *Context) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		repo := h.Repository.Named().Name()
		// check if this is a http method that does writes
		_, isRegistryWrite := writeMethods[r.Method]
		checkOngoingRename := isRegistryWrite && feature.OngoingRenameCheck.Enabled()

		if !checkOngoingRename || !h.useDatabase || h.App.redisCache == nil {
			handler.ServeHTTP(w, r)
			return
		}

		l := log.GetLogger(log.WithContext(h)).WithFields(log.Fields{"repository": repo})
		l.Debug("ongoing rename check: starting check for potential ongoing renames on the requested repository")
		// extract the repository projectPath from the Auth token resource
		projectPath, err := findProjectPath(repo, auth.AuthorizedResources(h))
		// prevent the request from proceeding if we cannot find the project path for the referenced repository
		if err != nil {
			err = errors.New("ongoing rename check: failed to find project path parameter in token")
			errortracking.Capture(err, errortracking.WithContext(h), errortracking.WithRequest(r), errortracking.WithStackTrace())
			h.Errors = append(h.Errors, errcode.FromUnknownError(err))
			return
		}

		if strings.HasPrefix(repo, projectPath+"/") || (repo == projectPath) {
			plStore, err := datastore.NewProjectLeaseStore(datastore.NewCentralProjectLeaseCache(h.App.redisCache))
			// prevent the request from proceeding if we cannot gain access to the lease store
			if err != nil {
				l.WithError(err).Error("ongoing rename check: failed to instantiate project lease store")
				h.Errors = append(h.Errors, errcode.FromUnknownError(err))
				return
			}

			// if any errors occur when accessing the underlying redis store we will not block write requests as this could seriously degrade the registry
			// until the redis store is fixed. Instead, we will skip checking renames, notify of the issue via errors and proceed to handle the request as usual.
			// See: https://gitlab.com/gitlab-org/container-registry/-/merge_requests/1333#note_1482777410 on the rationale behind this.
			exist, err := plStore.Exists(h.Context, projectPath)
			if err != nil {
				errortracking.Capture(err, errortracking.WithContext(h), errortracking.WithRequest(r), errortracking.WithStackTrace())
				l.WithError(err).Error("ongoing rename check: failed to check lease store for ongoing lease, skipping")
				handler.ServeHTTP(w, r)
				return
			}

			// prevent the request from proceeding if an ongoing rename operation is underway in the project space
			if exist {
				err = errors.New("ongoing rename check: the current repository is undergoing a rename")
				errortracking.Capture(err, errortracking.WithContext(h), errortracking.WithRequest(r), errortracking.WithStackTrace())
				h.Errors = append(h.Errors, v2.ErrorCodeRenameInProgress.WithDetail(fmt.Sprintf("the base repository path: %s, is undergoing a rename", projectPath)))
				return
			}
		}
		l.Debug("ongoing rename check: no ongoing renames were detected")

		handler.ServeHTTP(w, r)
	})
}

// validateRenameRequestAttributes verifies the attributes of the rename request are correct and also decides if the request
// is for renaming the repository name of an origin repository or moving the namespace of an origin repository.
func validateRenameRequestAttributes(renameObject *RenameRepositoryAPIRequest) (bool, error) {
	var (
		newName          = renameObject.Name
		newNamespacePath = renameObject.Namespace

		// isRenameNameRequest signifies the current request is to rename a repository's origin name to a new name
		isRenameNameRequest = newName != ""
		// isRenameNamespaceRequest signifies the current request is to move an origin repository to a new namespace
		isRenameNamespaceRequest = newNamespacePath != ""
	)

	// We do not support moving an origin repository to a new namespace and renaming the origin repository at the same time
	if isRenameNameRequest && isRenameNamespaceRequest {
		detail := v1.OnlyOneOfParamsErrorDetail("name", "namespace")
		return isRenameNamespaceRequest, v1.ErrorCodeInvalidBodyParam.WithDetail(detail)

	}

	if isRenameNamespaceRequest {
		// We only support moving/renaming of sub-namespaces and not top-level-namespaces
		splitNewNamespace := strings.Split(newNamespacePath, "/")
		if !reference.GitLabNamespacePathRegex.MatchString(newNamespacePath) {
			detail := v1.InvalidPatchBodyTypeErrorDetail("namespace", reference.GitLabNamespacePathRegex.String())
			return isRenameNamespaceRequest, v1.ErrorCodeInvalidBodyParamType.WithDetail(detail)

		}

		if len(splitNewNamespace) < 2 {
			return isRenameNamespaceRequest, v1.ErrorCodeInvalidBodyParam.WithDetail("top level namespaces can not be changed")
		}
	} else {
		// Validate the name suggested for the rename operation
		if !reference.GitLabProjectNameRegex.MatchString(newName) || !reference.NameComponentRegexp.MatchString(newName) {
			detail := v1.InvalidPatchBodyTypeErrorDetail("name", reference.GitLabProjectNameRegex.String(), reference.NameComponentRegexp.String())
			return isRenameNamespaceRequest, v1.ErrorCodeInvalidBodyParamType.WithDetail(detail)
		}
	}

	return isRenameNamespaceRequest, nil
}

// validateRenameRequest  validates (and extracts) the rename request for correctness.
func validateRenameRequest(ctx context.Context, r *http.Request, repoName string) (*validatedRenameRequest, error) {
	// extract the repository project path from the auth token resource
	projectPath, err := findProjectPath(repoName, auth.AuthorizedResources(ctx))
	if err != nil {
		return nil, err
	}

	// validate the api path parameter is the same as the token's project path
	if repoName != projectPath {
		return nil, v1.ErrorCodeMismatchProjectPath.WithDetail(fmt.Sprintf("meta.project_path claim does not match requested repository path %s", repoName))
	}

	// extract any necessary request parameters
	dryRun, renameObject, err := extractRenameRequestParams(r)
	if err != nil {
		return nil, err
	}

	isRenameNamespaceRequest, err := validateRenameRequestAttributes(renameObject)
	if err != nil {
		return nil, err
	}

	return &validatedRenameRequest{
		isDryRun:                 dryRun,
		isRenameNamespaceRequest: isRenameNamespaceRequest,
		renameObject:             renameObject,
	}, nil
}

// assertRenameRepositoryCount asserts that the repository count under the repository path is not zero and does not exceed the supported size.
func assertRenameRepositoryCount(ctx context.Context, repo *models.Repository, repoCount int) error {
	// verify the source path does not contain more than 1000 sub repositories.
	// This is a pre-cautious measure for scalability and performance reasons:
	// https://gitlab.com/gitlab-org/gitlab/-/issues/357014
	if repoCount > maxRepositoriesToRename {
		log.GetLogger(log.WithContext(ctx)).WithError(errors.New("failed to rename repository")).WithFields(log.Fields{
			"repository_count": repoCount,
		}).Info("repository exceeds rename limit")
		return v1.ErrorCodeExceedsLimit.WithDetail(v1.ExceedsRenameLimitErrorDetail(maxRepositoriesToRename))
	}

	// verify the source path has at least one sub-repository/repository
	// or return an ErrorCodeNameUnknown.
	if repoCount == 0 {
		return v2.ErrorCodeNameUnknown.WithDetail(map[string]string{"name": repo.Path})
	}

	return nil
}

// renameStoreParameters are attributes needed to execute a rename operation in the datastore.
type renameStoreParams struct {
	newPath          string
	newName          string
	source           *models.Repository
	isDryRun         bool
	isPathOriginRepo bool
}

// handleRenameStoreOperation procures the necessary lease on repositories, executes the rename request in our datastore and writes the appropriate response headers.
func handleRenameStoreOperation(ctx context.Context, w http.ResponseWriter, repo renameStoreParams, cache *gocache.Cache[any], db *datastore.DB) error {
	// create a repository lease store
	var opts []datastore.RepositoryLeaseStoreOption
	opts = append(opts, datastore.WithRepositoryLeaseCache(datastore.NewCentralRepositoryLeaseCache(cache)))
	rlstore := datastore.NewRepositoryLeaseStore(opts...)

	// verify a valid rename lease exists or create one if one can be created
	lease, err := enforceRenameLease(ctx, rlstore, repo.newPath, repo.source.Path)
	if err != nil {
		return errcode.FromUnknownError(err)
	}

	// set a time limit for the rename operation query (both dry-run and real run)
	var repositoryRenameOperationTTL time.Duration
	if repo.isDryRun {
		repositoryRenameOperationTTL = defaultDryRunRenameOperationTimeout
	} else {
		// selects the lower of `defaultDryRunRenameOperationTimeout` and the TTL of the lease
		repositoryRenameOperationTTL, err = getDynamicRenameOperationTTL(ctx, rlstore, lease)
		if err != nil {
			return errcode.FromUnknownError(err)
		}
	}

	if !repo.isDryRun {
		// enact a lease on the source project path which will be used to block all
		// write operations to the existing repositories in the given GitLab project.
		plStore, err := datastore.NewProjectLeaseStore(datastore.NewCentralProjectLeaseCache(cache))
		if err != nil {
			return errcode.FromUnknownError(err)
		}
		// this lease expires in less than  repositoryRenameOperationTTL + 1 second.
		// where repositoryRenameOperationTTL is at most 5 seconds.
		if err := plStore.Set(ctx, repo.source.Path, (repositoryRenameOperationTTL + 1*time.Second)); err != nil {
			return errcode.FromUnknownError(err)
		}
		defer func() {
			if err := plStore.Invalidate(ctx, repo.source.Path); err != nil {
				errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
			}
		}()
	}

	// start a transaction to rename the repository (and sub-repository attributes)
	// and specify a timeout limit to prevent long running repository rename operations
	txCtx, cancel := context.WithTimeout(ctx, repositoryRenameOperationTTL)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errcode.FromUnknownError(fmt.Errorf("failed to create database transaction: %w", err))
	}
	defer tx.Rollback()

	// run the rename operation in a transaction
	if err = executeRenameOperation(txCtx, tx, repo.source, repo.isPathOriginRepo, repo.newPath, repo.newName); err != nil {
		return errcode.FromUnknownError(err)
	}

	// only commit the transaction if the request was not a dry-run
	if !repo.isDryRun {
		if err := tx.Commit(); err != nil {
			return errcode.FromUnknownError(fmt.Errorf("failed to commit database transaction: %w", err))
		}
		w.WriteHeader(http.StatusNoContent)

		// When a lease fails to be destroyed after it is no longer needed it should not impact the response to the caller.
		// The lease will eventually expire regardless, but we still need to record these failed cases.
		if err := rlstore.Destroy(ctx, lease); err != nil {
			errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
		}
	} else {
		w.WriteHeader(http.StatusAccepted)
		w.Header().Set("Content-Type", "application/json")

		repositoryRenameOperationTTL, err = rlstore.GetTTL(ctx, lease)
		if err != nil {
			return errcode.FromUnknownError(err)
		}
		if err := json.NewEncoder(w).Encode(&RenameRepositoryAPIResponse{TTL: time.Now().Add(repositoryRenameOperationTTL).UTC()}); err != nil {
			return errcode.FromUnknownError(err)
		}
	}
	return nil
}
