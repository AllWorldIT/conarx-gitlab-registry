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

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	v1 "github.com/docker/distribution/registry/api/gitlab/v1"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"gitlab.com/gitlab-org/labkit/errortracking"

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
		http.MethodGet:   http.HandlerFunc(repositoryHandler.GetRepository),
		http.MethodPatch: http.HandlerFunc(repositoryHandler.RenameRepository),
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
type RenameRepositoryAPIResponse struct {
	TTL time.Duration `json:"ttl"`
}

type RenameRepositoryAPIRequest struct {
	Name string `json:"name"`
}

const (
	sizeQueryParamKey                      = "size"
	sizeQueryParamSelfValue                = "self"
	sizeQueryParamSelfWithDescendantsValue = "self_with_descendants"
	nQueryParamKey                         = "n"
	nQueryParamValueMin                    = 1
	nQueryParamValueMax                    = 1000
	lastQueryParamKey                      = "last"
	dryRunParamKey                         = "dry_run"
	defaultDryRunRenameOperationTimeout    = 5 * time.Second
	maxRepositoriesToRename                = 1000
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

// replacePathName removes the last part (i.e the name) of `originPath` and replaces it with `newName`
func replacePathName(originPath string, newName string) string {
	dir := path.Dir(originPath)
	return path.Join(dir, newName)
}

// extractDryRunQueryParamValue extracts a valid `dry_run` query parameter value from `url`.
// when no `dry_run` key is found it returns true by default, when a key is found the function
// returns the value of the key or returns an error if the vaues are neither "true" or "false".
func extractDryRunQueryParamValue(url url.Values) (dryRun bool, err error) {
	dryRun = true
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
	Name         string `json:"name"`
	Digest       string `json:"digest"`
	ConfigDigest string `json:"config_digest,omitempty"`
	MediaType    string `json:"media_type"`
	Size         int64  `json:"size_bytes"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at,omitempty"`
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
		if t.ConfigDigest.Valid {
			d.ConfigDigest = t.ConfigDigest.Digest.String()
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

	// extract the repository name to create the a preliminary repository
	path := h.Repository.Named().Name()
	repo := &models.Repository{Path: path}

	// try to find the namespaceid for the repo if it exists
	topLevelPathSegment := repo.TopLevelPathSegment()
	nStore := datastore.NewNamespaceStore(h.db)
	namespace, err := nStore.FindByName(h.Context, topLevelPathSegment)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
	if namespace == nil {
		h.Errors = append(h.Errors, v2.ErrorCodeNameUnknown.WithDetail(map[string]string{"namespace": topLevelPathSegment}))
		return
	}

	// path and namespace ID are the two required parameters for the queries in repositoryStore.FindPagingatedRepositoriesForPath,
	// so we must fill those. We also fill the name for consistency on the response.
	repo.NamespaceID = namespace.ID
	repo.Name = repo.Path[strings.LastIndex(repo.Path, "/")+1:]

	rStore := datastore.NewRepositoryStore(h.db)
	repoList, err := rStore.FindPagingatedRepositoriesForPath(h.Context, repo, lastEntry, maxEntries)
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

// RenameRepository renames a given base repository (name and path - if exist) and updates the paths of all sub-repositories originating
// from the refrenced base repository path. If the query param: `dry_run` is set to true, then this operation
// only attempts to verify that a rename is possible for a provided repository and name.
// When no `dry_run` option is provided, this function defaults to `dry_run=true`.
func (h *repositoryHandler) RenameRepository(w http.ResponseWriter, r *http.Request) {
	l := log.GetLogger(log.WithContext(h)).WithFields(log.Fields{"path": h.Repository.Named().Name()})

	// this endpoint is only available on a registry that utilizes the redis cache,
	// we make sure we fail with a 404 and detailing a missing dependecy error if no redis cache is found
	if h.App.redisCache == nil {
		detail := v1.MissingServerDependencyTypeErrorDetail("redis")
		h.Errors = append(h.Errors, v1.ErrorCodeNotImplemented.WithDetail(detail))
		return
	}

	// extract any necessary request parameters
	dryRun, renameObject, err := extractRenameRequestParams(r)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	// validate the name suggested for the rename operation
	newName := renameObject.Name
	if !reference.GitLabProjectNameRegex.MatchString(newName) {
		detail := v1.InvalidPatchBodyTypeErrorDetail("name", reference.GitLabProjectNameRegex)
		h.Errors = append(h.Errors, v1.ErrorCodeInvalidBodyParamType.WithDetail(detail))
		return
	}

	rStore := datastore.NewRepositoryStore(h.db)
	nStore := datastore.NewNamespaceStore(h.db)

	// find the base repository for the path to be renamed (if it exists), if the base path does not exist
	// we still need to check and rename the sub-repositories of the provided path (if they exist)
	repo, renameBaseRepo, err := inferRepository(h.Context, h.Repository.Named().Name(), rStore, nStore)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	// verify the repository path does not contain more than 1000 sub repositories.
	// this is a pre-cautious limitation for scalability and performance reasons.
	// https://gitlab.com/gitlab-org/gitlab/-/issues/357014s
	repoCount, err := rStore.CountPathSubRepositories(h.Context, repo.NamespaceID, repo.Path)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}
	if repoCount > maxRepositoriesToRename {
		l.WithError(err).WithFields(log.Fields{
			"repository_count": repoCount,
		}).Info("repository exceeds rename limit")
		detail := v1.ExceedsRenameLimitErrorDetail(maxRepositoriesToRename)
		h.Errors = append(h.Errors, v1.ErrorCodeExceedsLimit.WithDetail(detail))
		return
	}

	newPath := replacePathName(repo.Path, newName)

	// check that no base repository or sub repository exists for the new path
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
	// with procuring a lease for the rename operation.

	// create a repository lease store
	var opts []datastore.RepositoryLeaseStoreOption
	opts = append(opts, datastore.WithRepositoryLeaseCache(datastore.NewCentralRepositoryLeaseCache(h.App.redisCache)))
	rlstore := datastore.NewRepositoryLeaseStore(opts...)

	// verify a valid rename lease exists or create one if one can be created
	lease, err := enforceRenameLease(h.Context, rlstore, newPath, repo.Path)
	if err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	// set a time limit for the rename operation query (both dry-run and real run)
	var repositoryRenameOperationTTL time.Duration
	if dryRun {
		repositoryRenameOperationTTL = defaultDryRunRenameOperationTimeout
	} else {
		// selects the lower of `defaultDryRunRenameOperationTimeout` and the TTL of the lease
		repositoryRenameOperationTTL, err = getDynamicRenameOperationTTL(h.Context, rlstore, lease)
		if err != nil {
			h.Errors = append(h.Errors, errcode.FromUnknownError(err))
			return
		}

	}

	// start a transaction to rename the repository (and sub-repository attributes)
	// and specify a timeout limit to prevent long running repository rename operations
	txCtx, cancel := context.WithTimeout(h.Context, repositoryRenameOperationTTL)
	defer cancel()
	tx, err := h.db.BeginTx(h.Context, nil)
	if err != nil {
		h.Errors = append(h.Errors,
			errcode.FromUnknownError(fmt.Errorf("failed to create database transaction: %w", err)))
		return
	}
	defer tx.Rollback()

	// run the rename operation in a transaction
	if err = executeRenameOperation(txCtx, tx, repo, renameBaseRepo, newPath, newName); err != nil {
		h.Errors = append(h.Errors, errcode.FromUnknownError(err))
		return
	}

	// only commit the transaction if the request was not a dry-run
	if !dryRun {
		if err := tx.Commit(); err != nil {
			h.Errors = append(h.Errors,
				errcode.FromUnknownError(fmt.Errorf("failed to commit database transaction: %w", err)))
			return
		}
		w.WriteHeader(http.StatusNoContent)

		// When a lease fails to be destroyed after it is no longer needed it should not impact the response to the caller.
		// The lease will eventually expire regardless, but we still need to record these failed cases.
		if err := rlstore.Destroy(h.Context, lease); err != nil {
			errortracking.Capture(err, errortracking.WithContext(h.Context))
		}
	} else {
		w.Header().Set("Content-Type", "application/json")
		repositoryRenameOperationTTL, err = rlstore.GetTTL(h.Context, lease)
		if err != nil {
			h.Errors = append(h.Errors, errcode.FromUnknownError(err))
			return
		}
		if err := json.NewEncoder(w).Encode(&RenameRepositoryAPIResponse{TTL: repositoryRenameOperationTTL}); err != nil {
			h.Errors = append(h.Errors, errcode.FromUnknownError(err))
			return
		}
	}
}

// enforceRenameLease makes sure a conflicting rename lease does not already exist for `forPath` that is not granted to `grantedToPath`
// if a rename lease exist for the `forPath` that is not granted to `grantedToPath` it returns an errcode.Error.
// if a rename lease exist for the `forPath` with the same `grantedToPath` it refreshes the TTL the lease.
// if no rename lease exist for the `forPath` whatsoever it allocates a new lease for `forPath` to `grantedToPath`.
func enforceRenameLease(ctx *Context, rlstore datastore.RepositoryLeaseStore, forPath, grantedToPath string) (*models.RepositoryLease, error) {
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
func executeRenameOperation(ctx context.Context, tx datastore.Transactor, repo *models.Repository, renameBaseRepo bool, newPath, newName string) error {
	rStoreTx := datastore.NewRepositoryStore(tx)
	oldpath := repo.Path
	if renameBaseRepo {
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
	var renameBaseRepo bool
	repo, err := rStore.FindByPath(context, path)
	if err != nil {
		return nil, renameBaseRepo, err
	}

	if repo != nil {
		renameBaseRepo = true
	}

	// if a base repository was not found we infer a repository using the paths namespace
	if repo == nil {
		// build a preliminary repository object
		repo = &models.Repository{Path: path}
		topLevelPathSegment := repo.TopLevelPathSegment()

		// find the repository namespace and update the preliminary repository object
		namespace, err := nStore.FindByName(context, topLevelPathSegment)
		if err != nil {
			return nil, renameBaseRepo, err
		}
		if namespace == nil {
			return nil, renameBaseRepo, v2.ErrorCodeNameUnknown.WithDetail(map[string]string{"namespace": topLevelPathSegment})
		}
		repo.NamespaceID = namespace.ID
	}
	return repo, renameBaseRepo, nil
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
	// that no sub-repositories potentially exist withing the nested path
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
