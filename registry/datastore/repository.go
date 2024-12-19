//go:generate mockgen -package mocks -destination mocks/repository.go . RepositoryCache

package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/registry/datastore/metrics"
	"github.com/docker/distribution/registry/datastore/models"
	iredis "github.com/docker/distribution/registry/internal/redis"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/redis/go-redis/v9"
	"gitlab.com/gitlab-org/labkit/errortracking"
)

type SortOrder string

const (
	// cacheOpTimeout defines the timeout applied to cache operations against Redis
	cacheOpTimeout = 500 * time.Millisecond

	// OrderDesc is the normalized string to be used for sorting results in descending order
	OrderDesc SortOrder = "desc"
	// OrderAsc is the normalized string to be used for sorting results in ascending order
	OrderAsc SortOrder = "asc"

	orderByName = "name"

	lessThan    = "<"
	greaterThan = ">"

	// sizeWithDescendantsKeyTTL is the TTL for the cached value of the "size with descendants" for a given repository.
	sizeWithDescendantsKeyTTL = 5 * time.Minute

	// lsnKeyTTL is the TTL for the cached primary database Log Sequence Number (LSN) for a given repository.
	lsnKeyTTL = 1 * time.Hour
)

// FilterParams contains the specific filters used to get
// the request results from the repositoryStore.
type FilterParams struct {
	SortOrder        SortOrder
	OrderBy          string
	Name             string
	ExactName        string
	BeforeEntry      string
	LastEntry        string
	PublishedAt      string
	MaxEntries       int
	IncludeReferrers bool
	ReferrerTypes    []string
}

// RepositoryReader is the interface that defines read operations for a repository store.
type RepositoryReader interface {
	FindAll(ctx context.Context) (models.Repositories, error)
	FindAllPaginated(ctx context.Context, filters FilterParams) (models.Repositories, error)
	FindByPath(ctx context.Context, path string) (*models.Repository, error)
	FindDescendantsOf(ctx context.Context, id int64) (models.Repositories, error)
	FindAncestorsOf(ctx context.Context, id int64) (models.Repositories, error)
	FindSiblingsOf(ctx context.Context, id int64) (models.Repositories, error)
	Count(ctx context.Context) (int, error)
	CountAfterPath(ctx context.Context, path string) (int, error)
	CountPathSubRepositories(ctx context.Context, topLevelNamespaceID int64, path string) (int, error)
	Manifests(ctx context.Context, r *models.Repository) (models.Manifests, error)
	Tags(ctx context.Context, r *models.Repository) (models.Tags, error)
	TagsPaginated(ctx context.Context, r *models.Repository, filters FilterParams) (models.Tags, error)
	HasTagsAfterName(ctx context.Context, r *models.Repository, filters FilterParams) (bool, error)
	HasTagsBeforeName(ctx context.Context, r *models.Repository, filters FilterParams) (bool, error)
	ManifestTags(ctx context.Context, r *models.Repository, m *models.Manifest) (models.Tags, error)
	FindManifestByDigest(ctx context.Context, r *models.Repository, d digest.Digest) (*models.Manifest, error)
	FindManifestByTagName(ctx context.Context, r *models.Repository, tagName string) (*models.Manifest, error)
	FindTagByName(ctx context.Context, r *models.Repository, name string) (*models.Tag, error)
	Blobs(ctx context.Context, r *models.Repository) (models.Blobs, error)
	FindBlob(ctx context.Context, r *models.Repository, d digest.Digest) (*models.Blob, error)
	ExistsBlob(ctx context.Context, r *models.Repository, d digest.Digest) (bool, error)
	Size(ctx context.Context, r *models.Repository) (RepositorySize, error)
	SizeWithDescendants(ctx context.Context, r *models.Repository) (RepositorySize, error)
	EstimatedSizeWithDescendants(ctx context.Context, r *models.Repository) (RepositorySize, error)
	TagsDetailPaginated(ctx context.Context, r *models.Repository, filters FilterParams) ([]*models.TagDetail, error)
	FindPaginatedRepositoriesForPath(ctx context.Context, r *models.Repository, filters FilterParams) (models.Repositories, error)
	TagDetail(ctx context.Context, r *models.Repository, tagName string) (*models.TagDetail, error)
}

// RepositoryWriter is the interface that defines write operations for a repository store.
type RepositoryWriter interface {
	Create(ctx context.Context, r *models.Repository) error
	CreateByPath(ctx context.Context, path string, opts ...repositoryOption) (*models.Repository, error)
	CreateOrFind(ctx context.Context, r *models.Repository) error
	CreateOrFindByPath(ctx context.Context, path string, opts ...repositoryOption) (*models.Repository, error)
	Update(ctx context.Context, r *models.Repository) error
	LinkBlob(ctx context.Context, r *models.Repository, d digest.Digest) error
	UnlinkBlob(ctx context.Context, r *models.Repository, d digest.Digest) (bool, error)
	DeleteTagByName(ctx context.Context, r *models.Repository, name string) (bool, error)
	DeleteManifest(ctx context.Context, r *models.Repository, d digest.Digest) (bool, error)
	RenamePathForSubRepositories(ctx context.Context, topLevelNamespaceID int64, oldPath, newPath string) error
	Rename(ctx context.Context, r *models.Repository, newPath, newName string) error
	UpdateLastPublishedAt(ctx context.Context, r *models.Repository, t *models.Tag) error
}

type repositoryOption func(*models.Repository)

// RepositoryStoreOption allows customizing a repositoryStore with additional options.
type RepositoryStoreOption func(*repositoryStore)

// WithRepositoryCache instantiates the repositoryStore with a cache which will
// attempt to retrieve a *models.Repository from methods with return that type,
// rather than communicating with the database.
func WithRepositoryCache(cache RepositoryCache) RepositoryStoreOption {
	return func(rstore *repositoryStore) {
		rstore.cache = cache
	}
}

// RepositoryStore is the interface that a repository store should conform to.
type RepositoryStore interface {
	RepositoryReader
	RepositoryWriter
}

// repositoryStore is the concrete implementation of a RepositoryStore.
type repositoryStore struct {
	// db can be either a *sql.DB or *sql.Tx
	db    Queryer
	cache RepositoryCache
}

// NewRepositoryStore builds a new repositoryStore.
func NewRepositoryStore(db Queryer, opts ...RepositoryStoreOption) RepositoryStore {
	rStore := &repositoryStore{db: db, cache: &noOpRepositoryCache{}}

	for _, o := range opts {
		o(rStore)
	}

	return rStore
}

// RepositoryManifestService implements the validation.ManifestExister
// interface for repository-scoped manifests.
type RepositoryManifestService struct {
	RepositoryReader
	RepositoryPath string
}

// Exists returns true if the manifest is linked in the repository.
func (rms *RepositoryManifestService) Exists(ctx context.Context, dgst digest.Digest) (bool, error) {
	r, err := rms.FindByPath(ctx, rms.RepositoryPath)
	if err != nil {
		return false, err
	}

	if r == nil {
		return false, errors.New("unable to find repository in database")
	}

	m, err := rms.FindManifestByDigest(ctx, r, dgst)
	if err != nil {
		return false, err
	}

	return m != nil, nil
}

// RepositoryBlobService implements the distribution.BlobStatter interface for
// repository-scoped blobs.
type RepositoryBlobService struct {
	RepositoryReader
	RepositoryPath string
}

// Stat returns the descriptor of the blob with the provided digest, returns
// distribution.ErrBlobUnknown if not found.
func (rbs *RepositoryBlobService) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	r, err := rbs.FindByPath(ctx, rbs.RepositoryPath)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	if r == nil {
		return distribution.Descriptor{}, errors.New("unable to find repository in database")
	}

	b, err := rbs.FindBlob(ctx, r, dgst)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	if b == nil {
		return distribution.Descriptor{}, distribution.ErrBlobUnknown
	}

	return distribution.Descriptor{Digest: b.Digest, Size: b.Size, MediaType: b.MediaType}, nil
}

// RepositoryCache is a cache for *models.Repository objects.
type RepositoryCache interface {
	Get(ctx context.Context, path string) *models.Repository
	Set(ctx context.Context, repo *models.Repository)
	InvalidateSize(ctx context.Context, repo *models.Repository)

	SizeWithDescendantsTimedOut(ctx context.Context, r *models.Repository)
	HasSizeWithDescendantsTimedOut(ctx context.Context, r *models.Repository) bool

	// SetSizeWithDescendants sets the computed "size with descendants" of a given repository with a TTL of
	// sizeWithDescendantsKeyTTL.
	SetSizeWithDescendants(ctx context.Context, r *models.Repository, size int64)
	// GetSizeWithDescendants gets the computed "size with descendants" of a given repository. Returns whether the key
	// was found and its value.
	GetSizeWithDescendants(ctx context.Context, r *models.Repository) (bool, int64)

	// SetLSN records the primary database Log Sequence Number (LSN) associated with a given repository.
	// See https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/database-load-balancing.md?ref_type=heads#primary-sticking
	SetLSN(ctx context.Context, r *models.Repository, lsn string) error

	// GetLSN gets the primary database Log Sequence Number (LSN) associated with a given repository.
	// See https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/database-load-balancing.md?ref_type=heads#primary-sticking
	GetLSN(ctx context.Context, r *models.Repository) (string, error)
}

// noOpRepositoryCache satisfies the RepositoryCache, but does not cache anything.
// Useful as a default and for testing.
type noOpRepositoryCache struct{}

// NewNoOpRepositoryCache creates a new non-operational cache for a repository object.
// This implementation does nothing and returns nothing for all its methods.
func NewNoOpRepositoryCache() RepositoryCache {
	return &noOpRepositoryCache{}
}

func (*noOpRepositoryCache) Get(context.Context, string) *models.Repository                  { return nil }
func (*noOpRepositoryCache) Set(context.Context, *models.Repository)                         {}
func (*noOpRepositoryCache) InvalidateSize(context.Context, *models.Repository)              {}
func (*noOpRepositoryCache) SizeWithDescendantsTimedOut(context.Context, *models.Repository) {}
func (*noOpRepositoryCache) HasSizeWithDescendantsTimedOut(context.Context, *models.Repository) bool {
	return false
}
func (*noOpRepositoryCache) SetSizeWithDescendants(context.Context, *models.Repository, int64) {}
func (*noOpRepositoryCache) GetSizeWithDescendants(context.Context, *models.Repository) (bool, int64) {
	return false, 0
}

func (*noOpRepositoryCache) InvalidateRootSizeWithDescendants(context.Context, *models.Repository) {
}

func (*noOpRepositoryCache) SetLSN(context.Context, *models.Repository, string) error { return nil }
func (*noOpRepositoryCache) GetLSN(context.Context, *models.Repository) (string, error) {
	return "", nil
}

// singleRepositoryCache caches a single repository in-memory. This implementation is not thread-safe. Deprecated in
// favor of centralRepositoryCache.
type singleRepositoryCache struct {
	r *models.Repository
}

// NewSingleRepositoryCache creates a new local in-memory cache for a single repository object. This implementation is
// not thread-safe. Deprecated in favor of NewCentralRepositoryCache.
func NewSingleRepositoryCache() RepositoryCache {
	return &singleRepositoryCache{}
}

func (c *singleRepositoryCache) Get(_ context.Context, path string) *models.Repository {
	if c.r == nil || c.r.Path != path {
		return nil
	}

	return c.r
}

func (c *singleRepositoryCache) Set(_ context.Context, r *models.Repository) {
	if r != nil {
		c.r = r
	}
}

func (c *singleRepositoryCache) InvalidateSize(_ context.Context, r *models.Repository) {
	if r != nil {
		c.r.Size = nil
	}
}

// SizeWithDescendantsTimedOut is a noop. We're phasing out the singleRepositoryCache cache implementation in favor of
// the centralRepositoryCache one, and the only place where we'll be making use of the related functionality (estimated
// size), the GitLab V1 API repositories handler, is explicitly making use of the latter.
func (*singleRepositoryCache) SizeWithDescendantsTimedOut(context.Context, *models.Repository) {}

// HasSizeWithDescendantsTimedOut is a noop. We're phasing out the singleRepositoryCache cache implementation in favor
// of the centralRepositoryCache one, and the only place where we'll be making use of the related functionality
// (estimated size), the GitLab V1 API repositories handler, is explicitly making use of the latter.
func (*singleRepositoryCache) HasSizeWithDescendantsTimedOut(context.Context, *models.Repository) bool {
	return false
}

// SetSizeWithDescendants is a noop. We're phasing out the singleRepositoryCache cache implementation in favor of
// the centralRepositoryCache one, the only implementation where we'll be making use of the related functionality.
func (*singleRepositoryCache) SetSizeWithDescendants(context.Context, *models.Repository, int64) {}

// GetSizeWithDescendants is a noop. We're phasing out the singleRepositoryCache cache implementation in favor of
// the centralRepositoryCache one, the only implementation where we'll be making use of the related functionality.
func (*singleRepositoryCache) GetSizeWithDescendants(context.Context, *models.Repository) (bool, int64) {
	return false, 0
}

// InvalidateRootSizeWithDescendants is a noop. We're phasing out the singleRepositoryCache cache implementation in favor
// of the centralRepositoryCache one, the only implementation where we'll be making use of the related functionality.
func (*singleRepositoryCache) InvalidateRootSizeWithDescendants(context.Context, *models.Repository) {
}

// SetLSN is a noop as this functionality depends on Redis.
func (*singleRepositoryCache) SetLSN(context.Context, *models.Repository, string) error { return nil }

// GetLSN is a noop as this functionality depends on Redis.
func (*singleRepositoryCache) GetLSN(context.Context, *models.Repository) (string, error) {
	return "", nil
}

// centralRepositoryCache is the interface for the centralized repository object cache backed by Redis.
type centralRepositoryCache struct {
	cache *iredis.Cache
}

// NewCentralRepositoryCache creates an interface for the centralized repository object cache backed by Redis.
func NewCentralRepositoryCache(cache *iredis.Cache) RepositoryCache {
	return &centralRepositoryCache{cache}
}

// key generates a valid Redis key string for a given repository object. The used key format is described in
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/redis-dev-guidelines.md#key-format.
func (*centralRepositoryCache) key(path string) string {
	nsPrefix := strings.Split(path, "/")[0]
	hex := digest.FromString(path).Hex()
	return fmt.Sprintf("registry:db:{repository:%s:%s}", nsPrefix, hex)
}

// sizeWithDescendantsTimedOutKey generates a valid Redis key string for a flag used to indicate whether the last "size
// with descendants" query has timed out for a given repository object.
// This flag needs to be stored as a separate key instead of being embedded in the repository struct because we need it
// to expire after a specific TTL, independently of the repository object key TTL.
func (c *centralRepositoryCache) sizeWithDescendantsTimedOutKey(path string) string {
	// "swd" stands for "size with descendants" for the sake of compactness (the name of these keys can get really long)
	return fmt.Sprintf("%s:swd-timeout", c.key(path))
}

// Get implements RepositoryCache.
func (c *centralRepositoryCache) Get(ctx context.Context, path string) *models.Repository {
	l := log.GetLogger(log.WithContext(ctx))

	getCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	var repo models.Repository
	if err := c.cache.UnmarshalGet(getCtx, c.key(path), &repo); err != nil {
		// a wrapped redis.Nil is returned when the key is not found in Redis
		if !errors.Is(err, redis.Nil) {
			l.WithError(err).Error("failed to read repository from cache")
		}
		return nil
	}

	// Double check that the obtained and decoded repository object has the same path that we're looking for. This
	// prevents leaking data from other repositories in case of a path hash collision.
	if repo.Path != path {
		l.WithFields(log.Fields{"path": path, "cached_path": repo.Path}).Warn("path hash collision detected when getting repository from cache")
		return nil
	}

	return &repo
}

// Set implements RepositoryCache.
func (c *centralRepositoryCache) Set(ctx context.Context, r *models.Repository) {
	if r == nil {
		return
	}
	setCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	if err := c.cache.MarshalSet(setCtx, c.key(r.Path), r); err != nil {
		log.GetLogger(log.WithContext(ctx)).WithError(err).Warn("failed to write repository to cache")
	}
}

const sizeTimedOutKeyTTL = 24 * time.Hour

// SizeWithDescendantsTimedOut creates a key in Redis for repository r whenever the SizeWithDescendants query has timed
// out. This key has a TTL of sizeTimedOutKeyTTL to avoid consecutive failures.
func (c *centralRepositoryCache) SizeWithDescendantsTimedOut(ctx context.Context, r *models.Repository) {
	if r == nil {
		return
	}
	setCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	if err := c.cache.Set(setCtx, c.sizeWithDescendantsTimedOutKey(r.Path), "true", iredis.WithTTL(sizeTimedOutKeyTTL)); err != nil {
		log.GetLogger(log.WithContext(ctx)).WithError(err).Warn("failed to create size with descendants timeout key in cache")
	}
}

// HasSizeWithDescendantsTimedOut checks if a size with descendants timeout key exists in Redis for repository r. This
// is then used to avoid consecutive failures during the TTL of this key (sizeTimedOutKeyTTL).
func (c *centralRepositoryCache) HasSizeWithDescendantsTimedOut(ctx context.Context, r *models.Repository) bool {
	setCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	if _, err := c.cache.Get(setCtx, c.sizeWithDescendantsTimedOutKey(r.Path)); err != nil {
		// a wrapped redis.Nil is returned when the key is not found in Redis
		if !errors.Is(err, redis.Nil) {
			msg := "failed to read size with descendants timeout key from cache"
			log.GetLogger(log.WithContext(ctx)).WithError(err).Error(msg)
			errortracking.Capture(fmt.Errorf("%s: %w", msg, err), errortracking.WithContext(ctx), errortracking.WithStackTrace())
		}
		return false
	}
	// we don't care about the value of the key, only if it exists
	return true
}

// InvalidateSize implements RepositoryCache.
func (c *centralRepositoryCache) InvalidateSize(ctx context.Context, r *models.Repository) {
	inValCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	r.Size = nil
	if err := c.cache.MarshalSet(inValCtx, c.key(r.Path), r); err != nil {
		detail := "failed to invalidate repository size in cache for repo: " + r.Path
		log.GetLogger(log.WithContext(ctx)).WithError(err).Warn(detail)
		err := fmt.Errorf("%q: %q", detail, err)
		errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
	}
}

// sizeWithDescendantsKey generates a valid Redis key string for the cached result of the last "size with descendants"
// query for a given repository.
// This flag is stored as a separate key instead of being embedded in the repository struct because we need it to
// expire after a specific TTL, independently of the repository object key TTL.
func (c *centralRepositoryCache) sizeWithDescendantsKey(path string) string {
	// "swd" stands for "size with descendants" for the sake of compactness
	return fmt.Sprintf("%s:swd", c.key(path))
}

// SetSizeWithDescendants implements RepositoryCache.
func (c *centralRepositoryCache) SetSizeWithDescendants(ctx context.Context, r *models.Repository, size int64) {
	if r == nil {
		return
	}
	setCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	if err := c.cache.MarshalSet(setCtx, c.sizeWithDescendantsKey(r.Path), size, iredis.WithTTL(sizeWithDescendantsKeyTTL)); err != nil {
		log.GetLogger(log.WithContext(ctx)).WithError(err).Warn("failed to create size with descendants key in cache")
	}
}

// GetSizeWithDescendants implements RepositoryCache.
func (c *centralRepositoryCache) GetSizeWithDescendants(ctx context.Context, r *models.Repository) (bool, int64) {
	getCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	var size int64
	if err := c.cache.UnmarshalGet(getCtx, c.sizeWithDescendantsKey(r.Path), &size); err != nil {
		// a wrapped redis.Nil is returned when the key is not found in Redis
		if !errors.Is(err, redis.Nil) {
			log.GetLogger(log.WithContext(ctx)).WithError(err).Error("failed to read size with descendants key from cache")
		}
		return false, 0
	}

	return true, size
}

// lsnKey generates a valid Redis key string for the cached primary database Log Sequence Number (LSN) for a
// given repository.
func (c *centralRepositoryCache) lsnKey(path string) string {
	return fmt.Sprintf("%s:lsn", c.key(path))
}

// This Lua script atomically updates a PostgreSQL Log Sequence Number (LSN) stored in Redis. It compares the new LSN
// with the existing LSN (if any) in Redis and only updates if the new LSN is greater or new.
// See https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/database-load-balancing.md#primary-sticking
//
// Conversion process (based on how PostgreSQL represents LSNs using its XLogRecPtr type):
// 1. Split the LSN into major and minor parts using string manipulation;
// 2. Convert the major and minor parts from hexadecimal to decimal;
// 3. The final numeric representation is calculated as: (major_part * 2^32) + minor_part.
//
// Inputs:
// KEYS[1]  - The Redis key where the LSN is stored.
// ARGV[1]  - The new LSN to compare, in 'X/Y' format (hexadecimal).
// ARGV[2]  - The TTL (in seconds) to set on the Redis key if the LSN is updated.
var lsnUpdateScript = redis.NewScript(`
local key, new_lsn, ttl = KEYS[1], ARGV[1], tonumber(ARGV[2])
local current_lsn = redis.call('GET', key)

if not current_lsn then
    return redis.call('SET', key, new_lsn, 'EX', ttl)
end

local function parse_lsn(lsn)
    local slash_pos = string.find(lsn, '/')
    local major_part = tonumber(string.sub(lsn, 1, slash_pos - 1), 16)
    local minor_part = tonumber(string.sub(lsn, slash_pos + 1), 16)
    return (major_part * 2^32) + minor_part
end

if parse_lsn(new_lsn) > parse_lsn(current_lsn) then
    return redis.call('SET', key, new_lsn, 'EX', ttl)
end

return false
`)

// SetLSN implements RepositoryCache.
func (c *centralRepositoryCache) SetLSN(ctx context.Context, r *models.Repository, lsn string) error {
	setCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	report := metrics.LSNCacheSet()
	_, err := c.cache.RunScript(setCtx, lsnUpdateScript, []string{c.lsnKey(r.Path)}, []any{lsn, lsnKeyTTL.Seconds()})
	// ignore a redis.Nil error, which is the result of returning false from the script (no update occurred)
	if errors.Is(err, redis.Nil) {
		err = nil
	}
	report(err)

	return err
}

// GetLSN implements RepositoryCache.
func (c *centralRepositoryCache) GetLSN(ctx context.Context, r *models.Repository) (string, error) {
	getCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	report := metrics.LSNCacheGet()
	lsn, err := c.cache.Get(getCtx, c.lsnKey(r.Path))
	report(err)
	if err != nil {
		// a wrapped redis.Nil is returned when the key is not found in Redis
		if !errors.Is(err, redis.Nil) {
			return "", fmt.Errorf("failed to read LSN key from cache: %w", err)
		}
		return "", nil
	}

	return lsn, nil
}

func scanFullRepository(row *sql.Row) (*models.Repository, error) {
	r := new(models.Repository)

	if err := row.Scan(&r.ID, &r.NamespaceID, &r.Name, &r.Path, &r.ParentID, &r.CreatedAt, &r.UpdatedAt, &r.LastPublishedAt); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("scanning repository: %w", err)
		}
		return nil, nil
	}

	return r, nil
}

func scanFullRepositories(rows *sql.Rows) (models.Repositories, error) {
	rr := make(models.Repositories, 0)
	defer rows.Close()

	for rows.Next() {
		r := new(models.Repository)
		if err := rows.Scan(&r.ID, &r.NamespaceID, &r.Name, &r.Path, &r.ParentID, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning repository: %w", err)
		}
		rr = append(rr, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanning repositories: %w", err)
	}

	return rr, nil
}

// FindByPath finds a repository by path.
func (s *repositoryStore) FindByPath(ctx context.Context, path string) (*models.Repository, error) {
	if cached := s.cache.Get(ctx, path); cached != nil {
		return cached, nil
	}

	defer metrics.InstrumentQuery("repository_find_by_path")()
	q := `SELECT
			id,
			top_level_namespace_id,
			name,
			path,
			parent_id,
			created_at,
			updated_at,
			last_published_at
		FROM
			repositories
		WHERE
			path = $1
			AND deleted_at IS NULL` // temporary measure for the duration of https://gitlab.com/gitlab-org/container-registry/-/issues/570

	row := s.db.QueryRowContext(ctx, q, path)

	r, err := scanFullRepository(row)
	if err != nil {
		return r, err
	}

	s.cache.Set(ctx, r)

	return r, nil
}

// FindAll finds all repositories.
func (s *repositoryStore) FindAll(ctx context.Context) (models.Repositories, error) {
	defer metrics.InstrumentQuery("repository_find_all")()
	q := `SELECT
			id,
			top_level_namespace_id,
			name,
			path,
			parent_id,
			created_at,
			updated_at
		FROM
			repositories`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("finding repositories: %w", err)
	}

	return scanFullRepositories(rows)
}

// FindAllPaginated finds up to `filters.MaxEntries` repositories with path lexicographically after `filters.LastEntry`. This is used exclusively
// for the GET /v2/_catalog API route, where pagination is done with a marker (`filters.LastEntry`). Empty repositories (which do
// not have at least a manifest) are ignored. Also, even if there is no repository with a path of `filters.LastEntry`, the returned
// repositories will always be those with a path lexicographically after `filters.LastEntry`. Finally, repositories are
// lexicographically sorted. These constraints exists to preserve the existing API behavior (when doing a filesystem
// walk based pagination).
func (s *repositoryStore) FindAllPaginated(ctx context.Context, filters FilterParams) (models.Repositories, error) {
	defer metrics.InstrumentQuery("repository_find_all_paginated")()
	q := `SELECT
			r.id,
			r.top_level_namespace_id,
			r.name,
			r.path,
			r.parent_id,
			r.created_at,
			r.updated_at
		FROM
			repositories AS r
		WHERE
			EXISTS (
				SELECT
				FROM
					manifests AS m
				WHERE
					m.top_level_namespace_id = r.top_level_namespace_id
					AND m.repository_id = r.id)
			AND r.path > $1
		ORDER BY
			r.path
		LIMIT $2`
	rows, err := s.db.QueryContext(ctx, q, filters.LastEntry, filters.MaxEntries)
	if err != nil {
		return nil, fmt.Errorf("finding repositories with pagination: %w", err)
	}

	return scanFullRepositories(rows)
}

// FindDescendantsOf finds all descendants of a given repository.
func (s *repositoryStore) FindDescendantsOf(ctx context.Context, id int64) (models.Repositories, error) {
	defer metrics.InstrumentQuery("repository_find_descendants_of")()
	q := `WITH RECURSIVE descendants AS (
			SELECT
				id,
				top_level_namespace_id,
				name,
				path,
				parent_id,
				created_at,
				updated_at
			FROM
				repositories
			WHERE
				id = $1
			UNION ALL
			SELECT
				r.id,
				r.top_level_namespace_id,
				r.name,
				r.path,
				r.parent_id,
				r.created_at,
				r.updated_at
			FROM
				repositories AS r
				JOIN descendants ON descendants.id = r.parent_id
		)
		SELECT
			*
		FROM
			descendants
		WHERE
			descendants.id != $1`

	rows, err := s.db.QueryContext(ctx, q, id)
	if err != nil {
		return nil, fmt.Errorf("finding descendants of repository: %w", err)
	}

	return scanFullRepositories(rows)
}

// FindAncestorsOf finds all ancestors of a given repository.
func (s *repositoryStore) FindAncestorsOf(ctx context.Context, id int64) (models.Repositories, error) {
	defer metrics.InstrumentQuery("repository_find_ancestors_of")()
	q := `WITH RECURSIVE ancestors AS (
			SELECT
				id,
				top_level_namespace_id,
				name,
				path,
				parent_id,
				created_at,
				updated_at
			FROM
				repositories
			WHERE
				id = $1
			UNION ALL
			SELECT
				r.id,
				r.top_level_namespace_id,
				r.name,
				r.path,
				r.parent_id,
				r.created_at,
				r.updated_at
			FROM
				repositories AS r
				JOIN ancestors ON ancestors.parent_id = r.id
		)
		SELECT
			*
		FROM
			ancestors
		WHERE
			ancestors.id != $1`

	rows, err := s.db.QueryContext(ctx, q, id)
	if err != nil {
		return nil, fmt.Errorf("finding ancestors of repository: %w", err)
	}

	return scanFullRepositories(rows)
}

// FindSiblingsOf finds all siblings of a given repository.
func (s *repositoryStore) FindSiblingsOf(ctx context.Context, id int64) (models.Repositories, error) {
	defer metrics.InstrumentQuery("repository_find_siblings_of")()
	q := `SELECT
			siblings.id,
			siblings.top_level_namespace_id,
			siblings.name,
			siblings.path,
			siblings.parent_id,
			siblings.created_at,
			siblings.updated_at
		FROM
			repositories AS siblings
			LEFT JOIN repositories AS anchor ON siblings.parent_id = anchor.parent_id
		WHERE
			anchor.id = $1
			AND siblings.id != $1`

	rows, err := s.db.QueryContext(ctx, q, id)
	if err != nil {
		return nil, fmt.Errorf("finding siblings of repository: %w", err)
	}

	return scanFullRepositories(rows)
}

// Tags finds all tags of a given repository.
func (s *repositoryStore) Tags(ctx context.Context, r *models.Repository) (models.Tags, error) {
	defer metrics.InstrumentQuery("repository_tags")()
	q := `SELECT
			id,
			top_level_namespace_id,
			name,
			repository_id,
			manifest_id,
			created_at,
			updated_at
		FROM
			tags
		WHERE
			repository_id = $1`

	rows, err := s.db.QueryContext(ctx, q, r.ID)
	if err != nil {
		return nil, fmt.Errorf("finding tags: %w", err)
	}

	return scanFullTags(rows)
}

// TagsPaginated finds up to `filters.MaxEntries` tags of a given repository with name lexicographically after `filters.LastEntry`. This is used
// exclusively for the GET /v2/<name>/tags/list API route, where pagination is done with a marker (`filters.LastEntry`). Even if
// there is no tag with a name of `filters.LastEntry`, the returned tags will always be those with a path lexicographically after
// `filters.LastEntry`. Finally, tags are lexicographically sorted. These constraints exists to preserve the existing API behavior
// (when doing a filesystem walk based pagination).
func (s *repositoryStore) TagsPaginated(ctx context.Context, r *models.Repository, filters FilterParams) (models.Tags, error) {
	defer metrics.InstrumentQuery("repository_tags_paginated")()
	q := `SELECT
			id,
			top_level_namespace_id,
			name,
			repository_id,
			manifest_id,
			created_at,
			updated_at
		FROM
			tags
		WHERE
			top_level_namespace_id = $1
			AND repository_id = $2
			AND name > $3
		ORDER BY
			name
		LIMIT $4`
	rows, err := s.db.QueryContext(ctx, q, r.NamespaceID, r.ID, filters.LastEntry, filters.MaxEntries)
	if err != nil {
		return nil, fmt.Errorf("finding tags with pagination: %w", err)
	}

	return scanFullTags(rows)
}

func scanFullTagsDetail(rows *sql.Rows) ([]*models.TagDetail, error) {
	tt := make([]*models.TagDetail, 0)
	defer rows.Close()

	for rows.Next() {
		var dgst Digest
		var cfgDgst sql.NullString
		t := new(models.TagDetail)
		if err := rows.Scan(&t.Name, &dgst, &cfgDgst, &t.MediaType, &t.Size, &t.CreatedAt, &t.UpdatedAt, &t.PublishedAt); err != nil {
			return nil, fmt.Errorf("scanning tag details: %w", err)
		}

		var err error
		t.Digest, err = dgst.Parse()
		if err != nil {
			return nil, err
		}

		t.ConfigDigest, err = parseConfigDigest(cfgDgst)
		if err != nil {
			return nil, err
		}

		tt = append(tt, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanning tag details: %w", err)
	}

	return tt, nil
}

func parseConfigDigest(cfgDgst sql.NullString) (models.NullDigest, error) {
	var dgst models.NullDigest

	if cfgDgst.Valid {
		cd, err := Digest(cfgDgst.String).Parse()
		if err != nil {
			return dgst, err
		}

		dgst = models.NullDigest{
			Digest: cd,
			Valid:  true,
		}
	}

	return dgst, nil
}

// The query for this method takes a list of TagDetails and returns a list of
// manifests which are referrers to those tags - i.e. `subject_id` points to
// one of the tags. The method modifies the `tags` collection by populating
// the `Referrers` field with the query results.
func (s *repositoryStore) appendTagsDetailReferrers(ctx context.Context, r *models.Repository, tags []*models.TagDetail, artifactTypes []string) error {
	if len(tags) == 0 {
		return nil
	}

	sbjDigests := make([]string, 0, len(tags))
	for _, tag := range tags {
		nd, err := NewDigest(tag.Digest)
		if err != nil {
			return err
		}
		sbjDigests = append(sbjDigests, nd.HexDecode())
	}

	q := `SELECT
			encode(m.digest, 'hex') AS digest,
			COALESCE(at.media_type, cmt.media_type) AS artifact_type,
			encode(ms.digest, 'hex') AS subject_digest
		FROM
			manifests AS m
			JOIN manifests AS ms ON m.top_level_namespace_id = ms.top_level_namespace_id
				AND m.subject_id = ms.id
			LEFT JOIN media_types AS at ON at.id = m.artifact_media_type_id
			LEFT JOIN media_types AS cmt ON cmt.id = m.configuration_media_type_id
		WHERE
			m.top_level_namespace_id = $1
			AND m.repository_id = $2
			AND m.subject_id IN (
				SELECT
					id
				FROM
					manifests
				WHERE
					top_level_namespace_id = $1
					AND repository_id = $2
					AND digest = ANY ($3))`

	var rows *sql.Rows
	var err error

	if len(artifactTypes) > 0 {
		ats, errInner := s.mediaTypeIDs(ctx, artifactTypes)
		if errInner != nil {
			return errInner
		}
		q += " AND (m.artifact_media_type_id = ANY ($4) OR m.configuration_media_type_id = ANY ($4))"
		rows, err = s.db.QueryContext(ctx, q, r.NamespaceID, r.ID, sbjDigests, ats) // nolint: staticcheck // err is checked below
	} else {
		rows, err = s.db.QueryContext(ctx, q, r.NamespaceID, r.ID, sbjDigests)
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	refMap := make(map[string][]models.TagReferrerDetail)
	var (
		dgst, sbjDgst Digest
		at, sbjStr    string
	)
	for rows.Next() {
		if err = rows.Scan(&dgst, &at, &sbjDgst); err != nil {
			return fmt.Errorf("scanning referrer: %w", err)
		}

		d, err := dgst.Parse()
		if err != nil {
			return err
		}
		sbj, err := sbjDgst.Parse()
		if err != nil {
			return err
		}
		sbjStr = sbj.String()

		if refMap[sbjStr] == nil {
			refMap[sbjStr] = make([]models.TagReferrerDetail, 0)
		}
		refMap[sbjStr] = append(refMap[sbjStr], models.TagReferrerDetail{
			Digest:       d.String(),
			ArtifactType: at,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scanning referrers: %w", err)
	}

	for _, tag := range tags {
		tag.Referrers = refMap[tag.Digest.String()]
	}

	return nil
}

// sqlPartialMatch builds a string that can be passed as value for a SQL `LIKE` expression. Besides surrounding the
// input value with `%` wildcard characters for a partial match, this function also escapes the `_` and `%`
// metacharacters supported in Postgres `LIKE` expressions.
// See https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-LIKE for more details.
func sqlPartialMatch(value string) string {
	value = strings.ReplaceAll(value, "_", `\_`)
	value = strings.ReplaceAll(value, "%", `\%`)

	return fmt.Sprintf("%%%s%%", value)
}

// TagsDetailPaginated finds up to `filters.MaxEntries` tags of a given repository with name lexicographically after `filters.LastEntry`. This is
// used exclusively for the GET /gitlab/v1/<name>/tags/list API, where pagination is done with a marker (`filters.LastEntry`).
// Even if there is no tag with a name of `filters.LastEntry`, the returned tags will always be those with a path lexicographically
// after `filters.LastEntry`. Tags are lexicographically sorted.
// Optionally, it is possible to pass a string to be used as a partial match filter for tag names using `filters.Name` and exact match using
// `filters.ExactName`. The search is not filtered if both of these values are empty.
func (s *repositoryStore) TagsDetailPaginated(ctx context.Context, r *models.Repository, filters FilterParams) ([]*models.TagDetail, error) {
	defer metrics.InstrumentQuery("repository_tags_detail_paginated")()

	q, args, err := tagsDetailPaginatedQuery(r, filters)
	if err != nil {
		return nil, fmt.Errorf("constructing tags detail paginated query: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("finding tags detail with pagination: %w", err)
	}

	tags, err := scanFullTagsDetail(rows)
	if err != nil {
		return nil, err
	}

	if filters.IncludeReferrers {
		if err := s.appendTagsDetailReferrers(ctx, r, tags, filters.ReferrerTypes); err != nil {
			return nil, fmt.Errorf("populating referrers: %w", err)
		}
	}
	return tags, nil
}

// SingleTagDetail returns the detail of a tag with its manifest payload
// and its configuration payload for single manifests. The configuration
// payload will be empty for manifest lists.
func (s *repositoryStore) TagDetail(ctx context.Context, r *models.Repository, tagName string) (*models.TagDetail, error) {
	defer metrics.InstrumentQuery("repository_tag_detail")()

	q := `
		SELECT
			t.name,
			encode(m.digest, 'hex') AS digest,
			encode(m.configuration_blob_digest, 'hex') AS config_digest,
			mt.media_type,
			m.total_size,
			t.created_at,
			t.updated_at,
			GREATEST(t.created_at, t.updated_at) as published_at,
			m.payload,
			m.configuration_payload
		FROM tags t
			JOIN manifests AS m ON m.top_level_namespace_id = t.top_level_namespace_id
				AND m.repository_id = t.repository_id
				AND m.id = t.manifest_id
			JOIN media_types AS mt ON mt.id = m.media_type_id
		WHERE
			t.top_level_namespace_id = $1
			AND t.repository_id = $2
			AND t.name = $3
	    `

	manifestPayload := new(models.Payload)
	cfgPayload := new(models.Payload)
	td := &models.TagDetail{}

	var dgst Digest
	var cfgDgst sql.NullString

	err := s.db.QueryRowContext(ctx, q, r.NamespaceID, r.ID, tagName).Scan(
		&td.Name, &dgst, &cfgDgst, &td.MediaType,
		&td.Size, &td.CreatedAt, &td.UpdatedAt, &td.PublishedAt, &manifestPayload, &cfgPayload)
	if err != nil {
		return nil, fmt.Errorf("finding single tag detail: %w", err)
	}

	td.Digest, err = dgst.Parse()
	if err != nil {
		return nil, err
	}

	td.ConfigDigest, err = parseConfigDigest(cfgDgst)
	if err != nil {
		return nil, err
	}

	td.ManifestPayload = *manifestPayload

	switch td.MediaType {
	case manifestlist.MediaTypeManifestList, v1.MediaTypeImageIndex:
	// no op
	default:
		td.ConfigPayload = *cfgPayload
	}

	return td, nil
}

func (s *repositoryStore) mediaTypeIDs(ctx context.Context, types []string) ([]string, error) {
	if len(types) == 0 {
		return nil, nil
	}

	q := "SELECT id FROM media_types WHERE media_type = ANY ($1)"
	rows, err := s.db.QueryContext(ctx, q, types)
	if err != nil {
		return nil, fmt.Errorf("selecting media types by name: %w", err)
	}
	defer rows.Close()

	var ids []string

	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scanning media type ids: %w", err)
		}
		ids = append(ids, strconv.FormatInt(id, 10))
	}

	return ids, nil
}

func tagsDetailPaginatedQuery(r *models.Repository, filters FilterParams) (string, []any, error) {
	qb := NewQueryBuilder()

	err := qb.Build(
		`SELECT
			t.name,
			encode(m.digest, 'hex') AS digest,
			encode(m.configuration_blob_digest, 'hex') AS config_digest,
			mt.media_type,
			m.total_size,
			t.created_at,
			t.updated_at,
			GREATEST(t.created_at, t.updated_at) as published_at
		FROM
			tags AS t
			JOIN manifests AS m ON m.top_level_namespace_id = t.top_level_namespace_id
				AND m.repository_id = t.repository_id
				AND m.id = t.manifest_id
			JOIN media_types AS mt ON mt.id = m.media_type_id
		WHERE
			t.top_level_namespace_id = ?
			AND t.repository_id = ?
		`,
		r.NamespaceID, r.ID,
	)
	if err != nil {
		return "", nil, err
	}

	if filters.ExactName != "" {
		// NOTE(prozlach): In the case when there is exact match requested,
		// there is going to be only single entry in the response, or none. So
		// there is no point in adding pagination and sorting keywords here.
		err := qb.Build("AND t.name = ?", filters.ExactName)
		if err != nil {
			return "", nil, err
		}
		return qb.SQL(), qb.Params(), nil
	}

	// NOTE(prozlach): We handle both cases in this path - empty and not
	// empty `Name` filter
	err = qb.Build("AND t.name LIKE ?\n", sqlPartialMatch(filters.Name))
	if err != nil {
		return "", nil, err
	}

	// default to ascending order to keep backwards compatibility
	if filters.SortOrder == "" {
		filters.SortOrder = OrderAsc
	}
	if filters.OrderBy == "" {
		filters.OrderBy = orderByName
	}

	switch {
	case filters.LastEntry == "" && filters.BeforeEntry == "" && filters.PublishedAt == "":
		// this should always return the first page up to filters.MaxEntries
		if filters.OrderBy == "published_at" {
			err = qb.Build(
				fmt.Sprintf(`ORDER BY published_at %s, name %s LIMIT ?`, filters.SortOrder, filters.SortOrder),
				filters.MaxEntries,
			)
			if err != nil {
				return "", nil, err
			}
		} else {
			err = qb.Build(
				fmt.Sprintf(`ORDER BY name %s LIMIT ?`, filters.SortOrder),
				filters.MaxEntries,
			)
			if err != nil {
				return "", nil, err
			}
		}
	case filters.LastEntry != "":
		err := getLastEntryQuery(qb, filters)
		if err != nil {
			return "", nil, err
		}
	case filters.BeforeEntry != "":
		err := getBeforeEntryQuery(qb, filters)
		if err != nil {
			return "", nil, err
		}
	case filters.PublishedAt != "":
		err := getPublishedAtQuery(qb, filters)
		if err != nil {
			return "", nil, err
		}
	}

	return qb.SQL(), qb.Params(), nil
}

func getPublishedAtQuery(qb *QueryBuilder, filters FilterParams) error {
	f := func(comparisonSign, sortOrder SortOrder) string {
		filterFmt := `AND GREATEST(t.created_at,t.updated_at) %s= ?
		ORDER BY
			published_at %s,
			t.name %s
		LIMIT ?`

		return fmt.Sprintf(filterFmt, comparisonSign, sortOrder, sortOrder)
	}

	if filters.SortOrder == OrderDesc {
		err := qb.Build(f(lessThan, OrderAsc), filters.PublishedAt, filters.MaxEntries)
		if err != nil {
			return err
		}
		// The results will be reversed, so we need to wrap the query in a
		// SELECT statement that sorts the tags in the correct order
		return qb.WrapIntoSubqueryOf(
			fmt.Sprintf(`SELECT * FROM (%%s) AS tags ORDER BY tags.%s DESC`, filters.OrderBy),
		)
	}
	return qb.Build(f(greaterThan, OrderAsc), filters.PublishedAt, filters.MaxEntries)
}

func getLastEntryQuery(qb *QueryBuilder, filters FilterParams) error {
	var (
		comparisonOperator string
		orderDirection     SortOrder
	)

	switch filters.SortOrder {
	case OrderDesc:
		orderDirection = OrderDesc
		comparisonOperator = lessThan
	case OrderAsc:
		orderDirection = OrderAsc
		comparisonOperator = greaterThan
	}

	if filters.PublishedAt != "" {
		return qb.Build(
			formatTagFilterWithPublishedAt(comparisonOperator, orderDirection),
			filters.PublishedAt, filters.LastEntry, filters.MaxEntries,
		)
	}
	return qb.Build(
		formatTagFilter(comparisonOperator, filters.OrderBy, orderDirection),
		filters.LastEntry, filters.MaxEntries,
	)
}

func getBeforeEntryQuery(qb *QueryBuilder, filters FilterParams) error {
	var (
		comparisonOperator     string
		rootQueryOderDirection string
		orderDirection         SortOrder
	)

	switch filters.SortOrder {
	case OrderDesc:
		orderDirection = OrderAsc
		comparisonOperator = greaterThan
		rootQueryOderDirection = "DESC"
	case OrderAsc:
		orderDirection = OrderDesc
		comparisonOperator = lessThan
		rootQueryOderDirection = "ASC"
	}

	if filters.PublishedAt != "" {
		err := qb.Build(
			formatTagFilterWithPublishedAt(comparisonOperator, orderDirection),
			filters.PublishedAt, filters.BeforeEntry, filters.MaxEntries,
		)
		if err != nil {
			return err
		}
	} else {
		err := qb.Build(
			formatTagFilter(comparisonOperator, filters.OrderBy, orderDirection),
			filters.BeforeEntry, filters.MaxEntries,
		)
		if err != nil {
			return err
		}
	}

	// The results will be reversed, so we need to wrap the query in a
	// SELECT statement that sorts the tags in the correct order
	// if we are fetching by filters.BeforeEntry we need to sort in DESC order
	return qb.WrapIntoSubqueryOf(
		fmt.Sprintf(`SElECT * FROM (%%s) AS tags ORDER BY tags.%s %s`, filters.OrderBy, rootQueryOderDirection),
	)
}

// formatTagFilter using the base query from tagsDetailPaginatedQuery as reference
func formatTagFilter(comparisonSign, orderBy string, sortOrder SortOrder) string {
	filter := `AND t.name %s ?
		ORDER BY
			%s %s
		LIMIT ?`

	return fmt.Sprintf(filter, comparisonSign, orderBy, sortOrder)
}

// formatTagFilterWithPublishedAt using the base query from tagsDetailPaginatedQuery as reference
func formatTagFilterWithPublishedAt(comparisonSign string, sortOrder SortOrder) string {
	filter := `AND (GREATEST(t.created_at, t.updated_at), t.name) %s (?, ?)
		ORDER BY
			published_at %s,
			t.name %s
		LIMIT ?`

	return fmt.Sprintf(filter, comparisonSign, sortOrder, sortOrder)
}

// HasTagsAfterName checks if a given repository has any more tags after `filters.LastEntry`. This is used
// exclusively for the GET /v2/<name>/tags/list API route, where pagination is done with a marker (`filters.LastEntry`). Even if
// there is no tag with a name of `filters.LastEntry`, the counted tags will always be those with a path lexicographically after
// `filters.LastEntry`. This constraint exists to preserve the existing API behavior (when doing a filesystem walk based
// pagination). Optionally, it is possible to pass a string to be used as a partial match filter for tag names using `filters.Name`.
// The search is not filtered if this value is an empty string.
func (s *repositoryStore) HasTagsAfterName(ctx context.Context, r *models.Repository, filters FilterParams) (bool, error) {
	defer metrics.InstrumentQuery("repository_tags_count_after_name")()

	qb := NewQueryBuilder()
	err := qb.Build(`SELECT
			1
		FROM
			tags
		WHERE
			top_level_namespace_id = ?
			AND repository_id = ?
			AND name LIKE ?`,
		r.NamespaceID, r.ID, sqlPartialMatch(filters.Name),
	)
	if err != nil {
		return false, err
	}

	comparison := greaterThan
	if filters.SortOrder == OrderDesc {
		comparison = lessThan
	}

	if filters.OrderBy != "published_at" {
		err = qb.Build(fmt.Sprintf(`AND name %s ?`, comparison), filters.LastEntry)
		if err != nil {
			return false, err
		}
	} else {
		err = qb.Build(
			fmt.Sprintf(`AND (GREATEST(created_at, updated_at), name) %s (?, ?)`, comparison),
			filters.PublishedAt, filters.LastEntry,
		)
		if err != nil {
			return false, err
		}
	}

	var count int
	if err := s.db.QueryRowContext(ctx, qb.SQL(), qb.Params()...).Scan(&count); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("checking if there are more tags after name: %w", err)
	}

	return count == 1, nil
}

// HasTagsBeforeName checks if a given repository has any more tags before `filters.BeforeEntry`. This is used
// exclusively for the GET /v2/<name>/tags/list API route, where pagination is done with a marker (`filters.BeforeEntry`). Even if
// there is no tag with a name of `filters.BeforeEntry`, the counted tags will always be those with a path lexicographically before
// `filters.BeforeEntry`. This constraint exists to preserve the existing API behavior (when doing a filesystem walk based
// pagination). Optionally, it is possible to pass a string to be used as a partial match filter for tag names using `filters.Name`.
// The search is not filtered if this value is an empty string.
func (s *repositoryStore) HasTagsBeforeName(ctx context.Context, r *models.Repository, filters FilterParams) (bool, error) {
	// There is no point in querying this as it would mean we need to count ALL the tags
	if filters.BeforeEntry == "" {
		return false, nil
	}

	defer metrics.InstrumentQuery("repository_tags_count_before_name")()

	qb := NewQueryBuilder()
	err := qb.Build(`SELECT
			1
		FROM
			tags
		WHERE
			top_level_namespace_id = ?
			AND repository_id = ?
			AND name LIKE ?`,
		r.NamespaceID, r.ID, sqlPartialMatch(filters.Name),
	)
	if err != nil {
		return false, err
	}

	comparison := lessThan
	if filters.SortOrder == OrderDesc {
		comparison = greaterThan
	}

	if filters.OrderBy != "published_at" {
		err = qb.Build(fmt.Sprintf(`AND name %s ?`, comparison), filters.BeforeEntry)
		if err != nil {
			return false, err
		}
	} else {
		err = qb.Build(
			fmt.Sprintf(`AND (GREATEST(created_at, updated_at), name) %s (?, ?)`, comparison),
			filters.PublishedAt, filters.BeforeEntry,
		)
		if err != nil {
			return false, err
		}
	}

	var count int
	if err := s.db.QueryRowContext(ctx, qb.SQL(), qb.Params()...).Scan(&count); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("checking if there are more tags before name: %w", err)
	}

	return count == 1, nil
}

// ManifestTags finds all tags of a given repository manifest.
func (s *repositoryStore) ManifestTags(ctx context.Context, r *models.Repository, m *models.Manifest) (models.Tags, error) {
	defer metrics.InstrumentQuery("repository_manifest_tags")()
	q := `SELECT
			id,
			top_level_namespace_id,
			name,
			repository_id,
			manifest_id,
			created_at,
			updated_at
		FROM
			tags
		WHERE
			top_level_namespace_id = $1
			AND repository_id = $2
			AND manifest_id = $3`

	rows, err := s.db.QueryContext(ctx, q, r.NamespaceID, r.ID, m.ID)
	if err != nil {
		return nil, fmt.Errorf("finding tags: %w", err)
	}

	return scanFullTags(rows)
}

// Count counts all repositories.
func (s *repositoryStore) Count(ctx context.Context) (int, error) {
	defer metrics.InstrumentQuery("repository_count")()
	q := "SELECT COUNT(*) FROM repositories"
	var count int

	if err := s.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return count, fmt.Errorf("counting repositories: %w", err)
	}

	return count, nil
}

// CountAfterPath counts all repositories with path lexicographically after lastPath. This is used exclusively
// for the GET /v2/_catalog API route, where pagination is done with a marker (lastPath). Empty repositories (which do
// not have at least a manifest) are ignored. Also, even if there is no repository with a path of lastPath, the counted
// repositories will always be those with a path lexicographically after lastPath. These constraints exists to preserve
// the existing API behavior (when doing a filesystem walk based pagination).
func (s *repositoryStore) CountAfterPath(ctx context.Context, path string) (int, error) {
	defer metrics.InstrumentQuery("repository_count_after_path")()
	q := `SELECT
			COUNT(*)
		FROM
			repositories AS r
		WHERE
			EXISTS (
				SELECT
				FROM
					manifests AS m
				WHERE
					m.top_level_namespace_id = r.top_level_namespace_id -- PROBLEM - cross partition scan
					AND m.repository_id = r.id)
			AND r.path > $1`

	var count int
	if err := s.db.QueryRowContext(ctx, q, path).Scan(&count); err != nil {
		return count, fmt.Errorf("counting repositories lexicographically after path: %w", err)
	}

	return count, nil
}

// CountPathSubRepositories counts all sub repositories of a repository path (including the base repository).
func (s *repositoryStore) CountPathSubRepositories(ctx context.Context, topLevelNamespaceID int64, path string) (int, error) {
	defer metrics.InstrumentQuery("repository_count_sub_repositories")()

	q := "SELECT COUNT(*) FROM repositories WHERE top_level_namespace_id = $1 AND (path = $2 OR path LIKE $3)"
	var count int
	if err := s.db.QueryRowContext(ctx, q, topLevelNamespaceID, path, path+"/%").Scan(&count); err != nil {
		return count, fmt.Errorf("counting sub-repositories: %w", err)
	}

	return count, nil
}

// Manifests finds all manifests associated with a repository.
func (s *repositoryStore) Manifests(ctx context.Context, r *models.Repository) (models.Manifests, error) {
	defer metrics.InstrumentQuery("repository_manifests")()
	q := `SELECT
			m.id,
			m.top_level_namespace_id,
			m.repository_id,
			m.total_size,
			m.schema_version,
			mt.media_type,
			at.media_type as artifact_type,
			encode(m.digest, 'hex') as digest,
			m.payload,
			mtc.media_type as configuration_media_type,
			encode(m.configuration_blob_digest, 'hex') as configuration_blob_digest,
			m.configuration_payload,
			m.non_conformant,
			m.non_distributable_layers,
			m.subject_id,
			m.created_at
		FROM
			manifests AS m
			JOIN media_types AS mt ON mt.id = m.media_type_id
			LEFT JOIN media_types AS mtc ON mtc.id = m.configuration_media_type_id
			LEFT JOIN media_types AS at ON at.id = m.artifact_media_type_id
		WHERE
			m.top_level_namespace_id = $1
			AND m.repository_id = $2
		ORDER BY m.id`

	rows, err := s.db.QueryContext(ctx, q, r.NamespaceID, r.ID)
	if err != nil {
		return nil, fmt.Errorf("finding manifests: %w", err)
	}

	return scanFullManifests(rows)
}

// FindManifestByDigest finds a manifest by digest within a repository.
func (s *repositoryStore) FindManifestByDigest(ctx context.Context, r *models.Repository, d digest.Digest) (*models.Manifest, error) {
	defer metrics.InstrumentQuery("repository_find_manifest_by_digest")()

	dgst, err := NewDigest(d)
	if err != nil {
		return nil, err
	}

	return findManifestByDigest(ctx, s.db, r.NamespaceID, r.ID, dgst)
}

// FindManifestByTagName finds a manifest by tag name within a repository.
func (s *repositoryStore) FindManifestByTagName(ctx context.Context, r *models.Repository, tagName string) (*models.Manifest, error) {
	defer metrics.InstrumentQuery("repository_find_manifest_by_tag_name")()
	q := `SELECT
			m.id,
			m.top_level_namespace_id,
			m.repository_id,
			m.total_size,
			m.schema_version,
			mt.media_type,
			at.media_type as artifact_type,
			encode(m.digest, 'hex') as digest,
			m.payload,
			mtc.media_type as configuration_media_type,
			encode(m.configuration_blob_digest, 'hex') as configuration_blob_digest,
			m.configuration_payload,
			m.non_conformant,
			m.non_distributable_layers,
			m.subject_id,
			m.created_at
		FROM
			manifests AS m
			JOIN media_types AS mt ON mt.id = m.media_type_id
			LEFT JOIN media_types AS mtc ON mtc.id = m.configuration_media_type_id
			LEFT JOIN media_types AS at ON at.id = m.artifact_media_type_id
			JOIN tags AS t ON t.top_level_namespace_id = m.top_level_namespace_id
				AND t.repository_id = m.repository_id
				AND t.manifest_id = m.id
		WHERE
			m.top_level_namespace_id = $1
			AND m.repository_id = $2
			AND t.name = $3`

	row := s.db.QueryRowContext(ctx, q, r.NamespaceID, r.ID, tagName)

	return scanFullManifest(row)
}

// Blobs finds all blobs associated with the repository.
func (s *repositoryStore) Blobs(ctx context.Context, r *models.Repository) (models.Blobs, error) {
	defer metrics.InstrumentQuery("repository_blobs")()
	q := `SELECT
			mt.media_type,
			encode(b.digest, 'hex') as digest,
			b.size,
			b.created_at
		FROM
			blobs AS b
			JOIN repository_blobs AS rb ON rb.blob_digest = b.digest
			JOIN repositories AS r ON r.id = rb.repository_id
			JOIN media_types AS mt ON mt.id = b.media_type_id
		WHERE
			r.top_level_namespace_id = $1
			AND r.id = $2`

	rows, err := s.db.QueryContext(ctx, q, r.NamespaceID, r.ID)
	if err != nil {
		return nil, fmt.Errorf("finding blobs: %w", err)
	}

	return scanFullBlobs(rows)
}

// FindBlob finds a blob by digest within a repository.
func (s *repositoryStore) FindBlob(ctx context.Context, r *models.Repository, d digest.Digest) (*models.Blob, error) {
	defer metrics.InstrumentQuery("repository_find_blob")()
	q := `SELECT
			mt.media_type,
			encode(b.digest, 'hex') as digest,
			b.size,
			b.created_at
		FROM
			blobs AS b
			JOIN media_types AS mt ON mt.id = b.media_type_id
			JOIN repository_blobs AS rb ON rb.blob_digest = b.digest
		WHERE
			rb.top_level_namespace_id = $1
			AND rb.repository_id = $2
			AND b.digest = decode($3, 'hex')`

	dgst, err := NewDigest(d)
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx, q, r.NamespaceID, r.ID, dgst)

	return scanFullBlob(row)
}

// ExistsBlob finds if a blob with a given digest exists within a repository.
func (s *repositoryStore) ExistsBlob(ctx context.Context, r *models.Repository, d digest.Digest) (bool, error) {
	defer metrics.InstrumentQuery("repository_exists_blob")()
	q := `SELECT
			EXISTS (
				SELECT
					1
				FROM
					repository_blobs
				WHERE
					top_level_namespace_id = $1
					AND repository_id = $2
					AND blob_digest = decode($3, 'hex'))`

	dgst, err := NewDigest(d)
	if err != nil {
		return false, err
	}

	var exists bool
	row := s.db.QueryRowContext(ctx, q, r.NamespaceID, r.ID, dgst)
	if err := row.Scan(&exists); err != nil {
		return false, fmt.Errorf("scanning blob: %w", err)
	}

	return exists, nil
}

// Create saves a new repository.
func (s *repositoryStore) Create(ctx context.Context, r *models.Repository) error {
	defer metrics.InstrumentQuery("repository_create")()

	q := `INSERT INTO repositories (top_level_namespace_id, name, path, parent_id)
			VALUES ($1, $2, $3, $4)
		RETURNING
			id, created_at`

	row := s.db.QueryRowContext(ctx, q, r.NamespaceID, r.Name, r.Path, r.ParentID)
	if err := row.Scan(&r.ID, &r.CreatedAt); err != nil {
		return fmt.Errorf("creating repository: %w", err)
	}

	s.cache.Set(ctx, r)

	return nil
}

// FindTagByName finds a tag by name within a repository.
func (s *repositoryStore) FindTagByName(ctx context.Context, r *models.Repository, name string) (*models.Tag, error) {
	defer metrics.InstrumentQuery("repository_find_tag_by_name")()
	q := `SELECT
			id,
			top_level_namespace_id,
			name,
			repository_id,
			manifest_id,
			created_at,
			updated_at
		FROM
			tags
		WHERE
			top_level_namespace_id = $1
			AND repository_id = $2
			AND name = $3`
	row := s.db.QueryRowContext(ctx, q, r.NamespaceID, r.ID, name)

	return scanFullTag(row)
}

// Size returns the deduplicated size of a repository. This is the sum of the size of all unique layers referenced by
// at least one tagged (directly or indirectly) manifest. No error is returned if the repository does not exist. It is
// the caller's responsibility to ensure it exists before calling this method and proceed accordingly if that matters.
func (s *repositoryStore) Size(ctx context.Context, r *models.Repository) (RepositorySize, error) {
	// Check the cached repository object for the size attribute first
	if r.Size != nil {
		return RepositorySize{bytes: *r.Size}, nil
	}
	defer metrics.InstrumentQuery("repository_size")()

	q := `SELECT
			coalesce(sum(q.size), 0)
		FROM ( WITH RECURSIVE cte AS (
				SELECT
					m.id AS manifest_id
				FROM
					manifests AS m
				WHERE
					m.top_level_namespace_id = $1
					AND m.repository_id = $2
					AND EXISTS (
						SELECT
						FROM
							tags AS t
						WHERE
							t.top_level_namespace_id = m.top_level_namespace_id
							AND t.repository_id = m.repository_id
							AND t.manifest_id = m.id)
					UNION
					SELECT
						mr.child_id AS manifest_id
					FROM
						manifest_references AS mr
						JOIN cte ON mr.parent_id = cte.manifest_id
					WHERE
						mr.top_level_namespace_id = $1
						AND mr.repository_id = $2)
					SELECT DISTINCT ON (l.digest)
						l.size
					FROM
						layers AS l
						JOIN cte ON l.top_level_namespace_id = $1
							AND l.repository_id = $2
							AND l.manifest_id = cte.manifest_id) AS q`

	var b int64
	if err := s.db.QueryRowContext(ctx, q, r.NamespaceID, r.ID).Scan(&b); err != nil {
		return RepositorySize{}, fmt.Errorf("calculating repository size: %w", err)
	}

	// Update the size attribute for the cached repository object
	r.Size = &b
	s.cache.Set(ctx, r)

	return RepositorySize{bytes: b}, nil
}

// topLevelSizeWithDescendants is an optimization for SizeWithDescendants when the target repository is a top-level
// repository. This allows using an optimized SQL query for this specific scenario.
func (s *repositoryStore) topLevelSizeWithDescendants(ctx context.Context, r *models.Repository) (int64, error) {
	defer metrics.InstrumentQuery("repository_size_with_descendants_top_level")()

	q := `SELECT
			coalesce(sum(q.size), 0)
		FROM ( WITH RECURSIVE cte AS (
				SELECT
					m.id AS manifest_id,
					m.repository_id
				FROM
					manifests AS m
				WHERE
					m.top_level_namespace_id = $1
					AND EXISTS (
						SELECT
						FROM
							tags AS t
						WHERE
							t.top_level_namespace_id = m.top_level_namespace_id
							AND t.repository_id = m.repository_id
							AND t.manifest_id = m.id)
					UNION
					SELECT
						mr.child_id AS manifest_id,
						mr.repository_id
					FROM
						manifest_references AS mr
						JOIN cte ON mr.repository_id = cte.repository_id
							AND mr.parent_id = cte.manifest_id
					WHERE
						mr.top_level_namespace_id = $1
		)
					SELECT DISTINCT ON (l.digest)
						l.size
					FROM
						cte
					CROSS JOIN LATERAL (
						SELECT
							digest,
							size
						FROM
							layers
						WHERE
							top_level_namespace_id = $1
							AND repository_id = cte.repository_id
							AND manifest_id = cte.manifest_id
						ORDER BY
							digest) l) AS q`

	var size int64
	if err := s.db.QueryRowContext(ctx, q, r.NamespaceID).Scan(&size); err != nil {
		return 0, fmt.Errorf("calculating top-level repository size with descendants: %w", err)
	}

	return size, nil
}

// nonTopLevelSizeWithDescendants is an optimization for SizeWithDescendants when the target repository is not a
// top-level repository. This allows using an optimized SQL query for this specific scenario.
func (s *repositoryStore) nonTopLevelSizeWithDescendants(ctx context.Context, r *models.Repository) (int64, error) {
	defer metrics.InstrumentQuery("repository_size_with_descendants")()

	q := `SELECT
			coalesce(sum(q.size), 0)
		FROM ( WITH RECURSIVE repository_ids AS MATERIALIZED (
				SELECT
					id
				FROM
					repositories
				WHERE
					top_level_namespace_id = $1
					AND (
						path = $2
						OR path LIKE $3
					)
				),
				cte AS (
					SELECT
						m.id AS manifest_id
					FROM
						manifests AS m
					WHERE
						m.top_level_namespace_id = $1
						AND m.repository_id IN (
							SELECT
								id
							FROM
								repository_ids)
							AND EXISTS (
								SELECT
								FROM
									tags AS t
								WHERE
									t.top_level_namespace_id = m.top_level_namespace_id
									AND t.repository_id = m.repository_id
									AND t.manifest_id = m.id)
							UNION
							SELECT
								mr.child_id AS manifest_id
							FROM
								manifest_references AS mr
								JOIN cte ON mr.parent_id = cte.manifest_id
							WHERE
								mr.top_level_namespace_id = $1
								AND mr.repository_id IN (
									SELECT
										id
									FROM
										repository_ids))
								SELECT DISTINCT ON (l.digest)
									l.size
								FROM
									layers AS l
									JOIN cte ON l.top_level_namespace_id = $1
										AND l.repository_id IN (
											SELECT
												id
											FROM
												repository_ids)
											AND l.manifest_id = cte.manifest_id) AS q`

	var size int64
	if err := s.db.QueryRowContext(ctx, q, r.NamespaceID, r.Path, r.Path+"/%").Scan(&size); err != nil {
		return 0, fmt.Errorf("calculating repository size with descendants: %w", err)
	}

	return size, nil
}

var ErrSizeHasTimedOut = errors.New("size query timed out previously")

// RepositorySize represents the result of calculating the size of a repository.
type RepositorySize struct {
	bytes  int64
	cached bool
}

// Bytes returns the size in bytes.
func (s RepositorySize) Bytes() int64 {
	return s.bytes
}

// Cached indicates whether the size of the repository was obtained from cache.
func (s RepositorySize) Cached() bool {
	return s.cached
}

// SizeWithDescendants returns the deduplicated size of a repository, including all descendants (if any). This is the
// sum of the size of all unique layers referenced by at least one tagged (directly or indirectly) manifest. No error is
// returned if the repository does not exist. It is the caller's responsibility to ensure it exists before calling this
// method and proceed accordingly if that matters.
// If this method, for this repository, failed with a statement timeout in the last 24h, then ErrSizeHasTimedOut is
// returned to prevent consecutive failures. The caller can then fall back to EstimatedSizeWithDescendants. This is a
// mitigation strategy for https://gitlab.com/gitlab-org/container-registry/-/issues/779.
func (s *repositoryStore) SizeWithDescendants(ctx context.Context, r *models.Repository) (RepositorySize, error) {
	if !r.IsTopLevel() {
		b, err := s.nonTopLevelSizeWithDescendants(ctx, r)
		return RepositorySize{bytes: b}, err
	}

	if found, b := s.cache.GetSizeWithDescendants(ctx, r); found {
		return RepositorySize{b, true}, nil
	}

	if s.cache.HasSizeWithDescendantsTimedOut(ctx, r) {
		return RepositorySize{}, ErrSizeHasTimedOut
	}

	b, err := s.topLevelSizeWithDescendants(ctx, r)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.QueryCanceled {
			// flag query failure for target repository to avoid consecutive failures
			s.cache.SizeWithDescendantsTimedOut(ctx, r)
		}
	}
	s.cache.SetSizeWithDescendants(ctx, r, b)

	return RepositorySize{bytes: b}, err
}

// estimateTopLevelSizeWithDescendants is a simplified alternative to topLevelSizeWithDescendants which does not exclude
// estimateTopLevelSizeWithDescendants is a significantly faster alternative to topLevelSizeWithDescendants which does
// not exclude unreferenced layers. Therefore, the measured size should be considered an estimate.
func (s *repositoryStore) estimateTopLevelSizeWithDescendants(ctx context.Context, r *models.Repository) (int64, error) {
	defer metrics.InstrumentQuery("repository_size_with_descendants_top_level_estimate")()

	q := `SELECT
			coalesce(sum(q.size), 0)
		FROM ( SELECT DISTINCT ON (digest)
				size
			FROM
				layers
			WHERE
				top_level_namespace_id = $1) q`

	var size int64
	if err := s.db.QueryRowContext(ctx, q, r.NamespaceID).Scan(&size); err != nil {
		return 0, fmt.Errorf("estimating top-level repository size with descendants: %w", err)
	}

	return size, nil
}

var ErrOnlyRootEstimates = errors.New("only the size of root repositories can be estimated")

// EstimatedSizeWithDescendants is an alternative to SizeWithDescendants that relies on a simpler query that does not
// exclude unreferenced layers. Therefore, the measured size should be considered an estimate. This is a partial
// mitigation for https://gitlab.com/gitlab-org/container-registry/-/issues/779. For now, only top-level namespaces are
// supported. ErrOnlyRootEstimates is returned if attempting to estimate the size of a non-root repository.
func (s *repositoryStore) EstimatedSizeWithDescendants(ctx context.Context, r *models.Repository) (RepositorySize, error) {
	if !r.IsTopLevel() {
		return RepositorySize{}, ErrOnlyRootEstimates
	}

	if found, b := s.cache.GetSizeWithDescendants(ctx, r); found {
		return RepositorySize{b, true}, nil
	}
	b, err := s.estimateTopLevelSizeWithDescendants(ctx, r)
	if err != nil {
		return RepositorySize{}, err
	}
	s.cache.SetSizeWithDescendants(ctx, r, b)

	return RepositorySize{bytes: b}, err
}

// CreateOrFind attempts to create a repository. If the repository already exists (same path) that record is loaded from
// the database into r. This is similar to a FindByPath followed by a Create, but without being prone to race conditions
// on write operations between the corresponding read (FindByPath) and write (Create) operations. Separate Find* and
// Create method calls should be preferred to this when race conditions are not a concern.
func (s *repositoryStore) CreateOrFind(ctx context.Context, r *models.Repository) error {
	if cached := s.cache.Get(ctx, r.Path); cached != nil {
		*r = *cached
		return nil
	}

	if r.NamespaceID == 0 {
		n := &models.Namespace{Name: r.TopLevelPathSegment()}
		ns := NewNamespaceStore(s.db)
		if err := ns.SafeFindOrCreate(ctx, n); err != nil {
			return fmt.Errorf("finding or creating namespace: %w", err)
		}
		r.NamespaceID = n.ID
	}

	defer metrics.InstrumentQuery("repository_create_or_find")()

	// First, check if the repository already exists, this avoids incrementing the repositories.id sequence
	// unnecessarily as we know that the target repository will already exist for all requests except the first.
	tmp, err := s.FindByPath(ctx, r.Path)
	if err != nil {
		return err
	}
	if tmp != nil {
		*r = *tmp
		return nil
	}

	// if not, proceed with creation attempt...
	// ON CONFLICT (path) DO UPDATE SET is a temporary measure until
	// https://gitlab.com/gitlab-org/container-registry/-/issues/625. If a repo record already exists for `path` but is
	// marked as soft deleted, we should undo the soft delete and proceed gracefully.
	q := `INSERT INTO repositories (top_level_namespace_id, name, path, parent_id)
			VALUES ($1, $2, $3, $4)
		ON CONFLICT (path)
			DO UPDATE SET
				deleted_at = NULL
		RETURNING
			id, created_at, deleted_at` // deleted_at returned for test validation purposes only

	row := s.db.QueryRowContext(ctx, q, r.NamespaceID, r.Name, r.Path, r.ParentID)
	if err := row.Scan(&r.ID, &r.CreatedAt, &r.DeletedAt); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("creating repository: %w", err)
		}
		// if the result set has no rows, then the repository already exists
		tmp, err := s.FindByPath(ctx, r.Path)
		if err != nil {
			return err
		}
		*r = *tmp
		s.cache.Set(ctx, r)
	}

	return nil
}

func splitRepositoryPath(path string) []string {
	return strings.Split(filepath.Clean(path), "/")
}

// repositoryName parses a repository path (e.g. `"a/b/c"`) and returns its name (e.g. `"c"`).
func repositoryName(path string) string {
	segments := splitRepositoryPath(path)
	return segments[len(segments)-1]
}

// CreateByPath creates the repository for a given path. An error is returned if the repository already exists.
func (s *repositoryStore) CreateByPath(ctx context.Context, path string, opts ...repositoryOption) (*models.Repository, error) {
	if cached := s.cache.Get(ctx, path); cached != nil {
		return cached, nil
	}

	n := &models.Namespace{Name: strings.Split(path, "/")[0]}
	ns := NewNamespaceStore(s.db)
	if err := ns.SafeFindOrCreate(ctx, n); err != nil {
		return nil, fmt.Errorf("finding or creating namespace: %w", err)
	}

	defer metrics.InstrumentQuery("repository_create_by_path")()
	r := &models.Repository{NamespaceID: n.ID, Name: repositoryName(path), Path: path}

	for _, opt := range opts {
		opt(r)
	}

	if err := s.Create(ctx, r); err != nil {
		return nil, err
	}

	s.cache.Set(ctx, r)

	return r, nil
}

// CreateOrFindByPath is the fully idempotent version of CreateByPath, where no error is returned if the repository
// already exists.
func (s *repositoryStore) CreateOrFindByPath(ctx context.Context, path string, opts ...repositoryOption) (*models.Repository, error) {
	if cached := s.cache.Get(ctx, path); cached != nil {
		return cached, nil
	}

	n := &models.Namespace{Name: strings.Split(path, "/")[0]}
	ns := NewNamespaceStore(s.db)
	if err := ns.SafeFindOrCreate(ctx, n); err != nil {
		return nil, fmt.Errorf("finding or creating namespace: %w", err)
	}

	defer metrics.InstrumentQuery("repository_create_or_find_by_path")()
	r := &models.Repository{NamespaceID: n.ID, Name: repositoryName(path), Path: path}

	for _, opt := range opts {
		opt(r)
	}

	if err := s.CreateOrFind(ctx, r); err != nil {
		return nil, err
	}

	s.cache.Set(ctx, r)

	return r, nil
}

// Update updates an existing repository.
func (s *repositoryStore) Update(ctx context.Context, r *models.Repository) error {
	defer metrics.InstrumentQuery("repository_update")()
	q := `UPDATE
			repositories
		SET
			(name, path, parent_id, updated_at) = ($1, $2, $3, now())
		WHERE
			top_level_namespace_id = $4
			AND id = $5
		RETURNING
			updated_at`

	row := s.db.QueryRowContext(ctx, q, r.Name, r.Path, r.ParentID, r.NamespaceID, r.ID)
	if err := row.Scan(&r.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("repository not found")
		}
		return fmt.Errorf("updating repository: %w", err)
	}

	s.cache.Set(ctx, r)

	return nil
}

// LinkBlob links a blob to a repository. It does nothing if already linked.
func (s *repositoryStore) LinkBlob(ctx context.Context, r *models.Repository, d digest.Digest) error {
	defer metrics.InstrumentQuery("repository_link_blob")()
	q := `INSERT INTO repository_blobs (top_level_namespace_id, repository_id, blob_digest)
			VALUES ($1, $2, decode($3, 'hex'))
		ON CONFLICT (top_level_namespace_id, repository_id, blob_digest)
			DO NOTHING`

	dgst, err := NewDigest(d)
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, q, r.NamespaceID, r.ID, dgst); err != nil {
		return fmt.Errorf("linking blob: %w", err)
	}

	return nil
}

// UnlinkBlob unlinks a blob from a repository. It does nothing if not linked. A boolean is returned to denote whether
// the link was deleted or not. This avoids the need for a separate preceding `SELECT` to find if it exists.
func (s *repositoryStore) UnlinkBlob(ctx context.Context, r *models.Repository, d digest.Digest) (bool, error) {
	defer metrics.InstrumentQuery("repository_unlink_blob")()
	q := "DELETE FROM repository_blobs WHERE top_level_namespace_id = $1 AND repository_id = $2 AND blob_digest = decode($3, 'hex')"

	dgst, err := NewDigest(d)
	if err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx, q, r.NamespaceID, r.ID, dgst)
	if err != nil {
		return false, fmt.Errorf("linking blob: %w", err)
	}

	count, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("linking blob: %w", err)
	}

	return count == 1, nil
}

// DeleteTagByName deletes a tag by name within a repository. A boolean is returned to denote whether the tag was
// deleted or not. This avoids the need for a separate preceding `SELECT` to find if it exists.
func (s *repositoryStore) DeleteTagByName(ctx context.Context, r *models.Repository, name string) (bool, error) {
	defer metrics.InstrumentQuery("repository_delete_tag_by_name")()
	q := "DELETE FROM tags WHERE top_level_namespace_id = $1 AND repository_id = $2 AND name = $3"

	res, err := s.db.ExecContext(ctx, q, r.NamespaceID, r.ID, name)
	if err != nil {
		return false, fmt.Errorf("deleting tag: %w", err)
	}

	count, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("deleting tag: %w", err)
	}

	s.cache.InvalidateSize(ctx, r)

	return count == 1, nil
}

// DeleteManifest deletes a manifest from a repository. A boolean is returned to denote whether the manifest was deleted
// or not. This avoids the need for a separate preceding `SELECT` to find if it exists. A manifest cannot be deleted if
// it is referenced by a manifest list.
func (s *repositoryStore) DeleteManifest(ctx context.Context, r *models.Repository, d digest.Digest) (bool, error) {
	defer metrics.InstrumentQuery("repository_delete_manifest")()
	q := "DELETE FROM manifests WHERE top_level_namespace_id = $1 AND repository_id = $2 AND digest = decode($3, 'hex')"

	dgst, err := NewDigest(d)
	if err != nil {
		return false, err
	}

	res, err := s.db.ExecContext(ctx, q, r.NamespaceID, r.ID, dgst)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.ForeignKeyViolation && pgErr.TableName == "manifest_references" {
			return false, fmt.Errorf("deleting manifest: %w", ErrManifestReferencedInList)
		}
		return false, fmt.Errorf("deleting manifest: %w", err)
	}

	count, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("deleting manifest: %w", err)
	}

	s.cache.InvalidateSize(ctx, r)

	return count == 1, nil
}

// FindPaginatedRepositoriesForPath finds all repositories (up to `filters.MaxEntries` repositories) that have the same base path as the requested repository.
// The results are ordered lexicographically by repository path and only begin from `filters.LastEntry`.
// Empty repositories (which do not have at least 1 tag) are ignored in the returned list.
// Also, even if there is no repository with a path equivalent to `filters.LastEntry`, the returned
// repositories will still be those with a base path of the requested repository and lexicographically after `filters.LastEntry`.
func (s *repositoryStore) FindPaginatedRepositoriesForPath(ctx context.Context, r *models.Repository, filters FilterParams) (models.Repositories, error) {
	// start from a path lexicographically before r.Path when no last path is available.
	// this improves the query performance as we will not need to filter from `r.path > ""` in the query below.
	if filters.LastEntry == "" {
		filters.LastEntry = lexicographicallyBeforePath(r.Path)
	}

	defer metrics.InstrumentQuery("repository_find_paginated_repositories_for_path")()
	q := `SELECT  
			id,
			top_level_namespace_id,
			name,
			path,
			parent_id,
			created_at,
			updated_at  
		FROM
			repositories AS r
		WHERE
			(r.path = $1 OR r.path LIKE $2)
			AND EXISTS ( SELECT FROM tags AS t WHERE t.top_level_namespace_id = r.top_level_namespace_id AND t.repository_id = r.id )
			AND (r.path > $3 AND r.path < $4)
			AND r.top_level_namespace_id = $5
		ORDER BY r.path
		LIMIT $6`

	rows, err := s.db.QueryContext(ctx, q, r.Path, r.Path+"/%", filters.LastEntry, lexicographicallyNextPath(r.Path), r.NamespaceID, filters.MaxEntries)
	if err != nil {
		return nil, fmt.Errorf("finding pagingated list of repository for path: %w", err)
	}

	return scanFullRepositories(rows)
}

// RenamePathForSubRepositories updates all sub repositories that start with a repository `oldPath` to a `newPath`
// e.g: All sub repositories with `oldPath`: my-group/my-sub-group/old-repo-name will be changed to:
// my-group/my-sub-group/new-repo-name, where the `newPath` argument is `my-group/my-sub-group/new-repo-name`.
// This does not change the base repository's path however.
func (s *repositoryStore) RenamePathForSubRepositories(ctx context.Context, topLevelNamespaceID int64, oldPath, newPath string) error {
	defer metrics.InstrumentQuery("repository_rename_sub_repositories_path")()

	q := "UPDATE repositories SET path = REPLACE(path, $1, $2) WHERE top_level_namespace_id = $3 AND path LIKE $4"
	_, err := s.db.ExecContext(ctx, q, oldPath, newPath, topLevelNamespaceID, oldPath+"/%")
	if err != nil {
		return fmt.Errorf("renaming sub-repository paths: %w", err)
	}
	return nil
}

// Rename updates a repository's path and name attributes to a `newPath` and `newName` respectively.
// This must always be followed by `RenamePathForSubRepositories` to make sure sub-repositories starting with
// the `oldPath`of the repository are also updated to start with the `newPath`.
func (s *repositoryStore) Rename(ctx context.Context, r *models.Repository, newPath, newName string) error {
	defer metrics.InstrumentQuery("repository_rename")()

	q := "UPDATE repositories SET path = $1, name = $2 WHERE top_level_namespace_id = $3 AND path = $4 RETURNING updated_at"
	row := s.db.QueryRowContext(ctx, q, newPath, newName, r.NamespaceID, r.Path)
	if err := row.Scan(&r.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("repository not found")
		}
		return fmt.Errorf("renaming repository: %w", err)
	}

	s.cache.Set(ctx, r)

	return nil
}

// UpdateLastPublishedAt updates the timestamp of the last tag published to the repository. This is the greatest value
// between the tag created at and published at timestamps.
func (s *repositoryStore) UpdateLastPublishedAt(ctx context.Context, r *models.Repository, t *models.Tag) error {
	defer metrics.InstrumentQuery("repository_update_last_published_at")()
	q := `UPDATE
			repositories
		SET
			last_published_at = $1
		WHERE
			top_level_namespace_id = $2
			AND id = $3
		RETURNING
			last_published_at`

	timestamp := t.CreatedAt
	if t.UpdatedAt.Valid {
		timestamp = t.UpdatedAt.Time
	}

	row := s.db.QueryRowContext(ctx, q, timestamp, r.NamespaceID, r.ID)
	if err := row.Scan(&r.LastPublishedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("repository not found")
		}
		return fmt.Errorf("updating repository last published at: %w", err)
	}

	s.cache.Set(ctx, r)

	return nil
}

// lexicographicallyNextPath takes a path string and returns the next lexicographical path string.
// Empty paths (i.e a path of the form "") will result in an "a", paths with only "z" characters will append an a.
// All other paths will result in their next logical lexicographical variation (e.g gitlab-com => gitlab-con , gitlab-com.  => gitlab-com/)
// This function serves primarily as a helper for optimizing paginated db queries used in `FindPaginatedRepositoriesForPath.
func lexicographicallyNextPath(path string) string {
	// Find first character from right
	// which is not z.
	i := strings.LastIndexFunc(path, func(r rune) bool {
		return r != 'z'
	})

	var nexLexPath string
	// If all characters are 'z' or empty string, append
	// an 'a' at the end.
	if i == -1 {
		nexLexPath = path + "a"
	} else {
		// If there are some non-z characters
		rPath := []rune(path)
		rPath[i]++
		nexLexPath = string(rPath)
	}
	return nexLexPath
}

// lexicographicallyBeforePath takes a path string and returns the lexicographical path just before the provided path.
// e.g gitlab-con => gitlab-com , gitlab-com/  => gitlab-com.
// In the event that an empty string is provided as a path it returns 'z'.
// In the event where only "a's" exist in the path; the last 'a' character of the path is converted to a "z".
// This function serves primarily as a helper for optimizing paginated db queries used in `FindPaginatedRepositoriesForPath.
func lexicographicallyBeforePath(path string) string {
	// this shouldn't be possible but to be safe we return a z on empty path
	if path == "" {
		return "z"
	}

	// Find first character from right
	// which is not a.
	i := strings.LastIndexFunc(path, func(r rune) bool {
		return r != 'a'
	})

	var beforeLexPath string
	// If all characters are 'a',
	// change the last character to 'z'.
	if i == -1 {
		beforeLexPath = strings.TrimSuffix(path, "a") + "z"
	} else {
		// If there are some non "a" characters,
		// convert the very last one to its reverse lexicographical unicode character
		rPath := []rune(path)
		rPath[i]--
		beforeLexPath = string(rPath)
	}
	return beforeLexPath
}
