package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/datastore/metrics"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/internal/migration"

	gocache "github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/marshaler"
	libstore "github.com/eko/gocache/lib/v4/store"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/opencontainers/go-digest"
	"github.com/redis/go-redis/v9"
	"gitlab.com/gitlab-org/labkit/errortracking"
)

// cacheOpTimeout defines the timeout applied to cache operations against Redis
const cacheOpTimeout = 500 * time.Millisecond

// RepositoryReader is the interface that defines read operations for a repository store.
type RepositoryReader interface {
	FindAll(ctx context.Context) (models.Repositories, error)
	FindAllPaginated(ctx context.Context, limit int, lastPath string) (models.Repositories, error)
	FindByPath(ctx context.Context, path string) (*models.Repository, error)
	FindDescendantsOf(ctx context.Context, id int64) (models.Repositories, error)
	FindAncestorsOf(ctx context.Context, id int64) (models.Repositories, error)
	FindSiblingsOf(ctx context.Context, id int64) (models.Repositories, error)
	Count(ctx context.Context) (int, error)
	CountAfterPath(ctx context.Context, path string) (int, error)
	CountPathSubRepositories(ctx context.Context, topLevelNamespaceID int64, path string) (int, error)
	Manifests(ctx context.Context, r *models.Repository) (models.Manifests, error)
	Tags(ctx context.Context, r *models.Repository) (models.Tags, error)
	TagsPaginated(ctx context.Context, r *models.Repository, limit int, lastName string) (models.Tags, error)
	TagsCountAfterName(ctx context.Context, r *models.Repository, lastName string) (int, error)
	ManifestTags(ctx context.Context, r *models.Repository, m *models.Manifest) (models.Tags, error)
	FindManifestByDigest(ctx context.Context, r *models.Repository, d digest.Digest) (*models.Manifest, error)
	FindManifestByTagName(ctx context.Context, r *models.Repository, tagName string) (*models.Manifest, error)
	FindTagByName(ctx context.Context, r *models.Repository, name string) (*models.Tag, error)
	Blobs(ctx context.Context, r *models.Repository) (models.Blobs, error)
	FindBlob(ctx context.Context, r *models.Repository, d digest.Digest) (*models.Blob, error)
	ExistsBlob(ctx context.Context, r *models.Repository, d digest.Digest) (bool, error)
	Size(ctx context.Context, r *models.Repository) (int64, error)
	SizeWithDescendants(ctx context.Context, r *models.Repository) (int64, error)
	EstimatedSizeWithDescendants(ctx context.Context, r *models.Repository) (int64, error)
	TagsDetailPaginated(ctx context.Context, r *models.Repository, limit int, lastName string) ([]*models.TagDetail, error)
	FindPagingatedRepositoriesForPath(ctx context.Context, r *models.Repository, lastPath string, limit int) (models.Repositories, error)
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
}

type repositoryOption func(*models.Repository)

// WithMigrationStatus instantiates the repository with the provided
// migration status.
func WithMigrationStatus(status migration.RepositoryStatus) repositoryOption {
	return func(r *models.Repository) {
		r.MigrationStatus = status
	}
}

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
func NewRepositoryStore(db Queryer, opts ...RepositoryStoreOption) *repositoryStore {
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
}

// noOpRepositoryCache satisfies the RepositoryCache, but does not cache anything.
// Useful as a default and for testing.
type noOpRepositoryCache struct{}

// NewNoOpRepositoryCache creates a new non-operational cache for a repository object.
// This implementation does nothing and returns nothing for all its methods.
func NewNoOpRepositoryCache() *noOpRepositoryCache {
	return &noOpRepositoryCache{}
}

func (n *noOpRepositoryCache) Get(context.Context, string) *models.Repository                  { return nil }
func (n *noOpRepositoryCache) Set(context.Context, *models.Repository)                         {}
func (n *noOpRepositoryCache) InvalidateSize(context.Context, *models.Repository)              {}
func (n *noOpRepositoryCache) SizeWithDescendantsTimedOut(context.Context, *models.Repository) {}
func (n *noOpRepositoryCache) HasSizeWithDescendantsTimedOut(context.Context, *models.Repository) bool {
	return false
}

// singleRepositoryCache caches a single repository in-memory. This implementation is not thread-safe. Deprecated in
// favor of centralRepositoryCache.
type singleRepositoryCache struct {
	r *models.Repository
}

// NewSingleRepositoryCache creates a new local in-memory cache for a single repository object. This implementation is
// not thread-safe. Deprecated in favor of NewCentralRepositoryCache.
func NewSingleRepositoryCache() *singleRepositoryCache {
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
func (c *singleRepositoryCache) SizeWithDescendantsTimedOut(context.Context, *models.Repository) {}

// HasSizeWithDescendantsTimedOut is a noop. We're phasing out the singleRepositoryCache cache implementation in favor
// of the centralRepositoryCache one, and the only place where we'll be making use of the related functionality
// (estimated size), the GitLab V1 API repositories handler, is explicitly making use of the latter.
func (c *singleRepositoryCache) HasSizeWithDescendantsTimedOut(context.Context, *models.Repository) bool {
	return false
}

// centralRepositoryCache is the interface for the centralized repository object cache backed by Redis.
type centralRepositoryCache struct {
	// cache provides access to the raw gocache interface
	cache *gocache.Cache[any]
	// marshaler provides access to a MessagePack backed marshaling interface
	marshaler *marshaler.Marshaler
}

// NewCentralRepositoryCache creates an interface for the centralized repository object cache backed by Redis.
func NewCentralRepositoryCache(cache *gocache.Cache[any]) *centralRepositoryCache {
	return &centralRepositoryCache{cache, marshaler.New(cache)}
}

// key generates a valid Redis key string for a given repository object. The used key format is described in
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs-gitlab/redis-dev-guidelines.md#key-format.
func (c *centralRepositoryCache) key(path string) string {
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

	tmp, err := c.marshaler.Get(getCtx, c.key(path), new(models.Repository))
	if err != nil {
		// a wrapped redis.Nil is returned when the key is not found in Redis
		if !errors.Is(err, redis.Nil) {
			l.WithError(err).Error("failed to read repository from cache")
		}
		return nil
	}

	repo, ok := tmp.(*models.Repository)
	if !ok {
		l.Warn("failed to unmarshal repository from cache")
		return nil
	}

	// Double check that the obtained and decoded repository object has the same path that we're looking for. This
	// prevents leaking data from other repositories in case of a path hash collision.
	if repo.Path != path {
		l.WithFields(log.Fields{"path": path, "cached_path": repo.Path}).Warn("path hash collision detected when getting repository from cache")
		return nil
	}

	return repo
}

// Set implements RepositoryCache.
func (c *centralRepositoryCache) Set(ctx context.Context, r *models.Repository) {
	if r == nil {
		return
	}
	setCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	if err := c.marshaler.Set(setCtx, c.key(r.Path), r); err != nil {
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

	if err := c.cache.Set(setCtx, c.sizeWithDescendantsTimedOutKey(r.Path), "true", libstore.WithExpiration(sizeTimedOutKeyTTL)); err != nil {
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
			errortracking.Capture(fmt.Errorf("%s: %w", msg, err), errortracking.WithContext(ctx))
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
	if err := c.marshaler.Set(inValCtx, c.key(r.Path), r); err != nil {
		detail := "failed to invalidate repository size in cache for repo: " + r.Path
		log.GetLogger(log.WithContext(ctx)).WithError(err).Warn(detail)
		err := fmt.Errorf("%q: %q", detail, err)
		errortracking.Capture(err, errortracking.WithContext(ctx))
	}
}

func scanFullRepository(row *sql.Row) (*models.Repository, error) {
	r := new(models.Repository)

	if err := row.Scan(&r.ID, &r.NamespaceID, &r.Name, &r.Path, &r.ParentID, &r.MigrationStatus, &r.MigrationError, &r.CreatedAt, &r.UpdatedAt); err != nil {
		if err != sql.ErrNoRows {
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
		if err := rows.Scan(&r.ID, &r.NamespaceID, &r.Name, &r.Path, &r.ParentID, &r.MigrationStatus, &r.MigrationError, &r.CreatedAt, &r.UpdatedAt); err != nil {
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
			migration_status,
			migration_error,
			created_at,
			updated_at
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
			migration_status,
			migration_error,
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

// FindAllPaginated finds up to limit repositories with path lexicographically after lastPath. This is used exclusively
// for the GET /v2/_catalog API route, where pagination is done with a marker (lastPath). Empty repositories (which do
// not have at least a manifest) are ignored. Also, even if there is no repository with a path of lastPath, the returned
// repositories will always be those with a path lexicographically after lastPath. Finally, repositories are
// lexicographically sorted. These constraints exists to preserve the existing API behavior (when doing a filesystem
// walk based pagination).
func (s *repositoryStore) FindAllPaginated(ctx context.Context, limit int, lastPath string) (models.Repositories, error) {
	defer metrics.InstrumentQuery("repository_find_all_paginated")()
	q := `SELECT
			r.id,
			r.top_level_namespace_id,
			r.name,
			r.path,
			r.parent_id,
			r.migration_status,
			r.migration_error,
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
	rows, err := s.db.QueryContext(ctx, q, lastPath, limit)
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
				migration_status,
				migration_error,
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
				r.migration_status,
				r.migration_error,
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
				migration_status,
				migration_error,
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
				r.migration_status,
				r.migration_error,
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
			siblings.migration_status,
			siblings.migration_error,
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

// TagsPaginated finds up to limit tags of a given repository with name lexicographically after lastName. This is used
// exclusively for the GET /v2/<name>/tags/list API route, where pagination is done with a marker (lastName). Even if
// there is no tag with a name of lastName, the returned tags will always be those with a path lexicographically after
// lastName. Finally, tags are lexicographically sorted. These constraints exists to preserve the existing API behavior
// (when doing a filesystem walk based pagination).
func (s *repositoryStore) TagsPaginated(ctx context.Context, r *models.Repository, limit int, lastName string) (models.Tags, error) {
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
	rows, err := s.db.QueryContext(ctx, q, r.NamespaceID, r.ID, lastName, limit)
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
		if err := rows.Scan(&t.Name, &dgst, &cfgDgst, &t.MediaType, &t.Size, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning tag details: %w", err)
		}

		d, err := dgst.Parse()
		if err != nil {
			return nil, err
		}
		t.Digest = d

		if cfgDgst.Valid {
			cd, err := Digest(cfgDgst.String).Parse()
			if err != nil {
				return nil, err
			}
			t.ConfigDigest = models.NullDigest{
				Digest: cd,
				Valid:  true,
			}
		}

		tt = append(tt, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanning tag details: %w", err)
	}

	return tt, nil
}

// TagsDetailPaginated finds up to limit tags of a given repository with name lexicographically after lastName. This is
// used exclusively for the GET /gitlab/v1/<name>/tags/list API, where pagination is done with a marker (lastName).
// Even if there is no tag with a name of lastName, the returned tags will always be those with a path lexicographically
// after lastName. Finally, tags are lexicographically sorted.
func (s *repositoryStore) TagsDetailPaginated(ctx context.Context, r *models.Repository, limit int, lastName string) ([]*models.TagDetail, error) {
	defer metrics.InstrumentQuery("repository_tags_detail_paginated")()
	q := `SELECT
			t.name,
			encode(m.digest, 'hex') AS digest,
			encode(m.configuration_blob_digest, 'hex') AS config_digest,
			mt.media_type,
			m.total_size,
			t.created_at,
			t.updated_at
		FROM
			tags AS t
			JOIN manifests AS m ON m.top_level_namespace_id = t.top_level_namespace_id
				AND m.repository_id = t.repository_id
				AND m.id = t.manifest_id
			JOIN media_types AS mt ON mt.id = m.media_type_id
		WHERE
			t.top_level_namespace_id = $1
			AND t.repository_id = $2
			AND t.name > $3
		ORDER BY
			t.name
		LIMIT $4`

	rows, err := s.db.QueryContext(ctx, q, r.NamespaceID, r.ID, lastName, limit)
	if err != nil {
		return nil, fmt.Errorf("finding tags detail with pagination: %w", err)
	}

	return scanFullTagsDetail(rows)
}

// TagsCountAfterName counts all tags of a given repository with name lexicographically after lastName. This is used
// exclusively for the GET /v2/<name>/tags/list API route, where pagination is done with a marker (lastName). Even if
// there is no tag with a name of lastName, the counted tags will always be those with a path lexicographically after
// lastName. This constraint exists to preserve the existing API behavior (when doing a filesystem walk based
// pagination).
func (s *repositoryStore) TagsCountAfterName(ctx context.Context, r *models.Repository, lastName string) (int, error) {
	defer metrics.InstrumentQuery("repository_tags_count_after_name")()
	q := `SELECT
			COUNT(id)
		FROM
			tags
		WHERE
			top_level_namespace_id = $1
			AND repository_id = $2
			AND name > $3`

	var count int
	if err := s.db.QueryRowContext(ctx, q, r.NamespaceID, r.ID, lastName).Scan(&count); err != nil {
		return count, fmt.Errorf("counting tags lexicographically after name: %w", err)
	}

	return count, nil
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
			encode(m.digest, 'hex') as digest,
			m.payload,
			mtc.media_type as configuration_media_type,
			encode(m.configuration_blob_digest, 'hex') as configuration_blob_digest,
			m.configuration_payload,
			m.non_conformant,
			m.non_distributable_layers,
			m.created_at
		FROM
			manifests AS m
			JOIN media_types AS mt ON mt.id = m.media_type_id
			LEFT JOIN media_types AS mtc ON mtc.id = m.configuration_media_type_id
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
			encode(m.digest, 'hex') as digest,
			m.payload,
			mtc.media_type as configuration_media_type,
			encode(m.configuration_blob_digest, 'hex') as configuration_blob_digest,
			m.configuration_payload,
			m.non_conformant,
			m.non_distributable_layers,
			m.created_at
		FROM
			manifests AS m
			JOIN media_types AS mt ON mt.id = m.media_type_id
			LEFT JOIN media_types AS mtc ON mtc.id = m.configuration_media_type_id
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

	if r.MigrationStatus == "" {
		r.MigrationStatus = migration.RepositoryStatusNative
	}

	q := `INSERT INTO repositories (top_level_namespace_id, name, path, parent_id, migration_status)
			VALUES ($1, $2, $3, $4, $5)
		RETURNING
			id, created_at`

	row := s.db.QueryRowContext(ctx, q, r.NamespaceID, r.Name, r.Path, r.ParentID, r.MigrationStatus)
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
func (s *repositoryStore) Size(ctx context.Context, r *models.Repository) (int64, error) {
	// Check the cached repository object for the size attribute first
	if r.Size != nil {
		return *r.Size, nil
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

	var size int64
	if err := s.db.QueryRowContext(ctx, q, r.NamespaceID, r.ID).Scan(&size); err != nil {
		return 0, fmt.Errorf("calculating repository size: %w", err)
	}

	// Update the size attribute for the cached repository object
	r.Size = &size
	s.cache.Set(ctx, r)

	return size, nil
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

// SizeWithDescendants returns the deduplicated size of a repository, including all descendants (if any). This is the
// sum of the size of all unique layers referenced by at least one tagged (directly or indirectly) manifest. No error is
// returned if the repository does not exist. It is the caller's responsibility to ensure it exists before calling this
// method and proceed accordingly if that matters.
// If this method, for this repository, failed with a statement timeout in the last 24h, then ErrSizeHasTimedOut is
// returned to prevent consecutive failures. The caller can then fall back to EstimatedSizeWithDescendants. This is a
// mitigation strategy for https://gitlab.com/gitlab-org/container-registry/-/issues/779.
func (s *repositoryStore) SizeWithDescendants(ctx context.Context, r *models.Repository) (int64, error) {
	if !r.IsTopLevel() {
		return s.nonTopLevelSizeWithDescendants(ctx, r)
	}

	if s.cache.HasSizeWithDescendantsTimedOut(ctx, r) {
		return 0, ErrSizeHasTimedOut
	}

	size, err := s.topLevelSizeWithDescendants(ctx, r)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.QueryCanceled {
			// flag query failure for target repository to avoid consecutive failures
			s.cache.SizeWithDescendantsTimedOut(ctx, r)
		}
	}

	return size, err
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
func (s *repositoryStore) EstimatedSizeWithDescendants(ctx context.Context, r *models.Repository) (size int64, err error) {
	if !r.IsTopLevel() {
		return 0, ErrOnlyRootEstimates
	}

	return s.estimateTopLevelSizeWithDescendants(ctx, r)
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

	if r.MigrationStatus == "" {
		r.MigrationStatus = migration.RepositoryStatusNative
	}

	// if not, proceed with creation attempt...
	// ON CONFLICT (path) DO UPDATE SET is a temporary measure until
	// https://gitlab.com/gitlab-org/container-registry/-/issues/625. If a repo record already exists for `path` but is
	// marked as soft deleted, we should undo the soft delete and proceed gracefully. Additionally, we also update the
	// `migration_status` as this method is also used by the Importer and in such case we want to switch the `native`
	// status to `(pre_)import_in_progress`.
	q := `INSERT INTO repositories (top_level_namespace_id, name, path, parent_id, migration_status)
			VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (path)
			DO UPDATE SET
				deleted_at = NULL, migration_status = $5
		RETURNING
			id, created_at, deleted_at` // deleted_at returned for test validation purposes only

	row := s.db.QueryRowContext(ctx, q, r.NamespaceID, r.Name, r.Path, r.ParentID, r.MigrationStatus)
	if err := row.Scan(&r.ID, &r.CreatedAt, &r.DeletedAt); err != nil {
		if err != sql.ErrNoRows {
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
			(name, path, parent_id, updated_at, migration_status, migration_error) = ($1, $2, $3, now(), $6, left($7, 255))
		WHERE
			top_level_namespace_id = $4
			AND id = $5
		RETURNING
			updated_at`

	row := s.db.QueryRowContext(ctx, q, r.Name, r.Path, r.ParentID, r.NamespaceID, r.ID, r.MigrationStatus, r.MigrationError)
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

// FindPagingatedRepositoriesForPath finds all repositories (up to `limit` repositories) that have a base path `path`.
// The results are ordered lexicographically by repository path and only begin from `lastPath`.
// Empty repositories (which do not have at least a tag) are ignored.
// Also, even if there are no repository with a `path` of `lastPath`, the returned
// repositories will still be those with a base path `path` and lexicographically after lastPath.
func (s *repositoryStore) FindPagingatedRepositoriesForPath(ctx context.Context, r *models.Repository, lastPath string, limit int) (models.Repositories, error) {
	// start from a path lexicographically before r.Path when no last path is availaible.
	// this improves the query performance as we will not need to filter from `r.path > ""` in the query below.
	if lastPath == "" {
		lastPath = lexicographicallyBeforePath(r.Path)
	}

	defer metrics.InstrumentQuery("repository_find_paginated_repositories_for_path")()
	q := `SELECT  
			id,
			top_level_namespace_id,
			name,
			path,
			parent_id,
			migration_status,
			migration_error,
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

	rows, err := s.db.QueryContext(ctx, q, r.Path, r.Path+"/%", lastPath, lexicographicallyNextPath(r.Path), r.NamespaceID, limit)
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

// lexicographicallyNextPath takes a path string and returns the next lexicographical path string.
// Empty paths (i.e a path of the form "") will result in an "a", paths with only "z" characters will append an a.
// All other paths will result in their next logical lexicographical variation (e.g gitlab-com => gitlab-con , gitlab-com.  => gitlab-com/)
// This function serves primarily as a helper for optimizing paginated db queries used in `FindPagingatedRepositoriesForPath.
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
// In the event where only "a's" exist in the path; the last 'a' carachter of the path is converted to a "z".
// This function serves primarily as a helper for optimizing paginated db queries used in `FindPagingatedRepositoriesForPath.
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
