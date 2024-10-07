package datastore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/datastore/models"
	iredis "github.com/docker/distribution/registry/internal/redis"
	"github.com/opencontainers/go-digest"
	"github.com/redis/go-redis/v9"
)

var (
	errLeaseNotFound      = errors.New("repository lease not found")
	errLeaseUpsertIsEmpty = errors.New("repository lease to be upserted can not be empty")
)

const (
	// renameLeaseTTL defines how long a rename lease should last in Redis
	renameLeaseTTL = 60 * time.Second
)

// enforces repositoryLeaseStore implements RepositoryLeaseStore
var _ RepositoryLeaseStore = &repositoryLeaseStore{}

// RepositoryLeaseStoreOption allows customizing a repositoryLeaseStore with additional options.
type RepositoryLeaseStoreOption func(*repositoryLeaseStore)

// WithRepositoryLeaseCache instantiates the repositoryStore with a cache for lease management
func WithRepositoryLeaseCache(cache RepositoryLeaseCache) RepositoryLeaseStoreOption {
	return func(rlstore *repositoryLeaseStore) {
		rlstore.cache = cache
	}
}

// RepositoryLeaseStore is the interface that a repository store should conform to.
type RepositoryLeaseStore interface {
	// FindRenameByPath searches the underlying store for the rename lease.
	FindRenameByPath(ctx context.Context, path string) (*models.RepositoryLease, error)
	// GetTTL returns the amount of time left till a lease expires.
	GetTTL(ctx context.Context, lease *models.RepositoryLease) (time.Duration, error)
	// UpsertRename creates a new repository lease (or updates an existing one) with ttl = `renameLeaseTTL`.
	UpsertRename(ctx context.Context, r *models.RepositoryLease) (*models.RepositoryLease, error)
	// Destroy removes a repository lease from the cache.
	Destroy(ctx context.Context, r *models.RepositoryLease) error
}

// repositoryLeaseStore is the concrete implementation of a RepositoryLeaseStore.
type repositoryLeaseStore struct {
	// this struct can be extended to have a db field
	// which can be used as a drop in replacement for the cache
	// or to work in tandem with the cache
	cache RepositoryLeaseCache
}

// NewRepositoryLeaseStore builds a new repositoryLeaseStore.
func NewRepositoryLeaseStore(opts ...RepositoryLeaseStoreOption) *repositoryLeaseStore {
	rlStore := &repositoryLeaseStore{cache: &noOpRepositoryLeaseCache{}}

	for _, o := range opts {
		o(rlStore)
	}

	return rlStore
}

// RepositoryLeaseCache is a cache for *models.RepositoryLease objects.
type RepositoryLeaseCache interface {
	Get(ctx context.Context, path string, leaseType models.LeaseType) (*models.RepositoryLease, error)
	Set(ctx context.Context, lease *models.RepositoryLease, ttl time.Duration) error
	Invalidate(ctx context.Context, path string) error
	TTL(ctx context.Context, lease *models.RepositoryLease) (time.Duration, error)
}

// noOpRepositoryLeaseCache satisfies the RepositoryLeaseCache, but does not do anything.
// Useful as a default and for testing.
type noOpRepositoryLeaseCache struct{}

// NewNoOpRepositoryLeaseCache creates a new non-operational cache for a repository lease object.
// This implementation does nothing and returns nothing for all its methods.
func NewNoOpRepositoryLeaseCache() *noOpRepositoryLeaseCache {
	return &noOpRepositoryLeaseCache{}
}

func (n *noOpRepositoryLeaseCache) Get(ctx context.Context, path string, leaseType models.LeaseType) (*models.RepositoryLease, error) {
	return nil, nil
}

func (n *noOpRepositoryLeaseCache) Set(ctx context.Context, lease *models.RepositoryLease, ttl time.Duration) error {
	return nil
}

func (n *noOpRepositoryLeaseCache) TTL(ctx context.Context, lease *models.RepositoryLease) (time.Duration, error) {
	return 0, nil
}

func (n *noOpRepositoryLeaseCache) Invalidate(ctx context.Context, path string) error {
	return nil
}

// centralRepositoryLeaseCache is the interface for the centralized repository lease cache backed by Redis.
type centralRepositoryLeaseCache struct {
	cache *iredis.Cache
}

// NewCentralRepositoryLeaseCache creates an interface for the centralized repository object cache backed by Redis.
func NewCentralRepositoryLeaseCache(cache *iredis.Cache) *centralRepositoryLeaseCache {
	return &centralRepositoryLeaseCache{cache}
}

// key generates a valid Redis key string for a given repository lease object. The used key format is described in
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/redis-dev-guidelines.md#key-format.
func (c *centralRepositoryLeaseCache) key(path string) string {
	nsPrefix := strings.Split(path, "/")[0]
	hex := digest.FromString(path).Hex()
	return fmt.Sprintf("registry:api:{repository-lease:%s:%s}", nsPrefix, hex)
}

// Get a repository lease from the cache.
func (c *centralRepositoryLeaseCache) Get(ctx context.Context, path string, leaseType models.LeaseType) (*models.RepositoryLease, error) {
	l := log.GetLogger(log.WithContext(ctx))

	getCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	var lease models.RepositoryLease
	if err := c.cache.UnmarshalGet(getCtx, c.key(path), &lease); err != nil {
		l.WithError(err).Warn("repository lease cache: failed to read lease from cache")
		// redis.Nil is returned when the key is not found in Redis
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	if lease.Type != leaseType {
		l.Warn("failed to find the repository lease matching the lease type")
		return nil, nil
	}

	return &lease, nil
}

// Set a repository lease in the cache.
func (c *centralRepositoryLeaseCache) Set(ctx context.Context, lease *models.RepositoryLease, ttl time.Duration) error {
	setCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	if err := c.cache.MarshalSet(setCtx, c.key(lease.Path), lease, iredis.WithTTL(ttl)); err != nil {
		return fmt.Errorf("failed to write repository lease to cache: %w", err)
	}
	return nil
}

// TTL gets the object's TTL from the cache.
func (c *centralRepositoryLeaseCache) TTL(ctx context.Context, lease *models.RepositoryLease) (time.Duration, error) {
	l := log.GetLogger(log.WithContext(ctx))

	// find any existing ttl for the lease path
	var cachedLease models.RepositoryLease
	ttl, err := c.cache.UnmarshalGetWithTTL(ctx, c.key(lease.Path), &cachedLease)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, errLeaseNotFound
		}
		return ttl, fmt.Errorf("failed to read lease TTL from cache: %w", err)
	}

	// verify the lease was granted to the same repository requesting the TTL
	if cachedLease.GrantedTo != lease.GrantedTo {
		l.Warn("the lease retrieved has a different grantor from the one requested")
		return 0, nil
	}

	return ttl, nil
}

// Invalidate the lease for a given path in the cache.
func (c *centralRepositoryLeaseCache) Invalidate(ctx context.Context, path string) error {
	invalCtx, cancel := context.WithTimeout(ctx, cacheOpTimeout)
	defer cancel()

	if err := c.cache.Delete(invalCtx, c.key(path)); err != nil {
		detail := "failed to invalidate repository lease in cache for leased path: " + path
		log.GetLogger(log.WithContext(ctx)).WithError(err).Warn(detail)
		return fmt.Errorf("failed to invalidate repository lease: %w", err)
	}
	return nil
}

// FindRenameByPath returns a rename lease object if it exists.
func (s *repositoryLeaseStore) FindRenameByPath(ctx context.Context, path string) (*models.RepositoryLease, error) {
	rl, err := s.cache.Get(ctx, path, models.RenameLease)
	if err != nil {
		return nil, fmt.Errorf("finding rename lease: %w", err)
	}
	return rl, nil
}

// UpsertRename creates (or updates the ttl of) a rename lease object with ttl = `renameLeaseTTL`
func (s *repositoryLeaseStore) UpsertRename(ctx context.Context, rl *models.RepositoryLease) (*models.RepositoryLease, error) {
	if rl == nil {
		return nil, errLeaseUpsertIsEmpty
	}

	rl.Type = models.RenameLease
	err := s.cache.Set(ctx, rl, renameLeaseTTL)
	if err != nil {
		log.GetLogger(log.WithContext(ctx)).Warn("upserting rename lease failed")
		return nil, fmt.Errorf("upserting rename lease: %w", err)
	}
	return rl, nil
}

// GetTTL returns the TTL of an existing lease
func (s *repositoryLeaseStore) GetTTL(ctx context.Context, lease *models.RepositoryLease) (time.Duration, error) {
	return s.cache.TTL(ctx, lease)
}

// Destroy invalidates a lease object
func (s *repositoryLeaseStore) Destroy(ctx context.Context, lease *models.RepositoryLease) error {
	return s.cache.Invalidate(ctx, lease.Path)
}
