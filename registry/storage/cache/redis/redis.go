package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/cache"
	"github.com/docker/distribution/registry/storage/cache/metrics"

	"github.com/opencontainers/go-digest"
	"github.com/redis/go-redis/v9"
)

// redisBlobStatService provides an implementation of
// BlobDescriptorCacheProvider based on redis. Blob descriptors are stored in
// two parts. The first provide fast access to repository membership through a
// redis set for each repo. The second is a redis hash keyed by the digest of
// the layer, providing path, length and mediatype information. There is also
// a per-repository redis hash of the blob descriptor, allowing override of
// data. This is currently used to override the mediatype on a per-repository
// basis.
//
// Note that there is no implied relationship between these two caches. The
// layer may exist in one, both or none and the code must be written this way.
type redisBlobDescriptorService struct {
	client redis.UniversalClient
}

// NewRedisBlobDescriptorCacheProvider returns a new redis-based
// BlobDescriptorCacheProvider using the provided redis connection pool.
func NewRedisBlobDescriptorCacheProvider(client redis.UniversalClient) cache.BlobDescriptorCacheProvider {
	return metrics.NewPrometheusCacheProvider(
		&redisBlobDescriptorService{
			client: client,
		},
		"cache_redis",
		"Number of seconds taken by redis",
	)
}

// RepositoryScoped returns the scoped cache.
func (rbds *redisBlobDescriptorService) RepositoryScoped(repo string) (distribution.BlobDescriptorService, error) {
	if _, err := reference.ParseNormalizedNamed(repo); err != nil {
		return nil, err
	}

	return &repositoryScopedRedisBlobDescriptorService{
		repo:     repo,
		upstream: rbds,
	}, nil
}

// Stat retrieves the descriptor data from the redis hash entry.
func (rbds *redisBlobDescriptorService) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	if err := dgst.Validate(); err != nil {
		return distribution.Descriptor{}, err
	}

	return rbds.stat(ctx, dgst)
}

func (rbds *redisBlobDescriptorService) Clear(ctx context.Context, dgst digest.Digest) error {
	if err := dgst.Validate(); err != nil {
		return err
	}

	reply, err := rbds.client.HDel(ctx, rbds.blobDescriptorHashKey(dgst), "digest", "size", "mediatype").Result()
	if err != nil {
		return err
	}

	if reply == 0 {
		return distribution.ErrBlobUnknown
	}

	return nil
}

// stat provides an internal stat call.
func (rbds *redisBlobDescriptorService) stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	reply, err := rbds.client.HMGet(ctx, rbds.blobDescriptorHashKey(dgst), "digest", "size", "mediatype").Result()
	if err != nil {
		return distribution.Descriptor{}, err
	}

	// NOTE(stevvooe): The "size" field used to be "length". We treat a
	// missing "size" field here as an unknown blob, which causes a cache
	// miss, effectively migrating the field.
	if len(reply) < 3 || reply[0] == nil || reply[1] == nil { // don't care if mediatype is nil
		return distribution.Descriptor{}, distribution.ErrBlobUnknown
	}

	var desc distribution.Descriptor

	dgst, err = digest.Parse(fmt.Sprintf("%v", reply[0]))
	if err != nil {
		return distribution.Descriptor{}, err
	}
	desc.Digest = dgst

	val, err := strconv.Atoi(fmt.Sprintf("%v", reply[1]))
	if err != nil {
		return distribution.Descriptor{}, err
	}
	desc.Size = int64(val)

	if reply[2] != nil {
		desc.MediaType = fmt.Sprintf("%v", reply[2])
	}

	return desc, nil
}

// SetDescriptor sets the descriptor data for the given digest using a redis
// hash. A hash is used here since we may store unrelated fields about a layer
// in the future.
func (rbds *redisBlobDescriptorService) SetDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.Descriptor) error {
	if err := dgst.Validate(); err != nil {
		return err
	}

	if err := cache.ValidateDescriptor(desc); err != nil {
		return err
	}

	return rbds.setDescriptor(ctx, dgst, desc)
}

func (rbds *redisBlobDescriptorService) setDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.Descriptor) error {
	if err := rbds.client.HMSet(ctx, rbds.blobDescriptorHashKey(dgst), map[string]interface{}{
		"digest": desc.Digest.String(),
		"size":   desc.Size,
	}).Err(); err != nil {
		return err
	}

	// Only set mediatype if not already set.
	if err := rbds.client.HSetNX(ctx, rbds.blobDescriptorHashKey(dgst), "mediatype", desc.MediaType).Err(); err != nil {
		return err
	}

	return nil
}

func (rbds *redisBlobDescriptorService) blobDescriptorHashKey(dgst digest.Digest) string {
	return "blobs::" + dgst.String()
}

type repositoryScopedRedisBlobDescriptorService struct {
	repo     string
	upstream *redisBlobDescriptorService
}

var _ distribution.BlobDescriptorService = &repositoryScopedRedisBlobDescriptorService{}

// Stat ensures that the digest is a member of the specified repository and
// forwards the descriptor request to the global blob store. If the media type
// differs for the repository, we override it.
func (rsrbds *repositoryScopedRedisBlobDescriptorService) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	if err := dgst.Validate(); err != nil {
		return distribution.Descriptor{}, err
	}

	// Check membership to repository first
	member, err := rsrbds.upstream.client.SIsMember(ctx, rsrbds.repositoryBlobSetKey(), dgst.String()).Result()
	if err != nil {
		return distribution.Descriptor{}, err
	}

	if !member {
		return distribution.Descriptor{}, distribution.ErrBlobUnknown
	}

	upstream, err := rsrbds.upstream.stat(ctx, dgst)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	// We allow a per repository mediatype, let's look it up here.
	mediatype, err := rsrbds.upstream.client.HGet(ctx, rsrbds.blobDescriptorHashKey(dgst), "mediatype").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return distribution.Descriptor{}, distribution.ErrBlobUnknown
		}

		return distribution.Descriptor{}, err
	}

	if mediatype != "" {
		upstream.MediaType = mediatype
	}

	return upstream, nil
}

// Clear removes the descriptor from the cache and forwards to the upstream descriptor store
func (rsrbds *repositoryScopedRedisBlobDescriptorService) Clear(ctx context.Context, dgst digest.Digest) error {
	if err := dgst.Validate(); err != nil {
		return err
	}

	// Check membership to repository first
	member, err := rsrbds.upstream.client.SIsMember(ctx, rsrbds.repositoryBlobSetKey(), dgst.String()).Result()
	if err != nil {
		return err
	}

	if !member {
		return distribution.ErrBlobUnknown
	}

	return rsrbds.upstream.Clear(ctx, dgst)
}

func (rsrbds *repositoryScopedRedisBlobDescriptorService) SetDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.Descriptor) error {
	if err := dgst.Validate(); err != nil {
		return err
	}

	if err := cache.ValidateDescriptor(desc); err != nil {
		return err
	}

	if dgst != desc.Digest {
		if dgst.Algorithm() == desc.Digest.Algorithm() {
			return fmt.Errorf("redis cache: digest for descriptors differ but algorithm does not: %q != %q", dgst, desc.Digest)
		}
	}

	return rsrbds.setDescriptor(ctx, dgst, desc)
}

func (rsrbds *repositoryScopedRedisBlobDescriptorService) setDescriptor(ctx context.Context, dgst digest.Digest, desc distribution.Descriptor) error {
	if err := rsrbds.upstream.client.SAdd(ctx, rsrbds.repositoryBlobSetKey(), dgst.String()).Err(); err != nil {
		return err
	}

	if err := rsrbds.upstream.setDescriptor(ctx, dgst, desc); err != nil {
		return err
	}

	// Override repository mediatype.
	if err := rsrbds.upstream.client.HSet(ctx, rsrbds.blobDescriptorHashKey(dgst), "mediatype", desc.MediaType).Err(); err != nil {
		return err
	}

	// Also set the values for the primary descriptor, if they differ by
	// algorithm (ie sha256 vs sha512).
	if desc.Digest != "" && dgst != desc.Digest && dgst.Algorithm() != desc.Digest.Algorithm() {
		if err := rsrbds.setDescriptor(ctx, desc.Digest, desc); err != nil {
			return err
		}
	}

	return nil
}

func (rsrbds *repositoryScopedRedisBlobDescriptorService) blobDescriptorHashKey(dgst digest.Digest) string {
	return "repository::" + rsrbds.repo + "::blobs::" + dgst.String()
}

func (rsrbds *repositoryScopedRedisBlobDescriptorService) repositoryBlobSetKey() string {
	return "repository::" + rsrbds.repo + "::blobs"
}
