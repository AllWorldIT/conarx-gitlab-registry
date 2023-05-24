package storage

import (
	"context"
	"errors"
	"sync"

	"github.com/docker/distribution"

	"github.com/docker/distribution/log"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

var _ distribution.TagService = &cachedTagStore{}

// cachedTagStore provides the same behavior as tagStore, however the All and
// Lookup operations are cached. For operations which require the registry to
// be in readonly mode this can be greatly beneficial. This struct is not
// intended for use in cases where the tags can be expected to be modified.
type cachedTagStore struct {
	*tagStore
	once sync.Once

	// tags is a list of tags by name and digest.
	tags map[string]digest.Digest
}

// newCachedTagStore returns a new cachedTagStore, using the provided tagStore
// as a cache populator.
func newCachedTagStore(ts *tagStore) *cachedTagStore {
	return &cachedTagStore{
		tagStore: ts,
		once:     sync.Once{},
		tags:     make(map[string]digest.Digest),
	}
}

// Prime pre populates the cache. This method is thread-safe and will only be
// ran once per instance of cachedTagStore.
func (cts *cachedTagStore) Prime(ctx context.Context) error {
	var retError error
	l := log.GetLogger(log.WithContext(ctx))

	cts.once.Do(func() {
		allTags, err := cts.tagStore.All(ctx)
		if err != nil {
			retError = err
			return
		}

		for _, tag := range allTags {
			tagLinkPathSpec := manifestTagCurrentPathSpec{
				name: cts.tagStore.repository.Named().Name(),
				tag:  tag,
			}

			tagLinkPath, err := pathFor(tagLinkPathSpec)
			if err != nil {
				retError = err
				return
			}
			tagDigest, err := cts.tagStore.blobStore.readlink(ctx, tagLinkPath)
			if err != nil {
				if errors.As(err, &storagedriver.PathNotFoundError{}) {
					continue
				}
				if errors.Is(err, digest.ErrDigestInvalidFormat) {
					// the tag link is corrupted, just log a warning and skip
					l.WithError(err).Warn("broken tag link, skipping")
					continue
				}

				retError = err
				return
			}

			cts.tags[tag] = tagDigest
		}
	})

	return retError
}

// All returns all tags.
func (cts *cachedTagStore) All(ctx context.Context) ([]string, error) {
	// Ensure cache is primed.
	if err := cts.Prime(ctx); err != nil {
		return nil, err
	}

	var i int
	tagNames := make([]string, len(cts.tags))

	for k := range cts.tags {
		tagNames[i] = k
		i++
	}

	return tagNames, nil
}

// Lookup recovers a list of tags which refer to this digest. When a manifest is deleted by
// digest, tag entries which point to it need to be recovered to avoid dangling tags.
func (cts *cachedTagStore) Lookup(ctx context.Context, desc distribution.Descriptor) ([]string, error) {
	// Ensure cache is primed.
	if err := cts.Prime(ctx); err != nil {
		return nil, err
	}

	var tags []string
	for tag, digest := range cts.tags {
		if digest == desc.Digest {
			tags = append(tags, tag)
		}
	}

	return tags, nil
}
