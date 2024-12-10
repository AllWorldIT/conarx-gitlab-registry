package storage

import (
	"context"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type tagsTestEnv struct {
	d   driver.StorageDriver
	ts  distribution.TagService
	ctx context.Context
}

func testTagStore(t *testing.T) *tagsTestEnv {
	ctx := context.Background()
	d := inmemory.New()
	reg, err := NewRegistry(ctx, d)
	require.NoError(t, err)

	repoRef, _ := reference.WithName("a/b")
	repo, err := reg.Repository(ctx, repoRef)
	require.NoError(t, err)

	return &tagsTestEnv{
		d:   d,
		ctx: ctx,
		ts:  repo.Tags(ctx),
	}
}

func TestTagStoreTag(t *testing.T) {
	env := testTagStore(t)
	tags := env.ts
	ctx := env.ctx

	d := distribution.Descriptor{}
	err := tags.Tag(ctx, "latest", d)
	// nolint: testifylint // require-error
	assert.Error(t, err, "unexpected error putting malformed descriptor")

	d.Digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	err = tags.Tag(ctx, "latest", d)
	// nolint: testifylint // require-error
	assert.NoError(t, err)

	d1, err := tags.Get(ctx, "latest")
	// nolint: testifylint // require-error
	assert.NoError(t, err)

	assert.Equal(t, d.Digest, d1.Digest, "put and get digest differ")

	// Overwrite existing
	d.Digest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	err = tags.Tag(ctx, "latest", d)
	// nolint: testifylint // require-error
	assert.NoError(t, err)

	d1, err = tags.Get(ctx, "latest")
	// nolint: testifylint // require-error
	assert.NoError(t, err)

	assert.Equal(t, d.Digest, d1.Digest, "put and get digest differ")
}

func TestTagStoreUnTag(t *testing.T) {
	env := testTagStore(t)
	tags := env.ts
	ctx := env.ctx
	desc := distribution.Descriptor{Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}

	err := tags.Untag(ctx, "latest")
	// nolint: testifylint // require-error
	assert.Error(t, err, "expected error untagging non-existing tag")

	err = tags.Tag(ctx, "latest", desc)
	// nolint: testifylint // require-error
	assert.NoError(t, err)

	err = tags.Untag(ctx, "latest")
	// nolint: testifylint // require-error
	assert.NoError(t, err)

	errExpect := distribution.ErrTagUnknown{Tag: "latest"}.Error()
	_, err = tags.Get(ctx, "latest")
	// nolint: testifylint // require-error
	assert.EqualError(t, err, errExpect, "expected error getting untagged tag")
}

func TestTagStoreAll(t *testing.T) {
	env := testTagStore(t)
	tagStore := env.ts
	ctx := env.ctx

	alpha := "abcdefghijklmnopqrstuvwxyz"
	for i := 0; i < len(alpha); i++ {
		tag := alpha[i]
		desc := distribution.Descriptor{Digest: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}
		err := tagStore.Tag(ctx, string(tag), desc)
		require.NoError(t, err)
	}

	all, err := tagStore.All(ctx)
	require.NoError(t, err)
	require.Len(t, all, len(alpha), "unexpected count returned from enumerate")

	for i, c := range all {
		require.Equal(t, string(alpha[i]), c, "unexpected tag in enumerate")
	}

	removed := "a"
	err = tagStore.Untag(ctx, removed)
	require.NoError(t, err)

	all, err = tagStore.All(ctx)
	require.NoError(t, err)
	assert.NotContains(t, all, removed, "unexpected tag in enumerate")
}

func TestTagLookup(t *testing.T) {
	env := testTagStore(t)
	tagStore := env.ts
	ctx := env.ctx

	descA := distribution.Descriptor{Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	desc0 := distribution.Descriptor{Digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"}

	tags, err := tagStore.Lookup(ctx, descA)
	require.NoError(t, err)
	require.Empty(t, tags, "lookup returned > 0 tags from empty store")

	err = tagStore.Tag(ctx, "a", descA)
	require.NoError(t, err)

	err = tagStore.Tag(ctx, "b", descA)
	require.NoError(t, err)

	err = tagStore.Tag(ctx, "0", desc0)
	require.NoError(t, err)

	err = tagStore.Tag(ctx, "1", desc0)
	require.NoError(t, err)

	tags, err = tagStore.Lookup(ctx, descA)
	require.NoError(t, err)

	require.Len(t, tags, 2, "lookup of descA returned unexpected number of tags")

	tags, err = tagStore.Lookup(ctx, desc0)
	require.NoError(t, err)

	require.Len(t, tags, 2, "lookup of descB returned unexpected number of tags")
}
