package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestCachedTagStoreAllHasSameResult0Tags(t *testing.T) {
	env := testTagStore(t)
	ctx := context.Background()

	ts, ok := env.ts.(*tagStore)
	require.True(t, ok, "the tagservice must a tagStore")

	cts := newCachedTagStore(ts)

	allTags, err := env.ts.All(ctx)

	_, ok = err.(distribution.ErrRepositoryUnknown)
	require.Truef(t, ok, "expected err to be of type distribution.ErrRepositoryUnknown  got %T", err)

	require.Emptyf(t, allTags, "expected 0 tags, got %d", len(allTags))

	cachedAllTags, err := cts.All(ctx)
	_, ok = err.(distribution.ErrRepositoryUnknown)
	require.Truef(t, ok, "expected err to be of type distribution.ErrRepositoryUnknown  got %T", err)

	require.Emptyf(t, cachedAllTags, "expected 0 tags, got %d", len(cachedAllTags))
}

func TestCachedTagStoreAllHasSameResult1Tag(t *testing.T) {
	testCachedTagStoreAllHasSameResult(t, 1)
}

func TestCachedTagStoreAllHasSameResult1200(t *testing.T) {
	testCachedTagStoreAllHasSameResult(t, 1200)
}

func testCachedTagStoreAllHasSameResult(t *testing.T, numTags int) {
	env := testTagStore(t)
	ctx := context.Background()

	// Populate tagStore with random tags.
	for i := 0; i < numTags; i++ {
		err := uploadTagWithRandomDigest(ctx, env.ts, strconv.Itoa(i))
		require.NoErrorf(t, err, "error populating tags: %v", err)
	}

	ts, ok := env.ts.(*tagStore)
	require.True(t, ok, "the tagservice must a tagStore")

	cts := newCachedTagStore(ts)

	allTags, err := env.ts.All(ctx)
	require.NoErrorf(t, err, "failed to retrieve all tags from tag store: %v", err)

	cachedAllTags, err := cts.All(ctx)
	require.NoErrorf(t, err, "failed to retrieve all tags from primed cache: %v", err)

	require.ElementsMatch(t, allTags, cachedAllTags)
}

func TestCachedTagStoreAllIgnoresCorruptTags(t *testing.T) {
	var (
		err error
		env = testTagStore(t)
		ctx = context.Background()
		tag = "foo"
	)

	// populate tagStore with `tag=foo`
	err = uploadTagWithRandomDigest(ctx, env.ts, tag)
	require.NoError(t, err)

	// retrieve all existing tags
	allTags, err := env.ts.All(ctx)
	require.NoError(t, err)
	require.Len(t, allTags, 1)
	require.Contains(t, allTags, tag)

	// obtain the tag link for the populated tag
	ts, ok := env.ts.(*tagStore)
	require.True(t, ok, "the tagservice must be a tagStore")
	tagLinkPathSpec := manifestTagCurrentPathSpec{
		name: ts.repository.Named().Name(),
		tag:  tag,
	}

	// corrupt the tag link
	err = corruptTagDigest(ctx, env.d, tagLinkPathSpec)
	require.NoError(t, err)

	// retrieve all existing tags - only keeping the validated tags (e.g keeping tags without broken links)
	cts := newCachedTagStore(ts)
	cachedAllTags, err := cts.All(ctx)
	// assert there were no errors when retrieving tags
	require.NoError(t, err)

	// assert the retrieved tag does not contain the broken tags
	require.NotContains(t, cachedAllTags, tag)
}

func TestCachedTagStoreLookupHasSameResults0Tags(t *testing.T) {
	testCachedTagStoreLookupHasSameResults(t, 0)
}

func TestCachedTagStoreLookupHasSameResults1Tag(t *testing.T) {
	testCachedTagStoreLookupHasSameResults(t, 1)
}

func TestCachedTagStoreLookupHasSameResults500Tags(t *testing.T) {
	testCachedTagStoreLookupHasSameResults(t, 500)
}

func testCachedTagStoreLookupHasSameResults(t *testing.T, numTags int) {
	env := testTagStore(t)
	ctx := context.Background()

	// Populate tagStore with random tags.
	for i := 0; i < numTags; i++ {
		err := uploadTagWithRandomDigest(ctx, env.ts, strconv.Itoa(i))
		require.NoErrorf(t, err, "error populating tags: %v", err)
	}

	ts, ok := env.ts.(*tagStore)
	require.True(t, ok, "the tagservice must a tagStore")

	cts := newCachedTagStore(ts)

	// Compare standard and primed cache.
	for i := 0; i < numTags; i++ {
		compareLookup(ctx, t, env.ts, cts, strconv.Itoa(i))
	}
}

func compareLookup(ctx context.Context, t *testing.T, ts distribution.TagService, cts *cachedTagStore, tag string) {
	desc, err := ts.Get(ctx, tag)
	require.NoErrorf(t, err, "failed to retrieve tag %s from tagStore: %v", tag, err)

	cachedDesc, err := cts.Get(ctx, tag)
	require.NoErrorf(t, err, "failed to retrieve tag %s from cachedTagStore: %v", tag, err)

	require.Equalf(t,
		desc.Digest, cachedDesc.Digest,
		"tagStore and cachedTagStore did not find the same digest for tag %s, tagStore found %v, cachedTagStore found %v",
		tag, desc.Digest, cachedDesc,
	)

	result, err := ts.Lookup(ctx, desc)
	require.NoErrorf(t, err, "failed to lookup tag %s from tagStore: %v", tag, err)

	cachedResult, err := cts.Lookup(ctx, cachedDesc)
	require.NoErrorf(t, err, "failed to lookup tag %s from cachedTagStore: %v", tag, err)

	require.ElementsMatch(t, result, cachedResult)
}

func uploadTagWithRandomDigest(ctx context.Context, ts distribution.TagService, tag string) error {
	bytes := make([]byte, 0)
	hash := sha256.New()
	hash.Write(bytes)
	dgst := "sha256:" + hex.EncodeToString(hash.Sum(nil))

	err := ts.Tag(ctx, tag, distribution.Descriptor{Digest: digest.Digest(dgst)})
	if err != nil {
		return err
	}
	return nil
}

func corruptTagDigest(ctx context.Context, d driver.StorageDriver, tagLinkPathSpec manifestTagCurrentPathSpec) error {
	tagLinkPath, err := pathFor(tagLinkPathSpec)
	if err != nil {
		return err
	}

	writer, err := d.Writer(ctx, tagLinkPath, false)
	if err != nil {
		return err
	}
	// replace the existing digest string with a corrupt digest
	_, err = writer.Write([]byte("corrupt"))
	if err != nil {
		return err
	}
	return nil
}
