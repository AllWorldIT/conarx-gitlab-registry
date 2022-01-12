package storage

import (
	"context"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"crypto/sha256"
	"encoding/hex"

	"github.com/docker/distribution"
	"github.com/opencontainers/go-digest"
)

func TestCachedTagStoreAllHasSameResult0Tags(t *testing.T) {
	env := testTagStore(t)
	ctx := context.Background()

	ts, ok := env.ts.(*tagStore)
	if !ok {
		t.Fatalf("the tagservice must a tagStore")
	}

	cts := newCachedTagStore(ts)

	allTags, err := env.ts.All(ctx)

	_, ok = err.(distribution.ErrRepositoryUnknown)
	if !ok {
		t.Fatalf("expected err to be of type distribution.ErrRepositoryUnknown  got %T", err)
	}

	if len(allTags) != 0 {
		t.Fatalf("expected 0 tags, got %d", len(allTags))
	}

	cachedAllTags, err := cts.All(ctx)
	_, ok = err.(distribution.ErrRepositoryUnknown)
	if !ok {
		t.Fatalf("expected err to be of type distribution.ErrRepositoryUnknown  got %T", err)
	}

	if len(cachedAllTags) != 0 {
		t.Fatalf("expected 0 tags, got %d", len(cachedAllTags))
	}
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
		if err := uploadTagWithRandomDigest(ctx, env.ts, strconv.Itoa(i)); err != nil {
			t.Fatalf("error populating tags: %v", err)
		}
	}

	ts, ok := env.ts.(*tagStore)
	if !ok {
		t.Fatalf("the tagservice must a tagStore")
	}

	cts := newCachedTagStore(ts)

	allTags, err := env.ts.All(ctx)
	if err != nil {
		t.Fatalf("failed to retrieve all tags from tag store: %v", err)
	}
	sort.Strings(allTags)

	cachedAllTags, err := cts.All(ctx)
	if err != nil {
		t.Fatalf("failed to retrieve all tags from primed cache: %v", err)
	}
	sort.Strings(cachedAllTags)

	if !reflect.DeepEqual(allTags, cachedAllTags) {
		t.Fatalf("expected:\n\t%+v\n, got:\n\t%+v", allTags, cachedAllTags)
	}
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
		if err := uploadTagWithRandomDigest(ctx, env.ts, strconv.Itoa(i)); err != nil {
			t.Fatalf("error populating tags: %v", err)
		}
	}

	ts, ok := env.ts.(*tagStore)
	if !ok {
		t.Fatalf("the tagservice must a tagStore")
	}

	cts := newCachedTagStore(ts)

	// Compare standard and primed cache.
	for i := 0; i < numTags; i++ {
		compareLookup(ctx, t, env.ts, cts, strconv.Itoa(i))
	}
}

func compareLookup(ctx context.Context, t *testing.T, ts distribution.TagService, cts *cachedTagStore, tag string) {
	desc, err := ts.Get(ctx, tag)
	if err != nil {
		t.Fatalf("failed to retrieve tag %s from tagStore: %v", tag, err)
	}

	cachedDesc, err := cts.Get(ctx, tag)
	if err != nil {
		t.Fatalf("failed to retrieve tag %s from cachedTagStore: %v", tag, err)
	}

	if desc.Digest != cachedDesc.Digest {
		t.Fatalf("tagStore and cachedTagStore did not find the same digest for tag %s, tagStore found %v, cachedTagStore found %v",
			tag, desc.Digest, cachedDesc)
	}

	result, err := ts.Lookup(ctx, desc)
	if err != nil {
		t.Fatalf("failed to lookup tag %s from tagStore: %v", tag, err)
	}

	cachedResult, err := cts.Lookup(ctx, cachedDesc)
	if err != nil {
		t.Fatalf("failed to lookup tag %s from cachedTagStore: %v", tag, err)
	}

	sort.Strings(result)
	sort.Strings(cachedResult)

	if !reflect.DeepEqual(result, cachedResult) {
		t.Fatalf("expected:\n\t%+v\n, got:\n\t%+v", result, cachedResult)
	}
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
