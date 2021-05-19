package storage

import (
	"fmt"
	"io"
	"path"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	"github.com/docker/distribution/testutil"
	"github.com/docker/libtrust"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func createRegistry(tb testing.TB, driver driver.StorageDriver, options ...RegistryOption) distribution.Namespace {
	tb.Helper()
	ctx := context.Background()

	k, err := libtrust.GenerateECP256PrivateKey()
	require.NoError(tb, err)

	options = append([]RegistryOption{EnableDelete, Schema1SigningKey(k), EnableSchema1}, options...)
	registry, err := NewRegistry(ctx, driver, options...)
	require.NoError(tb, err)

	return registry
}

func makeRepository(tb testing.TB, registry distribution.Namespace, name string) distribution.Repository {
	tb.Helper()
	ctx := context.Background()

	// Initialize a dummy repository
	named, err := reference.WithName(name)
	require.NoError(tb, err)

	repo, err := registry.Repository(ctx, named)
	require.NoError(tb, err)

	return repo
}

func allManifests(tb testing.TB, manifestService distribution.ManifestService) map[digest.Digest]struct{} {
	tb.Helper()
	ctx := context.Background()

	allManSet := newSyncDigestSet()
	manifestEnumerator, ok := manifestService.(distribution.ManifestEnumerator)
	require.True(tb, ok)

	err := manifestEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
		allManSet.add(dgst)
		return nil
	})
	require.NoError(tb, err)

	return allManSet.members
}

func allBlobs(tb testing.TB, registry distribution.Namespace) map[digest.Digest]struct{} {
	tb.Helper()
	ctx := context.Background()
	blobService := registry.Blobs()
	allBlobsSet := newSyncDigestSet()

	err := blobService.Enumerate(ctx, func(desc distribution.Descriptor) error {
		allBlobsSet.add(desc.Digest)
		return nil
	})
	require.NoError(tb, err)

	return allBlobsSet.members
}

func TestNoDeletionNoEffect(t *testing.T) {
	inmemoryDriver := inmemory.New()
	registry := createRegistry(t, inmemoryDriver)
	repo := makeRepository(t, registry, "palailogos")

	testutil.UploadRandomImageList(t, registry, repo)

	before := allBlobs(t, registry)
	require.NotEmpty(t, before)

	// Run GC
	err := MarkAndSweep(context.Background(), inmemoryDriver, registry, GCOpts{
		DryRun:         false,
		RemoveUntagged: false,
	})
	require.NoError(t, err)

	after := allBlobs(t, registry)
	require.Equal(t, before, after)
}

func TestDeleteManifestIfTagNotFound(t *testing.T) {
	ctx := context.Background()
	inmemoryDriver := inmemory.New()

	registry := createRegistry(t, inmemoryDriver)
	repo := makeRepository(t, registry, "deletemanifests")
	manifestService, _ := repo.Manifests(ctx)

	// Create random layers
	randomLayers1, err := testutil.CreateRandomLayers(3)
	require.NoError(t, err)

	randomLayers2, err := testutil.CreateRandomLayers(3)
	require.NoError(t, err)

	// Upload all layers
	err = testutil.UploadBlobs(repo, randomLayers1)
	require.NoError(t, err)

	err = testutil.UploadBlobs(repo, randomLayers2)
	require.NoError(t, err)

	// Upload manifests
	_, err = testutil.UploadRandomSchema2Image(repo)
	require.NoError(t, err)

	_, err = testutil.UploadRandomSchema2Image(repo)
	require.NoError(t, err)

	manifestEnumerator, _ := manifestService.(distribution.ManifestEnumerator)
	err = manifestEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
		repo.Tags(ctx).Tag(ctx, "test", distribution.Descriptor{Digest: dgst})
		return nil
	})
	require.NoError(t, err)

	before1 := allBlobs(t, registry)
	require.NotEmpty(t, before1)

	before2 := allManifests(t, manifestService)
	require.NotEmpty(t, before2)

	// run GC with dry-run (should not remove anything)
	err = MarkAndSweep(context.Background(), inmemoryDriver, registry, GCOpts{
		DryRun:         true,
		RemoveUntagged: true,
	})
	require.NoError(t, err)

	afterDry1 := allBlobs(t, registry)
	afterDry2 := allManifests(t, manifestService)
	require.Equal(t, before1, afterDry1)
	require.Equal(t, before2, afterDry2)

	// Run GC, removes all but one tagged manifest and its blobs.
	err = MarkAndSweep(context.Background(), inmemoryDriver, registry, GCOpts{
		DryRun:         false,
		RemoveUntagged: true,
	})
	require.NoError(t, err)

	after1 := allBlobs(t, registry)
	after2 := allManifests(t, manifestService)

	// Should be only one tagged manifest by now.
	require.Len(t, after2, 1)

	// We should have removed some, but not all the blobs
	require.NotEmpty(t, after2)
	require.Less(t, len(after2), len(after1))
}

func TestGCWithMissingManifests(t *testing.T) {
	ctx := context.Background()
	d := inmemory.New()

	registry := createRegistry(t, d)
	repo := makeRepository(t, registry, "testrepo")
	_, err := testutil.UploadRandomSchema1Image(repo)
	require.NoError(t, err)

	// Simulate a missing _manifests directory
	revPath, err := pathFor(manifestRevisionsPathSpec{"testrepo"})
	require.NoError(t, err)

	_manifestsPath := path.Dir(revPath)
	err = d.Delete(ctx, _manifestsPath)
	require.NoError(t, err)

	err = MarkAndSweep(context.Background(), d, registry, GCOpts{
		DryRun:         false,
		RemoveUntagged: false,
	})
	require.NoError(t, err)

	blobs := allBlobs(t, registry)
	require.Empty(t, blobs)
}

func TestDeletionHasEffect(t *testing.T) {
	ctx := context.Background()
	inmemoryDriver := inmemory.New()

	registry := createRegistry(t, inmemoryDriver)
	repo := makeRepository(t, registry, "komnenos")
	manifests, _ := repo.Manifests(ctx)

	image1, err := testutil.UploadRandomSchema1Image(repo)
	require.NoError(t, err)

	image2, err := testutil.UploadRandomSchema1Image(repo)
	require.NoError(t, err)

	image3, err := testutil.UploadRandomSchema2Image(repo)
	require.NoError(t, err)

	err = manifests.Delete(ctx, image2.ManifestDigest)
	require.NoError(t, err)

	err = manifests.Delete(ctx, image3.ManifestDigest)
	require.NoError(t, err)

	// Run GC
	err = MarkAndSweep(context.Background(), inmemoryDriver, registry, GCOpts{
		DryRun:         false,
		RemoveUntagged: false,
	})
	require.NoError(t, err)

	blobs := allBlobs(t, registry)

	// check that the image1 manifest and all the layers are still in blobs
	require.Contains(t, blobs, image1.ManifestDigest)

	for layer := range image1.Layers {
		require.Contains(t, blobs, layer)
	}

	// check that image2 and image3 layers are not still around
	for layer := range image2.Layers {
		require.NotContains(t, blobs, layer)
	}

	for layer := range image3.Layers {
		require.NotContains(t, blobs, layer)
	}
}

func getAnyKey(digests map[digest.Digest]io.ReadSeeker) (d digest.Digest) {
	for d = range digests {
		break
	}
	return
}

func getKeys(digests map[digest.Digest]io.ReadSeeker) (ds []digest.Digest) {
	for d := range digests {
		ds = append(ds, d)
	}
	return
}

func TestDeletionWithSharedLayer(t *testing.T) {
	ctx := context.Background()
	inmemoryDriver := inmemory.New()

	registry := createRegistry(t, inmemoryDriver)
	repo := makeRepository(t, registry, "tzimiskes")

	// Create random layers
	randomLayers1, err := testutil.CreateRandomLayers(3)
	require.NoError(t, err)

	randomLayers2, err := testutil.CreateRandomLayers(3)
	require.NoError(t, err)

	// Upload all layers
	err = testutil.UploadBlobs(repo, randomLayers1)
	require.NoError(t, err)

	err = testutil.UploadBlobs(repo, randomLayers2)
	require.NoError(t, err)

	// Construct manifests
	manifest1, err := testutil.MakeSchema1Manifest(getKeys(randomLayers1))
	require.NoError(t, err)

	sharedKey := getAnyKey(randomLayers1)
	manifest2, err := testutil.MakeSchema2Manifest(repo, append(getKeys(randomLayers2), sharedKey))
	require.NoError(t, err)

	manifestService, err := testutil.MakeManifestService(repo)
	require.NoError(t, err)

	// Upload manifests
	_, err = manifestService.Put(ctx, manifest1)
	require.NoError(t, err)

	manifestDigest2, err := manifestService.Put(ctx, manifest2)
	require.NoError(t, err)

	// delete
	err = manifestService.Delete(ctx, manifestDigest2)
	require.NoError(t, err)

	// check that all of the layers in layer 1 are still there
	blobs := allBlobs(t, registry)
	for dgst := range randomLayers1 {
		require.Contains(t, blobs, dgst)
	}
}

func TestOrphanBlobDeleted(t *testing.T) {
	inmemoryDriver := inmemory.New()

	registry := createRegistry(t, inmemoryDriver)
	repo := makeRepository(t, registry, "michael_z_doukas")

	digests, err := testutil.CreateRandomLayers(1)
	require.NoError(t, err)

	err = testutil.UploadBlobs(repo, digests)
	require.NoError(t, err)

	// formality to create the necessary directories
	testutil.UploadRandomSchema2Image(repo)
	require.NoError(t, err)

	// Run GC
	err = MarkAndSweep(context.Background(), inmemoryDriver, registry, GCOpts{
		DryRun:         false,
		RemoveUntagged: false,
	})
	require.NoError(t, err)

	blobs := allBlobs(t, registry)

	// check that orphan blob layers are not still around
	for dgst := range digests {
		require.NotContains(t, blobs, dgst)
	}
}

// TestGarbageCollectAfterLastTagRemoved was added to validate the scenario in which the last tag from the repository
// is removed which in turn removes the <repository>/_manifests/tags folder. This was throwing a distribution.ErrRepositoryUnknown
// error that is now being captured in garbagecollect.MarkAndSweep.
// https://gitlab.com/gitlab-org/gitlab/issues/28201
func TestGarbageCollectAfterLastTagRemoved(t *testing.T) {
	ctx := context.Background()
	inmemoryDriver := inmemory.New()

	registry := createRegistry(t, inmemoryDriver)
	repo := makeRepository(t, registry, "testgarbagecollectafterlasttagremoved")

	manifestService, err := repo.Manifests(ctx)
	require.NoError(t, err)

	// Setup for tests
	randomLayers1, err := testutil.CreateRandomLayers(3)
	require.NoError(t, err)

	err = testutil.UploadBlobs(repo, randomLayers1)
	require.NoError(t, err)

	manifest1, err := testutil.MakeSchema1Manifest(getKeys(randomLayers1))
	require.NoError(t, err)

	_, err = manifestService.Put(ctx, manifest1)
	require.NoError(t, err)

	manifestEnumerator, _ := manifestService.(distribution.ManifestEnumerator)
	err = manifestEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
		repo.Tags(ctx).Tag(ctx, "testTag", distribution.Descriptor{Digest: dgst})
		return nil
	})
	require.NoError(t, err)
	// -- End setup

	// Delete the repository's _manifests/tags path
	tagsPath, err := pathFor(manifestTagsPathSpec{"testgarbagecollectafterlasttagremoved"})
	require.NoError(t, err)

	t.Logf(tagsPath)
	err = inmemoryDriver.Delete(ctx, tagsPath)
	require.NoError(t, err)

	// Run garbage collection with tags folder removed to validate error handling
	err = MarkAndSweep(context.Background(), inmemoryDriver, registry, GCOpts{
		DryRun:         false,
		RemoveUntagged: true,
	})
	require.NoError(t, err)

	// Assert that no blobs or manifests were left behind (because the only manifest has no tags path)
	afterBlobs := allBlobs(t, registry)
	require.Empty(t, afterBlobs)

	afterManifests := allManifests(t, manifestService)
	require.Empty(t, afterManifests)
}

func TestGarbageCollectManifestListReferences(t *testing.T) {
	ctx := context.Background()
	inmemoryDriver := inmemory.New()

	registry := createRegistry(t, inmemoryDriver)
	repo := makeRepository(t, registry, "testgarbagecollectkeeptaggedmanifestlistreferences")

	// Create a manifest list and tag only the manifest list.
	taggedList := testutil.UploadRandomImageList(t, registry, repo)

	err := repo.Tags(ctx).Tag(ctx, "manifestlist-latest", distribution.Descriptor{Digest: taggedList.ManifestDigest})
	require.NoError(t, err)

	untaggedList := testutil.UploadRandomImageList(t, registry, repo)

	// Create a manifest list and tag only the manifests refernced by the list.
	untaggedListWithTaggedImages := testutil.UploadRandomImageList(t, registry, repo)
	for i, img := range untaggedListWithTaggedImages.Images {
		err := repo.Tags(ctx).Tag(ctx, fmt.Sprintf("tagged-img-%d", i), distribution.Descriptor{Digest: img.ManifestDigest})
		require.NoError(t, err)
	}

	// Run garbage collection.
	err = MarkAndSweep(context.Background(), inmemoryDriver, registry, GCOpts{
		DryRun:         false,
		RemoveUntagged: true,
	})
	require.NoError(t, err)

	manifestService, err := repo.Manifests(ctx)
	require.NoError(t, err)

	// Tagged list and its manifests and their blobs should still exist.
	ok, err := manifestService.Exists(ctx, taggedList.ManifestDigest)
	require.NoError(t, err)
	require.True(t, ok)

	for _, img := range taggedList.Images {
		_, err := manifestService.Exists(ctx, img.ManifestDigest)
		require.NoError(t, err)
		require.True(t, ok)
	}

	// Untagged list and its manifests should have been removed.
	ok, err = manifestService.Exists(ctx, untaggedList.ManifestDigest)
	require.NoError(t, err)
	require.False(t, ok)

	for _, img := range untaggedList.Images {
		ok, err := manifestService.Exists(ctx, img.ManifestDigest)
		require.NoError(t, err)
		require.False(t, ok)
	}

	// Untagged list with tagged images should have been removed, while its
	// manifests should still exist.
	ok, err = manifestService.Exists(ctx, untaggedListWithTaggedImages.ManifestDigest)
	require.NoError(t, err)
	require.False(t, ok)

	for _, img := range untaggedListWithTaggedImages.Images {
		ok, err := manifestService.Exists(ctx, img.ManifestDigest)
		require.NoError(t, err)
		require.True(t, ok)
	}
}
