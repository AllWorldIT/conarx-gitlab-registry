package storage

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/cache/memory"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	"github.com/docker/distribution/testutil"
	"github.com/docker/libtrust"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

type manifestStoreTestEnv struct {
	ctx        context.Context
	driver     driver.StorageDriver
	registry   distribution.Namespace
	repository distribution.Repository
	name       reference.Named
	tag        string
}

func newManifestStoreTestEnv(t *testing.T, name reference.Named, options ...RegistryOption) *manifestStoreTestEnv {
	ctx := context.Background()
	driverInMemory := inmemory.New()
	tag := "thetag"
	registry, err := NewRegistry(ctx, driverInMemory, options...)
	require.NoError(t, err, "error creating registry")

	repo, err := registry.Repository(ctx, name)
	require.NoError(t, err, "unexpected error getting repo")

	return &manifestStoreTestEnv{
		ctx:        ctx,
		driver:     driverInMemory,
		registry:   registry,
		repository: repo,
		name:       name,
		tag:        tag,
	}
}

func TestManifestStorage(t *testing.T) {
	k, err := libtrust.GenerateECP256PrivateKey()
	require.NoError(t, err)
	testManifestStorageImpl(t, true, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), EnableDelete, EnableRedirect, Schema1SigningKey(k), EnableSchema1)
}

func TestManifestStorageV1Unsupported(t *testing.T) {
	k, err := libtrust.GenerateECP256PrivateKey()
	require.NoError(t, err)
	testManifestStorageImpl(t, false, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), EnableDelete, EnableRedirect, Schema1SigningKey(k))
}

func testManifestStorageImpl(t *testing.T, schema1Enabled bool, options ...RegistryOption) {
	repoName, _ := reference.WithName("foo/bar")
	env := newManifestStoreTestEnv(t, repoName, options...)
	ctx := context.Background()
	ms, err := env.repository.Manifests(ctx)
	require.NoError(t, err)

	m := schema1.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 1,
		},
		Name: env.name.Name(),
		Tag:  env.tag,
	}

	// Build up some test layers and add them to the manifest, saving the
	// readseekers for upload later.
	testLayers := make(map[digest.Digest]io.ReadSeeker)
	for i := 0; i < 2; i++ {
		rs, dgst, err := testutil.CreateRandomTarFile(testutil.MustChaChaSeed(t))
		require.NoError(t, err, "unexpected error generating test layer file")

		testLayers[dgst] = rs
		m.FSLayers = append(m.FSLayers, schema1.FSLayer{
			BlobSum: dgst,
		})
		m.History = append(m.History, schema1.History{
			V1Compatibility: "",
		})
	}

	pk, err := libtrust.GenerateECP256PrivateKey()
	require.NoError(t, err, "unexpected error generating private key")

	sm, merr := schema1.Sign(&m, pk)
	require.NoError(t, merr, "error signing manifest")

	_, err = ms.Put(ctx, sm)
	require.Error(t, err, "expected errors putting manifest with full verification")

	// If schema1 is not enabled, do a short version of this test, just checking
	// if we get the right error when we Put
	if !schema1Enabled {
		require.ErrorIs(t, err, distribution.ErrSchemaV1Unsupported, "got the wrong error when schema1 is disabled")
		return
	}

	switch err := err.(type) {
	case distribution.ErrManifestVerification:
		require.Len(t, err, 2, "expected 2 verification errors")

		for _, err := range err {
			_, ok := err.(distribution.ErrManifestBlobUnknown)
			require.True(t, ok, "unexpected error type")
		}
	default:
		require.FailNow(t, fmt.Sprintf("unexpected error verifying manifest: %v", err))
	}

	// Now, upload the layers that were missing!
	for dgst, rs := range testLayers {
		wr, err := env.repository.Blobs(env.ctx).Create(env.ctx)
		require.NoError(t, err, "unexpected error creating test upload")

		_, err = io.Copy(wr, rs)
		require.NoError(t, err, "unexpected error copying to upload")

		_, err = wr.Commit(env.ctx, distribution.Descriptor{Digest: dgst})
		require.NoError(t, err, "unexpected error finishing upload")
	}

	var manifestDigest digest.Digest
	manifestDigest, err = ms.Put(ctx, sm)
	require.NoError(t, err, "unexpected error putting manifest")

	exists, err := ms.Exists(ctx, manifestDigest)
	require.NoError(t, err, "unexpected error checking manifest existence")

	require.True(t, exists, "manifest should exist")

	fromStore, err := ms.Get(ctx, manifestDigest)
	require.NoError(t, err, "unexpected error fetching manifest")

	fetchedManifest, ok := fromStore.(*schema1.SignedManifest)
	require.True(t, ok, "unexpected manifest type from signedstore")

	require.Equal(t, sm.Canonical, fetchedManifest.Canonical, "fetched payload does not match original payload")

	_, pl, err := fetchedManifest.Payload()
	require.NoError(t, err, "error getting payload")

	fetchedJWS, err := libtrust.ParsePrettySignature(pl, "signatures")
	require.NoError(t, err, "unexpected error parsing jws")

	payload, err := fetchedJWS.Payload()
	require.NoError(t, err, "unexpected error extracting payload")

	// Now that we have a payload, take a moment to check that the manifest is
	// return by the payload digest.

	dgst := digest.FromBytes(payload)
	exists, err = ms.Exists(ctx, dgst)
	require.NoError(t, err, "error checking manifest existence by digest")

	require.True(t, exists, "manifest %s should exist", dgst)

	fetchedByDigest, err := ms.Get(ctx, dgst)
	require.NoError(t, err, "unexpected error fetching manifest by digest")

	byDigestManifest, ok := fetchedByDigest.(*schema1.SignedManifest)
	require.True(t, ok, "unexpected manifest type from signedstore")

	require.Equal(t, fetchedManifest.Canonical, byDigestManifest.Canonical, "fetched manifest not equal")

	sigs, err := fetchedJWS.Signatures()
	require.NoError(t, err, "unable to extract signatures")

	require.Len(t, sigs, 1, "unexpected number of signatures")

	// Now, push the same manifest with a different key
	pk2, err := libtrust.GenerateECP256PrivateKey()
	require.NoError(t, err, "unexpected error generating private key")

	sm2, err := schema1.Sign(&m, pk2)
	require.NoError(t, err, "unexpected error signing manifest")
	_, pl, err = sm2.Payload()
	require.NoError(t, err, "error getting payload")

	jws2, err := libtrust.ParsePrettySignature(pl, "signatures")
	require.NoError(t, err, "error parsing signature")

	sigs2, err := jws2.Signatures()
	require.NoError(t, err, "unable to extract signatures")

	require.Len(t, sigs2, 1, "unexpected number of signatures")

	manifestDigest, err = ms.Put(ctx, sm2)
	require.NoError(t, err, "unexpected error putting manifest")

	fromStore, err = ms.Get(ctx, manifestDigest)
	require.NoError(t, err, "unexpected error fetching manifest")

	fetched, ok := fromStore.(*schema1.SignedManifest)
	require.True(t, ok, "unexpected type from signed manifeststore")

	_, err = schema1.Verify(fetched)
	require.NoError(t, err, "unexpected error verifying manifest")

	_, pl, err = fetched.Payload()
	require.NoError(t, err, "error getting payload")

	receivedJWS, err := libtrust.ParsePrettySignature(pl, "signatures")
	require.NoError(t, err, "unexpected error parsing jws")

	receivedPayload, err := receivedJWS.Payload()
	require.NoError(t, err, "unexpected error extracting received payload")

	require.Equal(t, payload, receivedPayload, "payloads are not equal")

	// Test deleting manifests
	err = ms.Delete(ctx, dgst)
	require.NoError(t, err, "unexpected an error deleting manifest by digest")

	exists, err = ms.Exists(ctx, dgst)
	require.NoError(t, err, "error querying manifest existence")
	require.False(t, exists, "deleted manifest should not exist")

	deletedManifest, err := ms.Get(ctx, dgst)
	require.Error(t, err, "unexpected success getting deleted manifest")
	require.IsType(t, distribution.ErrManifestUnknownRevision{}, err, "unexpected error getting deleted manifest")

	require.Nil(t, deletedManifest, "deleted manifest get returned non-nil")

	// Re-upload should restore manifest to a good state
	_, err = ms.Put(ctx, sm)
	require.NoError(t, err, "error re-uploading deleted manifest")

	exists, err = ms.Exists(ctx, dgst)
	require.NoError(t, err, "error querying manifest existence")
	require.True(t, exists, "restored manifest should exist")

	deletedManifest, err = ms.Get(ctx, dgst)
	require.NoError(t, err, "unexpected error getting manifest")
	require.NotNil(t, deletedManifest, "deleted manifest get returned non-nil")

	r, err := NewRegistry(ctx, env.driver, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), EnableRedirect)
	require.NoError(t, err, "error creating registry")
	repo, err := r.Repository(ctx, env.name)
	require.NoError(t, err, "unexpected error getting repo")
	ms, err = repo.Manifests(ctx)
	require.NoError(t, err)
	err = ms.Delete(ctx, dgst)
	require.Error(t, err, "unexpected success deleting while disabled")
}

func TestOCIManifestStorage(t *testing.T) {
	testOCIManifestStorageImpl(t, "includeMediaTypes=true", true)
	testOCIManifestStorageImpl(t, "includeMediaTypes=false", false)
}

func testOCIManifestStorageImpl(t *testing.T, testname string, includeMediaTypes bool) {
	var imageMediaType string
	var indexMediaType string
	if includeMediaTypes {
		imageMediaType = v1.MediaTypeImageManifest
		indexMediaType = v1.MediaTypeImageIndex
	} else {
		imageMediaType = ""
		indexMediaType = ""
	}

	repoName, _ := reference.WithName("foo/bar")
	env := newManifestStoreTestEnv(t, repoName, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), EnableDelete, EnableRedirect)

	ctx := context.Background()
	ms, err := env.repository.Manifests(ctx)
	require.NoErrorf(t, err, "%s: unexpected error", testname)

	// Build a manifest and store it and its layers in the registry

	blobStore := env.repository.Blobs(ctx)
	manifestBuilder := ocischema.NewManifestBuilder(blobStore, make([]byte, 0), make(map[string]string))
	err = manifestBuilder.(*ocischema.Builder).SetMediaType(imageMediaType)
	require.NoErrorf(t, err, "%s: unexpected error", testname)

	// Add some layers
	for i := 0; i < 2; i++ {
		rs, dgst, err := testutil.CreateRandomTarFile(testutil.MustChaChaSeed(t))
		require.NoErrorf(t, err, "%s: unexpected error generating test layer file", testname)

		wr, err := env.repository.Blobs(env.ctx).Create(env.ctx)
		require.NoErrorf(t, err, "%s: unexpected error creating test upload", testname)

		_, err = io.Copy(wr, rs)
		require.NoErrorf(t, err, "%s: unexpected error copying to upload", testname)

		_, err = wr.Commit(env.ctx, distribution.Descriptor{Digest: dgst})
		require.NoErrorf(t, err, "%s: unexpected error finishing upload", testname)

		manifestBuilder.AppendReference(distribution.Descriptor{Digest: dgst})
	}

	m, err := manifestBuilder.Build(ctx)
	require.NoErrorf(t, err, "%s: unexpected error generating manifest", testname)

	// before putting the manifest test for proper handling of SchemaVersion

	require.Equalf(t, 2, m.(*ocischema.DeserializedManifest).Manifest.SchemaVersion, "%s: unexpected error generating default version for oci manifest", testname)
	m.(*ocischema.DeserializedManifest).Manifest.SchemaVersion = 0

	var manifestDigest digest.Digest
	manifestDigest, err = ms.Put(ctx, m)
	if err != nil {
		require.EqualError(t, err, "unrecognized manifest schema version 0", "%s: unexpected error putting manifest", testname)
		m.(*ocischema.DeserializedManifest).Manifest.SchemaVersion = 2
		manifestDigest, err = ms.Put(ctx, m)
		require.NoErrorf(t, err, "%s: unexpected error putting manifest", testname)
	}

	// Also create an image index that contains the manifest

	descriptor, err := env.registry.BlobStatter().Stat(ctx, manifestDigest)
	require.NoErrorf(t, err, "%s: unexpected error getting manifest descriptor", testname)
	descriptor.MediaType = v1.MediaTypeImageManifest

	platformSpec := manifestlist.PlatformSpec{
		Architecture: "atari2600",
		OS:           "CP/M",
	}

	manifestDescriptors := []manifestlist.ManifestDescriptor{
		{
			Descriptor: descriptor,
			Platform:   platformSpec,
		},
	}

	imageIndex, err := manifestlist.FromDescriptorsWithMediaType(manifestDescriptors, indexMediaType)
	require.NoErrorf(t, err, "%s: unexpected error creating image index", testname)

	indexDigest, err := ms.Put(ctx, imageIndex)
	require.NoErrorf(t, err, "%s: unexpected error putting image index", testname)

	// Now check that we can retrieve the manifest

	fromStore, err := ms.Get(ctx, manifestDigest)
	require.NoErrorf(t, err, "%s: unexpected error fetching manifest", testname)

	fetchedManifest, ok := fromStore.(*ocischema.DeserializedManifest)
	require.Truef(t, ok, "%s: unexpected type for fetched manifest", testname)

	require.Equalf(t, fetchedManifest.MediaType, imageMediaType, "%s: unexpected MediaType for result, %s", testname, fetchedManifest.MediaType)

	require.Equalf(t, fetchedManifest.SchemaVersion, ocischema.SchemaVersion.SchemaVersion, "%s: unexpected schema version for result, %d", testname, fetchedManifest.SchemaVersion)

	payloadMediaType, _, err := fromStore.Payload()
	require.NoError(t, err, "%s: error getting payload", testname)

	require.Equalf(t, v1.MediaTypeImageManifest, payloadMediaType, "%s: unexpected MediaType for manifest payload, %s", testname, payloadMediaType)

	// and the image index

	fromStore, err = ms.Get(ctx, indexDigest)
	require.NoErrorf(t, err, "%s: unexpected error fetching image index", testname)

	fetchedIndex, ok := fromStore.(*manifestlist.DeserializedManifestList)
	require.Truef(t, ok, "%s: unexpected type for fetched manifest", testname)

	require.Equalf(t, fetchedIndex.MediaType, indexMediaType, "%s: unexpected MediaType for result, %s", testname, fetchedManifest.MediaType)

	payloadMediaType, _, err = fromStore.Payload()
	require.NoErrorf(t, err, "%s: error getting payload", testname)

	require.Equalf(t, v1.MediaTypeImageIndex, payloadMediaType, "%s: unexpected MediaType for index payload, %s", testname, payloadMediaType)
}

// Storing empty manifests are not an expected behavior of the registry, but
// but they may still be encountered on the storage backend due to a previous
// bug, corrupted data, or the backend's data being modified directly.
func TestEmptyManifestContent(t *testing.T) {
	repoRef, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	env := newManifestStoreTestEnv(t, repoRef)

	// Create an tag an schema2 manifest.
	img, err := testutil.UploadRandomSchema2Image(env.repository)
	require.NoError(t, err)

	ctx := context.Background()
	tagStore := env.repository.Tags(ctx)
	tagStore.Tag(ctx, env.tag, distribution.Descriptor{Digest: img.ManifestDigest})

	// Wipe the content of the manifest in common blob storage, but leave
	// the metadata references.
	blobPath, err := pathFor(blobDataPathSpec{digest: img.ManifestDigest})
	require.NoError(t, err)
	env.driver.PutContent(ctx, blobPath, make([]byte, 0))

	manifestService, err := env.repository.Manifests(ctx)
	require.NoError(t, err)

	// This function still returns true, nil, even with empty manifest content.
	ok, err := manifestService.Exists(ctx, img.ManifestDigest)
	require.NoError(t, err)
	require.True(t, ok)

	expectedErr := &distribution.ErrManifestEmpty{Name: repoRef.Name(), Digest: img.ManifestDigest}
	_, err = manifestService.Get(ctx, img.ManifestDigest)
	require.Error(t, err)
	require.EqualError(t, err, expectedErr.Error())
}

// TestLinkPathFuncs ensures that the link path functions behavior are locked
// down and implemented as expected.
func TestLinkPathFuncs(t *testing.T) {
	for _, testcase := range []struct {
		repo       string
		digest     digest.Digest
		linkPathFn linkPathFunc
		expected   string
	}{
		{
			repo:       "foo/bar",
			digest:     "sha256:deadbeaf98fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			linkPathFn: blobLinkPath,
			expected:   "/docker/registry/v2/repositories/foo/bar/_layers/sha256/deadbeaf98fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/link",
		},
		{
			repo:       "foo/bar",
			digest:     "sha256:deadbeaf98fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			linkPathFn: manifestRevisionLinkPath,
			expected:   "/docker/registry/v2/repositories/foo/bar/_manifests/revisions/sha256/deadbeaf98fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/link",
		},
	} {
		p, err := testcase.linkPathFn(testcase.repo, testcase.digest)
		require.NoErrorf(t, err, "unexpected error calling linkPathFn(pm, %q, %q): %v", testcase.repo, testcase.digest, err)

		require.Equal(t, testcase.expected, p, "incorrect path returned")
	}
}
