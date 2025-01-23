package notifications

import (
	"io"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/notifications/meta"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage"
	"github.com/docker/distribution/registry/storage/cache/memory"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	"github.com/docker/distribution/testutil"
	"github.com/docker/libtrust"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestListener(t *testing.T) {
	ctx := context.Background()
	k, err := libtrust.GenerateECP256PrivateKey()
	require.NoError(t, err)

	registry, err := storage.NewRegistry(ctx, inmemory.New(), storage.BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), storage.EnableDelete, storage.EnableRedirect, storage.Schema1SigningKey(k), storage.EnableSchema1)
	require.NoError(t, err, "error creating registry")

	tl := &testListener{
		ops: make(map[string]int),
	}

	repoRef, _ := reference.WithName("foo/bar")
	repository, err := registry.Repository(ctx, repoRef)
	require.NoError(t, err, "unexpected error getting repo")

	remover, ok := registry.(distribution.RepositoryRemover)
	require.True(t, ok, "registry does not implement RepositoryRemover")

	repository, remover = Listen(repository, remover, tl, false)

	// Now take the registry through a number of operations
	checkExerciseRepository(t, repository, remover)

	expectedOps := map[string]int{
		"manifest:push":   1,
		"manifest:pull":   1,
		"manifest:delete": 1,
		"layer:push":      2,
		"layer:pull":      2,
		"layer:delete":    2,
		"tag:delete":      1,
		"repo:delete":     1,
	}

	require.Equal(t, expectedOps, tl.ops, "counts do not match")
}

type testListener struct {
	ops map[string]int
}

func (tl *testListener) ManifestPushed(_ reference.Named, _ distribution.Manifest, _ ...distribution.ManifestServiceOption) error {
	tl.ops["manifest:push"]++
	return nil
}

func (tl *testListener) ManifestPulled(_ reference.Named, _ distribution.Manifest, _ ...distribution.ManifestServiceOption) error {
	tl.ops["manifest:pull"]++
	return nil
}

func (tl *testListener) ManifestDeleted(_ reference.Named, _ digest.Digest) error {
	tl.ops["manifest:delete"]++
	return nil
}

func (tl *testListener) BlobPushed(_ reference.Named, _ distribution.Descriptor) error {
	tl.ops["layer:push"]++
	return nil
}

func (tl *testListener) BlobPulled(_ reference.Named, _ distribution.Descriptor, _ *meta.Blob) error {
	tl.ops["layer:pull"]++
	return nil
}

func (tl *testListener) BlobMounted(_ reference.Named, _ distribution.Descriptor, _ reference.Named) error {
	tl.ops["layer:mount"]++
	return nil
}

func (tl *testListener) BlobDeleted(_ reference.Named, _ digest.Digest) error {
	tl.ops["layer:delete"]++
	return nil
}

func (tl *testListener) TagDeleted(_ reference.Named, _ string) error {
	tl.ops["tag:delete"]++
	return nil
}

func (tl *testListener) RepoDeleted(_ reference.Named) error {
	tl.ops["repo:delete"]++
	return nil
}

// checkExerciseRegistry takes the registry through all of its operations,
// carrying out generic checks.
func checkExerciseRepository(t *testing.T, repository distribution.Repository, remover distribution.RepositoryRemover) {
	ctx := context.Background()
	tag := "thetag"
	// todo: change this to use Builder

	m := schema1.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 1,
		},
		Name: repository.Named().Name(),
		Tag:  tag,
	}

	var blobDigests []digest.Digest
	blobs := repository.Blobs(ctx)
	for i := 0; i < 2; i++ {
		rs, dgst, err := testutil.CreateRandomTarFile(testutil.MustChaChaSeed(t))
		require.NoError(t, err, "error creating test layer")

		blobDigests = append(blobDigests, dgst)

		wr, err := blobs.Create(ctx)
		require.NoError(t, err, "error creating layer upload")

		// Use the resumes, as well!
		wr, err = blobs.Resume(ctx, wr.ID())
		require.NoError(t, err, "error resuming layer upload")

		_, err = io.Copy(wr, rs)
		require.NoError(t, err)

		_, err = wr.Commit(ctx, distribution.Descriptor{Digest: dgst})
		require.NoError(t, err, "unexpected error finishing upload")

		m.FSLayers = append(m.FSLayers, schema1.FSLayer{
			BlobSum: dgst,
		})
		m.History = append(m.History, schema1.History{
			V1Compatibility: "",
		})

		// Then fetch the blobs
		rc, err := blobs.Open(ctx, dgst)
		require.NoError(t, err, "error fetching layer")
		// nolint: revive // defer
		defer rc.Close()
	}

	pk, err := libtrust.GenerateECP256PrivateKey()
	require.NoError(t, err, "unexpected error generating key")

	sm, err := schema1.Sign(&m, pk)
	require.NoError(t, err, "unexpected error signing manifest")

	manifests, err := repository.Manifests(ctx)
	require.NoError(t, err)

	var digestPut digest.Digest
	digestPut, err = manifests.Put(ctx, sm)
	require.NoError(t, err, "unexpected error putting the manifest")

	dgst := digest.FromBytes(sm.Canonical)
	require.Equal(t, dgst, digestPut, "mismatching digest from payload and put")

	_, err = manifests.Get(ctx, dgst)
	require.NoError(t, err, "unexpected error fetching manifest")

	err = repository.Tags(ctx).Tag(ctx, tag, distribution.Descriptor{Digest: dgst})
	require.NoError(t, err, "unexpected error tagging manifest")

	err = manifests.Delete(ctx, dgst)
	require.NoError(t, err, "unexpected error deleting blob")

	for _, d := range blobDigests {
		err = blobs.Delete(ctx, d)
		require.NoError(t, err, "unexpected error deleting blob")
	}

	err = repository.Tags(ctx).Untag(ctx, m.Tag)
	require.NoError(t, err, "unexpected error deleting tag")

	err = remover.Remove(ctx, repository.Named())
	require.NoError(t, err, "unexpected error deleting repo")
}
