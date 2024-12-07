package schema1

import (
	"testing"

	"github.com/docker/distribution/context"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/reference"
	"github.com/docker/libtrust"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeSignedManifest(t *testing.T, pk libtrust.PrivateKey, refs []Reference) *SignedManifest {
	u := &Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 1,
		},
		Name:         "foo/bar",
		Tag:          "latest",
		Architecture: "amd64",
	}

	for i := len(refs) - 1; i >= 0; i-- {
		u.FSLayers = append(u.FSLayers, FSLayer{
			BlobSum: refs[i].Digest,
		})
		u.History = append(u.History, History{
			V1Compatibility: refs[i].History.V1Compatibility,
		})
	}

	signedManifest, err := Sign(u, pk)
	require.NoError(t, err, "unexpected error signing manifest")
	return signedManifest
}

func TestReferenceBuilder(t *testing.T) {
	pk, err := libtrust.GenerateECP256PrivateKey()
	require.NoError(t, err, "unexpected error generating private key")

	r1 := Reference{
		Digest:  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Size:    1,
		History: History{V1Compatibility: "{\"a\" : 1 }"},
	}
	r2 := Reference{
		Digest:  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Size:    2,
		History: History{V1Compatibility: "{\"\a\" : 2 }"},
	}

	handCrafted := makeSignedManifest(t, pk, []Reference{r1, r2})

	ref, err := reference.WithName(handCrafted.Manifest.Name)
	require.NoError(t, err, "could not parse reference")
	ref, err = reference.WithTag(ref, handCrafted.Manifest.Tag)
	require.NoError(t, err, "could not add tag")

	b := NewReferenceManifestBuilder(pk, ref, handCrafted.Manifest.Architecture)
	_, err = b.Build(context.Background())
	require.Error(t, err, "expected error building zero length manifest")

	require.NoError(t, b.AppendReference(r1))
	require.NoError(t, b.AppendReference(r2))

	refs := b.References()
	require.Len(t, refs, 2, "unexpected reference count")

	// Ensure ordering
	require.Equal(t, r2.Digest, refs[0].Digest, "unexpected reference")

	m, err := b.Build(context.Background())
	require.NoError(t, err)

	built, ok := m.(*SignedManifest)
	require.True(t, ok, "unexpected type from Build()")

	d1 := digest.FromBytes(built.Canonical)
	d2 := digest.FromBytes(handCrafted.Canonical)
	assert.Equal(t, d2, d1, "mismatching canonical JSON")
}
