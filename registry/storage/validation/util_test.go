package validation_test

import (
	"context"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/manifestlist"
	mlcompat "github.com/docker/distribution/manifest/manifestlist/compat"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	"github.com/docker/distribution/registry/storage/validation"
	"github.com/docker/distribution/testutil"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

// return a oci manifest with a pre-pushed config placeholder.
func makeOCIManifestTemplate(t *testing.T, repo distribution.Repository) ocischema.Manifest {
	ctx := context.Background()

	config, err := repo.Blobs(ctx).Put(ctx, v1.MediaTypeImageConfig, nil)
	require.NoError(t, err)

	return ocischema.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     v1.MediaTypeImageConfig,
		},
		Config: config,
	}
}

func createRegistry(t *testing.T) distribution.Namespace {
	ctx := context.Background()

	registry, err := storage.NewRegistry(ctx, inmemory.New())
	if err != nil {
		t.Fatalf("Failed to construct namespace")
	}
	return registry
}

// nolint:unparam //(name always receives "test")
func makeRepository(t *testing.T, registry distribution.Namespace, name string) distribution.Repository {
	ctx := context.Background()

	// Initialize a dummy repository
	named, err := reference.WithName(name)
	if err != nil {
		t.Fatalf("Failed to parse name %s:  %v", name, err)
	}

	repo, err := registry.Repository(ctx, named)
	if err != nil {
		t.Fatalf("Failed to construct repository: %v", err)
	}
	return repo
}

// return a schema2 manifest with a pre-pushed config placeholder.
func makeSchema2ManifestTemplate(t *testing.T, repo distribution.Repository) schema2.Manifest {
	ctx := context.Background()

	config, err := repo.Blobs(ctx).Put(ctx, schema2.MediaTypeImageConfig, nil)
	require.NoError(t, err)

	return schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
		Config: config,
	}
}

// return a manifest descriptor with a pre-pushed manifest placeholder.
func makeManifestDescriptor(t *testing.T, repo distribution.Repository) manifestlist.ManifestDescriptor {
	ctx := context.Background()

	manifestService, err := testutil.MakeManifestService(repo)
	require.NoError(t, err)

	m := makeSchema2ManifestTemplate(t, repo)

	dm, err := schema2.FromStruct(m)
	require.NoError(t, err)

	dgst, err := manifestService.Put(ctx, dm)
	require.NoError(t, err)

	return manifestlist.ManifestDescriptor{Descriptor: distribution.Descriptor{Digest: dgst, MediaType: schema2.MediaTypeManifest}}
}

func manifestlistValidator(t *testing.T, repo distribution.Repository, refLimit, payloadLimit int) *validation.ManifestListValidator {
	t.Helper()

	manifestService, err := testutil.MakeManifestService(repo)
	require.NoError(t, err)

	blobService := repo.Blobs(context.Background())

	return validation.NewManifestListValidator(manifestService, blobService, refLimit, payloadLimit)
}

// return a image cache descriptor with a pre-pushed blob placeholder.
func makeImageCacheLayerDescriptor(t *testing.T, repo distribution.Repository) manifestlist.ManifestDescriptor {
	t.Helper()

	ctx := context.Background()

	descriptor, err := repo.Blobs(ctx).Put(ctx, v1.MediaTypeImageLayer, nil)
	require.NoError(t, err)

	mDesc := manifestlist.ManifestDescriptor{
		Descriptor: descriptor,
		Platform: manifestlist.PlatformSpec{
			Architecture: "atari2600",
			OS:           "CP/M",
			Variant:      "ternary",
			Features:     []string{"VLIW", "superscalaroutoforderdevnull"},
		},
	}

	// patch with the correct media type, put returns application/octet-stream.
	mDesc.Descriptor.MediaType = v1.MediaTypeImageLayer

	return mDesc
}

// return a image cache descriptor with a pre-pushed blob placeholder.
func makeImageCacheConfigDescriptor(t *testing.T, repo distribution.Repository) manifestlist.ManifestDescriptor {
	t.Helper()

	mDesc := makeImageCacheLayerDescriptor(t, repo)

	// patch with the buildx config media type
	mDesc.Descriptor.MediaType = mlcompat.MediaTypeBuildxCacheConfig

	return mDesc
}
