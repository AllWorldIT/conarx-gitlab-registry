package testutil

import (
	"fmt"
	"io"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/manifestlist"
	mlcompat "github.com/docker/distribution/manifest/manifestlist/compat"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/libtrust"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

type Image struct {
	manifest       distribution.Manifest
	ManifestDigest digest.Digest
	Layers         map[digest.Digest]io.ReadSeeker
}

type ImageList struct {
	manifest       distribution.Manifest
	ManifestDigest digest.Digest
	Images         []Image
}

const SampleManifestJSON = `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{"mediaType":"application/vnd.docker.container.image.v1+json","size":1640,"digest":"sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9"},"layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":2802957,"digest":"sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9"},{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":108,"digest":"sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21"}]}`

// MakeManifestList constructs a manifest list out of a list of manifest digests
func MakeManifestList(blobstatter distribution.BlobStatter, manifestDigests []digest.Digest) (*manifestlist.DeserializedManifestList, error) {
	ctx := context.Background()

	var manifestDescriptors []manifestlist.ManifestDescriptor
	for _, manifestDigest := range manifestDigests {
		descriptor, err := blobstatter.Stat(ctx, manifestDigest)
		if err != nil {
			return nil, err
		}
		platformSpec := manifestlist.PlatformSpec{
			Architecture: "atari2600",
			OS:           "CP/M",
			Variant:      "ternary",
			Features:     []string{"VLIW", "superscalaroutoforderdevnull"},
		}
		manifestDescriptor := manifestlist.ManifestDescriptor{
			Descriptor: descriptor,
			Platform:   platformSpec,
		}
		manifestDescriptors = append(manifestDescriptors, manifestDescriptor)
	}

	return manifestlist.FromDescriptors(manifestDescriptors)
}

// MakeSchema1Manifest constructs a schema 1 manifest from a given list of digests and returns
// the digest of the manifest
func MakeSchema1Manifest(digests []digest.Digest) (distribution.Manifest, error) {
	manifest := schema1.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 1,
		},
		Name: "who",
		Tag:  "cares",
	}

	for _, digest := range digests {
		manifest.FSLayers = append(manifest.FSLayers, schema1.FSLayer{BlobSum: digest})
		manifest.History = append(manifest.History, schema1.History{V1Compatibility: ""})
	}

	pk, err := libtrust.GenerateECP256PrivateKey()
	if err != nil {
		return nil, fmt.Errorf("unexpected error generating private key: %v", err)
	}

	signedManifest, err := schema1.Sign(&manifest, pk)
	if err != nil {
		return nil, fmt.Errorf("error signing manifest: %v", err)
	}

	return signedManifest, nil
}

// MakeSchema2Manifest constructs a schema 2 manifest from a given list of digests and returns
// the digest of the manifest
func MakeSchema2Manifest(repository distribution.Repository, digests []digest.Digest) (distribution.Manifest, error) {
	ctx := context.Background()
	blobStore := repository.Blobs(ctx)
	builder := schema2.NewManifestBuilder(blobStore, schema2.MediaTypeImageConfig, []byte{})
	for _, digest := range digests {
		err := builder.AppendReference(distribution.Descriptor{Digest: digest})
		if err != nil {
			return nil, fmt.Errorf("appending reference: %w", err)
		}
	}

	manifest, err := builder.Build(ctx)
	if err != nil {
		return nil, fmt.Errorf("unexpected error generating manifest: %v", err)
	}

	return manifest, nil
}

func UploadRandomSchema1Image(repository distribution.Repository) (Image, error) {
	randomLayers, err := CreateRandomLayers(2)
	if err != nil {
		return Image{}, err
	}

	digests := []digest.Digest{}
	for digest := range randomLayers {
		digests = append(digests, digest)
	}

	manifest, err := MakeSchema1Manifest(digests)
	if err != nil {
		return Image{}, err
	}

	manifestDigest, err := UploadImage(repository, Image{manifest: manifest, Layers: randomLayers})
	if err != nil {
		return Image{}, err
	}

	return Image{
		manifest:       manifest,
		ManifestDigest: manifestDigest,
		Layers:         randomLayers,
	}, nil
}

func UploadRandomSchema2Image(repository distribution.Repository) (Image, error) {
	randomLayers, err := CreateRandomLayers(2)
	if err != nil {
		return Image{}, err
	}

	digests := []digest.Digest{}
	for digest := range randomLayers {
		digests = append(digests, digest)
	}

	manifest, err := MakeSchema2Manifest(repository, digests)
	if err != nil {
		return Image{}, err
	}

	manifestDigest, err := UploadImage(repository, Image{manifest: manifest, Layers: randomLayers})
	if err != nil {
		return Image{}, err
	}

	return Image{
		manifest:       manifest,
		ManifestDigest: manifestDigest,
		Layers:         randomLayers,
	}, nil
}

func UploadRandomImageList(tb testing.TB, registry distribution.Namespace, repository distribution.Repository) ImageList {
	ctx := context.Background()
	var manifestDescriptors []manifestlist.ManifestDescriptor
	images := make([]Image, 4)

	for i := range images {
		image, err := UploadRandomSchema2Image(repository)
		require.NoError(tb, err)

		images[i] = image

		blobstatter := registry.BlobStatter()

		descriptor, err := blobstatter.Stat(ctx, image.ManifestDigest)
		require.NoError(tb, err)

		// Set correct mediatype for image descriptor.
		mt, _, err := image.manifest.Payload()
		require.NoError(tb, err)
		descriptor.MediaType = mt

		platformSpec := manifestlist.PlatformSpec{
			Architecture: "atari2600",
			OS:           "CP/M",
			Variant:      "ternary",
			Features:     []string{"VLIW", "superscalaroutoforderdevnull"},
		}
		manifestDescriptor := manifestlist.ManifestDescriptor{
			Descriptor: descriptor,
			Platform:   platformSpec,
		}
		manifestDescriptors = append(manifestDescriptors, manifestDescriptor)
	}

	ml, err := manifestlist.FromDescriptors(manifestDescriptors)
	require.NoError(tb, err)

	manifestService, err := repository.Manifests(ctx)
	require.NoError(tb, err)

	dgst, err := manifestService.Put(ctx, ml)
	require.NoError(tb, err)

	return ImageList{
		manifest:       ml,
		ManifestDigest: dgst,
		Images:         images,
	}
}

func UploadRandomNonConformantBuildxCache(tb testing.TB, registry distribution.Namespace, repository distribution.Repository) ImageList {
	ctx := context.Background()
	var manifestDescriptors []manifestlist.ManifestDescriptor

	randomLayers, err := CreateRandomLayers(4)
	require.NoError(tb, err)

	err = UploadBlobs(repository, randomLayers)
	require.NoError(tb, err)

	for layerDigest := range randomLayers {
		blobstatter := registry.BlobStatter()

		descriptor, err := blobstatter.Stat(ctx, layerDigest)
		require.NoError(tb, err)

		platformSpec := manifestlist.PlatformSpec{
			Architecture: "atari2600",
			OS:           "CP/M",
			Variant:      "ternary",
			Features:     []string{"VLIW", "superscalaroutoforderdevnull"},
		}
		manifestDescriptor := manifestlist.ManifestDescriptor{
			Descriptor: descriptor,
			Platform:   platformSpec,
		}
		manifestDescriptors = append(manifestDescriptors, manifestDescriptor)
	}

	// Ensure one reference looks like a buildx cache config for validation.
	manifestDescriptors[0].Descriptor.MediaType = mlcompat.MediaTypeBuildxCacheConfig

	ml, err := manifestlist.FromDescriptors(manifestDescriptors)
	require.NoError(tb, err)
	ml.MediaType = v1.MediaTypeImageIndex

	manifestService, err := repository.Manifests(ctx)
	require.NoError(tb, err)

	dgst, err := manifestService.Put(ctx, ml)
	require.NoError(tb, err)

	return ImageList{
		manifest:       ml,
		ManifestDigest: dgst,
	}
}

func UploadImage(repository distribution.Repository, im Image) (digest.Digest, error) {
	// upload layers
	err := UploadBlobs(repository, im.Layers)
	if err != nil {
		return "", fmt.Errorf("layer upload failed: %v", err)
	}

	// upload manifest
	ctx := context.Background()

	manifestService, err := MakeManifestService(repository)
	if err != nil {
		return "", fmt.Errorf("failed to create manifest service: %v", err)
	}

	manifestDigest, err := manifestService.Put(ctx, im.manifest)
	if err != nil {
		return "", fmt.Errorf("manifest upload failed: %v", err)
	}

	return manifestDigest, nil
}

func MakeManifestService(repository distribution.Repository) (distribution.ManifestService, error) {
	ctx := context.Background()

	manifestService, err := repository.Manifests(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to construct manifest store: %v", err)
	}
	return manifestService, nil
}
