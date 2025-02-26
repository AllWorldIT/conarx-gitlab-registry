package compat

import (
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

func TestReferences(t *testing.T) {
	tests := []struct {
		name              string
		descriptors       []manifestlist.ManifestDescriptor
		expectedManifests []distribution.Descriptor
		expectedBlobs     []distribution.Descriptor
	}{
		{
			name: "OCI Image Index",
			descriptors: []manifestlist.ManifestDescriptor{
				{
					Descriptor: distribution.Descriptor{
						MediaType: v1.MediaTypeImageManifest,
						Size:      2343,
						Digest:    digest.FromString("OCI Manifest 1"),
					},
				},
				{
					Descriptor: distribution.Descriptor{
						MediaType: v1.MediaTypeImageManifest,
						Size:      354,
						Digest:    digest.FromString("OCI Manifest 2"),
					},
				},
			},
			expectedManifests: []distribution.Descriptor{
				{
					MediaType: v1.MediaTypeImageManifest,
					Size:      2343,
					Digest:    digest.FromString("OCI Manifest 1"),
				},
				{
					MediaType: v1.MediaTypeImageManifest,
					Size:      354,
					Digest:    digest.FromString("OCI Manifest 2"),
				},
			},
			expectedBlobs: make([]distribution.Descriptor, 0),
		},
		{
			name: "Buildx Cache Manifest",
			descriptors: []manifestlist.ManifestDescriptor{
				{
					Descriptor: distribution.Descriptor{
						MediaType: v1.MediaTypeImageLayer,
						Size:      792343,
						Digest:    digest.FromString("OCI Layer 1"),
					},
				},
				{
					Descriptor: distribution.Descriptor{
						MediaType: v1.MediaTypeImageLayer,
						Size:      35324234,
						Digest:    digest.FromString("OCI Layer 2"),
					},
				},
				{
					Descriptor: distribution.Descriptor{
						MediaType: "application/vnd.buildkit.cacheconfig.v0",
						Size:      4233,
						Digest:    digest.FromString("Cache Config 1"),
					},
				},
			},
			expectedManifests: make([]distribution.Descriptor, 0),
			expectedBlobs: []distribution.Descriptor{
				{
					MediaType: v1.MediaTypeImageLayer,
					Size:      792343,
					Digest:    digest.FromString("OCI Layer 1"),
				},
				{
					MediaType: v1.MediaTypeImageLayer,
					Size:      35324234,
					Digest:    digest.FromString("OCI Layer 2"),
				},
				{
					MediaType: MediaTypeBuildxCacheConfig,
					Size:      4233,
					Digest:    digest.FromString("Cache Config 1"),
				},
			},
		},
		{
			name: "Mixed Manifest List",
			descriptors: []manifestlist.ManifestDescriptor{
				{
					Descriptor: distribution.Descriptor{
						MediaType: schema2.MediaTypeManifest,
						Size:      723,
						Digest:    digest.FromString("Schema2 Manifest 1"),
					},
				},
				{
					Descriptor: distribution.Descriptor{
						MediaType: schema2.MediaTypeLayer,
						Size:      2340184,
						Digest:    digest.FromString("Schema 2 Layer 1"),
					},
				},
			},
			expectedManifests: []distribution.Descriptor{
				{
					MediaType: schema2.MediaTypeManifest,
					Size:      723,
					Digest:    digest.FromString("Schema2 Manifest 1"),
				},
			},
			expectedBlobs: []distribution.Descriptor{
				{
					MediaType: schema2.MediaTypeLayer,
					Size:      2340184,
					Digest:    digest.FromString("Schema 2 Layer 1"),
				},
			},
		},
	}

	for _, tt := range tests {
		ml, err := manifestlist.FromDescriptors(tt.descriptors)
		require.NoError(t, err)

		splitRef := References(ml)
		require.ElementsMatch(t, tt.expectedManifests, splitRef.Manifests)
		require.ElementsMatch(t, tt.expectedBlobs, splitRef.Blobs)

		allRef := make([]distribution.Descriptor, len(splitRef.Manifests), len(splitRef.Manifests)+len(splitRef.Blobs))
		copy(allRef, splitRef.Manifests)
		allRef = append(allRef, splitRef.Blobs...)

		require.ElementsMatch(t, ml.References(), allRef)
	}
}

func TestIsLikeyBuildxCache(t *testing.T) {
	tests := []struct {
		name        string
		descriptors []manifestlist.ManifestDescriptor
		expected    bool
	}{
		{
			name: "OCI Image Index",
			descriptors: []manifestlist.ManifestDescriptor{
				{
					Descriptor: distribution.Descriptor{
						MediaType: v1.MediaTypeImageManifest,
						Size:      2343,
						Digest:    digest.FromString("OCI Manifest 1"),
					},
				},
				{
					Descriptor: distribution.Descriptor{
						MediaType: v1.MediaTypeImageManifest,
						Size:      354,
						Digest:    digest.FromString("OCI Manifest 2"),
					},
				},
			},
			expected: false,
		},
		{
			name: "Buildx Cache Manifest",
			descriptors: []manifestlist.ManifestDescriptor{
				{
					Descriptor: distribution.Descriptor{
						MediaType: v1.MediaTypeImageLayer,
						Size:      792343,
						Digest:    digest.FromString("OCI Layer 1"),
					},
				},
				{
					Descriptor: distribution.Descriptor{
						MediaType: v1.MediaTypeImageLayer,
						Size:      35324234,
						Digest:    digest.FromString("OCI Layer 2"),
					},
				},
				{
					Descriptor: distribution.Descriptor{
						MediaType: MediaTypeBuildxCacheConfig,
						Size:      4233,
						Digest:    digest.FromString("Cache Config 1"),
					},
				},
			},
			expected: true,
		},
		{
			name: "Mixed Manifest List",
			descriptors: []manifestlist.ManifestDescriptor{
				{
					Descriptor: distribution.Descriptor{
						MediaType: schema2.MediaTypeManifest,
						Size:      723,
						Digest:    digest.FromString("Schema2 Manifest 1"),
					},
				},
				{
					Descriptor: distribution.Descriptor{
						MediaType: schema2.MediaTypeLayer,
						Size:      2340184,
						Digest:    digest.FromString("Schema 2 Layer 1"),
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		ml, err := manifestlist.FromDescriptors(tt.descriptors)
		require.NoError(t, err)

		require.Equal(t, tt.expected, LikelyBuildxCache(ml))
	}
}

func TestContainsBlobs(t *testing.T) {
	tests := []struct {
		name        string
		descriptors []manifestlist.ManifestDescriptor
		expected    bool
	}{
		{
			name: "OCI Image Index",
			descriptors: []manifestlist.ManifestDescriptor{
				{
					Descriptor: distribution.Descriptor{
						MediaType: v1.MediaTypeImageManifest,
						Size:      2343,
						Digest:    digest.FromString("OCI Manifest 1"),
					},
				},
				{
					Descriptor: distribution.Descriptor{
						MediaType: v1.MediaTypeImageManifest,
						Size:      354,
						Digest:    digest.FromString("OCI Manifest 2"),
					},
				},
			},
			expected: false,
		},
		{
			name: "Buildx Cache Manifest",
			descriptors: []manifestlist.ManifestDescriptor{
				{
					Descriptor: distribution.Descriptor{
						MediaType: v1.MediaTypeImageLayer,
						Size:      792343,
						Digest:    digest.FromString("OCI Layer 1"),
					},
				},
				{
					Descriptor: distribution.Descriptor{
						MediaType: v1.MediaTypeImageLayer,
						Size:      35324234,
						Digest:    digest.FromString("OCI Layer 2"),
					},
				},
				{
					Descriptor: distribution.Descriptor{
						MediaType: MediaTypeBuildxCacheConfig,
						Size:      4233,
						Digest:    digest.FromString("Cache Config 1"),
					},
				},
			},
			expected: true,
		},
		{
			name: "Mixed Manifest List",
			descriptors: []manifestlist.ManifestDescriptor{
				{
					Descriptor: distribution.Descriptor{
						MediaType: schema2.MediaTypeManifest,
						Size:      723,
						Digest:    digest.FromString("Schema2 Manifest 1"),
					},
				},
				{
					Descriptor: distribution.Descriptor{
						MediaType: schema2.MediaTypeLayer,
						Size:      2340184,
						Digest:    digest.FromString("Schema 2 Layer 1"),
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		ml, err := manifestlist.FromDescriptors(tt.descriptors)
		require.NoError(t, err)

		require.Equal(t, tt.expected, ContainsBlobs(ml))
	}
}

func TestOCIManifestFromBuildkitIndex(t *testing.T) {
	cfg := distribution.Descriptor{
		MediaType: MediaTypeBuildxCacheConfig,
		Size:      4233,
		Digest:    digest.FromString("Cache Config"),
	}
	layer1 := distribution.Descriptor{
		MediaType: v1.MediaTypeImageLayer,
		Size:      792343,
		Digest:    digest.FromString("OCI Layer 1"),
	}
	layer2 := distribution.Descriptor{
		MediaType: v1.MediaTypeImageLayer,
		Size:      35324234,
		Digest:    digest.FromString("OCI Layer 2"),
	}

	tests := []struct {
		name         string
		arg          *manifestlist.DeserializedManifestList
		wantManifest *ocischema.Manifest
		wantErr      bool
	}{
		{
			name: "success",
			arg: &manifestlist.DeserializedManifestList{
				ManifestList: manifestlist.ManifestList{
					Versioned: manifest.Versioned{
						SchemaVersion: 2,
						MediaType:     v1.MediaTypeImageIndex,
					},
					Manifests: []manifestlist.ManifestDescriptor{
						{Descriptor: layer1},
						{Descriptor: layer2},
						{Descriptor: cfg},
					},
				},
			},
			wantManifest: &ocischema.Manifest{
				Versioned: ocischema.SchemaVersion,
				Config:    cfg,
				Layers:    []distribution.Descriptor{layer1, layer2},
			},
		},
		{
			name: "no references",
			arg: &manifestlist.DeserializedManifestList{
				ManifestList: manifestlist.ManifestList{
					Versioned: manifest.Versioned{
						SchemaVersion: 2,
						MediaType:     v1.MediaTypeImageIndex,
					},
					Manifests: make([]manifestlist.ManifestDescriptor, 0),
				},
			},
			wantErr: true,
		},
		{
			name: "no config",
			arg: &manifestlist.DeserializedManifestList{
				ManifestList: manifestlist.ManifestList{
					Versioned: manifest.Versioned{
						SchemaVersion: 2,
						MediaType:     v1.MediaTypeImageIndex,
					},
					Manifests: []manifestlist.ManifestDescriptor{
						{Descriptor: layer1},
						{Descriptor: layer2},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "no layers",
			arg: &manifestlist.DeserializedManifestList{
				ManifestList: manifestlist.ManifestList{
					Versioned: manifest.Versioned{
						SchemaVersion: 2,
						MediaType:     v1.MediaTypeImageIndex,
					},
					Manifests: []manifestlist.ManifestDescriptor{
						{Descriptor: cfg},
					},
				},
			},
			wantManifest: &ocischema.Manifest{
				Versioned: ocischema.SchemaVersion,
				Config:    cfg,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := OCIManifestFromBuildkitIndex(tt.arg)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tt.wantManifest != nil {
				dm, err := ocischema.FromStruct(*tt.wantManifest)
				require.NoError(t, err)
				require.Equal(t, dm, got)
			} else {
				require.Nil(t, got)
			}
		})
	}
}
