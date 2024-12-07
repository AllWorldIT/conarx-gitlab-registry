package schema2

import (
	"encoding/json"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var expectedManifestSerialization = []byte(`{
   "schemaVersion": 2,
   "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
   "config": {
      "mediaType": "application/vnd.docker.container.image.v1+json",
      "size": 985,
      "digest": "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b"
   },
   "layers": [
      {
         "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
         "size": 153263,
         "digest": "sha256:62d8908bee94c202b2d35224a221aaa2058318bfa9879fa541efaecba272331b"
      }
   ]
}`)

func makeTestManifest(mediaType string) Manifest {
	return Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     mediaType,
		},
		Config: distribution.Descriptor{
			Digest:    "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
			Size:      985,
			MediaType: MediaTypeImageConfig,
		},
		Layers: []distribution.Descriptor{
			{
				Digest:    "sha256:62d8908bee94c202b2d35224a221aaa2058318bfa9879fa541efaecba272331b",
				Size:      153263,
				MediaType: MediaTypeLayer,
			},
		},
	}
}

func TestManifest(t *testing.T) {
	testManifest := makeTestManifest(MediaTypeManifest)

	deserialized, err := FromStruct(testManifest)
	require.NoError(t, err, "error creating DeserializedManifest")

	mediaType, canonical, _ := deserialized.Payload()

	require.Equal(t, MediaTypeManifest, mediaType, "unexpected media type")

	// Check that the canonical field is the same as json.MarshalIndent
	// with these parameters.
	p, err := json.MarshalIndent(&testManifest, "", "   ")
	require.NoError(t, err, "error marshaling manifest")
	require.Equal(t, p, canonical, "manifest bytes not equal")

	// Check that canonical field matches expected value.
	require.Equal(t, expectedManifestSerialization, canonical, "manifest bytes not equal")

	var unmarshalled DeserializedManifest
	require.NoError(t, json.Unmarshal(deserialized.canonical, &unmarshalled), "error unmarshaling manifest")

	require.Equal(t, &unmarshalled, deserialized, "manifests are different after unmarshaling")

	target := deserialized.Target()
	require.Equal(t, "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b", target.Digest.String(), "unexpected digest in target")
	require.Equal(t, MediaTypeImageConfig, target.MediaType, "unexpected media type in target")
	require.Equal(t, int64(985), target.Size, "unexpected size in target")

	references := deserialized.References()
	require.Len(t, references, 2, "unexpected number of references")

	assert.Equal(t, target, references[0], "first reference should be target")

	// Test the second reference
	assert.Equal(t, "sha256:62d8908bee94c202b2d35224a221aaa2058318bfa9879fa541efaecba272331b", references[1].Digest.String(), "unexpected digest in reference")
	assert.Equal(t, MediaTypeLayer, references[1].MediaType, "unexpected media type in reference")
	assert.Equal(t, int64(153263), references[1].Size, "unexpected size in reference")
}

func mediaTypeTest(t *testing.T, mediaType string, shouldError bool) {
	testManifest := makeTestManifest(mediaType)

	deserialized, err := FromStruct(testManifest)
	require.NoError(t, err, "error creating DeserializedManifest")

	unmarshalled, descriptor, err := distribution.UnmarshalManifest(
		MediaTypeManifest,
		deserialized.canonical)

	if shouldError {
		require.Error(t, err, "bad content type should have produced error")
	} else {
		require.NoError(t, err, "error unmarshaling manifest")

		asManifest := unmarshalled.(*DeserializedManifest)
		require.Equal(t, mediaType, asManifest.MediaType, "bad media type as unmarshalled")

		require.Equal(t, MediaTypeManifest, descriptor.MediaType, "bad media type for descriptor")

		unmarshalledMediaType, _, _ := unmarshalled.Payload()
		require.Equal(t, MediaTypeManifest, unmarshalledMediaType, "bad media type for payload")
	}
}

func TestMediaTypes(t *testing.T) {
	mediaTypeTest(t, "", true)
	mediaTypeTest(t, MediaTypeManifest, false)
	mediaTypeTest(t, MediaTypeManifest+"XXX", true)
}

func TestTotalSize(t *testing.T) {
	testManifest := makeTestManifest(MediaTypeManifest)

	deserialized, err := FromStruct(testManifest)
	require.NoError(t, err)

	_, payload, err := deserialized.Payload()
	require.NoError(t, err)

	var refSize int64
	for _, ref := range testManifest.References() {
		refSize += ref.Size
	}

	require.Equal(t, refSize+int64(len(payload)), deserialized.TotalSize())
}

func TestDistributableLayers(t *testing.T) {
	m := Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     MediaTypeManifest,
		},
		Config: distribution.Descriptor{
			Digest:    "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
			Size:      985,
			MediaType: MediaTypeImageConfig,
		},
		Layers: []distribution.Descriptor{
			{
				Digest:    "sha256:15799a4d823625388b0529dec26be44f13b0a69bd5fe5401ec37888eff558fb0",
				Size:      12345,
				MediaType: MediaTypeLayer,
			},
			{
				Digest:    "sha256:46e4d1d3fb114f8a35c0c38749f6b2e6710a31cd3cd11cbb060a9df50b1f24cc",
				Size:      67890,
				MediaType: MediaTypeForeignLayer,
			},
			{
				Digest:    "sha256:b5f85c2d653c4a56e6b5036b81a8629db7f4b1be3949309770ed29dc1a1f3bb0",
				Size:      91264,
				MediaType: MediaTypeForeignLayer,
			},
			{
				Digest:    "sha256:684137c60c5abd386c875e6dfc0a944110d0155817d5e3f1df6db6145e73dadb",
				Size:      977463,
				MediaType: "application/vnd.foo.image.layer.v0.tar+gzip",
			},
		},
	}

	dm, err := FromStruct(m)
	require.NoError(t, err)

	expectedLayers := []distribution.Descriptor{
		{
			Digest:    "sha256:15799a4d823625388b0529dec26be44f13b0a69bd5fe5401ec37888eff558fb0",
			Size:      12345,
			MediaType: MediaTypeLayer,
		},
		{
			Digest:    "sha256:684137c60c5abd386c875e6dfc0a944110d0155817d5e3f1df6db6145e73dadb",
			Size:      977463,
			MediaType: "application/vnd.foo.image.layer.v0.tar+gzip",
		},
	}

	dls := dm.DistributableLayers()
	require.Equal(t, expectedLayers, dls)
}
