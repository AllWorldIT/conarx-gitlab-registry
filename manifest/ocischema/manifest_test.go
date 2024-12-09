package ocischema

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTestManifest(mediaType string) Manifest {
	return Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     mediaType,
		},
		Config: distribution.Descriptor{
			Digest:      "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
			Size:        985,
			MediaType:   v1.MediaTypeImageConfig,
			Annotations: map[string]string{"apple": "orange"},
		},
		Layers: []distribution.Descriptor{
			{
				Digest:      "sha256:62d8908bee94c202b2d35224a221aaa2058318bfa9879fa541efaecba272331b",
				Size:        153263,
				MediaType:   v1.MediaTypeImageLayerGzip,
				Annotations: map[string]string{"lettuce": "wrap"},
			},
		},
		Annotations: map[string]string{"hot": "potato"},
	}
}

func makeTestManifestWithSubject(mediaType string) Manifest {
	m := makeTestManifest(mediaType)
	m.Subject = &distribution.Descriptor{
		Digest:    "sha256:57d3be92c2f857566ecc7f9306a80021c0a7fa631e0ef5146957235aea859961",
		Size:      23456,
		MediaType: v1.MediaTypeImageManifest,
	}
	return m
}

func makeTestManifestWithArtifactType(mediaType, artifactType string) Manifest {
	m := makeTestManifest(mediaType)
	m.ArtifactType = artifactType
	return m
}

func TestManifest(t *testing.T) {
	expectedManifestSerialization := []byte(`{
   "schemaVersion": 2,
   "mediaType": "application/vnd.oci.image.manifest.v1+json",
   "config": {
      "mediaType": "application/vnd.oci.image.config.v1+json",
      "size": 985,
      "digest": "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
      "annotations": {
         "apple": "orange"
      }
   },
   "layers": [
      {
         "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
         "size": 153263,
         "digest": "sha256:62d8908bee94c202b2d35224a221aaa2058318bfa9879fa541efaecba272331b",
         "annotations": {
            "lettuce": "wrap"
         }
      }
   ],
   "annotations": {
      "hot": "potato"
   }
}`)

	testManifest := makeTestManifest(v1.MediaTypeImageManifest)
	deserialized, err := FromStruct(testManifest)
	require.NoError(t, err, "error creating DeserializedManifest")

	mediaType, canonical, _ := deserialized.Payload()

	require.Equal(t, v1.MediaTypeImageManifest, mediaType, "unexpected media type")

	// Check that the canonical field is the same as json.MarshalIndent
	// with these parameters.
	p, err := json.MarshalIndent(&testManifest, "", "   ")
	require.NoError(t, err, "error marshaling manifest")
	require.Equal(t, p, canonical, "manifest bytes not equal")

	// Check that canonical field matches expected value.
	require.Equal(t, expectedManifestSerialization, canonical, "manifest bytes not equal")

	var unmarshalled DeserializedManifest
	require.NoError(t, json.Unmarshal(deserialized.canonical, &unmarshalled), "error unmarshaling manifest")

	require.Equal(t, unmarshalled, *deserialized, "manifests are different after unmarshaling")
	require.Equal(t, "potato", deserialized.Annotations["hot"], "unexpected annotation in manifest")

	target := deserialized.Target()
	require.Equal(t, "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b", target.Digest.String(), "unexpected digest in target")
	require.Equal(t, v1.MediaTypeImageConfig, target.MediaType, "unexpected media type in target")
	require.EqualValues(t, 985, target.Size, "unexpected size in target")
	require.Equal(t, "orange", target.Annotations["apple"], "unexpected annotation in target")

	references := deserialized.References()
	require.Len(t, references, 2, "unexpected number of references")

	assert.Equal(t, references[0], target, "first reference should be target")

	// Test the second reference
	assert.Equal(t, "sha256:62d8908bee94c202b2d35224a221aaa2058318bfa9879fa541efaecba272331b", references[1].Digest.String(), "unexpected digest in reference")
	assert.Equal(t, v1.MediaTypeImageLayerGzip, references[1].MediaType, "unexpected media type in reference")
	assert.EqualValues(t, 153263, references[1].Size, "unexpected size in reference")
	assert.Equal(t, "wrap", references[1].Annotations["lettuce"], "unexpected annotation in reference")
}

func TestManifestWithSubject(t *testing.T) {
	expectedManifestSerialization := []byte(`{
   "schemaVersion": 2,
   "mediaType": "application/vnd.oci.image.manifest.v1+json",
   "config": {
      "mediaType": "application/vnd.oci.image.config.v1+json",
      "size": 985,
      "digest": "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
      "annotations": {
         "apple": "orange"
      }
   },
   "layers": [
      {
         "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
         "size": 153263,
         "digest": "sha256:62d8908bee94c202b2d35224a221aaa2058318bfa9879fa541efaecba272331b",
         "annotations": {
            "lettuce": "wrap"
         }
      }
   ],
   "subject": {
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "size": 23456,
      "digest": "sha256:57d3be92c2f857566ecc7f9306a80021c0a7fa631e0ef5146957235aea859961"
   },
   "annotations": {
      "hot": "potato"
   }
}`)

	testManifest := makeTestManifestWithSubject(v1.MediaTypeImageManifest)

	deserialized, err := FromStruct(testManifest)
	require.NoError(t, err)

	mediaType, canonical, _ := deserialized.Payload()
	require.Equal(t, v1.MediaTypeImageManifest, mediaType)

	// Check that canonical field matches expected value.
	require.Truef(t, bytes.Equal(expectedManifestSerialization, canonical),
		"manifest bytes not equal: %q != %q", string(canonical), string(expectedManifestSerialization))

	// Test the subject
	subject := deserialized.Subject()
	require.Equal(t, "sha256:57d3be92c2f857566ecc7f9306a80021c0a7fa631e0ef5146957235aea859961", subject.Digest.String())
	require.EqualValues(t, 23456, subject.Size)
	require.Equal(t, v1.MediaTypeImageManifest, subject.MediaType)

	// Should include the subject in this manifest's references
	references := deserialized.References()
	require.Len(t, references, 3)
}

func TestManifestWithArtifactType(t *testing.T) {
	expectedManifestSerialization := []byte(`{
   "schemaVersion": 2,
   "mediaType": "application/vnd.oci.image.manifest.v1+json",
   "artifactType": "application/vnd.dev.cosign.artifact.sbom.v1+json",
   "config": {
      "mediaType": "application/vnd.oci.image.config.v1+json",
      "size": 985,
      "digest": "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
      "annotations": {
         "apple": "orange"
      }
   },
   "layers": [
      {
         "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
         "size": 153263,
         "digest": "sha256:62d8908bee94c202b2d35224a221aaa2058318bfa9879fa541efaecba272331b",
         "annotations": {
            "lettuce": "wrap"
         }
      }
   ],
   "annotations": {
      "hot": "potato"
   }
}`)

	testManifest := makeTestManifestWithArtifactType(v1.MediaTypeImageManifest, "application/vnd.dev.cosign.artifact.sbom.v1+json")

	deserialized, err := FromStruct(testManifest)
	require.NoError(t, err)

	mediaType, canonical, _ := deserialized.Payload()
	require.Equal(t, v1.MediaTypeImageManifest, mediaType)

	// Check that canonical field matches expected value.
	require.Truef(t, bytes.Equal(expectedManifestSerialization, canonical),
		"manifest bytes not equal: %q != %q", string(canonical), string(expectedManifestSerialization))
}

func mediaTypeTest(t *testing.T, mediaType string, shouldError bool) {
	testManifest := makeTestManifest(mediaType)

	deserialized, err := FromStruct(testManifest)
	require.NoError(t, err, "error creating DeserializedManifest")

	unmarshalled, descriptor, err := distribution.UnmarshalManifest(
		v1.MediaTypeImageManifest,
		deserialized.canonical)

	if shouldError {
		require.Error(t, err, "bad content type should have produced error")
	} else {
		require.NoError(t, err, "error unmarshaling manifest")

		asManifest := unmarshalled.(*DeserializedManifest)
		require.Equal(t, mediaType, asManifest.MediaType, "bad media type as unmarshalled")

		require.Equal(t, v1.MediaTypeImageManifest, descriptor.MediaType, "bad media type for descriptor")

		unmarshalledMediaType, _, _ := unmarshalled.Payload()
		require.Equal(t, v1.MediaTypeImageManifest, unmarshalledMediaType, "bad media type for payload")
	}
}

func TestMediaTypes(t *testing.T) {
	mediaTypeTest(t, "", false)
	mediaTypeTest(t, v1.MediaTypeImageManifest, false)
	mediaTypeTest(t, v1.MediaTypeImageManifest+"XXX", true)
}

func TestTotalSize(t *testing.T) {
	testManifest := makeTestManifest(v1.MediaTypeImageManifest)

	deserialized, err := FromStruct(testManifest)
	require.NoError(t, err)

	_, payload, err := deserialized.Payload()
	require.NoError(t, err)

	var layerSize int64
	for _, layer := range deserialized.Layers() {
		layerSize += layer.Size
	}

	require.Equal(t, layerSize+deserialized.Config().Size+int64(len(payload)), deserialized.TotalSize())
}

func TestTotalSizeWithSubject(t *testing.T) {
	testManifest := makeTestManifestWithSubject(v1.MediaTypeImageManifest)

	deserialized, err := FromStruct(testManifest)
	require.NoError(t, err)

	_, payload, err := deserialized.Payload()
	require.NoError(t, err)

	var layerSize int64
	for _, layer := range deserialized.Layers() {
		layerSize += layer.Size
	}

	require.Equal(t, layerSize+deserialized.Config().Size+int64(len(payload)), deserialized.TotalSize())
}

func TestDistributableLayers(t *testing.T) {
	m := Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     v1.MediaTypeImageManifest,
		},
		Config: distribution.Descriptor{
			Digest:    "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
			Size:      985,
			MediaType: v1.MediaTypeImageConfig,
		},
		Layers: []distribution.Descriptor{
			{
				Digest:    "sha256:15799a4d823625388b0529dec26be44f13b0a69bd5fe5401ec37888eff558fb0",
				Size:      12345,
				MediaType: v1.MediaTypeImageLayerGzip,
			},
			{
				Digest:    "sha256:46e4d1d3fb114f8a35c0c38749f6b2e6710a31cd3cd11cbb060a9df50b1f24cc",
				Size:      67890,
				MediaType: v1.MediaTypeImageLayerNonDistributable,
			},
			{
				Digest:    "sha256:b5f85c2d653c4a56e6b5036b81a8629db7f4b1be3949309770ed29dc1a1f3bb0",
				Size:      91264,
				MediaType: v1.MediaTypeImageLayerNonDistributableGzip,
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
			MediaType: v1.MediaTypeImageLayerGzip,
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
