package manifestlist

import (
	"encoding/json"
	"testing"

	"github.com/docker/distribution"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var expectedManifestListSerialization = []byte(`{
   "schemaVersion": 2,
   "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
   "manifests": [
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 985,
         "digest": "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
         "platform": {
            "architecture": "amd64",
            "os": "linux",
            "features": [
               "sse4"
            ]
         }
      },
      {
         "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
         "size": 2392,
         "digest": "sha256:6346340964309634683409684360934680934608934608934608934068934608",
         "platform": {
            "architecture": "sun4m",
            "os": "sunos"
         }
      }
   ]
}`)

func makeTestManifestList(t *testing.T, mediaType string) ([]ManifestDescriptor, *DeserializedManifestList) {
	manifestDescriptors := []ManifestDescriptor{
		{
			Descriptor: distribution.Descriptor{
				Digest:    "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
				Size:      985,
				MediaType: "application/vnd.docker.distribution.manifest.v2+json",
			},
			Platform: PlatformSpec{
				Architecture: "amd64",
				OS:           "linux",
				Features:     []string{"sse4"},
			},
		},
		{
			Descriptor: distribution.Descriptor{
				Digest:    "sha256:6346340964309634683409684360934680934608934608934608934068934608",
				Size:      2392,
				MediaType: "application/vnd.docker.distribution.manifest.v2+json",
			},
			Platform: PlatformSpec{
				Architecture: "sun4m",
				OS:           "sunos",
			},
		},
	}

	deserialized, err := FromDescriptorsWithMediaType(manifestDescriptors, mediaType)
	require.NoError(t, err, "error creating DeserializedManifestList")

	return manifestDescriptors, deserialized
}

func TestManifestList(t *testing.T) {
	manifestDescriptors, deserialized := makeTestManifestList(t, MediaTypeManifestList)
	mediaType, canonical, _ := deserialized.Payload()

	require.Equal(t, MediaTypeManifestList, mediaType, "unexpected media type")

	// Check that the canonical field is the same as json.MarshalIndent
	// with these parameters.
	p, err := json.MarshalIndent(&deserialized.ManifestList, "", "   ")
	require.NoError(t, err, "error marshaling manifest list")
	require.Equal(t, p, canonical, "manifest bytes not equal")

	// Check that the canonical field has the expected value.
	require.Equal(t, expectedManifestListSerialization, canonical, "manifest bytes not equal to expected")

	var unmarshalled DeserializedManifestList
	require.NoError(t, json.Unmarshal(deserialized.canonical, &unmarshalled), "error unmarshaling manifest")

	require.Equal(t, unmarshalled, *deserialized, "manifests are different after unmarshaling")

	references := deserialized.References()
	require.Len(t, references, 2, "unexpected number of references")
	for i := range references {
		assert.Equal(t, references[i], manifestDescriptors[i].Descriptor, "unexpected value returned by References")
	}
}

// TODO (mikebrow): add annotations on the manifest list (index) and support for
// empty platform structs (move to Platform *Platform `json:"platform,omitempty"`
// from current Platform PlatformSpec `json:"platform"`) in the manifest descriptor.
// Requires changes to docker/distribution/manifest/manifestlist.ManifestList and .ManifestDescriptor
// and associated serialization APIs in manifestlist.go. Or split the OCI index and
// docker manifest list implementations, which would require a lot of refactoring.
var expectedOCIImageIndexSerialization = []byte(`{
   "schemaVersion": 2,
   "mediaType": "application/vnd.oci.image.index.v1+json",
   "manifests": [
      {
         "mediaType": "application/vnd.oci.image.manifest.v1+json",
         "size": 985,
         "digest": "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
         "platform": {
            "architecture": "amd64",
            "os": "linux",
            "features": [
               "sse4"
            ]
         }
      },
      {
         "mediaType": "application/vnd.oci.image.manifest.v1+json",
         "size": 985,
         "digest": "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
         "annotations": {
            "platform": "none"
         },
         "platform": {
            "architecture": "",
            "os": ""
         }
      },
      {
         "mediaType": "application/vnd.oci.image.manifest.v1+json",
         "size": 2392,
         "digest": "sha256:6346340964309634683409684360934680934608934608934608934068934608",
         "annotations": {
            "what": "for"
         },
         "platform": {
            "architecture": "sun4m",
            "os": "sunos"
         }
      }
   ]
}`)

func makeTestOCIImageIndex(t *testing.T, mediaType string) ([]ManifestDescriptor, *DeserializedManifestList) {
	manifestDescriptors := []ManifestDescriptor{
		{
			Descriptor: distribution.Descriptor{
				Digest:    "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
				Size:      985,
				MediaType: "application/vnd.oci.image.manifest.v1+json",
			},
			Platform: PlatformSpec{
				Architecture: "amd64",
				OS:           "linux",
				Features:     []string{"sse4"},
			},
		},
		{
			Descriptor: distribution.Descriptor{
				Digest:      "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
				Size:        985,
				MediaType:   "application/vnd.oci.image.manifest.v1+json",
				Annotations: map[string]string{"platform": "none"},
			},
		},
		{
			Descriptor: distribution.Descriptor{
				Digest:      "sha256:6346340964309634683409684360934680934608934608934608934068934608",
				Size:        2392,
				MediaType:   "application/vnd.oci.image.manifest.v1+json",
				Annotations: map[string]string{"what": "for"},
			},
			Platform: PlatformSpec{
				Architecture: "sun4m",
				OS:           "sunos",
			},
		},
	}

	deserialized, err := FromDescriptorsWithMediaType(manifestDescriptors, mediaType)
	require.NoError(t, err, "error creating DeserializedManifestList")

	return manifestDescriptors, deserialized
}

func TestOCIImageIndex(t *testing.T) {
	manifestDescriptors, deserialized := makeTestOCIImageIndex(t, v1.MediaTypeImageIndex)

	mediaType, canonical, _ := deserialized.Payload()

	require.Equal(t, v1.MediaTypeImageIndex, mediaType, "unexpected media type")

	// Check that the canonical field is the same as json.MarshalIndent
	// with these parameters.
	p, err := json.MarshalIndent(&deserialized.ManifestList, "", "   ")
	require.NoError(t, err, "error marshaling manifest list")
	require.Equal(t, p, canonical, "manifest bytes not equal")

	// Check that the canonical field has the expected value.
	require.Equal(t, expectedOCIImageIndexSerialization, canonical, "manifest bytes not equal to expected")

	var unmarshalled DeserializedManifestList
	require.NoError(t, json.Unmarshal(deserialized.canonical, &unmarshalled), "error unmarshaling manifest")

	require.Equal(t, &unmarshalled, deserialized, "manifests are different after unmarshaling")

	references := deserialized.References()
	require.Len(t, references, 3, "unexpected number of references")
	for i := range references {
		assert.Equal(t, references[i], manifestDescriptors[i].Descriptor, "unexpected value returned by References")
	}
}

func mediaTypeTest(t *testing.T, contentType, mediaType string, shouldError bool) {
	var m *DeserializedManifestList
	if contentType == MediaTypeManifestList {
		_, m = makeTestManifestList(t, mediaType)
	} else {
		_, m = makeTestOCIImageIndex(t, mediaType)
	}

	_, canonical, err := m.Payload()
	require.NoError(t, err, "error getting payload")

	unmarshalled, descriptor, err := distribution.UnmarshalManifest(
		contentType,
		canonical)

	if shouldError {
		require.Error(t, err, "bad content type should have produced error")
	} else {
		require.NoError(t, err, "error unmarshaling manifest")

		asManifest := unmarshalled.(*DeserializedManifestList)
		require.Equal(t, mediaType, asManifest.MediaType, "bad media type as unmarshalled")

		require.Equal(t, contentType, descriptor.MediaType, "bad media type for descriptor")

		unmarshalledMediaType, _, _ := unmarshalled.Payload()
		require.Equal(t, contentType, unmarshalledMediaType, "bad media type for payload")
	}
}

func TestMediaTypes(t *testing.T) {
	mediaTypeTest(t, MediaTypeManifestList, "", true)
	mediaTypeTest(t, MediaTypeManifestList, MediaTypeManifestList, false)
	mediaTypeTest(t, MediaTypeManifestList, MediaTypeManifestList+"XXX", true)
	mediaTypeTest(t, v1.MediaTypeImageIndex, "", false)
	mediaTypeTest(t, v1.MediaTypeImageIndex, v1.MediaTypeImageIndex, false)
	mediaTypeTest(t, v1.MediaTypeImageIndex, v1.MediaTypeImageIndex+"XXX", true)
}
