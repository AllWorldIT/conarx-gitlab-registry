package schema2

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest"
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
	if err != nil {
		t.Fatalf("error creating DeserializedManifest: %v", err)
	}

	mediaType, canonical, _ := deserialized.Payload()

	if mediaType != MediaTypeManifest {
		t.Fatalf("unexpected media type: %s", mediaType)
	}

	// Check that the canonical field is the same as json.MarshalIndent
	// with these parameters.
	p, err := json.MarshalIndent(&testManifest, "", "   ")
	if err != nil {
		t.Fatalf("error marshaling manifest: %v", err)
	}
	if !bytes.Equal(p, canonical) {
		t.Fatalf("manifest bytes not equal: %q != %q", string(canonical), string(p))
	}

	// Check that canonical field matches expected value.
	if !bytes.Equal(expectedManifestSerialization, canonical) {
		t.Fatalf("manifest bytes not equal: %q != %q", string(canonical), string(expectedManifestSerialization))
	}

	var unmarshalled DeserializedManifest
	if err := json.Unmarshal(deserialized.canonical, &unmarshalled); err != nil {
		t.Fatalf("error unmarshaling manifest: %v", err)
	}

	if !reflect.DeepEqual(&unmarshalled, deserialized) {
		t.Fatalf("manifests are different after unmarshaling: %v != %v", unmarshalled, *deserialized)
	}

	target := deserialized.Target()
	if target.Digest != "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b" {
		t.Fatalf("unexpected digest in target: %s", target.Digest.String())
	}
	if target.MediaType != MediaTypeImageConfig {
		t.Fatalf("unexpected media type in target: %s", target.MediaType)
	}
	if target.Size != 985 {
		t.Fatalf("unexpected size in target: %d", target.Size)
	}

	references := deserialized.References()
	if len(references) != 2 {
		t.Fatalf("unexpected number of references: %d", len(references))
	}

	if !reflect.DeepEqual(references[0], target) {
		t.Fatalf("first reference should be target: %v != %v", references[0], target)
	}

	// Test the second reference
	if references[1].Digest != "sha256:62d8908bee94c202b2d35224a221aaa2058318bfa9879fa541efaecba272331b" {
		t.Fatalf("unexpected digest in reference: %s", references[0].Digest.String())
	}
	if references[1].MediaType != MediaTypeLayer {
		t.Fatalf("unexpected media type in reference: %s", references[0].MediaType)
	}
	if references[1].Size != 153263 {
		t.Fatalf("unexpected size in reference: %d", references[0].Size)
	}
}

func mediaTypeTest(t *testing.T, mediaType string, shouldError bool) {
	testManifest := makeTestManifest(mediaType)

	deserialized, err := FromStruct(testManifest)
	if err != nil {
		t.Fatalf("error creating DeserializedManifest: %v", err)
	}

	unmarshalled, descriptor, err := distribution.UnmarshalManifest(
		MediaTypeManifest,
		deserialized.canonical)

	if shouldError {
		if err == nil {
			t.Fatalf("bad content type should have produced error")
		}
	} else {
		if err != nil {
			t.Fatalf("error unmarshaling manifest, %v", err)
		}

		asManifest := unmarshalled.(*DeserializedManifest)
		if asManifest.MediaType != mediaType {
			t.Fatalf("Bad media type '%v' as unmarshalled", asManifest.MediaType)
		}

		if descriptor.MediaType != MediaTypeManifest {
			t.Fatalf("Bad media type '%v' for descriptor", descriptor.MediaType)
		}

		unmarshalledMediaType, _, _ := unmarshalled.Payload()
		if unmarshalledMediaType != MediaTypeManifest {
			t.Fatalf("Bad media type '%v' for payload", unmarshalledMediaType)
		}
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
