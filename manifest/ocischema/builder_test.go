package ocischema

import (
	"context"
	"testing"

	"github.com/docker/distribution"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockBlobService struct {
	descriptors map[digest.Digest]distribution.Descriptor
}

func (bs *mockBlobService) Stat(_ context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	if descriptor, ok := bs.descriptors[dgst]; ok {
		return descriptor, nil
	}
	return distribution.Descriptor{}, distribution.ErrBlobUnknown
}

func (*mockBlobService) Get(_ context.Context, _ digest.Digest) ([]byte, error) {
	panic("not implemented")
}

func (*mockBlobService) Open(_ context.Context, _ digest.Digest) (distribution.ReadSeekCloser, error) {
	panic("not implemented")
}

func (bs *mockBlobService) Put(_ context.Context, _ string, p []byte) (distribution.Descriptor, error) {
	d := distribution.Descriptor{
		Digest:    digest.FromBytes(p),
		Size:      int64(len(p)),
		MediaType: "application/octet-stream",
	}
	bs.descriptors[d.Digest] = d
	return d, nil
}

func (*mockBlobService) Create(_ context.Context, _ ...distribution.BlobCreateOption) (distribution.BlobWriter, error) {
	panic("not implemented")
}

func (*mockBlobService) Resume(_ context.Context, _ string) (distribution.BlobWriter, error) {
	panic("not implemented")
}

func TestBuilder(t *testing.T) {
	imgJSON := []byte(`{
    "created": "2015-10-31T22:22:56.015925234Z",
    "author": "Alyssa P. Hacker <alyspdev@example.com>",
    "architecture": "amd64",
    "os": "linux",
    "config": {
        "User": "alice",
        "ExposedPorts": {
            "8080/tcp": {}
        },
        "Env": [
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "FOO=oci_is_a",
            "BAR=well_written_spec"
        ],
        "Entrypoint": [
            "/bin/my-app-binary"
        ],
        "Cmd": [
            "--foreground",
            "--config",
            "/etc/my-app.d/default.cfg"
        ],
        "Volumes": {
            "/var/job-result-data": {},
            "/var/log/my-app-logs": {}
        },
        "WorkingDir": "/home/alice",
        "Labels": {
            "com.example.project.git.url": "https://example.com/project.git",
            "com.example.project.git.commit": "45a939b2999782a3f005621a8d0f29aa387e1d6b"
        }
    },
    "rootfs": {
      "diff_ids": [
        "sha256:c6f988f4874bb0add23a778f753c65efe992244e148a1d2ec2a8b664fb66bbd1",
        "sha256:5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef"
      ],
      "type": "layers"
    },
    "annotations": {
       "hot": "potato"
    }
    "history": [
      {
        "created": "2015-10-31T22:22:54.690851953Z",
        "created_by": "/bin/sh -c #(nop) ADD file:a3bc1e842b69636f9df5256c49c5374fb4eef1e281fe3f282c65fb853ee171c5 in /"
      },
      {
        "created": "2015-10-31T22:22:55.613815829Z",
        "created_by": "/bin/sh -c #(nop) CMD [\"sh\"]",
        "empty_layer": true
      }
    ]
}`)
	configDigest := digest.FromBytes(imgJSON)

	descriptors := []distribution.Descriptor{
		{
			Digest:      digest.Digest("sha256:a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4"),
			Size:        5312,
			MediaType:   v1.MediaTypeImageLayerGzip,
			Annotations: map[string]string{"apple": "orange", "lettuce": "wrap"},
		},
		{
			Digest:    digest.Digest("sha256:86e0e091d0da6bde2456dbb48306f3956bbeb2eae1b5b9a43045843f69fe4aaa"),
			Size:      235231,
			MediaType: v1.MediaTypeImageLayerGzip,
		},
		{
			Digest:    digest.Digest("sha256:b4ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4"),
			Size:      639152,
			MediaType: v1.MediaTypeImageLayerGzip,
		},
	}
	annotations := map[string]string{"hot": "potato"}

	bs := &mockBlobService{descriptors: make(map[digest.Digest]distribution.Descriptor)}
	builder := NewManifestBuilder(bs, imgJSON, annotations)

	for _, d := range descriptors {
		require.NoError(t, builder.AppendReference(d), "AppendReference returned error")
	}

	built, err := builder.Build(context.Background())
	require.NoError(t, err, "Build returned error")

	// Check that the config was put in the blob store
	_, err = bs.Stat(context.Background(), configDigest)
	require.NoError(t, err, "config was not put in the blob store")

	manifest := built.(*DeserializedManifest).Manifest
	require.Equal(t, "potato", manifest.Annotations["hot"], "unexpected annotation in manifest")

	require.Equal(t, 2, manifest.Versioned.SchemaVersion, "SchemaVersion != 2")

	target := manifest.Target()
	require.Equal(t, configDigest, target.Digest, "unexpected digest in target")
	require.Equal(t, v1.MediaTypeImageConfig, target.MediaType, "unexpected media type in target")
	require.Equal(t, int64(1632), target.Size, "unexpected size in target")

	references := manifest.References()
	expected := append([]distribution.Descriptor{manifest.Target()}, descriptors...)
	assert.Equal(t, expected, references, "References() does not match the descriptors added")
}
