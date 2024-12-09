package schema2

import (
	"context"
	"testing"

	"github.com/docker/distribution"
	"github.com/opencontainers/go-digest"
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
    "architecture": "amd64",
    "config": {
        "AttachStderr": false,
        "AttachStdin": false,
        "AttachStdout": false,
        "Cmd": [
            "/bin/sh",
            "-c",
            "echo hi"
        ],
        "Domainname": "",
        "Entrypoint": null,
        "Env": [
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "derived=true",
            "asdf=true"
        ],
        "Hostname": "23304fc829f9",
        "Image": "sha256:4ab15c48b859c2920dd5224f92aabcd39a52794c5b3cf088fb3bbb438756c246",
        "Labels": {},
        "OnBuild": [],
        "OpenStdin": false,
        "StdinOnce": false,
        "Tty": false,
        "User": "",
        "Volumes": null,
        "WorkingDir": ""
    },
    "container": "e91032eb0403a61bfe085ff5a5a48e3659e5a6deae9f4d678daa2ae399d5a001",
    "container_config": {
        "AttachStderr": false,
        "AttachStdin": false,
        "AttachStdout": false,
        "Cmd": [
            "/bin/sh",
            "-c",
            "#(nop) CMD [\"/bin/sh\" \"-c\" \"echo hi\"]"
        ],
        "Domainname": "",
        "Entrypoint": null,
        "Env": [
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "derived=true",
            "asdf=true"
        ],
        "Hostname": "23304fc829f9",
        "Image": "sha256:4ab15c48b859c2920dd5224f92aabcd39a52794c5b3cf088fb3bbb438756c246",
        "Labels": {},
        "OnBuild": [],
        "OpenStdin": false,
        "StdinOnce": false,
        "Tty": false,
        "User": "",
        "Volumes": null,
        "WorkingDir": ""
    },
    "created": "2015-11-04T23:06:32.365666163Z",
    "docker_version": "1.9.0-dev",
    "history": [
        {
            "created": "2015-10-31T22:22:54.690851953Z",
            "created_by": "/bin/sh -c #(nop) ADD file:a3bc1e842b69636f9df5256c49c5374fb4eef1e281fe3f282c65fb853ee171c5 in /"
        },
        {
            "created": "2015-10-31T22:22:55.613815829Z",
            "created_by": "/bin/sh -c #(nop) CMD [\"sh\"]"
        },
        {
            "created": "2015-11-04T23:06:30.934316144Z",
            "created_by": "/bin/sh -c #(nop) ENV derived=true",
            "empty_layer": true
        },
        {
            "created": "2015-11-04T23:06:31.192097572Z",
            "created_by": "/bin/sh -c #(nop) ENV asdf=true",
            "empty_layer": true
        },
        {
            "created": "2015-11-04T23:06:32.083868454Z",
            "created_by": "/bin/sh -c dd if=/dev/zero of=/file bs=1024 count=1024"
        },
        {
            "created": "2015-11-04T23:06:32.365666163Z",
            "created_by": "/bin/sh -c #(nop) CMD [\"/bin/sh\" \"-c\" \"echo hi\"]",
            "empty_layer": true
        }
    ],
    "os": "linux",
    "rootfs": {
        "diff_ids": [
            "sha256:c6f988f4874bb0add23a778f753c65efe992244e148a1d2ec2a8b664fb66bbd1",
            "sha256:5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef",
            "sha256:13f53e08df5a220ab6d13c58b2bf83a59cbdc2e04d0a3f041ddf4b0ba4112d49"
        ],
        "type": "layers"
    }
}`)
	configDigest := digest.FromBytes(imgJSON)

	descriptors := []distribution.Descriptor{
		{
			Digest:    digest.Digest("sha256:a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4"),
			Size:      5312,
			MediaType: MediaTypeLayer,
		},
		{
			Digest:    digest.Digest("sha256:86e0e091d0da6bde2456dbb48306f3956bbeb2eae1b5b9a43045843f69fe4aaa"),
			Size:      235231,
			MediaType: MediaTypeLayer,
		},
		{
			Digest:    digest.Digest("sha256:b4ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4"),
			Size:      639152,
			MediaType: MediaTypeLayer,
		},
	}

	bs := &mockBlobService{descriptors: make(map[digest.Digest]distribution.Descriptor)}
	builder := NewManifestBuilder(bs, MediaTypeImageConfig, imgJSON)

	for _, d := range descriptors {
		require.NoError(t, builder.AppendReference(d), "AppendReference returned error")
	}

	built, err := builder.Build(context.Background())
	require.NoError(t, err, "Build returned error")

	// Check that the config was put in the blob store
	_, err = bs.Stat(context.Background(), configDigest)
	require.NoError(t, err, "config was not put in the blob store")

	manifest := built.(*DeserializedManifest).Manifest

	require.Equal(t, 2, manifest.Versioned.SchemaVersion, "SchemaVersion != 2")

	target := manifest.Target()
	require.Equal(t, configDigest, target.Digest, "unexpected digest in target")
	require.Equal(t, MediaTypeImageConfig, target.MediaType, "unexpected media type in target")
	require.Equal(t, int64(3153), target.Size, "unexpected size in target")

	references := manifest.References()
	expected := append([]distribution.Descriptor{manifest.Target()}, descriptors...)
	require.Equal(t, expected, references, "References() does not match the descriptors added")
}
