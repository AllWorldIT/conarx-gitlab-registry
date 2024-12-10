package storage

import (
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestPathMapper(t *testing.T) {
	for _, testcase := range []struct {
		spec     pathSpec
		expected string
		err      error
	}{
		{
			spec: manifestRevisionPathSpec{
				name:     "foo/bar",
				revision: "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
			},
			expected: "/docker/registry/v2/repositories/foo/bar/_manifests/revisions/sha256/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		},
		{
			spec: manifestRevisionLinkPathSpec{
				name:     "foo/bar",
				revision: "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
			},
			expected: "/docker/registry/v2/repositories/foo/bar/_manifests/revisions/sha256/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789/link",
		},
		{
			spec: manifestTagsPathSpec{
				name: "foo/bar",
			},
			expected: "/docker/registry/v2/repositories/foo/bar/_manifests/tags",
		},
		{
			spec: manifestTagPathSpec{
				name: "foo/bar",
				tag:  "thetag",
			},
			expected: "/docker/registry/v2/repositories/foo/bar/_manifests/tags/thetag",
		},
		{
			spec: manifestTagCurrentPathSpec{
				name: "foo/bar",
				tag:  "thetag",
			},
			expected: "/docker/registry/v2/repositories/foo/bar/_manifests/tags/thetag/current/link",
		},
		{
			spec: manifestTagIndexPathSpec{
				name: "foo/bar",
				tag:  "thetag",
			},
			expected: "/docker/registry/v2/repositories/foo/bar/_manifests/tags/thetag/index",
		},
		{
			spec: manifestTagIndexEntryPathSpec{
				name:     "foo/bar",
				tag:      "thetag",
				revision: "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
			},
			expected: "/docker/registry/v2/repositories/foo/bar/_manifests/tags/thetag/index/sha256/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		},
		{
			spec: manifestTagIndexEntryLinkPathSpec{
				name:     "foo/bar",
				tag:      "thetag",
				revision: "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
			},
			expected: "/docker/registry/v2/repositories/foo/bar/_manifests/tags/thetag/index/sha256/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789/link",
		},

		{
			spec: uploadDataPathSpec{
				name: "foo/bar",
				id:   "asdf-asdf-asdf-adsf",
			},
			expected: "/docker/registry/v2/repositories/foo/bar/_uploads/asdf-asdf-asdf-adsf/data",
		},
		{
			spec: uploadStartedAtPathSpec{
				name: "foo/bar",
				id:   "asdf-asdf-asdf-adsf",
			},
			expected: "/docker/registry/v2/repositories/foo/bar/_uploads/asdf-asdf-asdf-adsf/startedat",
		},
		{
			spec: repositoryRootPathSpec{
				name: "foo/bar",
			},
			expected: "/docker/registry/v2/repositories/foo/bar",
		},
		{
			spec: lockFilePathSpec{
				name: "test.lock",
			},
			expected: "/docker/registry/lockfiles/test.lock",
		},
	} {
		p, err := pathFor(testcase.spec)
		require.NoError(t, err, "unexpected error generating path (%T)", testcase.spec)
		require.Equal(t, testcase.expected, p, "unexpected path generated (%T)", testcase.spec)
	}

	// Add a few test cases to ensure we cover some errors

	// Specify a path that requires a revision and get a digest validation error.
	badpath, err := pathFor(manifestRevisionPathSpec{
		name: "foo/bar",
	})

	require.Error(t, err, "expected an error when mapping an invalid revision: %s", badpath)
}

func TestDigestFromPath(t *testing.T) {
	for _, testcase := range []struct {
		path       string
		expected   digest.Digest
		multilevel bool
		err        error
	}{
		{
			path:       "/docker/registry/v2/blobs/sha256/99/9943fffae777400c0344c58869c4c2619c329ca3ad4df540feda74d291dd7c86/data",
			multilevel: true,
			expected:   "sha256:9943fffae777400c0344c58869c4c2619c329ca3ad4df540feda74d291dd7c86",
			err:        nil,
		},
	} {
		result, err := digestFromPath(testcase.path)
		require.Equal(t, testcase.err, err, "unexpected error value")
		require.Equal(t, testcase.expected, result, "unexpected result value")
	}
}
