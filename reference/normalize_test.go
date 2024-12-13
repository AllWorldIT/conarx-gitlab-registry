package reference

import (
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateReferenceName(t *testing.T) {
	validRepoNames := []string{
		"docker/docker",
		"library/debian",
		"debian",
		"docker.io/docker/docker",
		"docker.io/library/debian",
		"docker.io/debian",
		"index.docker.io/docker/docker",
		"index.docker.io/library/debian",
		"index.docker.io/debian",
		"127.0.0.1:5000/docker/docker",
		"127.0.0.1:5000/library/debian",
		"127.0.0.1:5000/debian",
		"thisisthesongthatneverendsitgoesonandonandonthisisthesongthatnev",

		// This test case was moved from invalid to valid since it is valid input
		// when specified with a hostname, it removes the ambiguity from about
		// whether the value is an identifier or repository name
		"docker.io/1a3f5e7d9c1b3a5f7e9d1c3b5a7f9e1d3c5b7a9f1e3d5d7c9b1a3f5e7d9c1b3a",
	}
	invalidRepoNames := []string{
		"https://github.com/docker/docker",
		"docker/Docker",
		"-docker",
		"-docker/docker",
		"-docker.io/docker/docker",
		"docker///docker",
		"docker.io/docker/Docker",
		"docker.io/docker///docker",
		"1a3f5e7d9c1b3a5f7e9d1c3b5a7f9e1d3c5b7a9f1e3d5d7c9b1a3f5e7d9c1b3a",
	}

	for _, name := range invalidRepoNames {
		_, err := ParseNormalizedNamed(name)
		// nolint: testifylint // require-error
		assert.Errorf(t, err, "expected invalid repo name for %q", name)
	}

	for _, name := range validRepoNames {
		_, err := ParseNormalizedNamed(name)
		assert.NoErrorf(t, err, "error parsing repo name %s", name)
	}
}

func TestValidateRemoteName(t *testing.T) {
	validRepositoryNames := []string{
		// Sanity check.
		"docker/docker",

		// Allow 64-character non-hexadecimal names (hexadecimal names are forbidden).
		"thisisthesongthatneverendsitgoesonandonandonthisisthesongthatnev",

		// Allow embedded hyphens.
		"docker-rules/docker",

		// Allow multiple hyphens as well.
		"docker---rules/docker",

		// Username doc and image name docker being tested.
		"doc/docker",

		// single character names are now allowed.
		"d/docker",
		"jess/t",

		// Consecutive underscores.
		"dock__er/docker",
	}
	for _, repositoryName := range validRepositoryNames {
		_, err := ParseNormalizedNamed(repositoryName)
		// nolint: testifylint // require-error
		assert.NoErrorf(t, err, "repository name should be valid: %v", repositoryName)
	}

	invalidRepositoryNames := []string{
		// Disallow capital letters.
		"docker/Docker",

		// Only allow one slash.
		"docker///docker",

		// Disallow 64-character hexadecimal.
		"1a3f5e7d9c1b3a5f7e9d1c3b5a7f9e1d3c5b7a9f1e3d5d7c9b1a3f5e7d9c1b3a",

		// Disallow leading and trailing hyphens in namespace.
		"-docker/docker",
		"docker-/docker",
		"-docker-/docker",

		// Don't allow underscores everywhere (as opposed to hyphens).
		"____/____",

		"_docker/_docker",

		// Disallow consecutive periods.
		"dock..er/docker",
		"dock_.er/docker",
		"dock-.er/docker",

		// No repository.
		"docker/",

		// namespace too long
		"this_is_not_a_valid_namespace_because_its_lenth_is_greater_than_255_this_is_not_a_valid_namespace_because_its_lenth_is_greater_than_255_this_is_not_a_valid_namespace_because_its_lenth_is_greater_than_255_this_is_not_a_valid_namespace_because_its_lenth_is_greater_than_255/docker",
	}
	for _, repositoryName := range invalidRepositoryNames {
		_, err := ParseNormalizedNamed(repositoryName)
		assert.Errorf(t, err, "repository name should be invalid: %v", repositoryName)
	}
}

func TestParseRepositoryInfo(t *testing.T) {
	type tcase struct {
		RemoteName, FamiliarName, FullName, AmbiguousName, Domain string
	}

	tcases := []tcase{
		{
			RemoteName:    "fooo/bar",
			FamiliarName:  "fooo/bar",
			FullName:      "docker.io/fooo/bar",
			AmbiguousName: "index.docker.io/fooo/bar",
			Domain:        "docker.io",
		},
		{
			RemoteName:    "library/ubuntu",
			FamiliarName:  "ubuntu",
			FullName:      "docker.io/library/ubuntu",
			AmbiguousName: "library/ubuntu",
			Domain:        "docker.io",
		},
		{
			RemoteName:    "nonlibrary/ubuntu",
			FamiliarName:  "nonlibrary/ubuntu",
			FullName:      "docker.io/nonlibrary/ubuntu",
			AmbiguousName: "",
			Domain:        "docker.io",
		},
		{
			RemoteName:    "other/library",
			FamiliarName:  "other/library",
			FullName:      "docker.io/other/library",
			AmbiguousName: "",
			Domain:        "docker.io",
		},
		{
			RemoteName:    "private/moonbase",
			FamiliarName:  "127.0.0.1:8000/private/moonbase",
			FullName:      "127.0.0.1:8000/private/moonbase",
			AmbiguousName: "",
			Domain:        "127.0.0.1:8000",
		},
		{
			RemoteName:    "privatebase",
			FamiliarName:  "127.0.0.1:8000/privatebase",
			FullName:      "127.0.0.1:8000/privatebase",
			AmbiguousName: "",
			Domain:        "127.0.0.1:8000",
		},
		{
			RemoteName:    "private/moonbase",
			FamiliarName:  "example.com/private/moonbase",
			FullName:      "example.com/private/moonbase",
			AmbiguousName: "",
			Domain:        "example.com",
		},
		{
			RemoteName:    "privatebase",
			FamiliarName:  "example.com/privatebase",
			FullName:      "example.com/privatebase",
			AmbiguousName: "",
			Domain:        "example.com",
		},
		{
			RemoteName:    "private/moonbase",
			FamiliarName:  "example.com:8000/private/moonbase",
			FullName:      "example.com:8000/private/moonbase",
			AmbiguousName: "",
			Domain:        "example.com:8000",
		},
		{
			RemoteName:    "privatebasee",
			FamiliarName:  "example.com:8000/privatebasee",
			FullName:      "example.com:8000/privatebasee",
			AmbiguousName: "",
			Domain:        "example.com:8000",
		},
		{
			RemoteName:    "library/ubuntu-12.04-base",
			FamiliarName:  "ubuntu-12.04-base",
			FullName:      "docker.io/library/ubuntu-12.04-base",
			AmbiguousName: "index.docker.io/library/ubuntu-12.04-base",
			Domain:        "docker.io",
		},
		{
			RemoteName:    "library/foo",
			FamiliarName:  "foo",
			FullName:      "docker.io/library/foo",
			AmbiguousName: "docker.io/foo",
			Domain:        "docker.io",
		},
		{
			RemoteName:    "library/foo/bar",
			FamiliarName:  "library/foo/bar",
			FullName:      "docker.io/library/foo/bar",
			AmbiguousName: "",
			Domain:        "docker.io",
		},
		{
			RemoteName:    "store/foo/bar",
			FamiliarName:  "store/foo/bar",
			FullName:      "docker.io/store/foo/bar",
			AmbiguousName: "",
			Domain:        "docker.io",
		},
	}

	for _, tcase := range tcases {
		refStrings := []string{tcase.FamiliarName, tcase.FullName}
		if tcase.AmbiguousName != "" {
			refStrings = append(refStrings, tcase.AmbiguousName)
		}

		var refs []Named
		for _, r := range refStrings {
			named, err := ParseNormalizedNamed(r)
			require.NoError(t, err)
			refs = append(refs, named)
		}

		for _, r := range refs {
			assert.Equal(t, tcase.FamiliarName, FamiliarName(r), "invalid normalized reference for %q", r)
			assert.Equal(t, tcase.FullName, r.String(), "invalid canonical reference for %q", r)
			assert.Equal(t, tcase.Domain, Domain(r), "invalid domain for %q", r)
			assert.Equal(t, tcase.RemoteName, Path(r), "invalid remoteName for %q", r)
		}
	}
}

func TestParseReferenceWithTagAndDigest(t *testing.T) {
	shortRef := "busybox:latest@sha256:86e0e091d0da6bde2456dbb48306f3956bbeb2eae1b5b9a43045843f69fe4aaa"
	ref, err := ParseNormalizedNamed(shortRef)
	require.NoError(t, err)
	assert.Equal(t, "docker.io/library/"+shortRef, ref.String(), "invalid parsed reference for %q", ref)

	assert.Implements(t, (*NamedTagged)(nil), ref, "reference from %q should support tag", ref)
	assert.Implements(t, (*Canonical)(nil), ref, "reference from %q should support digest", ref)
	assert.Equal(t, shortRef, FamiliarString(ref), "invalid parsed reference for %q", ref)
}

func TestInvalidReferenceComponents(t *testing.T) {
	_, err := ParseNormalizedNamed("-foo")
	// nolint: testifylint // require-error
	assert.Error(t, err, "expected WithName to detect invalid name")

	// nolint: testifylint // require-error
	ref, err := ParseNormalizedNamed("busybox")
	require.NoError(t, err)

	_, err = WithTag(ref, "-foo")
	// nolint: testifylint // require-error
	assert.Error(t, err, "expected WithName to detect invalid tag")

	_, err = WithDigest(ref, digest.Digest("foo"))
	// nolint: testifylint // require-error
	assert.Error(t, err, "expected WithDigest to detect invalid digest")
}

func equalReference(r1, r2 Reference) bool {
	switch v1 := r1.(type) {
	case digestReference:
		if v2, ok := r2.(digestReference); ok {
			return v1 == v2
		}
	case repository:
		if v2, ok := r2.(repository); ok {
			return v1 == v2
		}
	case taggedReference:
		if v2, ok := r2.(taggedReference); ok {
			return v1 == v2
		}
	case canonicalReference:
		if v2, ok := r2.(canonicalReference); ok {
			return v1 == v2
		}
	case reference:
		if v2, ok := r2.(reference); ok {
			return v1 == v2
		}
	}
	return false
}

func TestParseAnyReference(t *testing.T) {
	tcases := []struct {
		Reference  string
		Equivalent string
		Expected   Reference
	}{
		{
			Reference:  "redis",
			Equivalent: "docker.io/library/redis",
		},
		{
			Reference:  "redis:latest",
			Equivalent: "docker.io/library/redis:latest",
		},
		{
			Reference:  "docker.io/library/redis:latest",
			Equivalent: "docker.io/library/redis:latest",
		},
		{
			Reference:  "redis@sha256:dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c",
			Equivalent: "docker.io/library/redis@sha256:dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c",
		},
		{
			Reference:  "docker.io/library/redis@sha256:dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c",
			Equivalent: "docker.io/library/redis@sha256:dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c",
		},
		{
			Reference:  "dmcgowan/myapp",
			Equivalent: "docker.io/dmcgowan/myapp",
		},
		{
			Reference:  "dmcgowan/myapp:latest",
			Equivalent: "docker.io/dmcgowan/myapp:latest",
		},
		{
			Reference:  "docker.io/mcgowan/myapp:latest",
			Equivalent: "docker.io/mcgowan/myapp:latest",
		},
		{
			Reference:  "dmcgowan/myapp@sha256:dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c",
			Equivalent: "docker.io/dmcgowan/myapp@sha256:dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c",
		},
		{
			Reference:  "docker.io/dmcgowan/myapp@sha256:dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c",
			Equivalent: "docker.io/dmcgowan/myapp@sha256:dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c",
		},
		{
			Reference:  "dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c",
			Expected:   digestReference("sha256:dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c"),
			Equivalent: "sha256:dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c",
		},
		{
			Reference:  "sha256:dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c",
			Expected:   digestReference("sha256:dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c"),
			Equivalent: "sha256:dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9c",
		},
		{
			Reference:  "dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9",
			Equivalent: "docker.io/library/dbcc1c35ac38df41fd2f5e4130b32ffdb93ebae8b3dbe638c23575912276fc9",
		},
		{
			Reference:  "dbcc1",
			Equivalent: "docker.io/library/dbcc1",
		},
	}

	for _, tcase := range tcases {
		var ref Reference
		var err error
		ref, err = ParseAnyReference(tcase.Reference)
		require.NoError(t, err, "Error parsing reference %s", tcase.Reference)
		require.Equal(t, ref.String(), tcase.Equivalent)

		expected := tcase.Expected
		if expected == nil {
			expected, err = Parse(tcase.Equivalent)
			require.NoError(t, err, "Error parsing reference %s", err)
		}
		require.True(t, equalReference(ref, expected), "Unexpected reference %#v, expected %#v", ref, expected)
	}
}

func TestNormalizedSplitHostname(t *testing.T) {
	testcases := []struct {
		input  string
		domain string
		name   string
	}{
		{
			input:  "test.com/foo",
			domain: "test.com",
			name:   "foo",
		},
		{
			input:  "test_com/foo",
			domain: "docker.io",
			name:   "test_com/foo",
		},
		{
			input:  "docker/migrator",
			domain: "docker.io",
			name:   "docker/migrator",
		},
		{
			input:  "test.com:8080/foo",
			domain: "test.com:8080",
			name:   "foo",
		},
		{
			input:  "test-com:8080/foo",
			domain: "test-com:8080",
			name:   "foo",
		},
		{
			input:  "foo",
			domain: "docker.io",
			name:   "library/foo",
		},
		{
			input:  "xn--n3h.com/foo",
			domain: "xn--n3h.com",
			name:   "foo",
		},
		{
			input:  "xn--n3h.com:18080/foo",
			domain: "xn--n3h.com:18080",
			name:   "foo",
		},
		{
			input:  "docker.io/foo",
			domain: "docker.io",
			name:   "library/foo",
		},
		{
			input:  "docker.io/library/foo",
			domain: "docker.io",
			name:   "library/foo",
		},
		{
			input:  "docker.io/library/foo/bar",
			domain: "docker.io",
			name:   "library/foo/bar",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.input, func(t *testing.T) {
			named, err := ParseNormalizedNamed(testcase.input)
			require.NoError(t, err)
			domain, name := SplitHostname(named)
			require.Equal(t, domain, testcase.domain)
			require.Equal(t, name, testcase.name)
		})
	}
}

func TestMatchError(t *testing.T) {
	named, err := ParseAnyReference("foo")
	require.NoError(t, err)
	_, err = FamiliarMatch("[-x]", named)
	require.Error(t, err)
}

func TestMatch(t *testing.T) {
	matchCases := []struct {
		reference string
		pattern   string
		expected  bool
	}{
		{
			reference: "foo",
			pattern:   "foo/**/ba[rz]",
			expected:  false,
		},
		{
			reference: "foo/any/bat",
			pattern:   "foo/**/ba[rz]",
			expected:  false,
		},
		{
			reference: "foo/a/bar",
			pattern:   "foo/**/ba[rz]",
			expected:  true,
		},
		{
			reference: "foo/b/baz",
			pattern:   "foo/**/ba[rz]",
			expected:  true,
		},
		{
			reference: "foo/c/baz:tag",
			pattern:   "foo/**/ba[rz]",
			expected:  true,
		},
		{
			reference: "foo/c/baz:tag",
			pattern:   "foo/*/baz:tag",
			expected:  true,
		},
		{
			reference: "foo/c/baz:tag",
			pattern:   "foo/c/baz:tag",
			expected:  true,
		},
		{
			reference: "example.com/foo/c/baz:tag",
			pattern:   "*/foo/c/baz",
			expected:  true,
		},
		{
			reference: "example.com/foo/c/baz:tag",
			pattern:   "example.com/foo/c/baz",
			expected:  true,
		},
	}
	for _, c := range matchCases {
		named, err := ParseAnyReference(c.reference)
		require.NoError(t, err)
		actual, err := FamiliarMatch(c.pattern, named)
		require.NoError(t, err)
		require.Equal(t, c.expected, actual)
	}
}
