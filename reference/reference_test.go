package reference

import (
	_ "crypto/sha256"
	_ "crypto/sha512"
	"encoding/json"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReferenceParse(t *testing.T) {
	// referenceTestcases is a unified set of testcases for
	// testing the parsing of references
	referenceTestcases := []struct {
		// input is the repository name or name component testcase
		input string
		// err is the error expected from Parse, or nil
		err error
		// repository is the string representation for the reference
		repository string
		// domain is the domain expected in the reference
		domain string
		// tag is the tag for the reference
		tag string
		// digest is the digest for the reference (enforces digest reference)
		digest string
	}{
		{
			input:      "test_com",
			repository: "test_com",
		},
		{
			input:      "test.com:tag",
			repository: "test.com",
			tag:        "tag",
		},
		{
			input:      "test.com:5000",
			repository: "test.com",
			tag:        "5000",
		},
		{
			input:      "test.com/repo:tag",
			domain:     "test.com",
			repository: "test.com/repo",
			tag:        "tag",
		},
		{
			input:      "test:5000/repo",
			domain:     "test:5000",
			repository: "test:5000/repo",
		},
		{
			input:      "test:5000/repo:tag",
			domain:     "test:5000",
			repository: "test:5000/repo",
			tag:        "tag",
		},
		{
			input:      "test:5000/repo@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			domain:     "test:5000",
			repository: "test:5000/repo",
			digest:     "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		},
		{
			input:      "test:5000/repo:tag@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			domain:     "test:5000",
			repository: "test:5000/repo",
			tag:        "tag",
			digest:     "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		},
		{
			input: "",
			err:   ErrNameEmpty,
		},
		{
			input: ":justtag",
			err:   ErrReferenceInvalidFormat,
		},
		{
			input: "@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			err:   ErrReferenceInvalidFormat,
		},
		{
			input: "repo@sha256:ffffffffffffffffffffffffffffffffff",
			err:   digest.ErrDigestInvalidLength,
		},
		{
			input: "validname@invaliddigest:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			err:   digest.ErrDigestUnsupported,
		},
		{
			input: "Uppercase:tag",
			err:   ErrNameContainsUppercase,
		},
		// FIXME "Uppercase" is incorrectly handled as a domain-name here, therefore passes.
		// See https://github.com/docker/distribution/pull/1778, and https://github.com/docker/docker/pull/20175
		// {
		//	input: "Uppercase/lowercase:tag",
		//	err:   ErrNameContainsUppercase,
		// },
		{
			input: "test:5000/Uppercase/lowercase:tag",
			err:   ErrNameContainsUppercase,
		},
		{
			input:      "lowercase:Uppercase",
			repository: "lowercase",
			tag:        "Uppercase",
		},
		{
			input: strings.Repeat("a/", 128) + "a:tag",
			err:   ErrNameTooLong,
		},
		{
			input:      strings.Repeat("a/", 127) + "a:tag-puts-this-over-max",
			domain:     "a",
			repository: strings.Repeat("a/", 127) + "a",
			tag:        "tag-puts-this-over-max",
		},
		{
			input: "aa/asdf$$^/aa",
			err:   ErrReferenceInvalidFormat,
		},
		{
			input:      "sub-dom1.foo.com/bar/baz/quux",
			domain:     "sub-dom1.foo.com",
			repository: "sub-dom1.foo.com/bar/baz/quux",
		},
		{
			input:      "sub-dom1.foo.com/bar/baz/quux:some-long-tag",
			domain:     "sub-dom1.foo.com",
			repository: "sub-dom1.foo.com/bar/baz/quux",
			tag:        "some-long-tag",
		},
		{
			input:      "b.gcr.io/test.example.com/my-app:test.example.com",
			domain:     "b.gcr.io",
			repository: "b.gcr.io/test.example.com/my-app",
			tag:        "test.example.com",
		},
		{
			input:      "xn--n3h.com/myimage:xn--n3h.com", // ☃.com in punycode
			domain:     "xn--n3h.com",
			repository: "xn--n3h.com/myimage",
			tag:        "xn--n3h.com",
		},
		{
			input:      "xn--7o8h.com/myimage:xn--7o8h.com@sha512:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", // 🐳.com in punycode
			domain:     "xn--7o8h.com",
			repository: "xn--7o8h.com/myimage",
			tag:        "xn--7o8h.com",
			digest:     "sha512:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		},
		{
			input:      "foo_bar.com:8080",
			repository: "foo_bar.com",
			tag:        "8080",
		},
		{
			input:      "foo/foo_bar.com:8080",
			domain:     "foo",
			repository: "foo/foo_bar.com",
			tag:        "8080",
		},
	}
	for _, testcase := range referenceTestcases {
		t.Run(testcase.input, func(t *testing.T) {
			repo, err := Parse(testcase.input)
			if testcase.err != nil {
				require.ErrorIs(t, err, testcase.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testcase.input, repo.String())

			if named, ok := repo.(Named); ok {
				require.Equal(t, testcase.repository, named.Name())
				domain, _ := SplitHostname(named)
				require.Equal(t, testcase.domain, domain)
			} else {
				require.Emptyf(t, testcase.repository, "expected named type, got %T", repo)
				require.Emptyf(t, testcase.domain, "expected named type, got %T", repo)
			}

			tagged, ok := repo.(Tagged)
			if testcase.tag != "" {
				if assert.Truef(t, ok, "expected tagged type, got %T", repo) {
					require.Equal(t, testcase.tag, tagged.Tag())
				}
			} else {
				require.False(t, ok, "unexpected tagged type")
			}

			digested, ok := repo.(Digested)
			if testcase.digest != "" {
				if assert.Truef(t, ok, "expected digested type, got %T", repo) {
					require.Equal(t, testcase.digest, digested.Digest().String())
				}
			} else {
				require.False(t, ok, "unexpected digested type")
			}
		})
	}
}

// TestWithNameFailure tests cases where WithName should fail. Cases where it
// should succeed are covered by TestSplitHostname, below.
func TestWithNameFailure(t *testing.T) {
	testcases := []struct {
		input string
		name  string
		err   error
	}{
		{
			name:  "empty",
			input: "",
			err:   ErrReferenceInvalidFormat,
		},
		{
			name:  "emptyJustTag",
			input: ":justtag",
			err:   ErrReferenceInvalidFormat,
		},
		{
			name:  "refInvalidRefEmptyName",
			input: "@sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			err:   ErrReferenceInvalidFormat,
		},
		{
			name:  "refInvalidDigest",
			input: "validname@invaliddigest:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			err:   ErrReferenceInvalidFormat,
		},
		{
			name:  "nameTooLong",
			input: strings.Repeat("a/", 128) + "a:tag",
			err:   ErrNameTooLong,
		},
		{
			name:  "refInvalidFormat",
			input: "aa/asdf$$^/aa",
			err:   ErrReferenceInvalidFormat,
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			_, err := WithName(testcase.input)
			require.ErrorIs(t, err, testcase.err)
		})
	}
}

func TestSplitHostname(t *testing.T) {
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
			domain: "",
			name:   "test_com/foo",
		},
		{
			input:  "test:8080/foo",
			domain: "test:8080",
			name:   "foo",
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
			input:  "xn--n3h.com:18080/foo",
			domain: "xn--n3h.com:18080",
			name:   "foo",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.input, func(t *testing.T) {
			named, err := WithName(testcase.input)
			require.NoError(t, err, "error parsing name for input: %s", testcase.input)
			domain, name := SplitHostname(named)
			assert.Equal(t, testcase.domain, domain)
			assert.Equal(t, testcase.name, name)
		})
	}
}

type serializationType struct {
	Description string
	Field       Field
}

func TestSerialization(t *testing.T) {
	testcases := []struct {
		description string
		input       string
		name        string
		tag         string
		digest      string
		err         error
	}{
		{
			description: "empty value",
			err:         ErrNameEmpty,
		},
		{
			description: "just a name",
			input:       "example.com:8000/named",
			name:        "example.com:8000/named",
		},
		{
			description: "name with a tag",
			input:       "example.com:8000/named:tagged",
			name:        "example.com:8000/named",
			tag:         "tagged",
		},
		{
			description: "name with digest",
			input:       "other.com/named@sha256:1234567890098765432112345667890098765432112345667890098765432112",
			name:        "other.com/named",
			digest:      "sha256:1234567890098765432112345667890098765432112345667890098765432112",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.description, func(t *testing.T) {
			m := map[string]string{
				"Description": testcase.description,
				"Field":       testcase.input,
			}
			b, err := json.Marshal(m)
			require.NoError(t, err)
			st := serializationType{}

			err = json.Unmarshal(b, &st)
			if testcase.err != nil {
				require.ErrorIs(t, err, testcase.err)
				return
			}
			require.NoError(t, err)

			require.Equal(t, testcase.description, st.Description, "wrong description")

			ref := st.Field.Reference()

			if named, ok := ref.(Named); ok {
				require.Equal(t, testcase.name, named.Name())
			} else {
				require.Emptyf(t, testcase.name, "expected named type, got %T", ref)
			}

			tagged, ok := ref.(Tagged)
			if testcase.tag != "" {
				if assert.Truef(t, ok, "expected tagged type, got %T", ref) {
					require.Equal(t, testcase.tag, tagged.Tag())
				}
			} else {
				require.False(t, ok, "unexpected tagged type")
			}

			digested, ok := ref.(Digested)
			if testcase.digest != "" {
				if assert.Truef(t, ok, "expected tagged type, got %T", ref) {
					require.Equal(t, testcase.digest, digested.Digest().String())
				}
			} else {
				require.False(t, ok, "unexpected digested type")
			}

			st = serializationType{
				Description: testcase.description,
				Field:       AsField(ref),
			}

			b2, err := json.Marshal(st)
			require.NoError(t, err, "error marshing serialization type")

			require.Equal(t, b, b2, "unexpected serialized value: expected %q, got %q", string(b), string(b2))

			// Ensure t.Field is not implementing "Reference" directly, getting
			// around the Reference type system
			require.NotImplements(t, (*Reference)(nil), st.Field, "field should not implement Reference interface")
		})
	}
}

func TestWithTag(t *testing.T) {
	testcases := []struct {
		name     string
		digest   digest.Digest
		tag      string
		combined string
	}{
		{
			name:     "test.com/foo",
			tag:      "tag",
			combined: "test.com/foo:tag",
		},
		{
			name:     "foo",
			tag:      "tag2",
			combined: "foo:tag2",
		},
		{
			name:     "test.com:8000/foo",
			tag:      "tag4",
			combined: "test.com:8000/foo:tag4",
		},
		{
			name:     "test.com:8000/foo",
			tag:      "TAG5",
			combined: "test.com:8000/foo:TAG5",
		},
		{
			name:     "test.com:8000/foo",
			digest:   "sha256:1234567890098765432112345667890098765",
			tag:      "TAG5",
			combined: "test.com:8000/foo:TAG5@sha256:1234567890098765432112345667890098765",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			named, err := WithName(testcase.name)
			require.NoError(t, err, "error parsing name")
			if testcase.digest != "" {
				canonical, err := WithDigest(named, testcase.digest)
				require.NoError(t, err, "error adding digest")
				named = canonical
			}

			tagged, err := WithTag(named, testcase.tag)
			require.NoError(t, err, "WithTag failed")
			assert.Equalf(t, tagged.String(), testcase.combined, "unexpected: got %q, expected %q", tagged.String(), testcase.combined)
		})
	}
}

func TestWithDigest(t *testing.T) {
	testcases := []struct {
		name     string
		digest   digest.Digest
		tag      string
		combined string
	}{
		{
			name:     "test.com/foo",
			digest:   "sha256:1234567890098765432112345667890098765",
			combined: "test.com/foo@sha256:1234567890098765432112345667890098765",
		},
		{
			name:     "foo",
			digest:   "sha256:1234567890098765432112345667890098765",
			combined: "foo@sha256:1234567890098765432112345667890098765",
		},
		{
			name:     "test.com:8000/foo",
			digest:   "sha256:1234567890098765432112345667890098765",
			combined: "test.com:8000/foo@sha256:1234567890098765432112345667890098765",
		},
		{
			name:     "test.com:8000/foo",
			digest:   "sha256:1234567890098765432112345667890098765",
			tag:      "latest",
			combined: "test.com:8000/foo:latest@sha256:1234567890098765432112345667890098765",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			named, err := WithName(testcase.name)
			require.NoError(t, err, "error parsing name")
			if testcase.tag != "" {
				tagged, err := WithTag(named, testcase.tag)
				require.NoError(t, err, "error adding tag")
				named = tagged
			}
			digested, err := WithDigest(named, testcase.digest)
			require.NoError(t, err, "WithDigest failed: %s", err)
			require.Equalf(t, digested.String(), testcase.combined, "unexpected: got %q, expected %q", digested.String(), testcase.combined)
		})
	}
}

func TestParseNamed(t *testing.T) {
	testcases := []struct {
		input  string
		domain string
		name   string
		err    error
	}{
		{
			input:  "test.com/foo",
			domain: "test.com",
			name:   "foo",
		},
		{
			input:  "test:8080/foo",
			domain: "test:8080",
			name:   "foo",
		},
		{
			input: "test_com/foo",
			err:   ErrNameNotCanonical,
		},
		{
			input: "test.com",
			err:   ErrNameNotCanonical,
		},
		{
			input: "foo",
			err:   ErrNameNotCanonical,
		},
		{
			input: "library/foo",
			err:   ErrNameNotCanonical,
		},
		{
			input:  "docker.io/library/foo",
			domain: "docker.io",
			name:   "library/foo",
		},
		// Ambiguous case, parser will add "library/" to foo
		{
			input: "docker.io/foo",
			err:   ErrNameNotCanonical,
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.input, func(t *testing.T) {
			named, err := ParseNamed(testcase.input)
			if testcase.err != nil {
				require.ErrorIs(t, err, testcase.err)
				return
			}
			require.NoError(t, err)

			domain, name := SplitHostname(named)
			assert.Equal(t, domain, testcase.domain)
			assert.Equal(t, name, testcase.name)
		})
	}
}
