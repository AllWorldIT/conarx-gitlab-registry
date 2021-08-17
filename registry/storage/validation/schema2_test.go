package validation_test

import (
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/registry/storage/validation"
	"github.com/docker/distribution/testutil"
	digest "github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

var (
	errUnexpectedURL = errors.New("unexpected URL on layer")
	errMissingURL    = errors.New("missing URL on layer")
	errInvalidURL    = errors.New("invalid URL on layer")
)

func TestVerifyManifest_Schema2_ForeignLayer(t *testing.T) {
	ctx := context.Background()

	registry := createRegistry(t)
	repo := makeRepository(t, registry, "test")

	manifestService, err := testutil.MakeManifestService(repo)
	require.NoError(t, err)

	layer, err := repo.Blobs(ctx).Put(ctx, schema2.MediaTypeLayer, nil)
	require.NoError(t, err)

	foreignLayer := distribution.Descriptor{
		Digest:    digest.FromString("foreignLayer-digest"),
		Size:      6323,
		MediaType: schema2.MediaTypeForeignLayer,
	}

	template := makeSchema2ManifestTemplate(t, repo)

	type testcase struct {
		BaseLayer distribution.Descriptor
		URLs      []string
		Err       error
	}

	cases := []testcase{
		{
			foreignLayer,
			nil,
			errMissingURL,
		},
		{
			// regular layers may have foreign urls
			layer,
			[]string{"http://foo/bar"},
			nil,
		},
		{
			foreignLayer,
			[]string{"file:///local/file"},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{"http://foo/bar#baz"},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{""},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{"https://foo/bar", ""},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{"", "https://foo/bar"},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{"http://nope/bar"},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{"http://foo/nope"},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{"http://foo/bar"},
			nil,
		},
		{
			foreignLayer,
			[]string{"https://foo/bar"},
			nil,
		},
	}

	for _, c := range cases {
		m := template
		l := c.BaseLayer
		l.URLs = c.URLs
		m.Layers = []distribution.Descriptor{l}
		dm, err := schema2.FromStruct(m)
		if err != nil {
			t.Error(err)
			continue
		}

		v := validation.NewSchema2Validator(
			manifestService,
			repo.Blobs(ctx),
			false,
			validation.ManifestURLs{
				Allow: regexp.MustCompile("^https?://foo"),
				Deny:  regexp.MustCompile("^https?://foo/nope"),
			})

		err = v.Validate(ctx, dm)
		if verr, ok := err.(distribution.ErrManifestVerification); ok {
			// Extract the first error
			if len(verr) == 2 {
				if _, ok = verr[1].(distribution.ErrManifestBlobUnknown); ok {
					err = verr[0]
				}
			}
		}
		require.Equal(t, c.Err, err)
	}
}

func TestVerifyManifest_Schema2_InvalidSchemaVersion(t *testing.T) {
	ctx := context.Background()

	registry := createRegistry(t)
	repo := makeRepository(t, registry, "test")

	manifestService, err := testutil.MakeManifestService(repo)
	require.NoError(t, err)

	m := makeSchema2ManifestTemplate(t, repo)
	m.Versioned.SchemaVersion = 42

	dm, err := schema2.FromStruct(m)
	require.NoError(t, err)

	v := validation.NewSchema2Validator(manifestService, repo.Blobs(ctx), false, validation.ManifestURLs{})
	err = v.Validate(ctx, dm)
	require.EqualError(t, err, fmt.Sprintf("unrecognized manifest schema version %d", m.Versioned.SchemaVersion))
}

func TestVerifyManifest_Schema2_SkipDependencyVerification(t *testing.T) {
	ctx := context.Background()

	registry := createRegistry(t)
	repo := makeRepository(t, registry, "test")

	manifestService, err := testutil.MakeManifestService(repo)
	require.NoError(t, err)

	m := makeSchema2ManifestTemplate(t, repo)
	m.Layers = []distribution.Descriptor{{Digest: digest.FromString("fake-digest")}}

	dm, err := schema2.FromStruct(m)
	require.NoError(t, err)

	v := validation.NewSchema2Validator(manifestService, repo.Blobs(ctx), true, validation.ManifestURLs{})

	err = v.Validate(ctx, dm)
	require.NoError(t, err)
}

func TestVerifyManifest_Schema2_ManifestLayer(t *testing.T) {
	ctx := context.Background()

	registry := createRegistry(t)
	repo := makeRepository(t, registry, "test")

	manifestService, err := testutil.MakeManifestService(repo)
	require.NoError(t, err)

	layer, err := repo.Blobs(ctx).Put(ctx, schema2.MediaTypeLayer, nil)
	require.NoError(t, err)

	// Test a manifest used as a layer. Looking at the original schema2 validation
	// logic it appears that schema2 manifests allow manifests as layers. So we
	// should try to preserve this rather odd behavior.
	depManifest := makeSchema2ManifestTemplate(t, repo)
	depManifest.Layers = []distribution.Descriptor{layer}

	depM, err := schema2.FromStruct(depManifest)
	require.NoError(t, err)

	mt, payload, err := depM.Payload()
	require.NoError(t, err)

	// If a manifest is used as a layer, it should have been pushed both as a
	// manifest as well as a blob.
	dgst, err := manifestService.Put(ctx, depM)
	require.NoError(t, err)

	_, err = repo.Blobs(ctx).Put(ctx, mt, payload)
	require.NoError(t, err)

	m := makeSchema2ManifestTemplate(t, repo)
	m.Layers = []distribution.Descriptor{{Digest: dgst, MediaType: mt}}

	dm, err := schema2.FromStruct(m)
	require.NoError(t, err)

	v := validation.NewSchema2Validator(manifestService, repo.Blobs(ctx), false, validation.ManifestURLs{})

	err = v.Validate(ctx, dm)
	require.NoErrorf(t, err, fmt.Sprintf("digest: %s", dgst))
}

func TestVerifyManifest_Schema2_MultipleErrors(t *testing.T) {
	ctx := context.Background()

	registry := createRegistry(t)
	repo := makeRepository(t, registry, "test")

	manifestService, err := testutil.MakeManifestService(repo)
	require.NoError(t, err)

	layer, err := repo.Blobs(ctx).Put(ctx, schema2.MediaTypeLayer, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create a manifest with three layers, two of which are missing. We should
	// see the digest of each missing layer in the error message.
	m := makeSchema2ManifestTemplate(t, repo)
	m.Layers = []distribution.Descriptor{
		{Digest: digest.FromString("fake-blob-layer"), MediaType: schema2.MediaTypeLayer},
		layer,
		{Digest: digest.FromString("fake-manifest-layer"), MediaType: schema2.MediaTypeManifest},
	}

	dm, err := schema2.FromStruct(m)
	require.NoError(t, err)

	v := validation.NewSchema2Validator(manifestService, repo.Blobs(ctx), false, validation.ManifestURLs{})

	err = v.Validate(ctx, dm)
	require.Error(t, err)

	require.Contains(t, err.Error(), m.Layers[0].Digest.String())
	require.NotContains(t, err.Error(), m.Layers[1].Digest.String())
	require.Contains(t, err.Error(), m.Layers[2].Digest.String())
}
