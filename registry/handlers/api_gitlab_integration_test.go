//go:build integration && api_gitlab_test

package handlers_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	v1 "github.com/docker/distribution/registry/api/gitlab/v1"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/auth/token"
	dbtestutil "github.com/docker/distribution/registry/datastore/testutil"
	"github.com/docker/distribution/registry/handlers"
	"github.com/docker/distribution/registry/internal/testutil"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

// iso8601MsFormat is a regular expression to validate ISO8601 timestamps with millisecond precision.
var iso8601MsFormat = regexp.MustCompile(`^(?:[0-9]{4}-[0-9]{2}-[0-9]{2})?(?:[ T][0-9]{2}:[0-9]{2}:[0-9]{2})?(?:[.][0-9]{3})`)

func testGitlabApiRepositoryGet(t *testing.T, opts ...configOpt) {
	t.Helper()

	env := newTestEnv(t, opts...)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	repoName := "bar"
	repoPath := fmt.Sprintf("foo/%s", repoName)
	tagName := "latest"
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	// try to get details of non-existing repository
	u, err := env.builder.BuildGitlabV1RepositoryURL(repoRef)
	require.NoError(t, err)

	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	checkBodyHasErrorCodes(t, "wrong response body error code", resp, v2.ErrorCodeNameUnknown)

	// try getting the details of an "empty" (no tagged artifacts) repository
	seedRandomSchema2Manifest(t, env, repoPath, putByDigest)

	u, err = env.builder.BuildGitlabV1RepositoryURL(repoRef, url.Values{
		"size": []string{"self"},
	})
	require.NoError(t, err)

	resp, err = http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var r handlers.RepositoryAPIResponse
	p, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(p, &r)
	require.NoError(t, err)

	require.Equal(t, r.Name, repoName)
	require.Equal(t, r.Path, repoPath)
	require.Zero(t, *r.Size)
	require.NotEmpty(t, r.CreatedAt)
	require.Regexp(t, iso8601MsFormat, r.CreatedAt)
	require.Empty(t, r.UpdatedAt)

	// repeat, but before that push another image, this time tagged
	dm := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))
	var expectedSize int64
	for _, d := range dm.Layers() {
		expectedSize += d.Size
	}

	resp, err = http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	r = handlers.RepositoryAPIResponse{}
	p, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(p, &r)
	require.NoError(t, err)

	require.Equal(t, r.Name, repoName)
	require.Equal(t, r.Path, repoPath)
	require.Equal(t, *r.Size, expectedSize)
	require.NotEmpty(t, r.CreatedAt)
	require.Regexp(t, iso8601MsFormat, r.CreatedAt)
	require.Empty(t, r.UpdatedAt)

	// Now create a new sub repository and push a new tagged image. When called with size=self_with_descendants, the
	// returned size should have been incremented when compared with size=self.
	subRepoPath := fmt.Sprintf("%s/car", repoPath)
	m2 := seedRandomSchema2Manifest(t, env, subRepoPath, putByTag(tagName))
	for _, d := range m2.Layers() {
		expectedSize += d.Size
	}

	u, err = env.builder.BuildGitlabV1RepositoryURL(repoRef, url.Values{
		"size": []string{"self_with_descendants"},
	})
	require.NoError(t, err)

	resp, err = http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	r = handlers.RepositoryAPIResponse{}
	p, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(p, &r)
	require.NoError(t, err)

	require.Equal(t, r.Name, repoName)
	require.Equal(t, r.Path, repoPath)
	require.Equal(t, *r.Size, expectedSize)
	require.NotEmpty(t, r.CreatedAt)
	require.Regexp(t, iso8601MsFormat, r.CreatedAt)
	require.Empty(t, r.UpdatedAt)

	// use invalid `size` query param value
	u, err = env.builder.BuildGitlabV1RepositoryURL(repoRef, url.Values{
		"size": []string{"selfff"},
	})
	require.NoError(t, err)

	resp, err = http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	checkBodyHasErrorCodes(t, "wrong response body error code", resp, v1.ErrorCodeInvalidQueryParamValue)
}

func TestGitlabAPI_Repository_Get(t *testing.T) {
	testGitlabApiRepositoryGet(t)
}

func TestGitlabAPI_Repository_Get_WithCentralRepositoryCache(t *testing.T) {
	srv := testutil.RedisServer(t)
	testGitlabApiRepositoryGet(t, withRedisCache(srv.Addr()))
}

func TestGitlabAPI_Repository_Get_SizeWithDescendants_NonExistingBase(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	// creating sub repository by pushing an image to it
	targetRepoPath := "foo/bar/car"
	dm := seedRandomSchema2Manifest(t, env, targetRepoPath, putByTag("latest"))
	var expectedSize int64
	for _, d := range dm.Layers() {
		expectedSize += d.Size
	}

	// get size with descendants of base (non-existing) repository
	baseRepoPath := "foo/bar"
	baseRepoRef, err := reference.WithName(baseRepoPath)
	u, err := env.builder.BuildGitlabV1RepositoryURL(baseRepoRef, url.Values{
		"size": []string{"self_with_descendants"},
	})
	require.NoError(t, err)

	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	r := handlers.RepositoryAPIResponse{}
	p, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(p, &r)
	require.NoError(t, err)

	require.Equal(t, "bar", r.Name)
	require.Equal(t, baseRepoPath, r.Path)
	require.Equal(t, *r.Size, expectedSize)
	require.Empty(t, r.CreatedAt)
	require.Empty(t, r.UpdatedAt)
}

func TestGitlabAPI_Repository_Get_SizeWithDescendants_NonExistingTopLevel(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	baseRepoPath := "foo/bar"
	baseRepoRef, err := reference.WithName(baseRepoPath)
	u, err := env.builder.BuildGitlabV1RepositoryURL(baseRepoRef, url.Values{
		"size": []string{"self_with_descendants"},
	})
	require.NoError(t, err)

	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGitlabAPI_RepositoryTagsList(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	sortedTags := []string{
		"0062048be81e0cd57f4743158add8c589fcfdfa3",
		"2j2ar",
		"asj9e",
		"dcsl6",
		"hpgkt",
		"jyi7b",
		"jyi7b-fxt1v",
		"kav2-jyi7b",
		"kb0j5",
		"n343n",
		"sjyi7by",
		"x_y_z",
	}
	sortedTagsDesc := []string{
		"x_y_z",
		"sjyi7by",
		"n343n",
		"kb0j5",
		"kav2-jyi7b",
		"jyi7b-fxt1v",
		"jyi7b",
		"hpgkt",
		"dcsl6",
		"asj9e",
		"2j2ar",
		"0062048be81e0cd57f4743158add8c589fcfdfa3",
	}

	// shuffle tags before creation to make sure results are consistent regardless of creation order
	shuffledTags := shuffledCopy(sortedTags)

	// To simplify and speed up things we don't create N new images but rather N tags for the same new image. As result,
	// the `digest` and `size` for all returned tag details will be the same and only `name` varies. This allows us to
	// simplify the test setup and assertions.
	dgst, cfgDgst, mediaType, size := createRepositoryWithMultipleIdenticalTags(t, env, imageName.Name(), shuffledTags)

	tt := []struct {
		name                string
		queryParams         url.Values
		expectedOrderedTags []string
		expectedLinkHeader  string
		expectedStatus      int
		expectedError       *errcode.ErrorCode
	}{
		{
			name:                "no query parameters",
			expectedStatus:      http.StatusOK,
			expectedOrderedTags: sortedTags,
		},
		{
			name:           "empty last query parameter",
			queryParams:    url.Values{"last": []string{""}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:           "empty n query parameter",
			queryParams:    url.Values{"n": []string{""}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamType,
		},
		{
			name:           "empty last and n query parameters",
			queryParams:    url.Values{"last": []string{""}, "n": []string{""}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamType,
		},
		{
			name:           "empty before query parameter",
			queryParams:    url.Values{"before": []string{""}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:           "invalid sort value",
			queryParams:    url.Values{"sort": []string{"bad"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name: "invalid sort multiple values",
			// we use values.Get(key) to get the first value for a given key,
			// so we don't need to test for url.Values{"sort": []string{"name", "created_at"}} as that creates a query
			// with 2 values such as ?sort=name&sort=created_at
			queryParams:    url.Values{"sort": []string{"name,created_at"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:           "before and last mutually exclusive",
			queryParams:    url.Values{"before": []string{"hpgkt"}, "last": []string{"dcsl6"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:           "non integer n query parameter",
			queryParams:    url.Values{"n": []string{"foo"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamType,
		},
		{
			name:           "1st page",
			queryParams:    url.Values{"n": []string{"4"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"0062048be81e0cd57f4743158add8c589fcfdfa3",
				"2j2ar",
				"asj9e",
				"dcsl6",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?last=dcsl6&n=4>; rel="next"`,
		},
		{
			name:           "nth page with tagname that seems like base64 encoded",
			queryParams:    url.Values{"last": []string{"0062048be81e0cd57f4743158add8c589fcfdfa3"}, "n": []string{"4"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"2j2ar",
				"asj9e",
				"dcsl6",
				"hpgkt",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?before=2j2ar&n=4>; rel="previous", </gitlab/v1/repositories/foo/bar/tags/list/?last=hpgkt&n=4>; rel="next"`,
		},
		{
			name:           "nth page",
			queryParams:    url.Values{"last": []string{"hpgkt"}, "n": []string{"4"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"jyi7b",
				"jyi7b-fxt1v",
				"kav2-jyi7b",
				"kb0j5",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?before=jyi7b&n=4>; rel="previous", </gitlab/v1/repositories/foo/bar/tags/list/?last=kb0j5&n=4>; rel="next"`,
		},
		{
			name:           "last page",
			queryParams:    url.Values{"last": []string{"kb0j5"}, "n": []string{"4"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"n343n",
				"sjyi7by",
				"x_y_z",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?before=n343n&n=4>; rel="previous"`,
		},
		{
			name:           "1st page with sort",
			queryParams:    url.Values{"n": []string{"4"}, "sort": []string{"name"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"0062048be81e0cd57f4743158add8c589fcfdfa3",
				"2j2ar",
				"asj9e",
				"dcsl6",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?last=dcsl6&n=4&sort=name>; rel="next"`,
		},
		{
			name:           "nth page with sort",
			queryParams:    url.Values{"last": []string{"hpgkt"}, "n": []string{"4"}, "sort": []string{"name"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"jyi7b",
				"jyi7b-fxt1v",
				"kav2-jyi7b",
				"kb0j5",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?before=jyi7b&n=4&sort=name>; rel="previous", </gitlab/v1/repositories/foo/bar/tags/list/?last=kb0j5&n=4&sort=name>; rel="next"`,
		},
		{
			name:           "last page with sort",
			queryParams:    url.Values{"last": []string{"kb0j5"}, "n": []string{"4"}, "sort": []string{"name"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"n343n",
				"sjyi7by",
				"x_y_z",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?before=n343n&n=4&sort=name>; rel="previous"`,
		},
		{
			name:           "1st page sort desc",
			queryParams:    url.Values{"n": []string{"4"}, "sort": []string{"-name"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"x_y_z",
				"sjyi7by",
				"n343n",
				"kb0j5",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?last=kb0j5&n=4&sort=-name>; rel="next"`,
		},
		{
			name:           "nth page sort desc",
			queryParams:    url.Values{"last": []string{"kb0j5"}, "n": []string{"4"}, "sort": []string{"-name"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"kav2-jyi7b",
				"jyi7b-fxt1v",
				"jyi7b",
				"hpgkt",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?before=kav2-jyi7b&n=4&sort=-name>; rel="previous", </gitlab/v1/repositories/foo/bar/tags/list/?last=hpgkt&n=4&sort=-name>; rel="next"`,
		},
		{
			name:           "last page sort desc",
			queryParams:    url.Values{"last": []string{"dcsl6"}, "n": []string{"4"}, "sort": []string{"-name"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"asj9e",
				"2j2ar",
				"0062048be81e0cd57f4743158add8c589fcfdfa3",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?before=asj9e&n=4&sort=-name>; rel="previous"`,
		},
		{
			name:           "zero page size",
			queryParams:    url.Values{"n": []string{"0"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:           "negative page size",
			queryParams:    url.Values{"n": []string{"-1"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:                "page size bigger than full list",
			queryParams:         url.Values{"n": []string{"1000"}},
			expectedStatus:      http.StatusOK,
			expectedOrderedTags: sortedTags,
		},
		{
			name:                "page size bigger than full list sort desc",
			queryParams:         url.Values{"n": []string{"1000"}, "sort": []string{"-name"}},
			expectedStatus:      http.StatusOK,
			expectedOrderedTags: sortedTagsDesc,
		},
		{
			name:           "after marker",
			queryParams:    url.Values{"last": []string{"kb0j5/pic0i"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"n343n",
				"sjyi7by",
				"x_y_z",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?before=n343n&n=100>; rel="previous"`,
		},
		{
			name:           "non existent marker",
			queryParams:    url.Values{"last": []string{"does-not-exist"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"hpgkt",
				"jyi7b",
				"jyi7b-fxt1v",
				"kav2-jyi7b",
				"kb0j5",
				"n343n",
				"sjyi7by",
				"x_y_z",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?before=hpgkt&n=100>; rel="previous"`,
		},
		{
			name:           "invalid marker",
			queryParams:    url.Values{"last": []string{"-"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:           "before marker",
			queryParams:    url.Values{"before": []string{"asj9e"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"0062048be81e0cd57f4743158add8c589fcfdfa3",
				"2j2ar",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?last=2j2ar&n=100>; rel="next"`,
		},
		{
			name:           "before marker nth",
			queryParams:    url.Values{"before": []string{"jyi7b"}, "n": []string{"2"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"dcsl6",
				"hpgkt",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?before=dcsl6&n=2>; rel="previous", </gitlab/v1/repositories/foo/bar/tags/list/?last=hpgkt&n=2>; rel="next"`,
		},
		{
			name:           "before marker last page",
			queryParams:    url.Values{"before": []string{"x_y_z"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"0062048be81e0cd57f4743158add8c589fcfdfa3",
				"2j2ar",
				"asj9e",
				"dcsl6",
				"hpgkt",
				"jyi7b",
				"jyi7b-fxt1v",
				"kav2-jyi7b",
				"kb0j5",
				"n343n",
				"sjyi7by",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?last=sjyi7by&n=100>; rel="next"`,
		},
		{
			name:           "before non-existent marker",
			queryParams:    url.Values{"before": []string{"z"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"0062048be81e0cd57f4743158add8c589fcfdfa3",
				"2j2ar",
				"asj9e",
				"dcsl6",
				"hpgkt",
				"jyi7b",
				"jyi7b-fxt1v",
				"kav2-jyi7b",
				"kb0j5",
				"n343n",
				"sjyi7by",
				"x_y_z",
			},
			expectedLinkHeader: ``,
		},
		{
			name:           "before marker filtered by name",
			queryParams:    url.Values{"before": []string{"sjyi7by"}, "name": []string{"jyi7b"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"jyi7b",
				"jyi7b-fxt1v",
				"kav2-jyi7b",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?last=kav2-jyi7b&n=100&name=jyi7b>; rel="next"`,
		},
		{
			name:           "filtered by name",
			queryParams:    url.Values{"name": []string{"jyi7b"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"jyi7b",
				"jyi7b-fxt1v",
				"kav2-jyi7b",
				"sjyi7by",
			},
		},
		{
			name:           "filtered by name with literal underscore",
			queryParams:    url.Values{"name": []string{"_y_"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"x_y_z",
			},
		},
		{
			name:           "filtered by name 1st page",
			queryParams:    url.Values{"name": []string{"jyi7b"}, "n": []string{"1"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"jyi7b",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?last=jyi7b&n=1&name=jyi7b>; rel="next"`,
		},
		{
			name:           "filtered by name nth page",
			queryParams:    url.Values{"name": []string{"jyi7b"}, "last": []string{"jyi7b"}, "n": []string{"1"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"jyi7b-fxt1v",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?before=jyi7b-fxt1v&n=1&name=jyi7b>; rel="previous", </gitlab/v1/repositories/foo/bar/tags/list/?last=jyi7b-fxt1v&n=1&name=jyi7b>; rel="next"`,
		},
		{
			name:           "filtered by name last page",
			queryParams:    url.Values{"name": []string{"jyi7b"}, "last": []string{"jyi7b-fxt1v"}, "n": []string{"2"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"kav2-jyi7b",
				"sjyi7by",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?before=kav2-jyi7b&n=2&name=jyi7b>; rel="previous"`,
		},
		{
			name:           "valid name filter value characters",
			queryParams:    url.Values{"name": []string{"_Foo..Bar--abc-"}},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "invalid name filter value characters",
			queryParams:    url.Values{"name": []string{"*foo&bar%"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:           "invalid name filter value length",
			queryParams:    url.Values{"name": []string{"LwyhP4sECWBzXfWHv8dHdnPKpLSut2DChaykZHTbPerFSwLJvGrzFZ5kSdesutqImBGsdKyRA7BepsHSVrqCkxSftStrTk8UY1HCsuGd4N8ZUYFkcwWbc8GzKmLC2MHqJ"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			u, err := env.builder.BuildGitlabV1RepositoryTagsURL(imageName, test.queryParams)
			require.NoError(t, err)
			resp, err := http.Get(u)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, test.expectedStatus, resp.StatusCode)

			if test.expectedError != nil {
				checkBodyHasErrorCodes(t, "", resp, *test.expectedError)
				return
			}

			var body []*handlers.RepositoryTagResponse
			dec := json.NewDecoder(resp.Body)
			err = dec.Decode(&body)
			require.NoError(t, err)

			expectedBody := make([]*handlers.RepositoryTagResponse, 0, len(test.expectedOrderedTags))
			for _, name := range test.expectedOrderedTags {
				expectedBody = append(expectedBody, &handlers.RepositoryTagResponse{
					// this is what changes
					Name: name,
					// the rest is the same for all objects as we have a single image that all tags point to
					Digest:       dgst.String(),
					ConfigDigest: cfgDgst.String(),
					MediaType:    mediaType,
					Size:         size,
				})
			}

			// Check that created_at and published_at are not empty but updated_at is.
			// We then need to erase the created_at and published_at attributes from the response payload
			// before comparing. This is the best we can do as we have no control/insight into the
			// timestamps at which records are inserted on the DB.
			for _, d := range body {
				require.Empty(t, d.UpdatedAt)
				require.NotEmpty(t, d.CreatedAt)
				require.NotEmpty(t, d.PublishedAt)
				d.CreatedAt = ""
				d.PublishedAt = ""
			}

			require.Equal(t, expectedBody, body)

			_, ok := resp.Header["Link"]
			if test.expectedLinkHeader != "" {
				require.True(t, ok)
				require.Equal(t, test.expectedLinkHeader, resp.Header.Get("Link"))
			} else {
				require.False(t, ok, "Link header should not exist: %s", resp.Header.Get("Link"))
			}
		})
	}
}

// TestGitlabAPI_RepositoryTagsList_PublishedAt is similar to TestGitlabAPI_RepositoryTagsList but
// we focus comparisons on sorting by published_at and updated_at.
func TestGitlabAPI_RepositoryTagsList_PublishedAt(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	dbtestutil.ReloadFixtures(t, env.db, "../datastore/",
		// A Tag has a foreign key for a Manifest, which in turn references a Repository (insert order matters)
		dbtestutil.NamespacesTable, dbtestutil.RepositoriesTable, dbtestutil.BlobsTable, dbtestutil.ManifestsTable, dbtestutil.TagsTable)
	t.Cleanup(func() {
		require.NoError(t, dbtestutil.TruncateAllTables(env.db))
	})

	// see ../datastore/testdata/fixtures/tags.sql
	imageName, err := reference.WithName("usage-group-2/sub-group-1/project-1")
	require.NoError(t, err)

	// by published at
	sortedTags := []string{
		"aaaa",   // 2023-01-01T00:00:01+00:00.000000Z
		"bbbb",   // 2023-02-01T00:00:01+00:00.000000Z
		"cccc",   // 2023-03-01T00:00:01+00:00.000000Z
		"dddd",   // 2023-04-30T00:00:01.00+00.000000Z
		"latest", // 2023-04-30T00:00:01.00+00.000000Z
		"ffff",   // 2023-05-31T00:00:01+00:00.000000Z
		"eeee",   // 2023-06-30T00:00:01+00:00.000000Z
	}

	sortedTagsDesc := []string{
		"eeee",
		"ffff",
		"latest",
		"dddd",
		"cccc",
		"bbbb",
		"aaaa",
	}

	encodeFilter := func(publishedAt, tagName string) string {
		// the Link header is escaped when the bytes are written, so we need to escape it before sending the query
		return url.QueryEscape(handlers.EncodeFilter(publishedAt, tagName))
	}
	encodedTags := map[string]string{
		"aaaa":   encodeFilter("2023-01-01T00:00:01.000000Z", "aaaa"),
		"bbbb":   encodeFilter("2023-02-01T00:00:01.000000Z", "bbbb"),
		"cccc":   encodeFilter("2023-03-01T00:00:01.000000Z", "cccc"),
		"dddd":   encodeFilter("2023-04-30T00:00:01.000000Z", "dddd"),
		"latest": encodeFilter("2023-04-30T00:00:01.000000Z", "latest"),
		"ffff":   encodeFilter("2023-05-31T00:00:01.000000Z", "ffff"),
		"eeee":   encodeFilter("2023-06-30T00:00:01.000000Z", "eeee"),
	}

	tt := map[string]struct {
		descending          bool
		queryParams         url.Values
		expectedOrderedTags []string
		expectedBefore      string
		expectedLast        string
	}{
		"all tags asc": {
			queryParams:         url.Values{},
			expectedOrderedTags: sortedTags,
		},
		"all tags desc": {
			descending:          true,
			queryParams:         url.Values{},
			expectedOrderedTags: sortedTagsDesc,
		},
		"last entry name without timestamp": {
			queryParams:         url.Values{"n": []string{"2"}, "last": []string{"dddd"}},
			expectedOrderedTags: []string{"latest", "ffff"},
			expectedBefore:      encodedTags["latest"],
			expectedLast:        encodedTags["ffff"],
		},
		"before entry name without timestamp": {
			queryParams:         url.Values{"n": []string{"2"}, "before": []string{"cccc"}},
			expectedOrderedTags: []string{"aaaa", "bbbb"},
			expectedLast:        encodedTags["bbbb"],
		},
		"last entry name without timestamp desc": {
			descending:          true,
			queryParams:         url.Values{"n": []string{"2"}, "last": []string{"dddd"}},
			expectedOrderedTags: []string{"cccc", "bbbb"},
			expectedBefore:      encodedTags["cccc"],
			expectedLast:        encodedTags["bbbb"],
		},
		"before entry name without timestamp desc": {
			descending:          true,
			queryParams:         url.Values{"n": []string{"2"}, "before": []string{"aaaa"}},
			expectedOrderedTags: []string{"cccc", "bbbb"},
			expectedBefore:      encodedTags["cccc"],
			expectedLast:        encodedTags["bbbb"],
		},
		"last entry asc first page size 2": {
			queryParams:         url.Values{"n": []string{"2"}},
			expectedOrderedTags: []string{"aaaa", "bbbb"},
			expectedLast:        encodedTags["bbbb"],
		},
		"last entry asc second page size 2": {
			queryParams:         url.Values{"n": []string{"2"}, "last": []string{encodedTags["bbbb"]}},
			expectedOrderedTags: []string{"cccc", "dddd"},
			expectedBefore:      encodedTags["cccc"],
			expectedLast:        encodedTags["dddd"],
		},
		"last entry asc last page size 2": {
			queryParams:         url.Values{"n": []string{"2"}, "last": []string{encodedTags["latest"]}},
			expectedOrderedTags: []string{"ffff", "eeee"},
			expectedBefore:      encodedTags["ffff"],
		},
		"last entry desc first page size 2": {
			descending:          true,
			queryParams:         url.Values{"n": []string{"2"}},
			expectedOrderedTags: []string{"eeee", "ffff"},
			expectedLast:        encodedTags["ffff"],
		},
		"last entry desc second page size 2": {
			descending:          true,
			queryParams:         url.Values{"n": []string{"2"}, "last": []string{encodedTags["latest"]}},
			expectedOrderedTags: []string{"dddd", "cccc"},
			expectedBefore:      encodedTags["dddd"],
			expectedLast:        encodedTags["cccc"],
		},
		"last entry desc last page size 2": {
			descending:          true,
			queryParams:         url.Values{"n": []string{"2"}, "last": []string{encodedTags["cccc"]}},
			expectedOrderedTags: []string{"bbbb", "aaaa"},
			expectedBefore:      encodedTags["bbbb"],
		},
		"before entry asc last page size 2": {
			queryParams:         url.Values{"n": []string{"2"}, "before": []string{encodeFilter("2023-06-29T00:00:01.000000Z", "z")}},
			expectedOrderedTags: []string{"latest", "ffff"},
			expectedBefore:      encodedTags["latest"],
			expectedLast:        encodedTags["ffff"],
		},
		"last entry asc last page size 1": {
			queryParams:         url.Values{"n": []string{"2"}, "last": []string{encodedTags["ffff"]}},
			expectedOrderedTags: []string{"eeee"},
			expectedBefore:      encodedTags["eeee"],
		},
		"before entry asc second page size 2": {
			queryParams:         url.Values{"n": []string{"2"}, "before": []string{encodedTags["latest"]}},
			expectedOrderedTags: []string{"cccc", "dddd"},
			expectedBefore:      encodedTags["cccc"],
			expectedLast:        encodedTags["dddd"],
		},
		"before entry asc first page size 2": {
			queryParams:         url.Values{"n": []string{"2"}, "before": []string{encodedTags["cccc"]}},
			expectedOrderedTags: []string{"aaaa", "bbbb"},
			expectedLast:        encodedTags["bbbb"],
		},
		"before entry desc first page size 2": {
			descending:          true,
			queryParams:         url.Values{"n": []string{"2"}, "before": []string{encodeFilter("2022-12-31T00:00:01.000000Z", "_")}},
			expectedOrderedTags: []string{"bbbb", "aaaa"},
			expectedBefore:      encodedTags["bbbb"],
		},
		"before entry desc second page size 2": {
			descending:          true,
			queryParams:         url.Values{"n": []string{"2"}, "before": []string{encodedTags["bbbb"]}},
			expectedOrderedTags: []string{"dddd", "cccc"},
			expectedBefore:      encodedTags["dddd"],
			expectedLast:        encodedTags["cccc"],
		},

		"before entry desc 2nd last page size 2": {
			descending:          true,
			queryParams:         url.Values{"n": []string{"2"}, "before": []string{encodedTags["dddd"]}},
			expectedOrderedTags: []string{"ffff", "latest"},
			expectedLast:        encodedTags["latest"],
			expectedBefore:      encodedTags["ffff"],
		},
	}

	for tn, test := range tt {
		t.Run(tn, func(t *testing.T) {
			sort := "published_at"
			if test.descending {
				sort = "-" + sort
			}
			test.queryParams.Set("sort", sort)

			u, err := env.builder.BuildGitlabV1RepositoryTagsURL(imageName, test.queryParams)
			require.NoError(t, err)
			resp, err := http.Get(u)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			var body []*handlers.RepositoryTagResponse
			dec := json.NewDecoder(resp.Body)
			err = dec.Decode(&body)
			require.NoError(t, err)

			require.Equal(t, len(test.expectedOrderedTags), len(body))

			// the updated tags will contain a different digest and setting this up is not practical
			// we can just test for the names in order and make sure that the published_at date is what
			// we expect
			for k, receivedRepoTag := range body {
				require.Equal(t, test.expectedOrderedTags[k], receivedRepoTag.Name)
				require.NotEmpty(t, receivedRepoTag.CreatedAt)
				require.NotEmpty(t, receivedRepoTag.PublishedAt)

				if receivedRepoTag.UpdatedAt != "" {
					require.Equal(t, receivedRepoTag.UpdatedAt, receivedRepoTag.PublishedAt)
				} else {
					require.Empty(t, receivedRepoTag.UpdatedAt)
					require.Equal(t, receivedRepoTag.CreatedAt, receivedRepoTag.PublishedAt)
				}
			}

			assertLinkHeaderForPublishedAt(t, resp.Header.Get("Link"), test.expectedBefore, test.expectedLast, imageName.Name(), sort)
		})
	}
}

// assertLinkHeaderForPublishedAt formats the expected links according to the response we want from the
// repositories tags list endpoint with the escaped base64 encoded pagination marker.
func assertLinkHeaderForPublishedAt(t *testing.T, gotLink, expectedBefore, expectedLast, path, sort string) {
	t.Helper()

	if expectedBefore == "" && expectedLast == "" {
		require.Empty(t, gotLink, "Link header should not exist: %s", gotLink)
	}

	linkBase := fmt.Sprintf(`</gitlab/v1/repositories/%s/tags/list/`, path)
	gotPreviousLink := ""
	gotNextLink := ""
	links := strings.Split(gotLink, ",")

	switch len(links) {
	case 1:
		switch {
		case strings.Contains(gotLink, "previous"):
			gotPreviousLink = gotLink
		case strings.Contains(gotLink, "next"):
			gotNextLink = gotLink
		}

	case 2:
		gotPreviousLink = strings.TrimSpace(links[0])
		gotNextLink = strings.TrimSpace(links[1])
	}

	if expectedBefore != "" {
		require.NotEmpty(t, gotPreviousLink, "previous link")
		expectedPreviousLink := fmt.Sprintf("%s?before=%s&n=2&sort=%s>; rel=\"previous\"", linkBase, expectedBefore, sort)
		require.Equal(t, expectedPreviousLink, gotPreviousLink)
	} else {
		require.Empty(t, gotPreviousLink)
	}

	if expectedLast != "" {
		require.NotEmpty(t, gotNextLink, "next link")
		expectedNextLink := fmt.Sprintf("%s?last=%s&n=2&sort=%s>; rel=\"next\"", linkBase, expectedLast, sort)
		require.Equal(t, expectedNextLink, gotNextLink)
	} else {
		require.Empty(t, gotNextLink)
	}

}

// TestGitlabAPI_RepositoryTagsList_DefaultPageSize asserts that the API enforces a default page size of 100. We do it
// here instead of TestGitlabAPI_RepositoryTagsList because we have to create more than 100 tags to test this. Doing it
// in the former test would mean more complicated table test definitions, instead of the current small set of tags that
// make it easy to follow/understand the expected results.
func TestGitlabAPI_RepositoryTagsList_DefaultPageSize(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	// generate 100+1 random tag names
	tags := make([]string, 0, 101)
	for i := 0; i <= 100; i++ {
		b := make([]byte, 10)
		rand.Read(b)
		tags = append(tags, fmt.Sprintf("%x", b)[:10])
	}

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)
	createRepositoryWithMultipleIdenticalTags(t, env, imageName.Name(), tags)

	u, err := env.builder.BuildGitlabV1RepositoryTagsURL(imageName)
	require.NoError(t, err)
	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	// simply assert the number of tag detail objects in the body
	var body []*handlers.RepositoryTagResponse
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&body)
	require.NoError(t, err)

	require.Len(t, body, 100)

	// make sure the next page link starts at tag 100th
	sort.Strings(tags)
	expectedLink := fmt.Sprintf(`</gitlab/v1/repositories/%s/tags/list/?last=%s&n=100>; rel="next"`, imageName.Name(), tags[99])
	require.Equal(t, expectedLink, resp.Header.Get("Link"))
}

func TestGitlabAPI_RepositoryTagsList_RepositoryNotFound(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	u, err := env.builder.BuildGitlabV1RepositoryTagsURL(imageName)
	require.NoError(t, err)

	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Link"))
	checkBodyHasErrorCodes(t, "repository not found", resp, v2.ErrorCodeNameUnknown)
}

func TestGitlabAPI_RepositoryTagsList_EmptyRepository(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	// create repository and then delete its only tag
	tag := "latest"
	createRepository(t, env, imageName.Name(), tag)

	ref, err := reference.WithTag(imageName, tag)
	require.NoError(t, err)

	tagURL, err := env.builder.BuildTagURL(ref)
	require.NoError(t, err)

	res, err := httpDelete(tagURL)
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, http.StatusAccepted, res.StatusCode)

	// assert response
	tagsURL, err := env.builder.BuildGitlabV1RepositoryTagsURL(imageName)
	require.NoError(t, err)

	resp, err := http.Get(tagsURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	var list []handlers.RepositoryTagResponse
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&list)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Link"))
	require.Empty(t, list)
}

func TestGitlabAPI_RepositoryTagsList_OmitEmptyConfigDigest(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	repoRef, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	tag := "latest"
	seedRandomOCIImageIndex(t, env, repoRef.Name(), putByTag(tag), withoutMediaType)

	// assert response
	tagsURL, err := env.builder.BuildGitlabV1RepositoryTagsURL(repoRef)
	require.NoError(t, err)

	resp, err := http.Get(tagsURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	payload, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Contains(t, string(payload), tag)
	require.NotContains(t, string(payload), "config_digest")
}

func TestGitlabAPI_RepositoryTagsList_FilterReferrersByArtifactType(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	repoRef, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	artifactType1 := "application/vnd.dev.cosign.artifact.sbom.v1+json"
	artifactType2 := "application/vnd.dev.cosign.artifact.sig.v1+json"
	artifactType3 := "application/vnd.oras.config.v1+json"

	mfsts := make([]*ocischema.DeserializedManifest, 5)
	mfsts[0] = seedRandomOCIManifest(t, env, repoRef.Name(), putByTag("apple"))
	mfsts[1] = seedRandomOCIManifest(t, env, repoRef.Name(), putByTag("apple-sbom"),
		withSubject(mfsts[0]), withArtifactType(artifactType1))
	mfsts[2] = seedRandomOCIManifest(t, env, repoRef.Name(), putByTag("apple-sig"),
		withSubject(mfsts[0]), withArtifactType(artifactType2))
	mfsts[3] = seedRandomOCIManifest(t, env, repoRef.Name(), putByTag("apple-config"),
		withSubject(mfsts[0]), withArtifactType(artifactType3))
	mfsts[4] = seedRandomOCIManifest(t, env, repoRef.Name(), putByTag("banana"))

	// store digests from manifests
	var mb []byte
	digests := make([]string, len(mfsts))
	for i := 0; i < len(mfsts); i++ {
		_, mb, err = mfsts[i].Payload()
		require.NoError(t, err)
		digests[i] = digest.FromBytes(mb).String()
	}

	params := url.Values{
		"referrers":     {"true"},
		"referrer_type": {artifactType1 + "," + artifactType2},
	}
	tagsURL, err := env.builder.BuildGitlabV1RepositoryTagsURL(repoRef, params)
	require.NoError(t, err)

	resp, err := http.Get(tagsURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var list []handlers.RepositoryTagResponse
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&list)
	require.NoError(t, err)

	require.Equal(t, 5, len(list))
	require.Equal(t, 2, len(list[0].Referrers))
	require.Contains(t, list[0].Referrers, handlers.RepositoryTagReferrerResponse{
		Digest:       digests[1],
		ArtifactType: artifactType1,
	})
	require.Contains(t, list[0].Referrers, handlers.RepositoryTagReferrerResponse{
		Digest:       digests[2],
		ArtifactType: artifactType2,
	})
}

func TestGitlabAPI_RepositoryTagsList_IncludeReferrers(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	repoRef, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	artifactType := "application/vnd.dev.cosign.artifact.sbom.v1+json"

	mfst := seedRandomOCIManifest(t, env, repoRef.Name(), putByTag("apple"))
	mfstRef1 := seedRandomOCIManifest(t, env, repoRef.Name(), putByTag("apple-sig-1"), withSubject(mfst))
	mfstRef2 := seedRandomOCIManifest(t, env, repoRef.Name(), putByTag("apple-sig-2"),
		withSubject(mfst), withArtifactType(artifactType))
	seedRandomOCIManifest(t, env, repoRef.Name(), putByTag("banana"))

	params := url.Values{
		"referrers": {"true"},
	}
	tagsURL, err := env.builder.BuildGitlabV1RepositoryTagsURL(repoRef, params)
	require.NoError(t, err)

	resp, err := http.Get(tagsURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var list []handlers.RepositoryTagResponse
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&list)
	require.NoError(t, err)

	m := make(map[string]handlers.RepositoryTagResponse)
	for _, tag := range list {
		m[tag.Name] = tag
	}
	require.Equal(t, 2, len(m["apple"].Referrers))
	require.Equal(t, 0, len(m["apple-sig"].Referrers))
	require.Equal(t, 0, len(m["banana"].Referrers))

	// check ref digests match signature digests
	_, mb1, err := mfstRef1.Payload()
	require.NoError(t, err)
	_, mb2, err := mfstRef2.Payload()
	require.NoError(t, err)

	dgst1, dgst2 := digest.FromBytes(mb1), digest.FromBytes(mb2)
	require.Contains(t, m["apple"].Referrers, handlers.RepositoryTagReferrerResponse{
		Digest:       dgst1.String(),
		ArtifactType: mfstRef1.Manifest.Config.MediaType,
	})
	require.Contains(t, m["apple"].Referrers, handlers.RepositoryTagReferrerResponse{
		Digest:       dgst2.String(),
		ArtifactType: artifactType,
	})
}

func TestGitlabAPI_RepositoryTagsList_DoNotIncludeReferrersByDefault(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	repoRef, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	artifactType := "application/vnd.dev.cosign.artifact.sbom.v1+json"

	mfst := seedRandomOCIManifest(t, env, repoRef.Name(), putByTag("apple"))
	seedRandomOCIManifest(t, env, repoRef.Name(), putByTag("apple-sig-1"), withSubject(mfst))
	seedRandomOCIManifest(t, env, repoRef.Name(), putByTag("apple-sig-2"),
		withSubject(mfst), withArtifactType(artifactType))
	seedRandomOCIManifest(t, env, repoRef.Name(), putByTag("banana"))

	// no referrers returned by default
	tagsURL, err := env.builder.BuildGitlabV1RepositoryTagsURL(repoRef)
	require.NoError(t, err)

	resp, err := http.Get(tagsURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var list []handlers.RepositoryTagResponse
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&list)
	require.NoError(t, err)

	m := make(map[string]handlers.RepositoryTagResponse)
	for _, tag := range list {
		m[tag.Name] = tag
	}
	require.Equal(t, 0, len(m["apple"].Referrers))
	require.Equal(t, 0, len(m["apple-sig"].Referrers))
	require.Equal(t, 0, len(m["banana"].Referrers))

	// no referrers returned if `referrers` param is set to something other than "true"
	params := url.Values{
		"referrers": {"false"},
	}
	tagsURL, err = env.builder.BuildGitlabV1RepositoryTagsURL(repoRef, params)
	require.NoError(t, err)

	resp, err = http.Get(tagsURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	dec = json.NewDecoder(resp.Body)
	err = dec.Decode(&list)
	require.NoError(t, err)

	m = make(map[string]handlers.RepositoryTagResponse)
	for _, tag := range list {
		m[tag.Name] = tag
	}
	require.Equal(t, 0, len(m["apple"].Referrers))
	require.Equal(t, 0, len(m["apple-sig"].Referrers))
	require.Equal(t, 0, len(m["banana"].Referrers))
}

func TestGitlabAPI_SubRepositoryList(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	sortedReposWithTag := []string{
		"foo/bar",
		"foo/bar/a",
		"foo/bar/b",
		"foo/bar/b/c",
	}

	baseRepoName, err := reference.WithName("foo/bar")

	repoWithoutTag := "foo/bar/b2"

	require.NoError(t, err)
	tagName := "latest"
	// seed repos with the same base path foo/bar with tags
	seedMultipleRepositoriesWithTaggedManifest(t, env, tagName, sortedReposWithTag)
	// seed a repo under the same base path foo/bar but without tags
	seedRandomSchema2Manifest(t, env, repoWithoutTag, putByDigest)

	tt := []struct {
		name               string
		queryParams        url.Values
		expectedRepoPaths  []string
		expectedLinkHeader string
		expectedStatus     int
		expectedError      *errcode.ErrorCode
	}{
		{
			name:              "no query parameters",
			expectedStatus:    http.StatusOK,
			expectedRepoPaths: sortedReposWithTag,
		},
		{
			name:           "empty last query parameter",
			queryParams:    url.Values{"last": []string{""}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:           "empty n query parameter",
			queryParams:    url.Values{"n": []string{""}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamType,
		},
		{
			name:           "empty last and n query parameters",
			queryParams:    url.Values{"last": []string{""}, "n": []string{""}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamType,
		},
		{
			name:           "non integer n query parameter",
			queryParams:    url.Values{"n": []string{"foo"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamType,
		},
		{
			name:               "1st page",
			queryParams:        url.Values{"n": []string{"3"}},
			expectedStatus:     http.StatusOK,
			expectedRepoPaths:  sortedReposWithTag[:3],
			expectedLinkHeader: fmt.Sprintf(`</gitlab/v1/repository-paths/%s/repositories/list/?last=%s&n=3>; rel="next"`, baseRepoName.Name(), url.QueryEscape(sortedReposWithTag[2])),
		},
		{
			name:               "nth page",
			queryParams:        url.Values{"last": []string{"foo/bar"}, "n": []string{"2"}},
			expectedStatus:     http.StatusOK,
			expectedRepoPaths:  sortedReposWithTag[1:3],
			expectedLinkHeader: fmt.Sprintf(`</gitlab/v1/repository-paths/%s/repositories/list/?last=%s&n=2>; rel="next"`, baseRepoName.Name(), url.QueryEscape(sortedReposWithTag[2])),
		},
		{
			name:              "last page",
			queryParams:       url.Values{"last": []string{"foo/bar/b/c"}, "n": []string{"4"}},
			expectedStatus:    http.StatusOK,
			expectedRepoPaths: []string{},
		},
		{
			name:           "zero page size",
			queryParams:    url.Values{"n": []string{"0"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:           "negative page size",
			queryParams:    url.Values{"n": []string{"-1"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:              "page size bigger than full list",
			queryParams:       url.Values{"n": []string{"1000"}},
			expectedStatus:    http.StatusOK,
			expectedRepoPaths: sortedReposWithTag,
		},
		{
			name:              "non existent marker sort",
			queryParams:       url.Values{"last": []string{"foo/bar/0"}},
			expectedStatus:    http.StatusOK,
			expectedRepoPaths: sortedReposWithTag[1:],
		},
		{
			name:           "invalid marker format",
			queryParams:    url.Values{"last": []string{":"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			u, err := env.builder.BuildGitlabV1SubRepositoriesURL(baseRepoName, test.queryParams)
			require.NoError(t, err)
			resp, err := http.Get(u)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, test.expectedStatus, resp.StatusCode)

			if test.expectedError != nil {
				checkBodyHasErrorCodes(t, "", resp, *test.expectedError)
				return
			}

			var body []*handlers.RepositoryAPIResponse
			dec := json.NewDecoder(resp.Body)
			err = dec.Decode(&body)
			require.NoError(t, err)
			expectedBody := make([]*handlers.RepositoryAPIResponse, 0, len(test.expectedRepoPaths))
			for _, path := range test.expectedRepoPaths {
				splitPath := strings.Split(path, "/")
				expectedBody = append(expectedBody, &handlers.RepositoryAPIResponse{
					Name:          splitPath[len(splitPath)-1],
					Path:          path,
					Size:          nil,
					SizePrecision: "",
				})
			}
			// Check that created_at is not empty but updated_at is. We then need to erase the created_at attribute from
			// the response payload before comparing. This is the best we can do as we have no control/insight into the
			// timestamps at which records are inserted on the DB.
			for _, d := range body {
				require.Empty(t, d.UpdatedAt)
				require.NotEmpty(t, d.CreatedAt)
				d.CreatedAt = ""
			}

			require.Equal(t, expectedBody, body)
			require.Equal(t, test.expectedLinkHeader, resp.Header.Get("Link"))
		})
	}
}

// TestGitlabAPI_SubRepositoryList_DefaultPageSize asserts that the API enforces a default page size of 100. We do it
// here instead of TestGitlabAPI_SubRepositoryList because we have to create more than 100 repositories
// w/tags to test this. Doing it in the former test would mean more complicated table test definitions,
// instead of the current small set of repositories w/tags that make it easy to follow/understand the expected results.
func TestGitlabAPI_SubRepositoryList_DefaultPageSize(t *testing.T) {

	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	baseRepoPath := "foo/bar"
	baseRepoName, err := reference.WithName(baseRepoPath)

	// generate 100+1 repos with tagged images
	reposWithTag := make([]string, 0, 101)
	reposWithTag = append(reposWithTag, baseRepoPath)
	for i := 0; i <= 100; i++ {
		reposWithTag = append(reposWithTag, fmt.Sprintf(baseRepoPath+"/%d", i))
	}
	require.NoError(t, err)

	// seed repos of the same base path foo/bar but with a tagged manifest
	tagName := "latest"
	seedMultipleRepositoriesWithTaggedManifest(t, env, tagName, reposWithTag)

	u, err := env.builder.BuildGitlabV1SubRepositoriesURL(baseRepoName)
	require.NoError(t, err)
	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	// simply assert the number of repositories in the body
	var body []*handlers.RepositoryAPIResponse
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&body)
	require.NoError(t, err)

	require.Len(t, body, 100)

	// make sure the next page link starts at repo 100th
	sort.Strings(reposWithTag)
	expectedLink := fmt.Sprintf(`</gitlab/v1/repository-paths/%s/repositories/list/?last=%s&n=100>; rel="next"`, baseRepoName.Name(), url.QueryEscape(reposWithTag[99]))
	require.Equal(t, expectedLink, resp.Header.Get("Link"))
}

func TestGitlabAPI_SubRepositoryList_EmptyTagRepository(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	baseRepoName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	// create repository and then delete its only tag
	tag := "latest"
	createRepository(t, env, baseRepoName.Name(), tag)

	ref, err := reference.WithTag(baseRepoName, tag)
	require.NoError(t, err)

	tagURL, err := env.builder.BuildTagURL(ref)
	require.NoError(t, err)

	res, err := httpDelete(tagURL)
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, http.StatusAccepted, res.StatusCode)

	// assert subrepositories response
	u, err := env.builder.BuildGitlabV1SubRepositoriesURL(baseRepoName)
	require.NoError(t, err)
	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body []*handlers.RepositoryAPIResponse
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&body)
	require.NoError(t, err)
	require.NotNil(t, body)
	require.ElementsMatch(t, body, []*handlers.RepositoryAPIResponse{})
}

func TestGitlabAPI_SubRepositoryList_NonExistentRepository(t *testing.T) {
	env := newTestEnv(t)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	baseRepoName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	u, err := env.builder.BuildGitlabV1SubRepositoriesURL(baseRepoName)
	require.NoError(t, err)
	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGitlabAPI_RenameRepository_WithNoBaseRepository(t *testing.T) {
	nestedRepos := []string{
		"foo/bar/a",
	}

	baseRepoName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	// create an auth token provider
	tokenProvider := NewAuthTokenProvider(t)

	// generate one full access auth token for all tests
	token := tokenProvider.TokenWithActions(fullAccessTokenWithProjectMeta(baseRepoName.Name(), baseRepoName.Name()))

	tt := []struct {
		name               string
		queryParams        url.Values
		requestBody        []byte
		expectedRespStatus int
		expectedRespError  *errcode.ErrorCode
		expectedRespBody   *handlers.RenameRepositoryAPIResponse
	}{
		{
			name:               "dry run param not set means implicit false",
			requestBody:        []byte(`{ "name" : "not-bar" }`),
			expectedRespStatus: http.StatusNoContent,
			expectedRespBody:   nil,
		},
		{
			name:               "dry run param is set explicitly to true",
			queryParams:        url.Values{"dry_run": []string{"true"}},
			requestBody:        []byte(`{ "name" : "not-bar" }`),
			expectedRespStatus: http.StatusAccepted,
			expectedRespBody:   &handlers.RenameRepositoryAPIResponse{},
		},
		{
			name:               "dry run param is set explicitly to false",
			queryParams:        url.Values{"dry_run": []string{"false"}},
			requestBody:        []byte(`{ "name" : "not-bar" }`),
			expectedRespStatus: http.StatusNoContent,
			expectedRespBody:   nil,
		},
		{
			name:               "bad json body",
			queryParams:        url.Values{"dry_run": []string{"false"}},
			requestBody:        []byte(`"name" : "not-bar"`),
			expectedRespStatus: http.StatusBadRequest,
			expectedRespError:  &v1.ErrorCodeInvalidJSONBody,
			expectedRespBody:   nil,
		},
		{
			name:               "invalid name parameter in request",
			queryParams:        url.Values{"dry_run": []string{"false"}},
			requestBody:        []byte(`{ "name" : "@@@" }`),
			expectedRespStatus: http.StatusBadRequest,
			expectedRespError:  &v1.ErrorCodeInvalidBodyParamType,
			expectedRespBody:   nil,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			// apply base app config/setup (without authorization) to allow seeding repository with test data
			env := newTestEnv(t)
			env.requireDB(t)
			t.Cleanup(env.Shutdown)

			// seed repos
			seedMultipleRepositoriesWithTaggedManifest(t, env, "latest", nestedRepos)

			// override test config/setup to use token based authorization for all proceeding requests
			srv := testutil.RedisServer(t)
			env = newTestEnv(t, withRedisCache(srv.Addr()), withTokenAuth(tokenProvider.CertPath(), defaultIssuerProps()))

			// create and execute test request
			u, err := env.builder.BuildGitlabV1RepositoryURL(baseRepoName, test.queryParams)
			require.NoError(t, err)

			req, err := http.NewRequest(http.MethodPatch, u, bytes.NewReader(test.requestBody))
			require.NoError(t, err)

			// attach authourization header to request
			req = tokenProvider.RequestWithAuthToken(req, token)

			// make request
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			// assert results
			require.Equal(t, test.expectedRespStatus, resp.StatusCode)
			if test.expectedRespError != nil {
				checkBodyHasErrorCodes(t, "", resp, *test.expectedRespError)
				return
			}
			// assert reponses with body are valid
			var body *handlers.RenameRepositoryAPIResponse
			err = json.NewDecoder(resp.Body).Decode(&body)
			if test.expectedRespBody != nil {
				require.NoError(t, err)
				// assert that the TTL parameter is set and is within 60 seconds
				requireRenameTTLInRange(t, body.TTL, 60*time.Second)
				// set the TTL parameter to zero value to avoid test time drift comparison
				body.TTL = time.Time{}
			}
			require.Equal(t, test.expectedRespBody, body)
		})
	}
}

func TestGitlabAPI_RenameRepository_WithBaseRepository(t *testing.T) {
	nestedRepos := []string{
		"foo/bar",
		"foo/bar/a",
	}

	baseRepoName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	// create an auth token provider
	tokenProvider := NewAuthTokenProvider(t)
	// generate one full access auth token for all tests
	token := tokenProvider.TokenWithActions(fullAccessTokenWithProjectMeta(baseRepoName.Name(), baseRepoName.Name()))

	tt := []struct {
		name               string
		queryParams        url.Values
		requestBody        []byte
		expectedRespStatus int
		expectedRespError  *errcode.ErrorCode
		expectedRespBody   *handlers.RenameRepositoryAPIResponse
	}{
		{
			name:               "dry run param not set means implicit false",
			requestBody:        []byte(`{ "name" : "not-bar" }`),
			expectedRespStatus: http.StatusNoContent,
			expectedRespBody:   nil,
		},
		{
			name:               "dry run param is set explicitly to true",
			queryParams:        url.Values{"dry_run": []string{"true"}},
			requestBody:        []byte(`{ "name" : "not-bar" }`),
			expectedRespStatus: http.StatusAccepted,
			expectedRespBody:   &handlers.RenameRepositoryAPIResponse{},
		},
		{
			name:               "dry run param is set explicitly to false",
			queryParams:        url.Values{"dry_run": []string{"false"}},
			requestBody:        []byte(`{ "name" : "not-bar" }`),
			expectedRespStatus: http.StatusNoContent,
			expectedRespBody:   nil,
		},
		{
			name:               "bad json body",
			queryParams:        url.Values{"dry_run": []string{"false"}},
			requestBody:        []byte(`"name" : "not-bar"`),
			expectedRespStatus: http.StatusBadRequest,
			expectedRespError:  &v1.ErrorCodeInvalidJSONBody,
			expectedRespBody:   nil,
		},
		{
			name:               "invalid name parameter in request",
			queryParams:        url.Values{"dry_run": []string{"false"}},
			requestBody:        []byte(`{ "name" : "@@@" }`),
			expectedRespStatus: http.StatusBadRequest,
			expectedRespError:  &v1.ErrorCodeInvalidBodyParamType,
			expectedRespBody:   nil,
		},
		{
			name:               "conflicting rename",
			queryParams:        url.Values{"dry_run": []string{"false"}},
			requestBody:        []byte(`{ "name" : "bar" }`),
			expectedRespStatus: http.StatusConflict,
			expectedRespError:  &v1.ErrorCodeRenameConflict,
			expectedRespBody:   nil,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			// apply base app config/setup (without authorization) to allow seeding repository with test data
			env := newTestEnv(t)
			env.requireDB(t)
			t.Cleanup(env.Shutdown)

			// seed repos
			seedMultipleRepositoriesWithTaggedManifest(t, env, "latest", nestedRepos)

			// override test config/setup to use token based authorization for all proceeding requests
			srv := testutil.RedisServer(t)
			env = newTestEnv(t, withRedisCache(srv.Addr()), withTokenAuth(tokenProvider.CertPath(), defaultIssuerProps()))

			// create request
			u, err := env.builder.BuildGitlabV1RepositoryURL(baseRepoName, test.queryParams)
			require.NoError(t, err)

			req, err := http.NewRequest(http.MethodPatch, u, bytes.NewReader(test.requestBody))
			require.NoError(t, err)

			// attach authourization header to request
			req = tokenProvider.RequestWithAuthToken(req, token)

			// execute request
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			// assert results
			require.Equal(t, test.expectedRespStatus, resp.StatusCode)
			if test.expectedRespError != nil {
				checkBodyHasErrorCodes(t, "", resp, *test.expectedRespError)
				return
			}
			// assert reponses with body are valid
			var body *handlers.RenameRepositoryAPIResponse
			err = json.NewDecoder(resp.Body).Decode(&body)
			if test.expectedRespBody != nil {
				require.NoError(t, err)
				// assert that the TTL parameter is set and is within 60 seconds
				requireRenameTTLInRange(t, body.TTL, 60*time.Second)
				// set the TTL parameter to zero to avoid test time drift comparison
				body.TTL = time.Time{}
			}
			require.Equal(t, test.expectedRespBody, body)
		})
	}
}

func TestGitlabAPI_RenameRepository_WithoutRedis(t *testing.T) {
	env := newTestEnv(t)
	env.requireDB(t)
	t.Cleanup(env.Shutdown)

	baseRepoName, err := reference.WithName("foo/foo")
	require.NoError(t, err)

	// create and execute test request
	u, err := env.builder.BuildGitlabV1RepositoryURL(baseRepoName, url.Values{"dry_run": []string{"false"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPatch, u, bytes.NewReader([]byte(`{"name" : "not-bar"}`)))
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert results
	checkBodyHasErrorCodes(t, "", resp, v1.ErrorCodeNotImplemented)
}

func TestGitlabAPI_RenameRepository_Namespace_Empty(t *testing.T) {
	// create an auth token provider
	tokenProvider := NewAuthTokenProvider(t)

	// config/setup to use token based
	// authorization for all proceeding requests
	env := newTestEnv(t, withRedisCache(testutil.RedisServer(t).Addr()), withTokenAuth(tokenProvider.CertPath(), defaultIssuerProps()))
	env.requireDB(t)
	t.Cleanup(env.Shutdown)

	baseRepoName, err := reference.WithName("foo/foo")
	require.NoError(t, err)

	// create and execute test request
	u, err := env.builder.BuildGitlabV1RepositoryURL(baseRepoName, url.Values{"dry_run": []string{"false"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPatch, u, bytes.NewReader([]byte(`{"name" : "not-bar"}`)))
	require.NoError(t, err)

	// attach authourization header to request
	req = tokenProvider.RequestWithAuthActions(req, fullAccessTokenWithProjectMeta(baseRepoName.Name(), baseRepoName.Name()))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert results
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGitlabAPI_RenameRepository_Namespace_Exist(t *testing.T) {

	// apply base app config/setup (without authorization) to allow seeding repository with test data
	env := newTestEnv(t)
	env.requireDB(t)
	t.Cleanup(env.Shutdown)

	// seed a repo into a project namespace "foo/bar"
	repoPath := "foo/bar/existing-repo"
	_, err := reference.WithName(repoPath)
	require.NoError(t, err)

	tagname := "latest"
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagname))

	// create an auth token provider
	tokenProvider := NewAuthTokenProvider(t)

	// override config/setup to use token based
	// authorization for all proceeding requests
	env = newTestEnv(t, withRedisCache(testutil.RedisServer(t).Addr()), withTokenAuth(tokenProvider.CertPath(), defaultIssuerProps()))

	// rename a non existing path (i.e. a path with no associated repositories or sub repositories)
	// under the seeded namespace "foo/bar"
	baseRepoName, err := reference.WithName("foo/bar/non-existing-repo")
	require.NoError(t, err)

	// create and execute test request
	u, err := env.builder.BuildGitlabV1RepositoryURL(baseRepoName, url.Values{"dry_run": []string{"false"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPatch, u, bytes.NewReader([]byte(`{"name" : "new-name"}`)))
	require.NoError(t, err)

	// attach authourization header to request
	req = tokenProvider.RequestWithAuthActions(req, fullAccessTokenWithProjectMeta(baseRepoName.Name(), baseRepoName.Name()))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert results
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGitlabAPI_RenameRepository_LeaseTaken(t *testing.T) {

	// apply base app config/setup (without authorization) to allow seeding repository with test data
	env := newTestEnv(t)
	env.requireDB(t)
	t.Cleanup(env.Shutdown)

	// seed two repos in the same namespace
	firstRepoPath := "foo/bar"
	secondRepoPath := "foo/foo"
	firstRepo, err := reference.WithName(firstRepoPath)
	require.NoError(t, err)
	secondRepo, err := reference.WithName(secondRepoPath)
	require.NoError(t, err)

	tagname := "latest"
	seedRandomSchema2Manifest(t, env, firstRepoPath, putByTag(tagname))
	seedRandomSchema2Manifest(t, env, secondRepoPath, putByTag(tagname))

	// override registry config/setup to use token based authorization for all proceeding requests
	tokenProvider := NewAuthTokenProvider(t)
	env = newTestEnv(t, withRedisCache(testutil.RedisServer(t).Addr()), withTokenAuth(tokenProvider.CertPath(), defaultIssuerProps()))

	// obtain lease for renaming the "bar" in "foo/bar" to "not-bar"
	u, err := env.builder.BuildGitlabV1RepositoryURL(firstRepo, url.Values{"dry_run": []string{"true"}})
	require.NoError(t, err)
	fiirstReq, err := http.NewRequest(http.MethodPatch, u, bytes.NewReader([]byte(`{"name" : "not-bar"}`)))
	require.NoError(t, err)
	// attach authourization header to request
	fiirstReq = tokenProvider.RequestWithAuthActions(fiirstReq, fullAccessTokenWithProjectMeta(firstRepo.Name(), firstRepo.Name()))

	// try to obtain lease for renaming the "foo" in "foo/foo" to "not-bar"
	u, err = env.builder.BuildGitlabV1RepositoryURL(secondRepo, url.Values{"dry_run": []string{"true"}})
	require.NoError(t, err)
	secondReq, err := http.NewRequest(http.MethodPatch, u, bytes.NewReader([]byte(`{"name" : "not-bar"}`)))
	require.NoError(t, err)
	// attach authourization header to request
	secondReq = tokenProvider.RequestWithAuthActions(secondReq, fullAccessTokenWithProjectMeta(secondRepo.Name(), secondRepo.Name()))

	// send first request
	resp, err := http.DefaultClient.Do(fiirstReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert that the lease was obtained
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	var body *handlers.RenameRepositoryAPIResponse
	err = json.NewDecoder(resp.Body).Decode(&body)
	require.NoError(t, err)
	requireRenameTTLInRange(t, body.TTL, 60*time.Second)

	// send second request
	resp, err = http.DefaultClient.Do(secondReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert there is a conflict obtaining the lease
	checkBodyHasErrorCodes(t, "", resp, v1.ErrorCodeRenameConflict)
}

func TestGitlabAPI_RenameRepository_LeaseTaken_Nested(t *testing.T) {
	// apply base registry config/setup (without authorization) to allow seeding repository with test data
	env := newTestEnv(t)
	env.requireDB(t)
	t.Cleanup(env.Shutdown)

	// seed two repos in the same namespace
	firstRepoPath := "foo/bar"
	secondRepoPath := "foo/bar/zag"
	firstRepo, err := reference.WithName(firstRepoPath)
	require.NoError(t, err)
	secondRepo, err := reference.WithName(secondRepoPath)
	require.NoError(t, err)

	tagname := "latest"
	seedRandomSchema2Manifest(t, env, firstRepoPath, putByTag(tagname))
	seedRandomSchema2Manifest(t, env, secondRepoPath, putByTag(tagname))

	// override registry config/setup to use token based authorization for all proceeding requests
	tokenProvider := NewAuthTokenProvider(t)
	env = newTestEnv(t, withRedisCache(testutil.RedisServer(t).Addr()), withTokenAuth(tokenProvider.CertPath(), defaultIssuerProps()))

	// obtain lease for renaming the "bar" in "foo/bar" to "not-bar"
	u, err := env.builder.BuildGitlabV1RepositoryURL(firstRepo, url.Values{"dry_run": []string{"true"}})
	require.NoError(t, err)
	fiirstReq, err := http.NewRequest(http.MethodPatch, u, bytes.NewReader([]byte(`{"name" : "not-bar"}`)))
	require.NoError(t, err)
	// attach authourization header to request
	fiirstReq = tokenProvider.RequestWithAuthActions(fiirstReq, fullAccessTokenWithProjectMeta(firstRepo.Name(), firstRepo.Name()))

	// try to obtain lease for renaming the "zag" in "foo/bar/zag" to "not-bar"
	u, err = env.builder.BuildGitlabV1RepositoryURL(secondRepo, url.Values{"dry_run": []string{"true"}})
	require.NoError(t, err)
	secondReq, err := http.NewRequest(http.MethodPatch, u, bytes.NewReader([]byte(`{"name" : "not-bar"}`)))
	require.NoError(t, err)
	// attach authourization header to request
	secondReq = tokenProvider.RequestWithAuthActions(secondReq, fullAccessTokenWithProjectMeta(secondRepo.Name(), secondRepo.Name()))

	// send first request
	resp, err := http.DefaultClient.Do(fiirstReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert that the lease was obtained
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	body := handlers.RenameRepositoryAPIResponse{}
	err = json.NewDecoder(resp.Body).Decode(&body)
	require.NoError(t, err)
	requireRenameTTLInRange(t, body.TTL, 60*time.Second)

	// send second request
	resp, err = http.DefaultClient.Do(secondReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert there is no conflict obtaining the second lease in the presence of the first
	// assert that the lease was obtained
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	body = handlers.RenameRepositoryAPIResponse{}
	err = json.NewDecoder(resp.Body).Decode(&body)
	require.NoError(t, err)
	requireRenameTTLInRange(t, body.TTL, 60*time.Second)
}

func TestGitlabAPI_RenameRepository_NameTaken(t *testing.T) {
	// apply base registry config/setup (without authorization) to allow seeding repository with test data
	env := newTestEnv(t)
	env.requireDB(t)
	t.Cleanup(env.Shutdown)

	// seed two repos in the same namespace
	firstRepoPath := "foo/bar"
	secondRepoPath := "foo/foo"
	firstRepo, err := reference.WithName(firstRepoPath)
	require.NoError(t, err)
	secondRepo, err := reference.WithName(secondRepoPath)
	require.NoError(t, err)

	tagname := "latest"
	seedRandomSchema2Manifest(t, env, firstRepoPath, putByTag(tagname))
	seedRandomSchema2Manifest(t, env, secondRepoPath, putByTag(tagname))

	// override registry config/setup to use token based authorization for all proceeding requests
	tokenProvider := NewAuthTokenProvider(t)
	env = newTestEnv(t, withRedisCache(testutil.RedisServer(t).Addr()), withTokenAuth(tokenProvider.CertPath(), defaultIssuerProps()))

	// obtain lease for renaming the "bar" in "foo/bar" to "not-bar"
	u, err := env.builder.BuildGitlabV1RepositoryURL(firstRepo, url.Values{"dry_run": []string{"false"}})
	require.NoError(t, err)
	fiirstReq, err := http.NewRequest(http.MethodPatch, u, bytes.NewReader([]byte(`{"name" : "not-bar"}`)))
	require.NoError(t, err)
	// attach authourization header to request
	fiirstReq = tokenProvider.RequestWithAuthActions(fiirstReq, fullAccessTokenWithProjectMeta(firstRepo.Name(), firstRepo.Name()))

	// try to obtain lease for renaming the "foo" in "foo/foo" to "not-bar"
	u, err = env.builder.BuildGitlabV1RepositoryURL(secondRepo, url.Values{"dry_run": []string{"false"}})
	require.NoError(t, err)
	secondReq, err := http.NewRequest(http.MethodPatch, u, bytes.NewReader([]byte(`{"name" : "not-bar"}`)))
	require.NoError(t, err)
	// attach authourization header to request
	secondReq = tokenProvider.RequestWithAuthActions(secondReq, fullAccessTokenWithProjectMeta(secondRepo.Name(), secondRepo.Name()))

	// send first request
	resp, err := http.DefaultClient.Do(fiirstReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert that rename succeeded
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// send second request
	resp, err = http.DefaultClient.Do(secondReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert there is a conflict obtaining the lease
	checkBodyHasErrorCodes(t, "", resp, v1.ErrorCodeRenameConflict)
}

func TestGitlabAPI_RenameRepository_ExceedsLimit(t *testing.T) {
	// apply base registry config/setup (without authorization) to allow seeding repository with test data
	env := newTestEnv(t)
	env.requireDB(t)
	t.Cleanup(env.Shutdown)

	// seed 1000 + 1 sub repos of base-repo: foo/bar
	baseRepoName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	nestedRepos := make([]string, 0, 1001)
	nestedRepos = append(nestedRepos, "foo/bar")
	for i := 0; i <= 1000; i++ {
		nestedRepos = append(nestedRepos, fmt.Sprintf("foo/bar/%d", i))
	}
	seedMultipleRepositoriesWithTaggedManifest(t, env, "latest", nestedRepos)

	// override registry config/setup to use token based authorization for all proceeding requests
	tokenProvider := NewAuthTokenProvider(t)
	env = newTestEnv(t, withRedisCache(testutil.RedisServer(t).Addr()), withTokenAuth(tokenProvider.CertPath(), defaultIssuerProps()))

	// create and execute test request
	u, err := env.builder.BuildGitlabV1RepositoryURL(baseRepoName, url.Values{"dry_run": []string{"false"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPatch, u, bytes.NewReader([]byte(`{"name" : "not-bar"}`)))
	require.NoError(t, err)

	// attach authourization header to request
	req = tokenProvider.RequestWithAuthActions(req, fullAccessTokenWithProjectMeta(baseRepoName.Name(), baseRepoName.Name()))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert results
	checkBodyHasErrorCodes(t, "", resp, v1.ErrorCodeExceedsLimit)
}

func TestGitlabAPI_RenameRepository_InvalidTokenProjectPathMeta(t *testing.T) {

	baseRepoName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	// apply base app config/setup (without authorization) to allow seeding repository with test data
	env := newTestEnv(t)
	env.requireDB(t)
	t.Cleanup(env.Shutdown)

	// create an auth token provider
	tokenProvider := NewAuthTokenProvider(t)

	// seed repo
	seedRandomSchema2Manifest(t, env, baseRepoName.Name(), putByTag("latest"))

	// override test config/setup to use token based authorization for all proceeding requests
	srv := testutil.RedisServer(t)
	env = newTestEnv(t, withRedisCache(srv.Addr()), withTokenAuth(tokenProvider.CertPath(), defaultIssuerProps()))

	requestBody := []byte(`{ "name" : "not-bar" }`)

	u, err := env.builder.BuildGitlabV1RepositoryURL(baseRepoName)
	require.NoError(t, err)

	tt := []struct {
		name               string
		expectedRespStatus int
		expectedRespError  *errcode.ErrorCode
		tokenActions       []*token.ResourceActions
	}{
		{
			name:               "no project path param in token",
			expectedRespError:  &v1.ErrorCodeUnknownProjectPath,
			expectedRespStatus: http.StatusBadRequest,
			tokenActions:       fullAccessToken(baseRepoName.Name()),
		},
		{
			name:               "token project path param not issued for the specified repository",
			expectedRespError:  &v1.ErrorCodeMismatchProjectPath,
			expectedRespStatus: http.StatusBadRequest,
			tokenActions:       fullAccessTokenWithProjectMeta("different/project/from/repository", baseRepoName.Name()),
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			// create request
			req, err := http.NewRequest(http.MethodPatch, u, bytes.NewReader(requestBody))
			require.NoError(t, err)

			// attach authourization header to request
			req = tokenProvider.RequestWithAuthActions(req, test.tokenActions)

			// make request
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			// assert results
			require.Equal(t, test.expectedRespStatus, resp.StatusCode)
			checkBodyHasErrorCodes(t, "", resp, *test.expectedRespError)
		})
	}
}
