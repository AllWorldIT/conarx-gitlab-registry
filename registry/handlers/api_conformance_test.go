//go:build integration && api_conformance_test

package handlers_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/notifications"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/internal/testutil"
	"github.com/docker/distribution/version"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testapiconformance runs a variety of tests against different environments
// where the external behavior of the api is expected to be equivalent.
func TestAPIConformance(t *testing.T) {
	testFuncs := []func(*testing.T, ...configOpt){
		baseURLAuth,
		baseURLPrefix,

		manifestPutSchema1ByTag,
		manifestPutSchema1ByDigest,
		manifestPutSchema2ByDigest,
		manifestPutSchema2ByDigestConfigNotAssociatedWithRepository,
		manifestPutSchema2ByDigestLayersNotAssociatedWithRepository,
		manifestPutSchema2ByTag,
		manifestPutSchema2ByTagIsIdempotent,
		manifestPutSchema2ByTagSameDigestParallelIsIdempotent,
		manifestPutSchema2MissingConfig,
		manifestPutSchema2MissingConfigAndLayers,
		manifestPutSchema2MissingLayers,
		manifestPutSchema2ReuseTagManifestToManifest,
		manifestPutSchema2ReferencesExceedLimit,
		manifestPutSchema2PayloadSizeExceedsLimit,
		manifestHeadSchema2,
		manifestHeadSchema2MissingManifest,
		manifestGetSchema2ByDigestMissingManifest,
		manifestGetSchema2ByDigestMissingRepository,
		manifestGetSchema2NoAcceptHeaders,
		manifestGetSchema2ByDigestNotAssociatedWithRepository,
		manifestGetSchema2ByTagMissingRepository,
		manifestGetSchema2ByTagMissingTag,
		manifestGetSchema2ByTagNotAssociatedWithRepository,
		manifestGetSchema2MatchingEtag,
		manifestGetSchema2NonMatchingEtag,
		manifestDeleteSchema2,
		manifestDeleteSchema2AlreadyDeleted,
		manifestDeleteSchema2Reupload,
		manifestDeleteSchema2MissingManifest,
		manifestDeleteSchema2ClearsTags,
		manifestDeleteSchema2DeleteDisabled,

		manifestDeleteTag,
		manifestDeleteTagUnknown,
		manifestDeleteTagDeleteDisabled,
		manifestDeleteTagUnknownRepository,
		manifestDeleteTagNotification,
		manifestDeleteTagNotificationWithAuth,
		manifestDeleteTagWithSameImageID,

		manifestPutSchema2WithNonDistributableLayers,
		manifestPutOCIByDigest,
		manifestPutOCIByTag,
		manifestPutOCIWithSubject,
		manifestPutOCIWithV2Subject,
		manifestPutOCIWithNonMatchingSubject,
		manifestPutOCIWithArtifactType,
		manifestGetOCIMatchingEtag,
		manifestGetOCINonMatchingEtag,

		manifestPutOCIImageIndexByDigest,
		manifestPutOCIImageIndexByTag,
		manifestGetOCIIndexMatchingEtag,
		manifestGetOCIIndexNonMatchingEtag,
		manifestPutOCIWithNonDistributableLayers,

		manifestGetManifestListFallbackToSchema2,
		manifestPutManifestListByTagSameDigestParallelIsIdempotent,

		blobHead,
		blobHeadBlobNotFound,
		blobHeadRepositoryNotFound,
		blobGet,
		blobGetBlobNotFound,
		blobGetRepositoryNotFound,
		blobDelete,
		blobDeleteAlreadyDeleted,
		blobDeleteDisabled,
		blobDeleteUnknownRepository,

		tagsGet,
		tagsGetEmptyRepository,
		tagsGetRepositoryNotFound,

		catalogGet,
		catalogGetEmpty,
		catalogGetTooLarge,
	}

	type envOpt struct {
		name                 string
		opts                 []configOpt
		webhookNotifications bool
	}

	envOpts := []envOpt{
		{
			name: "all",
			opts: make([]configOpt, 0),
		},
		{
			name:                 "with webhook notifications enabled",
			opts:                 make([]configOpt, 0),
			webhookNotifications: true,
		},
		{
			name: "with redis cache",
			opts: []configOpt{withRedisCache(testutil.RedisServer(t).Addr())},
		},
	}

	// Randomize test functions and environments to prevent failures
	// (and successes) due to order of execution effects.
	rand.Shuffle(len(testFuncs), func(i, j int) {
		testFuncs[i], testFuncs[j] = testFuncs[j], testFuncs[i]
	})

	for _, f := range testFuncs {
		rand.Shuffle(len(envOpts), func(i, j int) {
			envOpts[i], envOpts[j] = envOpts[j], envOpts[i]
		})

		for _, o := range envOpts {
			t.Run(funcName(f)+" "+o.name, func(t *testing.T) {
				// Use filesystem driver here. This way, we're able to test conformance
				// with migration mode enabled as the inmemory driver does not support
				// root directories.
				rootDir := t.TempDir()

				o.opts = append(o.opts, withFSDriver(rootDir), withAccessLog)

				if o.webhookNotifications {
					notifCfg := configuration.Notifications{
						FanoutTimeout: 3 * time.Second,
						Endpoints: []configuration.Endpoint{
							{
								Name:              t.Name(),
								Disabled:          false,
								Headers:           http.Header{"test-header": []string{t.Name()}},
								Timeout:           100 * time.Millisecond,
								Threshold:         1,
								Backoff:           100 * time.Millisecond,
								IgnoredMediaTypes: []string{"application/octet-stream"},
							},
						},
					}

					o.opts = append(o.opts, withWebhookNotifications(notifCfg))
				}

				f(t, o.opts...)
			})
		}
	}
}

func funcName(f func(*testing.T, ...configOpt)) string {
	ptr := reflect.ValueOf(f).Pointer()
	name := runtime.FuncForPC(ptr).Name()
	segments := strings.Split(name, ".")

	return segments[len(segments)-1]
}

func manifestPutSchema2ByTagIsIdempotent(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "idempotentag"
	repoPath := "schema2/idempotent"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath)

	// Build URLs and headers.
	manifestURL := buildManifestTagURL(t, env, repoPath, tagName)

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	testFunc := func() {
		resp, err := putManifest("putting manifest by tag no error", manifestURL, schema2.MediaTypeManifest, deserializedManifest.Manifest)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusCreated, resp.StatusCode)
		require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
		require.Equal(t, manifestDigestURL, resp.Header.Get("Location"))
		require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))

		if env.ns != nil {
			expectedEvent := buildEventManifestPush(schema2.MediaTypeManifest, repoPath, tagName, dgst, int64(len(payload)))
			env.ns.AssertEventNotification(t, expectedEvent)
		}
	}

	// Put the same manifest twice to test idempotentcy.
	testFunc()
	testFunc()
}

func manifestPutSchema2ByTagSameDigestParallelIsIdempotent(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName1 := "idempotentag-one"
	tagName2 := "idempotentag-two"
	repoPath := "schema2/idempotent"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath)

	// Build URLs and headers.
	manifestURL1 := buildManifestTagURL(t, env, repoPath, tagName1)
	manifestURL2 := buildManifestTagURL(t, env, repoPath, tagName2)

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	wg := &sync.WaitGroup{}
	// Put the same manifest digest with tag one and two to test idempotentcy.
	for _, manifestURL := range []string{manifestURL1, manifestURL2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := putManifest("putting manifest by tag no error", manifestURL, schema2.MediaTypeManifest, deserializedManifest.Manifest)
			assert.NoError(t, err)
			defer resp.Body.Close()
			// NOTE(prozlach): we can't use require in a goroutine.
			assert.Equal(t, http.StatusCreated, resp.StatusCode)
			assert.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
			assert.Equal(t, manifestDigestURL, resp.Header.Get("Location"))
			assert.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))
		}()
	}

	wg.Wait()
}

// proof for https://gitlab.com/gitlab-org/container-registry/-/issues/1132 where creating the same manifest list
// with the same digest in parallel can cause a race condition
func manifestPutManifestListByTagSameDigestParallelIsIdempotent(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagCount := 2
	repoPath := "schema2/manifest-list/idempotent"

	manifestList := &manifestlist.ManifestList{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     manifestlist.MediaTypeManifestList,
		},
		Manifests: make([]manifestlist.ManifestDescriptor, tagCount),
	}

	for i := 0; i < tagCount; i++ {
		deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByDigest)

		_, manifestPayload, err := deserializedManifest.Payload()
		require.NoError(t, err)
		manifestDigest := digest.FromBytes(manifestPayload)
		manifestList.Manifests[i] = manifestlist.ManifestDescriptor{
			Descriptor: distribution.Descriptor{
				Digest:    manifestDigest,
				MediaType: schema2.MediaTypeManifest,
			},
			Platform: randomPlatformSpec(),
		}
	}

	deserializedManifestList, err := manifestlist.FromDescriptorsWithMediaType(manifestList.Manifests, manifestlist.MediaTypeManifestList)
	require.NoError(t, err)

	manifestListURL1 := buildManifestTagURL(t, env, repoPath, "list-1")
	manifestListURL2 := buildManifestTagURL(t, env, repoPath, "list-2")

	_, payload, err := deserializedManifestList.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	expectedManifestListURL := buildManifestDigestURL(t, env, repoPath, deserializedManifestList)

	wg := &sync.WaitGroup{}
	// Put the same manifest list with tag one and two to test idempotency.
	for _, manifestListURL := range []string{manifestListURL1, manifestListURL2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := putManifest("putting manifest list by tag no error", manifestListURL, manifestlist.MediaTypeManifestList, deserializedManifestList)
			assert.NoError(t, err)
			defer resp.Body.Close()
			// NOTE(prozlach): we can't use require in a goroutine.
			assert.Equal(t, http.StatusCreated, resp.StatusCode)
			assert.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
			assert.Equal(t, expectedManifestListURL, resp.Header.Get("Location"))
			assert.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))
		}()
	}

	wg.Wait()
}

func manifestPutSchema2ReuseTagManifestToManifest(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "replacesmanifesttag"
	repoPath := "schema2/replacesmanifest"

	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	// Fetch original manifest by tag name
	manifestURL := buildManifestTagURL(t, env, repoPath, tagName)

	req, err := http.NewRequest(http.MethodGet, manifestURL, nil)
	require.NoError(t, err)

	req.Header.Set("Accept", schema2.MediaTypeManifest)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	checkResponse(t, "fetching uploaded manifest", resp, http.StatusOK)

	var fetchedOriginalManifest schema2.DeserializedManifest
	dec := json.NewDecoder(resp.Body)

	err = dec.Decode(&fetchedOriginalManifest)
	require.NoError(t, err)

	_, originalPayload, err := fetchedOriginalManifest.Payload()
	require.NoError(t, err)

	// Create a new manifest and push it up with the same tag.
	newManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	// Fetch new manifest by tag name
	req, err = http.NewRequest(http.MethodGet, manifestURL, nil)
	require.NoError(t, err)

	req.Header.Set("Accept", schema2.MediaTypeManifest)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	checkResponse(t, "fetching uploaded manifest", resp, http.StatusOK)

	var fetchedNewManifest schema2.DeserializedManifest
	dec = json.NewDecoder(resp.Body)

	err = dec.Decode(&fetchedNewManifest)
	require.NoError(t, err)

	// Ensure that we pulled down the new manifest by the same tag.
	require.Equal(t, *newManifest, fetchedNewManifest)

	// Ensure that the tag referred to different manifests over time.
	require.NotEqual(t, fetchedOriginalManifest, fetchedNewManifest)

	_, newPayload, err := fetchedNewManifest.Payload()
	require.NoError(t, err)

	require.NotEqual(t, originalPayload, newPayload)
}

func manifestPutSchema2ByTag(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema2happypathtag"
	repoPath := "schema2/happypath"

	// seedRandomSchema2Manifest with putByTag tests that the manifest put
	// happened without issue.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))
}

func manifestPutSchema2ByDigest(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	repoPath := "schema2/happypath"

	// seedRandomSchema2Manifest with putByDigest tests that the manifest put
	// happened without issue.
	seedRandomSchema2Manifest(t, env, repoPath, putByDigest)
}

func manifestGetSchema2NonMatchingEtag(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema2happypathtag"
	repoPath := "schema2/happypath"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	tt := []struct {
		name        string
		manifestURL string
		etag        string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
		{
			name:        "by tag non matching etag",
			manifestURL: tagURL,
			etag:        digest.FromString("no match").String(),
		},
		{
			name:        "by digest non matching etag",
			manifestURL: digestURL,
			etag:        digest.FromString("no match").String(),
		},
		{
			name:        "by tag malformed etag",
			manifestURL: tagURL,
			etag:        "bad etag",
		},
		{
			name:        "by digest malformed etag",
			manifestURL: digestURL,
			etag:        "bad etag",
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, test.manifestURL, nil)
			require.NoError(t, err)

			req.Header.Set("Accept", schema2.MediaTypeManifest)
			if test.etag != "" {
				req.Header.Set("If-None-Match", test.etag)
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
			require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))
			require.Equal(t, fmt.Sprintf(`"%s"`, dgst), resp.Header.Get("ETag"))

			var fetchedManifest *schema2.DeserializedManifest
			dec := json.NewDecoder(resp.Body)

			err = dec.Decode(&fetchedManifest)
			require.NoError(t, err)

			require.EqualValues(t, deserializedManifest, fetchedManifest)

			if env.ns != nil {
				sizeStr := resp.Header.Get("Content-Length")
				size, err := strconv.Atoi(sizeStr)
				require.NoError(t, err)

				expectedEvent := buildEventManifestPull(schema2.MediaTypeManifest, repoPath, dgst, int64(size))
				env.ns.AssertEventNotification(t, expectedEvent)
			}
		})
	}
}

func manifestGetSchema2MatchingEtag(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema2happypathtag"
	repoPath := "schema2/happypath"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	tt := []struct {
		name        string
		manifestURL string
		etag        string
	}{
		{
			name:        "by tag quoted etag",
			manifestURL: tagURL,
			etag:        fmt.Sprintf("%q", dgst),
		},
		{
			name:        "by digest quoted etag",
			manifestURL: digestURL,
			etag:        fmt.Sprintf("%q", dgst),
		},
		{
			name:        "by tag non quoted etag",
			manifestURL: tagURL,
			etag:        dgst.String(),
		},
		{
			name:        "by digest non quoted etag",
			manifestURL: digestURL,
			etag:        dgst.String(),
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, test.manifestURL, nil)
			require.NoError(t, err)

			req.Header.Set("Accept", schema2.MediaTypeManifest)
			req.Header.Set("If-None-Match", test.etag)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusNotModified, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Empty(t, body)
		})
	}
}

func baseURLAuth(t *testing.T, opts ...configOpt) {
	opts = append(opts, withSillyAuth)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	v2base, err := env.builder.BuildBaseURL()
	require.NoError(t, err)

	type test struct {
		name                    string
		url                     string
		wantExtFeatures         bool
		wantDistributionVersion bool
	}

	tests := []test{
		{
			name:                    "v2 base route",
			url:                     v2base,
			wantExtFeatures:         true,
			wantDistributionVersion: true,
		},
	}

	// The v1 API base route returns 404s if the database is not enabled.
	if env.config.Database.Enabled {
		gitLabV1Base, err := env.builder.BuildGitlabV1BaseURL()
		require.NoError(t, err)

		tests = append(tests, test{
			name: "GitLab v1 base route",
			url:  gitLabV1Base,
		})
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Get baseurl without auth secret, we should get an auth challenge back.
			resp, err := http.Get(test.url)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
			require.Equal(t, "Bearer realm=\"test-realm\",service=\"test-service\"", resp.Header.Get("WWW-Authenticate"))

			// Get baseurl with Authorization header set, which is the only thing
			// silly auth checks for.
			req, err := http.NewRequest(http.MethodGet, test.url, nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", "sillySecret")

			resp, err = http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
			require.Equal(t, "2", resp.Header.Get("Content-Length"))
			require.Equal(t, strings.TrimPrefix(version.Version, "v"), resp.Header.Get("Gitlab-Container-Registry-Version"))

			if test.wantExtFeatures {
				require.Equal(t, version.ExtFeatures, resp.Header.Get("Gitlab-Container-Registry-Features"))
				require.Equal(t, strconv.FormatBool(env.config.Database.Enabled), resp.Header.Get("Gitlab-Container-Registry-Database-Enabled"))
			} else {
				require.Empty(t, resp.Header.Get("Gitlab-Container-Registry-Features"))
			}

			if test.wantDistributionVersion {
				require.Equal(t, "registry/2.0", resp.Header.Get("Docker-Distribution-API-Version"))
			} else {
				require.Empty(t, resp.Header.Get("Docker-Distribution-API-Version"))
			}

			p, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			require.Equal(t, "{}", string(p))
		})
	}
}

func baseURLPrefix(t *testing.T, opts ...configOpt) {
	prefix := "/test/"
	opts = append(opts, withHTTPPrefix(prefix))
	env := newTestEnv(t, opts...)

	defer env.Shutdown()

	// Test V2 base URL.
	baseURL, err := env.builder.BuildBaseURL()
	require.NoError(t, err)

	parsed, err := url.Parse(baseURL)
	require.NoError(t, err)
	require.Truef(t, strings.HasPrefix(parsed.Path, prefix),
		"prefix %q not included in test url %q", prefix, baseURL)

	resp, err := http.Get(baseURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	require.Equal(t, "2", resp.Header.Get("Content-Length"))

	// Test GitLabV1 base URL.
	baseURL, err = env.builder.BuildGitlabV1BaseURL()
	require.NoError(t, err)

	parsed, err = url.Parse(baseURL)
	require.NoError(t, err)
	require.Truef(t, strings.HasPrefix(parsed.Path, prefix),
		"prefix %q not included in test url %q", prefix, baseURL)

	resp, err = http.Get(baseURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	// The V1 API base route returns 404s if the database is not enabled.
	if env.config.Database.Enabled {
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
		require.Equal(t, "2", resp.Header.Get("Content-Length"))
	} else {
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
		require.Equal(t, "", resp.Header.Get("Content-Type"))
		require.Equal(t, "0", resp.Header.Get("Content-Length"))
	}
}

func manifestPutSchema1ByTag(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema1tag"
	repoPath := "schema1"

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	unsignedManifest := &schema1.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 1,
		},
		Name: repoPath,
		Tag:  tagName,
		History: []schema1.History{
			{
				V1Compatibility: "",
			},
			{
				V1Compatibility: "",
			},
		},
	}

	// Create and push up 2 random layers.
	unsignedManifest.FSLayers = make([]schema1.FSLayer, 2)

	for i := range unsignedManifest.FSLayers {
		rs, dgst, _ := createRandomSmallLayer(t)

		uploadURLBase, _ := startPushLayer(t, env, repoRef)
		pushLayer(t, env.builder, repoRef, dgst, uploadURLBase, rs)

		unsignedManifest.FSLayers[i] = schema1.FSLayer{
			BlobSum: dgst,
		}
	}

	signedManifest, err := schema1.Sign(unsignedManifest, env.pk)
	require.NoError(t, err)

	manifestURL := buildManifestTagURL(t, env, repoPath, tagName)

	resp, err := putManifest("putting schema1 manifest bad request error", manifestURL, schema1.MediaTypeManifest, signedManifest)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	checkBodyHasErrorCodes(t, "invalid manifest", resp, v2.ErrorCodeManifestInvalid)
}

func manifestPutSchema2ByDigestConfigNotAssociatedWithRepository(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	repoPath1 := "schema2/layersnotassociated1"
	repoPath2 := "schema2/layersnotassociated2"

	repoRef1, err := reference.WithName(repoPath1)
	require.NoError(t, err)

	repoRef2, err := reference.WithName(repoPath2)
	require.NoError(t, err)

	testManifest := &schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
	}

	// Create a manifest config and push up its content.
	cfgPayload, cfgDesc := schema2Config()
	uploadURLBase, _ := startPushLayer(t, env, repoRef2)
	pushLayer(t, env.builder, repoRef2, cfgDesc.Digest, uploadURLBase, bytes.NewReader(cfgPayload))
	testManifest.Config = cfgDesc

	// Create and push up 2 random layers.
	testManifest.Layers = make([]distribution.Descriptor, 2)

	for i := range testManifest.Layers {
		rs, dgst, size := createRandomSmallLayer(t)

		uploadURLBase, _ := startPushLayer(t, env, repoRef1)
		pushLayer(t, env.builder, repoRef1, dgst, uploadURLBase, rs)

		testManifest.Layers[i] = distribution.Descriptor{
			Digest:    dgst,
			MediaType: schema2.MediaTypeLayer,
			Size:      size,
		}
	}

	deserializedManifest, err := schema2.FromStruct(*testManifest)
	require.NoError(t, err)

	manifestDigestURL := buildManifestDigestURL(t, env, repoPath1, deserializedManifest)

	resp, err := putManifest("putting manifest whose config is not present in the repository", manifestDigestURL, schema2.MediaTypeManifest, deserializedManifest.Manifest)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func manifestPutSchema2MissingConfig(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema2missingconfigtag"
	repoPath := "schema2/missingconfig"

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	testManifest := &schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
	}

	// Create a manifest config but do not push up its content.
	_, cfgDesc := schema2Config()
	testManifest.Config = cfgDesc

	// Create and push up 2 random layers.
	testManifest.Layers = make([]distribution.Descriptor, 2)

	for i := range testManifest.Layers {
		rs, dgst, size := createRandomSmallLayer(t)

		uploadURLBase, _ := startPushLayer(t, env, repoRef)
		pushLayer(t, env.builder, repoRef, dgst, uploadURLBase, rs)

		testManifest.Layers[i] = distribution.Descriptor{
			Digest:    dgst,
			MediaType: schema2.MediaTypeLayer,
			Size:      size,
		}
	}

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)

	deserializedManifest, err := schema2.FromStruct(*testManifest)
	require.NoError(t, err)

	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	tt := []struct {
		name        string
		manifestURL string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			// Push up the manifest with only the layer blobs pushed up.
			resp, err := putManifest("putting missing config manifest", test.manifestURL, schema2.MediaTypeManifest, testManifest)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))

			// Test that we have one missing blob.
			_, p, counts := checkBodyHasErrorCodes(t, "putting missing config manifest", resp, v2.ErrorCodeManifestBlobUnknown)
			expectedCounts := map[errcode.ErrorCode]int{v2.ErrorCodeManifestBlobUnknown: 1}

			require.EqualValuesf(t, expectedCounts, counts, "response body: %s", p)
		})
	}
}

func manifestPutSchema2MissingLayers(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema2missinglayerstag"
	repoPath := "schema2/missinglayers"

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	testManifest := &schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
	}

	// Create a manifest config and push up its content.
	cfgPayload, cfgDesc := schema2Config()
	uploadURLBase, _ := startPushLayer(t, env, repoRef)
	pushLayer(t, env.builder, repoRef, cfgDesc.Digest, uploadURLBase, bytes.NewReader(cfgPayload))
	testManifest.Config = cfgDesc

	// Create and push up 2 random layers, but do not push their content.
	testManifest.Layers = make([]distribution.Descriptor, 2)

	for i := range testManifest.Layers {
		_, dgst, size := createRandomSmallLayer(t)

		testManifest.Layers[i] = distribution.Descriptor{
			Digest:    dgst,
			MediaType: schema2.MediaTypeLayer,
			Size:      size,
		}
	}

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)

	deserializedManifest, err := schema2.FromStruct(*testManifest)
	require.NoError(t, err)

	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	tt := []struct {
		name        string
		manifestURL string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			// Push up the manifest with only the config blob pushed up.
			resp, err := putManifest("putting missing layers", test.manifestURL, schema2.MediaTypeManifest, testManifest)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))

			// Test that we have two missing blobs, one for each layer.
			_, p, counts := checkBodyHasErrorCodes(t, "putting missing config manifest", resp, v2.ErrorCodeManifestBlobUnknown)
			expectedCounts := map[errcode.ErrorCode]int{v2.ErrorCodeManifestBlobUnknown: 2}

			require.EqualValuesf(t, expectedCounts, counts, "response body: %s", p)
		})
	}
}

func manifestPutSchema2MissingConfigAndLayers(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema2missingconfigandlayerstag"
	repoPath := "schema2/missingconfigandlayers"

	testManifest := &schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
	}

	// Create a random layer and push up its content to ensure repository
	// exists and that we are only testing missing manifest references.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	rs, dgst, _ := createRandomSmallLayer(t)

	uploadURLBase, _ := startPushLayer(t, env, repoRef)
	pushLayer(t, env.builder, repoRef, dgst, uploadURLBase, rs)

	// Create a manifest config, but do not push up its content.
	_, cfgDesc := schema2Config()
	testManifest.Config = cfgDesc

	// Create and push up 2 random layers, but do not push their content.
	testManifest.Layers = make([]distribution.Descriptor, 2)

	for i := range testManifest.Layers {
		_, dgst, size := createRandomSmallLayer(t)

		testManifest.Layers[i] = distribution.Descriptor{
			Digest:    dgst,
			MediaType: schema2.MediaTypeLayer,
			Size:      size,
		}
	}

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)

	deserializedManifest, err := schema2.FromStruct(*testManifest)
	require.NoError(t, err)

	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	tt := []struct {
		name        string
		manifestURL string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			// Push up the manifest with only the config blob pushed up.
			resp, err := putManifest("putting missing layers", test.manifestURL, schema2.MediaTypeManifest, testManifest)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))

			// Test that we have two missing blobs, one for each layer, and one for the config.
			_, p, counts := checkBodyHasErrorCodes(t, "putting missing config manifest", resp, v2.ErrorCodeManifestBlobUnknown)
			expectedCounts := map[errcode.ErrorCode]int{v2.ErrorCodeManifestBlobUnknown: 3}

			require.EqualValuesf(t, expectedCounts, counts, "response body: %s", p)
		})
	}
}

func manifestPutSchema2ReferencesExceedLimit(t *testing.T, opts ...configOpt) {
	opts = append(opts, withReferenceLimit(5))
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema2toomanylayers"
	repoPath := "schema2/toomanylayers"

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	testManifest := &schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
	}

	// Create a manifest config and push up its content.
	cfgPayload, cfgDesc := schema2Config()
	uploadURLBase, _ := startPushLayer(t, env, repoRef)
	pushLayer(t, env.builder, repoRef, cfgDesc.Digest, uploadURLBase, bytes.NewReader(cfgPayload))
	testManifest.Config = cfgDesc

	// Create and push up 10 random layers.
	testManifest.Layers = make([]distribution.Descriptor, 10)

	for i := range testManifest.Layers {
		rs, dgst, size := createRandomSmallLayer(t)

		uploadURLBase, _ := startPushLayer(t, env, repoRef)
		pushLayer(t, env.builder, repoRef, dgst, uploadURLBase, rs)

		testManifest.Layers[i] = distribution.Descriptor{
			Digest:    dgst,
			MediaType: schema2.MediaTypeLayer,
			Size:      size,
		}
	}

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)

	deserializedManifest, err := schema2.FromStruct(*testManifest)
	require.NoError(t, err)

	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	tt := []struct {
		name        string
		manifestURL string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			// Push up the manifest.
			resp, err := putManifest("putting manifest with too many layers", test.manifestURL, schema2.MediaTypeManifest, testManifest)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))

			// Test that we report the reference limit error exactly once.
			_, p, counts := checkBodyHasErrorCodes(t, "manifest put with layers exceeding limit", resp, v2.ErrorCodeManifestReferenceLimit)
			expectedCounts := map[errcode.ErrorCode]int{v2.ErrorCodeManifestReferenceLimit: 1}

			require.EqualValuesf(t, expectedCounts, counts, "response body: %s", p)
		})
	}
}

func manifestPutSchema2PayloadSizeExceedsLimit(t *testing.T, opts ...configOpt) {
	payloadLimit := 5

	opts = append(opts, withPayloadSizeLimit(payloadLimit))
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema2toobig"
	repoPath := "schema2/toobig"

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	testManifest := &schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
	}

	// Create a manifest config and push up its content.
	cfgPayload, cfgDesc := schema2Config()
	uploadURLBase, _ := startPushLayer(t, env, repoRef)
	pushLayer(t, env.builder, repoRef, cfgDesc.Digest, uploadURLBase, bytes.NewReader(cfgPayload))
	testManifest.Config = cfgDesc

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)

	deserializedManifest, err := schema2.FromStruct(*testManifest)
	require.NoError(t, err)

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	manifestPayloadSize := len(payload)
	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	tt := []struct {
		name        string
		manifestURL string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			// Push up the manifest.
			resp, err := putManifest("putting oversized manifest", test.manifestURL, schema2.MediaTypeManifest, testManifest)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))

			// Test that we report the reference limit error exactly once.
			errs, p, counts := checkBodyHasErrorCodes(t, "manifest put exceeds payload size limit", resp, v2.ErrorCodeManifestPayloadSizeLimit)
			expectedCounts := map[errcode.ErrorCode]int{v2.ErrorCodeManifestPayloadSizeLimit: 1}

			require.EqualValuesf(t, expectedCounts, counts, "response body: %s", p)

			require.Len(t, errs, 1, "exactly one error")
			errc, ok := errs[0].(errcode.Error)
			require.True(t, ok)

			require.Equal(t,
				distribution.ErrManifestVerification{
					distribution.ErrManifestPayloadSizeExceedsLimit{PayloadSize: manifestPayloadSize, Limit: payloadLimit},
				}.Error(),
				errc.Detail,
			)
		})
	}
}

func manifestGetSchema2ByDigestMissingManifest(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "missingmanifesttag"
	repoPath := "schema2/missingmanifest"

	// Push up a manifest so that the repository is created. This way we can
	// test the case where a manifest is not present in a repository, as opposed
	// to the case where an entire repository does not exist.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	dgst := digest.FromString("bogus digest")

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	digestRef, err := reference.WithDigest(repoRef, dgst)
	require.NoError(t, err)

	bogusManifestDigestURL, err := env.builder.BuildManifestURL(digestRef)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodGet, bogusManifestDigestURL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", schema2.MediaTypeManifest)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	checkResponse(t, "getting non-existent manifest", resp, http.StatusNotFound)
	checkBodyHasErrorCodes(t, "getting non-existent manifest", resp, v2.ErrorCodeManifestUnknown)
}

func manifestGetSchema2ByDigestMissingRepository(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "missingrepositorytag"
	repoPath := "schema2/missingrepository"

	// Push up a manifest so that it exists within the registry. We'll attempt to
	// get the manifest by digest from a non-existent repository, which should fail.
	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	manifestDigestURL := buildManifestDigestURL(t, env, "fake/repo", deserializedManifest)

	req, err := http.NewRequest(http.MethodGet, manifestDigestURL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", schema2.MediaTypeManifest)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	checkResponse(t, "getting non-existent manifest", resp, http.StatusNotFound)
	checkBodyHasErrorCodes(t, "getting non-existent manifest", resp, v2.ErrorCodeManifestUnknown)
}

func manifestGetSchema2ByTagMissingRepository(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "missingrepositorytag"
	repoPath := "schema2/missingrepository"

	// Push up a manifest so that it exists within the registry. We'll attempt to
	// get the manifest by tag from a non-existent repository, which should fail.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	manifestURL := buildManifestTagURL(t, env, "fake/repo", tagName)

	req, err := http.NewRequest(http.MethodGet, manifestURL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", schema2.MediaTypeManifest)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	checkResponse(t, "getting non-existent manifest", resp, http.StatusNotFound)
	checkBodyHasErrorCodes(t, "getting non-existent manifest", resp, v2.ErrorCodeManifestUnknown)
}

func manifestGetSchema2ByTagMissingTag(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "missingtagtag"
	repoPath := "schema2/missingtag"

	// Push up a manifest so that it exists within the registry. We'll attempt to
	// get the manifest by a non-existent tag, which should fail.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	manifestURL := buildManifestTagURL(t, env, repoPath, "faketag")

	req, err := http.NewRequest(http.MethodGet, manifestURL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", schema2.MediaTypeManifest)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	checkResponse(t, "getting non-existent manifest", resp, http.StatusNotFound)
	checkBodyHasErrorCodes(t, "getting non-existent manifest", resp, v2.ErrorCodeManifestUnknown)
}

func manifestGetSchema2ByDigestNotAssociatedWithRepository(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName1 := "missingrepository1tag"
	repoPath1 := "schema2/missingrepository1"

	tagName2 := "missingrepository2tag"
	repoPath2 := "schema2/missingrepository2"

	// Push up two manifests in different repositories so that they both exist
	// within the registry. We'll attempt to get a manifest by digest from the
	// repository to which it does not belong, which should fail.
	seedRandomSchema2Manifest(t, env, repoPath1, putByTag(tagName1))
	deserializedManifest2 := seedRandomSchema2Manifest(t, env, repoPath2, putByTag(tagName2))

	mismatchedManifestURL := buildManifestDigestURL(t, env, repoPath1, deserializedManifest2)

	req, err := http.NewRequest(http.MethodGet, mismatchedManifestURL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", schema2.MediaTypeManifest)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	checkResponse(t, "getting non-existent manifest", resp, http.StatusNotFound)
	checkBodyHasErrorCodes(t, "getting non-existent manifest", resp, v2.ErrorCodeManifestUnknown)
}

func manifestGetSchema2ByTagNotAssociatedWithRepository(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName1 := "missingrepository1tag"
	repoPath1 := "schema2/missingrepository1"

	tagName2 := "missingrepository2tag"
	repoPath2 := "schema2/missingrepository2"

	// Push up two manifests in different repositories so that they both exist
	// within the registry. We'll attempt to get a manifest by tag from the
	// repository to which it does not belong, which should fail.
	seedRandomSchema2Manifest(t, env, repoPath1, putByTag(tagName1))
	seedRandomSchema2Manifest(t, env, repoPath2, putByTag(tagName2))

	mismatchedManifestURL := buildManifestTagURL(t, env, repoPath1, tagName2)

	req, err := http.NewRequest(http.MethodGet, mismatchedManifestURL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", schema2.MediaTypeManifest)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	checkResponse(t, "getting non-existent manifest", resp, http.StatusNotFound)
	checkBodyHasErrorCodes(t, "getting non-existent manifest", resp, v2.ErrorCodeManifestUnknown)
}

func manifestPutSchema2ByDigestLayersNotAssociatedWithRepository(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	repoPath1 := "schema2/layersnotassociated1"
	repoPath2 := "schema2/layersnotassociated2"

	repoRef1, err := reference.WithName(repoPath1)
	require.NoError(t, err)

	repoRef2, err := reference.WithName(repoPath2)
	require.NoError(t, err)

	testManifest := &schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
	}

	// Create a manifest config and push up its content.
	cfgPayload, cfgDesc := schema2Config()
	uploadURLBase, _ := startPushLayer(t, env, repoRef1)
	pushLayer(t, env.builder, repoRef1, cfgDesc.Digest, uploadURLBase, bytes.NewReader(cfgPayload))
	testManifest.Config = cfgDesc

	// Create and push up 2 random layers.
	testManifest.Layers = make([]distribution.Descriptor, 2)

	for i := range testManifest.Layers {
		rs, dgst, size := createRandomSmallLayer(t)

		uploadURLBase, _ := startPushLayer(t, env, repoRef2)
		pushLayer(t, env.builder, repoRef2, dgst, uploadURLBase, rs)

		testManifest.Layers[i] = distribution.Descriptor{
			Digest:    dgst,
			MediaType: schema2.MediaTypeLayer,
			Size:      size,
		}
	}

	deserializedManifest, err := schema2.FromStruct(*testManifest)
	require.NoError(t, err)

	manifestDigestURL := buildManifestDigestURL(t, env, repoPath1, deserializedManifest)

	resp, err := putManifest("putting manifest whose layers are not present in the repository", manifestDigestURL, schema2.MediaTypeManifest, deserializedManifest.Manifest)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func manifestPutSchema1ByDigest(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	repoPath := "schema1"

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	unsignedManifest := &schema1.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 1,
		},
		Name: repoPath,
		Tag:  "",
		History: []schema1.History{
			{
				V1Compatibility: "",
			},
			{
				V1Compatibility: "",
			},
		},
	}

	// Create and push up 2 random layers.
	unsignedManifest.FSLayers = make([]schema1.FSLayer, 2)

	for i := range unsignedManifest.FSLayers {
		rs, dgst, _ := createRandomSmallLayer(t)

		uploadURLBase, _ := startPushLayer(t, env, repoRef)
		pushLayer(t, env.builder, repoRef, dgst, uploadURLBase, rs)

		unsignedManifest.FSLayers[i] = schema1.FSLayer{
			BlobSum: dgst,
		}
	}

	signedManifest, err := schema1.Sign(unsignedManifest, env.pk)
	require.NoError(t, err)

	manifestURL := buildManifestDigestURL(t, env, repoPath, signedManifest)

	resp, err := putManifest("putting schema1 manifest bad request error", manifestURL, schema1.MediaTypeManifest, signedManifest)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	checkBodyHasErrorCodes(t, "invalid manifest", resp, v2.ErrorCodeManifestInvalid)
}

func manifestHeadSchema2(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "headtag"
	repoPath := "schema2/head"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	// Build URLs.
	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	tt := []struct {
		name        string
		manifestURL string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodHead, test.manifestURL, nil)
			require.NoError(t, err)
			req.Header.Set("Accept", schema2.MediaTypeManifest)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Empty(t, b, "body should be empty")

			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))

			cl, err := strconv.Atoi(resp.Header.Get("Content-Length"))
			require.NoError(t, err)
			require.EqualValues(t, len(payload), cl)

			require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))

			if env.ns != nil {
				expectedEvent := buildEventManifestPull(schema2.MediaTypeManifest, repoPath, dgst, int64(cl))
				env.ns.AssertEventNotification(t, expectedEvent)
			}
		})
	}
}

func manifestHeadSchema2MissingManifest(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "headtag"
	repoPath := "schema2/missingmanifest"

	// Push up a manifest so that the repository is created. This way we can
	// test the case where a manifest is not present in a repository, as opposed
	// to the case where an entire repository does not exist.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	// Build URLs.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	digestRef, err := reference.WithDigest(repoRef, digest.FromString("bogus digest"))
	require.NoError(t, err)

	digestURL, err := env.builder.BuildManifestURL(digestRef)
	require.NoError(t, err)

	tagURL := buildManifestTagURL(t, env, repoPath, "faketag")

	tt := []struct {
		name        string
		manifestURL string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodHead, test.manifestURL, nil)
			require.NoError(t, err)
			req.Header.Set("Accept", schema2.MediaTypeManifest)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusNotFound, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
		})
	}
}

func manifestGetSchema2NoAcceptHeaders(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "noaccepttag"
	repoPath := "schema2/noaccept"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	tt := []struct {
		name        string
		manifestURL string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			// Without any accept headers we should still get a schema2 manifest since
			// schema1 support has been dropped.
			resp, err := http.Get(test.manifestURL)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
			require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))
			require.Equal(t, fmt.Sprintf("%q", dgst), resp.Header.Get("ETag"))

			var fetchedManifest *schema2.DeserializedManifest
			dec := json.NewDecoder(resp.Body)

			err = dec.Decode(&fetchedManifest)
			require.NoError(t, err)

			require.EqualValues(t, deserializedManifest, fetchedManifest)

			if env.ns != nil {
				sizeStr := resp.Header.Get("Content-Length")
				size, err := strconv.Atoi(sizeStr)
				require.NoError(t, err)

				expectedEvent := buildEventManifestPull(schema2.MediaTypeManifest, repoPath, dgst, int64(size))
				env.ns.AssertEventNotification(t, expectedEvent)
			}
		})
	}
}

func manifestDeleteSchema2(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema2deletetag"
	repoPath := "schema2/delete"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	resp, err := httpDelete(manifestDigestURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	req, err := http.NewRequest(http.MethodGet, manifestDigestURL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", schema2.MediaTypeManifest)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	checkBodyHasErrorCodes(t, "getting freshly-deleted manifest", resp, v2.ErrorCodeManifestUnknown)

	if env.ns != nil {
		_, payload, err := deserializedManifest.Payload()
		require.NoError(t, err)

		dgst := digest.FromBytes(payload)

		expectedEventByDigest := buildEventSchema2ManifestDeleteByDigest(repoPath, dgst)
		env.ns.AssertEventNotification(t, expectedEventByDigest)

		if env.db == nil {
			expectedEvent := buildEventManifestDeleteByTag(schema2.MediaTypeManifest, repoPath, tagName)
			env.ns.AssertEventNotification(t, expectedEvent)
		}
	}
}

func manifestDeleteSchema2AlreadyDeleted(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema2deleteagain"
	repoPath := "schema2/deleteagain"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	resp, err := httpDelete(manifestDigestURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	if env.ns != nil {
		_, payload, err := deserializedManifest.Payload()
		require.NoError(t, err)

		dgst := digest.FromBytes(payload)

		expectedEventByDigest := buildEventSchema2ManifestDeleteByDigest(repoPath, dgst)
		env.ns.AssertEventNotification(t, expectedEventByDigest)

		if env.db == nil {
			expectedEventByTag := buildEventManifestDeleteByTag("", repoPath, tagName)
			env.ns.AssertEventNotification(t, expectedEventByTag)
		}
	}

	resp, err = httpDelete(manifestDigestURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func manifestDeleteSchema2Reupload(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema2deletereupload"
	repoPath := "schema2/deletereupload"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	resp, err := httpDelete(manifestDigestURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	if env.ns != nil {
		_, payload, err := deserializedManifest.Payload()
		require.NoError(t, err)

		dgst := digest.FromBytes(payload)

		expectedEventByDigest := buildEventSchema2ManifestDeleteByDigest(repoPath, dgst)
		env.ns.AssertEventNotification(t, expectedEventByDigest)

		if env.db == nil {
			expectedEvent := buildEventManifestDeleteByTag(schema2.MediaTypeManifest, repoPath, tagName)
			env.ns.AssertEventNotification(t, expectedEvent)
		}
	}

	// Re-upload manifest by digest
	resp, err = putManifest("reuploading manifest no error", manifestDigestURL, schema2.MediaTypeManifest, deserializedManifest.Manifest)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
	require.Equal(t, manifestDigestURL, resp.Header.Get("Location"))

	// Attempt to fetch re-uploaded deleted digest
	req, err := http.NewRequest(http.MethodGet, manifestDigestURL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", schema2.MediaTypeManifest)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func manifestDeleteSchema2MissingManifest(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	repoPath := "schema2/deletemissing"

	// Push up random manifest to ensure repo is created.
	seedRandomSchema2Manifest(t, env, repoPath, putByDigest)

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	dgst := digest.FromString("fake-manifest")

	digestRef, err := reference.WithDigest(repoRef, dgst)
	require.NoError(t, err)

	manifestDigestURL, err := env.builder.BuildManifestURL(digestRef)
	require.NoError(t, err)

	resp, err := httpDelete(manifestDigestURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func manifestDeleteSchema2ClearsTags(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema2deletecleartag"
	repoPath := "schema2/delete"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	tagsURL, err := env.builder.BuildTagsURL(repoRef)
	require.NoError(t, err)

	// Ensure that the tag is listed.
	resp, err := http.Get(tagsURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	tagsResponse := tagsAPIResponse{}
	err = dec.Decode(&tagsResponse)
	require.NoError(t, err)

	require.Equal(t, repoPath, tagsResponse.Name)
	require.NotEmpty(t, tagsResponse.Tags)
	require.Equal(t, tagName, tagsResponse.Tags[0])

	// Delete manifest
	resp, err = httpDelete(manifestDigestURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	if env.ns != nil {
		_, payload, err := deserializedManifest.Payload()
		require.NoError(t, err)

		dgst := digest.FromBytes(payload)

		expectedEventByDigest := buildEventSchema2ManifestDeleteByDigest(repoPath, dgst)
		env.ns.AssertEventNotification(t, expectedEventByDigest)

		if env.db == nil {
			expectedEvent := buildEventManifestDeleteByTag(schema2.MediaTypeManifest, repoPath, tagName)
			env.ns.AssertEventNotification(t, expectedEvent)
		}
	}

	// Ensure that the tag is not listed.
	resp, err = http.Get(tagsURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	dec = json.NewDecoder(resp.Body)
	err = dec.Decode(&tagsResponse)
	require.NoError(t, err)

	require.Equal(t, repoPath, tagsResponse.Name)
	require.Empty(t, tagsResponse.Tags)
}

func manifestDeleteSchema2DeleteDisabled(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "schema2deletedisabled"
	repoPath := "schema2/delete"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	resp, err := httpDelete(manifestDigestURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func manifestDeleteTag(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	tag := "latest"
	dgst := createRepository(t, env, imageName.Name(), tag)

	ref, err := reference.WithTag(imageName, tag)
	require.NoError(t, err)

	u, err := env.builder.BuildManifestURL(ref)
	require.NoError(t, err)

	resp, err := httpDelete(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	resp, err = http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	digestRef, err := reference.WithDigest(imageName, dgst)
	require.NoError(t, err)

	u, err = env.builder.BuildManifestURL(digestRef)
	require.NoError(t, err)

	resp, err = http.Head(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func manifestDeleteTagUnknown(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	// create repo with another tag so that we don't hit a repository unknown error
	createRepository(t, env, imageName.Name(), "1.2.3")

	ref, err := reference.WithTag(imageName, "3.2.1")
	require.NoError(t, err)

	u, err := env.builder.BuildManifestURL(ref)
	require.NoError(t, err)

	resp, err := httpDelete(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	checkBodyHasErrorCodes(t, "deleting unknown tag", resp, v2.ErrorCodeManifestUnknown)
}

func manifestDeleteTagUnknownRepository(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	ref, err := reference.WithTag(imageName, "latest")
	require.NoError(t, err)

	manifestURL, err := env.builder.BuildManifestURL(ref)
	require.NoError(t, err)

	resp, err := httpDelete(manifestURL)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	checkBodyHasErrorCodes(t, "repository not found", resp, v2.ErrorCodeNameUnknown)
}

func manifestDeleteTagDeleteDisabled(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)
	ref, err := reference.WithTag(imageName, "latest")
	require.NoError(t, err)

	u, err := env.builder.BuildManifestURL(ref)
	require.NoError(t, err)

	resp, err := httpDelete(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func manifestPutOCIByTag(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "ocihappypathtag"
	repoPath := "oci/happypath"

	// seedRandomOCIManifest with putByTag tests that the manifest put happened without issue.
	seedRandomOCIManifest(t, env, repoPath, putByTag(tagName))
}

func manifestPutOCIByDigest(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	repoPath := "oci/happypath"

	// seedRandomOCIManifest with putByDigest tests that the manifest put happened without issue.
	seedRandomOCIManifest(t, env, repoPath, putByDigest)
}

func manifestPutOCIWithSubject(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	repoPath := "oci/happypath"

	// create random manifest with seedRandomOCIManifest,
	// use this manifest as the subject for the artifact manifest
	mfst := seedRandomOCIManifest(t, env, repoPath, putByDigest)
	artifactMfst := seedRandomOCIManifest(t, env, repoPath, putByDigest, withSubject(mfst))

	// Fetch the artifact manifest just pushed and check subject is present
	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, artifactMfst)

	req, err := http.NewRequest(http.MethodGet, manifestDigestURL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", v1.MediaTypeImageManifest)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	var fetchedManifest *ocischema.DeserializedManifest
	dec := json.NewDecoder(resp.Body)

	err = dec.Decode(&fetchedManifest)
	require.NoError(t, err)

	_, subjPayload, err := mfst.Payload()
	require.NoError(t, err)

	require.Equal(t, digest.FromBytes(subjPayload), fetchedManifest.Subject().Digest)
}

func manifestPutOCIWithV2Subject(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	repoPath := "oci/happypath"

	// create random manifest with seedRandomSchema2Manifest,
	// use this manifest as the subject for the artifact manifest
	mfst := seedRandomSchema2Manifest(t, env, repoPath, putByDigest)
	artifactMfst := seedRandomOCIManifest(t, env, repoPath, putByDigest, withSubject(mfst))

	// Fetch the artifact manifest just pushed and check subject is present
	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, artifactMfst)

	req, err := http.NewRequest(http.MethodGet, manifestDigestURL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", v1.MediaTypeImageManifest)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	var fetchedManifest *ocischema.DeserializedManifest
	dec := json.NewDecoder(resp.Body)

	err = dec.Decode(&fetchedManifest)
	require.NoError(t, err)

	_, subjPayload, err := mfst.Payload()
	require.NoError(t, err)

	require.Equal(t, digest.FromBytes(subjPayload), fetchedManifest.Subject().Digest)
}

func manifestPutOCIWithNonMatchingSubject(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	repoPath := "oci/happypath"

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	testManifest := &ocischema.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     v1.MediaTypeImageManifest,
		},
	}

	// Create and push config layer
	cfgPayload, cfgDesc := ociConfig()
	uploadURLBase, _ := startPushLayer(t, env, repoRef)
	pushLayer(t, env.builder, repoRef, cfgDesc.Digest, uploadURLBase, bytes.NewReader(cfgPayload))
	testManifest.Config = cfgDesc

	// Create one random layer for this manifest
	testManifest.Layers = make([]distribution.Descriptor, 1)
	rs, dgst, size := createRandomSmallLayer(t)
	uploadURLBase, _ = startPushLayer(t, env, repoRef)
	pushLayer(t, env.builder, repoRef, dgst, uploadURLBase, rs)
	testManifest.Layers[0] = distribution.Descriptor{
		Digest:    dgst,
		MediaType: v1.MediaTypeImageLayer,
		Size:      size,
	}

	// Set subject as a nonexistent manifest reference
	testManifest.Subject = &distribution.Descriptor{
		Digest:    digest.Digest("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		MediaType: v1.MediaTypeImageManifest,
		Size:      42,
	}

	deserializedManifest, err := ocischema.FromStruct(*testManifest)
	require.NoError(t, err)

	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	resp, err := putManifest("putting manifest with missing subject", manifestDigestURL, v1.MediaTypeImageManifest, deserializedManifest)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	checkBodyHasErrorCodes(t, "putting manifest with missing subject", resp, v2.ErrorCodeManifestBlobUnknown)
}

func manifestPutOCIWithArtifactType(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	repoPath := "oci/happypath"
	artifactType := "application/vnd.dev.cosign.artifact.sbom.v1+json"

	mfst := seedRandomOCIManifest(t, env, repoPath, putByDigest, withArtifactType(artifactType))

	digestURL := buildManifestDigestURL(t, env, repoPath, mfst)

	req, err := http.NewRequest(http.MethodGet, digestURL, nil)
	require.NoError(t, err)

	req.Header.Set("Accept", v1.MediaTypeImageManifest)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var fetchedManifest *ocischema.DeserializedManifest
	dec := json.NewDecoder(resp.Body)

	err = dec.Decode(&fetchedManifest)
	require.NoError(t, err)

	require.EqualValues(t, mfst, fetchedManifest)
}

func manifestGetOCINonMatchingEtag(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "ocihappypathtag"
	repoPath := "oci/happypath"

	deserializedManifest := seedRandomOCIManifest(t, env, repoPath, putByTag(tagName))

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	tt := []struct {
		name        string
		manifestURL string
		etag        string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
		{
			name:        "by tag non matching etag",
			manifestURL: tagURL,
			etag:        digest.FromString("no match").String(),
		},
		{
			name:        "by digest non matching etag",
			manifestURL: digestURL,
			etag:        digest.FromString("no match").String(),
		},
		{
			name:        "by tag malformed etag",
			manifestURL: tagURL,
			etag:        "bad etag",
		},
		{
			name:        "by digest malformed etag",
			manifestURL: digestURL,
			etag:        "bad etag",
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, test.manifestURL, nil)
			require.NoError(t, err)

			req.Header.Set("Accept", v1.MediaTypeImageManifest)
			if test.etag != "" {
				req.Header.Set("If-None-Match", test.etag)
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
			require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))
			require.Equal(t, fmt.Sprintf(`"%s"`, dgst), resp.Header.Get("ETag"))

			var fetchedManifest *ocischema.DeserializedManifest
			dec := json.NewDecoder(resp.Body)

			err = dec.Decode(&fetchedManifest)
			require.NoError(t, err)

			require.EqualValues(t, deserializedManifest, fetchedManifest)

			if env.ns != nil {
				sizeStr := resp.Header.Get("Content-Length")
				size, err := strconv.Atoi(sizeStr)
				require.NoError(t, err)

				expectedEvent := buildEventManifestPull(v1.MediaTypeImageManifest, repoPath, dgst, int64(size))
				env.ns.AssertEventNotification(t, expectedEvent)
			}
		})
	}
}

func manifestGetOCIMatchingEtag(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "ocihappypathtag"
	repoPath := "oci/happypath"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	tt := []struct {
		name        string
		manifestURL string
		etag        string
	}{
		{
			name:        "by tag quoted etag",
			manifestURL: tagURL,
			etag:        fmt.Sprintf("%q", dgst),
		},
		{
			name:        "by digest quoted etag",
			manifestURL: digestURL,
			etag:        fmt.Sprintf("%q", dgst),
		},
		{
			name:        "by tag non quoted etag",
			manifestURL: tagURL,
			etag:        dgst.String(),
		},
		{
			name:        "by digest non quoted etag",
			manifestURL: digestURL,
			etag:        dgst.String(),
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, test.manifestURL, nil)
			require.NoError(t, err)

			req.Header.Set("Accept", v1.MediaTypeImageManifest)
			req.Header.Set("If-None-Match", test.etag)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusNotModified, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Empty(t, body)
		})
	}
}

func manifestPutOCIImageIndexByTag(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "ociindexhappypathtag"
	repoPath := "ociindex/happypath"

	// putRandomOCIImageIndex with putByTag tests that the manifest put happened without issue.
	seedRandomOCIImageIndex(t, env, repoPath, putByTag(tagName))
}

func manifestPutOCIImageIndexByDigest(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	repoPath := "ociindex/happypath"

	// putRandomOCIImageIndex with putByDigest tests that the manifest put happened without issue.
	seedRandomOCIImageIndex(t, env, repoPath, putByDigest)
}

func validateManifestPutWithNonDistributableLayers(t *testing.T, env *testEnv, repoRef reference.Named, m distribution.Manifest, mediaType string, foreignDigest digest.Digest) {
	// push manifest
	u := buildManifestDigestURL(t, env, repoRef.Name(), m)
	resp, err := putManifest("putting manifest no error", u, mediaType, m)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
	require.Equal(t, u, resp.Header.Get("Location"))

	_, payload, err := m.Payload()
	require.NoError(t, err)
	dgst := digest.FromBytes(payload)
	require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))

	// make sure that all referenced blobs except the non-distributable layer are known to the registry
	for _, desc := range m.References() {
		repoRef, err := reference.WithName(repoRef.Name())
		require.NoError(t, err)
		ref, err := reference.WithDigest(repoRef, desc.Digest)
		require.NoError(t, err)
		u, err := env.builder.BuildBlobURL(ref)
		require.NoError(t, err)

		res, err := http.Head(u)
		require.NoError(t, err)
		// nolint: revive // defer
		defer res.Body.Close()

		if desc.Digest == foreignDigest {
			require.Equal(t, http.StatusNotFound, res.StatusCode)
		} else {
			require.Equal(t, http.StatusOK, res.StatusCode)
		}
	}
}

func manifestPutOCIWithNonDistributableLayers(t *testing.T, opts ...configOpt) {
	opts = append(opts, withoutManifestURLValidation)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	repoPath := "non-distributable"
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	// seed random manifest and reuse its config and layers for the sake of simplicity
	tmp := seedRandomOCIManifest(t, env, repoPath, putByDigest)

	m := &ocischema.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     v1.MediaTypeImageManifest,
		},
		Config: tmp.Config(),
		Layers: tmp.Layers(),
	}

	// append a non-distributable layer
	d := digest.Digest("sha256:22205a49d57a21afe7918d2b453e17a426654262efadcc4eee6796822bb22669")
	m.Layers = append(m.Layers, distribution.Descriptor{
		MediaType: v1.MediaTypeImageLayerNonDistributableGzip,
		Size:      123456789,
		Digest:    d,
		URLs: []string{
			fmt.Sprintf("https://registry.secret.com/%s", d.String()),
			fmt.Sprintf("https://registry2.secret.com/%s", d.String()),
		},
	})

	dm, err := ocischema.FromStruct(*m)
	require.NoError(t, err)
	validateManifestPutWithNonDistributableLayers(t, env, repoRef, dm, v1.MediaTypeImageManifest, d)
}

func manifestPutSchema2WithNonDistributableLayers(t *testing.T, opts ...configOpt) {
	opts = append(opts, withoutManifestURLValidation)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	repoPath := "non-distributable"
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	// seed random manifest and reuse its config and layers for the sake of simplicity
	tmp := seedRandomSchema2Manifest(t, env, repoPath, putByDigest)

	m := &schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
		Config: tmp.Config(),
		Layers: tmp.Layers(),
	}

	// append a non-distributable layer
	d := digest.Digest("sha256:22205a49d57a21afe7918d2b453e17a426654262efadcc4eee6796822bb22669")
	m.Layers = append(m.Layers, distribution.Descriptor{
		MediaType: schema2.MediaTypeForeignLayer,
		Size:      123456789,
		Digest:    d,
		URLs: []string{
			fmt.Sprintf("https://registry.secret.com/%s", d.String()),
			fmt.Sprintf("https://registry2.secret.com/%s", d.String()),
		},
	})

	dm, err := schema2.FromStruct(*m)
	require.NoError(t, err)
	validateManifestPutWithNonDistributableLayers(t, env, repoRef, dm, schema2.MediaTypeManifest, d)
}

func manifestGetOCIIndexNonMatchingEtag(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "ociindexhappypathtag"
	repoPath := "ociindex/happypath"

	deserializedManifest := seedRandomOCIImageIndex(t, env, repoPath, putByTag(tagName))

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	tt := []struct {
		name        string
		manifestURL string
		etag        string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
		{
			name:        "by tag non matching etag",
			manifestURL: tagURL,
			etag:        digest.FromString("no match").String(),
		},
		{
			name:        "by digest non matching etag",
			manifestURL: digestURL,
			etag:        digest.FromString("no match").String(),
		},
		{
			name:        "by tag malformed etag",
			manifestURL: tagURL,
			etag:        "bad etag",
		},
		{
			name:        "by digest malformed etag",
			manifestURL: digestURL,
			etag:        "bad etag",
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, test.manifestURL, nil)
			require.NoError(t, err)

			req.Header.Set("Accept", v1.MediaTypeImageIndex)
			if test.etag != "" {
				req.Header.Set("If-None-Match", test.etag)
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
			require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))
			require.Equal(t, fmt.Sprintf(`"%s"`, dgst), resp.Header.Get("ETag"))

			var fetchedManifest *manifestlist.DeserializedManifestList
			dec := json.NewDecoder(resp.Body)

			err = dec.Decode(&fetchedManifest)
			require.NoError(t, err)

			require.EqualValues(t, deserializedManifest, fetchedManifest)

			if env.ns != nil {
				sizeStr := resp.Header.Get("Content-Length")
				size, err := strconv.Atoi(sizeStr)
				require.NoError(t, err)

				expectedEvent := buildEventManifestPull(v1.MediaTypeImageIndex, repoPath, dgst, int64(size))
				env.ns.AssertEventNotification(t, expectedEvent)
			}
		})
	}
}

func manifestGetOCIIndexMatchingEtag(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "ociindexhappypathtag"
	repoPath := "ociindex/happypath"

	deserializedManifest := seedRandomOCIImageIndex(t, env, repoPath, putByTag(tagName))

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	tt := []struct {
		name        string
		manifestURL string
		etag        string
	}{
		{
			name:        "by tag quoted etag",
			manifestURL: tagURL,
			etag:        fmt.Sprintf("%q", dgst),
		},
		{
			name:        "by digest quoted etag",
			manifestURL: digestURL,
			etag:        fmt.Sprintf("%q", dgst),
		},
		{
			name:        "by tag non quoted etag",
			manifestURL: tagURL,
			etag:        dgst.String(),
		},
		{
			name:        "by digest non quoted etag",
			manifestURL: digestURL,
			etag:        dgst.String(),
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, test.manifestURL, nil)
			require.NoError(t, err)

			req.Header.Set("Accept", v1.MediaTypeImageIndex)
			req.Header.Set("If-None-Match", test.etag)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusNotModified, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Empty(t, body)
		})
	}
}

func manifestGetManifestListFallbackToSchema2(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	tagName := "manifestlistfallbacktag"
	repoPath := "manifestlist/fallback"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByDigest)

	_, manifestPayload, err := deserializedManifest.Payload()
	require.NoError(t, err)
	manifestDigest := digest.FromBytes(manifestPayload)

	manifestList := &manifestlist.ManifestList{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			// MediaType field for OCI image indexes is reserved to maintain compatibility and can be blank:
			// https://github.com/opencontainers/image-spec/blob/master/image-index.md#image-index-property-descriptions
			MediaType: "",
		},
		Manifests: []manifestlist.ManifestDescriptor{
			{
				Descriptor: distribution.Descriptor{
					Digest:    manifestDigest,
					MediaType: schema2.MediaTypeManifest,
				},
				Platform: manifestlist.PlatformSpec{
					Architecture: "amd64",
					OS:           "linux",
				},
			},
		},
	}

	deserializedManifestList, err := manifestlist.FromDescriptors(manifestList.Manifests)
	require.NoError(t, err)

	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifestList)
	manifestTagURL := buildManifestTagURL(t, env, repoPath, tagName)

	// Push up manifest list.
	resp, err := putManifest("putting manifest list no error", manifestTagURL, manifestlist.MediaTypeManifestList, deserializedManifestList)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
	require.Equal(t, manifestDigestURL, resp.Header.Get("Location"))

	_, payload, err := deserializedManifestList.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)
	require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))

	if env.ns != nil {
		manifestEvent := buildEventManifestPush(schema2.MediaTypeManifest, repoPath, "", manifestDigest, int64(len(manifestPayload)))
		env.ns.AssertEventNotification(t, manifestEvent)
		manifestListEvent := buildEventManifestPush(manifestlist.MediaTypeManifestList, repoPath, tagName, dgst, int64(len(payload)))
		env.ns.AssertEventNotification(t, manifestListEvent)
	}

	// Get manifest list with without avertising support for manifest lists.
	req, err := http.NewRequest(http.MethodGet, manifestTagURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var fetchedManifest *schema2.DeserializedManifest
	dec := json.NewDecoder(resp.Body)

	err = dec.Decode(&fetchedManifest)
	require.NoError(t, err)

	require.EqualValues(t, deserializedManifest, fetchedManifest)

	if env.ns != nil {
		// we need to assert that the fetched manifest is the one that was sent in the event
		// however, the manifest list pull is not sent at all
		expectedEvent := buildEventManifestPull(schema2.MediaTypeManifest, repoPath, manifestDigest, int64(len(manifestPayload)))
		env.ns.AssertEventNotification(t, expectedEvent)
	}
}

func blobGet(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	// create repository with a layer
	args := makeBlobArgs(t)
	uploadURLBase, _ := startPushLayer(t, env, args.imageName)
	blobURL := pushLayer(t, env.builder, args.imageName, args.layerDigest, uploadURLBase, args.layerFile)

	// fetch layer
	res, err := http.Get(blobURL)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	// verify response headers
	_, err = args.layerFile.Seek(0, io.SeekStart)
	require.NoError(t, err)
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(args.layerFile)
	require.NoError(t, err)

	require.Equal(t, strconv.Itoa(buf.Len()), res.Header.Get("Content-Length"))
	require.Equal(t, "application/octet-stream", res.Header.Get("Content-Type"))
	require.Equal(t, args.layerDigest.String(), res.Header.Get("Docker-Content-Digest"))
	require.Equal(t, fmt.Sprintf(`"%s"`, args.layerDigest), res.Header.Get("ETag"))
	require.Equal(t, "max-age=31536000", res.Header.Get("Cache-Control"))

	// verify response body
	v := args.layerDigest.Verifier()
	_, err = io.Copy(v, res.Body)
	require.NoError(t, err)
	require.True(t, v.Verified())
}

func blobGetRepositoryNotFound(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	args := makeBlobArgs(t)
	ref, err := reference.WithDigest(args.imageName, args.layerDigest)
	require.NoError(t, err)

	blobURL, err := env.builder.BuildBlobURL(ref)
	require.NoError(t, err)

	resp, err := http.Get(blobURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	checkBodyHasErrorCodes(t, "repository not found", resp, v2.ErrorCodeBlobUnknown)
}

func blobGetBlobNotFound(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	// create repository with a layer
	args := makeBlobArgs(t)
	uploadURLBase, _ := startPushLayer(t, env, args.imageName)
	location := pushLayer(t, env.builder, args.imageName, args.layerDigest, uploadURLBase, args.layerFile)

	// delete blob link from repository
	res, err := httpDelete(location)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusAccepted, res.StatusCode)

	// test
	res, err = http.Get(location)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)
	checkBodyHasErrorCodes(t, "blob not found", res, v2.ErrorCodeBlobUnknown)
}

func blobHead(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	// create repository with a layer
	args := makeBlobArgs(t)
	uploadURLBase, _ := startPushLayer(t, env, args.imageName)
	blobURL := pushLayer(t, env.builder, args.imageName, args.layerDigest, uploadURLBase, args.layerFile)

	// check if layer exists
	res, err := http.Head(blobURL)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	// verify headers
	_, err = args.layerFile.Seek(0, io.SeekStart)
	require.NoError(t, err)
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(args.layerFile)
	require.NoError(t, err)

	require.Equal(t, "application/octet-stream", res.Header.Get("Content-Type"))
	require.Equal(t, strconv.Itoa(buf.Len()), res.Header.Get("Content-Length"))
	require.Equal(t, args.layerDigest.String(), res.Header.Get("Docker-Content-Digest"))
	require.Equal(t, fmt.Sprintf(`"%s"`, args.layerDigest), res.Header.Get("ETag"))
	require.Equal(t, "max-age=31536000", res.Header.Get("Cache-Control"))

	// verify body
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Empty(t, body)
}

func blobHeadRepositoryNotFound(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	args := makeBlobArgs(t)
	ref, err := reference.WithDigest(args.imageName, args.layerDigest)
	require.NoError(t, err)

	blobURL, err := env.builder.BuildBlobURL(ref)
	require.NoError(t, err)

	res, err := http.Head(blobURL)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Empty(t, body)
}

func blobHeadBlobNotFound(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	// create repository with a layer
	args := makeBlobArgs(t)
	uploadURLBase, _ := startPushLayer(t, env, args.imageName)
	location := pushLayer(t, env.builder, args.imageName, args.layerDigest, uploadURLBase, args.layerFile)

	// delete blob link from repository
	res, err := httpDelete(location)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusAccepted, res.StatusCode)

	// test
	res, err = http.Head(location)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Empty(t, body)
}

func blobDeleteDisabled(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	// create repository with a layer
	args := makeBlobArgs(t)
	uploadURLBase, _ := startPushLayer(t, env, args.imageName)
	location := pushLayer(t, env.builder, args.imageName, args.layerDigest, uploadURLBase, args.layerFile)

	// Attempt to delete blob link from repository.
	res, err := httpDelete(location)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusMethodNotAllowed, res.StatusCode)
}

func blobDeleteImpl(t *testing.T, opts ...configOpt) string {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	t.Cleanup(env.Shutdown)

	// create repository with a layer
	args := makeBlobArgs(t)
	uploadURLBase, _ := startPushLayer(t, env, args.imageName)
	location := pushLayer(t, env.builder, args.imageName, args.layerDigest, uploadURLBase, args.layerFile)

	// delete blob link from repository
	res, err := httpDelete(location)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusAccepted, res.StatusCode)

	// ensure it's gone
	res, err = http.Head(location)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Empty(t, body)

	return location
}

func blobDelete(t *testing.T, opts ...configOpt) {
	blobDeleteImpl(t, opts...)
}

func blobDeleteAlreadyDeleted(t *testing.T, opts ...configOpt) {
	location := blobDeleteImpl(t, opts...)

	// Attempt to delete blob link from repository again.
	res, err := httpDelete(location)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}

func blobDeleteUnknownRepository(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	// Create url for a blob whose repository does not exist.
	args := makeBlobArgs(t)

	digester := digest.Canonical.Digester()
	sha256Dgst := digester.Digest()

	ref, err := reference.WithDigest(args.imageName, sha256Dgst)
	require.NoError(t, err)

	location, err := env.builder.BuildBlobURL(ref)
	require.NoError(t, err)

	// delete blob link from repository
	res, err := httpDelete(location)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}

func tagsGet(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	sortedTags := []string{
		"2j2ar",
		"asj9e",
		"dcsl6",
		"hpgkt",
		"jyi7b",
		"jyi7b-fxt1v",
		"jyi7b-sgv2q",
		"kb0j5",
		"n343n",
		"sb71y",
	}

	// shuffle tags to make sure results are consistent regardless of creation order (it matters when running
	// against the metadata database)
	shuffledTags := shuffledCopy(sortedTags)

	createRepositoryWithMultipleIdenticalTags(t, env, imageName.Name(), shuffledTags)

	tt := []struct {
		name                string
		runWithoutDBEnabled bool
		queryParams         url.Values
		expectedBody        tagsAPIResponse
		expectedLinkHeader  string
	}{
		{
			name:                "no query parameters",
			expectedBody:        tagsAPIResponse{Name: imageName.Name(), Tags: sortedTags},
			runWithoutDBEnabled: true,
		},
		{
			name:         "empty last query parameter",
			queryParams:  url.Values{"last": []string{""}},
			expectedBody: tagsAPIResponse{Name: imageName.Name(), Tags: sortedTags},
		},
		{
			name:         "empty n query parameter",
			queryParams:  url.Values{"n": []string{""}},
			expectedBody: tagsAPIResponse{Name: imageName.Name(), Tags: sortedTags},
		},
		{
			name:         "empty last and n query parameters",
			queryParams:  url.Values{"last": []string{""}, "n": []string{""}},
			expectedBody: tagsAPIResponse{Name: imageName.Name(), Tags: sortedTags},
		},
		{
			name:         "non integer n query parameter",
			queryParams:  url.Values{"n": []string{"foo"}},
			expectedBody: tagsAPIResponse{Name: imageName.Name(), Tags: sortedTags},
		},
		{
			name:        "1st page",
			queryParams: url.Values{"n": []string{"4"}},
			expectedBody: tagsAPIResponse{Name: imageName.Name(), Tags: []string{
				"2j2ar",
				"asj9e",
				"dcsl6",
				"hpgkt",
			}},
			expectedLinkHeader: `</v2/foo/bar/tags/list?last=hpgkt&n=4>; rel="next"`,
		},
		{
			name:        "nth page",
			queryParams: url.Values{"last": []string{"hpgkt"}, "n": []string{"4"}},
			expectedBody: tagsAPIResponse{Name: imageName.Name(), Tags: []string{
				"jyi7b",
				"jyi7b-fxt1v",
				"jyi7b-sgv2q",
				"kb0j5",
			}},
			expectedLinkHeader: `</v2/foo/bar/tags/list?last=kb0j5&n=4>; rel="next"`,
		},
		{
			name:        "last page",
			queryParams: url.Values{"last": []string{"kb0j5"}, "n": []string{"4"}},
			expectedBody: tagsAPIResponse{Name: imageName.Name(), Tags: []string{
				"n343n",
				"sb71y",
			}},
		},
		{
			name:         "zero page size",
			queryParams:  url.Values{"n": []string{"0"}},
			expectedBody: tagsAPIResponse{Name: imageName.Name(), Tags: sortedTags},
		},
		{
			name:         "page size bigger than full list",
			queryParams:  url.Values{"n": []string{"100"}},
			expectedBody: tagsAPIResponse{Name: imageName.Name(), Tags: sortedTags},
		},
		{
			name:        "after marker",
			queryParams: url.Values{"last": []string{"kb0j5/pic0i"}},
			expectedBody: tagsAPIResponse{Name: imageName.Name(), Tags: []string{
				"n343n",
				"sb71y",
			}},
		},
		{
			name:        "after non existent marker",
			queryParams: url.Values{"last": []string{"does-not-exist"}},
			expectedBody: tagsAPIResponse{Name: imageName.Name(), Tags: []string{
				"hpgkt",
				"jyi7b",
				"jyi7b-fxt1v",
				"jyi7b-sgv2q",
				"kb0j5",
				"n343n",
				"sb71y",
			}},
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			if !test.runWithoutDBEnabled && !env.config.Database.Enabled {
				t.Skip("skipping test because the metadata database is not enabled")
			}

			tagsURL, err := env.builder.BuildTagsURL(imageName, test.queryParams)
			require.NoError(t, err)

			resp, err := http.Get(tagsURL)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			var body tagsAPIResponse
			dec := json.NewDecoder(resp.Body)
			err = dec.Decode(&body)
			require.NoError(t, err)

			require.Equal(t, test.expectedBody, body)
			require.Equal(t, test.expectedLinkHeader, resp.Header.Get("Link"))
		})
	}
}

func tagsGetRepositoryNotFound(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	tagsURL, err := env.builder.BuildTagsURL(imageName)
	require.NoError(t, err)

	resp, err := http.Get(tagsURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Link"))
	checkBodyHasErrorCodes(t, "repository not found", resp, v2.ErrorCodeNameUnknown)
}

func tagsGetEmptyRepository(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	// SETUP

	// create repository and then delete its only tag
	tag := "latest"
	createRepository(t, env, imageName.Name(), tag)

	ref, err := reference.WithTag(imageName, tag)
	require.NoError(t, err)

	manifestURL, err := env.builder.BuildManifestURL(ref)
	require.NoError(t, err)

	res, err := httpDelete(manifestURL)
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, http.StatusAccepted, res.StatusCode)

	// TEST

	tagsURL, err := env.builder.BuildTagsURL(imageName)
	require.NoError(t, err)

	resp, err := http.Get(tagsURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	var body tagsAPIResponse
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Link"))
	require.Equal(t, tagsAPIResponse{Name: imageName.Name()}, body)
}

func manifestDeleteTagNotification(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err, "building named object")

	tag := "latest"
	createRepository(t, env, imageName.Name(), tag)

	ref, err := reference.WithTag(imageName, tag)
	require.NoError(t, err, "building tag reference")

	manifestURL, err := env.builder.BuildManifestURL(ref)
	require.NoError(t, err, "building tag URL")

	resp, err := httpDelete(manifestURL)
	msg := "checking tag delete"
	require.NoError(t, err, msg)

	defer resp.Body.Close()

	checkResponse(t, msg, resp, http.StatusAccepted)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Empty(t, body)

	if env.ns != nil {
		expectedEvent := buildEventManifestDeleteByTag(schema2.MediaTypeManifest, "foo/bar", tag)
		env.ns.AssertEventNotification(t, expectedEvent)
	}
}

func manifestDeleteTagNotificationWithAuth(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err, "building named object")

	tag := "latest"
	createRepository(t, env, imageName.Name(), tag)

	ref, err := reference.WithTag(imageName, tag)
	require.NoError(t, err, "building tag reference")

	tokenProvider := newAuthTokenProvider(t)
	opts = append(opts, withTokenAuth(tokenProvider.certPath(), defaultIssuerProps()))

	// override registry config/setup to use token based authorization for all proceeding requests
	env = newTestEnv(t, opts...)

	manifestURL, err := env.builder.BuildManifestURL(ref)
	require.NoError(t, err, "building tag URL")

	req, err := http.NewRequest(http.MethodDelete, manifestURL, nil)
	require.NoError(t, err)

	// attach authourization header to request
	req = tokenProvider.requestWithAuthActions(req, deleteAccessToken("foo/bar"))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	msg := "checking tag delete"
	checkResponse(t, msg, resp, http.StatusAccepted)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Empty(t, body)

	if env.ns != nil {
		// see the token.ClaimSet inside generateAuthToken
		opt := func(e *notifications.Event) {
			e.Actor = notifications.ActorRecord{
				Name:     authUsername,
				UserType: authUserType,
				User:     authUserJWT,
			}
		}
		expectedEvent := buildEventManifestDeleteByTag(schema2.MediaTypeManifest, "foo/bar", tag, opt)
		env.ns.AssertEventNotification(t, expectedEvent)
	}
}

// manifestDeleteTagWithSameImageID tests that deleting a single image tag will not cause the deletion of other tags
// pointing to the same image ID.
func manifestDeleteTagWithSameImageID(t *testing.T, opts ...configOpt) {
	opts = append(opts, withDelete)
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err, "building named object")

	// build two tags pointing to the same image
	tag1 := "1.0.0"
	tag2 := "latest"
	createRepositoryWithMultipleIdenticalTags(t, env, imageName.Name(), []string{tag1, tag2})

	// delete one of the tags
	ref, err := reference.WithTag(imageName, tag1)
	require.NoError(t, err, "building tag reference")

	manifestURL, err := env.builder.BuildManifestURL(ref)
	require.NoError(t, err, "building tag URL")

	resp, err := httpDelete(manifestURL)
	msg := "checking tag delete"
	require.NoError(t, err, msg)

	defer resp.Body.Close()

	checkResponse(t, msg, resp, http.StatusAccepted)

	if env.ns != nil {
		expectedEvent := buildEventManifestDeleteByTag(schema2.MediaTypeManifest, imageName.String(), tag1)
		env.ns.AssertEventNotification(t, expectedEvent)
	}
	// check the other tag is still there
	tagsURL, err := env.builder.BuildTagsURL(imageName)
	require.NoError(t, err, "unexpected error building tags url")
	resp, err = http.Get(tagsURL)
	require.NoError(t, err, "unexpected error getting tags")
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	var tagsResponse tagsAPIResponse
	err = dec.Decode(&tagsResponse)
	require.NoError(t, err, "unexpected error decoding response")
	require.Equal(t, tagsResponse.Name, imageName.Name(), "tags name should match image name")
	require.Len(t, tagsResponse.Tags, 1)
	require.Equal(t, tagsResponse.Tags[0], tag2)
}

type catalogAPIResponse struct {
	Repositories []string `json:"repositories"`
}

// catalogGet tests the /v2/_catalog endpoint
func catalogGet(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	sortedRepos := []string{
		"2j2ar",
		"asj9e/ieakg",
		"dcsl6/xbd1z/9t56s",
		"hpgkt/bmawb",
		"jyi7b",
		"jyi7b/sgv2q/d5a2f",
		"jyi7b/sgv2q/fxt1v",
		"kb0j5/pic0i",
		"n343n",
		"sb71y",
	}

	// shuffle repositories to make sure results are consistent regardless of creation order (it matters when running
	// against the metadata database)
	shuffledRepos := shuffledCopy(sortedRepos)

	for _, repo := range shuffledRepos {
		createRepository(t, env, repo, "latest")
	}

	tt := []struct {
		name               string
		queryParams        url.Values
		expectedBody       catalogAPIResponse
		expectedLinkHeader string
	}{
		{
			name:         "no query parameters",
			expectedBody: catalogAPIResponse{Repositories: sortedRepos},
		},
		{
			name:         "empty last query parameter",
			queryParams:  url.Values{"last": []string{""}},
			expectedBody: catalogAPIResponse{Repositories: sortedRepos},
		},
		{
			name:         "empty n query parameter",
			queryParams:  url.Values{"n": []string{""}},
			expectedBody: catalogAPIResponse{Repositories: sortedRepos},
		},
		{
			name:         "empty last and n query parameters",
			queryParams:  url.Values{"last": []string{""}, "n": []string{""}},
			expectedBody: catalogAPIResponse{Repositories: sortedRepos},
		},
		{
			name:         "non integer n query parameter",
			queryParams:  url.Values{"n": []string{"foo"}},
			expectedBody: catalogAPIResponse{Repositories: sortedRepos},
		},
		{
			name:        "1st page",
			queryParams: url.Values{"n": []string{"4"}},
			expectedBody: catalogAPIResponse{Repositories: []string{
				"2j2ar",
				"asj9e/ieakg",
				"dcsl6/xbd1z/9t56s",
				"hpgkt/bmawb",
			}},
			expectedLinkHeader: `</v2/_catalog?last=hpgkt%2Fbmawb&n=4>; rel="next"`,
		},
		{
			name:        "nth page",
			queryParams: url.Values{"last": []string{"hpgkt/bmawb"}, "n": []string{"4"}},
			expectedBody: catalogAPIResponse{Repositories: []string{
				"jyi7b",
				"jyi7b/sgv2q/d5a2f",
				"jyi7b/sgv2q/fxt1v",
				"kb0j5/pic0i",
			}},
			expectedLinkHeader: `</v2/_catalog?last=kb0j5%2Fpic0i&n=4>; rel="next"`,
		},
		{
			name:        "last page",
			queryParams: url.Values{"last": []string{"kb0j5/pic0i"}, "n": []string{"4"}},
			expectedBody: catalogAPIResponse{Repositories: []string{
				"n343n",
				"sb71y",
			}},
		},
		{
			name:         "zero page size",
			queryParams:  url.Values{"n": []string{"0"}},
			expectedBody: catalogAPIResponse{Repositories: sortedRepos},
		},
		{
			name:         "page size bigger than full list",
			queryParams:  url.Values{"n": []string{"100"}},
			expectedBody: catalogAPIResponse{Repositories: sortedRepos},
		},
		{
			name:        "after marker",
			queryParams: url.Values{"last": []string{"kb0j5/pic0i"}},
			expectedBody: catalogAPIResponse{Repositories: []string{
				"n343n",
				"sb71y",
			}},
		},
		{
			name:        "after non existent marker",
			queryParams: url.Values{"last": []string{"does-not-exist"}},
			expectedBody: catalogAPIResponse{Repositories: []string{
				"hpgkt/bmawb",
				"jyi7b",
				"jyi7b/sgv2q/d5a2f",
				"jyi7b/sgv2q/fxt1v",
				"kb0j5/pic0i",
				"n343n",
				"sb71y",
			}},
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			catalogURL, err := env.builder.BuildCatalogURL(test.queryParams)
			require.NoError(t, err)

			resp, err := http.Get(catalogURL)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			var body catalogAPIResponse
			dec := json.NewDecoder(resp.Body)
			err = dec.Decode(&body)
			require.NoError(t, err)

			require.Equal(t, test.expectedBody, body)
			require.Equal(t, test.expectedLinkHeader, resp.Header.Get("Link"))
		})
	}
}

func catalogGetEmpty(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	catalogURL, err := env.builder.BuildCatalogURL()
	require.NoError(t, err)

	resp, err := http.Get(catalogURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body catalogAPIResponse
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&body)
	require.NoError(t, err)

	require.Empty(t, body.Repositories)
	require.Empty(t, resp.Header.Get("Link"))
}

func catalogGetTooLarge(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	catalogURL, err := env.builder.BuildCatalogURL(url.Values{"n": []string{"500000000000"}})
	require.NoError(t, err)

	resp, err := http.Get(catalogURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	checkBodyHasErrorCodes(t, "requesting too many repos", resp, v2.ErrorCodePaginationNumberInvalid)
}
