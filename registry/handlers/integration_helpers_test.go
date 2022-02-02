// +build integration

package handlers_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/internal/migration"
	"github.com/docker/distribution/testutil"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

type blobArgs struct {
	imageName   reference.Named
	layerFile   io.ReadSeeker
	layerDigest digest.Digest
}

func makeBlobArgs(t *testing.T) blobArgs {
	layerFile, layerDigest, err := testutil.CreateRandomTarFile()
	require.NoError(t, err)

	args := blobArgs{
		layerFile:   layerFile,
		layerDigest: layerDigest,
	}
	args.imageName, err = reference.WithName("foo/bar")
	require.NoError(t, err)

	return args
}

func makeBlobArgsWithRepoName(t *testing.T, repoName string) blobArgs {
	layerFile, layerDigest, err := testutil.CreateRandomTarFile()
	require.NoError(t, err)

	args := blobArgs{
		layerFile:   layerFile,
		layerDigest: layerDigest,
	}
	args.imageName, err = reference.WithName(repoName)
	require.NoError(t, err)

	return args
}

func asyncDo(f func()) chan struct{} {
	done := make(chan struct{})
	go func() {
		f()
		close(done)
	}()
	return done
}

func createRepoWithBlob(t *testing.T, env *testEnv) (blobArgs, string) {
	t.Helper()

	args := makeBlobArgs(t)
	uploadURLBase, _ := startPushLayer(t, env, args.imageName)
	blobURL := pushLayer(t, env.builder, args.imageName, args.layerDigest, uploadURLBase, args.layerFile)

	return args, blobURL
}

func createNamedRepoWithBlob(t *testing.T, env *testEnv, repoName string) (blobArgs, string) {
	t.Helper()

	args := makeBlobArgsWithRepoName(t, repoName)
	uploadURLBase, _ := startPushLayer(t, env, args.imageName)
	blobURL := pushLayer(t, env.builder, args.imageName, args.layerDigest, uploadURLBase, args.layerFile)

	return args, blobURL
}

func assertGetResponse(t *testing.T, url string, expectedStatus int) {
	t.Helper()

	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)
}

func assertHeadResponse(t *testing.T, url string, expectedStatus int) {
	t.Helper()

	resp, err := http.Head(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)
}

func assertPutResponse(t *testing.T, url string, body io.Reader, headers http.Header, expectedStatus int) {
	t.Helper()

	req, err := http.NewRequest("PUT", url, body)
	require.NoError(t, err)
	for k, vv := range headers {
		req.Header.Set(k, strings.Join(vv, ","))
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)
}

func assertPostResponse(t *testing.T, url string, body io.Reader, headers http.Header, expectedStatus int) {
	t.Helper()

	req, err := http.NewRequest("POST", url, body)
	require.NoError(t, err)
	for k, vv := range headers {
		req.Header.Set(k, strings.Join(vv, ","))
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)
}

func assertDeleteResponse(t *testing.T, url string, expectedStatus int) {
	t.Helper()

	resp, err := httpDelete(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)
}

func assertTagDeleteResponse(t *testing.T, env *testEnv, repoName, tagName string, expectedStatus int) {
	t.Helper()

	tmp, err := reference.WithName(repoName)
	require.NoError(t, err)
	named, err := reference.WithTag(tmp, tagName)
	require.NoError(t, err)
	u, err := env.builder.BuildTagURL(named)
	require.NoError(t, err)

	assertDeleteResponse(t, u, expectedStatus)
}

func assertBlobGetResponse(t *testing.T, env *testEnv, repoName string, dgst digest.Digest, expectedStatus int) {
	t.Helper()

	tmp, err := reference.WithName(repoName)
	require.NoError(t, err)
	ref, err := reference.WithDigest(tmp, dgst)
	require.NoError(t, err)
	u, err := env.builder.BuildBlobURL(ref)
	require.NoError(t, err)

	assertGetResponse(t, u, expectedStatus)
}

func assertBlobHeadResponse(t *testing.T, env *testEnv, repoName string, dgst digest.Digest, expectedStatus int) {
	t.Helper()

	tmp, err := reference.WithName(repoName)
	require.NoError(t, err)
	ref, err := reference.WithDigest(tmp, dgst)
	require.NoError(t, err)
	u, err := env.builder.BuildBlobURL(ref)
	require.NoError(t, err)

	assertHeadResponse(t, u, expectedStatus)
}

func assertBlobDeleteResponse(t *testing.T, env *testEnv, repoName string, dgst digest.Digest, expectedStatus int) {
	t.Helper()

	tmp, err := reference.WithName(repoName)
	require.NoError(t, err)
	ref, err := reference.WithDigest(tmp, dgst)
	require.NoError(t, err)
	u, err := env.builder.BuildBlobURL(ref)
	require.NoError(t, err)

	assertDeleteResponse(t, u, expectedStatus)
}

func assertBlobPutResponse(t *testing.T, env *testEnv, repoName string, dgst digest.Digest, body io.ReadSeeker, expectedStatus int) {
	t.Helper()

	name, err := reference.WithName(repoName)
	require.NoError(t, err)

	baseURL, _ := startPushLayer(t, env, name)
	u, err := url.Parse(baseURL)
	require.NoError(t, err)
	u.RawQuery = url.Values{
		"_state": u.Query()["_state"],
		"digest": []string{dgst.String()},
	}.Encode()

	assertPutResponse(t, u.String(), body, nil, expectedStatus)
}

func assertBlobPostMountResponse(t *testing.T, env *testEnv, srcRepoName, destRepoName string, dgst digest.Digest, expectedStatus int) {
	t.Helper()

	name, err := reference.WithName(destRepoName)
	require.NoError(t, err)
	u, err := env.builder.BuildBlobUploadURL(name, url.Values{
		"mount": []string{dgst.String()},
		"from":  []string{srcRepoName},
	})
	require.NoError(t, err)

	assertPostResponse(t, u, nil, nil, expectedStatus)
}

func assertManifestGetByDigestResponse(t *testing.T, env *testEnv, repoName string, m distribution.Manifest, expectedStatus int) {
	t.Helper()

	u := buildManifestDigestURL(t, env, repoName, m)
	assertGetResponse(t, u, expectedStatus)
}

func assertManifestGetByTagResponse(t *testing.T, env *testEnv, repoName, tagName string, expectedStatus int) {
	t.Helper()

	u := buildManifestTagURL(t, env, repoName, tagName)
	assertGetResponse(t, u, expectedStatus)
}

func assertManifestHeadByDigestResponse(t *testing.T, env *testEnv, repoName string, m distribution.Manifest, expectedStatus int) {
	t.Helper()

	u := buildManifestDigestURL(t, env, repoName, m)
	assertHeadResponse(t, u, expectedStatus)
}

func assertManifestHeadByTagResponse(t *testing.T, env *testEnv, repoName, tagName string, expectedStatus int) {
	t.Helper()

	u := buildManifestTagURL(t, env, repoName, tagName)
	assertHeadResponse(t, u, expectedStatus)
}

func assertManifestPutByDigestResponse(t *testing.T, env *testEnv, repoName string, m distribution.Manifest, mediaType string, expectedStatus int) {
	t.Helper()

	u := buildManifestDigestURL(t, env, repoName, m)
	_, body, err := m.Payload()
	require.NoError(t, err)

	assertPutResponse(t, u, bytes.NewReader(body), http.Header{"Content-Type": []string{mediaType}}, expectedStatus)
}

func assertManifestPutByTagResponse(t *testing.T, env *testEnv, repoName string, m distribution.Manifest, mediaType, tagName string, expectedStatus int) {
	t.Helper()

	u := buildManifestTagURL(t, env, repoName, tagName)
	_, body, err := m.Payload()
	require.NoError(t, err)

	assertPutResponse(t, u, bytes.NewReader(body), http.Header{"Content-Type": []string{mediaType}}, expectedStatus)
}

func assertManifestDeleteResponse(t *testing.T, env *testEnv, repoName string, m distribution.Manifest, expectedStatus int) {
	t.Helper()

	u := buildManifestDigestURL(t, env, repoName, m)
	assertDeleteResponse(t, u, expectedStatus)
}

type mockImportNotification struct {
	t             *testing.T
	receivedNotif chan migration.Notification
}

func newMockImportNotification(t *testing.T) *mockImportNotification {
	t.Helper()

	min := &mockImportNotification{
		t:             t,
		receivedNotif: make(chan migration.Notification),
	}
	t.Cleanup(func() {
		close(min.receivedNotif)
	})

	return min
}

func (min *mockImportNotification) handleNotificationRequest(w http.ResponseWriter, r *http.Request) {
	t := min.t
	t.Helper()

	// PUT /api/:version/registry/repositories/:path/migration/status
	require.Equal(t, http.MethodPut, r.Method, "method not allowed")

	actualNotification := migration.Notification{}
	err := json.NewDecoder(r.Body).Decode(&actualNotification)
	require.NoError(t, err)

	min.receivedNotif <- actualNotification

	w.WriteHeader(http.StatusOK)
}

func mockImportNotificationServer(t *testing.T, min *mockImportNotification) string {
	t.Helper()

	s := httptest.NewServer(http.HandlerFunc(min.handleNotificationRequest))

	return s.URL
}

func (min *mockImportNotification) waitForImportNotification(t *testing.T, path, status, detail string, timeout time.Duration) {
	t.Helper()

	expectedNotif := migration.Notification{
		Name:   repositoryName(path),
		Path:   path,
		Status: status,
		Detail: detail,
	}

	select {
	case receivedNotif := <-min.receivedNotif:
		require.Equal(t, expectedNotif, receivedNotif)
	case <-time.After(timeout):
		t.Errorf("timed out waiting for import notification")
	}
}

// repositoryName parses a repository path (e.g. `"a/b/c"`) and returns its name (e.g. `"c"`).
// copied from registry/datastore/repository.go
func repositoryName(path string) string {
	segments := strings.Split(filepath.Clean(path), "/")
	return segments[len(segments)-1]
}
