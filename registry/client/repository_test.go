package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/testutil"
	"github.com/docker/distribution/uuid"
	"github.com/docker/libtrust"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testServer(rrm testutil.RequestResponseMap) (string, func()) {
	h := testutil.NewHandler(rrm)
	s := httptest.NewServer(h)
	return s.URL, s.Close
}

func newRandomBlob(tb testing.TB, size int64) (digest.Digest, []byte) {
	tb.Helper()

	b := testutil.RandomBlob(tb, size)

	return digest.FromBytes(b), b
}

func addTestFetch(repo string, dgst digest.Digest, content []byte, m *testutil.RequestResponseMap) {
	*m = append(*m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodGet,
			Route:  "/v2/" + repo + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Body:       content,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})

	*m = append(*m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodHead,
			Route:  "/v2/" + repo + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})
}

func addTestCatalog(route string, content []byte, link string, m *testutil.RequestResponseMap) {
	headers := map[string][]string{
		"Content-Length": {strconv.Itoa(len(content))},
		"Content-Type":   {"application/json"},
	}
	if link != "" {
		headers["Link"] = append(headers["Link"], link)
	}

	*m = append(*m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodGet,
			Route:  route,
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Body:       content,
			Headers:    http.Header(headers),
		},
	})
}

func TestBlobDelete(t *testing.T) {
	dgst, _ := newRandomBlob(t, 1024)
	var m testutil.RequestResponseMap
	repo, _ := reference.WithName("test.example.com/repo1")
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodDelete,
			Route:  "/v2/" + repo.Name() + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Content-Length": {"0"},
			}),
		},
	})

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)
	l := r.Blobs(ctx)
	err = l.Delete(ctx, dgst)
	assert.NoError(t, err, "error deleting blob")
}

func TestBlobFetch(t *testing.T) {
	d1, b1 := newRandomBlob(t, 1024)
	var m testutil.RequestResponseMap
	addTestFetch("test.example.com/repo1", d1, b1, &m)

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	repo, _ := reference.WithName("test.example.com/repo1")
	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)
	l := r.Blobs(ctx)

	b, err := l.Get(ctx, d1)
	require.NoError(t, err)
	require.Equal(t, b1, b)

	// TODO(dmcgowan): Test for unknown blob case
}

func TestBlobExistsNoContentLength(t *testing.T) {
	var m testutil.RequestResponseMap

	repo, _ := reference.WithName("biff")
	dgst, content := newRandomBlob(t, 1024)
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodGet,
			Route:  "/v2/" + repo.Name() + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Body:       content,
			Headers: http.Header(map[string][]string{
				//			"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified": {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})

	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodHead,
			Route:  "/v2/" + repo.Name() + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				//			"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified": {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})
	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)
	l := r.Blobs(ctx)

	_, err = l.Stat(ctx, dgst)
	require.Error(t, err)
	assert.ErrorContains(t, err, "missing content-length header")
}

func TestBlobExists(t *testing.T) {
	d1, b1 := newRandomBlob(t, 1024)
	var m testutil.RequestResponseMap
	addTestFetch("test.example.com/repo1", d1, b1, &m)

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	repo, _ := reference.WithName("test.example.com/repo1")
	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)
	l := r.Blobs(ctx)

	stat, err := l.Stat(ctx, d1)
	require.NoError(t, err)

	assert.Equal(t, d1, stat.Digest, "unexpected digest")

	assert.Equal(t, int64(len(b1)), stat.Size, "unexpected length")

	// TODO(dmcgowan): Test error cases and ErrBlobUnknown case
}

func TestBlobUploadChunked(t *testing.T) {
	dgst, b1 := newRandomBlob(t, 1024)
	var m testutil.RequestResponseMap
	chunks := [][]byte{
		b1[0:256],
		b1[256:512],
		b1[512:513],
		b1[513:1024],
	}
	repo, _ := reference.WithName("test.example.com/uploadrepo")
	uuids := []string{uuid.Generate().String()}
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodPost,
			Route:  "/v2/" + repo.Name() + "/blobs/uploads/",
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Content-Length":     {"0"},
				"Location":           {"/v2/" + repo.Name() + "/blobs/uploads/" + uuids[0]},
				"Docker-Upload-UUID": {uuids[0]},
				"Range":              {"0-0"},
			}),
		},
	})
	offset := 0
	for i, chunk := range chunks {
		uuids = append(uuids, uuid.Generate().String())
		newOffset := offset + len(chunk)
		m = append(m, testutil.RequestResponseMapping{
			Request: testutil.Request{
				Method: http.MethodPatch,
				Route:  "/v2/" + repo.Name() + "/blobs/uploads/" + uuids[i],
				Body:   chunk,
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
				Headers: http.Header(map[string][]string{
					"Content-Length":     {"0"},
					"Location":           {"/v2/" + repo.Name() + "/blobs/uploads/" + uuids[i+1]},
					"Docker-Upload-UUID": {uuids[i+1]},
					"Range":              {fmt.Sprintf("%d-%d", offset, newOffset-1)},
				}),
			},
		})
		offset = newOffset
	}
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodPut,
			Route:  "/v2/" + repo.Name() + "/blobs/uploads/" + uuids[len(uuids)-1],
			QueryParams: map[string][]string{
				"digest": {dgst.String()},
			},
		},
		Response: testutil.Response{
			StatusCode: http.StatusCreated,
			Headers: http.Header(map[string][]string{
				"Content-Length":        {"0"},
				"Docker-Content-Digest": {dgst.String()},
				"Content-Range":         {fmt.Sprintf("0-%d", offset-1)},
			}),
		},
	})
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodHead,
			Route:  "/v2/" + repo.Name() + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(offset)},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)
	l := r.Blobs(ctx)

	upload, err := l.Create(ctx)
	require.NoError(t, err)

	require.Equal(t, uuids[0], upload.ID(), "unexpected UUID")

	for _, chunk := range chunks {
		n, err := upload.Write(chunk)
		require.NoError(t, err)
		require.Len(t, chunk, n, "unexpected length returned from write")
	}

	blob, err := upload.Commit(ctx, distribution.Descriptor{
		Digest: dgst,
		Size:   int64(len(b1)),
	})
	require.NoError(t, err)

	assert.Len(t, b1, int(blob.Size), "unexpected blob size")
}

func TestBlobUploadMonolithic(t *testing.T) {
	dgst, b1 := newRandomBlob(t, 1024)
	var m testutil.RequestResponseMap
	repo, _ := reference.WithName("test.example.com/uploadrepo")
	uploadID := uuid.Generate().String()
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodPost,
			Route:  "/v2/" + repo.Name() + "/blobs/uploads/",
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Content-Length":     {"0"},
				"Location":           {"/v2/" + repo.Name() + "/blobs/uploads/" + uploadID},
				"Docker-Upload-UUID": {uploadID},
				"Range":              {"0-0"},
			}),
		},
	})
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodPatch,
			Route:  "/v2/" + repo.Name() + "/blobs/uploads/" + uploadID,
			Body:   b1,
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Location":              {"/v2/" + repo.Name() + "/blobs/uploads/" + uploadID},
				"Docker-Upload-UUID":    {uploadID},
				"Content-Length":        {"0"},
				"Docker-Content-Digest": {dgst.String()},
				"Range":                 {fmt.Sprintf("0-%d", len(b1)-1)},
			}),
		},
	})
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodPut,
			Route:  "/v2/" + repo.Name() + "/blobs/uploads/" + uploadID,
			QueryParams: map[string][]string{
				"digest": {dgst.String()},
			},
		},
		Response: testutil.Response{
			StatusCode: http.StatusCreated,
			Headers: http.Header(map[string][]string{
				"Content-Length":        {"0"},
				"Docker-Content-Digest": {dgst.String()},
				"Content-Range":         {fmt.Sprintf("0-%d", len(b1)-1)},
			}),
		},
	})
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodHead,
			Route:  "/v2/" + repo.Name() + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(b1))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)
	l := r.Blobs(ctx)

	upload, err := l.Create(ctx)
	require.NoError(t, err)

	require.Equalf(t, upload.ID(), uploadID, "Unexpected UUID %s; expected %s", upload.ID(), uploadID)

	n, err := upload.ReadFrom(bytes.NewReader(b1))
	require.NoError(t, err)
	require.Len(t, b1, int(n))

	blob, err := upload.Commit(ctx, distribution.Descriptor{
		Digest: dgst,
		Size:   int64(len(b1)),
	})
	require.NoError(t, err)

	require.Len(t, b1, int(blob.Size))
}

func TestBlobMount(t *testing.T) {
	dgst, content := newRandomBlob(t, 1024)
	var m testutil.RequestResponseMap
	repo, _ := reference.WithName("test.example.com/uploadrepo")

	sourceRepo, _ := reference.WithName("test.example.com/sourcerepo")
	canonicalRef, _ := reference.WithDigest(sourceRepo, dgst)

	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method:      http.MethodPost,
			Route:       "/v2/" + repo.Name() + "/blobs/uploads/",
			QueryParams: map[string][]string{"from": {sourceRepo.Name()}, "mount": {dgst.String()}},
		},
		Response: testutil.Response{
			StatusCode: http.StatusCreated,
			Headers: http.Header(map[string][]string{
				"Content-Length":        {"0"},
				"Location":              {"/v2/" + repo.Name() + "/blobs/" + dgst.String()},
				"Docker-Content-Digest": {dgst.String()},
			}),
		},
	})
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodHead,
			Route:  "/v2/" + repo.Name() + "/blobs/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
			}),
		},
	})

	e, cancelF := testServer(m)
	defer cancelF()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)

	l := r.Blobs(ctx)

	bw, err := l.Create(ctx, WithMountFrom(canonicalRef))
	require.Nilf(t, bw, "Expected blob writer to be nil, was %v", bw)

	if ebm, ok := err.(distribution.ErrBlobMounted); ok {
		require.Equalf(t, ebm.From.Digest(), dgst, "Unexpected digest: %s, expected %s", ebm.From.Digest(), dgst)
		require.Equalf(t, ebm.From.Name(), sourceRepo.Name(), "Unexpected from: %s, expected %s", ebm.From.Name(), sourceRepo)
	} else {
		require.FailNow(t, fmt.Sprintf("Unexpected error: %v, expected an ErrBlobMounted", err))
	}
}

func newRandomSchemaV1Manifest(tb testing.TB, name reference.Named, tag string, blobCount int) (*schema1.SignedManifest, digest.Digest, []byte) {
	blobs := make([]schema1.FSLayer, blobCount)
	history := make([]schema1.History, blobCount)

	for i := 0; i < blobCount; i++ {
		dgst, blob := newRandomBlob(tb, int64((i%5)*16))

		blobs[i] = schema1.FSLayer{BlobSum: dgst}
		history[i] = schema1.History{V1Compatibility: fmt.Sprintf("{\"Hex\": \"%x\"}", blob)}
	}

	m := schema1.Manifest{
		Name:         name.String(),
		Tag:          tag,
		Architecture: "x86",
		FSLayers:     blobs,
		History:      history,
		Versioned: manifest.Versioned{
			SchemaVersion: 1,
		},
	}

	pk, err := libtrust.GenerateECP256PrivateKey()
	if err != nil {
		panic(err)
	}

	sm, err := schema1.Sign(&m, pk)
	if err != nil {
		panic(err)
	}

	return sm, digest.FromBytes(sm.Canonical), sm.Canonical
}

func addTestManifestWithEtag(repo reference.Named, referenceName string, content []byte, m *testutil.RequestResponseMap, dgst string) {
	actualDigest := digest.FromBytes(content)
	getReqWithEtag := testutil.Request{
		Method: http.MethodGet,
		Route:  "/v2/" + repo.Name() + "/manifests/" + referenceName,
		Headers: http.Header(map[string][]string{
			"If-None-Match": {fmt.Sprintf(`"%s"`, dgst)},
		}),
	}

	var getRespWithEtag testutil.Response
	if actualDigest.String() == dgst {
		getRespWithEtag = testutil.Response{
			StatusCode: http.StatusNotModified,
			Body:       make([]byte, 0),
			Headers: http.Header(map[string][]string{
				"Content-Length": {"0"},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				"Content-Type":   {schema1.MediaTypeSignedManifest},
			}),
		}
	} else {
		getRespWithEtag = testutil.Response{
			StatusCode: http.StatusOK,
			Body:       content,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				"Content-Type":   {schema1.MediaTypeSignedManifest},
			}),
		}
	}
	*m = append(*m, testutil.RequestResponseMapping{Request: getReqWithEtag, Response: getRespWithEtag})
}

func contentDigestString(mediatype string, content []byte) string {
	if mediatype == schema1.MediaTypeSignedManifest {
		m, _, _ := distribution.UnmarshalManifest(mediatype, content)
		content = m.(*schema1.SignedManifest).Canonical
	}
	return digest.Canonical.FromBytes(content).String()
}

func addTestManifest(repo reference.Named, referenceName, mediatype string, content []byte, m *testutil.RequestResponseMap) {
	*m = append(*m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodGet,
			Route:  "/v2/" + repo.Name() + "/manifests/" + referenceName,
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Body:       content,
			Headers: http.Header(map[string][]string{
				"Content-Length":        {fmt.Sprint(len(content))},
				"Last-Modified":         {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				"Content-Type":          {mediatype},
				"Docker-Content-Digest": {contentDigestString(mediatype, content)},
			}),
		},
	})
	*m = append(*m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodHead,
			Route:  "/v2/" + repo.Name() + "/manifests/" + referenceName,
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				"Content-Length":        {fmt.Sprint(len(content))},
				"Last-Modified":         {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				"Content-Type":          {mediatype},
				"Docker-Content-Digest": {digest.Canonical.FromBytes(content).String()},
			}),
		},
	})
}

func addTestManifestWithoutDigestHeader(repo reference.Named, referenceName, mediatype string, content []byte, m *testutil.RequestResponseMap) {
	*m = append(*m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodGet,
			Route:  "/v2/" + repo.Name() + "/manifests/" + referenceName,
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Body:       content,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				"Content-Type":   {mediatype},
			}),
		},
	})
	*m = append(*m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodHead,
			Route:  "/v2/" + repo.Name() + "/manifests/" + referenceName,
		},
		Response: testutil.Response{
			StatusCode: http.StatusOK,
			Headers: http.Header(map[string][]string{
				"Content-Length": {fmt.Sprint(len(content))},
				"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				"Content-Type":   {mediatype},
			}),
		},
	})
}

func checkEqualManifest(m1, m2 *schema1.SignedManifest) error {
	if m1.Name != m2.Name {
		return fmt.Errorf("name does not match %q != %q", m1.Name, m2.Name)
	}
	if m1.Tag != m2.Tag {
		return fmt.Errorf("tag does not match %q != %q", m1.Tag, m2.Tag)
	}
	if len(m1.FSLayers) != len(m2.FSLayers) {
		return fmt.Errorf("fs blob length does not match %d != %d", len(m1.FSLayers), len(m2.FSLayers))
	}
	for i := range m1.FSLayers {
		if m1.FSLayers[i].BlobSum != m2.FSLayers[i].BlobSum {
			return fmt.Errorf("blobsum does not match %q != %q", m1.FSLayers[i].BlobSum, m2.FSLayers[i].BlobSum)
		}
	}
	if len(m1.History) != len(m2.History) {
		return fmt.Errorf("history length does not match %d != %d", len(m1.History), len(m2.History))
	}
	for i := range m1.History {
		if m1.History[i].V1Compatibility != m2.History[i].V1Compatibility {
			return fmt.Errorf("blobsum does not match %q != %q", m1.History[i].V1Compatibility, m2.History[i].V1Compatibility)
		}
	}
	return nil
}

func TestV1ManifestFetch(t *testing.T) {
	ctx := context.Background()
	repo, _ := reference.WithName("test.example.com/repo")
	m1, dgst, _ := newRandomSchemaV1Manifest(t, repo, "latest", 6)
	var m testutil.RequestResponseMap
	_, pl, err := m1.Payload()
	require.NoError(t, err)
	addTestManifest(repo, dgst.String(), schema1.MediaTypeSignedManifest, pl, &m)
	addTestManifest(repo, "latest", schema1.MediaTypeSignedManifest, pl, &m)
	addTestManifest(repo, "badcontenttype", "text/html", pl, &m)

	e, c := testServer(m)
	defer c()

	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)

	ms, err := r.Manifests(ctx)
	require.NoError(t, err)

	ok, err := ms.Exists(ctx, dgst)
	require.NoError(t, err)
	require.True(t, ok, "Manifest does not exist")

	testManifest, err := ms.Get(ctx, dgst)
	require.NoError(t, err)

	v1manifest, ok := testManifest.(*schema1.SignedManifest)
	require.True(t, ok, "Unexpected manifest type from Get: %T", testManifest)

	err = checkEqualManifest(v1manifest, m1)
	require.NoError(t, err)

	var contentDigest digest.Digest
	testManifest, err = ms.Get(ctx, dgst, distribution.WithTag("latest"), ReturnContentDigest(&contentDigest))
	require.NoError(t, err)
	v1manifest, ok = testManifest.(*schema1.SignedManifest)
	require.True(t, ok, "Unexpected manifest type from Get: %T", testManifest)

	err = checkEqualManifest(v1manifest, m1)
	require.NoError(t, err)

	require.Equal(t, contentDigest, dgst)

	_, err = ms.Get(ctx, dgst, distribution.WithTag("badcontenttype"))
	require.Error(t, err)
	require.ErrorContains(t, err, "no mediaType in manifest")
}

func TestManifestFetchWithEtag(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo/by/tag")
	_, d1, p1 := newRandomSchemaV1Manifest(t, repo, "latest", 6)
	var m testutil.RequestResponseMap
	addTestManifestWithEtag(repo, "latest", p1, &m, d1.String())

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)

	ms, err := r.Manifests(ctx)
	require.NoError(t, err)

	clientManifestService, ok := ms.(*manifests)
	if !ok {
		panic("wrong type for client manifest service")
	}
	_, err = clientManifestService.Get(ctx, d1, distribution.WithTag("latest"), AddEtagToTag("latest", d1.String()))
	require.ErrorIs(t, err, distribution.ErrManifestNotModified)
}

func TestManifestFetchWithAccept(t *testing.T) {
	ctx := context.Background()
	repo, _ := reference.WithName("test.example.com/repo")
	_, dgst, _ := newRandomSchemaV1Manifest(t, repo, "latest", 6)
	headers := make(chan []string, 1)
	s := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		headers <- req.Header["Accept"]
	}))
	defer close(headers)
	defer s.Close()

	r, err := NewRepository(repo, s.URL, nil)
	require.NoError(t, err)
	ms, err := r.Manifests(ctx)
	require.NoError(t, err)

	testCases := []struct {
		// the media types we send
		mediaTypes []string
		// the expected Accept headers the server should receive
		expect []string
		// whether to sort the request and response values for comparison
		sort bool
	}{
		{
			mediaTypes: make([]string, 0),
			expect:     distribution.ManifestMediaTypes(),
			sort:       true,
		},
		{
			mediaTypes: []string{"test1", "test2"},
			expect:     []string{"test1", "test2"},
		},
		{
			mediaTypes: []string{"test1"},
			expect:     []string{"test1"},
		},
		{
			mediaTypes: []string{""},
			expect:     []string{""},
		},
	}
	for _, testCase := range testCases {
		ms.Get(ctx, dgst, distribution.WithManifestMediaTypes(testCase.mediaTypes))
		actual := <-headers
		if testCase.sort {
			sort.Strings(actual)
			sort.Strings(testCase.expect)
		}
		require.Equal(t, actual, testCase.expect)
	}
}

func TestManifestDelete(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo/delete")
	_, dgst1, _ := newRandomSchemaV1Manifest(t, repo, "latest", 6)
	_, dgst2, _ := newRandomSchemaV1Manifest(t, repo, "latest", 6)
	var m testutil.RequestResponseMap
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodDelete,
			Route:  "/v2/" + repo.Name() + "/manifests/" + dgst1.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Content-Length": {"0"},
			}),
		},
	})

	e, c := testServer(m)
	defer c()

	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)
	ctx := context.Background()
	ms, err := r.Manifests(ctx)
	require.NoError(t, err)

	err = ms.Delete(ctx, dgst1)
	require.NoError(t, err)

	err = ms.Delete(ctx, dgst2)
	require.Error(t, err, "Expected error deleting unknown manifest")
	// TODO(dmcgowan): Check for specific unknown error
}

func TestManifestPut(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo/delete")
	m1, dgst, _ := newRandomSchemaV1Manifest(t, repo, "other", 6)

	_, payload, err := m1.Payload()
	require.NoError(t, err)

	var m testutil.RequestResponseMap
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodPut,
			Route:  "/v2/" + repo.Name() + "/manifests/other",
			Body:   payload,
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Content-Length":        {"0"},
				"Docker-Content-Digest": {dgst.String()},
			}),
		},
	})

	putDgst := digest.FromBytes(m1.Canonical)
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodPut,
			Route:  "/v2/" + repo.Name() + "/manifests/" + putDgst.String(),
			Body:   payload,
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: http.Header(map[string][]string{
				"Content-Length":        {"0"},
				"Docker-Content-Digest": {putDgst.String()},
			}),
		},
	})

	e, c := testServer(m)
	defer c()

	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)
	ctx := context.Background()
	ms, err := r.Manifests(ctx)
	require.NoError(t, err)

	_, err = ms.Put(ctx, m1, distribution.WithTag(m1.Tag))
	require.NoError(t, err)

	_, err = ms.Put(ctx, m1)
	require.NoError(t, err)

	// TODO(dmcgowan): Check for invalid input error
}

func TestManifestTags(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo/tags/list")
	tagsList := []byte(strings.TrimSpace(`
{
	"name": "test.example.com/repo/tags/list",
	"tags": [
		"tag1",
		"tag2",
		"funtag"
	]
}
	`))
	var m testutil.RequestResponseMap
	for i := 0; i < 3; i++ {
		m = append(m, testutil.RequestResponseMapping{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/" + repo.Name() + "/tags/list",
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       tagsList,
				Headers: http.Header(map[string][]string{
					"Content-Length": {fmt.Sprint(len(tagsList))},
					"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
				}),
			},
		})
	}
	e, c := testServer(m)
	defer c()

	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)

	ctx := context.Background()
	tagService := r.Tags(ctx)

	tags, err := tagService.All(ctx)
	require.NoError(t, err)
	require.Len(t, tags, 3)

	expected := map[string]struct{}{
		"tag1":   {},
		"tag2":   {},
		"funtag": {},
	}
	for _, t := range tags {
		delete(expected, t)
	}
	require.Empty(t, expected)
	// TODO(dmcgowan): Check for error cases
}

func TestTagDelete(t *testing.T) {
	tag := "latest"
	repo, _ := reference.WithName("test.example.com/repo/delete")
	newRandomSchemaV1Manifest(t, repo, tag, 1)
	var m testutil.RequestResponseMap
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodDelete,
			Route:  "/v2/" + repo.Name() + "/manifests/" + tag,
		},
		Response: testutil.Response{
			StatusCode: http.StatusAccepted,
			Headers: map[string][]string{
				"Content-Length": {"0"},
			},
		},
	})

	e, c := testServer(m)
	defer c()

	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)
	ctx := context.Background()
	ts := r.Tags(ctx)

	err = ts.Untag(ctx, tag)
	require.NoError(t, err)

	err = ts.Untag(ctx, tag)
	require.Error(t, err, "expected error deleting unknown tag")
}

func TestObtainsErrorForMissingTag(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo")

	var m testutil.RequestResponseMap
	var errors errcode.Errors
	errors = append(errors, v2.ErrorCodeManifestUnknown.WithDetail("unknown manifest"))
	errBytes, err := json.Marshal(errors)
	require.NoError(t, err)
	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodGet,
			Route:  "/v2/" + repo.Name() + "/manifests/1.0.0",
		},
		Response: testutil.Response{
			StatusCode: http.StatusNotFound,
			Body:       errBytes,
			Headers: http.Header(map[string][]string{
				"Content-Type": {"application/json"},
			}),
		},
	})
	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)

	tagService := r.Tags(ctx)

	_, err = tagService.Get(ctx, "1.0.0")
	require.Error(t, err)
	require.ErrorContains(t, err, "manifest unknown")
}

func TestObtainsManifestForTagWithoutHeaders(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo")

	var m testutil.RequestResponseMap
	m1, dgst, _ := newRandomSchemaV1Manifest(t, repo, "latest", 6)
	_, pl, err := m1.Payload()
	require.NoError(t, err)
	addTestManifestWithoutDigestHeader(repo, "1.0.0", schema1.MediaTypeSignedManifest, pl, &m)

	e, c := testServer(m)
	defer c()

	ctx := context.Background()
	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)

	tagService := r.Tags(ctx)

	desc, err := tagService.Get(ctx, "1.0.0")
	require.NoError(t, err)
	require.Equal(t, desc.Digest, dgst)
}

func TestManifestTagsPaginated(t *testing.T) {
	s := httptest.NewServer(http.NotFoundHandler())
	defer s.Close()

	repo, _ := reference.WithName("test.example.com/repo/tags/list")
	tagsList := []string{"tag1", "tag2", "funtag"}
	var m testutil.RequestResponseMap
	for i := 0; i < 3; i++ {
		body, err := json.Marshal(map[string]any{
			"name": "test.example.com/repo/tags/list",
			"tags": []string{tagsList[i]},
		})
		require.NoError(t, err)
		queryParams := make(map[string][]string)
		if i > 0 {
			queryParams["n"] = []string{"1"}
			queryParams["last"] = []string{tagsList[i-1]}
		}

		// Test both relative and absolute links.
		relativeLink := "/v2/" + repo.Name() + "/tags/list?n=1&last=" + tagsList[i]
		var link string
		switch i {
		case 0:
			link = relativeLink
		case len(tagsList) - 1:
			link = ""
		default:
			link = s.URL + relativeLink
		}

		headers := http.Header(map[string][]string{
			"Content-Length": {fmt.Sprint(len(body))},
			"Last-Modified":  {time.Now().Add(-1 * time.Second).Format(time.ANSIC)},
		})
		if link != "" {
			headers.Set("Link", fmt.Sprintf(`<%s>; rel="next"`, link))
		}

		m = append(m, testutil.RequestResponseMapping{
			Request: testutil.Request{
				Method:      http.MethodGet,
				Route:       "/v2/" + repo.Name() + "/tags/list",
				QueryParams: queryParams,
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       body,
				Headers:    headers,
			},
		})
	}

	s.Config.Handler = testutil.NewHandler(m)

	r, err := NewRepository(repo, s.URL, nil)
	require.NoError(t, err)

	ctx := context.Background()
	tagService := r.Tags(ctx)

	tags, err := tagService.All(ctx)
	require.NoError(t, err)
	require.Len(t, tags, 3)

	expected := map[string]struct{}{
		"tag1":   {},
		"tag2":   {},
		"funtag": {},
	}
	for _, t := range tags {
		delete(expected, t)
	}
	require.Empty(t, expected)
}

func TestManifestUnauthorized(t *testing.T) {
	repo, _ := reference.WithName("test.example.com/repo")
	_, dgst, _ := newRandomSchemaV1Manifest(t, repo, "latest", 6)
	var m testutil.RequestResponseMap

	m = append(m, testutil.RequestResponseMapping{
		Request: testutil.Request{
			Method: http.MethodGet,
			Route:  "/v2/" + repo.Name() + "/manifests/" + dgst.String(),
		},
		Response: testutil.Response{
			StatusCode: http.StatusUnauthorized,
			Body:       []byte("<html>garbage</html>"),
		},
	})

	e, c := testServer(m)
	defer c()

	r, err := NewRepository(repo, e, nil)
	require.NoError(t, err)
	ctx := context.Background()
	ms, err := r.Manifests(ctx)
	require.NoError(t, err)

	_, err = ms.Get(ctx, dgst)
	require.Error(t, err)
	var v2Err errcode.Error
	require.ErrorAs(t, err, &v2Err)
	require.Equal(t, v2Err.Code, errcode.ErrorCodeUnauthorized)
	require.Equal(t, v2Err.Message, errcode.ErrorCodeUnauthorized.Message())
}

func TestCatalog(t *testing.T) {
	var m testutil.RequestResponseMap
	addTestCatalog(
		"/v2/_catalog?n=5",
		[]byte("{\"repositories\":[\"foo\", \"bar\", \"baz\"]}"), "", &m)

	e, c := testServer(m)
	defer c()

	entries := make([]string, 5)

	r, err := NewRegistry(e, nil)
	require.NoError(t, err)

	ctx := context.Background()
	numFilled, err := r.Repositories(ctx, entries, "")

	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 3, numFilled)
}

func TestCatalogInParts(t *testing.T) {
	var m testutil.RequestResponseMap
	addTestCatalog(
		"/v2/_catalog?n=2",
		[]byte("{\"repositories\":[\"bar\", \"baz\"]}"),
		"</v2/_catalog?last=baz&n=2>", &m)
	addTestCatalog(
		"/v2/_catalog?last=baz&n=2",
		[]byte("{\"repositories\":[\"foo\"]}"),
		"", &m)

	e, c := testServer(m)
	defer c()

	entries := make([]string, 2)

	r, err := NewRegistry(e, nil)
	require.NoError(t, err)

	ctx := context.Background()
	numFilled, err := r.Repositories(ctx, entries, "")
	require.NoError(t, err)

	require.Equal(t, 2, numFilled, "Got wrong number of repos")

	numFilled, err = r.Repositories(ctx, entries, "baz")
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, numFilled, "Got wrong number of repos")
}

func TestSanitizeLocation(t *testing.T) {
	for _, testcase := range []struct {
		description string
		location    string
		source      string
		expected    string
		err         error
	}{
		{
			description: "ensure relative location correctly resolved",
			location:    "/v2/foo/baasdf",
			source:      "http://blahalaja.com/v1",
			expected:    "http://blahalaja.com/v2/foo/baasdf",
		},
		{
			description: "ensure parameters are preserved",
			location:    "/v2/foo/baasdf?_state=asdfasfdasdfasdf&digest=foo",
			source:      "http://blahalaja.com/v1",
			expected:    "http://blahalaja.com/v2/foo/baasdf?_state=asdfasfdasdfasdf&digest=foo",
		},
		{
			description: "ensure new hostname overridden",
			location:    "https://mwhahaha.com/v2/foo/baasdf?_state=asdfasfdasdfasdf",
			source:      "http://blahalaja.com/v1",
			expected:    "https://mwhahaha.com/v2/foo/baasdf?_state=asdfasfdasdfasdf",
		},
	} {
		s, err := sanitizeLocation(testcase.location, testcase.source)
		if testcase.err != nil {
			// nolint: testifylint // require-error
			if !assert.ErrorIs(t, err, testcase.err) {
				continue
			}
		}
		// nolint: testifylint // require-error
		if !assert.NoError(t, err) {
			continue
		}

		assert.Equal(t, testcase.expected, s, "bad sanitize")
	}
}
