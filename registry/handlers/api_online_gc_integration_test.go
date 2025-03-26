//go:build integration && online_gc_test

package handlers_test

import (
	"bytes"
	"math/rand/v2"
	"net/http"
	"testing"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/reference"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

// This file is intended to test the HTTP API tolerance and behavior under scenarios that are prone to race conditions
// due to online GC.

// maxReviewAfterJitter is the maximum jitter in seconds that the online GC triggers will use to set a task's review
// due date (`review_after` column) whenever they are created or updated. The maximum jitter used by the database triggers
// when scheduling GC reviews in all GC review-table's `review_after` column is set to < 61 seconds in this migration:
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/registry/datastore/migrations/20220729143447_update_gc_review_after_function.go
const maxReviewAfterJitter = 61 * time.Second

func findAndLockGCManifestTask(t *testing.T, env *testEnv, repoName reference.Named, dgst digest.Digest) (*models.GCManifestTask, datastore.Transactor) {
	tx, err := env.db.Primary().BeginTx(env.ctx, nil)
	require.NoError(t, err)

	rStore := datastore.NewRepositoryStore(tx)
	r, err := rStore.FindByPath(env.ctx, repoName.Name())
	require.NoError(t, err)
	require.NotNil(t, r)

	m, err := rStore.FindManifestByDigest(env.ctx, r, dgst)
	require.NoError(t, err)
	require.NotNil(t, m)

	mts := datastore.NewGCManifestTaskStore(tx)
	mt, err := mts.FindAndLockBefore(env.ctx, r.NamespaceID, r.ID, m.ID, time.Now().Add(maxReviewAfterJitter))
	require.NoError(t, err)
	require.NotNil(t, mt)

	return mt, tx
}

func withoutOnlineGCReviewDelay(config *configuration.Configuration) {
	config.GC.ReviewAfter = -1
}

// TestTagsAPI_Delete_OnlineGC_BlocksAndResumesAfterGCReview tests that when we try to delete a tag that points to a
// manifest that is being reviewed by the online GC, the API is not able to delete the tag until GC completes.
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/online-garbage-collection.md#deleting-the-last-referencing-tag
func TestTagsAPI_Delete_OnlineGC_BlocksAndResumesAfterGCReview(t *testing.T) {
	env := newTestEnv(t, withDelete, withoutOnlineGCReviewDelay)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// create test repo with a single manifest and tag
	repoName, err := reference.WithName("test")
	require.NoError(t, err)

	tagName := "1.0.0"
	dgst := createRepository(t, env, repoName.Name(), tagName)

	// simulate GC process by locking the manifest review record
	mt, tx := findAndLockGCManifestTask(t, env, repoName, dgst)
	defer tx.Rollback()

	// simulate GC manifest review happening in the background while we make the API request
	lockDuration := 2 * time.Second
	time.AfterFunc(lockDuration, func() {
		// the manifest is not dangling, so we delete the GC task and commit transaction, as the GC would do
		mts := datastore.NewGCManifestTaskStore(tx)
		require.NoError(t, mts.Delete(env.ctx, mt))
		require.NoError(t, tx.Rollback())
	})

	// attempt to delete tag through the API, this should succeed after waiting for lockDuration
	ref, err := reference.WithTag(repoName, tagName)
	require.NoError(t, err)
	manifestURL, err := env.builder.BuildManifestURL(ref)
	require.NoError(t, err)

	start := time.Now()
	resp, err := httpDelete(manifestURL)
	end := time.Now()
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	require.WithinDuration(t, start, end, lockDuration+500*time.Millisecond)
}

// TestTagsAPI_Delete_OnlineGC_TimeoutOnProlongedReview tests that when we try to delete a tag that points to a
// manifest that is being reviewed by the online GC, and for some reason the review does not end within
// tagDeleteGCLockTimeout, the API request is aborted and a 503 Service Unavailable response is returned to clients.
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/online-garbage-collection.md#deleting-the-last-referencing-tag
func TestTagsAPI_Delete_OnlineGC_TimeoutOnProlongedReview(t *testing.T) {
	env := newTestEnv(t, withDelete, withoutOnlineGCReviewDelay)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// create test repo and tag
	repoName, err := reference.WithName("test")
	require.NoError(t, err)

	tagName := "1.0.0"
	dgst := createRepository(t, env, repoName.Name(), tagName)

	// simulate GC process by locking the manifest review record indefinitely
	_, tx := findAndLockGCManifestTask(t, env, repoName, dgst)
	defer tx.Rollback()

	// attempt to delete tag through the API, this should fail after waiting for tagDeleteGCLockTimeout (5 seconds)
	ref, err := reference.WithTag(repoName, tagName)
	require.NoError(t, err)

	manifestURL, err := env.builder.BuildManifestURL(ref)
	require.NoError(t, err)

	start := time.Now()
	resp, err := httpDelete(manifestURL)
	end := time.Now()
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.WithinDuration(t, start, end, 5*time.Second+100*time.Millisecond)
}

// TestManifestsAPI_DeleteList_OnlineGC_BlocksAndResumesAfterGCReview tests that when we try to delete a manifest list
// that points to a manifest that is being reviewed by the online GC, the API is not able to delete until GC completes.
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/online-garbage-collection.md#deleting-the-last-referencing-manifest-list
func TestManifestsAPI_DeleteList_OnlineGC_BlocksAndResumesAfterGCReview(t *testing.T) {
	env := newTestEnv(t, withDelete, withoutOnlineGCReviewDelay)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// create test repo with a single manifest list and two referenced manifests
	repoName, err := reference.WithName("test")
	require.NoError(t, err)
	ml := seedRandomOCIImageIndex(t, env, repoName.String(), putByTag("1.0.0"))

	// simulate GC process by locking the review record of one of the manifests referenced in the list
	refs := ml.References()
	ref := refs[rand.IntN(len(refs))]
	mt, tx := findAndLockGCManifestTask(t, env, repoName, ref.Digest)
	defer tx.Rollback()

	// simulate GC manifest review happening in the background while we make the API request
	lockDuration := 2 * time.Second
	time.AfterFunc(lockDuration, func() {
		// the manifest is not dangling, so we delete the GC tasks and commit transaction, as the GC would do
		mts := datastore.NewGCManifestTaskStore(tx)
		require.NoError(t, mts.Delete(env.ctx, mt))
		require.NoError(t, tx.Commit())
	})

	// attempt to delete manifest list through the API, this should succeed after waiting for lockDuration
	u := buildManifestDigestURL(t, env, repoName.String(), ml)
	start := time.Now()
	resp, err := httpDelete(u)
	end := time.Now()
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	require.WithinDuration(t, start, end, lockDuration+600*time.Millisecond)
}

// TestManifestsAPI_DeleteList_OnlineGC_BlocksAndResumesAfterGCReview tests that when we try to delete a manifest list
// that points to a manifest that is being reviewed by the online GC, and for some reason the review does not end within
// manifestDeleteGCLockTimeout, the API request is aborted and a 503 Service Unavailable response is returned.
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/online-garbage-collection.md#deleting-the-last-referencing-manifest-list
func TestManifestsAPI_DeleteList_OnlineGC_TimeoutOnProlongedReview(t *testing.T) {
	env := newTestEnv(t, withDelete, withoutOnlineGCReviewDelay)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// create test repo with a single manifest list and two referenced manifests
	repoName, err := reference.WithName("test")
	require.NoError(t, err)
	ml := seedRandomOCIImageIndex(t, env, repoName.String(), putByTag("1.0.0"))

	// simulate GC process by locking the review record of one of the manifests referenced in the list
	refs := ml.References()
	ref := refs[rand.IntN(len(refs))]
	_, tx := findAndLockGCManifestTask(t, env, repoName, ref.Digest)
	defer tx.Rollback()

	// attempt to delete list through the API, this should fail after waiting for manifestDeleteGCLockTimeout (5 seconds)
	u := buildManifestDigestURL(t, env, repoName.String(), ml)
	start := time.Now()
	resp, err := httpDelete(u)
	end := time.Now()
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.WithinDuration(t, start, end, 5*time.Second+100*time.Millisecond)
}

// TestManifestsAPI_Tag_OnlineGC_BlocksAndResumesAfterGCReview tests that when we try to tag a manifest that is being
// reviewed by the online GC, the API is not able to tag until GC completes.
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/online-garbage-collection.md#creating-a-tag-for-an-untagged-manifest
func TestManifestsAPI_Tag_OnlineGC_BlocksAndResumesAfterGCReview(t *testing.T) {
	env := newTestEnv(t, withDelete, withoutOnlineGCReviewDelay)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// create test repo and manifest with no tag
	repoName, err := reference.WithName("test")
	require.NoError(t, err)
	m := seedRandomSchema2Manifest(t, env, repoName.String(), putByDigest)
	_, payload, err := m.Payload()
	require.NoError(t, err)
	dgst := digest.FromBytes(payload)

	// simulate GC process by locking the manifest review record indefinitely
	mt, tx := findAndLockGCManifestTask(t, env, repoName, dgst)
	defer tx.Rollback()

	// simulate GC manifest review happening in the background while we make the API request
	lockDuration := 2 * time.Second
	time.AfterFunc(lockDuration, func() {
		// the manifest is not dangling, so we delete the GC tasks and commit transaction, as the GC would do
		mts := datastore.NewGCManifestTaskStore(tx)
		require.NoError(t, mts.Delete(env.ctx, mt))
		require.NoError(t, tx.Commit())
	})

	// attempt to tag manifest through the API, this should succeed after waiting for lockDuration
	u := buildManifestTagURL(t, env, repoName.String(), "latest")
	req, err := http.NewRequest(http.MethodPut, u, bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Content-Type", m.MediaType)

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	end := time.Now()
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.WithinDuration(t, start, end, lockDuration+500*time.Millisecond)
}

// TestManifestsAPI_Tag_OnlineGC_BlocksAndResumesAfterGCReview_DanglingManifest tests that when we try to tag a manifest
// that is being reviewed by the online GC, and it ends up being deleted because it was dangling, the API is not able to
// tag until GC completes. Once unblocked, the API should handle the "manifest not found" error gracefully and create
// and tag the manifest.
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/online-garbage-collection.md#creating-a-tag-for-an-untagged-manifest
func TestManifestsAPI_Tag_OnlineGC_BlocksAndResumesAfterGCReview_DanglingManifest(t *testing.T) {
	env := newTestEnv(t, withDelete, withoutOnlineGCReviewDelay)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// create test repo and manifest with no tag
	repoName, err := reference.WithName("test")
	require.NoError(t, err)
	m := seedRandomSchema2Manifest(t, env, repoName.String(), putByDigest)
	_, payload, err := m.Payload()
	require.NoError(t, err)
	dgst := digest.FromBytes(payload)

	// simulate GC process by locking the manifest review record indefinitely
	mt, tx := findAndLockGCManifestTask(t, env, repoName, dgst)
	defer tx.Rollback()

	// simulate GC manifest review happening in the background while we make the API request
	lockDuration := 2 * time.Second
	time.AfterFunc(lockDuration, func() {
		// the manifest is dangling, so we delete it and commit the transaction, as the GC would do
		ms := datastore.NewManifestStore(tx)
		dgst2, err := ms.Delete(env.ctx, mt.NamespaceID, mt.RepositoryID, mt.ManifestID)
		require.NoError(t, err)
		require.Equal(t, dgst, *dgst2)
		require.NoError(t, tx.Commit())
	})

	// attempt to tag manifest through the API, this should resume after lockDuration and recreate and tag the manifest
	u := buildManifestTagURL(t, env, repoName.String(), "latest")
	req, err := http.NewRequest(http.MethodPut, u, bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Content-Type", m.MediaType)

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	end := time.Now()
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.WithinDuration(t, start, end, lockDuration+500*time.Millisecond)
}

// TestManifestsAPI_Tag_OnlineGC_TimeoutOnProlongedReview tests that when we try to tag a manifest that is being
// reviewed by the online GC, and for some reason the review does not end within manifestTagGCLockTimeout, the API
// request is aborted and a 503 Service Unavailable response is returned.
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/online-garbage-collection.md#creating-a-tag-for-an-untagged-manifest
func TestManifestsAPI_Tag_OnlineGC_TimeoutOnProlongedReview(t *testing.T) {
	env := newTestEnv(t, withoutOnlineGCReviewDelay)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// create test repo and manifest with no tag
	repoName, err := reference.WithName("test")
	require.NoError(t, err)
	m := seedRandomSchema2Manifest(t, env, repoName.String(), putByDigest)
	_, payload, err := m.Payload()
	require.NoError(t, err)
	dgst := digest.FromBytes(payload)

	// simulate GC process by locking the manifest review record indefinitely
	_, tx := findAndLockGCManifestTask(t, env, repoName, dgst)
	defer tx.Rollback()

	// attempt to tag manifest through the API, this should fail after waiting for manifestTagGCLockTimeout (5 seconds)
	u := buildManifestTagURL(t, env, repoName.String(), "latest")
	req, err := http.NewRequest(http.MethodPut, u, bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Content-Type", m.MediaType)

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	end := time.Now()
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.WithinDuration(t, start, end, 5*time.Second+200*time.Millisecond)
}

// TestManifestsAPI_CreateList_OnlineGC_BlocksAndResumesAfterGCReview tests that when we try to create a manifest list
// that points to a manifest that is being reviewed by the online GC, the API is not able to proceed until GC completes.
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/online-garbage-collection.md#creating-a-manifest-list-referencing-an-unreferenced-manifest
func TestManifestsAPI_CreateList_OnlineGC_BlocksAndResumesAfterGCReview(t *testing.T) {
	env := newTestEnv(t, withDelete, withoutOnlineGCReviewDelay)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// create test repo and two manifests with no tags
	repoName, err := reference.WithName("test")
	require.NoError(t, err)

	m1 := seedRandomSchema2Manifest(t, env, repoName.String(), putByDigest)
	_, payload1, err := m1.Payload()
	require.NoError(t, err)
	dgst1 := digest.FromBytes(payload1)

	m2 := seedRandomSchema2Manifest(t, env, repoName.String(), putByDigest)
	_, payload2, err := m2.Payload()
	require.NoError(t, err)
	dgst2 := digest.FromBytes(payload2)

	// simulate GC process by locking the review record of one of the manifests referenced in the list
	dgsts := []digest.Digest{dgst1, dgst2}
	dgst := dgsts[rand.IntN(len(dgsts))]
	mt, tx := findAndLockGCManifestTask(t, env, repoName, dgst)
	defer tx.Rollback()

	// simulate GC manifest review happening in the background while we make the API request
	lockDuration := 2 * time.Second
	time.AfterFunc(lockDuration, func() {
		// the manifest is not dangling, so we delete the GC tasks and commit transaction, as the GC would do
		mts := datastore.NewGCManifestTaskStore(tx)
		require.NoError(t, mts.Delete(env.ctx, mt))
		require.NoError(t, tx.Commit())
	})

	// attempt to create manifest list through the API, this should succeed after waiting for lockDuration
	tmp := &manifestlist.ManifestList{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     manifestlist.MediaTypeManifestList,
		},
		Manifests: []manifestlist.ManifestDescriptor{
			{
				Descriptor: distribution.Descriptor{
					Digest:    dgst1,
					MediaType: m1.MediaType,
				},
				Platform: randomPlatformSpec(),
			},
			{
				Descriptor: distribution.Descriptor{
					Digest:    dgst2,
					MediaType: m2.MediaType,
				},
				Platform: randomPlatformSpec(),
			},
		},
	}

	ml, err := manifestlist.FromDescriptors(tmp.Manifests)
	require.NoError(t, err)

	u := buildManifestDigestURL(t, env, repoName.String(), ml)
	start := time.Now()
	resp, err := putManifest("", u, manifestlist.MediaTypeManifestList, ml)
	require.NoError(t, err)
	defer resp.Body.Close()
	end := time.Now()
	require.NoError(t, err)

	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.WithinDuration(t, start, end, lockDuration+200*time.Millisecond)
}

// TestManifestsAPI_CreateList_OnlineGC_TimeoutOnProlongedReview tests that when we try to create a manifest list
// that points to a manifest that is being reviewed by the online GC, and for some reason the review does not end within
// manifestListCreateGCLockTimeout, the API request is aborted and a 503 Service Unavailable response is returned.
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/online-garbage-collection.md#creating-a-manifest-list-referencing-an-unreferenced-manifest
func TestManifestsAPI_CreateList_OnlineGC_TimeoutOnProlongedReview(t *testing.T) {
	env := newTestEnv(t, withDelete, withoutOnlineGCReviewDelay)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// create test repo and two manifests with no tags
	repoName, err := reference.WithName("test")
	require.NoError(t, err)

	m1 := seedRandomSchema2Manifest(t, env, repoName.String(), putByDigest)
	_, payload1, err := m1.Payload()
	require.NoError(t, err)
	dgst1 := digest.FromBytes(payload1)

	m2 := seedRandomSchema2Manifest(t, env, repoName.String(), putByDigest)
	_, payload2, err := m2.Payload()
	require.NoError(t, err)
	dgst2 := digest.FromBytes(payload2)

	// simulate GC process by locking the review record of one of the manifests referenced in the list (indefinitely)
	dgsts := []digest.Digest{dgst1, dgst2}
	dgst := dgsts[rand.IntN(len(dgsts))]
	_, tx := findAndLockGCManifestTask(t, env, repoName, dgst)
	defer tx.Rollback()

	// attempt to create manifest list through the API, this should succeed after waiting for lockDuration
	tmp := &manifestlist.ManifestList{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     manifestlist.MediaTypeManifestList,
		},
		Manifests: []manifestlist.ManifestDescriptor{
			{
				Descriptor: distribution.Descriptor{
					Digest:    dgst1,
					MediaType: m1.MediaType,
				},
				Platform: randomPlatformSpec(),
			},
			{
				Descriptor: distribution.Descriptor{
					Digest:    dgst2,
					MediaType: m2.MediaType,
				},
				Platform: randomPlatformSpec(),
			},
		},
	}

	ml, err := manifestlist.FromDescriptors(tmp.Manifests)
	require.NoError(t, err)

	u := buildManifestDigestURL(t, env, repoName.String(), ml)
	start := time.Now()
	resp, err := putManifest("", u, manifestlist.MediaTypeManifestList, ml)
	require.NoError(t, err)
	defer resp.Body.Close()
	end := time.Now()
	require.NoError(t, err)

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.WithinDuration(t, start, end, 5*time.Second+200*time.Millisecond)
}

// TestManifestsAPI_CreateList_OnlineGC_BlocksAndResumesAfterGCReview_DanglingManifest tests that when we try to create
// a manifest list that references a manifest that is being reviewed by the online GC, and it ends up being deleted
// because it was dangling, the API is not able to proceed until GC completes. Once unblocked, the API should return a
// 400 Bad Request error, as one of the required manifests no longer exist.
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/online-garbage-collection.md#creating-a-manifest-list-referencing-an-unreferenced-manifest
func TestManifestsAPI_CreateList_OnlineGC_BlocksAndResumesAfterGCReview_DanglingManifest(t *testing.T) {
	env := newTestEnv(t, withDelete, withoutOnlineGCReviewDelay)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// create test repo and two manifests with no tags
	repoName, err := reference.WithName("test")
	require.NoError(t, err)

	m1 := seedRandomSchema2Manifest(t, env, repoName.String(), putByDigest)
	_, payload1, err := m1.Payload()
	require.NoError(t, err)
	dgst1 := digest.FromBytes(payload1)

	m2 := seedRandomSchema2Manifest(t, env, repoName.String(), putByDigest)
	_, payload2, err := m2.Payload()
	require.NoError(t, err)
	dgst2 := digest.FromBytes(payload2)

	// simulate GC process by locking the review record of one of the manifests referenced in the list
	dgsts := []digest.Digest{dgst1, dgst2}
	dgst := dgsts[rand.IntN(len(dgsts))]
	mt, tx := findAndLockGCManifestTask(t, env, repoName, dgst)
	defer tx.Rollback()

	// simulate GC manifest review happening in the background while we make the API request
	lockDuration := 2 * time.Second
	time.AfterFunc(lockDuration, func() {
		// the manifest is dangling, so we delete it and commit transaction, as the GC would do
		ms := datastore.NewManifestStore(tx)
		dgst2, err := ms.Delete(env.ctx, mt.NamespaceID, mt.RepositoryID, mt.ManifestID)
		require.NoError(t, err)
		require.Equal(t, dgst, *dgst2)
		require.NoError(t, tx.Commit())
	})

	// attempt to create manifest list through the API, this should fail after waiting for lockDuration
	tmp := &manifestlist.ManifestList{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     manifestlist.MediaTypeManifestList,
		},
		Manifests: []manifestlist.ManifestDescriptor{
			{
				Descriptor: distribution.Descriptor{
					Digest:    dgst1,
					MediaType: m1.MediaType,
				},
				Platform: randomPlatformSpec(),
			},
			{
				Descriptor: distribution.Descriptor{
					Digest:    dgst2,
					MediaType: m2.MediaType,
				},
				Platform: randomPlatformSpec(),
			},
		},
	}

	ml, err := manifestlist.FromDescriptors(tmp.Manifests)
	require.NoError(t, err)

	u := buildManifestDigestURL(t, env, repoName.String(), ml)
	start := time.Now()
	resp, err := putManifest("", u, manifestlist.MediaTypeManifestList, ml)
	require.NoError(t, err)
	defer resp.Body.Close()
	end := time.Now()
	require.NoError(t, err)

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	checkBodyHasErrorCodes(t, "", resp, v2.ErrorCodeManifestBlobUnknown)
	require.WithinDuration(t, start, end, lockDuration+200*time.Millisecond)
}
