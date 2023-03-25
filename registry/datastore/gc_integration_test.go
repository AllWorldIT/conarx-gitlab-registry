//go:build integration
// +build integration

package datastore_test

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func randomDigest(t testing.TB) digest.Digest {
	t.Helper()

	rand.Seed(time.Now().UnixNano())
	data := make([]byte, 100)
	_, err := rand.Read(data)
	require.NoError(t, err)

	return digest.FromBytes(data)
}

func randomBlob(t testing.TB) *models.Blob {
	t.Helper()

	rand.Seed(time.Now().UnixNano())
	return &models.Blob{
		MediaType: "application/octet-stream",
		Digest:    randomDigest(t),
		Size:      rand.Int63(),
	}
}

func randomRepository(t testing.TB) *models.Repository {
	t.Helper()

	rand.Seed(time.Now().UnixNano())
	n := strconv.Itoa(rand.Int())
	return &models.Repository{
		Name: n,
		Path: n,
	}
}

func randomManifest(t testing.TB, r *models.Repository, configBlob *models.Blob) *models.Manifest {
	t.Helper()

	m := &models.Manifest{
		NamespaceID:   r.NamespaceID,
		RepositoryID:  r.ID,
		SchemaVersion: 2,
		MediaType:     schema2.MediaTypeManifest,
		Digest:        randomDigest(t),
		Payload:       models.Payload(`{"foo": "bar"}`),
	}
	if configBlob != nil {
		m.Configuration = &models.Configuration{
			MediaType: schema2.MediaTypeImageConfig,
			Digest:    configBlob.Digest,
			Payload:   models.Payload(`{"foo": "bar"}`),
		}
	}

	return m
}

const (
	// defaultReviewAfterDelay is the default delay applied by online GC triggers to review tasks.
	defaultReviewAfterDelay = 24 * time.Hour

	// minReviewAfterJitter is the minimum jitter in seconds that the online GC triggers will use to set a task's review
	// due date (`review_after` column) whenever they are created or updated. The minimum jitter used by the database triggers
	// when scheduling GC reviews in all GC review-table's `review_after` column is set to 5 seconds in this migration:
	// https://gitlab.com/gitlab-org/container-registry/-/blob/master/registry/datastore/migrations/20220729143447_update_gc_review_after_function.go
	minReviewAfterJitter = 5 * time.Second

	// maxReviewAfterJitter is the maximum jitter in seconds that the online GC triggers will use to set a task's review
	// due date (`review_after` column) whenever they are created or updated. The maximum jitter used by the database triggers
	// when scheduling GC reviews in all GC review-table's `review_after` column is set to < 61 seconds in this migration:
	// https://gitlab.com/gitlab-org/container-registry/-/blob/master/registry/datastore/migrations/20220729143447_update_gc_review_after_function.go
	maxReviewAfterJitter = 61 * time.Second

	// defaultReviewAfterWithMinJitterDelay is the default delay plus the minimum jitter in seconds applied by online GC triggers
	// to review tasks plus the minimum jitter in seconds that the online GC triggers will use to set a task's review.
	defaultReviewAfterWithMinJitterDelay = defaultReviewAfterDelay + minReviewAfterJitter

	// defaultReviewAfterWithMaxJitterDelay is the default delay plus the minimum jitter in seconds applied by online GC triggers
	// to review tasks plus the maximum jitter in seconds that the online GC triggers will use to set a task's review.
	defaultReviewAfterWithMaxJitterDelay = defaultReviewAfterDelay + maxReviewAfterJitter
)

func TestGC_TrackBlobUploads(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// create blob
	bs := datastore.NewBlobStore(suite.db)
	b := randomBlob(t)
	err := bs.Create(suite.ctx, b)
	require.NoError(t, err)

	// Check that a corresponding task was created and scheduled for defaultReviewAfterDelay plus
	// [minReviewAfterJitter, maxReviewAfterJitter] ahead. This is done by the `gc_track_blob_uploads` trigger/function.
	brs := datastore.NewGCBlobTaskStore(suite.db)
	rr, err := brs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr))
	require.Equal(t, 0, rr[0].ReviewCount)
	require.Equal(t, b.Digest, rr[0].Digest)
	require.Equal(t, "blob_upload", rr[0].Event)
	assertGCReviewDelayInMinMaxRange(t, rr[0].ReviewAfter, b.CreatedAt)
}

func TestGC_TrackBlobUploads_PostponeReviewOnConflict(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// create blob
	bs := datastore.NewBlobStore(suite.db)
	b := randomBlob(t)
	err := bs.Create(suite.ctx, b)
	require.NoError(t, err)

	// delete it
	err = bs.Delete(suite.ctx, b.Digest)
	require.NoError(t, err)

	// grab existing review record (should be preserved, despite the blob deletion)
	brs := datastore.NewGCBlobTaskStore(suite.db)
	rr, err := brs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr))

	// re-create blob
	err = bs.Create(suite.ctx, b)
	require.NoError(t, err)

	// Check that we still have only one review record but its due date was postponed to now (re-create time) plus
	// defaultReviewAfterDelay and [minReviewAfterJitter, maxReviewAfterJitter]. This is done by the
	// `gc_track_blob_uploads` trigger/function.
	rr2, err := brs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr2))
	require.Equal(t, rr[0].ReviewCount, rr2[0].ReviewCount)
	require.Equal(t, rr[0].Digest, rr2[0].Digest)
	require.Equal(t, "blob_upload", rr[0].Event)
	// We cannot control the random jitter applied when the blob was first created and then recreated. This means that
	// for this particular test case, the "review after" after the recreation might be smaller than the "review after"
	// after the original create. So we cannot say that the latter must always be later than the former. The best we can
	// do is to assert that the "review after" has changed and that it's at least defaultReviewAfterDelay plus
	// [minReviewAfterJitter, maxReviewAfterJitter] ahead of the blob creation time.
	require.NotEqual(t, rr2[0].ReviewAfter, rr[0].ReviewAfter)
	assertGCReviewDelayInMinMaxRange(t, rr[0].ReviewAfter, b.CreatedAt)
}

func TestGC_TrackBlobUploads_DoesNothingIfTriggerDisabled(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	enable, err := testutil.GCTrackBlobUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create blob
	bs := datastore.NewBlobStore(suite.db)
	b := randomBlob(t)
	err = bs.Create(suite.ctx, b)
	require.NoError(t, err)

	// check that no review records were created
	brs := datastore.NewGCBlobTaskStore(suite.db)
	count, err := brs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestGC_TrackConfigurationBlobs(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err := rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create config blob
	bs := datastore.NewBlobStore(suite.db)
	b := randomBlob(t)
	err = bs.Create(suite.ctx, b)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, b)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// Check that a corresponding task was created and scheduled for 1 day ahead. This is done by the
	// `gc_track_configuration_blobs` trigger/function
	brs := datastore.NewGCConfigLinkStore(suite.db)
	rr, err := brs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr))
	require.NotEmpty(t, rr[0].ID)
	require.Equal(t, r.ID, rr[0].RepositoryID)
	require.Equal(t, m.ID, rr[0].ManifestID)
	require.Equal(t, b.Digest, rr[0].Digest)
}

func TestGC_TrackConfigurationBlobs_DoesNothingIfTriggerDisabled(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	enable, err := testutil.GCTrackConfigurationBlobsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create config blob
	bs := datastore.NewBlobStore(suite.db)
	b := randomBlob(t)
	err = bs.Create(suite.ctx, b)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, b)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// check that no records were created
	brs := datastore.NewGCConfigLinkStore(suite.db)
	count, err := brs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestGC_TrackLayerBlobs(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err := rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create layer blob
	bs := datastore.NewBlobStore(suite.db)
	b := randomBlob(t)
	err = bs.Create(suite.ctx, b)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// associate layer with manifest
	err = ms.AssociateLayerBlob(suite.ctx, m, b)
	require.NoError(t, err)

	// Check that a corresponding row was created. This is done by the `gc_track_layer_blobs` trigger/function
	brs := datastore.NewGCLayerLinkStore(suite.db)
	ll, err := brs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(ll))
	require.NotEmpty(t, ll[0].ID)
	require.Equal(t, r.ID, ll[0].RepositoryID)
	require.Equal(t, int64(1), ll[0].LayerID)
	require.Equal(t, b.Digest, ll[0].Digest)
}

func TestGC_TrackLayerBlobs_DoesNothingIfTriggerDisabled(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	enable, err := testutil.GCTrackLayerBlobsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create layer blob
	bs := datastore.NewBlobStore(suite.db)
	b := randomBlob(t)
	err = bs.Create(suite.ctx, b)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// associate layer with manifest
	err = ms.AssociateLayerBlob(suite.ctx, m, b)
	require.NoError(t, err)

	// check that no records were created
	brs := datastore.NewGCConfigLinkStore(suite.db)
	count, err := brs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestGC_TrackManifestUploads(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// create repository
	rs := datastore.NewRepositoryStore(suite.db)
	r := randomRepository(t)
	r, err := rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// Check that a corresponding task was created and scheduled for defaultReviewAfterDelay plus
	// [minReviewAfterJitter, maxReviewAfterJitter] ahead. This is done by the `gc_track_manifest_uploads`
	// trigger/function.
	brs := datastore.NewGCManifestTaskStore(suite.db)
	tt, err := brs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(tt))
	require.Equal(t, r.NamespaceID, tt[0].NamespaceID)
	require.Equal(t, r.ID, tt[0].RepositoryID)
	require.Equal(t, m.ID, tt[0].ManifestID)
	require.Equal(t, 0, tt[0].ReviewCount)
	require.Equal(t, m.CreatedAt, tt[0].CreatedAt)
	require.Equal(t, "manifest_upload", tt[0].Event)
	assertGCReviewDelayInMinMaxRange(t, tt[0].ReviewAfter, m.CreatedAt)
}

func TestGC_TrackManifestUploads_DoesNothingIfTriggerDisabled(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	enable, err := testutil.GCTrackManifestUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repository
	rs := datastore.NewRepositoryStore(suite.db)
	r := randomRepository(t)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// check that no review records were created
	mrs := datastore.NewGCManifestTaskStore(suite.db)
	count, err := mrs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestGC_TrackDeletedManifests(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// disable other triggers that also insert on gc_blob_review_queue so that they don't interfere with this test
	enable, err := testutil.GCTrackConfigurationBlobsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()
	enable, err = testutil.GCTrackBlobUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create config blob
	bs := datastore.NewBlobStore(suite.db)
	b := randomBlob(t)
	err = bs.Create(suite.ctx, b)
	require.NoError(t, err)
	err = rs.LinkBlob(suite.ctx, r, b.Digest)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, b)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// confirm that the review queue remains empty
	brs := datastore.NewGCBlobTaskStore(suite.db)
	count, err := brs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)

	// delete manifest
	deletedAt := time.Now()
	ok, err := rs.DeleteManifest(suite.ctx, r, m.Digest)
	require.NoError(t, err)
	require.True(t, ok)

	// Check that a corresponding task was created for the config blob and scheduled for defaultReviewAfterDelay plus
	// [minReviewAfterJitter, maxReviewAfterJitter] ahead. This is done by the `gc_track_deleted_manifests`
	// trigger/function.
	tt, err := brs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(tt))
	require.Equal(t, 0, tt[0].ReviewCount)
	require.Equal(t, b.Digest, tt[0].Digest)
	require.Equal(t, "manifest_delete", tt[0].Event)
	assertGCReviewDelayInMinMaxRange(t, tt[0].ReviewAfter, deletedAt)
	// ignore the few milliseconds between deleting the manifest and queueing a task in response to it
	require.Less(t, tt[0].CreatedAt, deletedAt.Add(200*time.Millisecond))
}

func TestGC_TrackDeletedManifests_PostponeReviewOnConflict(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err := rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create config blob
	bs := datastore.NewBlobStore(suite.db)
	b := randomBlob(t)
	err = bs.Create(suite.ctx, b)
	require.NoError(t, err)
	err = rs.LinkBlob(suite.ctx, r, b.Digest)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, b)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// grab existing review record (created by the gc_track_blob_uploads_trigger trigger)
	brs := datastore.NewGCBlobTaskStore(suite.db)
	rr, err := brs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr))

	// delete manifest
	deletedAt := time.Now()
	ok, err := rs.DeleteManifest(suite.ctx, r, m.Digest)
	require.NoError(t, err)
	require.True(t, ok)

	// Check that we still have only one review record but its due date was postponed to now (delete time) plus
	// defaultReviewAfterDelay and [minReviewAfterJitter, maxReviewAfterJitter]. This is done by the
	// `gc_track_deleted_manifests` trigger/function.
	rr2, err := brs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr2))
	require.Equal(t, rr[0].ReviewCount, rr2[0].ReviewCount)
	require.Equal(t, rr[0].Digest, rr2[0].Digest)
	// We cannot control the random jitter applied when the config blob was first created and then when the manifest was
	// deleted (causing the config blob task to be pushed forward). This means that for this particular test case, the
	// "review after" after the manifest delete might be smaller than the "review after" after the blob creation. So we
	// cannot say that the latter must always be later than the former. The best we can do is to assert that the "review
	// after" has changed and that it's at least defaultReviewAfterDelay plus
	// [minReviewAfterJitter, maxReviewAfterJitter] ahead of the manifest list delete time.
	require.NotEqual(t, rr2[0].ReviewAfter, rr[0].ReviewAfter)
	assertGCReviewDelayInMinMaxRange(t, rr2[0].ReviewAfter, deletedAt)
}

func TestGC_TrackDeletedManifests_DoesNothingIfTriggerDisabled(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	enable, err := testutil.GCTrackDeletedManifestsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()
	// disable other triggers that also insert on gc_blob_review_queue so that they don't interfere with this test
	enable, err = testutil.GCTrackConfigurationBlobsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()
	enable, err = testutil.GCTrackBlobUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create config blob
	bs := datastore.NewBlobStore(suite.db)
	b := randomBlob(t)
	err = bs.Create(suite.ctx, b)
	require.NoError(t, err)
	err = rs.LinkBlob(suite.ctx, r, b.Digest)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, b)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// delete manifest
	ok, err := rs.DeleteManifest(suite.ctx, r, m.Digest)
	require.NoError(t, err)
	require.True(t, ok)

	// check that no review records were created
	brs := datastore.NewGCBlobTaskStore(suite.db)
	count, err := brs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestGC_TrackDeletedLayers(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// disable other triggers that also insert on gc_blob_review_queue so that they don't interfere with this test
	enable, err := testutil.GCTrackBlobUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create layer blob
	bs := datastore.NewBlobStore(suite.db)
	b := randomBlob(t)
	err = bs.Create(suite.ctx, b)
	require.NoError(t, err)
	err = rs.LinkBlob(suite.ctx, r, b.Digest)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// associate layer with manifest
	err = ms.AssociateLayerBlob(suite.ctx, m, b)
	require.NoError(t, err)

	// confirm that the review queue remains empty
	brs := datastore.NewGCBlobTaskStore(suite.db)
	count, err := brs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)

	// dissociate layer blob
	dissociatedAt := time.Now()
	err = ms.DissociateLayerBlob(suite.ctx, m, b)
	require.NoError(t, err)

	// check that a corresponding task was created for the layer blob and scheduled for defaultReviewAfterDelay plus
	// [minReviewAfterJitter, maxReviewAfterJitter] ahead. This is done by the `gc_track_deleted_layers`
	// trigger/function.
	tt, err := brs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(tt))
	require.Equal(t, 0, tt[0].ReviewCount)
	require.Equal(t, b.Digest, tt[0].Digest)
	require.Equal(t, "layer_delete", tt[0].Event)
	require.Less(t, tt[0].ReviewAfter, dissociatedAt.Add(defaultReviewAfterWithMaxJitterDelay))
	// ignore the few milliseconds between dissociating the layer and queueing task in response to it
	require.Less(t, tt[0].CreatedAt, dissociatedAt.Add(200*time.Millisecond))
}

func TestGC_TrackDeletedLayers_PostponeReviewOnConflict(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err := rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create layer blob
	bs := datastore.NewBlobStore(suite.db)
	b := randomBlob(t)
	err = bs.Create(suite.ctx, b)
	require.NoError(t, err)
	err = rs.LinkBlob(suite.ctx, r, b.Digest)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// associate layer with manifest
	err = ms.AssociateLayerBlob(suite.ctx, m, b)
	require.NoError(t, err)

	// grab existing review record (created by the gc_track_blob_uploads_trigger trigger)
	brs := datastore.NewGCBlobTaskStore(suite.db)
	rr, err := brs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr))

	// dissociate layer blob
	err = ms.DissociateLayerBlob(suite.ctx, m, b)
	require.NoError(t, err)

	// Check that we still have only one review record but its due date was postponed to now (delete time) plus
	// defaultReviewAfterDelay and [minReviewAfterJitter, maxReviewAfterJitter]. This is done by the
	// `gc_track_deleted_layers` trigger/function.
	rr2, err := brs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr2))
	require.Equal(t, rr[0].ReviewCount, rr2[0].ReviewCount)
	require.Equal(t, rr[0].Digest, rr2[0].Digest)
	// We cannot control the random jitter applied when the layer was first associated and then dissociated. This means
	// that for this particular test case, the "review after" after the dissociation might be smaller than the "review
	// after" after the association. So we cannot say that the latter must always be later than the former. The best we
	// can do is to assert that the "review after" has changed and that it's at least defaultReviewAfterDelay plus
	// [minReviewAfterJitter, maxReviewAfterJitter] ahead of the manifest creation time.
	require.NotEqual(t, rr2[0].ReviewAfter, rr[0].ReviewAfter)
	assertGCReviewDelayInMinMaxRange(t, rr2[0].ReviewAfter, m.CreatedAt)
}

func TestGC_TrackDeletedLayers_DoesNothingIfTriggerDisabled(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	enable, err := testutil.GCTrackDeletedLayersTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()
	// disable other triggers that also insert on gc_blob_review_queue so that they don't interfere with this test
	enable, err = testutil.GCTrackBlobUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create layer blob
	bs := datastore.NewBlobStore(suite.db)
	b := randomBlob(t)
	err = bs.Create(suite.ctx, b)
	require.NoError(t, err)
	err = rs.LinkBlob(suite.ctx, r, b.Digest)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// associate layer with manifest
	err = ms.AssociateLayerBlob(suite.ctx, m, b)
	require.NoError(t, err)

	// dissociate layer blob
	err = ms.DissociateLayerBlob(suite.ctx, m, b)
	require.NoError(t, err)

	// check that no review records were created
	brs := datastore.NewGCBlobTaskStore(suite.db)
	count, err := brs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestGC_TrackDeletedManifestLists(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// disable other triggers that also insert on gc_manifest_review_queue so that they don't interfere with this test
	enable, err := testutil.GCTrackManifestUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// create manifest list
	ml := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, ml)
	require.NoError(t, err)
	err = ms.AssociateManifest(suite.ctx, ml, m)
	require.NoError(t, err)

	// confirm that the review queue remains empty
	mrs := datastore.NewGCManifestTaskStore(suite.db)
	count, err := mrs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)

	// delete manifest list
	deletedAt := time.Now()
	ok, err := rs.DeleteManifest(suite.ctx, r, ml.Digest)
	require.NoError(t, err)
	require.True(t, ok)

	// Check that a corresponding task was created and scheduled for defaultReviewAfterDelay plus
	// [minReviewAfterJitter, maxReviewAfterJitter] ahead. This is done by the `gc_track_deleted_manifest_lists`
	// trigger/function.
	rr, err := mrs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr))
	require.Equal(t, r.ID, rr[0].RepositoryID)
	require.Equal(t, m.ID, rr[0].ManifestID)
	require.Equal(t, 0, rr[0].ReviewCount)
	require.Equal(t, "manifest_list_delete", rr[0].Event)
	assertGCReviewDelayInMinMaxRange(t, rr[0].ReviewAfter, deletedAt)
	// ignore the few milliseconds between deleting the manifest list and queueing task in response to it
	require.Less(t, rr[0].CreatedAt, deletedAt.Add(200*time.Millisecond))
}

func TestGC_TrackDeletedManifestLists_PostponeReviewOnConflict(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err := rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// create manifest list
	ml := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, ml)
	require.NoError(t, err)
	err = ms.AssociateManifest(suite.ctx, ml, m)
	require.NoError(t, err)

	// Grab existing review records, one for the manifest and another for the manifest list (created by the
	// gc_track_manifest_uploads trigger)
	mrs := datastore.NewGCManifestTaskStore(suite.db)
	rr, err := mrs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(rr))

	// Grab the review record for the child manifest
	require.Equal(t, m.ID, rr[0].ManifestID)

	// delete manifest list
	deletedAt := time.Now()
	ok, err := rs.DeleteManifest(suite.ctx, r, ml.Digest)
	require.NoError(t, err)
	require.True(t, ok)

	// Check that we still have only one review record for m but its due date was postponed to now (delete time) plus
	// defaultReviewAfterDelay and [minReviewAfterJitter, maxReviewAfterJitter].
	rr2, err := mrs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr2)) // the manifest list delete cascaded and deleted its review record as well
	require.Equal(t, rr[0].RepositoryID, rr2[0].RepositoryID)
	require.Equal(t, rr[0].ManifestID, rr2[0].ManifestID)
	require.Equal(t, rr[0].ReviewCount, rr2[0].ReviewCount)
	// We cannot control the random jitter applied when the config blob was first created and then when the manifest was
	// deleted (causing the config blob task to be pushed forward). This means that for this particular test case, the
	// "review after" after the manifest delete might be smaller than the "review after" after the blob creation. So we
	// cannot say that the latter must always be later than the former. The best we can do is to assert that the "review
	// after" has changed and that it's at least defaultReviewAfterDelay plus
	// [minReviewAfterJitter, maxReviewAfterJitter] ahead of the manifest list delete time.
	require.NotEqual(t, rr2[0].ReviewAfter, rr[0].ReviewAfter)
	assertGCReviewDelayInMinMaxRange(t, rr2[0].ReviewAfter, deletedAt)

}

func TestGC_TrackDeletedManifestLists_DoesNothingIfTriggerDisabled(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	enable, err := testutil.GCTrackDeletedManifestListsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()
	// disable other triggers that also insert on gc_manifest_review_queue so that they don't interfere with this test
	enable, err = testutil.GCTrackManifestUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// create manifest list
	ml := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, ml)
	require.NoError(t, err)
	err = ms.AssociateManifest(suite.ctx, ml, m)
	require.NoError(t, err)

	// delete manifest list
	ok, err := rs.DeleteManifest(suite.ctx, r, ml.Digest)
	require.NoError(t, err)
	require.True(t, ok)

	// check that no review records were created
	brs := datastore.NewGCManifestTaskStore(suite.db)
	count, err := brs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestGC_TrackSwitchedTags(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// disable other triggers that also insert on gc_manifest_review_queue so that they don't interfere with this test
	enable, err := testutil.GCTrackManifestUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// tag manifest
	ts := datastore.NewTagStore(suite.db)
	err = ts.CreateOrUpdate(suite.ctx, &models.Tag{
		Name:         "latest",
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m.ID,
	})
	require.NoError(t, err)

	// confirm that the review queue remains empty
	mrs := datastore.NewGCManifestTaskStore(suite.db)
	count, err := mrs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)

	// create another manifest
	m2 := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m2)
	require.NoError(t, err)

	// switch tag to new manifest
	switchedAt := time.Now()
	err = ts.CreateOrUpdate(suite.ctx, &models.Tag{
		Name:         "latest",
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m2.ID,
	})
	require.NoError(t, err)

	// check that a corresponding task was created for the manifest and scheduled for defaultReviewAfterDelay plus
	// [minReviewAfterJitter, maxReviewAfterJitter] ahead. This is done by the `gc_track_switched_tags` trigger/function.
	rr, err := mrs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr))
	require.Equal(t, r.ID, rr[0].RepositoryID)
	require.Equal(t, m.ID, rr[0].ManifestID)
	require.Equal(t, 0, rr[0].ReviewCount)
	require.Equal(t, "tag_switch", rr[0].Event)
	assertGCReviewDelayInMinMaxRange(t, rr[0].ReviewAfter, switchedAt)
}

func TestGC_TrackSwitchedTags_PostponeReviewOnConflict(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err := rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// tag manifest
	ts := datastore.NewTagStore(suite.db)
	err = ts.CreateOrUpdate(suite.ctx, &models.Tag{
		Name:         "latest",
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m.ID,
	})
	require.NoError(t, err)

	// grab existing review record (created by the gc_track_manifest_uploads trigger)
	mrs := datastore.NewGCManifestTaskStore(suite.db)
	rr, err := mrs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr))

	// disable other triggers that also insert on gc_manifest_review_queue so that they don't interfere with this test
	enable, err := testutil.GCTrackManifestUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create another manifest
	m2 := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m2)
	require.NoError(t, err)

	// switch tag to new manifest
	switchedAt := time.Now()
	err = ts.CreateOrUpdate(suite.ctx, &models.Tag{
		Name:         "latest",
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m2.ID,
	})
	require.NoError(t, err)

	// check that we still have only one review record but its due date was postponed to now (delete time) plus
	// defaultReviewAfterDelay and [minReviewAfterJitter, maxReviewAfterJitter]. This is done by the
	// `gc_track_switched_tags` trigger/function.
	rr2, err := mrs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr2))
	require.Equal(t, rr[0].RepositoryID, rr2[0].RepositoryID)
	require.Equal(t, rr[0].ManifestID, rr2[0].ManifestID)
	require.Equal(t, 0, rr2[0].ReviewCount)
	// We cannot control the random jitter applied when the manifest was first created and then when its tag was
	// switched (causing the manifest task to be pushed forward). This means that for this particular test case, the
	// "review after" after the tag switch might be smaller than the "review after" after the manifest creation. So we
	// cannot say that the latter must always be later than the former. The best we can do is to assert that the "review
	// after" has changed and that it's at least defaultReviewAfterDelay plus
	// [minReviewAfterJitter, maxReviewAfterJitter] ahead of the tag switch time.
	require.NotEqual(t, rr2[0].ReviewAfter, rr[0].ReviewAfter)
	assertGCReviewDelayInMinMaxRange(t, rr2[0].ReviewAfter, switchedAt)
}

func TestGC_TrackSwitchedTags_DoesNothingIfTriggerDisabled(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	enable, err := testutil.GCTrackSwitchedTagsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()
	// disable other triggers that also insert on gc_manifest_review_queue so that they don't interfere with this test
	enable, err = testutil.GCTrackManifestUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// tag manifest
	ts := datastore.NewTagStore(suite.db)
	err = ts.CreateOrUpdate(suite.ctx, &models.Tag{
		Name:         "latest",
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m.ID,
	})
	require.NoError(t, err)

	// create another manifest
	m2 := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m2)
	require.NoError(t, err)

	// switch tag to new manifest
	err = ts.CreateOrUpdate(suite.ctx, &models.Tag{
		Name:         "latest",
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m2.ID,
	})
	require.NoError(t, err)

	// check that no review records were created
	mrs := datastore.NewGCManifestTaskStore(suite.db)
	count, err := mrs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestGC_TrackDeletedTags(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// disable other triggers that also insert on gc_manifest_review_queue so that they don't interfere with this test
	enable, err := testutil.GCTrackManifestUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// tag manifest
	ts := datastore.NewTagStore(suite.db)
	err = ts.CreateOrUpdate(suite.ctx, &models.Tag{
		Name:         "latest",
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m.ID,
	})
	require.NoError(t, err)

	// confirm that the review queue remains empty
	mrs := datastore.NewGCManifestTaskStore(suite.db)
	count, err := mrs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)

	// delete tag
	deletedAt := time.Now()
	ok, err := rs.DeleteTagByName(suite.ctx, r, "latest")
	require.NoError(t, err)
	require.True(t, ok)

	// Check that a corresponding task was created for the manifest and scheduled to now (delete time) plus
	// defaultReviewAfterDelay and [minReviewAfterJitter, maxReviewAfterJitter]. This is done by the
	// `gc_track_deleted_tags` trigger/function.
	rr, err := mrs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr))
	require.Equal(t, r.ID, rr[0].RepositoryID)
	require.Equal(t, m.ID, rr[0].ManifestID)
	require.Equal(t, 0, rr[0].ReviewCount)
	require.Equal(t, "tag_delete", rr[0].Event)
	assertGCReviewDelayInMinMaxRange(t, rr[0].ReviewAfter, deletedAt)
}

func TestGC_TrackDeletedTags_MultipleTags(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// disable other triggers that also insert on gc_manifest_review_queue so that they don't interfere with this test
	enable, err := testutil.GCTrackManifestUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// tag manifest twice
	ts := datastore.NewTagStore(suite.db)
	tags := []string{"1.0.0", "latest"}
	for _, tag := range tags {
		err = ts.CreateOrUpdate(suite.ctx, &models.Tag{
			Name:         tag,
			NamespaceID:  r.NamespaceID,
			RepositoryID: r.ID,
			ManifestID:   m.ID,
		})
		require.NoError(t, err)
	}

	// confirm that the review queue remains empty
	mrs := datastore.NewGCManifestTaskStore(suite.db)
	count, err := mrs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)

	// delete tags
	deletedAt := time.Now()
	for _, tag := range tags {
		ok, err := rs.DeleteTagByName(suite.ctx, r, tag)
		require.NoError(t, err)
		require.True(t, ok)
	}

	// Check that a single corresponding task was created for the manifest and scheduled for defaultReviewAfterDelay
	// plus [minReviewAfterJitter, maxReviewAfterJitter]. This is done by the `gc_track_deleted_tags` trigger/function.
	rr, err := mrs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr))
	require.Equal(t, r.ID, rr[0].RepositoryID)
	require.Equal(t, m.ID, rr[0].ManifestID)
	require.Equal(t, 0, rr[0].ReviewCount)
	require.Equal(t, "tag_delete", rr[0].Event)
	assertGCReviewDelayInMinMaxRange(t, rr[0].ReviewAfter, deletedAt)
}

func TestGC_TrackDeletedTags_ManifestDeleteCascade(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// disable other triggers that also insert on gc_manifest_review_queue so that they don't interfere with this test
	enable, err := testutil.GCTrackManifestUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// tag manifest
	ts := datastore.NewTagStore(suite.db)
	err = ts.CreateOrUpdate(suite.ctx, &models.Tag{
		Name:         "latest",
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m.ID,
	})
	require.NoError(t, err)

	// delete manifest (cascades to tags)
	ok, err := rs.DeleteManifest(suite.ctx, r, m.Digest)
	require.NoError(t, err)
	require.True(t, ok)

	// check that no task was created, as the corresponding manifest no longer exists
	mrs := datastore.NewGCManifestTaskStore(suite.db)
	rr, err := mrs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Empty(t, rr)
}

func TestGC_TrackDeletedTags_PostponeReviewOnConflict(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err := rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// tag manifest
	ts := datastore.NewTagStore(suite.db)
	err = ts.CreateOrUpdate(suite.ctx, &models.Tag{
		Name:         "latest",
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m.ID,
	})
	require.NoError(t, err)

	// grab existing review record (created by the gc_track_manifest_uploads trigger)
	mrs := datastore.NewGCManifestTaskStore(suite.db)
	rr, err := mrs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr))

	// delete tag
	deletedAt := time.Now()
	ok, err := rs.DeleteTagByName(suite.ctx, r, "latest")
	require.NoError(t, err)
	require.True(t, ok)

	// Check that we still have only one review record but its due date was postponed to now (delete time) plus
	// defaultReviewAfterDelay and [minReviewAfterJitter, maxReviewAfterJitter].
	rr2, err := mrs.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rr2))
	require.Equal(t, rr[0].RepositoryID, rr2[0].RepositoryID)
	require.Equal(t, rr[0].ManifestID, rr2[0].ManifestID)
	require.Equal(t, 0, rr2[0].ReviewCount)
	// We cannot control the random jitter applied when the manifest was first created and then when its tag was
	// deleted (causing the manifest task to be pushed forward). This means that for this particular test case, the
	// "review after" after the tag deletion might be smaller than the "review after" after the manifest creation. So we
	// cannot say that the latter must always be later than the former. The best we can do is to assert that the "review
	// after" has changed and that it's at least defaultReviewAfterDelay plus
	// [minReviewAfterJitter, maxReviewAfterJitter] ahead of the tag deletion time.
	require.NotEqual(t, rr2[0].ReviewAfter, rr[0].ReviewAfter)
	assertGCReviewDelayInMinMaxRange(t, rr2[0].ReviewAfter, deletedAt)
}

func TestGC_TrackDeletedTags_DoesNothingIfTriggerDisabled(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	enable, err := testutil.GCTrackDeletedTagsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()
	// disable other triggers that also insert on gc_manifest_review_queue so that they don't interfere with this test
	enable, err = testutil.GCTrackManifestUploadsTrigger.Disable(suite.db)
	require.NoError(t, err)
	defer enable()

	// create repo
	r := randomRepository(t)
	rs := datastore.NewRepositoryStore(suite.db)
	r, err = rs.CreateByPath(suite.ctx, r.Path)
	require.NoError(t, err)

	// create manifest
	ms := datastore.NewManifestStore(suite.db)
	m := randomManifest(t, r, nil)
	err = ms.Create(suite.ctx, m)
	require.NoError(t, err)

	// tag manifest
	ts := datastore.NewTagStore(suite.db)
	err = ts.CreateOrUpdate(suite.ctx, &models.Tag{
		Name:         "latest",
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m.ID,
	})
	require.NoError(t, err)

	// delete tag
	ok, err := rs.DeleteTagByName(suite.ctx, r, "latest")
	require.NoError(t, err)
	require.True(t, ok)

	// check that no review records were created
	mrs := datastore.NewGCManifestTaskStore(suite.db)
	count, err := mrs.Count(suite.ctx)
	require.NoError(t, err)
	require.Zero(t, count)
}

// assertGCReviewDelayInMinMaxRange makes sure that a GC task that is qeued for review has the expected
// delay for the operation (e.g object delete, object create, object switch) that triggered the review.
// The expected delay is described as:
// trigerOperationTime + x, defaultReviewAfterWithMinJitterDelay  < x < defaultReviewAfterWithMaxJitterDelay
// where trigerOperationTime is the time the operation that triggered the review task to be qeued was executed
// and actualReviewAfterTime is the actual time the review is set to be carried out.
func assertGCReviewDelayInMinMaxRange(t *testing.T, actualReviewAfterTime time.Time, trigerOperationTime time.Time) {
	t.Helper()

	lowerBound := trigerOperationTime.Add(defaultReviewAfterWithMinJitterDelay)
	upperBound := trigerOperationTime.Add(defaultReviewAfterWithMaxJitterDelay)
	require.WithinRange(t, actualReviewAfterTime, lowerBound, upperBound,
		"GC review delay of %s should be between %s and %s",
		actualReviewAfterTime.String(), lowerBound.String(), upperBound.String())
}
