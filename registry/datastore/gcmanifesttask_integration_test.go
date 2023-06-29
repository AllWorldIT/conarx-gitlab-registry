//go:build integration

package datastore_test

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/stretchr/testify/require"
)

func reloadGCManifestTaskFixtures(tb testing.TB) {
	reloadManifestFixtures(tb)
	testutil.ReloadFixtures(tb, suite.db, suite.basePath, testutil.GCManifestReviewQueueTable)
}

func unloadGCManifestTaskFixtures(tb testing.TB) {
	require.NoError(tb, testutil.TruncateTables(suite.db, testutil.GCManifestReviewQueueTable))
}

func TestGCManifestTaskStore_FindAll(t *testing.T) {
	reloadGCManifestTaskFixtures(t)

	s := datastore.NewGCManifestTaskStore(suite.db)
	rr, err := s.FindAll(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/gc_manifest_review_queue.sql
	local := rr[0].ReviewAfter.Location()
	expected := []*models.GCManifestTask{
		{
			NamespaceID:  1,
			RepositoryID: 4,
			ManifestID:   7,
			ReviewAfter:  testutil.ParseTimestamp(t, "2020-04-03 18:45:04.470711", local),
			ReviewCount:  2,
			CreatedAt:    testutil.ParseTimestamp(t, "2020-04-02 18:45:04.470711", local),
			Event:        "manifest_upload",
		},
		{
			NamespaceID:  1,
			RepositoryID: 4,
			ManifestID:   9,
			ReviewAfter:  testutil.ParseTimestamp(t, "9999-12-31 23:59:59.999999", local),
			ReviewCount:  0,
			CreatedAt:    testutil.ParseTimestamp(t, "9999-12-30 23:59:59.999999", local),
			Event:        "manifest_delete",
		},
		{
			NamespaceID:  1,
			RepositoryID: 4,
			ManifestID:   4,
			ReviewAfter:  testutil.ParseTimestamp(t, "2020-06-11 09:11:23.655121", local),
			ReviewCount:  0,
			CreatedAt:    testutil.ParseTimestamp(t, "2020-06-10 09:11:23.655121", local),
			Event:        "manifest_list_delete",
		},
		{
			NamespaceID:  1,
			RepositoryID: 3,
			ManifestID:   1,
			ReviewAfter:  testutil.ParseTimestamp(t, "2020-03-03 17:50:26.461745", local),
			ReviewCount:  0,
			CreatedAt:    testutil.ParseTimestamp(t, "2020-03-02 17:50:26.461745", local),
			Event:        "tag_switch",
		},
	}

	require.Equal(t, expected, rr)
}

func TestGCManifestTaskStore_FindAll_NotFound(t *testing.T) {
	unloadGCManifestTaskFixtures(t)

	s := datastore.NewGCManifestTaskStore(suite.db)
	rr, err := s.FindAll(suite.ctx)
	require.Empty(t, rr)
	require.NoError(t, err)
}

func TestGCManifestTaskStore_FindAndLock(t *testing.T) {
	reloadGCManifestTaskFixtures(t)

	ctx1, cancel1 := context.WithCancel(suite.ctx)
	defer cancel1()

	tx1, err := suite.db.BeginTx(ctx1, nil)
	require.NoError(t, err)
	defer tx1.Rollback()

	s := datastore.NewGCManifestTaskStore(tx1)

	// see testdata/fixtures/gc_manifest_review_queue.sql
	r, err := s.FindAndLock(suite.ctx, 1, 4, 7)
	require.NoError(t, err)

	local := r.ReviewAfter.Location()
	expected := &models.GCManifestTask{
		NamespaceID:  1,
		RepositoryID: 4,
		ManifestID:   7,
		ReviewAfter:  testutil.ParseTimestamp(t, "2020-04-03 18:45:04.470711", local),
		ReviewCount:  2,
		CreatedAt:    testutil.ParseTimestamp(t, "2020-04-02 18:45:04.470711", local),
		Event:        "manifest_upload",
	}

	require.Equal(t, expected, r)

	// attempt to find the same record in a separate transaction, it should block and eventually timeout
	ctx2, cancel2 := context.WithTimeout(suite.ctx, 500*time.Millisecond)
	defer cancel2()

	tx2, err := suite.db.BeginTx(ctx2, nil)
	require.NoError(t, err)
	defer tx2.Rollback()
	s = datastore.NewGCManifestTaskStore(tx2)
	r, err = s.FindAndLock(ctx2, 1, 4, 7)

	var netError net.Error
	ok := errors.As(err, &netError)
	require.True(t, ok)
	require.True(t, netError.Timeout())
	require.Nil(t, r)

	// attempt again, but this time tx1 completes before the tx3 timeout, and it gets to lock the row
	ctx3, cancel3 := context.WithTimeout(suite.ctx, 1*time.Second)
	defer cancel3()

	tx3, err := suite.db.BeginTx(ctx3, nil)
	require.NoError(t, err)
	defer tx3.Rollback()

	go time.AfterFunc(500*time.Millisecond, func() { tx1.Rollback() })

	s = datastore.NewGCManifestTaskStore(tx3)
	r, err = s.FindAndLock(ctx3, 1, 4, 7)
	require.NoError(t, err)
	require.Equal(t, expected, r)
}

func TestGCManifestTaskStore_FindAndLock_NotFound(t *testing.T) {
	reloadGCManifestTaskFixtures(t)

	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	s := datastore.NewGCManifestTaskStore(tx)

	r, err := s.FindAndLock(suite.ctx, 1, 400, 7)
	require.NoError(t, err)
	require.Nil(t, r)
}

func TestGCManifestTaskStore_FindAndLockBefore(t *testing.T) {
	reloadGCManifestTaskFixtures(t)

	ctx1, cancel1 := context.WithCancel(suite.ctx)
	defer cancel1()

	tx1, err := suite.db.BeginTx(ctx1, nil)
	require.NoError(t, err)
	defer tx1.Rollback()

	s := datastore.NewGCManifestTaskStore(tx1)

	// see testdata/fixtures/gc_manifest_review_queue.sql
	date := testutil.ParseTimestamp(t, "2020-04-03 18:45:04.470711", time.UTC).Add(1 * time.Minute)
	r, err := s.FindAndLockBefore(suite.ctx, 1, 4, 7, date)
	require.NoError(t, err)

	local := r.ReviewAfter.Location()
	expected := &models.GCManifestTask{
		NamespaceID:  1,
		RepositoryID: 4,
		ManifestID:   7,
		ReviewAfter:  testutil.ParseTimestamp(t, "2020-04-03 18:45:04.470711", local),
		ReviewCount:  2,
		CreatedAt:    testutil.ParseTimestamp(t, "2020-04-02 18:45:04.470711", local),
		Event:        "manifest_upload",
	}

	require.Equal(t, expected, r)

	// attempt to find the same record in a separate transaction, it should block and eventually timeout
	ctx2, cancel2 := context.WithTimeout(suite.ctx, 500*time.Millisecond)
	defer cancel2()

	tx2, err := suite.db.BeginTx(ctx2, nil)
	require.NoError(t, err)
	defer tx2.Rollback()

	s = datastore.NewGCManifestTaskStore(tx2)
	r, err = s.FindAndLockBefore(ctx2, 1, 4, 7, date)

	var netError net.Error
	ok := errors.As(err, &netError)
	require.True(t, ok)
	require.True(t, netError.Timeout())
	require.Nil(t, r)

	// attempt again, but this time tx1 completes before the tx3 timeout and it gets to lock the row
	ctx3, cancel3 := context.WithTimeout(suite.ctx, 1*time.Second)
	defer cancel3()

	tx3, err := suite.db.BeginTx(ctx3, nil)
	require.NoError(t, err)
	defer tx3.Rollback()

	go time.AfterFunc(500*time.Millisecond, func() { tx1.Rollback() })

	s = datastore.NewGCManifestTaskStore(tx3)
	r, err = s.FindAndLockBefore(ctx3, 1, 4, 7, date)
	require.NoError(t, err)
	require.Equal(t, expected, r)
}

func TestGCManifestTaskStore_FindAndLockBefore_NotFound(t *testing.T) {
	reloadGCManifestTaskFixtures(t)

	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	s := datastore.NewGCManifestTaskStore(tx)

	date := testutil.ParseTimestamp(t, "2020-04-03 18:45:04.470711", time.UTC)

	// when there is no such (repository_id, manifest_id) pair
	r, err := s.FindAndLockBefore(suite.ctx, 1, 400, 7, date.Add(1*time.Minute))
	require.NoError(t, err)
	require.Nil(t, r)

	// when the review_after is not before the given date
	r, err = s.FindAndLockBefore(suite.ctx, 1, 4, 7, date)
	require.NoError(t, err)
	require.Nil(t, r)
}

func TestGCManifestTaskStore_FindAndLockNBefore(t *testing.T) {
	reloadGCManifestTaskFixtures(t)

	ctx1, cancel1 := context.WithCancel(suite.ctx)
	defer cancel1()

	tx1, err := suite.db.BeginTx(ctx1, nil)
	require.NoError(t, err)
	defer tx1.Rollback()

	s := datastore.NewGCManifestTaskStore(tx1)

	// see testdata/fixtures/gc_manifest_review_queue.sql
	date := testutil.ParseTimestamp(t, "2020-06-11 09:11:23.655121", time.UTC).Add(1 * time.Minute)
	ids := []int64{7, 4}
	rr, err := s.FindAndLockNBefore(suite.ctx, 1, 4, ids, date)
	require.NoError(t, err)

	local := rr[0].ReviewAfter.Location()
	expected := []*models.GCManifestTask{
		{
			NamespaceID:  1,
			RepositoryID: 4,
			ManifestID:   4,
			ReviewAfter:  testutil.ParseTimestamp(t, "2020-06-11 09:11:23.655121", local),
			ReviewCount:  0,
			CreatedAt:    testutil.ParseTimestamp(t, "2020-06-10 09:11:23.655121", local),
			Event:        "manifest_list_delete",
		},
		{
			NamespaceID:  1,
			RepositoryID: 4,
			ManifestID:   7,
			ReviewAfter:  testutil.ParseTimestamp(t, "2020-04-03 18:45:04.470711", local),
			ReviewCount:  2,
			CreatedAt:    testutil.ParseTimestamp(t, "2020-04-02 18:45:04.470711", local),
			Event:        "manifest_upload",
		},
	}

	require.Equal(t, expected, rr)

	// attempt to find the same records in a separate transaction, it should block and eventually timeout
	ctx2, cancel2 := context.WithTimeout(suite.ctx, 500*time.Millisecond)
	defer cancel2()

	tx2, err := suite.db.BeginTx(ctx2, nil)
	require.NoError(t, err)
	defer tx2.Rollback()

	s = datastore.NewGCManifestTaskStore(tx2)
	rr, err = s.FindAndLockNBefore(ctx2, 1, 4, ids, date)

	var netError net.Error
	ok := errors.As(err, &netError)
	require.True(t, ok)
	require.True(t, netError.Timeout())
	require.Empty(t, rr)

	// attempt again, but this time tx1 completes before the tx3 timeout and it gets to lock the row
	ctx3, cancel3 := context.WithTimeout(suite.ctx, 1*time.Second)
	defer cancel3()

	tx3, err := suite.db.BeginTx(ctx3, nil)
	require.NoError(t, err)
	defer tx3.Rollback()

	go time.AfterFunc(500*time.Millisecond, func() { tx1.Rollback() })

	s = datastore.NewGCManifestTaskStore(tx3)
	rr, err = s.FindAndLockNBefore(suite.ctx, 1, 4, ids, date)
	require.NoError(t, err)
	require.Equal(t, expected, rr)
}

func TestGCManifestTaskStore_FindAndLockNBefore_NotFound(t *testing.T) {
	reloadGCManifestTaskFixtures(t)

	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	s := datastore.NewGCManifestTaskStore(tx)

	// when there is no such (repository_id, manifest_id) pair
	date := testutil.ParseTimestamp(t, "2020-06-11 09:11:23.655121", time.UTC).Add(1 * time.Minute)
	rr, err := s.FindAndLockNBefore(suite.ctx, 1, 400, []int64{7, 4}, date)
	require.NoError(t, err)
	require.Empty(t, rr)

	// when the review_after is not before the given date
	date = testutil.ParseTimestamp(t, "2020-04-03 18:45:04.470711", time.UTC)
	rr, err = s.FindAndLockNBefore(suite.ctx, 1, 4, []int64{7, 4}, date)
	require.NoError(t, err)
	require.Empty(t, rr)
}

func TestGcManifestTaskStore_Count(t *testing.T) {
	reloadGCManifestTaskFixtures(t)

	s := datastore.NewGCManifestTaskStore(suite.db)
	count, err := s.Count(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/gc_manifest_review_queue.sql
	require.Equal(t, 4, count)
}

func nextGCManifestTask(t *testing.T) (datastore.Transactor, *models.GCManifestTask) {
	t.Helper()

	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, tx)

	s := datastore.NewGCManifestTaskStore(tx)
	m, err := s.Next(suite.ctx)
	require.NoError(t, err)

	return tx, m
}

func TestGCManifestTaskStore_Next(t *testing.T) {
	// see testdata/fixtures/gc_manifest_review_queue.sql
	reloadGCManifestTaskFixtures(t)

	// the 1st call should return the record with the oldest review_after
	tx1, m1 := nextGCManifestTask(t)
	defer tx1.Rollback()

	local := m1.ReviewAfter.Location()
	require.Equal(t, &models.GCManifestTask{
		NamespaceID:  1,
		RepositoryID: 3,
		ManifestID:   1,
		ReviewAfter:  testutil.ParseTimestamp(t, "2020-03-03 17:50:26.461745", local),
		ReviewCount:  0,
		CreatedAt:    testutil.ParseTimestamp(t, "2020-03-02 17:50:26.461745", local),
		Event:        "tag_switch",
	}, m1)

	// The 2nd call should yield the unlocked record with the 2nd oldest review_after. In case of a draw (multiple
	// records with the same review_after), which occurs here, the returned row is the one that was first inserted.
	tx2, m2 := nextGCManifestTask(t)
	defer tx2.Rollback()

	expectedM2 := &models.GCManifestTask{
		NamespaceID:  1,
		RepositoryID: 4,
		ManifestID:   7,
		ReviewAfter:  testutil.ParseTimestamp(t, "2020-04-03 18:45:04.470711", local),
		ReviewCount:  2,
		CreatedAt:    testutil.ParseTimestamp(t, "2020-04-02 18:45:04.470711", local),
		Event:        "manifest_upload",
	}
	require.Equal(t, expectedM2, m2)

	// The 3rd call should yield the unlocked record with the 3rd oldest review_after.
	tx3, m3 := nextGCManifestTask(t)
	defer tx3.Rollback()

	expectedM3 := &models.GCManifestTask{
		NamespaceID:  1,
		RepositoryID: 4,
		ManifestID:   4,
		ReviewAfter:  testutil.ParseTimestamp(t, "2020-06-11 09:11:23.655121", local),
		ReviewCount:  0,
		CreatedAt:    testutil.ParseTimestamp(t, "2020-06-10 09:11:23.655121", local),
		Event:        "manifest_list_delete",
	}
	require.Equal(t, expectedM3, m3)

	// Calling Next again yields nothing and does not block, as the remaining unlocked record has a review_after in
	// the future.
	tx4, m4 := nextGCManifestTask(t)
	defer tx4.Rollback()
	require.Nil(t, m4)

	// unlocking m3 and calling Next returns m3 once again
	require.NoError(t, tx3.Rollback())
	tx5, m5 := nextGCManifestTask(t)
	defer tx5.Rollback()
	require.Equal(t, expectedM3, m5)
}

func TestGcManifestTaskStore_Next_None(t *testing.T) {
	unloadGCManifestTaskFixtures(t)

	tx, m := nextGCManifestTask(t)
	defer tx.Rollback()
	require.Nil(t, m)
}

func TestGcManifestTaskStore_Postpone(t *testing.T) {
	// see testdata/fixtures/gc_manifest_review_queue.sql
	reloadGCManifestTaskFixtures(t)

	tx, m := nextGCManifestTask(t)
	defer tx.Rollback()

	oldReviewAfter := m.ReviewAfter
	oldReviewCount := m.ReviewCount
	d := 24 * time.Hour

	s := datastore.NewGCManifestTaskStore(tx)
	err := s.Postpone(suite.ctx, m, d)
	require.NoError(t, err)
	require.Equal(t, oldReviewAfter.Add(d), m.ReviewAfter)
	require.Equal(t, oldReviewCount+1, m.ReviewCount)
}

func TestGcManifestTaskStore_Postpone_NotFound(t *testing.T) {
	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	s := datastore.NewGCManifestTaskStore(tx)
	err = s.Postpone(suite.ctx, &models.GCManifestTask{RepositoryID: 1, ManifestID: 3}, 0)
	require.EqualError(t, err, "GC manifest task not found")
}

func existsGCManifestTask(t *testing.T, db datastore.Queryer, repositoryID, manifestID int64) bool {
	t.Helper()

	q := `SELECT
			EXISTS (
				SELECT
					1
				FROM
					gc_manifest_review_queue
				WHERE
					repository_id = $1
					AND manifest_id = $2)`

	var exists bool
	require.NoError(t, db.QueryRowContext(suite.ctx, q, repositoryID, manifestID).Scan(&exists))

	return exists
}

func TestExistsGCManifestTask(t *testing.T) {
	// see testdata/fixtures/gc_manifest_review_queue.sql
	reloadGCManifestTaskFixtures(t)

	require.True(t, existsGCManifestTask(t, suite.db, 4, 7))
	require.False(t, existsGCManifestTask(t, suite.db, 6, 2))
}

func pickGCManifestTask(t *testing.T, db datastore.Queryer, namespaceID, repositoryID, manifestID int64) *models.GCManifestTask {
	t.Helper()

	q := `SELECT
			review_after,
			review_count
		FROM
			gc_manifest_review_queue
		WHERE
			top_level_namespace_id = $1
			AND repository_id = $2
			AND manifest_id = $3
		FOR UPDATE`

	m := &models.GCManifestTask{NamespaceID: namespaceID, RepositoryID: repositoryID, ManifestID: manifestID}
	if err := db.QueryRowContext(suite.ctx, q, m.NamespaceID, m.RepositoryID, m.ManifestID).Scan(&m.ReviewAfter, &m.ReviewCount); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		t.Error(err)
	}

	return m
}

func TestPickGCManifestTask(t *testing.T) {
	// see testdata/fixtures/gc_manifest_review_queue.sql
	reloadGCManifestTaskFixtures(t)

	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	namespaceID := int64(1)
	repositoryID := int64(4)
	manifestID := int64(7)
	m := pickGCManifestTask(t, tx, namespaceID, repositoryID, manifestID)

	require.Equal(t, &models.GCManifestTask{
		NamespaceID:  namespaceID,
		RepositoryID: repositoryID,
		ManifestID:   manifestID,
		ReviewAfter:  testutil.ParseTimestamp(t, "2020-04-03 18:45:04.470711", m.ReviewAfter.Location()),
		ReviewCount:  2,
	}, m)
	require.Nil(t, pickGCManifestTask(t, tx, 2, 6, 2))
}

func TestGcManifestTaskStore_Delete(t *testing.T) {
	// see testdata/fixtures/gc_manifest_review_queue.sql
	reloadGCManifestTaskFixtures(t)

	tx, m := nextGCManifestTask(t)
	defer tx.Rollback()

	s := datastore.NewGCManifestTaskStore(tx)
	err := s.Delete(suite.ctx, m)
	require.NoError(t, err)
	require.False(t, existsGCManifestTask(t, tx, m.RepositoryID, m.ManifestID))
}

func TestGcManifestTaskStore_Delete_NotFound(t *testing.T) {
	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	s := datastore.NewGCManifestTaskStore(tx)
	err = s.Delete(suite.ctx, &models.GCManifestTask{RepositoryID: 6, ManifestID: 2})
	require.EqualError(t, err, "GC manifest task not found")
}

func TestGcManifestTaskStore_IsDangling_Yes(t *testing.T) {
	s := datastore.NewGCManifestTaskStore(suite.db)
	yn, err := s.IsDangling(suite.ctx, &models.GCManifestTask{RepositoryID: 6, ManifestID: 2})
	require.NoError(t, err)
	require.True(t, yn)
}

func TestGcManifestTaskStore_IsDangling_No_Tagged(t *testing.T) {
	// see testdata/fixtures/[gc_manifest_review_queue|tags].sql
	reloadGCManifestTaskFixtures(t)

	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	m := pickGCManifestTask(t, tx, 1, 4, 7)
	require.NotNil(t, m)

	s := datastore.NewGCManifestTaskStore(tx)
	yn, err := s.IsDangling(suite.ctx, m)
	require.NoError(t, err)
	require.False(t, yn)
}

func TestGcManifestTaskStore_IsDangling_No_ReferencedByList(t *testing.T) {
	// see testdata/fixtures/[gc_manifest_review_queue|manifest_references].sql
	reloadGCManifestTaskFixtures(t)

	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	m := pickGCManifestTask(t, tx, 1, 4, 9)
	require.NotNil(t, m)

	s := datastore.NewGCManifestTaskStore(tx)
	yn, err := s.IsDangling(suite.ctx, m)
	require.NoError(t, err)
	require.False(t, yn)
}

func TestGcManifestTaskStore_IsDangling_No_TaggedAndReferencedByList(t *testing.T) {
	// see testdata/fixtures/[gc_manifest_review_queue|tags|manifest_references].sql
	reloadGCManifestTaskFixtures(t)

	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	// tag an (untagged) manifest referenced by a manifest list
	ts := datastore.NewTagStore(tx)
	err = ts.CreateOrUpdate(suite.ctx, &models.Tag{NamespaceID: 1, RepositoryID: 4, ManifestID: 9, Name: "foo"})
	require.NoError(t, err)

	m := pickGCManifestTask(t, tx, 1, 4, 9)
	require.NotNil(t, m)

	s := datastore.NewGCManifestTaskStore(tx)
	yn, err := s.IsDangling(suite.ctx, m)
	require.NoError(t, err)
	require.False(t, yn)
}
