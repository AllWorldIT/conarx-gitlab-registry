//go:build integration

package datastore_test

import (
	"testing"
	"time"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/stretchr/testify/require"
)

func reloadGCReviewAfterDefaultFixtures(tb testing.TB) {
	tb.Helper()
	testutil.ReloadFixtures(tb, suite.db, suite.basePath, testutil.GCReviewAfterDefaultsTable)
}

func findAllGCReviewAfterDefaults(tb testing.TB, db datastore.Queryer) []*models.GCReviewAfterDefault {
	tb.Helper()

	// the `value` column is of type `interval` so we read it in seconds into an int64
	q := `SELECT
			event,
			EXTRACT(epoch FROM value)::bigint
		FROM
			gc_review_after_defaults
		ORDER BY
			event`

	rows, err := db.QueryContext(suite.ctx, q)
	require.NoError(tb, err)
	defer rows.Close()

	dd := make([]*models.GCReviewAfterDefault, 0)

	for rows.Next() {
		var secs int64
		d := &models.GCReviewAfterDefault{}

		err = rows.Scan(&d.Event, &secs)
		require.NoError(tb, err)

		d.Value = time.Duration(secs) * time.Second
		dd = append(dd, d)
	}
	require.NoError(tb, rows.Err())

	return dd
}

func Test_findAllGCReviewAfterDefaults(t *testing.T) {
	reloadGCReviewAfterDefaultFixtures(t)

	// see testdata/fixtures/gc_review_after_defaults.sql
	expected := []*models.GCReviewAfterDefault{
		{"blob_upload", 24 * time.Hour},
		{"layer_delete", 16 * time.Hour},
		{"manifest_delete", 1 * time.Hour},
		{"manifest_list_delete", 1 * time.Minute},
		{"manifest_upload", 7 * 24 * time.Hour},
		{"tag_delete", 21 * time.Minute},
		{"tag_switch", 0},
	}

	actual := findAllGCReviewAfterDefaults(t, suite.db)
	require.Equal(t, expected, actual)
}

func TestGCSettingsStore_UpdateAllReviewAfterDefaults(t *testing.T) {
	reloadGCReviewAfterDefaultFixtures(t)

	s := datastore.NewGCSettingsStore(suite.db)
	updated, err := s.UpdateAllReviewAfterDefaults(suite.ctx, 1*time.Minute)
	require.NoError(t, err)
	require.True(t, updated)

	// see testdata/fixtures/gc_review_after_defaults.sql
	expected := []*models.GCReviewAfterDefault{
		{"blob_upload", 1 * time.Minute},
		{"layer_delete", 1 * time.Minute},
		{"manifest_delete", 1 * time.Minute},
		{"manifest_list_delete", 1 * time.Minute},
		{"manifest_upload", 1 * time.Minute},
		{"tag_delete", 1 * time.Minute},
		{"tag_switch", 1 * time.Minute},
	}

	actual := findAllGCReviewAfterDefaults(t, suite.db)
	require.Equal(t, expected, actual)

	// make sure it's idempotent
	updated, err = s.UpdateAllReviewAfterDefaults(suite.ctx, 1*time.Minute)
	require.NoError(t, err)
	require.False(t, updated)

	actual = findAllGCReviewAfterDefaults(t, suite.db)
	require.Equal(t, expected, actual)
}
