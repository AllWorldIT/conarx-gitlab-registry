//go:build integration

package datastore_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func unloadImportStatisticsFixtures(tb testing.TB) {
	require.NoError(tb, testutil.TruncateTables(
		suite.db,
		testutil.ImportStatisticsTable,
	))
}

func TestImportStatisticsStore_ImplementsInterface(t *testing.T) {
	require.Implements(t, (*datastore.ImportStatisticsStore)(nil), datastore.NewImportStatisticsStore(suite.db))
}

func TestImportStatisticsStore_Create(t *testing.T) {
	unloadImportStatisticsFixtures(t)

	s := datastore.NewImportStatisticsStore(suite.db)

	now := time.Now()
	stats := &models.ImportStatistics{
		StartedAt:         now,
		PreImport:         true,
		TagImport:         true,
		BlobImport:        false,
		RepositoriesCount: 5,
		TagsCount:         25,
		ManifestsCount:    15,
		BlobsCount:        100,
		BlobsSizeBytes:    1024000,
		StorageDriver:     "test driver",
	}

	err := s.Create(suite.ctx, stats)
	require.NoError(t, err)
	assert.NotZero(t, stats.ID)
	assert.NotZero(t, stats.CreatedAt)
}

func TestImportStatisticsStore_Create_AllFields(t *testing.T) {
	unloadImportStatisticsFixtures(t)

	s := datastore.NewImportStatisticsStore(suite.db)

	now := time.Now()
	preStart := now.Add(-10 * time.Minute)
	preFinish := now.Add(-8 * time.Minute)
	tagStart := now.Add(-7 * time.Minute)
	tagFinish := now.Add(-3 * time.Minute)
	blobStart := now.Add(-2 * time.Minute)
	blobFinish := now.Add(-1 * time.Minute)
	finished := now

	preError := "test pre import error"
	tagError := "test tag import error"
	blobError := "test blob import error"

	stats := &models.ImportStatistics{
		StartedAt:            preStart,
		FinishedAt:           sql.NullTime{Time: finished, Valid: true},
		PreImport:            true,
		PreImportStartedAt:   sql.NullTime{Time: preStart, Valid: true},
		PreImportFinishedAt:  sql.NullTime{Time: preFinish, Valid: true},
		PreImportError:       sql.NullString{String: preError, Valid: true},
		TagImport:            true,
		TagImportStartedAt:   sql.NullTime{Time: tagStart, Valid: true},
		TagImportFinishedAt:  sql.NullTime{Time: tagFinish, Valid: true},
		TagImportError:       sql.NullString{String: tagError, Valid: true},
		BlobImport:           true,
		BlobImportStartedAt:  sql.NullTime{Time: blobStart, Valid: true},
		BlobImportFinishedAt: sql.NullTime{Time: blobFinish, Valid: true},
		BlobImportError:      sql.NullString{String: blobError, Valid: true},
		RepositoriesCount:    42,
		TagsCount:            150,
		ManifestsCount:       75,
		BlobsCount:           500,
		BlobsSizeBytes:       5000000,
		StorageDriver:        "test driver",
	}

	err := s.Create(suite.ctx, stats)
	require.NoError(t, err)
	assert.NotZero(t, stats.ID)
	assert.NotZero(t, stats.CreatedAt)
}

func TestImportStatisticsStore_FindByID(t *testing.T) {
	unloadImportStatisticsFixtures(t)

	s := datastore.NewImportStatisticsStore(suite.db)

	now := time.Now()
	original := &models.ImportStatistics{
		StartedAt:         now,
		PreImport:         true,
		TagImport:         true,
		BlobImport:        false,
		RepositoriesCount: 10,
		TagsCount:         50,
		ManifestsCount:    30,
		BlobsCount:        200,
		BlobsSizeBytes:    2048000,
		StorageDriver:     "test driver",
	}

	err := s.Create(suite.ctx, original)
	require.NoError(t, err)

	found, err := s.FindByID(suite.ctx, original.ID)
	require.NoError(t, err)
	require.NotNil(t, found)

	assert.Equal(t, original.ID, found.ID)
	assert.Equal(t, original.PreImport, found.PreImport)
	assert.Equal(t, original.TagImport, found.TagImport)
	assert.Equal(t, original.BlobImport, found.BlobImport)
	assert.Equal(t, original.RepositoriesCount, found.RepositoriesCount)
	assert.Equal(t, original.TagsCount, found.TagsCount)
	assert.Equal(t, original.ManifestsCount, found.ManifestsCount)
	assert.Equal(t, original.BlobsCount, found.BlobsCount)
	assert.Equal(t, original.BlobsSizeBytes, found.BlobsSizeBytes)
	assert.Equal(t, original.StorageDriver, found.StorageDriver)
	assert.WithinDuration(t, original.CreatedAt, found.CreatedAt, 500*time.Millisecond)
	assert.WithinDuration(t, original.StartedAt, found.StartedAt, 500*time.Millisecond)
}

func TestImportStatisticsStore_FindByID_NotFound(t *testing.T) {
	unloadImportStatisticsFixtures(t)

	s := datastore.NewImportStatisticsStore(suite.db)

	found, err := s.FindByID(suite.ctx, 999)
	require.NoError(t, err)
	assert.Nil(t, found)
}

func TestImportStatisticsStore_FindByID_WithCompleteData(t *testing.T) {
	unloadImportStatisticsFixtures(t)

	s := datastore.NewImportStatisticsStore(suite.db)

	// Create a record with all fields populated
	now := time.Now()
	preStart := now.Add(-15 * time.Minute)
	preFinish := now.Add(-12 * time.Minute)
	tagStart := now.Add(-10 * time.Minute)
	tagFinish := now.Add(-5 * time.Minute)
	blobStart := now.Add(-4 * time.Minute)
	blobFinish := now.Add(-1 * time.Minute)
	finished := now

	preError := "test pre import error"
	tagError := "test tag import error"
	blobError := "test blob import error"

	original := &models.ImportStatistics{
		StartedAt:            preStart,
		FinishedAt:           sql.NullTime{Time: finished, Valid: true},
		PreImport:            true,
		PreImportStartedAt:   sql.NullTime{Time: preStart, Valid: true},
		PreImportFinishedAt:  sql.NullTime{Time: preFinish, Valid: true},
		PreImportError:       sql.NullString{String: preError, Valid: true},
		TagImport:            true,
		TagImportStartedAt:   sql.NullTime{Time: tagStart, Valid: true},
		TagImportFinishedAt:  sql.NullTime{Time: tagFinish, Valid: true},
		TagImportError:       sql.NullString{String: tagError, Valid: true},
		BlobImport:           true,
		BlobImportStartedAt:  sql.NullTime{Time: blobStart, Valid: true},
		BlobImportFinishedAt: sql.NullTime{Time: blobFinish, Valid: true},
		BlobImportError:      sql.NullString{String: blobError, Valid: true},
		RepositoriesCount:    100,
		TagsCount:            1000,
		ManifestsCount:       500,
		BlobsCount:           5000,
		BlobsSizeBytes:       10000000,
		StorageDriver:        "test driver",
	}

	err := s.Create(suite.ctx, original)
	require.NoError(t, err)

	// Find and verify all fields
	found, err := s.FindByID(suite.ctx, original.ID)
	require.NoError(t, err)
	require.NotNil(t, found)

	assert.Equal(t, original.ID, found.ID)
	assert.True(t, found.FinishedAt.Valid)
	assert.WithinDuration(t, original.FinishedAt.Time, found.FinishedAt.Time, 500*time.Millisecond)

	assert.Equal(t, original.PreImport, found.PreImport)
	assert.True(t, found.PreImportStartedAt.Valid)
	assert.WithinDuration(t, original.PreImportStartedAt.Time, found.PreImportStartedAt.Time, 500*time.Millisecond)
	assert.True(t, found.PreImportFinishedAt.Valid)
	assert.WithinDuration(t, original.PreImportFinishedAt.Time, found.PreImportFinishedAt.Time, 500*time.Millisecond)
	assert.True(t, found.PreImportError.Valid)
	assert.Equal(t, original.PreImportError.String, found.PreImportError.String)

	assert.Equal(t, original.TagImport, found.TagImport)
	assert.True(t, found.TagImportStartedAt.Valid)
	assert.WithinDuration(t, original.TagImportStartedAt.Time, found.TagImportStartedAt.Time, 500*time.Millisecond)
	assert.True(t, found.TagImportFinishedAt.Valid)
	assert.WithinDuration(t, original.TagImportFinishedAt.Time, found.TagImportFinishedAt.Time, 500*time.Millisecond)
	assert.True(t, found.TagImportError.Valid)
	assert.Equal(t, original.TagImportError.String, found.TagImportError.String)

	assert.Equal(t, original.BlobImport, found.BlobImport)
	assert.True(t, found.BlobImportStartedAt.Valid)
	assert.WithinDuration(t, original.BlobImportStartedAt.Time, found.BlobImportStartedAt.Time, 500*time.Millisecond)
	assert.True(t, found.BlobImportFinishedAt.Valid)
	assert.WithinDuration(t, original.BlobImportFinishedAt.Time, found.BlobImportFinishedAt.Time, 500*time.Millisecond)
	assert.True(t, found.BlobImportError.Valid)
	assert.Equal(t, original.BlobImportError.String, found.BlobImportError.String)

	assert.Equal(t, original.RepositoriesCount, found.RepositoriesCount)
	assert.Equal(t, original.TagsCount, found.TagsCount)
	assert.Equal(t, original.ManifestsCount, found.ManifestsCount)
	assert.Equal(t, original.BlobsCount, found.BlobsCount)
	assert.Equal(t, original.BlobsSizeBytes, found.BlobsSizeBytes)
	assert.Equal(t, original.StorageDriver, found.StorageDriver)
}

func TestImportStatisticsStore_FindAll(t *testing.T) {
	unloadImportStatisticsFixtures(t)

	s := datastore.NewImportStatisticsStore(suite.db)

	// Create multiple test records
	now := time.Now()

	stats1 := &models.ImportStatistics{
		StartedAt:         now.Add(-2 * time.Hour),
		PreImport:         true,
		TagImport:         false,
		BlobImport:        false,
		RepositoriesCount: 5,
		TagsCount:         25,
		ManifestsCount:    15,
		BlobsCount:        100,
		BlobsSizeBytes:    1024000,
		StorageDriver:     "test driver 1",
	}

	stats2 := &models.ImportStatistics{
		StartedAt:         now.Add(-1 * time.Hour),
		PreImport:         false,
		TagImport:         true,
		BlobImport:        false,
		RepositoriesCount: 10,
		TagsCount:         50,
		ManifestsCount:    30,
		BlobsCount:        200,
		BlobsSizeBytes:    2048000,
		StorageDriver:     "test driver 2",
	}

	stats3 := &models.ImportStatistics{
		StartedAt:         now,
		PreImport:         true,
		TagImport:         true,
		BlobImport:        true,
		RepositoriesCount: 15,
		TagsCount:         75,
		ManifestsCount:    45,
		BlobsCount:        300,
		BlobsSizeBytes:    3072000,
		StorageDriver:     "test driver 3",
	}

	err := s.Create(suite.ctx, stats1)
	require.NoError(t, err)
	err = s.Create(suite.ctx, stats2)
	require.NoError(t, err)
	err = s.Create(suite.ctx, stats3)
	require.NoError(t, err)

	// Find all records
	allStats, err := s.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Len(t, allStats, 3)

	// Should be ordered by ID (ascending)
	assert.Equal(t, stats1.ID, allStats[0].ID)
	assert.Equal(t, stats2.ID, allStats[1].ID)
	assert.Equal(t, stats3.ID, allStats[2].ID)

	// Verify some key fields from each record
	assert.Equal(t, int64(5), allStats[0].RepositoriesCount)
	assert.Equal(t, int64(10), allStats[1].RepositoriesCount)
	assert.Equal(t, int64(15), allStats[2].RepositoriesCount)

	assert.True(t, allStats[0].PreImport)
	assert.False(t, allStats[1].PreImport)
	assert.True(t, allStats[2].PreImport)
}

func TestImportStatisticsStore_FindAll_Empty(t *testing.T) {
	unloadImportStatisticsFixtures(t)

	s := datastore.NewImportStatisticsStore(suite.db)

	allStats, err := s.FindAll(suite.ctx)
	require.NoError(t, err)
	assert.Empty(t, allStats)
}

func TestImportStatisticsStore_FindAll_OrderedByID(t *testing.T) {
	unloadImportStatisticsFixtures(t)

	s := datastore.NewImportStatisticsStore(suite.db)

	baseTime := time.Now()

	// Create records with different start times to verify we're ordering by ID, not by timestamps
	stats := []*models.ImportStatistics{
		{
			StartedAt:         baseTime.Add(-1 * time.Hour), // newest start time
			PreImport:         true,
			RepositoriesCount: 1,
		},
		{
			StartedAt:         baseTime.Add(-3 * time.Hour), // oldest start time
			PreImport:         true,
			RepositoriesCount: 2,
		},
		{
			StartedAt:         baseTime.Add(-2 * time.Hour), // middle start time
			PreImport:         true,
			RepositoriesCount: 3,
		},
	}

	// Create them in order (so IDs will be sequential)
	for _, stat := range stats {
		err := s.Create(suite.ctx, stat)
		require.NoError(t, err)
	}

	// Find all should return in ID order (ascending)
	allStats, err := s.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Len(t, allStats, 3)

	// Should be ordered by ID (ascending), regardless of start times
	assert.Equal(t, int64(1), allStats[0].RepositoriesCount) // first created (lowest ID)
	assert.Equal(t, int64(2), allStats[1].RepositoriesCount) // second created (middle ID)
	assert.Equal(t, int64(3), allStats[2].RepositoriesCount) // third created (highest ID)

	// Verify IDs are in ascending order
	assert.Less(t, allStats[0].ID, allStats[1].ID)
	assert.Less(t, allStats[1].ID, allStats[2].ID)
}

func TestImportStatisticsStore_CreateWithNilOptionalFields(t *testing.T) {
	unloadImportStatisticsFixtures(t)

	s := datastore.NewImportStatisticsStore(suite.db)

	// Create record with minimal required fields and invalid optional fields
	stats := &models.ImportStatistics{
		StartedAt:            time.Now(),
		FinishedAt:           sql.NullTime{Valid: false},
		PreImport:            true,
		PreImportStartedAt:   sql.NullTime{Valid: false},
		PreImportFinishedAt:  sql.NullTime{Valid: false},
		PreImportError:       sql.NullString{Valid: false},
		TagImport:            false,
		TagImportStartedAt:   sql.NullTime{Valid: false},
		TagImportFinishedAt:  sql.NullTime{Valid: false},
		TagImportError:       sql.NullString{Valid: false},
		BlobImport:           false,
		BlobImportStartedAt:  sql.NullTime{Valid: false},
		BlobImportFinishedAt: sql.NullTime{Valid: false},
		BlobImportError:      sql.NullString{Valid: false},
		RepositoriesCount:    0,
		TagsCount:            0,
		ManifestsCount:       0,
		BlobsCount:           0,
		BlobsSizeBytes:       0,
		StorageDriver:        "",
	}

	err := s.Create(suite.ctx, stats)
	require.NoError(t, err)
	require.NotZero(t, stats.ID)

	// Verify we can retrieve it successfully
	found, err := s.FindByID(suite.ctx, stats.ID)
	require.NoError(t, err)
	require.NotNil(t, found)

	// Verify null fields remain invalid
	assert.False(t, found.FinishedAt.Valid)
	assert.False(t, found.PreImportStartedAt.Valid)
	assert.False(t, found.PreImportFinishedAt.Valid)
	assert.False(t, found.PreImportError.Valid)
	assert.False(t, found.TagImportStartedAt.Valid)
	assert.False(t, found.TagImportFinishedAt.Valid)
	assert.False(t, found.TagImportError.Valid)
	assert.False(t, found.BlobImportStartedAt.Valid)
	assert.False(t, found.BlobImportFinishedAt.Valid)
	assert.False(t, found.BlobImportError.Valid)
}
