//go:build integration

package datastore_test

import (
	"context"
	"testing"
	"time"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/stretchr/testify/require"
)

func reloadBackgroundMigrationFixtures(tb testing.TB) {
	testutil.ReloadFixtures(tb, suite.db, suite.basePath, testutil.BackgroundMigrationTable)
}

func reloadBackgroundMigrationJobFixtures(tb testing.TB) {
	testutil.ReloadFixtures(tb, suite.db, suite.basePath, testutil.BackgroundMigrationJobsTable)
}

func unloadBackgroundMigrationFixtures(tb testing.TB) {
	require.NoError(tb, testutil.TruncateTables(suite.db, testutil.BackgroundMigrationTable))
}

func TestBackgroundMigrationStore_FindByID(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	m, err := s.FindById(suite.ctx, 1)
	require.NoError(t, err)

	// see testdata/fixtures/batched_background_migrations.sql
	expected := &models.BackgroundMigration{
		ID:           1,
		Name:         "CopyMediaTypesIDToNewIDColumn",
		Status:       models.BackgroundMigrationFinished,
		StartID:      1,
		EndID:        100,
		BatchSize:    20,
		JobName:      "CopyMediaTypesIDToNewIDColumn",
		TargetTable:  "public.media_types",
		TargetColumn: "id",
	}
	require.Equal(t, expected, m)
}

func TestBackgroundMigrationStore_FindByID_NotFound(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	m, err := s.FindById(suite.ctx, 100)
	require.Nil(t, m)
	require.NoError(t, err)
}

func TestBackgroundMigrationStore_FindByName(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	name := "CopyMediaTypesIDToNewIDColumn"
	m, err := s.FindByName(suite.ctx, name)
	require.NoError(t, err)

	// see testdata/fixtures/batched_background_migrations.sql
	expected := &models.BackgroundMigration{
		ID:           1,
		Name:         name,
		Status:       models.BackgroundMigrationFinished,
		StartID:      1,
		EndID:        100,
		BatchSize:    20,
		JobName:      name,
		TargetTable:  "public.media_types",
		TargetColumn: "id",
	}
	require.Equal(t, expected, m)
}

func TestBackgroundMigrationStore_FindByName_NotFound(t *testing.T) {
	s := datastore.NewBackgroundMigrationStore(suite.db)
	m, err := s.FindByName(suite.ctx, "NoNExistentName")
	require.Nil(t, m)
	require.NoError(t, err)
}

func TestBackgroundMigrationStore_FindNext(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	m, err := s.FindNext(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/batched_background_migrations.sql
	expected := &models.BackgroundMigration{
		ID:           4,
		Name:         "CopyRepositoryIDToNewIDColumn2",
		Status:       models.BackgroundMigrationRunning,
		StartID:      1,
		EndID:        16,
		BatchSize:    1,
		JobName:      "CopyRepositoryIDToNewIDColumn2",
		TargetTable:  "public.repositories",
		TargetColumn: "id",
	}
	require.Equal(t, expected, m)
}

func TestBackgroundMigrationStore_FindJobEndFromJobStart(t *testing.T) {
	// schedule a job to run on the "repositories" table
	// see testdata/fixtures/batched_background_migrations.sql and
	// testdata/fixtures/batched_background_migration_jobs.sql
	reloadNamespaceFixtures(t)
	reloadRepositoryFixtures(t)
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	j, err := s.FindJobEndFromJobStart(suite.ctx, "public.repositories", "id", 1, 100, 2)
	require.NoError(t, err)
	require.Equal(t, 2, j)
}

func TestBackgroundMigrationStore_FindJobEndFromJobStart_FewerRecordsThanBatchSizeRemaining(t *testing.T) {
	// schedule a job to run on the "repositories" table
	// see testdata/fixtures/batched_background_migrations.sql and
	// testdata/fixtures/batched_background_migration_jobs.sql
	reloadNamespaceFixtures(t)
	reloadRepositoryFixtures(t)
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)

	// request the next end ID cursor, allowing for a range of up to 50 records
	// between the provided start ID (1) and the returned end ID(100).
	// Note: The "public.repositories" table contains only 17 records (IDs 0 to 16),
	// as specified in testdata/fixtures/repositories.sql.
	endID := 100
	j, err := s.FindJobEndFromJobStart(suite.ctx, "public.repositories", "id", 1, endID, 50)
	require.NoError(t, err)

	// verify that the returned cursor is the end ID argument.
	require.Equal(t, endID, j)
}

func TestBackgroundMigrationStore_FindJobEndFromJobStart_TableNotFound(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	_, err := s.FindJobEndFromJobStart(suite.ctx, "NonExistentTableName", "id", 1, 100, 2)
	require.Error(t, err)
}

func TestBackgroundMigrationStore_SetTotalTupleCount(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)

	// pick an existing BBM from fixtures (id=2)
	const id = 2
	const total int64 = 12345
	require.NoError(t, s.SetTotalTupleCount(suite.ctx, id, total))

	// verify it was persisted
	m, err := s.FindById(suite.ctx, id)
	require.NoError(t, err)
	require.NotNil(t, m)
	require.True(t, m.TotalTupleCount.Valid)
	require.Equal(t, total, m.TotalTupleCount.Int64)
}

func TestBackgroundMigrationStore_EstimateTotalTupleCount_SerialStrategy(t *testing.T) {
	// Create and populate a temporary table without schema qualification (defaults to public)
	_, err := suite.db.Exec(`CREATE TABLE IF NOT EXISTS tmp_bbm_estimate_serial (id INT PRIMARY KEY)`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = suite.db.Exec(`DROP TABLE IF EXISTS tmp_bbm_estimate_serial`)
	})

	// Insert 42 rows
	_, err = suite.db.Exec(`INSERT INTO tmp_bbm_estimate_serial (id)
		SELECT i FROM generate_series(1, 42) AS i`)
	require.NoError(t, err)

	// Ensure statistics are available
	_, err = suite.db.Exec(`ANALYZE tmp_bbm_estimate_serial`)
	require.NoError(t, err)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	bbm := &models.BackgroundMigration{
		TargetTable:      "tmp_bbm_estimate_serial",
		TargetColumn:     "id",
		BatchingStrategy: models.SerialKeySetBatchingBBMStrategy,
	}

	total, err := s.EstimateTotalTupleCount(suite.ctx, bbm)
	require.NoError(t, err)
	// For small tables with ANALYZE, reltuples should match exact row count
	require.EqualValues(t, 42, total)
}

func TestBackgroundMigrationStore_EstimateTotalTupleCount_NullBatching(t *testing.T) {
	// Create and populate a temporary table with a controlled NULL fraction
	_, err := suite.db.Exec(`CREATE TABLE IF NOT EXISTS tmp_bbm_estimate_null (id INT PRIMARY KEY, val INT NULL)`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = suite.db.Exec(`DROP TABLE IF EXISTS tmp_bbm_estimate_null`)
	})

	// Insert 100 rows where 30% have NULL in val
	_, err = suite.db.Exec(`INSERT INTO tmp_bbm_estimate_null (id, val)
		SELECT i,
			CASE WHEN (i % 10) < 3 THEN NULL ELSE 1 END
		FROM generate_series(1, 100) AS i`)
	require.NoError(t, err)

	// Ensure statistics are available (needed for pg_stats.null_frac)
	_, err = suite.db.Exec(`ANALYZE tmp_bbm_estimate_null`)
	require.NoError(t, err)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	bbm := &models.BackgroundMigration{
		TargetTable:      "tmp_bbm_estimate_null",
		TargetColumn:     "val",
		BatchingStrategy: models.NullBatchingBBMStrategy,
	}

	total, err := s.EstimateTotalTupleCount(suite.ctx, bbm)
	require.NoError(t, err)

	// Compare estimator to actual NULL count with small tolerance for pg_stats sampling/rounding
	var actualNulls int64
	err = suite.db.QueryRow(`SELECT COUNT(*) FROM tmp_bbm_estimate_null WHERE val IS NULL`).Scan(&actualNulls)
	require.NoError(t, err)

	if actualNulls < 0 {
		actualNulls = 0
	}
	diff := actualNulls - total
	if diff < 0 {
		diff = -diff
	}
	require.LessOrEqual(t, diff, int64(1), "estimate should be within ±1 of actual NULL count")
}

func TestBackgroundMigrationStore_FindJobEndFromJobStart_ColumnNotFound(t *testing.T) {
	reloadNamespaceFixtures(t)
	reloadRepositoryFixtures(t)
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	_, err := s.FindJobEndFromJobStart(suite.ctx, "public.repositories", "NonExistentColumn", 1, 100, 2)
	require.Error(t, err)
}

func TestBackgroundMigrationStore_FindLastJob(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	bbm := &models.BackgroundMigration{
		ID:           1,
		Name:         "CopyMediaTypesIDToNewIDColumn",
		Status:       models.BackgroundMigrationFinished,
		StartID:      1,
		EndID:        100,
		BatchSize:    20,
		JobName:      "CopyMediaTypesIDToNewIDColumn",
		TargetTable:  "public.media_types",
		TargetColumn: "id",
	}
	j, err := s.FindLastJob(suite.ctx, bbm)
	require.NoError(t, err)

	// see testdata/fixtures/batched_background_migration_jobs.sql
	expected := &models.BackgroundMigrationJob{
		ID:       2,
		BBMID:    bbm.ID,
		Status:   models.BackgroundMigrationFinished,
		StartID:  21,
		EndID:    40,
		Attempts: 1,
	}
	require.Equal(t, expected, j)
}

func TestBackgroundMigrationStore_FindLastJob_NotFound(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	bbm := &models.BackgroundMigration{
		ID: 1001,
	}
	j, err := s.FindLastJob(suite.ctx, bbm)
	require.NoError(t, err)
	require.Nil(t, j)
}

func TestBackgroundMigrationStore_FindJobWithEndID(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	bbm := &models.BackgroundMigration{
		ID: 1,
	}
	j, err := s.FindJobWithEndID(suite.ctx, bbm.ID, 20)
	require.NoError(t, err)

	// see testdata/fixtures/batched_background_migration_jobs.sql
	expected := &models.BackgroundMigrationJob{
		ID:       1,
		BBMID:    bbm.ID,
		Status:   models.BackgroundMigrationFinished,
		StartID:  1,
		EndID:    20,
		Attempts: 1,
	}
	require.Equal(t, expected, j)
}

func TestBackgroundMigrationStore_FindJobWithEndID_BBMNotFound(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	bbm := &models.BackgroundMigration{
		ID: 1001,
	}
	j, err := s.FindJobWithEndID(suite.ctx, bbm.ID, 1)
	require.NoError(t, err)
	require.Nil(t, j)
}

func TestBackgroundMigrationStore_FindJobWithEndID_JobNotFound(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	bbm := &models.BackgroundMigration{
		ID: 1,
	}

	j, err := s.FindJobWithEndID(suite.ctx, bbm.ID, 1001)
	require.NoError(t, err)
	require.Nil(t, j)
}

func TestBackgroundMigrationStore_FindJobWithStatus(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	j, err := s.FindJobWithStatus(suite.ctx, 1, models.BackgroundMigrationFinished)
	require.NoError(t, err)

	// see testdata/fixtures/batched_background_migration_jobs.sql
	expected := &models.BackgroundMigrationJob{
		ID:       1,
		BBMID:    1,
		Status:   models.BackgroundMigrationFinished,
		StartID:  1,
		EndID:    20,
		Attempts: 1,
	}
	require.Equal(t, expected, j)
}

func TestBackgroundMigrationStore_FindJobWithStatus_StatusNotFound(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	bbm := &models.BackgroundMigration{
		ID: 1,
	}
	j, err := s.FindJobWithStatus(suite.ctx, bbm.ID, 99)
	require.NoError(t, err)
	require.Nil(t, j)
}

func TestBackgroundMigrationStore_FindJobWithStatus_BBMNotFound(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	bbm := &models.BackgroundMigration{
		ID: 1001,
	}
	j, err := s.FindJobWithStatus(suite.ctx, bbm.ID, models.BackgroundMigrationFinished)
	require.NoError(t, err)
	require.Nil(t, j)
}

func TestBackgroundMigrationStore_CreateNewJob(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	j := &models.BackgroundMigrationJob{
		BBMID:   1,
		StartID: 1,
		EndID:   2,
	}
	err := s.CreateNewJob(suite.ctx, j)

	require.NoError(t, err)
	require.Equal(t, 1, j.StartID)
	require.Equal(t, 2, j.EndID)
	require.Equal(t, 0, j.Attempts)
	require.NotEmpty(t, j.ID)
	require.Equal(t, models.BackgroundMigrationActive, j.Status)
}

func TestBackgroundMigrationStore_UpdateStatusWithErrorCode(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	errCode := models.UnknownBBMErrorCode
	s := datastore.NewBackgroundMigrationStore(suite.db)
	// see testdata/fixtures/batched_background_migrations.sql
	bm := &models.BackgroundMigration{
		ID:        1,
		Status:    models.BackgroundMigrationFailed,
		ErrorCode: errCode,
	}
	err := s.UpdateStatus(suite.ctx, bm)
	require.NoError(t, err)

	m, err := s.FindById(suite.ctx, 1)
	require.NoError(t, err)

	require.Equal(t, models.BackgroundMigrationFailed, m.Status)
	require.NotNil(t, m.ErrorCode)
	require.Equal(t, errCode, m.ErrorCode)
}

func TestBackgroundMigrationStore_UpdateStatus(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	// see testdata/fixtures/batched_background_migrations.sql
	bm := &models.BackgroundMigration{
		ID:     1,
		Status: models.BackgroundMigrationFailed,
	}
	err := s.UpdateStatus(suite.ctx, bm)
	require.NoError(t, err)

	m, err := s.FindById(suite.ctx, 1)
	require.NoError(t, err)

	require.Equal(t, models.BackgroundMigrationFailed, m.Status)
}

func TestBackgroundMigrationStore_UpdateStatus_NotFound(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	bm := &models.BackgroundMigration{
		ID:     100,
		Status: models.BackgroundMigrationFailed,
	}
	s := datastore.NewBackgroundMigrationStore(suite.db)
	err := s.UpdateStatus(suite.ctx, bm)
	require.EqualError(t, err, "background migration not found")
}

func TestBackgroundMigrationStore_UpdateJobStatus(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	job := &models.BackgroundMigrationJob{
		ID:     1,
		Status: models.BackgroundMigrationFailed,
	}

	s := datastore.NewBackgroundMigrationStore(suite.db)
	err := s.UpdateJobStatus(suite.ctx, job)
	require.NoError(t, err)

	j, err := s.FindJobWithStatus(suite.ctx, 1, models.BackgroundMigrationFailed)
	require.NoError(t, err)

	// see testdata/fixtures/batched_background_migration_jobs.sql
	expected := &models.BackgroundMigrationJob{
		ID:       1,
		BBMID:    1,
		Status:   models.BackgroundMigrationFailed,
		StartID:  1,
		EndID:    20,
		Attempts: 1,
	}
	require.Equal(t, expected, j)
}

func TestBackgroundMigrationStore_UpdateJobStatusWithErrorCode(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)
	errCode := models.UnknownBBMErrorCode
	job := &models.BackgroundMigrationJob{
		ID:        1,
		Status:    models.BackgroundMigrationFailed,
		ErrorCode: errCode,
	}

	s := datastore.NewBackgroundMigrationStore(suite.db)
	err := s.UpdateJobStatus(suite.ctx, job)
	require.NoError(t, err)

	j, err := s.FindJobWithStatus(suite.ctx, 1, models.BackgroundMigrationFailed)
	require.NoError(t, err)

	// see testdata/fixtures/batched_background_migration_jobs.sql
	expected := &models.BackgroundMigrationJob{
		ID:        1,
		BBMID:     1,
		Status:    models.BackgroundMigrationFailed,
		StartID:   1,
		EndID:     20,
		Attempts:  1,
		ErrorCode: errCode,
	}
	require.Equal(t, expected, j)
}

func TestBackgroundMigrationStore_UpdateJobStatus_NotFound(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	job := &models.BackgroundMigrationJob{
		ID:     100,
		Status: models.BackgroundMigrationFailed,
	}

	s := datastore.NewBackgroundMigrationStore(suite.db)
	err := s.UpdateJobStatus(suite.ctx, job)
	require.EqualError(t, err, "background migration job not found")
}

func TestBackgroundMigrationStore_IncrementJobAttempts(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	err := s.IncrementJobAttempts(suite.ctx, 3)
	require.NoError(t, err)

	j, err := s.FindJobWithStatus(suite.ctx, 2, models.BackgroundMigrationActive)
	require.NoError(t, err)

	// see testdata/fixtures/batched_background_migration_jobs.sql
	expected := &models.BackgroundMigrationJob{
		ID:       3,
		BBMID:    2,
		Status:   models.BackgroundMigrationActive,
		StartID:  1,
		EndID:    40,
		Attempts: 2, // attempt incremented from 1 to 2
	}
	require.Equal(t, expected, j)
}

func TestBackgroundMigrationStore_IncrementJobAttempts_NotFound(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	err := s.IncrementJobAttempts(suite.ctx, 100)
	require.EqualError(t, err, "background migration job not found")
}

func TestBackgroundMigrationStore_Lock(t *testing.T) {
	// use transactions for obtaining pg transaction-level advisory locks.

	// obtain the lock in the first transaction
	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	s := datastore.NewBackgroundMigrationStore(tx)
	require.NoError(t, s.Lock(suite.ctx))

	// try to obtain the lock in a second transaction (while lock is locked by the first transaction)
	tx2, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx2.Rollback()
	s2 := datastore.NewBackgroundMigrationStore(tx2)
	require.ErrorIs(t, datastore.ErrBackgroundMigrationLockInUse, s2.Lock(suite.ctx))
}

func TestBackgroundMigrationStore_ExistsTable(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	ok, err := s.ExistsTable(suite.ctx, "public", "repositories")
	require.NoError(t, err)
	require.True(t, ok)
}

func TestBackgroundMigrationStore_ExistsTable_NotFound(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	ok, err := s.ExistsTable(suite.ctx, "public", "does_not_exist")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestBackgroundMigrationStore_ValidateMigrationColumn(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	ok, err := s.ExistsColumn(suite.ctx, "public", "repositories", "id")
	require.NoError(t, err)
	require.True(t, ok)
}

func TestBackgroundMigrationStore_ValidateMigrationColumn_NotFound(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)
	reloadBackgroundMigrationJobFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	ok, err := s.ExistsColumn(suite.ctx, "public", "repositories", "does_not_exist")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestBackgroundMigrationStore_FindAll(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	bb, err := s.FindAll(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/batched_background_migrations.sql
	expected := models.BackgroundMigrations{
		{
			ID:           1,
			Name:         "CopyMediaTypesIDToNewIDColumn",
			Status:       models.BackgroundMigrationFinished,
			StartID:      1,
			EndID:        100,
			BatchSize:    20,
			JobName:      "CopyMediaTypesIDToNewIDColumn",
			TargetTable:  "public.media_types",
			TargetColumn: "id",
		},
		{
			ID:           2,
			Name:         "CopyBlobIDToNewIDColumn",
			Status:       models.BackgroundMigrationActive,
			StartID:      5,
			EndID:        10,
			BatchSize:    1,
			JobName:      "CopyBlobIDToNewIDColumn",
			TargetTable:  "public.blobs",
			TargetColumn: "id",
		},
		{
			ID:           3,
			Name:         "CopyRepositoryIDToNewIDColumn",
			Status:       models.BackgroundMigrationActive,
			StartID:      1,
			EndID:        16,
			BatchSize:    1,
			JobName:      "CopyRepositoryIDToNewIDColumn",
			TargetTable:  "public.repositories",
			TargetColumn: "id",
		},
		{
			ID:           4,
			Name:         "CopyRepositoryIDToNewIDColumn2",
			Status:       models.BackgroundMigrationRunning,
			StartID:      1,
			EndID:        16,
			BatchSize:    1,
			JobName:      "CopyRepositoryIDToNewIDColumn2",
			TargetTable:  "public.repositories",
			TargetColumn: "id",
		},
		{
			ID:           5,
			Name:         "CopyRepositoryIDToNewIDColumn3",
			Status:       models.BackgroundMigrationPaused,
			StartID:      1,
			EndID:        16,
			BatchSize:    1,
			JobName:      "CopyRepositoryIDToNewIDColumn3",
			TargetTable:  "public.repositories",
			TargetColumn: "id",
		},
	}

	require.Equal(t, expected, bb)
}

func TestBackgroundMigrationStore_FindAll_NotFound(t *testing.T) {
	unloadBackgroundMigrationFixtures(t)
	s := datastore.NewBackgroundMigrationStore(suite.db)
	bb, err := s.FindAll(suite.ctx)
	require.Empty(t, bb)
	require.NoError(t, err)
}

func TestBackgroundMigrationStore_Pause(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	err := s.Pause(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/batched_background_migrations.sql
	expected := models.BackgroundMigrations{
		{
			ID:           1,
			Name:         "CopyMediaTypesIDToNewIDColumn",
			Status:       models.BackgroundMigrationFinished,
			StartID:      1,
			EndID:        100,
			BatchSize:    20,
			JobName:      "CopyMediaTypesIDToNewIDColumn",
			TargetTable:  "public.media_types",
			TargetColumn: "id",
		},
		{
			ID:           2,
			Name:         "CopyBlobIDToNewIDColumn",
			Status:       models.BackgroundMigrationPaused,
			StartID:      5,
			EndID:        10,
			BatchSize:    1,
			JobName:      "CopyBlobIDToNewIDColumn",
			TargetTable:  "public.blobs",
			TargetColumn: "id",
		},
		{
			ID:           3,
			Name:         "CopyRepositoryIDToNewIDColumn",
			Status:       models.BackgroundMigrationPaused,
			StartID:      1,
			EndID:        16,
			BatchSize:    1,
			JobName:      "CopyRepositoryIDToNewIDColumn",
			TargetTable:  "public.repositories",
			TargetColumn: "id",
		},
		{
			ID:           4,
			Name:         "CopyRepositoryIDToNewIDColumn2",
			Status:       models.BackgroundMigrationPaused,
			StartID:      1,
			EndID:        16,
			BatchSize:    1,
			JobName:      "CopyRepositoryIDToNewIDColumn2",
			TargetTable:  "public.repositories",
			TargetColumn: "id",
		},
		{
			ID:           5,
			Name:         "CopyRepositoryIDToNewIDColumn3",
			Status:       models.BackgroundMigrationPaused,
			StartID:      1,
			EndID:        16,
			BatchSize:    1,
			JobName:      "CopyRepositoryIDToNewIDColumn3",
			TargetTable:  "public.repositories",
			TargetColumn: "id",
		},
	}

	var actual models.BackgroundMigrations
	actual, err = s.FindAll(suite.ctx)
	require.NoError(t, err)

	require.Equal(t, expected, actual)
}

func TestBackgroundMigrationStore_FindNextByStatus(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	status := models.BackgroundMigrationActive
	bbm, err := s.FindNextByStatus(suite.ctx, status)
	require.NoError(t, err)

	expected := &models.BackgroundMigration{
		ID:           2,
		Name:         "CopyBlobIDToNewIDColumn",
		Status:       models.BackgroundMigrationActive,
		StartID:      5,
		EndID:        10,
		BatchSize:    1,
		JobName:      "CopyBlobIDToNewIDColumn",
		TargetTable:  "public.blobs",
		TargetColumn: "id",
	}
	require.Equal(t, expected, bbm)
}

func TestBackgroundMigrationStore_FindNextByStatus_NotFound(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	status := models.BackgroundMigrationFailed
	bbm, err := s.FindNextByStatus(suite.ctx, status)
	require.NoError(t, err)
	require.Nil(t, bbm)
}

func TestBackgroundMigrationStore_Resume(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	bbm := &models.BackgroundMigration{
		ID: 5,
	}
	err := s.Resume(suite.ctx)
	require.NoError(t, err)

	// Verify the status has been updated to running
	bbm, err = s.FindById(suite.ctx, bbm.ID)
	require.NoError(t, err)
	require.Equal(t, models.BackgroundMigrationActive, bbm.Status)
}

func TestBackgroundMigrationStore_SyncLock_Timeout(t *testing.T) {
	// use transactions for obtaining pg transaction-level advisory locks.

	// obtain the lock in the first transaction
	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	s := datastore.NewBackgroundMigrationStore(tx)
	require.NoError(t, s.Lock(suite.ctx))

	// try to obtain the lock in a second transaction (while lock is locked by the first transaction)
	tx2, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx2.Rollback()
	s2 := datastore.NewBackgroundMigrationStore(tx2)
	timeoutCtx, cncl := context.WithTimeout(suite.ctx, 100*time.Millisecond)
	defer cncl()
	err = s2.SyncLock(timeoutCtx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestBackgroundMigrationStore_Multiple_SyncLock(t *testing.T) {
	// First lock should succeed
	tx1, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx1.Rollback()
	s1 := datastore.NewBackgroundMigrationStore(tx1)
	timeoutCtx, cncl := context.WithTimeout(suite.ctx, 100*time.Millisecond)
	defer cncl()
	require.NoError(t, s1.SyncLock(timeoutCtx))

	// Second lock should fail
	tx2, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx2.Rollback()
	s2 := datastore.NewBackgroundMigrationStore(tx2)
	timeoutCtx2, cncl2 := context.WithTimeout(suite.ctx, 100*time.Millisecond)
	defer cncl2()
	err = s2.SyncLock(timeoutCtx2)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Release the first lock
	require.NoError(t, tx1.Rollback())

	// Now the lock should be available again
	tx3, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx3.Rollback()
	s3 := datastore.NewBackgroundMigrationStore(tx3)
	err = s3.SyncLock(suite.ctx)
	require.NoError(t, err)
}

func TestBackgroundMigrationStore_SyncLock(t *testing.T) {
	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	s := datastore.NewBackgroundMigrationStore(tx)
	require.NoError(t, s.SyncLock(suite.ctx))
}

func TestBackgroundMigrationStore_CountByStatus(t *testing.T) {
	reloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	expectedStatusCount := map[models.BackgroundMigrationStatus]int{
		models.BackgroundMigrationActive:   2,
		models.BackgroundMigrationFinished: 1,
		models.BackgroundMigrationPaused:   1,
		models.BackgroundMigrationRunning:  1,
	}
	statusCount, err := s.CountByStatus(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, expectedStatusCount, statusCount)
}

func TestBackgroundMigrationStore_CountByStatus_NotFound(t *testing.T) {
	unloadBackgroundMigrationFixtures(t)

	s := datastore.NewBackgroundMigrationStore(suite.db)
	statusCount, err := s.CountByStatus(suite.ctx)
	require.Empty(t, statusCount)
	require.NoError(t, err)
}

func TestBackgroundMigrationStore_GetPendingWALCount(t *testing.T) {
	// We won't be able to mock the varying count response for different WAL segment lag
	// because `pg_stat_archiver` is a system view that provides read-only statistics.
	s := datastore.NewBackgroundMigrationStore(suite.db)
	count, err := s.GetPendingWALCount(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, -1, count)
}

func TestBackgroundMigrationStore_HasNullIndex(t *testing.T) {
	// Create a temporary table in the default schema (no schema prefix in identifier)
	_, err := suite.db.Exec(`CREATE TABLE IF NOT EXISTS tmp_nullidx_test (id INT PRIMARY KEY, val INT NULL)`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = suite.db.Exec(`DROP TABLE IF EXISTS tmp_nulltest`)
	})

	s := datastore.NewBackgroundMigrationStore(suite.db)

	// Expect false when no suitable index exists
	hasNullIdx, err := s.HasNullIndex(suite.ctx, "tmp_nullidx_test", "val")
	require.NoError(t, err)
	require.False(t, hasNullIdx)

	// Create index that is not suitable for null batching
	_, err = suite.db.Exec(`CREATE INDEX idx_bad ON tmp_nullidx_test (val) where id > 10`)
	require.NoError(t, err)

	// Still expect false
	hasNullIdx, err = s.HasNullIndex(suite.ctx, "tmp_nullidx_test", "val")
	require.NoError(t, err)
	require.False(t, hasNullIdx)

	// Create index that is suitable for null batching
	_, err = suite.db.Exec(`CREATE INDEX idx_good ON tmp_nullidx_test (val) where val is null`)
	require.NoError(t, err)

	hasNullIdx, err = s.HasNullIndex(suite.ctx, "tmp_nullidx_test", "val")
	require.NoError(t, err)
	require.True(t, hasNullIdx)
}

func TestBackgroundMigrationStore_HasNullValues_SimpleTable(t *testing.T) {
	// Create a temporary table in the default schema (no schema prefix in identifier)
	_, err := suite.db.Exec(`CREATE TABLE IF NOT EXISTS tmp_nulltest (id INT PRIMARY KEY, val INT NULL)`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = suite.db.Exec(`DROP TABLE IF EXISTS tmp_nulltest`)
	})

	// Insert rows with one NULL
	_, err = suite.db.Exec(`INSERT INTO tmp_nulltest (id, val) VALUES (1, NULL), (2, 5)`)
	require.NoError(t, err)

	s := datastore.NewBackgroundMigrationStore(suite.db)

	// Expect true when NULL exists
	hasNulls, err := s.HasNullValues(suite.ctx, "tmp_nulltest", "val")
	require.NoError(t, err)
	require.True(t, hasNulls)

	// Flip NULL to non-null
	_, err = suite.db.Exec(`UPDATE tmp_nulltest SET val = 1 WHERE id = 1`)
	require.NoError(t, err)

	// Expect false when no NULL remains
	hasNulls, err = s.HasNullValues(suite.ctx, "tmp_nulltest", "val")
	require.NoError(t, err)
	require.False(t, hasNulls)
}

func TestBackgroundMigrationStore_HasNullValues_InvalidIdentifier(t *testing.T) {
	s := datastore.NewBackgroundMigrationStore(suite.db)
	// Table name with a quote should be rejected by identifier validation
	_, err := s.HasNullValues(suite.ctx, `bad"name`, "val")
	require.Error(t, err)
}
