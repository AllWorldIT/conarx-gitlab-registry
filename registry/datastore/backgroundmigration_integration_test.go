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
