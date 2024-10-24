//go:build integration

package bbm_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/docker/distribution/registry/bbm"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/stretchr/testify/require"
)

// TestSyncNoBackgroundMigrations tests that no background migrations are run when none are present.
func (s *BackgroundMigrationTestSuite) TestSyncNoBackgroundMigrations() {
	// Ensure the database is clean
	m := newMigrator(s.T(), s.db.DB, nil, nil)
	m.runSchemaMigration(s.T())

	// Start the synchronous background migration process
	err := bbm.NewSyncWorker(s.db).Run(context.Background())
	s.Require().NoError(err)
}

// TestSyncRunSingleFailedJobRecovery tests the recovery of a single failed job in the background migration process.
func (s *BackgroundMigrationTestSuite) TestSyncRunSingleFailedJobRecovery() {
	// Insert background migration fixtures
	up, down := generateSingleFailedJobBBMFixtures()
	m := newMigrator(s.T(), s.db.DB, up, down)
	m.runSchemaMigration(s.T())

	// Expected background migration
	expectedBBM := &models.BackgroundMigration{
		ID:           1,
		Name:         "CopyIDColumnInTestTableToNewIDColumn",
		Status:       models.BackgroundMigrationFinished,
		ErrorCode:    models.BBMErrorCode{},
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}

	expectedBBMJobs := []models.BackgroundMigrationJob{
		{
			ID:        1,
			BBMID:     expectedBBM.ID,
			StartID:   1,
			EndID:     50,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  5,
			ErrorCode: models.BBMErrorCode{},
		}, {
			ID:        2,
			BBMID:     expectedBBM.ID,
			StartID:   51,
			EndID:     93,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  1,
			ErrorCode: models.BBMErrorCode{},
		},
	}

	// Start the synchronous background migration process
	opts := []bbm.SyncWorkerOption{
		bbm.WithWorkMap(map[string]bbm.Work{
			expectedBBM.JobName: {
				Name: expectedBBM.JobName,
				Do:   CopyIDColumnInTestTableToNewIDColumn,
			},
		}),
	}
	err := bbm.NewSyncWorker(s.db, opts...).Run(context.Background())
	s.Require().NoError(err)

	// Assert the background migration runs to completion
	s.requireBBMFinally(expectedBBM.Name, expectedBBM)
	s.requireBBMJobsFinally(expectedBBM.Name, expectedBBMJobs)
	s.requireMigrationLogicComplete(func(db *datastore.DB) (exists bool, err error) {
		query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE %s BETWEEN $1 AND $2)", expectedBBM.TargetTable, targetBBMNewColumn)
		err = db.QueryRow(query, expectedBBM.StartID, expectedBBM.EndID).Scan(&exists)
		return
	})
}

// TestSyncRunExceedMaxRetry tests the behavior when the maximum retry attempts are exceeded.
func (s *BackgroundMigrationTestSuite) TestSyncRunExceedMaxRetry() {
	// Insert background migration fixtures
	up, down := generateSingleFailedJobBBMFixtures()
	m := newMigrator(s.T(), s.db.DB, up, down)
	m.runSchemaMigration(s.T())

	// Expected background migration
	expectedBBM := &models.BackgroundMigration{
		ID:           1,
		Name:         "CopyIDColumnInTestTableToNewIDColumn",
		Status:       models.BackgroundMigrationFailed,
		ErrorCode:    models.BBMErrorCode{},
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}

	expectedBBMJobs := []models.BackgroundMigrationJob{
		{
			ID:        1,
			BBMID:     expectedBBM.ID,
			StartID:   1,
			EndID:     50,
			Status:    models.BackgroundMigrationFailed,
			Attempts:  5,
			ErrorCode: models.BBMErrorCode{},
		}, {
			ID:        2,
			BBMID:     expectedBBM.ID,
			StartID:   51,
			EndID:     93,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  1,
			ErrorCode: models.BBMErrorCode{},
		},
	}
	expectedAttempt := 5

	var callCount int
	// Start the synchronous background migration process
	opts := []bbm.SyncWorkerOption{
		bbm.WithWorkMap(map[string]bbm.Work{
			expectedBBM.JobName: {
				Name: expectedBBM.JobName,
				Do:   doErrorReturn(errAnError, &callCount),
			},
		}),
		bbm.WithSyncMaxJobAttempt(expectedAttempt),
	}
	err := bbm.NewSyncWorker(s.db, opts...).Run(context.Background())
	s.Require().Error(err)

	require.Equal(s.T(), expectedAttempt, callCount)
	// Assert the background migration fails after exceeding max retry attempts
	s.requireBBMFinally(expectedBBM.Name, expectedBBM)
	s.requireBBMJobsFinally(expectedBBM.Name, expectedBBMJobs)
}

// TestSyncRunMultipleFailedJobRecovery tests the recovery of multiple failed jobs in the background migration process.
func (s *BackgroundMigrationTestSuite) TestSyncRunMultipleFailedJobRecovery() {
	// Insert background migration fixtures
	up, down := generateMultipleFailedJobsBBMFixtures()
	m := newMigrator(s.T(), s.db.DB, up, down)
	m.runSchemaMigration(s.T())

	// Expected background migration
	expectedBBM := &models.BackgroundMigration{
		ID:           1,
		Name:         "CopyIDColumnInTestTableToNewIDColumn",
		Status:       models.BackgroundMigrationFinished,
		ErrorCode:    models.BBMErrorCode{},
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}

	expectedBBMJobs := []models.BackgroundMigrationJob{
		{
			ID:        1,
			BBMID:     expectedBBM.ID,
			StartID:   1,
			EndID:     50,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  5,
			ErrorCode: models.BBMErrorCode{},
		}, {
			ID:        2,
			BBMID:     expectedBBM.ID,
			StartID:   51,
			EndID:     93,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  1,
			ErrorCode: models.BBMErrorCode{},
		},
	}

	// Start the synchronous background migration process
	opts := []bbm.SyncWorkerOption{
		bbm.WithWorkMap(map[string]bbm.Work{
			expectedBBM.JobName: {
				Name: expectedBBM.JobName,
				Do:   CopyIDColumnInTestTableToNewIDColumn,
			},
		}),
	}
	err := bbm.NewSyncWorker(s.db, opts...).Run(context.Background())
	s.Require().NoError(err)

	// Assert the background migration runs to completion
	s.requireBBMFinally(expectedBBM.Name, expectedBBM)
	s.requireBBMJobsFinally(expectedBBM.Name, expectedBBMJobs)
	s.requireMigrationLogicComplete(func(db *datastore.DB) (exists bool, err error) {
		query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE %s BETWEEN $1 AND $2)", expectedBBM.TargetTable, targetBBMNewColumn)
		err = db.QueryRow(query, expectedBBM.StartID, expectedBBM.EndID).Scan(&exists)
		return
	})
}

// TestSyncRunFinishedBBM tests the behavior when the background migration is already finished.
func (s *BackgroundMigrationTestSuite) TestSyncRunFinishedBBM() {
	// Insert background migration fixtures
	up, down := generateFinishedBBMFixtures()
	m := newMigrator(s.T(), s.db.DB, up, down)
	m.runSchemaMigration(s.T())

	// Expected background migration
	expectedBBM := &models.BackgroundMigration{
		ID:           1,
		Name:         "CopyIDColumnInTestTableToNewIDColumn",
		Status:       models.BackgroundMigrationFinished,
		ErrorCode:    models.BBMErrorCode{},
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}

	expectedBBMJobs := []models.BackgroundMigrationJob{
		{
			ID:        1,
			BBMID:     expectedBBM.ID,
			StartID:   1,
			EndID:     50,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  5,
			ErrorCode: models.NullErrCode,
		}, {
			ID:        2,
			BBMID:     expectedBBM.ID,
			StartID:   51,
			EndID:     93,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  1,
			ErrorCode: models.NullErrCode,
		},
	}

	// Start the synchronous background migration process
	opts := []bbm.SyncWorkerOption{
		bbm.WithWorkMap(map[string]bbm.Work{
			expectedBBM.JobName: {
				Name: expectedBBM.JobName,
				Do:   CopyIDColumnInTestTableToNewIDColumn,
			},
		}),
	}
	err := bbm.NewSyncWorker(s.db, opts...).Run(context.Background())
	s.Require().NoError(err)

	// Assert the background migration runs to completion
	s.requireBBMFinally(expectedBBM.Name, expectedBBM)
	s.requireBBMJobsFinally(expectedBBM.Name, expectedBBMJobs)
}

// TestSyncRunMultipleActiveBBM tests the behavior when multiple background migrations are active.
func (s *BackgroundMigrationTestSuite) TestSyncRunMultipleActiveBBM() {
	s.testSyncRunMultipleActiveRunningBBM(models.BackgroundMigrationActive)
}

// TestSyncRunSingleActiveBBM tests the behavior when a single background migration is active.
func (s *BackgroundMigrationTestSuite) TestSyncRunSingleActiveBBM() {
	s.testSyncRunSingleActiveRunningBBM(models.BackgroundMigrationActive)
}

// TestSyncRunMultipleRunningBBM tests the behavior when multiple background migrations are running.
func (s *BackgroundMigrationTestSuite) TestSyncRunMultipleRunningBBM() {
	s.testSyncRunMultipleActiveRunningBBM(models.BackgroundMigrationRunning)
}

// TestSyncRunSingleRunningBBM tests the behavior when a single background migration is running.
func (s *BackgroundMigrationTestSuite) TestSyncRunSingleRunningBBM() {
	s.testSyncRunSingleActiveRunningBBM(models.BackgroundMigrationRunning)
}

// testSyncRunSingleActiveRunningBBM is a helper function to test the behavior of a single active or running background migration.
func (s *BackgroundMigrationTestSuite) testSyncRunSingleActiveRunningBBM(status models.BackgroundMigrationStatus) {
	// Insert background migration fixtures
	up, down := generateSingleActiveRunningBBMFixtures(status)
	m := newMigrator(s.T(), s.db.DB, up, down)
	m.runSchemaMigration(s.T())

	// Expected background migration
	expectedBBM := &models.BackgroundMigration{
		ID:           1,
		Name:         "CopyIDColumnInTestTableToNewIDColumn",
		Status:       models.BackgroundMigrationFinished,
		ErrorCode:    models.BBMErrorCode{},
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}

	expectedBBMJobs := []models.BackgroundMigrationJob{
		{
			ID:        1,
			BBMID:     expectedBBM.ID,
			StartID:   1,
			EndID:     50,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  0,
			ErrorCode: models.BBMErrorCode{},
		}, {
			ID:        2,
			BBMID:     expectedBBM.ID,
			StartID:   51,
			EndID:     93,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  0,
			ErrorCode: models.BBMErrorCode{},
		},
	}

	// Start the synchronous background migration process
	opts := []bbm.SyncWorkerOption{
		bbm.WithWorkMap(map[string]bbm.Work{
			expectedBBM.JobName: {
				Name: expectedBBM.JobName,
				Do:   CopyIDColumnInTestTableToNewIDColumn,
			},
		}),
	}
	err := bbm.NewSyncWorker(s.db, opts...).Run(context.Background())
	s.Require().NoError(err)

	// Assert the background migration runs to completion
	s.requireBBMFinally(expectedBBM.Name, expectedBBM)
	s.requireBBMJobsFinally(expectedBBM.Name, expectedBBMJobs)
	s.requireMigrationLogicComplete(func(db *datastore.DB) (exists bool, err error) {
		query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE %s BETWEEN $1 AND $2)", expectedBBM.TargetTable, targetBBMNewColumn)
		err = db.QueryRow(query, expectedBBM.StartID, expectedBBM.EndID).Scan(&exists)
		return
	})
}

// testSyncRunMultipleActiveRunningBBM is a helper function to test the behavior of multiple active or running background migrations.
func (s *BackgroundMigrationTestSuite) testSyncRunMultipleActiveRunningBBM(status models.BackgroundMigrationStatus) {
	// Insert background migration fixtures
	up, down := generateMultipleActiveRunningBBMFixtures(status)
	m := newMigrator(s.T(), s.db.DB, up, down)
	m.runSchemaMigration(s.T())

	// Expected background migration 1
	expectedBBM1 := &models.BackgroundMigration{
		ID:           1,
		Name:         "CopyIDColumnInTestTableToNewIDColumn1",
		Status:       models.BackgroundMigrationFinished,
		ErrorCode:    models.BBMErrorCode{},
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn1",
	}

	expectedBBMJobs1 := []models.BackgroundMigrationJob{
		{
			ID:        1,
			BBMID:     expectedBBM1.ID,
			StartID:   1,
			EndID:     50,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  0,
			ErrorCode: models.BBMErrorCode{},
		}, {
			ID:        2,
			BBMID:     expectedBBM1.ID,
			StartID:   51,
			EndID:     93,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  0,
			ErrorCode: models.BBMErrorCode{},
		},
	}

	// Expected background migration 2
	expectedBBM2 := &models.BackgroundMigration{
		ID:           2,
		Name:         "CopyIDColumnInTestTableToNewIDColumn2",
		Status:       models.BackgroundMigrationFinished,
		ErrorCode:    models.BBMErrorCode{},
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn2",
	}

	expectedBBMJobs2 := []models.BackgroundMigrationJob{
		{
			ID:        3,
			BBMID:     expectedBBM2.ID,
			StartID:   1,
			EndID:     50,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  0,
			ErrorCode: models.BBMErrorCode{},
		}, {
			ID:        4,
			BBMID:     expectedBBM2.ID,
			StartID:   51,
			EndID:     93,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  0,
			ErrorCode: models.BBMErrorCode{},
		},
	}

	// Start the synchronous background migration process
	opts := []bbm.SyncWorkerOption{bbm.WithWorkMap(map[string]bbm.Work{
		expectedBBM1.JobName: {Name: expectedBBM1.JobName, Do: CopyIDColumnInTestTableToNewIDColumn},
		expectedBBM2.JobName: {Name: expectedBBM2.JobName, Do: CopyIDColumnInTestTableToNewIDColumn},
	})}
	err := bbm.NewSyncWorker(s.db, opts...).Run(context.Background())
	s.Require().NoError(err)

	// Assert the background migration runs to completion
	s.requireBBMFinally(expectedBBM1.Name, expectedBBM1)
	s.requireBBMJobsFinally(expectedBBM1.Name, expectedBBMJobs1)
	s.requireBBMFinally(expectedBBM2.Name, expectedBBM2)
	s.requireBBMJobsFinally(expectedBBM2.Name, expectedBBMJobs2)
}

// TestSyncRunAllStateCombinationsBBM tests the behavior of all state combinations in the background migration process.
func (s *BackgroundMigrationTestSuite) TestSyncRunAllStateCombinationsBBM() {
	// Insert background migration fixtures
	up, down := generateMixedStateBBMFixtures()
	m := newMigrator(s.T(), s.db.DB, up, down)
	m.runSchemaMigration(s.T())
	defaultJobName := "CopyIDColumnInTestTableToNewIDColumn"

	// Start the synchronous background migration process
	opts := []bbm.SyncWorkerOption{bbm.WithWorkMap(map[string]bbm.Work{defaultJobName: {Name: defaultJobName, Do: CopyIDColumnInTestTableToNewIDColumn}})}
	worker := bbm.NewSyncWorker(s.db, opts...)
	err := worker.Run(context.Background())
	s.Require().NoError(err)

	// Assert the background migration runs to completion
	s.requireBackgroundMigrations(allMixedFinished)
	// We expect `len(allMixedFinished)-1` completed migrations by the worker because one of the BBMs from `generateMixedStateBBMFixtures()`
	// was already in the `finished` state prior to the worker run
	s.Require().Equal(len(allMixedFinished)-1, worker.FinishedMigrationCount())
}

// requireBackgroundMigrations checks that the background migrations match the expected attributes.
func (s *BackgroundMigrationTestSuite) requireBackgroundMigrations(expected map[models.BackgroundMigration][]models.BackgroundMigrationJob) {
	// Handle nil map
	if expected == nil {
		// Ensure no migrations exist in the database
		qBM := `SELECT COUNT(*) FROM batched_background_migrations`
		var count int
		err := s.db.QueryRow(qBM).Scan(&count)
		s.Require().NoError(err)
		s.Require().Equal(0, count, "Expected no background migrations in the database, but found some.")
		return
	}

	for expectedBM, expectedJobs := range expected {
		// Check the background migration
		qBM := `SELECT
					id,
					name,
					min_value,
					max_value,
					batch_size,
					status,
					job_signature_name,
					table_name,
					column_name,
					failure_error_code
				FROM
					batched_background_migrations
				WHERE
					name = $1`

		rowBM := s.db.QueryRow(qBM, expectedBM.Name)
		actualBM := new(models.BackgroundMigration)
		err := rowBM.Scan(&actualBM.ID, &actualBM.Name, &actualBM.StartID, &actualBM.EndID, &actualBM.BatchSize, &actualBM.Status, &actualBM.JobName, &actualBM.TargetTable, &actualBM.TargetColumn, &actualBM.ErrorCode)
		s.Require().NoError(err)

		require.EqualValues(s.T(), &expectedBM, actualBM)

		// Handle nil map
		if expectedJobs == nil {
			// Ensure no migrations exist in the database
			qBM := `SELECT COUNT(*) FROM batched_background_migration_jobs WHERE background_migration_id = $1`
			var count int
			err := s.db.QueryRow(qBM, actualBM.ID).Scan(&count)
			s.Require().NoError(err)
			s.Require().Equal(0, count, fmt.Sprintf("Expected no background migration jobs for background migration: %d, but found some", actualBM.ID))
			return
		} else {

			// Check the associated jobs
			qJobs := `SELECT
						batched_background_migration_id,
						min_value,
						max_value,
						status,
						attempts
					FROM
						batched_background_migration_jobs
					WHERE
						batched_background_migration_id = $1`

			rows, err := s.db.Query(qJobs, actualBM.ID)
			s.Require().NoError(err)
			defer rows.Close()

			var actualJobs []models.BackgroundMigrationJob
			for rows.Next() {
				job := models.BackgroundMigrationJob{}
				err := rows.Scan(&job.BBMID, &job.StartID, &job.EndID, &job.Status, &job.Attempts)
				s.Require().NoError(err)
				actualJobs = append(actualJobs, job)
			}

			require.ElementsMatch(s.T(), expectedJobs, actualJobs, fmt.Sprintf("Expected background migration jobs for background migration: %d to match", actualBM.ID))
		}
	}
}

// requireBBMFinally checks that a background migration with name `bbmName` is found with the expected attributes `expectedBBM`.
func (s *BackgroundMigrationTestSuite) requireBBMFinally(bbmName string, expectedBBM *models.BackgroundMigration) {
	q := `SELECT
			id,
			name,
			min_value,
			max_value,
			batch_size,
			status,
			job_signature_name,
			table_name,
			column_name,
			failure_error_code
		FROM
			batched_background_migrations
		WHERE
			name = $1`

	row := s.db.QueryRow(q, bbmName)

	bm := new(models.BackgroundMigration)
	if err := row.Scan(&bm.ID, &bm.Name, &bm.StartID, &bm.EndID, &bm.BatchSize, &bm.Status, &bm.JobName, &bm.TargetTable, &bm.TargetColumn, &bm.ErrorCode); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			err = nil
		}
		s.Require().NoError(err)
	}

	require.EqualValues(s.T(), expectedBBM, bm)
}

// generateBBMFixtures creates schema migration records for background migration entries.
func generateBBMFixtures(bbms map[models.BackgroundMigration][]models.BackgroundMigrationJob) ([]string, []string) {
	var up []string
	var down []string

	for bbm, jobs := range bbms {

		up = append(up,
			fmt.Sprintf(`INSERT INTO batched_background_migrations ("id", "name", "min_value", "max_value", "batch_size", "status", "job_signature_name", "table_name", "column_name")
				VALUES ('%d','%s', %d, %d,  %d, %d, '%s', '%s', '%s')`, bbm.ID, bbm.Name, bbm.StartID, bbm.EndID, bbm.BatchSize, bbm.Status, bbm.JobName, bbm.TargetTable, bbm.TargetColumn))

		for _, job := range jobs {
			up = append(up,
				fmt.Sprintf(`INSERT INTO batched_background_migration_jobs ("min_value", "max_value", "status", "attempts", "batched_background_migration_id")
					VALUES ('%d', %d, %d,  %d,%d)`, job.StartID, job.EndID, job.Status, job.Attempts, job.BBMID))
		}

		down = append(down,
			[]string{
				fmt.Sprintf(`DELETE FROM batched_background_migration_jobs WHERE batched_background_migration_id IN (SELECT id FROM batched_background_migrations WHERE name = '%s')`, bbm.Name),
				fmt.Sprintf(`DELETE FROM batched_background_migrations WHERE "name" = '%s'`, bbm.Name),
			}...)
	}

	return up, down
}

// generateSingleFailedJobBBMFixtures generates a single failed background migration with one failed job.
func generateSingleFailedJobBBMFixtures() ([]string, []string) {
	return generateBBMFixtures(map[models.BackgroundMigration][]models.BackgroundMigrationJob{{
		ID:           1,
		Name:         "CopyIDColumnInTestTableToNewIDColumn",
		Status:       models.BackgroundMigrationFailed,
		ErrorCode:    models.BBMErrorCode{},
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}: {
		{
			ID:        1,
			BBMID:     1,
			StartID:   1,
			EndID:     50,
			Status:    models.BackgroundMigrationFailed,
			Attempts:  5,
			ErrorCode: models.UnknownBBMErrorCode,
		}, {
			ID:        2,
			BBMID:     1,
			StartID:   51,
			EndID:     93,
			Status:    models.BackgroundMigrationFinished,
			Attempts:  1,
			ErrorCode: models.BBMErrorCode{},
		},
	}})
}

// generateMultipleFailedJobsBBMFixtures generates a single failed background migration with 2 failed job.
func generateMultipleFailedJobsBBMFixtures() ([]string, []string) {
	return generateBBMFixtures(map[models.BackgroundMigration][]models.BackgroundMigrationJob{{
		ID:           1,
		Name:         "CopyIDColumnInTestTableToNewIDColumn",
		Status:       models.BackgroundMigrationFailed,
		ErrorCode:    models.BBMErrorCode{},
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}: {
		{
			ID:        1,
			BBMID:     1,
			StartID:   1,
			EndID:     50,
			Status:    models.BackgroundMigrationFailed,
			Attempts:  5,
			ErrorCode: models.UnknownBBMErrorCode,
		}, {
			ID:        2,
			BBMID:     1,
			StartID:   51,
			EndID:     93,
			Status:    models.BackgroundMigrationFailed,
			Attempts:  1,
			ErrorCode: models.BBMErrorCode{},
		},
	}})
}

// generateFinishedBBMFixtures generates a finished background migration with two finished jobs.
func generateFinishedBBMFixtures() ([]string, []string) {
	return generateBBMFixtures(map[models.BackgroundMigration][]models.BackgroundMigrationJob{
		{
			ID:           1,
			Name:         "CopyIDColumnInTestTableToNewIDColumn",
			Status:       models.BackgroundMigrationFinished,
			ErrorCode:    models.BBMErrorCode{},
			StartID:      1,
			EndID:        93,
			TargetTable:  targetBBMTable,
			TargetColumn: targetBBMColumn,
			BatchSize:    50,
			JobName:      "CopyIDColumnInTestTableToNewIDColumn",
		}: {
			{
				ID:        1,
				BBMID:     1,
				StartID:   1,
				EndID:     50,
				Status:    models.BackgroundMigrationFinished,
				Attempts:  5,
				ErrorCode: models.NullErrCode,
			}, {
				ID:        2,
				BBMID:     1,
				StartID:   51,
				EndID:     93,
				Status:    models.BackgroundMigrationFinished,
				Attempts:  1,
				ErrorCode: models.NullErrCode,
			},
		},
	})
}

// generateSingleActiveRunningBBMFixtures generates a single active or running background migration with no jobs yet.
func generateSingleActiveRunningBBMFixtures(status models.BackgroundMigrationStatus) ([]string, []string) {
	return generateBBMFixtures(map[models.BackgroundMigration][]models.BackgroundMigrationJob{
		{
			ID:           1,
			Name:         "CopyIDColumnInTestTableToNewIDColumn",
			Status:       status,
			ErrorCode:    models.BBMErrorCode{},
			StartID:      1,
			EndID:        93,
			TargetTable:  targetBBMTable,
			TargetColumn: targetBBMColumn,
			BatchSize:    50,
			JobName:      "CopyIDColumnInTestTableToNewIDColumn",
		}: {},
	})
}

// generateMultipleActiveRunningBBMFixtures generates two active background migrations with no jobs yet.
func generateMultipleActiveRunningBBMFixtures(status models.BackgroundMigrationStatus) ([]string, []string) {
	return generateBBMFixtures(map[models.BackgroundMigration][]models.BackgroundMigrationJob{
		{
			ID:           1,
			Name:         "CopyIDColumnInTestTableToNewIDColumn1",
			Status:       status,
			ErrorCode:    models.BBMErrorCode{},
			StartID:      1,
			EndID:        93,
			TargetTable:  targetBBMTable,
			TargetColumn: targetBBMColumn,
			BatchSize:    50,
			JobName:      "CopyIDColumnInTestTableToNewIDColumn1",
		}: {},
		{
			ID:           2,
			Name:         "CopyIDColumnInTestTableToNewIDColumn2",
			Status:       status,
			ErrorCode:    models.BBMErrorCode{},
			StartID:      1,
			EndID:        93,
			TargetTable:  targetBBMTable,
			TargetColumn: targetBBMColumn,
			BatchSize:    50,
			JobName:      "CopyIDColumnInTestTableToNewIDColumn2",
		}: {},
	})
}

// generateMixedStateBBMFixtures generates background migrations with various states and jobs:
// 1 failed stateBBMwith 2 failed jobs
// 1 active stateBBMwith no jobs yet
// 1 active stateBBMwith 1 finished job
// 1 running stateBBMwith only 1 failed job
// 1 running stateBBMwith only 1 finished job
// 1 finished stateBBMwith 2 finished jobs
func generateMixedStateBBMFixtures() ([]string, []string) {
	return generateBBMFixtures(map[models.BackgroundMigration][]models.BackgroundMigrationJob{
		// Failed state with two failed jobs
		{
			ID:           1,
			Name:         "CopyIDColumnInTestTableToNewIDColumn1",
			Status:       models.BackgroundMigrationFailed,
			ErrorCode:    models.BBMErrorCode{},
			StartID:      1,
			EndID:        93,
			TargetTable:  targetBBMTable,
			TargetColumn: targetBBMColumn,
			BatchSize:    50,
			JobName:      "CopyIDColumnInTestTableToNewIDColumn",
		}: {
			{
				ID:        1,
				BBMID:     1,
				StartID:   1,
				EndID:     50,
				Status:    models.BackgroundMigrationFailed,
				Attempts:  5,
				ErrorCode: models.UnknownBBMErrorCode,
			}, {
				ID:        2,
				BBMID:     1,
				StartID:   51,
				EndID:     93,
				Status:    models.BackgroundMigrationFailed,
				Attempts:  1,
				ErrorCode: models.UnknownBBMErrorCode,
			},
		},

		// Active state with no jobs yet
		{
			ID:           2,
			Name:         "CopyIDColumnInTestTableToNewIDColumn2",
			Status:       models.BackgroundMigrationActive,
			ErrorCode:    models.NullErrCode,
			StartID:      1,
			EndID:        93,
			TargetTable:  targetBBMTable,
			TargetColumn: targetBBMColumn,
			BatchSize:    50,
			JobName:      "CopyIDColumnInTestTableToNewIDColumn",
		}: {},

		// Active state with one finished job
		{
			ID:           3,
			Name:         "CopyIDColumnInTestTableToNewIDColumn3",
			Status:       models.BackgroundMigrationActive,
			ErrorCode:    models.NullErrCode,
			StartID:      1,
			EndID:        93,
			TargetTable:  targetBBMTable,
			TargetColumn: targetBBMColumn,
			BatchSize:    50,
			JobName:      "CopyIDColumnInTestTableToNewIDColumn",
		}: {
			{
				ID:        3,
				BBMID:     3,
				StartID:   1,
				EndID:     50,
				Status:    models.BackgroundMigrationFinished,
				Attempts:  5,
				ErrorCode: models.NullErrCode,
			},
		},

		// Running state with one failed job
		{
			ID:           4,
			Name:         "CopyIDColumnInTestTableToNewIDColumn4",
			Status:       models.BackgroundMigrationRunning,
			ErrorCode:    models.NullErrCode,
			StartID:      1,
			EndID:        93,
			TargetTable:  targetBBMTable,
			TargetColumn: targetBBMColumn,
			BatchSize:    50,
			JobName:      "CopyIDColumnInTestTableToNewIDColumn",
		}: {
			{
				ID:        4,
				BBMID:     4,
				StartID:   1,
				EndID:     50,
				Status:    models.BackgroundMigrationFailed,
				Attempts:  5,
				ErrorCode: models.UnknownBBMErrorCode,
			},
		},

		// Running state with one finished job
		{
			ID:           5,
			Name:         "CopyIDColumnInTestTableToNewIDColumn5",
			Status:       models.BackgroundMigrationRunning,
			ErrorCode:    models.NullErrCode,
			StartID:      1,
			EndID:        93,
			TargetTable:  targetBBMTable,
			TargetColumn: targetBBMColumn,
			BatchSize:    50,
			JobName:      "CopyIDColumnInTestTableToNewIDColumn",
		}: {
			{
				ID:        5,
				BBMID:     5,
				StartID:   1,
				EndID:     50,
				Status:    models.BackgroundMigrationFinished,
				Attempts:  5,
				ErrorCode: models.NullErrCode,
			},
		},

		// Finished state with two finished jobs
		{
			ID:           6,
			Name:         "CopyIDColumnInTestTableToNewIDColumn6",
			Status:       models.BackgroundMigrationFinished,
			ErrorCode:    models.NullErrCode,
			StartID:      1,
			EndID:        93,
			TargetTable:  targetBBMTable,
			TargetColumn: targetBBMColumn,
			BatchSize:    50,
			JobName:      "CopyIDColumnInTestTableToNewIDColumn",
		}: {
			{
				ID:        6,
				BBMID:     6,
				StartID:   1,
				EndID:     50,
				Status:    models.BackgroundMigrationFinished,
				Attempts:  5,
				ErrorCode: models.NullErrCode,
			}, {
				ID:        7,
				BBMID:     6,
				StartID:   51,
				EndID:     93,
				Status:    models.BackgroundMigrationFinished,
				Attempts:  1,
				ErrorCode: models.NullErrCode,
			},
		},
	})
}

var allMixedFinished = map[models.BackgroundMigration][]models.BackgroundMigrationJob{
	// Failed state with two failed jobs
	{
		ID:           1,
		Name:         "CopyIDColumnInTestTableToNewIDColumn1",
		Status:       models.BackgroundMigrationFinished,
		ErrorCode:    models.BBMErrorCode{},
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}: {
		{
			BBMID:    1,
			StartID:  1,
			EndID:    50,
			Status:   models.BackgroundMigrationFinished,
			Attempts: 5,
		}, {
			BBMID:    1,
			StartID:  51,
			EndID:    93,
			Status:   models.BackgroundMigrationFinished,
			Attempts: 1,
		},
	},

	// Active state with no jobs yet
	{
		ID:           2,
		Name:         "CopyIDColumnInTestTableToNewIDColumn2",
		Status:       models.BackgroundMigrationFinished,
		ErrorCode:    models.NullErrCode,
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}: {
		{
			BBMID:    2,
			StartID:  1,
			EndID:    50,
			Status:   models.BackgroundMigrationFinished,
			Attempts: 0,
		}, {
			BBMID:    2,
			StartID:  51,
			EndID:    93,
			Status:   models.BackgroundMigrationFinished,
			Attempts: 0,
		},
	},

	// Active state with one finished job
	{
		ID:           3,
		Name:         "CopyIDColumnInTestTableToNewIDColumn3",
		Status:       models.BackgroundMigrationFinished,
		ErrorCode:    models.NullErrCode,
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}: {
		{
			BBMID:    3,
			StartID:  1,
			EndID:    50,
			Status:   models.BackgroundMigrationFinished,
			Attempts: 5,
		},
		{
			BBMID:    3,
			StartID:  51,
			EndID:    93,
			Status:   models.BackgroundMigrationFinished,
			Attempts: 0,
		},
	},

	// Running state with one failed job
	{
		ID:           4,
		Name:         "CopyIDColumnInTestTableToNewIDColumn4",
		Status:       models.BackgroundMigrationFinished,
		ErrorCode:    models.NullErrCode,
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}: {
		{
			BBMID:    4,
			StartID:  1,
			EndID:    50,
			Status:   models.BackgroundMigrationFinished,
			Attempts: 5,
		},
		{
			BBMID:    4,
			StartID:  51,
			EndID:    93,
			Status:   models.BackgroundMigrationFinished,
			Attempts: 0,
		},
	},

	// Running state with one finished job
	{
		ID:           5,
		Name:         "CopyIDColumnInTestTableToNewIDColumn5",
		Status:       models.BackgroundMigrationFinished,
		ErrorCode:    models.NullErrCode,
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}: {
		{
			BBMID:    5,
			StartID:  1,
			EndID:    50,
			Status:   models.BackgroundMigrationFinished,
			Attempts: 5,
		},
		{
			BBMID:    5,
			StartID:  51,
			EndID:    93,
			Status:   models.BackgroundMigrationFinished,
			Attempts: 0,
		},
	},

	// Finished state with two finished jobs
	{
		ID:           6,
		Name:         "CopyIDColumnInTestTableToNewIDColumn6",
		Status:       models.BackgroundMigrationFinished,
		ErrorCode:    models.NullErrCode,
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}: {
		{
			BBMID:    6,
			StartID:  1,
			EndID:    50,
			Status:   models.BackgroundMigrationFinished,
			Attempts: 5,
		}, {
			BBMID:    6,
			StartID:  51,
			EndID:    93,
			Status:   models.BackgroundMigrationFinished,
			Attempts: 1,
		},
	},
}

// doErrorReturn returns a function that increments the call count and returns the provided error.
func doErrorReturn(err error, callCount *int) func(_ context.Context, _ datastore.Handler, _ string, _ string, _, _, _ int) error {
	return func(_ context.Context, _ datastore.Handler, _ string, _ string, _, _, _ int) error {
		*callCount++
		return err
	}
}
