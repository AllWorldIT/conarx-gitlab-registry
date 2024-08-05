//go:build integration

package bbm_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/docker/distribution/registry/bbm"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/migrations"
	mock "github.com/docker/distribution/registry/datastore/migrations/mocks"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

const (
	// targetBBMTable is the table the tests in this file target when running background migrations.
	targetBBMTable = "public.test"
	// targetBBMColumn is the column of `targetBBMTable` that is used for selecting batches for the background migrations in this test file.
	targetBBMColumn = "id"
	// targetBBMNewColumn is the initially empty column that the background migrations CopyIDColumnInTestTableToNewIDColumn tries to fill as part of its execution.
	targetBBMNewColumn = "new_id"
)

var (
	allBBMUpSchemaMigration, allBBMDownSchemaMigration []string
	errAnError                                         = errors.New("an error")
	bbmBaseSchemaMigrationIDs                          = []string{
		"20240604074823_create_batched_background_migrations_table",
		"20240604074846_create_batched_background_migration_jobs_table",
		"20240711175726_add_background_migration_failure_error_code_column",
		"20240711211048_add_background_migration_jobs_indeces",
	}
)

// init loads (from the standard migrator) all necessary schema migrations
// that are required for creating background migrations tables.
func init() {
	stdMigrator := migrations.NewMigrator(nil)

	// temporary slice to collect Down migrations
	var downMigrations []string

	for _, v := range bbmBaseSchemaMigrationIDs {
		if mig := stdMigrator.FindMigrationByID(v); mig != nil {
			allBBMUpSchemaMigration = append(allBBMUpSchemaMigration, mig.Up...)
			// we skip `20240711175726_add_background_migration_failure_error_code_column` down migration
			// because, for the purpose of testing all calls to `down` migrate must be idempotent to facilitate test cleanups. This specific down migration:
			// "ALTER TABLE batched_background_migrations DROP COLUMN IF EXISTS failure_error_code" is not idempotent if the table `batched_background_migrations` does not exist.
			if v == "20240711175726_add_background_migration_failure_error_code_column" {
				continue
			}
			downMigrations = append(downMigrations, mig.Down...)
		}
	}
	// down migration are run in reverse as opposed to up migrations
	reverse(downMigrations)
	allBBMDownSchemaMigration = downMigrations
}

// reverse reverses a slice of strings in place
func reverse(s []string) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

type Migrator struct {
	*mock.MockMigrator
	db *sql.DB
}

// newMigrator returns a schema migrator that is setup to only run the required up and down migrations for a test.
func newMigrator(t *testing.T, db *sql.DB, up, down []string) *Migrator {
	migrator := mock.NewMockMigrator(gomock.NewController(t))

	m := &Migrator{
		migrator,
		db,
	}

	m.upSetup(t, up...)
	m.downSetup(t, down...)

	return m
}

// upSetup sets-up a redirection of a call to "migrate up" to executes only the minimum base queries required for background migration testing.
// Additional queries to be executed after the base queries can be passed in the `extra` argument.
func (m *Migrator) upSetup(t *testing.T, extra ...string) {
	m.EXPECT().Up().Do(upMigrate(t, m.db, extra...)).AnyTimes().Return(0, nil)
}

// downSetup sets-up a redirection of a call to "migrate down" to revert the schema changes added by `upSetup` call.
// Additional queries to be executed before the base queries can be passed in the `extra` argument.
func (m *Migrator) downSetup(t *testing.T, extra ...string) {
	m.EXPECT().Down().Do(downMigrate(t, m.db, extra...)).AnyTimes().Return(0, nil)
}

// runSchemaMigration runs up migrations and register cleanup to perform down migrations
func (m *Migrator) runSchemaMigration(t *testing.T) {
	t.Helper()

	_, err := m.Up()
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := m.Down()
		require.NoError(t, err)
	})
}

// upMigrate is an implementation of an "up" schema migration that focusses
// only on the necessary schema changes for background migrations testing.
func upMigrate(t *testing.T, db *sql.DB, extra ...string) func() {
	return func() {
		base := []string{
			// create the bbm target table (`public.test`) that we will run a migration on
			fmt.Sprintf(
				`CREATE TABLE %s (
				%s smallint NOT NULL GENERATED BY DEFAULT AS IDENTITY, 
				%s bigint)`, targetBBMTable, targetBBMColumn, targetBBMNewColumn),
			// insert some records into the bbm target table (`public.test`)
			fmt.Sprintf(`INSERT INTO %s (%s)
				SELECT * FROM generate_series(1, 90)`, targetBBMTable, targetBBMColumn),
		}

		base = append(append(base, allBBMUpSchemaMigration...), extra...)
		for _, v := range base {
			_, err := db.Exec(v)
			require.NoError(t, err)
		}
	}
}

// downMigrate is an implementation of a "down" schema migration that focusses
// only on the applied schema changes introduced in `upMigrate` for background migrations testing.
func downMigrate(t *testing.T, db *sql.DB, extra ...string) func() {
	return func() {
		base := []string{
			fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", targetBBMTable),
		}

		extra = append(append(extra, allBBMDownSchemaMigration...), base...)
		for _, v := range extra {
			_, err := db.Exec(v)
			require.NoError(t, err)
		}
	}
}

type BackgroundMigrationTestSuite struct {
	suite.Suite
	db         *datastore.DB
	dbMigrator migrations.Migrator
}

func TestBackgroundMigrationTestSuite(t *testing.T) {
	suite.Run(t, &BackgroundMigrationTestSuite{})
}

func (s *BackgroundMigrationTestSuite) SetupSuite() {
	if os.Getenv("REGISTRY_DATABASE_ENABLED") != "true" {
		s.T().Skip("skipping tests because the metadata database is not enabled")
	}

	var err error
	s.db, err = testutil.NewDBFromEnv()
	s.Require().NoError(err)

	// create a schema migrator to use to run test schema migrations
	s.dbMigrator = newMigrator(s.T(), s.db.DB, nil, nil)
}

func (s *BackgroundMigrationTestSuite) TearDownSuite() {
	s.T().Cleanup(func() {
		s.db.Close()
	})
	// Rollback to the standard schema migrations
	var err error
	s.T().Log("rolling back base database migrations")
	_, err = s.dbMigrator.Down()
	s.Require().NoError(err)
}

func (s *BackgroundMigrationTestSuite) SetupTest() {

	// wipe the database and start from a clean slate
	_, err := s.dbMigrator.Down()
	s.Require().NoError(err)
}

// bbmToSchemaMigratorRecord creates a schema migration record for a bbm entry.
func bbmToSchemaMigratorRecord(bbm models.BackgroundMigration) ([]string, []string) {
	return []string{
			fmt.Sprintf(`INSERT INTO batched_background_migrations ("name", "min_value", "max_value", "batch_size", "status", "job_signature_name", "table_name", "column_name")
				VALUES ('%s', %d, %d,  %d, %d, '%s', '%s', '%s')`, bbm.Name, bbm.StartID, bbm.EndID, bbm.BatchSize, models.BackgroundMigrationActive, bbm.JobName, bbm.TargetTable, bbm.TargetColumn),
		},
		[]string{
			fmt.Sprintf(`DELETE FROM batched_background_migration_jobs WHERE batched_background_migration_id IN (SELECT id FROM batched_background_migrations WHERE name = '%s')`, bbm.Name),
			fmt.Sprintf(`DELETE FROM batched_background_migrations WHERE "name" = '%s'`, bbm.Name),
		}
}

// startAsyncBBMWorker helper function to start async background migration worker
func (s *BackgroundMigrationTestSuite) startAsyncBBMWorker(worker *bbm.Worker) {
	doneChan := make(chan struct{})
	s.T().Cleanup(func() { close(doneChan) })
	_, err := worker.ListenForBackgroundMigration(context.Background(), doneChan)
	s.Require().NoError(err)
}

func (s *BackgroundMigrationTestSuite) testAsyncBackgroundMigration(workerCount int) {

	// Expectations:
	expectedBBM := models.BackgroundMigration{
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
			Attempts:  1,
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

	// Setup:
	// In this test the background migration will be copying a column in an existing table to a new column in the same table.
	// To do this, we must run a db schema migration to introduce and trigger the background migration that will do the copying.
	// This is much like how a registry developer will introduce a background migrations to the registry.
	up, down := bbmToSchemaMigratorRecord(expectedBBM)
	m := newMigrator(s.T(), s.db.DB, up, down)
	m.runSchemaMigration(s.T())

	// Start Test:
	// start the BBM process in the background, with the registered work function as specified in the migration record
	var err error
	for i := 0; i < workerCount; i++ {
		var worker *bbm.Worker
		worker, err = bbm.RegisterWork([]bbm.Work{{Name: expectedBBM.JobName, Do: CopyIDColumnInTestTableToNewIDColumn}}, bbm.WithJobInterval(100*time.Millisecond), bbm.WithDB(s.db))
		s.Require().NoError(err)
		s.startAsyncBBMWorker(worker)
	}

	// Test Assertions:
	// assert the background migration runs to completion asynchronously
	s.requireBBMEventually(expectedBBM, 20*time.Second, 100*time.Millisecond)
	s.requireBBMJobsFinally(expectedBBM.Name, expectedBBMJobs)
	s.requireMigrationLogicComplete(func(db *datastore.DB) (exists bool, err error) {
		query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE %s BETWEEN $1 AND $2)", expectedBBM.TargetTable, targetBBMNewColumn)
		err = db.QueryRow(query, expectedBBM.StartID, expectedBBM.EndID).Scan(&exists)
		return
	})
}

// Test_AsyncBackgroundMigration tests that if a background migration is introduced via regular database migration, it will be picked up and executed asynchronously to completion.
func (s *BackgroundMigrationTestSuite) Test_AsyncBackgroundMigration() {
	s.testAsyncBackgroundMigration(1)
}

// Test_AsyncBackgroundMigration_Concurrent tests that if a background migration is introduced via regular database migration, and there are two workers, the migration will still be picked up and executed asynchronously to completion.
func (s *BackgroundMigrationTestSuite) Test_AsyncBackgroundMigration_Concurrent() {
	s.testAsyncBackgroundMigration(2)
}

// Test_AsyncBackgroundMigration_UnknownTable tests that if a background migration is introduced with an unknown table it will be marked as failed.
func (s *BackgroundMigrationTestSuite) Test_AsyncBackgroundMigration_UnknownTable() {

	// Expectations:
	expectedBBM := models.BackgroundMigration{
		ID:           1,
		Name:         "CopyIDColumnInTestTableToNewIDColumn",
		Status:       models.BackgroundMigrationFailed,
		ErrorCode:    models.InvalidTableBBMErrCode,
		StartID:      1,
		EndID:        93,
		TargetTable:  "public.unknown_table",
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}

	s.testAsyncBackgroundMigrationExpected(expectedBBM, nil,
		[]bbm.Work{{Name: expectedBBM.JobName, Do: CopyIDColumnInTestTableToNewIDColumn}}, bbm.WithJobInterval(100*time.Millisecond), bbm.WithDB(s.db))
}

// Test_AsyncBackgroundMigration_UnknownColumn tests that if a background migration is introduced with an unknown column it will be marked as failed.
func (s *BackgroundMigrationTestSuite) Test_AsyncBackgroundMigration_UnknownColumn() {

	// Expectations:
	expectedBBM := models.BackgroundMigration{
		ID:           1,
		Name:         "CopyIDColumnInTestTableToNewIDColumn",
		Status:       models.BackgroundMigrationFailed,
		ErrorCode:    models.InvalidColumnBBMErrCode,
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: "unknown",
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}

	s.testAsyncBackgroundMigrationExpected(expectedBBM, nil,
		[]bbm.Work{{Name: expectedBBM.JobName, Do: CopyIDColumnInTestTableToNewIDColumn}}, bbm.WithJobInterval(100*time.Millisecond), bbm.WithDB(s.db))
}

// Test_AsyncBackgroundMigration_JobSignatureNotFound tests that if a background migration is introduced and the job signature is not registered in the registry it will be marked as failed.
func (s *BackgroundMigrationTestSuite) Test_AsyncBackgroundMigration_JobSignatureNotFound() {

	// Expectations:
	expectedBBM := models.BackgroundMigration{
		ID:           1,
		Name:         "CopyIDColumnInTestTableToNewIDColumn",
		Status:       models.BackgroundMigrationFailed,
		ErrorCode:    models.InvalidJobSignatureBBMErrCode,
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "CopyIDColumnInTestTableToNewIDColumn",
	}

	s.testAsyncBackgroundMigrationExpected(expectedBBM, nil,
		[]bbm.Work{}, bbm.WithJobInterval(100*time.Millisecond), bbm.WithDB(s.db))
}

// Test_AsyncBackgroundMigration_JobExceedsRetryAttempt_Failed tests that if a background migration is introduced and its
// job exceeds the maximum retry-able attempts it will be marked as failed.
func (s *BackgroundMigrationTestSuite) Test_AsyncBackgroundMigration_JobExceedsRetryAttempt_Failed() {

	// Expectations:
	expectedBBM := models.BackgroundMigration{
		ID:           1,
		Name:         "CopyIDColumnInTestTableToNewIDColumn",
		Status:       models.BackgroundMigrationFailed,
		ErrorCode:    models.JobExceedsMaxAttemptBBMErrCode,
		StartID:      1,
		EndID:        93,
		TargetTable:  targetBBMTable,
		TargetColumn: targetBBMColumn,
		BatchSize:    50,
		JobName:      "ErrorOnCall",
	}

	expectedBBMJobs := []models.BackgroundMigrationJob{
		{
			ID:        1,
			BBMID:     expectedBBM.ID,
			StartID:   1,
			EndID:     50,
			Status:    models.BackgroundMigrationFailed,
			Attempts:  2,
			ErrorCode: models.UnknownBBMErrorCode,
		}, {
			ID:        2,
			BBMID:     expectedBBM.ID,
			StartID:   51,
			EndID:     93,
			Status:    models.BackgroundMigrationFailed,
			Attempts:  1,
			ErrorCode: models.UnknownBBMErrorCode,
		},
	}

	DoFunc := func(ctx context.Context, db *datastore.DB, paginationTable string, paginationColumn string, paginationAfter, paginationBefore, limit int) error {
		return errAnError
	}
	s.testAsyncBackgroundMigrationExpected(expectedBBM, expectedBBMJobs,
		[]bbm.Work{{Name: expectedBBM.JobName, Do: DoFunc}}, bbm.WithJobInterval(100*time.Millisecond), bbm.WithDB(s.db), bbm.WithDB(s.db), bbm.WithMaxJobAttempt(2))
}

// CopyIDColumnInTestTableToNewIDColumn is the job function that is executed for a background migration with a `job_signature_name` column value of `CopyIDColumnInTestTableToNewIDColumn`
func CopyIDColumnInTestTableToNewIDColumn(ctx context.Context, db *datastore.DB, paginationTable string, paginationColumn string, paginationAfter, paginationBefore, limit int) error {
	fmt.Printf(`Copying from column id to new_id,  Starting from id %d to %d on column %s`, paginationAfter, paginationBefore, paginationColumn)
	q := fmt.Sprintf(`UPDATE %s SET %s = %s WHERE id >= $1 AND id <= $2`, targetBBMTable, targetBBMNewColumn, targetBBMColumn)
	_, err := db.ExecContext(ctx, q, paginationAfter, paginationBefore)
	if err != nil {
		return err
	}
	return nil
}

// testAsyncBackgroundMigrationExpected sets up and runs a background migration based on the provided parameters and asserts the expected outcome.
func (s *BackgroundMigrationTestSuite) testAsyncBackgroundMigrationExpected(expectedBBM models.BackgroundMigration, expectedBBMJobs []models.BackgroundMigrationJob, work []bbm.Work, opts ...bbm.WorkerOption) {
	// Setup:
	// load the test bbm record via schema migrations
	up, down := bbmToSchemaMigratorRecord(expectedBBM)
	m := newMigrator(s.T(), s.db.DB, up, down)
	m.runSchemaMigration(s.T())

	// Start Test:
	// start the BBM process in the background, with the registered work function as specified in the migration record
	worker, err := bbm.RegisterWork(work, opts...)
	s.Require().NoError(err)
	s.startAsyncBBMWorker(worker)

	// Test Assertions:
	// assert the background migration runs to the expected state
	s.requireBBMEventually(expectedBBM, 30*time.Second, 100*time.Millisecond)
	s.requireBBMJobsFinally(expectedBBM.Name, expectedBBMJobs)

}

// requireBBMEventually checks on every `tick` that a BBM in the database with name `bbmName` eventually matches `expectedBBM` in `waitFor` duration.
func (s *BackgroundMigrationTestSuite) requireBBMEventually(expectedBBM models.BackgroundMigration, waitFor time.Duration, tick time.Duration) {
	s.Require().Eventually(
		func() bool {
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
			row := s.db.QueryRow(q, expectedBBM.Name)

			actualBBM := new(models.BackgroundMigration)
			err := row.Scan(&actualBBM.ID, &actualBBM.Name, &actualBBM.StartID, &actualBBM.EndID, &actualBBM.BatchSize, &actualBBM.Status, &actualBBM.JobName, &actualBBM.TargetTable, &actualBBM.TargetColumn, &actualBBM.ErrorCode)
			if err != nil {
				s.T().Logf("scanning background migration failed: %v", err)
				return false
			} else if expectedBBM != *actualBBM {
				s.T().Logf("expected background migration: %v does not equal actual: %v", expectedBBM, actualBBM)
				return false
			}
			return true

		}, waitFor, tick)
}

// requireBBMJobsFinally checks that the expected array of BBM jobs are present in the database.
func (s *BackgroundMigrationTestSuite) requireBBMJobsFinally(bbmName string, expectedBBMJobs []models.BackgroundMigrationJob) {
	q := `SELECT
			id,
			batched_background_migration_id,
			min_value,
			max_value,
			status,
			attempts,
			failure_error_code
		FROM
			batched_background_migration_jobs
		WHERE
			batched_background_migration_id IN 
			(SELECT id FROM batched_background_migrations WHERE name = $1)`

	rows, err := s.db.Query(q, bbmName)
	s.Require().NoError(err)

	defer rows.Close()

	actualJobs := []models.BackgroundMigrationJob{}
	for rows.Next() {
		bbmj := new(models.BackgroundMigrationJob)
		err = rows.Scan(&bbmj.ID, &bbmj.BBMID, &bbmj.StartID, &bbmj.EndID, &bbmj.Status, &bbmj.Attempts, &bbmj.ErrorCode)
		s.Require().NoError(err)

		actualJobs = append(actualJobs, *bbmj)
	}

	require.ElementsMatch(s.T(), expectedBBMJobs, actualJobs)
}

type validateMigrationFunc func(db *datastore.DB) (bool, error)

// requireMigrationLogicComplete checks that a background migration that was completed has the expected effect. It does this by asserting the user provided validation logic.
func (s *BackgroundMigrationTestSuite) requireMigrationLogicComplete(validationFunc validateMigrationFunc) {
	complete, err := validationFunc(s.db)
	s.Require().NoError(err)
	s.Require().True(complete)
}
