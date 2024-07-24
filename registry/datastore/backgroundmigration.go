package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/docker/distribution/registry/datastore/metrics"
	"github.com/docker/distribution/registry/datastore/models"
)

var (
	// ErrBackgroundMigrationLockInUse is returned the Background Migration distributed lock could not be obtained.
	// This is most likely to occur when the lock has not been released by another process.
	ErrBackgroundMigrationLockInUse = errors.New("background migration lock is already taken")
	// ErrUnknownColumn is returned when a column referenced in a background migration is unknown.
	ErrUnknownColumn = errors.New("unknown column reference in background migration record")
	// ErrUnknownTable is returned when a table referenced in a background migration is unknown.
	ErrUnknownTable = errors.New("unknown table reference in background migration record")
)

// advisoryLockKey is a key that uniquely identifies the background migration lock acquired by the migration process.
const advisoryLockKey = 1

// BackgroundMigrationStore is the interface that a background migration store should conform to.
type BackgroundMigrationStore interface {
	// FindById find a BackgroundMigration with id `id`.
	FindById(ctx context.Context, id int) (*models.BackgroundMigration, error)
	// FindByName find a Background Migration with name `name`.
	FindByName(ctx context.Context, name string) (*models.BackgroundMigration, error)
	// FindNext finds the first active or running Background Migration by ascending order on the Background Migration `id“ column.
	FindNext(ctx context.Context) (*models.BackgroundMigration, error)
	// FindJobEndFromJobStart finds the end cursor for a job based on the start of the job and the batch size of the Background Migration the job is associated with.
	FindJobEndFromJobStart(ctx context.Context, table string, column string, start, last, batchSize int) (int, error)
	// FindLastJob returns the last job created for a Background Migration.
	FindLastJob(ctx context.Context, backgroundMigration *models.BackgroundMigration) (*models.BackgroundMigrationJob, error)
	// FindJobWithEndID returns any jobs with the end id `endID`.
	FindJobWithEndID(ctx context.Context, bmID, endID int) (*models.BackgroundMigrationJob, error)
	// FindJobWithStatus returns any jobs with the status `status`.
	FindJobWithStatus(ctx context.Context, bmID int, status models.BackgroundMigrationStatus) (*models.BackgroundMigrationJob, error)
	// ExistsTable validates that the table name exists in the datastore's catalog.
	ExistsTable(ctx context.Context, schema, table string) (bool, error)
	// ExistsColumn validates that the table's column exists in the datastore's catalog.
	ExistsColumn(ctx context.Context, schema, table, column string) (bool, error)
	// CreateNewJob creates a new job entry in the `batched_background_migration_jobs` table.
	CreateNewJob(ctx context.Context, newJob *models.BackgroundMigrationJob) error
	// UpdateStatus updates the `status` and `failure_error_code` (if necessary) of a Background Migration.
	UpdateStatus(ctx context.Context, bbm *models.BackgroundMigration) error
	// IncrementJobAttempts updates the number of attempts of a Background Migration job to `attempts` by 1.
	IncrementJobAttempts(ctx context.Context, jobID int) error
	// UpdateJobStatus updates the `status` and `failure_error_code` (if necessary) of a Background Migration job.
	UpdateJobStatus(ctx context.Context, job *models.BackgroundMigrationJob) error
	// Lock sets a lock to prevent new Background Migration jobs from running.
	Lock(ctx context.Context) error
	// ValidateMigrationTableAndColumn asserts that the column and table exists in the database.
	ValidateMigrationTableAndColumn(ctx context.Context, tableWithSchema, column string) error
}

// NewBackgroundMigrationStore builds a new backgroundMigrationStore.
func NewBackgroundMigrationStore(db Queryer) *backgroundMigrationStore {
	return &backgroundMigrationStore{db: db}
}

// backgroundMigrationStore is the concrete implementation of a BackgroundMigrationStore.
type backgroundMigrationStore struct {
	// db can be either a *sql.DB or *sql.Tx
	db Queryer
}

// ExistsTable validates that the table name exists in the datastore's catalog.
func (bms *backgroundMigrationStore) ExistsTable(ctx context.Context, schema, table string) (bool, error) {
	defer metrics.InstrumentQuery("bbm_exists_table")()

	q := `SELECT
			EXISTS (
				SELECT
					1
				FROM
					pg_tables
				WHERE
					schemaname = $1
					AND tablename = $2)`

	var ok bool
	err := bms.db.QueryRowContext(ctx, q, schema, table).Scan(&ok)
	if err != nil {
		if err == sql.ErrNoRows {
			return ok, nil
		}
		return ok, fmt.Errorf("validating batched background migration table name: %w", err)
	}

	return ok, nil
}

// ExistsColumn validates that the table's column exists in the datastore's catalog.
func (bms *backgroundMigrationStore) ExistsColumn(ctx context.Context, schema, table, column string) (bool, error) {
	defer metrics.InstrumentQuery("bbm_exists_column")()

	q := `SELECT
			EXISTS (
				SELECT
					1
				FROM
					information_schema.columns
				WHERE
					table_schema = $1
					AND table_name = $2
					AND column_name = $3)`

	var ok bool
	err := bms.db.QueryRowContext(ctx, q, schema, table, column).Scan(&ok)
	if err != nil {
		if err == sql.ErrNoRows {
			return ok, nil
		}
		return ok, fmt.Errorf("validating batched background migration column name: %w", err)
	}

	return ok, nil
}

// FindJobEndFromJobStart finds the end cursor for a job based on the start of the job and the batch size of the Background Migration the job is associated with.
func (bms *backgroundMigrationStore) FindJobEndFromJobStart(ctx context.Context, table string, column string, start, last, batchSize int) (int, error) {
	err := bms.ValidateMigrationTableAndColumn(ctx, table, column)
	if err != nil {
		return 0, err
	}

	if start+batchSize >= last {
		return last, nil
	}

	defer metrics.InstrumentQuery("bbm_find_job_end_from_job_start")()

	q := fmt.Sprintf(`SELECT %s FROM %s WHERE %s >= $1 AND %s <= $2 ORDER BY %s ASC LIMIT 1 OFFSET $3 - 1`,
		column, table, column, column, column)

	var end int
	err = bms.db.QueryRowContext(ctx, q, start, last, batchSize).Scan(&end)
	if err != nil {
		if err == sql.ErrNoRows {
			return start, nil
		}
		return end, fmt.Errorf("calculating batched background migration job end id: %w", err)
	}

	return end, nil
}

// FindLastJob returns the last job created for a Background Migration.
func (bms *backgroundMigrationStore) FindLastJob(ctx context.Context, backgroundMigration *models.BackgroundMigration) (*models.BackgroundMigrationJob, error) {
	defer metrics.InstrumentQuery("bbm_find_last_job")()

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
				batched_background_migration_id = $1
			ORDER BY
				id DESC
			LIMIT 1`

	row := bms.db.QueryRowContext(ctx, q, backgroundMigration.ID)

	return scanBackgroundMigrationJob(row)
}

// FindNext finds the first active or running Background Migration by ascending order on the Background Migration `id“ column.
func (bms *backgroundMigrationStore) FindNext(ctx context.Context) (*models.BackgroundMigration, error) {
	defer metrics.InstrumentQuery("bbm_find_next")()

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
			status = $1
			OR status = $2
		ORDER BY
			CASE WHEN status = $2 THEN 
				0 
			ELSE 
				1
			END,
			id ASC
		LIMIT 1`

	row := bms.db.QueryRowContext(ctx, q, models.BackgroundMigrationActive, models.BackgroundMigrationRunning)

	return scanBackgroundMigration(row)
}

// FindJobWithEndID returns any jobs with the end id `endID`.
func (bms *backgroundMigrationStore) FindJobWithEndID(ctx context.Context, bmID, endID int) (*models.BackgroundMigrationJob, error) {
	defer metrics.InstrumentQuery("bbm_find_job_with_end_id")()

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
			batched_background_migration_id = $1
			AND max_value = $2
		LIMIT 1`

	row := bms.db.QueryRowContext(ctx, q, bmID, endID)

	return scanBackgroundMigrationJob(row)
}

// FindJobWithStatus returns any jobs with the status `status`.
func (bms *backgroundMigrationStore) FindJobWithStatus(ctx context.Context, bmID int, status models.BackgroundMigrationStatus) (*models.BackgroundMigrationJob, error) {
	defer metrics.InstrumentQuery("bbm_find_job_with_status")()

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
			batched_background_migration_id = $1
			AND status = $2
		ORDER BY
			id ASC
		LIMIT 1`

	row := bms.db.QueryRowContext(ctx, q, bmID, status)

	return scanBackgroundMigrationJob(row)
}

// FindById find a BackgroundMigration with id `id`.
func (bms *backgroundMigrationStore) FindById(ctx context.Context, id int) (*models.BackgroundMigration, error) {
	defer metrics.InstrumentQuery("bbm_find_by_id")()

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
			id = $1`

	row := bms.db.QueryRowContext(ctx, q, id)

	return scanBackgroundMigration(row)
}

// FindByName find a Background Migration with name `name`.
func (bms *backgroundMigrationStore) FindByName(ctx context.Context, name string) (*models.BackgroundMigration, error) {
	defer metrics.InstrumentQuery("bbm_find_by_name")()

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

	row := bms.db.QueryRowContext(ctx, q, name)

	return scanBackgroundMigration(row)
}

// CreateNewJob creates a new job entry in the `batched_background_migration_jobs` table.
func (bms *backgroundMigrationStore) CreateNewJob(ctx context.Context, newJob *models.BackgroundMigrationJob) error {
	defer metrics.InstrumentQuery("bbm_create_new_job")()

	q := `INSERT INTO batched_background_migration_jobs (batched_background_migration_id, min_value, max_value)
			VALUES ($1, $2, $3)
		RETURNING
			id, status, attempts`
	row := bms.db.QueryRowContext(ctx, q, newJob.BBMID, newJob.StartID, newJob.EndID)
	if err := row.Scan(&newJob.ID, &newJob.Status, &newJob.Attempts); err != nil {
		return fmt.Errorf("creating batched background migration job: %w", err)
	}
	return nil
}

// UpdateStatus updates the `status` and `failure_error_code` (if necessary) of a Background Migration.
func (bms *backgroundMigrationStore) UpdateStatus(ctx context.Context, bbm *models.BackgroundMigration) error {
	defer metrics.InstrumentQuery("bbm_update_status")()

	// Update both status and failure_error_code
	q := `UPDATE
    		batched_background_migrations
		SET
			status = $1,
			failure_error_code = $2,
			updated_at = now()
		WHERE
			id = $3
		RETURNING
			status,
			failure_error_code`
	row := bms.db.QueryRowContext(ctx, q, bbm.Status, bbm.ErrorCode, bbm.ID)
	if err := row.Scan(&bbm.Status, &bbm.ErrorCode); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("background migration not found")
		}
		return fmt.Errorf("updating background migration status: %w", err)
	}
	return nil
}

// IncrementJobAttempts updates the number of attempts of a BackgroundMigration job by 1.
func (bms *backgroundMigrationStore) IncrementJobAttempts(ctx context.Context, jobID int) error {
	defer metrics.InstrumentQuery("bbm_increment_job_attempts")()

	q := `UPDATE batched_background_migration_jobs SET attempts = attempts + 1, updated_at = now() WHERE id = $1 RETURNING attempts`
	row := bms.db.QueryRowContext(ctx, q, jobID)

	var attempts int
	if err := row.Scan(&attempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("background migration job not found")
		}
		return fmt.Errorf("updating background migration job attempts: %w", err)
	}
	return nil
}

// UpdateJobStatus updates the `status` and `failure_error_code` (if necessary) of a Background Migration job.
func (bms *backgroundMigrationStore) UpdateJobStatus(ctx context.Context, job *models.BackgroundMigrationJob) error {
	defer metrics.InstrumentQuery("bbm_update_job_status")()

	// Update both status and failure_error_code
	q := `UPDATE
			batched_background_migration_jobs
		SET
			status = $1,
			failure_error_code = $2,
			updated_at = now()
		WHERE
			id = $3
		RETURNING
			status,
			failure_error_code`
	row := bms.db.QueryRowContext(ctx, q, job.Status, job.ErrorCode, job.ID)
	if err := row.Scan(&job.Status, &job.ErrorCode); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("background migration job not found")
		}
		return fmt.Errorf("updating background migration job status: %w", err)
	}
	return nil
}

// Lock sets a lock to prevent concurrent execution of new Background Migration jobs.
// This implementation uses PostgreSQL's Transaction Advisory Locks via `pg_try_advisory_xact_lock()` and should be used within a transaction context.
// For details on Advisory Locks, see: https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS
func (bms *backgroundMigrationStore) Lock(ctx context.Context) error {
	var result bool
	defer metrics.InstrumentQuery("bbm_lock")()

	q := "SELECT pg_try_advisory_xact_lock($1)"
	err := bms.db.QueryRowContext(ctx, q, advisoryLockKey).Scan(&result)
	if err != nil {
		return err
	}
	if !result {
		return ErrBackgroundMigrationLockInUse
	}
	return nil
}

// ValidateMigrationTableAndColumn asserts that the column and table exists in the database.
func (bms *backgroundMigrationStore) ValidateMigrationTableAndColumn(ctx context.Context, tableWithSchema, column string) error {
	// TODO: Consider improving the validation here by using a type system such that we're taking some kind of ValidatedTable and ValidatedColumn types
	// that need to be constructed from the raw strings and have unexported fields with Getters.
	// https://gitlab.com/gitlab-org/container-registry/-/merge_requests/1669#note_2000736259

	var (
		table  string
		schema string
	)
	if s := strings.Split(tableWithSchema, "."); len(s) != 2 {
		return fmt.Errorf("table must be in the format '<schema>.<table>: %w", ErrUnknownTable)
	} else {
		schema = s[0]
		table = s[1]
	}
	ok, err := bms.ExistsTable(ctx, schema, table)
	if err != nil {
		return err
	}

	if !ok {
		err = ErrUnknownTable
		return err
	}

	ok, err = bms.ExistsColumn(ctx, schema, table, column)
	if err != nil {
		return err
	}

	if !ok {
		err = ErrUnknownColumn
	}

	return err
}

func scanBackgroundMigrationJob(row *sql.Row) (*models.BackgroundMigrationJob, error) {
	j := new(models.BackgroundMigrationJob)
	if err := row.Scan(&j.ID, &j.BBMID, &j.StartID, &j.EndID, &j.Status, &j.Attempts, &j.ErrorCode); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("scanning batched background migration job: %w", err)
		}
		return nil, nil
	}
	return j, nil
}

func scanBackgroundMigration(row *sql.Row) (*models.BackgroundMigration, error) {
	bm := new(models.BackgroundMigration)
	if err := row.Scan(&bm.ID, &bm.Name, &bm.StartID, &bm.EndID, &bm.BatchSize, &bm.Status, &bm.JobName, &bm.TargetTable, &bm.TargetColumn, &bm.ErrorCode); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("scanning batched background migration: %w", err)
		}
		return nil, nil
	}

	return bm, nil
}
