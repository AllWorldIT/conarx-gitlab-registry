package datastore

import (
	"context"

	"github.com/docker/distribution/registry/datastore/models"
)

// BackgroundMigrationStore is the interface that a background migration store should conform to.
type BackgroundMigrationStore interface {
	// FindJobEndFromJobStart finds the end cursor for a job based on the start of the job and the batch size of the Background Migration the job is associated with.
	FindJobEndFromJobStart(ctx context.Context, table string, column string, start, last, batchSize int) (*int, error)
	// FindLastJob returns the last job created for a Background Migration.
	FindLastJob(ctx context.Context, backgroundMigration *models.BackgroundMigration) (*models.BackgroundMigrationJob, error)
	// FindNext finds the first active or running Background Migration by ascending order on the Background Migration `id“ column.
	FindNext(ctx context.Context) (*models.BackgroundMigration, error)
	// FindJobWithEndID returns any jobs with the end id `endID`.
	FindJobWithEndID(ctx context.Context, id, endID int) (*models.BackgroundMigrationJob, error)
	// FindJobWithStatus returns any jobs with the status `status`.
	FindJobWithStatus(ctx context.Context, id, status int) (*models.BackgroundMigrationJob, error)
	// FindById find a BackgroundMigration with id `id`.
	FindById(ctx context.Context, id int) (*models.BackgroundMigration, error)
	// FindByName find a Background Migration with name `name`.
	FindByName(ctx context.Context, name string) (*models.BackgroundMigration, error)
	// FindAllCompleted find the names of all completed Background Migrations .
	FindAllCompleted(ctx context.Context) (map[string]struct{}, error)
	// CreateNewJob creates a new job entry in the `batched_background_migration_jobs` table.
	CreateNewJob(ctx context.Context, newJob *models.BackgroundMigrationJob) (*models.BackgroundMigrationJob, error)
	// UpdateStatus updates the status of a Background Migration to `status`.
	UpdateStatus(ctx context.Context, id, status int) error
	// IncrementJobAttempts updates the number of attempts of a Background Migration job to `attempts` by 1.
	IncrementJobAttempts(ctx context.Context, jobID int) error
	// UpdateJobStatus updates the updates the status of a BBM job to `status`.
	UpdateJobStatus(ctx context.Context, jobID, status int) error
	// Lock sets a lock to prevent new Background Migration jobs from running.
	Lock(ctx context.Context) error
	// Unlock releases the lock to allow new Background Migration jobs to run.
	Unlock(ctx context.Context) error
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

// FindJobEndFromJobStart finds the end cursor for a job based on the start of the job and the batch size of the Background Migration the job is associated with.
func (bms *backgroundMigrationStore) FindJobEndFromJobStart(ctx context.Context, table string, column string, start, last, batchSize int) (*int, error) {
	// TODO: Implemetation
	return nil, nil
}

// FindLastJob returns the last job created for a Background Migration.
func (bms *backgroundMigrationStore) FindLastJob(ctx context.Context, backgroundMigration *models.BackgroundMigration) (*models.BackgroundMigrationJob, error) {
	// TODO: Implemetation
	return nil, nil
}

// FindNext finds the first active or running Background Migration by ascending order on the Background Migration `id“ column.
func (bms *backgroundMigrationStore) FindNext(ctx context.Context) (*models.BackgroundMigration, error) {
	// TODO: Implemetation
	return nil, nil
}

// FindJobWithEndID returns any jobs with the end id `endID`.
func (bms *backgroundMigrationStore) FindJobWithEndID(ctx context.Context, id, endID int) (*models.BackgroundMigrationJob, error) {
	// TODO: Implemetation
	return nil, nil
}

// FindJobWithStatus returns any jobs with the status `status`.
func (bms *backgroundMigrationStore) FindJobWithStatus(ctx context.Context, id, status int) (*models.BackgroundMigrationJob, error) {
	// TODO: Implemetation
	return nil, nil
}

// FindById find a BackgroundMigration with id `id`.
func (bms *backgroundMigrationStore) FindById(ctx context.Context, id int) (*models.BackgroundMigration, error) {
	// TODO: Implemetation
	return nil, nil
}

// FindByName find a Background Migration with name `name`.
func (bms *backgroundMigrationStore) FindByName(ctx context.Context, name string) (*models.BackgroundMigration, error) {
	// TODO: Implemetation
	return nil, nil
}

// FindAllCompleted find the names of all completed Background Migrations .
func (bms *backgroundMigrationStore) FindAllCompleted(ctx context.Context) (map[string]struct{}, error) {
	// TODO: Implemetation
	return nil, nil
}

// CreateNewJob creates a new job entry in the `batched_background_migration_jobs` table.
func (bms *backgroundMigrationStore) CreateNewJob(ctx context.Context, newJob *models.BackgroundMigrationJob) (*models.BackgroundMigrationJob, error) {
	// TODO: Implemetation
	return nil, nil
}

// UpdateStatus updates the status of a Background Migration to `status`.
func (bms *backgroundMigrationStore) UpdateStatus(ctx context.Context, id, status int) error {
	// TODO: Implemetation
	return nil
}

// IncrementJobAttempts updates the number of attempts of a BackgroundMigration job by 1.
func (bms *backgroundMigrationStore) IncrementJobAttempts(ctx context.Context, jobID int) error {
	// TODO: Implemetation
	return nil
}

// UpdateJobStatus updates the updates the status of a BBM job to `status`.
func (bms *backgroundMigrationStore) UpdateJobStatus(ctx context.Context, jobID, status int) error {
	// TODO: Implemetation
	return nil
}

// Lock sets a lock to prevent new Background Migration jobs from running.
func (bms *backgroundMigrationStore) Lock(ctx context.Context) error {
	// TODO: Implemetation
	return nil
}

// Unlock releases the lock to allow new Background Migration jobs to run.
func (bms *backgroundMigrationStore) Unlock(ctx context.Context) error {
	// TODO: Implemetation
	return nil
}
