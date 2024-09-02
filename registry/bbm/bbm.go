//go:generate mockgen -package mocks -destination mocks/bbm.go . Handler

package bbm

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"gitlab.com/gitlab-org/labkit/correlation"
	"gitlab.com/gitlab-org/labkit/errortracking"
)

const (
	componentKey             = "component"
	workerName               = "registry.bbm.Worker"
	defaultMaxJobAttempt     = 5
	defaultJobInterval       = 1 * time.Minute
	jobIntervalJitterSeconds = 5
	// maxRunTimeout caps the duration of each background migration run to 3 mins.
	maxRunTimeout = 3 * time.Minute

	// Background Migration job log keys
	jobIDKey        = "job_id"
	jobBBMIDKey     = "job_bbm_id"
	jobNameKey      = "job_name"
	jobAttemptsKey  = "job_attempts"
	jobStartIDKey   = "job_start_id"
	jobEndIDKey     = "job_end_id"
	jobBatchSizeKey = "job_batch_size"
	jobStatusKey    = "job_status"
	jobColumnKey    = "job_pagination_column"
	jobTableKey     = "job_pagination_table"

	// Background Migration log keys
	bbmIDKey           = "bbm_id"
	bbmNameKey         = "bbm_name"
	bbmBatchSizeKey    = "bbm_batch_size"
	bbmStatusKey       = "bbm_status"
	bbmJobSignatureKey = "bbm_job_signature_key"
	bbmTargetColumnKey = "bbm_target_column"
	bbmTargetTableKey  = "bbm_target_table"
)

var (
	// ErrJobEndpointNotFound is returned when a job's endpoint can not be calculated.
	ErrJobEndpointNotFound = errors.New("job endpoint could not be calculated")
	// ErrMaxJobAttemptsReached is returned when the maximum attempt to try a job has elapsed.
	ErrMaxJobAttemptsReached = errors.New("maximum job attempt reached")
	// ErrWorkFunctionNotFound is returned when a referenced job has no corresponding work function.
	ErrWorkFunctionNotFound = errors.New("work function not found")
)

type WorkFunc func(ctx context.Context, db datastore.Handler, paginationTable, paginationColumn string, paginationAfter, paginationBefore, limit int) error

// Work represents the underlying functions that a Background Migration job is capable of executing.
type Work struct {
	// Name must correspond to the `job_signature_name` in `batched_background_migrations` table.
	Name string
	// Do is the work function that is assigned to a job.
	Do WorkFunc
}

// AllWork is a list of all background migration work functions known to the registry.
// When a registry developer wants to link a background migration in the database to a work function, they must make sure the corresponding work entry is added to the list here.
// The `Work.name` must correspond to the value in `batched_background_migrations.job_signature_name` column for the specific the background migration,
// otherwise the migration will fail to run with an `ErrWorkFunctionNotFound` when picked up.
func AllWork() []Work {
	return []Work{
		//{Name: "ExampleNameThatMatchesTheJobSignatureNameColumn", Do: ExampleDoFunction}
	}
}

// RegisterWork registers all known work functions to the Background Migration worker.
func RegisterWork(work []Work, opts ...WorkerOption) (*Worker, error) {
	workMap := map[string]Work{}
	for _, val := range work {
		if _, found := workMap[val.Name]; found {
			return nil, fmt.Errorf("can not have work with the same name %s", val.Name)
		}
		workMap[val.Name] = val
	}
	return NewWorker(workMap, opts...), nil
}

// Worker is the Background Migration agent of execution. It listens for pending Background Migration jobs and tries to execute the corresponding work function.
type Worker struct {
	Work          map[string]Work
	logger        log.Logger
	db            datastore.Handler
	jobInterval   time.Duration
	maxJobAttempt int
	wh            Handler
}

// Handler defines the methods required for handling background migration jobs. It provides flexibility to use different implementations for job management.
// This is currently leveraged in tests to facilitate mocking worker functions.
type Handler interface {
	FindJob(context.Context, datastore.BackgroundMigrationStore) (*models.BackgroundMigrationJob, error)
	GrabLock(context.Context, datastore.BackgroundMigrationStore) error
	ExecuteJob(context.Context, datastore.BackgroundMigrationStore, *models.BackgroundMigrationJob) error
}

// WorkerOption provides functional options for NewWorker.
type WorkerOption func(*Worker)

// WithJobInterval sets the interval between job scans/runs. Defaults to 1 seconds.
func WithJobInterval(d time.Duration) WorkerOption {
	return func(a *Worker) {
		a.jobInterval = d
	}
}

// WithMaxJobAttempt sets the maximum attempts to try to execute a job when an error occurs.
func WithMaxJobAttempt(d int) WorkerOption {
	return func(jw *Worker) {
		jw.maxJobAttempt = d
	}
}

// WithLogger sets the logger.
func WithLogger(l log.Logger) WorkerOption {
	return func(jw *Worker) {
		jw.logger = l
	}
}

// WithDB sets the DB.
func WithDB(db datastore.Handler) WorkerOption {
	return func(jw *Worker) {
		jw.db = db
	}
}

// WithHandler sets the worker handle.
func WithHandler(wh Handler) WorkerOption {
	return func(jw *Worker) {
		jw.wh = wh
	}
}

func (jw *Worker) applyDefaults() {
	if jw.logger == nil {
		jw.logger = log.GetLogger()
	}
	if jw.jobInterval == 0 {
		jw.jobInterval = defaultJobInterval
	}
	if jw.maxJobAttempt == 0 {
		jw.maxJobAttempt = defaultMaxJobAttempt
	}
	if jw.wh == nil {
		jw.wh = jw
	}
}

// NewWorker creates a new Worker.
func NewWorker(workMap map[string]Work, opts ...WorkerOption) *Worker {
	jw := &Worker{Work: workMap}
	jw.applyDefaults()

	for _, opt := range opts {
		opt(jw)
	}

	jw.logger = jw.logger.WithFields(log.Fields{componentKey: workerName})

	return jw
}

// ListenForBackgroundMigration allows a Worker to inspect the Background Migration datastore for pending jobs that need to be executed and if applicable execute the work function of the pending job.
// The inspection of the Background Migration datastore for potential work is carried out at a period of `jobInterval` set in the configuration.
// Once a job is found the worker attempts to obtain the distributed Background Migration lock and execute the job function to completion.
// However, if the distributed lock is held by another worker in a separate process it returns an `ErrlockInUse` and waits another `jobInterval` duration to try again.
func (jw *Worker) ListenForBackgroundMigration(ctx context.Context, doneChan <-chan struct{}) (chan struct{}, error) {
	// gracefullFinish is used to signal to an upstream processes that a worker has completed any in-flight jobs and the upstream can terminate if needed.
	gracefullFinish := make(chan struct{})
	// Create a period that this worker searches and executes work on (use a random jitter of jobIntervalJitterSeconds for obscurity).
	ticker := time.NewTicker(jw.jobInterval + time.Duration(rand.Intn(jobIntervalJitterSeconds))*time.Second)

	go func() {
		for {
			jw.logger.Info("waiting for next cycle...")
			select {
			// The upstream process is terminating, this worker should exit.
			case <-doneChan:
				// cleanup
				jw.logger.Info("received shutdown signal: shutting down...")
				close(gracefullFinish)
				return
			// A period has elapsed, time to try to find and execute any available jobs.
			case <-ticker.C:
				jw.logger.Info("starting worker run...")
				jw.run(ctx)
			}
		}
	}()

	return gracefullFinish, nil
}

// run is responsible for orchestrating and executing Background Migrations when found.
// it does this by attempting to obtain the Background Migration distributed lock,
// before proceeding to find and execute any applicable Background Migration jobs.
func (jw *Worker) run(ctx context.Context) {
	// inject correlation id to logs
	jw.logger = jw.logger.WithFields(log.Fields{correlation.FieldName: correlation.ExtractFromContextOrGenerate(ctx)})

	// Cancel the job worker context when the `maxRunTimeout` duration elapses
	ctx, cancel := context.WithTimeout(ctx, maxRunTimeout)
	defer cancel()

	// start a transaction to run the background migration process
	tx, err := jw.db.BeginTx(ctx, nil)
	if err != nil {
		jw.logger.WithError(err).Error("failed to create database transaction")
		errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
		return

	}
	defer tx.Rollback()

	bbmStore := datastore.NewBackgroundMigrationStore(tx)

	// Grab distributed lock
	jw.logger.Info("obtaining lock...")
	if err = jw.wh.GrabLock(ctx, bbmStore); err != nil {
		if !errors.Is(err, datastore.ErrBackgroundMigrationLockInUse) {
			errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
		}
		jw.logger.WithError(err).Info("failed to obtain lock")

		return
	}

	// Search for available jobs
	jw.logger.Info("searching for job...")
	job, err := jw.wh.FindJob(ctx, bbmStore)
	if err != nil {
		jw.logger.WithError(err).Error("failed to find job")
		errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
		return
	}
	if job == nil {
		jw.logger.Info("no jobs to run...")
		if err = tx.Commit(); err != nil {
			jw.logger.WithError(err).Error("failed to commit database transaction")
			errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
		}
		return
	}

	l := jw.logger.WithFields(log.Fields{
		jobIDKey:        job.ID,
		jobBBMIDKey:     job.BBMID,
		jobNameKey:      job.JobName,
		jobAttemptsKey:  job.Attempts,
		jobStartIDKey:   job.StartID,
		jobEndIDKey:     job.EndID,
		jobBatchSizeKey: job.BatchSize,
		jobStatusKey:    job.Status.String(),
		jobColumnKey:    job.PaginationColumn,
		jobTableKey:     job.PaginationTable,
	})

	// A job was found, lets execute it
	jw.logger.Info("job found, executing")
	err = jw.wh.ExecuteJob(ctx, bbmStore, job)
	if err != nil {
		l.WithError(err).Error("failed to execute job")
		errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
		return
	}

	if err = tx.Commit(); err != nil {
		jw.logger.WithError(err).Error("failed to commit database transaction")
		errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
		return

	}

	l.Info("finished background migration job run")
}

// GrabLock attempts to grab the distributed lock used for co-ordination between all Background Migration processes.
func (jw *Worker) GrabLock(ctx context.Context, bbmStore datastore.BackgroundMigrationStore) (err error) {
	// Acquire a lock so no other Background Migration process can run.
	err = bbmStore.Lock(ctx)
	if err != nil {
		return err
	}
	jw.logger.Info("obtained lock")

	return nil
}

// FindJob checks for any Background Migration job that needs to be executed.
// If a job needs to be executed it either fetches the job or creates the job
// associated with the chosen Background Migration.
func (jw *Worker) FindJob(ctx context.Context, bbmStore datastore.BackgroundMigrationStore) (*models.BackgroundMigrationJob, error) {
	// Find a Background Migration that needs to be run.
	bbm, err := bbmStore.FindNext(ctx)
	if err != nil {
		return nil, err
	}
	if bbm == nil {
		return nil, nil
	}

	l := jw.logger.WithFields(log.Fields{
		bbmNameKey:         bbm.Name,
		bbmIDKey:           bbm.ID,
		bbmJobSignatureKey: bbm.JobName,
		bbmStatusKey:       bbm.Status.String(),
		bbmBatchSizeKey:    bbm.BatchSize,
		bbmTargetColumnKey: bbm.TargetColumn,
		bbm.TargetTable:    bbm.TargetTable,
	})

	l.Info("a background migration was found that needs to be executed")

	err = validateMigration(ctx, bbmStore, jw.Work, bbm)
	if err != nil {
		l.WithError(err).Error("background migration failed validation")

		if errors.Is(err, datastore.ErrUnknownColumn) || errors.Is(err, datastore.ErrUnknownTable) || errors.Is(err, ErrWorkFunctionNotFound) {
			// Mark migration as failed and surface the error to sentry if we don't know the column or the table referenced in the migration
			var errCode models.BBMErrorCode
			switch {
			case errors.Is(err, datastore.ErrUnknownColumn):
				errCode = models.InvalidColumnBBMErrCode
			case errors.Is(err, datastore.ErrUnknownTable):
				errCode = models.InvalidTableBBMErrCode
			case errors.Is(err, ErrWorkFunctionNotFound):
				errCode = models.InvalidJobSignatureBBMErrCode
			}
			errortracking.Capture(err, errortracking.WithContext(ctx))
			bbm.Status = models.BackgroundMigrationFailed
			bbm.ErrorCode = errCode
			return nil, bbmStore.UpdateStatus(ctx, bbm)
		}
		return nil, err
	}

	var job *models.BackgroundMigrationJob

	done, err := hasRunAllBBMJobsAtLeastOnce(ctx, bbmStore, bbm)
	if err != nil {
		return nil, err
	}

	// if we haven't run all jobs for the Background Migration at least once first find and exhaust all new jobs before considering failed/retryable jobs.
	if !done {
		return findNewJob(ctx, bbmStore, bbm)
	} else {
		job, err = findRetryableJobs(ctx, bbmStore, bbm)
		if err != nil {
			return nil, err
		}
		if job != nil {

			l := l.WithFields(log.Fields{
				jobIDKey:        job.ID,
				jobBBMIDKey:     job.BBMID,
				jobNameKey:      job.JobName,
				jobAttemptsKey:  job.Attempts,
				jobStartIDKey:   job.StartID,
				jobEndIDKey:     job.EndID,
				jobBatchSizeKey: job.BatchSize,
				jobStatusKey:    job.Status.String(),
				jobColumnKey:    job.PaginationColumn,
				jobTableKey:     job.PaginationTable,
			})

			// check that the selected job does not exceed the configured `MaxJobAttempt`
			if job.Attempts >= jw.maxJobAttempt {
				l.WithError(ErrMaxJobAttemptsReached).Error("marking background migration as failed due to job failure")
				bbm.ErrorCode = models.JobExceedsMaxAttemptBBMErrCode
				bbm.Status = models.BackgroundMigrationFailed
				return nil, bbmStore.UpdateStatus(ctx, bbm)
			}
		}
		return job, nil
	}
}

// ExecuteJob attempts to execute the function associated with a Background Migration job from the job's start-end range.
func (jw *Worker) ExecuteJob(ctx context.Context, bbmStore datastore.BackgroundMigrationStore, job *models.BackgroundMigrationJob) error {
	// update the job attempts
	err := bbmStore.IncrementJobAttempts(ctx, job.ID)
	if err != nil {
		return err
	}

	// find the job function from the registered work map and execute it.
	if work, found := jw.Work[job.JobName]; found {
		err := work.Do(ctx, jw.db, job.PaginationTable, job.PaginationColumn, job.StartID, job.EndID, job.BatchSize)
		if err != nil {
			jw.logger.WithError(err).Error("failed executing job")
			job.Status = models.BackgroundMigrationFailed
			job.ErrorCode = models.UnknownBBMErrorCode
			return bbmStore.UpdateJobStatus(ctx, job)
		} else {
			job.Status = models.BackgroundMigrationFinished
		}
		return bbmStore.UpdateJobStatus(ctx, job)
	} else {
		job.Status = models.BackgroundMigrationFailed
		job.ErrorCode = models.InvalidJobSignatureBBMErrCode
		err := bbmStore.UpdateJobStatus(ctx, job)
		if err != nil {
			return err
		}
		return ErrWorkFunctionNotFound
	}
}

// AllMigrations returns all background migrations.
func (jw *Worker) AllMigrations(ctx context.Context) (models.BackgroundMigrations, error) {
	return datastore.NewBackgroundMigrationStore(jw.db).FindAll(ctx)
}

// PauseEligibleMigrations pauses all running or active background migrations.
func (jw *Worker) PauseEligibleMigrations(ctx context.Context) error {
	return datastore.NewBackgroundMigrationStore(jw.db).Pause(ctx)
}

// findRetryableJobs looks for jobs that failed prior in the scope of a specific Background Migration.
// if no failed jobs are found in the Background Migration it sets the status of the Background Migration to finished.
func findRetryableJobs(ctx context.Context, bbmStore datastore.BackgroundMigrationStore, bbm *models.BackgroundMigration) (*models.BackgroundMigrationJob, error) {
	job, err := bbmStore.FindJobWithStatus(ctx, bbm.ID, models.BackgroundMigrationFailed)
	if err != nil {
		return nil, err
	}
	// if there are no jobs that failed update the migration to finished state
	if job == nil {
		bbm.Status = models.BackgroundMigrationFinished
		return nil, bbmStore.UpdateStatus(ctx, bbm)
	}
	// Otherwise, decorate any found job with the parent (Background Migration) attributes.
	job.JobName = bbm.JobName
	job.PaginationTable = bbm.TargetTable
	job.PaginationColumn = bbm.TargetColumn
	job.BatchSize = bbm.BatchSize

	return job, nil
}

// findNewJob creates the next job in the batch sequence to be run for a Background Migration.
func findNewJob(ctx context.Context, bbmStore datastore.BackgroundMigrationStore, bbm *models.BackgroundMigration) (*models.BackgroundMigrationJob, error) {
	var (
		start int
		last  = bbm.EndID
	)

	// find the last job that was created for the Background Migration.
	lastCreatedJob, err := bbmStore.FindLastJob(ctx, bbm)
	if err != nil {
		return nil, err
	}
	// if the Background Migration does not have any job, this implies it was never been run/started, so start it!
	if lastCreatedJob == nil {
		start = bbm.StartID
		bbm.Status = models.BackgroundMigrationRunning
		err = bbmStore.UpdateStatus(ctx, bbm)
		if err != nil {
			return nil, err
		}
	} else {
		// otherwise find the starting point for the next job that should be created for the Background Migration.
		start = lastCreatedJob.EndID + 1
		// the start point of the job to be created must not be greater than the Background Migration end bound.
		if start > last {
			return nil, nil
		}
	}

	// Based on the Background Migration batch size and the start point of the job, find the job's end point.
	// TODO: we could off-load some of this logic to the store layer where we can potentially craft a query that will give us both start and end job IDs.
	end, err := bbmStore.FindJobEndFromJobStart(ctx, bbm.TargetTable, bbm.TargetColumn, start, last, bbm.BatchSize)
	if err != nil {
		return nil, err
	}

	// create the job representation and decorate the job with some of the parent (Background Migration) attributes
	job := &models.BackgroundMigrationJob{
		BBMID:            bbm.ID,
		StartID:          start,
		EndID:            end,
		BatchSize:        bbm.BatchSize,
		JobName:          bbm.JobName,
		PaginationColumn: bbm.TargetColumn,
		PaginationTable:  bbm.TargetTable,
	}

	err = bbmStore.CreateNewJob(ctx, job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

// hasRunAllBBMJobsAtLeastOnce checks if a Background Migration has run all its jobs at least once.
func hasRunAllBBMJobsAtLeastOnce(ctx context.Context, bbmStore datastore.BackgroundMigrationStore, bbm *models.BackgroundMigration) (bool, error) {
	// Check if any jobs for the selected Background Migration exist with the end bound of the Background Migration,
	// if it does, it signifies we've run all jobs of the selected Background Migration at least once.
	finalJob, err := bbmStore.FindJobWithEndID(ctx, bbm.ID, bbm.EndID)
	if finalJob != nil {
		return true, err
	}
	return false, err
}

func validateMigration(ctx context.Context, bbmStore datastore.BackgroundMigrationStore, workFuncs map[string]Work, bbm *models.BackgroundMigration) error {
	if _, ok := workFuncs[bbm.JobName]; !ok {
		return ErrWorkFunctionNotFound
	}

	if err := bbmStore.ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn); err != nil {
		return err
	}
	return nil
}
