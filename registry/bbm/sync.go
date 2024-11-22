package bbm

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"gitlab.com/gitlab-org/labkit/correlation"
)

// SyncWorker is the synchronous Background Migration agent of execution.
type SyncWorker struct {
	work                 map[string]Work
	logger               log.Logger
	db                   datastore.Handler
	maxJobAttempt        int
	maxJobPerBatch       int
	maxBatchTimeout      time.Duration
	lockWaitTimeout      time.Duration
	jobTimeout           time.Duration
	wh                   Handler
	lastRunCompletedBBMs int
}

// SyncWorkerOption provides functional options for NewSyncWorker.
type SyncWorkerOption func(*SyncWorker)

// WithSyncMaxBatchTimeout sets the maximum batch timeout.
func WithSyncMaxBatchTimeout(d time.Duration) SyncWorkerOption {
	return func(a *SyncWorker) {
		a.maxBatchTimeout = d
	}
}

// WithJobTimeout sets the maximum job timeout.
func WithJobTimeout(d time.Duration) SyncWorkerOption {
	return func(a *SyncWorker) {
		a.jobTimeout = d
	}
}

// WithSyncMaxJobPerBatch sets the maximum number of jobs per batch.
func WithSyncMaxJobPerBatch(d int) SyncWorkerOption {
	return func(a *SyncWorker) {
		a.maxJobPerBatch = d
	}
}

// WithSyncMaxJobAttempt sets the maximum number of attempts to try to execute a job when an error occurs.
func WithSyncMaxJobAttempt(d int) SyncWorkerOption {
	return func(jw *SyncWorker) {
		jw.maxJobAttempt = d
	}
}

// WithSyncLogger sets the logger.
func WithSyncLogger(l log.Logger) SyncWorkerOption {
	return func(jw *SyncWorker) {
		jw.logger = l
	}
}

// WithWorkMap sets the work map.
func WithWorkMap(wm map[string]Work) SyncWorkerOption {
	return func(jw *SyncWorker) {
		jw.work = wm
	}
}

// WithSyncHandler sets the worker handler.
func WithSyncHandler(wh Handler) SyncWorkerOption {
	return func(jw *SyncWorker) {
		jw.wh = wh
	}
}

// NewSyncWorker creates a new SyncWorker with the provided options.
func NewSyncWorker(db datastore.Handler, opts ...SyncWorkerOption) *SyncWorker {
	jw := &SyncWorker{db: db}

	jw.applyDefaults()

	for _, opt := range opts {
		opt(jw)
	}

	jw.logger = jw.logger.WithFields(log.Fields{componentKey: syncWorkerName})

	return jw
}

const (
	syncWorkerName         = "registry.bbm.SyncWorker"
	defaultMaxBatchTimeout = 10 * time.Minute       // Default maximum timeout for a batch
	defaultJobTimeout      = 2 * time.Minute        // Default maximum timeout for a migration
	defaultLockWaitTimeout = 1 * time.Minute        // Default timeout to wait for a lock
	defaultMaxJobPerBatch  = 5                      // Default maximum number of jobs per batch
	minDelayPerRun         = 100 * time.Millisecond // Minimum delay between runs
	maxDelayPerRun         = 2 * time.Second        // Maximum delay between runs
)

// applyDefaults sets the default values for SyncWorker fields if they are not already set.
func (jw *SyncWorker) applyDefaults() {
	if jw.logger == nil {
		jw.logger = log.GetLogger()
	}
	if jw.maxBatchTimeout == 0 {
		jw.maxBatchTimeout = defaultMaxBatchTimeout
	}
	if jw.jobTimeout == 0 {
		jw.jobTimeout = defaultJobTimeout
	}
	if jw.maxJobAttempt == 0 {
		jw.maxJobAttempt = defaultMaxJobAttempt
	}
	if jw.maxJobPerBatch == 0 {
		jw.maxJobPerBatch = defaultMaxJobPerBatch
	}
	if jw.lockWaitTimeout == 0 {
		jw.lockWaitTimeout = defaultLockWaitTimeout
	}
	if jw.work == nil {
		workMap := map[string]Work{}
		for _, val := range AllWork() {
			workMap[val.Name] = val
		}
		jw.work = workMap
	}
	if jw.wh == nil {
		jw.wh = jw
	}
}

// Run executes all unfinished background migrations until they are either finished or a job exceeds the maxJobRetry count.
func (jw *SyncWorker) Run(ctx context.Context) error {
	return jw.run(ctx)
}

// run executes the main loop for processing synchronous background migration jobs. It performs the following steps:
// 1. Starts a new transaction and acquires a distributed lock.
// 2. Processes up to `maxJobPerBatch` jobs or until `maxBatchTimeout` elapses.
// 3. For each job:
//   - Finds an eligible job using FindJob.
//   - If a job is found, executes it using ExecuteJob.
//   - If no job is found, commits the transaction and returns.
//
// 4. After processing the batch, commits the transaction and releases the lock.
// 5. Waits for a random duration between `minDelayPerRun` and `maxDelayPerRun` before starting the next iteration.
// This method continues running until all jobs are processed or an error occurs.
func (jw *SyncWorker) run(ctx context.Context) error {
	jw.lastRunCompletedBBMs = 0
	for {
		jw.logger = jw.logger.WithFields(log.Fields{"batch_id": correlation.SafeRandomID()})
		jw.logger.Info(fmt.Sprintf("starting new batch run of %v jobs", jw.maxJobPerBatch))
		// This loop runs at most `maxJobPerBatch` jobs (or until `maxBatchTimeout` elapses) before committing and releasing the background migration lock.
		// This ensures efficient use of the acquired lock, reducing the number of times we need to challenge the asynchronous process for the lock,
		// while also adding a buffer to avoid overwhelming the database.
		ctx, cancel := context.WithTimeout(ctx, jw.maxBatchTimeout)
		defer cancel()

		// Start a transaction to run the background migration
		tx, err := jw.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to create database transaction: %w", err)
		}
		defer tx.Rollback()

		bbmStore := datastore.NewBackgroundMigrationStore(tx)

		// Grab distributed lock
		lockCtx, lockCancel := context.WithTimeout(ctx, jw.lockWaitTimeout)
		defer lockCancel()

		if err = jw.wh.GrabLock(lockCtx, bbmStore); err != nil {
			jw.logger.WithError(err).Error("failed to obtain lock, terminating batch run")
			return err
		}

		for i := 0; i < jw.maxJobPerBatch; i++ {
			jw.logger = jw.logger.WithFields(log.Fields{correlation.FieldName: correlation.SafeRandomID()})

			// each job should take at most the lesser of the parent ctx timeout or jobTimeout`.
			jobCtx, jobCancel := context.WithTimeout(ctx, jw.jobTimeout)
			defer jobCancel()

			job, err := jw.wh.FindJob(jobCtx, bbmStore)
			if err != nil {
				jw.logger.WithError(err).Error("failed to find job, terminating batch run")
				return err
			}
			if job != nil {
				l := jw.logger.WithFields(log.Fields{
					jobIDKey:        job.ID,
					jobBBMIDKey:     job.BBMID,
					jobNameKey:      job.JobName,
					jobStartIDKey:   job.StartID,
					jobEndIDKey:     job.EndID,
					jobBatchSizeKey: job.BatchSize,
					jobStatusKey:    job.Status.String(),
					jobColumnKey:    job.PaginationColumn,
					jobTableKey:     job.PaginationTable,
				})

				l.Info("found job, starting execution")
				err := jw.wh.ExecuteJob(jobCtx, bbmStore, job)
				if err != nil {
					l.WithError(err).Error("failed to execute job, terminating batch run")
					return err
				}
				l.Info("job completed")
			} else {
				// Commit the transaction if no more jobs are found
				if err := tx.Commit(); err != nil {
					return err
				}
				jw.logger.Info("no more jobs to run")
				return nil
			}
			jobCancel()
		}
		// Commit the transaction after processing the batch of jobs
		if err := tx.Commit(); err != nil {
			return err
		}
		lockCancel()
		cancel()

		// Randomized delay between `minDelayPerRun` and `maxDelayPerRun`
		// nolint: gosec
		sleep := time.Duration(rand.Int63n(int64(maxDelayPerRun-minDelayPerRun))) + minDelayPerRun
		jw.logger.WithFields(log.Fields{"duration_s": sleep.Seconds()}).
			Info("released lock, sleeping before next run")
		time.Sleep(sleep)
	}
}

// ResumeEligibleMigrations resumes all paused background migrations.
func (jw *SyncWorker) ResumeEligibleMigrations(ctx context.Context) error {
	return datastore.NewBackgroundMigrationStore(jw.db).Resume(ctx)
}

// FindJob finds the next eligible job to be run.
func (jw *SyncWorker) FindJob(ctx context.Context, bbmStore datastore.BackgroundMigrationStore) (*models.BackgroundMigrationJob, error) {
	var (
		job *models.BackgroundMigrationJob
		bbm *models.BackgroundMigration
		err error
	)

	for {
		// Find failed background migrations first
		bbm, job, err = findFailed(ctx, bbmStore)
		if err != nil {
			jw.logger.WithError(err).Error("failed to find failed background migrations job")
			return nil, fmt.Errorf("failed to find failed background migrations job: %w", err)
		}

		if bbm != nil {
			// Update status to running if migration was left in failed state but no longer has any failed migration jobs.
			if job == nil {
				jw.logger.Info("found failed migration without failed jobs, setting it back to running")
				bbm.ErrorCode = models.NullErrCode
				bbm.Status = models.BackgroundMigrationRunning

				if err = bbmStore.UpdateStatus(ctx, bbm); err != nil {
					jw.logger.WithError(err).Error("failed to update status of background migration")
					return nil, fmt.Errorf("failed to update status of background migration: %w", err)
				}
				jw.logger.Info("updated migration status, continuing to look for other jobs")
				continue
			}
			enrichJobWithBBMAttributes(job, bbm)
			return job, err
		}

		bbm, job, err = findRunningOrActive(ctx, bbmStore)
		if err != nil {
			jw.logger.WithError(err).Error("failed to find running or active background migrations job")
			return nil, fmt.Errorf("failed to find running or active background migrations job: %w", err)
		}
		if bbm != nil {
			if job == nil {
				jw.logger.Info("found running/active migration without jobs, setting it to finished")
				bbm.ErrorCode = models.NullErrCode
				bbm.Status = models.BackgroundMigrationFinished
				if err = bbmStore.UpdateStatus(ctx, bbm); err != nil {
					jw.logger.WithError(err).Error("failed to update status of background migration")
					return nil, fmt.Errorf("failed to update status of background migration: %w", err)
				}
				jw.lastRunCompletedBBMs++
				jw.logger.Info("updated migration status, continuing to look for other jobs")
				continue
			}
		}
		enrichJobWithBBMAttributes(job, bbm)

		return job, err
	}
}

// findRunningOrActive finds the next background migration in the running or active state and the next eligible job to be run under the migration.
func findRunningOrActive(ctx context.Context, bbmStore datastore.BackgroundMigrationStore) (*models.BackgroundMigration, *models.BackgroundMigrationJob, error) {
	var (
		job *models.BackgroundMigrationJob
		bbm *models.BackgroundMigration
	)

	// Find the next running or active background migration
	bbm, err := bbmStore.FindNext(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find active/running background migrations: %w", err)
	}

	if bbm == nil {
		// No more background migrations to process, exit loop
		return nil, nil, nil
	}

	// Prioritize failed jobs if any before considering new jobs
	job, err = bbmStore.FindJobWithStatus(ctx, bbm.ID, models.BackgroundMigrationFailed)
	if err != nil {
		return nil, nil, err
	}

	if job == nil {
		// If no failed jobs are found, look for new jobs
		job, err = findNewJob(ctx, bbmStore, bbm)
		if err != nil {
			return nil, nil, err
		}
	}

	return bbm, job, err
}

// findFailed finds the next background migration in the failed state and the next eligible failed job to be run under the migration.
func findFailed(ctx context.Context, bbmStore datastore.BackgroundMigrationStore) (*models.BackgroundMigration, *models.BackgroundMigrationJob, error) {
	var (
		job *models.BackgroundMigrationJob
		bbm *models.BackgroundMigration
		err error
	)
	// Find failed background migrations
	bbm, err = bbmStore.FindNextByStatus(ctx, models.BackgroundMigrationFailed)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find failed background migrations: %w", err)
	}
	// If failing background migration found, find failed jobs and rerun them
	if bbm != nil {
		// Find failed jobs for background migration
		job, err = bbmStore.FindJobWithStatus(ctx, bbm.ID, models.BackgroundMigrationFailed)
		if err != nil {
			return bbm, nil, fmt.Errorf("failed to find failed background migrations job: %w", err)
		}
	}
	return bbm, job, err
}

// ExecuteJob executes a background migration's job unit of work.
func (jw *SyncWorker) ExecuteJob(ctx context.Context, bbmStore datastore.BackgroundMigrationStore, job *models.BackgroundMigrationJob) error {
	// Find the job function from the registered work map and execute it.
	if work, found := jw.work[job.JobName]; found {
		for i := 0; i < jw.maxJobAttempt; i++ {
			jw.logger.WithFields(log.Fields{"sync_attempt": i + 1}).Info("executing job")
			err := work.Do(ctx, jw.db, job.PaginationTable, job.PaginationColumn, job.StartID, job.EndID, job.BatchSize)
			if err != nil {
				jw.logger.WithError(err).Error("failed executing job, retrying")
			} else {
				jw.logger.Info("job execution completed successfully, updating job status")
				job.Status = models.BackgroundMigrationFinished
				return bbmStore.UpdateJobStatus(ctx, job)
			}
		}
	} else {
		jw.logger.WithFields(log.Fields{"job_name": job.JobName}).Error("job function not found in work map")
		return ErrWorkFunctionNotFound
	}
	jw.logger.Error("maximum number of job attempts reached")
	return ErrMaxJobAttemptsReached
}

// GrabLock attempts to grab the distributed lock used for coordination between all Background Migration processes.
func (jw *SyncWorker) GrabLock(ctx context.Context, bbmStore datastore.BackgroundMigrationStore) (err error) {
	jw.logger.Info("attempting to acquire lock...")

	err = bbmStore.SyncLock(ctx)
	if err != nil {
		jw.logger.WithError(err).Error("failed to acquire lock")
		return err
	}
	jw.logger.Info("successfully obtained lock")
	return nil
}

// enrichJobWithBBMAttributes enriches the job with attributes from the background migration.
func enrichJobWithBBMAttributes(job *models.BackgroundMigrationJob, bbm *models.BackgroundMigration) {
	if job != nil && bbm != nil {
		job.JobName = bbm.JobName
		job.PaginationTable = bbm.TargetTable
		job.PaginationColumn = bbm.TargetColumn
		job.BatchSize = bbm.BatchSize
	}
}

// FinishedMigrationCount returns the count of background migrations completed in the last run.
func (jw *SyncWorker) FinishedMigrationCount() int {
	return jw.lastRunCompletedBBMs
}
