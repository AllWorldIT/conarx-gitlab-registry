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
)

const (
	componentKey             = "component"
	workerName               = "registry.bbm.Worker"
	defaultMaxJobAttempt     = 5
	defaultJobInterval       = 1 * time.Minute
	jobIntervalJitterSeconds = 5

	// Background Migration job log keys
	jobIDKey        = "job_id"
	jobBBMIDKey     = "job_bbm_id"
	jobNameKey      = "job_name"
	jobAttemptsKey  = "job_attempts"
	jobStartIDKey   = "job_start_id"
	jobEndIDKey     = "job_end_id"
	jobBatchSizeKey = "job_batch_size"
	jobStatusKey    = "job_status"

	// Background Migration log keys
	bbmIDKey           = "bbm_id"
	bbmNameKey         = "bbm_name"
	bbmBatchSizeKey    = "bbm_batch_size"
	bbmStatusKey       = "bbm_status"
	bbmJobSignatureKey = "bbm_job_signature_key"
)

var (
	// ErrLockInUse is returned when a  migration job worker can not obtain the Background Migration distributed lock.
	// This is most likely to occur when the lock has not been released by another job worker.
	ErrLockInUse = errors.New("background migration lock is already taken")
	// ErrJobEndpointNotFound is returned when a job's endpoint can not be calculated.
	ErrJobEndpointNotFound = errors.New("job endpoint could not be calculated")
	// ErrMaxJobAttemptsReached is returned when the maximum attempt to try a job has elapsed.
	ErrMaxJobAttemptsReached = errors.New("maximum job attempt reached")
	// ErrWorkFunctionNotFound is returned when a refrenced job has no corresponding work function.
	ErrWorkFunctionNotFound = errors.New("work function not found for")
)

// status are the Background Migration and Background Migration job statuses as defined in:
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/database-background-migrations.md#batch-background-migration-bbm-creation
type status int

const (
	paused status = iota
	active
	finished
	failed
	running
)

func (s status) String() string {
	switch s {
	case paused:
		return "paused"
	case active:
		return "active"
	case finished:
		return "finished"
	case failed:
		return "failed"
	case running:
		return "running"
	}
	return "unknown"
}

type WorkFunc func(ctx context.Context, db *datastore.DB, paginationColumn string, paginationAfter, paginationBefore, limit int) error

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
	db            *datastore.DB
	jobInterval   time.Duration
	maxJobAttempt int
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
func WithDB(db *datastore.DB) WorkerOption {
	return func(jw *Worker) {
		jw.db = db
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
				//cleanup
				jw.logger.Info("received shutdown signal: Shutting down...")
				close(gracefullFinish)
			// A period has elapsed, time to try to find and execute any availaible jobs.
			case <-ticker.C:
				jw.logger.Info("starting worker run...")
				jw.run(ctx)
			}
		}
	}()

	return gracefullFinish, nil
}

// run attempts to obtain the Background Migration distributed lock, before proceeding to find and execute any applicable Background Migration jobs.
func (jw *Worker) run(ctx context.Context) {
	// Cancel the job worker context when the `jobInterval` duration elapses
	ctx, cancel := context.WithTimeout(ctx, jw.jobInterval)
	defer cancel()

	// Grab distributed lock
	jw.logger.Info("obtaining Lock...")
	releaseFunc, err := jw.GrabLock(ctx)
	defer releaseFunc()
	if err != nil {
		if errors.Is(err, ErrLockInUse) {
			jw.logger.WithError(err).Info("failed to obtain lock")
		} else {
			// TODO: Surface any other errors in sentry
			jw.logger.WithError(err).Error("failed to obtain lock")
		}
		return
	}

	// Search for availaible jobs
	jw.logger.Info("searching for job...")
	job, err := jw.FindJob(ctx, jw.maxJobAttempt)
	if err != nil {
		// TODO: Surface any other errors in sentry
		jw.logger.WithError(err).Error("failed to find job")
		return
	}
	if job == nil {
		jw.logger.Info("no jobs to run...")
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
		jobStatusKey:    status(job.Status).String(),
	})

	// A job was found, lets execute it
	jw.logger.Info("job found! executing Job")
	err = jw.ExecuteJob(ctx, job, jw.Work)
	if err != nil {
		// TODO: Surface other error in sentry
		l.WithError(err).Error("failed to execute job")
		return
	}
	l.Info("executed background migration job")

}

// GrabLock attempts to grab the distributed lock used for co-ordination between all Background Migration processes.
func (jw *Worker) GrabLock(ctx context.Context) (releaseFunc func(), err error) {
	releaseFunc = func() {}

	bbmStore := datastore.NewBackgroundMigrationStore(jw.db)
	// Acquire a lock so no other Background Migration process can run.
	err = bbmStore.Lock(ctx)
	if err != nil {
		return releaseFunc, err
	}
	jw.logger.Info("obtained lock")

	return func() {
		if err := bbmStore.Unlock(ctx); err != nil {
			// TODO: Surface error in sentry
			jw.logger.WithError(err).Error("failed to release background migration lock")
			return
		}
		jw.logger.Info("released lock")
	}, nil
}

// FindJob checks for any Background Migration job that needs to be executed.
// If a job needs to be executed it either fetches the job or creates the job
// associated with the chosen Background Migration.
func (jw *Worker) FindJob(ctx context.Context, maxJobAttempts int) (*models.BackgroundMigrationJob, error) {

	bbmStore := datastore.NewBackgroundMigrationStore(jw.db)

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
		bbmStatusKey:       status(bbm.Status).String(),
		bbmBatchSizeKey:    bbm.BatchSize,
	})

	l.Info("a background migration was found that needs to be executed")

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

		l := l.WithFields(log.Fields{
			jobIDKey:        job.ID,
			jobBBMIDKey:     job.BBMID,
			jobNameKey:      job.JobName,
			jobAttemptsKey:  job.Attempts,
			jobStartIDKey:   job.StartID,
			jobEndIDKey:     job.EndID,
			jobBatchSizeKey: job.BatchSize,
			jobStatusKey:    status(job.Status).String(),
		})

		// check that the selected job does not exceed the configured `MaxJobAttempt`
		if job.Attempts >= maxJobAttempts {
			l.WithError(ErrMaxJobAttemptsReached).Error("marking background migration as failed due to job failure")
			return nil, bbmStore.UpdateStatus(ctx, bbm.ID, int(failed))
		}
		return job, nil
	}
}

// ExecuteJob attempts to execute the function associated with a Background Migration job from the job's start-end range.
func (jw *Worker) ExecuteJob(ctx context.Context, job *models.BackgroundMigrationJob, registeredWork map[string]Work) error {

	bbmStore := datastore.NewBackgroundMigrationStore(jw.db)

	// update the job attempts
	err := bbmStore.IncrementJobAttempts(ctx, job.ID)
	if err != nil {
		return err
	}

	// find the job function from the registered work map and execute it.
	if work, found := registeredWork[job.JobName]; found {
		err := work.Do(ctx, jw.db, job.PaginationColumn, job.StartID, job.EndID, job.BatchSize)
		if err != nil {
			// mark job as failed
			err = bbmStore.UpdateJobStatus(ctx, job.ID, int(failed))
			if err != nil {
				return err
			}
			return fmt.Errorf("failed executing job for id %d: %w", job.ID, err)
		} else {
			// mark job as finished
			return bbmStore.UpdateJobStatus(ctx, job.ID, int(finished))
		}
	} else {
		// mark job as failed
		err := bbmStore.UpdateJobStatus(ctx, job.ID, int(failed))
		if err != nil {
			return err
		}
		return ErrWorkFunctionNotFound
	}
}

// findRetryableJobs looks for jobs that failed prior in the scope of a specific Background Migration.
// if no failed jobs are found in the Background Migration it sets the status of the Background Migration to finished.
func findRetryableJobs(ctx context.Context, bbmStore datastore.BackgroundMigrationStore, bbm *models.BackgroundMigration) (*models.BackgroundMigrationJob, error) {

	job, err := bbmStore.FindJobWithStatus(ctx, bbm.ID, int(failed))
	if err != nil {
		return nil, err
	}
	// if there are no jobs that failed update the migration to finished state
	if job == nil {
		return nil, bbmStore.UpdateStatus(ctx, bbm.ID, int(finished))
	}
	// Otherwise, decorate any found job with the parent (Background Migration) attributes.
	job.JobName = bbm.JobName
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
		err = bbmStore.UpdateStatus(ctx, bbm.ID, int(running))
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
	foundEnd, err := bbmStore.FindJobEndFromJobStart(ctx, bbm.TargetTable, bbm.TargetColumn, start, last, bbm.BatchSize)
	if err != nil {
		return nil, err
	}
	if foundEnd == nil {
		return nil, ErrJobEndpointNotFound
	}
	end := *foundEnd

	// create the job representation
	newJob := &models.BackgroundMigrationJob{
		BBMID:     bbm.ID,
		StartID:   start,
		EndID:     end,
		BatchSize: bbm.BatchSize,
		Status:    int(active),
	}
	job, err := bbmStore.CreateNewJob(ctx, newJob)
	if err != nil {
		return nil, err
	}
	// decorate job with some of the parent (Background Migration) attributes
	job.JobName = bbm.JobName
	job.PaginationColumn = bbm.TargetColumn
	job.BatchSize = bbm.BatchSize

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
