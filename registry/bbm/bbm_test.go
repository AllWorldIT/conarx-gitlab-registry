package bbm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/docker/distribution/log"
	bbm_mocks "github.com/docker/distribution/registry/bbm/mocks"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/mocks"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	workFunctionName = "doSomething"
	errAnError       = errors.New("an error")
	bbm              = &models.BackgroundMigration{ID: 1, EndID: 2, JobName: workFunctionName}
	job              = &models.BackgroundMigrationJob{ID: 1, JobName: workFunctionName}
	workFunc         = map[string]Work{
		job.JobName: {
			Name: workFunctionName,
			Do:   doErrorReturn(errAnError),
		},
	}
)

func doErrorReturn(ret error) func(_ context.Context, _ datastore.Handler, _, _ string, _, _, _ int) error {
	return func(_ context.Context, _ datastore.Handler, _, _ string, _, _, _ int) error {
		return ret
	}
}

// TestFindJob_Errors tests all the error paths on the `FindJob` method.
func TestFindJob_Errors(t *testing.T) {
	// Declare the context for the tests
	ctx := context.TODO()

	tt := []struct {
		name       string
		setupMocks func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore
		worker     *Worker
	}{
		{
			name:   "error when checking for next job",
			worker: NewWorker(nil),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				bbmStoreMock.EXPECT().FindNext(ctx).Return(nil, errAnError).Times(1)
				return bbmStoreMock
			},
		},
		{
			name:   "error when next job function not found and failed update bbm",
			worker: NewWorker(nil),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				failedBBM := bbm
				failedBBM.Status = models.BackgroundMigrationFailed
				failedBBM.ErrorCode = models.InvalidJobSignatureBBMErrCode

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, bbm).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:   "unknown error when validating table and column",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:   "error when updating bbm after failed validation for table",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				failedBBM := bbm
				failedBBM.Status = models.BackgroundMigrationFailed
				failedBBM.ErrorCode = models.InvalidTableBBMErrCode

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(errors.Join(errAnError, datastore.ErrUnknownTable)).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, failedBBM).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:   "error when updating bbm after failed validation for column",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				failedBBM := bbm
				failedBBM.Status = models.BackgroundMigrationFailed
				failedBBM.ErrorCode = models.InvalidColumnBBMErrCode

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(errors.Join(errAnError, datastore.ErrUnknownColumn)).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, failedBBM).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:   "error when checking if all jobs for selected bbm have run at least once",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(nil, errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:   "error when checking for last run job of bbm",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, bbm).Return(nil, errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:   "error when updating status of new migration to running",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, bbm).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, bbm).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:   "error when finding a job end cursor",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				lastJob := &models.BackgroundMigrationJob{}

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, bbm).Return(lastJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobEndFromJobStart(ctx, bbm.TargetTable, bbm.TargetColumn, max(lastJob.StartID+1, bbm.StartID), bbm.EndID, bbm.BatchSize).Return(0, errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:   "error when creating a new job",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				lastJob := &models.BackgroundMigrationJob{}
				expectEndId := 2

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, bbm).Return(lastJob, nil).Times(1),

					bbmStoreMock.EXPECT().FindJobEndFromJobStart(ctx, bbm.TargetTable, bbm.TargetColumn, max(lastJob.StartID+1, bbm.StartID), bbm.EndID, bbm.BatchSize).Return(expectEndId, nil).Times(1),
					bbmStoreMock.EXPECT().CreateNewJob(ctx, &models.BackgroundMigrationJob{
						BBMID:            bbm.ID,
						StartID:          max(lastJob.StartID+1, bbm.StartID),
						EndID:            expectEndId,
						BatchSize:        bbm.BatchSize,
						JobName:          bbm.JobName,
						PaginationColumn: bbm.TargetColumn,
					}).Return(errAnError))

				return bbmStoreMock
			},
		},
		{
			name:   "error when finding failed retryable job",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				finalJob := &models.BackgroundMigrationJob{}

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(finalJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, bbm.ID, models.BackgroundMigrationFailed).Return(nil, errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:   "no retryable jobs but error when updating bbm to finished",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				finalJob := &models.BackgroundMigrationJob{}
				finshedBBM := bbm
				bbm.Status = models.BackgroundMigrationFinished

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(finalJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, bbm.ID, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, finshedBBM).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:   "error when updating job failure attempts",
			worker: NewWorker(workFunc, WithMaxJobAttempt(1)),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				finalJob := &models.BackgroundMigrationJob{}
				retryableJob := &models.BackgroundMigrationJob{Attempts: 1}
				failedBBM := bbm
				failedBBM.Status = models.BackgroundMigrationFailed

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(finalJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, bbm.ID, models.BackgroundMigrationFailed).Return(retryableJob, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, failedBBM).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			test := test
			t.Parallel()
			bbmStore := test.setupMocks(gomock.NewController(t))
			job, err := test.worker.FindJob(ctx, bbmStore)
			require.ErrorIs(t, err, errAnError)
			require.Nil(t, job)
		})
	}
}

// TestFindJob tests all the happy paths on the `FindJob` method.
func TestFindJob(t *testing.T) {
	ctx := context.TODO()
	expectedJob := models.BackgroundMigrationJob{
		BBMID:   1,
		EndID:   3,
		JobName: workFunctionName,
	}

	tt := []struct {
		name        string
		setupMocks  func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore
		worker      *Worker
		expectedJob *models.BackgroundMigrationJob
	}{
		{
			name:   "no pending background migration found",
			worker: NewWorker(nil),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				bbmStoreMock.EXPECT().FindNext(ctx).Return(nil, nil).Times(1)
				return bbmStoreMock
			},
			expectedJob: nil,
		},
		{
			name:   "found a new job to run for an active migration",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				jobEndID := 3
				// create the job representation and decorate the job with some of the parent (Background Migration) attributes
				job := &models.BackgroundMigrationJob{
					BBMID:            bbm.ID,
					StartID:          bbm.StartID,
					EndID:            jobEndID,
					BatchSize:        bbm.BatchSize,
					JobName:          bbm.JobName,
					PaginationColumn: bbm.TargetColumn,
				}

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, bbm).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, bbm).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobEndFromJobStart(ctx, bbm.TargetTable, bbm.TargetColumn, bbm.StartID, bbm.EndID, bbm.BatchSize).Return(jobEndID, nil).Times(1),
					bbmStoreMock.EXPECT().CreateNewJob(ctx, job).Return(nil),
				)
				return bbmStoreMock
			},
			expectedJob: &expectedJob,
		},
		{
			name:   "found a new job to run for an already running migration",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				jobEndID := 3
				// create the job representation and decorate the job with some of the parent (Background Migration) attributes
				job := &models.BackgroundMigrationJob{
					BBMID:            bbm.ID,
					StartID:          bbm.StartID,
					EndID:            jobEndID,
					BatchSize:        bbm.BatchSize,
					JobName:          bbm.JobName,
					PaginationColumn: bbm.TargetColumn,
				}

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, bbm).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, bbm).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobEndFromJobStart(ctx, bbm.TargetTable, bbm.TargetColumn, bbm.StartID, bbm.EndID, bbm.BatchSize).Return(jobEndID, nil).Times(1),
					bbmStoreMock.EXPECT().CreateNewJob(ctx, job).Return(nil),
				)

				return bbmStoreMock
			},
			expectedJob: &expectedJob,
		},
		{
			name:   "found job is greater than nigration end bound",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				foundJob := job
				foundJob.EndID = bbm.EndID + 1

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, bbm).Return(foundJob, nil).Times(1),
				)

				return bbmStoreMock
			},
			expectedJob: nil,
		},
		{
			name:   "found retryable job",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				finalJob := &models.BackgroundMigrationJob{}

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(finalJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, bbm.ID, models.BackgroundMigrationFailed).Return(&expectedJob, nil).Times(1),
				)
				return bbmStoreMock
			},
			expectedJob: &expectedJob,
		},
		{
			name:   "no retryable or new jobs left in a migration",
			worker: NewWorker(workFunc),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				finalJob := &models.BackgroundMigrationJob{}

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(finalJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, bbm.ID, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, bbm).Return(nil).Times(1),
				)

				return bbmStoreMock
			},
			expectedJob: nil,
		},
		{
			name:   "found job exceeds migration max attempts",
			worker: NewWorker(workFunc, WithMaxJobAttempt(-1)),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				finalJob := &models.BackgroundMigrationJob{}

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetTable).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(finalJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, bbm.ID, models.BackgroundMigrationFailed).Return(&expectedJob, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, bbm).Return(nil).Times(1),
				)
				return bbmStoreMock
			},
			expectedJob: nil,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			test := test
			bbmStore := test.setupMocks(gomock.NewController(t))
			job, err := test.worker.FindJob(ctx, bbmStore)
			require.NoError(t, err)
			require.Equal(t, test.expectedJob, job)
		})
	}
}

// TestExecuteJob_Errors tests all the error paths on the `ExecuteJob` method.
func TestExecuteJob_Errors(t *testing.T) {
	ctx := context.TODO()
	tt := []struct {
		name          string
		setupMocks    func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore
		worker        *Worker
		expectedError error
	}{
		{
			name:   "error when incrementing job attempts",
			worker: NewWorker(nil),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				bbmStoreMock.EXPECT().IncrementJobAttempts(ctx, job.ID).Return(errAnError).Times(1)
				return bbmStoreMock
			},
			expectedError: errAnError,
		},
		{
			name:   "error on update status when unrecognized job signature",
			worker: NewWorker(nil),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				failedJob := job
				failedJob.Status = models.BackgroundMigrationFailed
				failedJob.ErrorCode = models.InvalidJobSignatureBBMErrCode

				gomock.InOrder(
					bbmStoreMock.EXPECT().IncrementJobAttempts(ctx, job.ID).Return(nil).Times(1),
					bbmStoreMock.EXPECT().UpdateJobStatus(ctx, failedJob).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
			expectedError: errAnError,
		},
		{
			name: "error on update status when job executed successfully",
			worker: NewWorker(map[string]Work{
				job.JobName: {
					Name: job.JobName,
					Do:   doErrorReturn(nil),
				},
			}),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				finishedJob := job
				finishedJob.Status = models.BackgroundMigrationFinished

				gomock.InOrder(
					bbmStoreMock.EXPECT().IncrementJobAttempts(ctx, job.ID).Return(nil).Times(1),
					bbmStoreMock.EXPECT().UpdateJobStatus(ctx, finishedJob).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
			expectedError: errAnError,
		},
		{
			name: "error on update status when job failed execution",
			worker: NewWorker(map[string]Work{
				job.JobName: {
					Name: job.JobName,
					Do:   doErrorReturn(errAnError),
				},
			}),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				failedJob := job
				failedJob.Status = models.BackgroundMigrationFailed
				failedJob.ErrorCode = models.UnknownBBMErrorCode

				gomock.InOrder(
					bbmStoreMock.EXPECT().IncrementJobAttempts(ctx, job.ID).Return(nil).Times(1),
					bbmStoreMock.EXPECT().UpdateJobStatus(ctx, failedJob).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
			expectedError: errAnError,
		},
		{
			name: "error when job failed execution",
			worker: NewWorker(map[string]Work{
				job.JobName: {
					Name: job.JobName,
					Do:   doErrorReturn(errAnError),
				},
			}),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				failedJob := job
				failedJob.Status = models.BackgroundMigrationFailed
				failedJob.ErrorCode = models.UnknownBBMErrorCode

				gomock.InOrder(
					bbmStoreMock.EXPECT().IncrementJobAttempts(ctx, job.ID).Return(nil).Times(1),
					bbmStoreMock.EXPECT().UpdateJobStatus(ctx, failedJob).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
			expectedError: errAnError,
		},
		{
			name:   "error when job function not found",
			worker: NewWorker(nil),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				failedJob := job
				failedJob.Status = models.BackgroundMigrationFailed
				failedJob.ErrorCode = models.InvalidJobSignatureBBMErrCode

				gomock.InOrder(
					bbmStoreMock.EXPECT().IncrementJobAttempts(ctx, job.ID).Return(nil).Times(1),
					bbmStoreMock.EXPECT().UpdateJobStatus(ctx, failedJob).Return(nil).Times(1),
				)

				return bbmStoreMock
			},
			expectedError: ErrWorkFunctionNotFound,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			test := test
			t.Parallel()
			bbmStore := test.setupMocks(gomock.NewController(t))
			err := test.worker.ExecuteJob(ctx, bbmStore, job)
			require.ErrorIs(t, err, test.expectedError)
		})
	}
}

// TestExecuteJob tests all the happy paths on the `ExecuteJob` method.
func TestExecuteJob(t *testing.T) {
	ctx := context.TODO()
	worker := NewWorker(map[string]Work{
		job.JobName: {
			Name: job.JobName,
			Do:   doErrorReturn(nil),
		},
	})

	setupMocks := func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
		bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
		finishedJob := job
		finishedJob.Status = models.BackgroundMigrationFinished

		gomock.InOrder(
			bbmStoreMock.EXPECT().IncrementJobAttempts(ctx, job.ID).Return(nil).Times(1),
			bbmStoreMock.EXPECT().UpdateJobStatus(ctx, finishedJob).Return(nil).Times(1),
		)

		return bbmStoreMock
	}

	err := worker.ExecuteJob(ctx, setupMocks(gomock.NewController(t)), job)
	require.NoError(t, err)
}

// TestGrabLock tests all the paths on the `GrabLock` method.
func TestGrabLock(t *testing.T) {
	ctx := context.TODO()
	worker := NewWorker(nil)
	setupMocks := func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
		bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)

		gomock.InOrder(
			bbmStoreMock.EXPECT().Lock(ctx).Return(errAnError).Times(1),
			bbmStoreMock.EXPECT().Lock(ctx).Return(nil).Times(1),
		)

		return bbmStoreMock
	}

	bbmStore := setupMocks(gomock.NewController(t))
	err := worker.GrabLock(ctx, bbmStore)
	require.ErrorIs(t, err, errAnError)
	err = worker.GrabLock(ctx, bbmStore)
	require.NoError(t, err)
}

// TestRegisterWork_Errors tests all the error paths on the `RegisterWork` function.
func TestRegisterWork_Errors(t *testing.T) {
	name := "repeated_name"
	work := []Work{
		{
			Name: name,
			Do:   doErrorReturn(nil),
		},
		{
			Name: name,
			Do:   doErrorReturn(nil),
		},
	}
	worker, err := RegisterWork(work)
	require.Nil(t, worker)
	require.Error(t, err)
}

// TestRegisterWork tests all the happy paths on the `RegisterWork` function.
func TestRegisterWork(t *testing.T) {
	wh := bbm_mocks.NewMockHandler(gomock.NewController(t))
	tt := []struct {
		name           string
		opts           []WorkerOption
		expectedWorker func() *Worker
	}{
		{
			name: "no options",
			expectedWorker: func() *Worker {
				w := &Worker{
					Work:          make(map[string]Work, 0),
					logger:        log.GetLogger().WithFields(log.Fields{componentKey: workerName}),
					jobInterval:   defaultJobInterval,
					maxJobAttempt: defaultMaxJobAttempt,
				}

				w.wh = w
				return w
			},
		},
		{
			name: "WithJobInterval",
			opts: []WorkerOption{WithJobInterval(1 * time.Second)},
			expectedWorker: func() *Worker {
				w := &Worker{
					Work:          make(map[string]Work, 0),
					logger:        log.GetLogger().WithFields(log.Fields{componentKey: workerName}),
					jobInterval:   1 * time.Second,
					maxJobAttempt: defaultMaxJobAttempt,
				}
				w.wh = w
				return w
			},
		},
		{
			name: "WithMaxJobAttempt",
			opts: []WorkerOption{WithMaxJobAttempt(9)},
			expectedWorker: func() *Worker {
				w := &Worker{
					Work:          make(map[string]Work, 0),
					logger:        log.GetLogger().WithFields(log.Fields{componentKey: workerName}),
					jobInterval:   defaultJobInterval,
					maxJobAttempt: 9,
				}
				w.wh = w
				return w
			},
		},
		{
			name: "WithDB",
			opts: []WorkerOption{WithDB(&datastore.DB{})},
			expectedWorker: func() *Worker {
				w := &Worker{
					Work:          make(map[string]Work, 0),
					db:            &datastore.DB{},
					logger:        log.GetLogger().WithFields(log.Fields{componentKey: workerName}),
					jobInterval:   defaultJobInterval,
					maxJobAttempt: defaultMaxJobAttempt,
				}
				w.wh = w
				return w
			},
		},
		{
			name: "WithLogger",
			opts: []WorkerOption{WithLogger(log.GetLogger().WithFields(log.Fields{"random_key": "random_value"}))},
			expectedWorker: func() *Worker {
				w := &Worker{
					Work:          make(map[string]Work, 0),
					logger:        log.GetLogger().WithFields(log.Fields{"random_key": "random_value", componentKey: workerName}),
					jobInterval:   defaultJobInterval,
					maxJobAttempt: defaultMaxJobAttempt,
				}
				w.wh = w
				return w
			},
		},
		{
			name: "WithHandler",
			opts: []WorkerOption{WithHandler(wh)},
			expectedWorker: func() *Worker {
				w := &Worker{
					Work:          make(map[string]Work, 0),
					logger:        log.GetLogger().WithFields(log.Fields{componentKey: workerName}),
					jobInterval:   defaultJobInterval,
					maxJobAttempt: defaultMaxJobAttempt,
					wh:            wh,
				}
				return w
			},
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			test := test
			t.Parallel()
			worker, err := RegisterWork(nil, test.opts...)
			require.NoError(t, err)
			require.Equal(t, test.expectedWorker(), worker)
		})
	}
}

// TestWorker_Run tests the run methon of worker.
func TestWorker_Run(t *testing.T) {
	tests := []struct {
		name       string
		setupMocks func(ctrl *gomock.Controller) *Worker
	}{
		{
			name: "transaction creation failure",
			setupMocks: func(ctrl *gomock.Controller) *Worker {
				dbMock := mocks.NewMockHandler(ctrl)
				worker := NewWorker(nil, WithDB(dbMock))

				dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(nil, errAnError).Times(1)
				return worker
			},
		},
		{
			name: "failed to obtain lock",
			setupMocks: func(ctrl *gomock.Controller) *Worker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewWorker(nil, WithDB(dbMock), WithHandler(handler))
				bbmStore := datastore.NewBackgroundMigrationStore(txMock)

				gomock.InOrder(
					dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(txMock, nil).Times(1),
					handler.EXPECT().GrabLock(gomock.Any(), bbmStore).Return(errAnError).Times(1),
					txMock.EXPECT().Rollback().Return(nil).Times(1),
				)

				return worker
			},
		},
		{
			name: "job retrieval failure",
			setupMocks: func(ctrl *gomock.Controller) *Worker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewWorker(nil, WithDB(dbMock), WithHandler(handler))
				bbmStore := datastore.NewBackgroundMigrationStore(txMock)

				gomock.InOrder(
					dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(txMock, nil).Times(1),
					handler.EXPECT().GrabLock(gomock.Any(), bbmStore).Return(nil).Times(1),
					handler.EXPECT().FindJob(gomock.Any(), bbmStore).Return(nil, errAnError).Times(1),
					txMock.EXPECT().Rollback().Return(nil).Times(1),
				)

				return worker
			},
		},
		{
			name: "no jobs available",
			setupMocks: func(ctrl *gomock.Controller) *Worker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewWorker(nil, WithDB(dbMock), WithHandler(handler))
				bbmStore := datastore.NewBackgroundMigrationStore(txMock)

				gomock.InOrder(
					dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(txMock, nil).Times(1),
					handler.EXPECT().GrabLock(gomock.Any(), bbmStore).Return(nil).Times(1),
					handler.EXPECT().FindJob(gomock.Any(), bbmStore).Return(nil, nil).Times(1),
					txMock.EXPECT().Commit().Return(nil).Times(1),
					txMock.EXPECT().Rollback().Return(nil).Times(1),
				)

				return worker
			},
		},
		{
			name: "no jobs available commit failure",
			setupMocks: func(ctrl *gomock.Controller) *Worker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewWorker(nil, WithDB(dbMock), WithHandler(handler))
				bbmStore := datastore.NewBackgroundMigrationStore(txMock)

				gomock.InOrder(
					dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(txMock, nil).Times(1),
					handler.EXPECT().GrabLock(gomock.Any(), bbmStore).Return(nil).Times(1),
					handler.EXPECT().FindJob(gomock.Any(), bbmStore).Return(nil, nil).Times(1),
					txMock.EXPECT().Commit().Return(errAnError).Times(1),
					txMock.EXPECT().Rollback().Return(nil).Times(1),
				)

				return worker
			},
		},
		{
			name: "job execution failure",
			setupMocks: func(ctrl *gomock.Controller) *Worker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewWorker(nil, WithDB(dbMock), WithHandler(handler))
				bbmStore := datastore.NewBackgroundMigrationStore(txMock)

				gomock.InOrder(
					dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(txMock, nil).Times(1),
					handler.EXPECT().GrabLock(gomock.Any(), bbmStore).Return(nil).Times(1),
					handler.EXPECT().FindJob(gomock.Any(), bbmStore).Return(job, nil).Times(1),
					handler.EXPECT().ExecuteJob(gomock.Any(), bbmStore, job).Return(errAnError).Times(1),
					txMock.EXPECT().Rollback().Return(nil).Times(1),
				)

				return worker
			},
		},
		{
			name: "post-execution transaction commit failure",
			setupMocks: func(ctrl *gomock.Controller) *Worker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewWorker(nil, WithDB(dbMock), WithHandler(handler))
				bbmStore := datastore.NewBackgroundMigrationStore(txMock)

				gomock.InOrder(
					dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(txMock, nil).Times(1),
					handler.EXPECT().GrabLock(gomock.Any(), bbmStore).Return(nil).Times(1),
					handler.EXPECT().FindJob(gomock.Any(), bbmStore).Return(job, nil).Times(1),
					handler.EXPECT().ExecuteJob(gomock.Any(), bbmStore, job).Return(nil).Times(1),
					txMock.EXPECT().Commit().Return(errAnError).Times(1),
					txMock.EXPECT().Rollback().Return(nil).Times(1),
				)

				return worker
			},
		},
		{
			name: "successful run",
			setupMocks: func(ctrl *gomock.Controller) *Worker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewWorker(nil, WithDB(dbMock), WithHandler(handler))
				bbmStore := datastore.NewBackgroundMigrationStore(txMock)

				gomock.InOrder(
					dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(txMock, nil).Times(1),
					handler.EXPECT().GrabLock(gomock.Any(), bbmStore).Return(nil).Times(1),
					handler.EXPECT().FindJob(gomock.Any(), bbmStore).Return(job, nil).Times(1),
					handler.EXPECT().ExecuteJob(gomock.Any(), bbmStore, job).Return(nil).Times(1),
					txMock.EXPECT().Commit().Return(nil).Times(1),
					txMock.EXPECT().Rollback().Return(nil).Times(1),
				)

				return worker
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test := test
			t.Parallel()
			worker := test.setupMocks(gomock.NewController(t))
			// Execute the run method and assert the necessary methods/functions are called
			worker.run(context.TODO())
		})
	}
}
