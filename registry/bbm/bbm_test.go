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

	testCases := []struct {
		name          string
		setupMocks    func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore
		worker        *Worker
		expectedError error
	}{
		{
			name:          "error when checking for next job",
			worker:        NewWorker(nil),
			expectedError: errAnError,
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				bbmStoreMock.EXPECT().FindNext(ctx).Return(nil, errAnError).Times(1)
				return bbmStoreMock
			},
		},
		{
			name:          "error when next job function not found and failed update bbm",
			worker:        NewWorker(nil),
			expectedError: ErrWorkFunctionNotFound,
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				failedBBM := bbm
				failedBBM.Status = models.BackgroundMigrationFailed
				failedBBM.ErrorCode = models.InvalidJobSignatureBBMErrCode

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:          "unknown error when validating table and column",
			worker:        NewWorker(workFunc),
			expectedError: errAnError,
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:          "error when updating bbm after failed validation for table",
			worker:        NewWorker(workFunc),
			expectedError: errAnError,
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				failedBBM := bbm
				failedBBM.Status = models.BackgroundMigrationFailed
				failedBBM.ErrorCode = models.InvalidTableBBMErrCode

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(errors.Join(errAnError, datastore.ErrUnknownTable)).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, failedBBM).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:          "error when updating bbm after failed validation for column",
			worker:        NewWorker(workFunc),
			expectedError: errAnError,
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				failedBBM := bbm
				failedBBM.Status = models.BackgroundMigrationFailed
				failedBBM.ErrorCode = models.InvalidColumnBBMErrCode

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(errors.Join(errAnError, datastore.ErrUnknownColumn)).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, failedBBM).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:          "error when checking if all jobs for selected bbm have run at least once",
			worker:        NewWorker(workFunc),
			expectedError: errAnError,
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(nil, errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:          "error when checking for last run job of bbm",
			worker:        NewWorker(workFunc),
			expectedError: errAnError,
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, bbm).Return(nil, errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:          "error when updating status of new migration to running",
			worker:        NewWorker(workFunc),
			expectedError: errAnError,
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				bbmLocal := *bbm
				bbmLocal.Status = models.BackgroundMigrationActive
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(&bbmLocal, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbmLocal.TargetTable, bbmLocal.TargetColumn).Return(nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, &bbmLocal).Return(errAnError).Times(1),
				)
				return bbmStoreMock
			},
		},
		{
			name:          "error when finding a job end cursor",
			worker:        NewWorker(workFunc),
			expectedError: errAnError,
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				lastJob := &models.BackgroundMigrationJob{}

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, bbm).Return(lastJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobEndFromJobStart(ctx, bbm.TargetTable, bbm.TargetColumn, max(lastJob.StartID+1, bbm.StartID), bbm.EndID, bbm.BatchSize).Return(0, errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:          "error when creating a new job",
			worker:        NewWorker(workFunc),
			expectedError: errAnError,
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				lastJob := &models.BackgroundMigrationJob{}
				expectEndId := 2

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(nil).Times(1),
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
			name:          "error when finding failed retryable job",
			worker:        NewWorker(workFunc),
			expectedError: errAnError,
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				finalJob := &models.BackgroundMigrationJob{}

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(finalJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, bbm.ID, models.BackgroundMigrationFailed).Return(nil, errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:          "no retryable jobs but error when updating bbm to finished",
			worker:        NewWorker(workFunc),
			expectedError: errAnError,
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				finalJob := &models.BackgroundMigrationJob{}
				finshedBBM := bbm
				bbm.Status = models.BackgroundMigrationFinished

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(finalJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, bbm.ID, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, finshedBBM).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
		{
			name:          "error when updating job failure attempts",
			worker:        NewWorker(workFunc, WithMaxJobAttempt(1)),
			expectedError: errAnError,
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				finalJob := &models.BackgroundMigrationJob{}
				retryableJob := &models.BackgroundMigrationJob{Attempts: 1}
				failedBBM := bbm
				failedBBM.Status = models.BackgroundMigrationFailed

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNext(ctx).Return(bbm, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(finalJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, bbm.ID, models.BackgroundMigrationFailed).Return(retryableJob, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, failedBBM).Return(errAnError).Times(1),
				)

				return bbmStoreMock
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			test := tc
			tt.Parallel()
			bbmStore := test.setupMocks(gomock.NewController(tt))
			job, err := test.worker.FindJob(ctx, bbmStore)
			require.ErrorIs(tt, err, test.expectedError)
			require.Nil(tt, job)
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

	testCases := []struct {
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
				// use a local copy to avoid shared global mutations across parallel tests
				bbmLocal := *bbm
				bbmLocal.Status = models.BackgroundMigrationActive
				bbmLocalRunning := bbmLocal
				bbmLocalRunning.Status = models.BackgroundMigrationRunning
				// create the job representation and decorate the job with some of the parent (Background Migration) attributes
				job := &models.BackgroundMigrationJob{
					BBMID:            bbmLocal.ID,
					StartID:          bbmLocal.StartID,
					EndID:            jobEndID,
					BatchSize:        bbmLocal.BatchSize,
					JobName:          bbmLocal.JobName,
					PaginationColumn: bbmLocal.TargetColumn,
				}

				gomock.InOrder(bbmStoreMock.EXPECT().FindNext(ctx).Return(&bbmLocal, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbmLocal.TargetTable, bbmLocal.TargetColumn).Return(nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, &bbmLocalRunning).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbmLocal.ID, bbmLocal.EndID).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, &bbmLocal).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().EstimateTotalTupleCount(ctx, gomock.Any()).Return(int64(1000), nil).Times(1),
					bbmStoreMock.EXPECT().SetTotalTupleCount(ctx, bbmLocal.ID, gomock.Any()).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobEndFromJobStart(ctx, bbmLocal.TargetTable, bbmLocal.TargetColumn, bbmLocal.StartID, bbmLocal.EndID, bbmLocal.BatchSize).Return(jobEndID, nil).Times(1),
					bbmStoreMock.EXPECT().CreateNewJob(ctx, job).Return(nil).Times(1),
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
				bbmLocal := *bbm
				bbmLocal.Status = models.BackgroundMigrationRunning
				// create the job representation and decorate the job with some of the parent (Background Migration) attributes
				job := &models.BackgroundMigrationJob{
					BBMID:            bbmLocal.ID,
					StartID:          bbmLocal.StartID,
					EndID:            jobEndID,
					BatchSize:        bbmLocal.BatchSize,
					JobName:          bbmLocal.JobName,
					PaginationColumn: bbmLocal.TargetColumn,
				}

				gomock.InOrder(bbmStoreMock.EXPECT().FindNext(ctx).Return(&bbmLocal, nil).Times(1),
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbmLocal.TargetTable, bbmLocal.TargetColumn).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbmLocal.ID, bbmLocal.EndID).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, &bbmLocal).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().EstimateTotalTupleCount(ctx, gomock.Any()).Return(int64(1000), nil).Times(1),
					bbmStoreMock.EXPECT().SetTotalTupleCount(ctx, bbmLocal.ID, gomock.Any()).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobEndFromJobStart(ctx, bbmLocal.TargetTable, bbmLocal.TargetColumn, bbmLocal.StartID, bbmLocal.EndID, bbmLocal.BatchSize).Return(jobEndID, nil).Times(1),
					bbmStoreMock.EXPECT().CreateNewJob(ctx, job).Return(nil).Times(1),
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
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(nil).Times(1),
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
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(nil).Times(1),
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
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(nil).Times(1),
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
					bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, bbm.TargetTable, bbm.TargetColumn).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithEndID(ctx, bbm.ID, bbm.EndID).Return(finalJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, bbm.ID, models.BackgroundMigrationFailed).Return(&expectedJob, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, bbm).Return(nil).Times(1),
				)
				return bbmStoreMock
			},
			expectedJob: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			test := tc
			bbmStore := test.setupMocks(gomock.NewController(tt))
			job, err := test.worker.FindJob(ctx, bbmStore)
			require.NoError(tt, err)
			require.Equal(tt, test.expectedJob, job)
		})
	}
}

func TestFindJob_NullBatchingStrategy_CreateJobWhenNullsExist(t *testing.T) {
	ctx := context.TODO()

	// Work map must contain the job name for validateMigration to pass
	work := map[string]Work{
		"NullBackfill": {Name: "NullBackfill", Do: doErrorReturn(nil)},
	}

	// Background migration configured with Null batching strategy
	nb := &models.BackgroundMigration{
		ID:               10,
		Name:             "NullBackfill",
		Status:           models.BackgroundMigrationActive,
		StartID:          0,
		EndID:            0,
		BatchSize:        100,
		JobName:          "NullBackfill",
		TargetTable:      "public.repositories",
		TargetColumn:     "id",
		BatchingStrategy: models.NullBatchingBBMStrategy,
	}

	expectedJob := &models.BackgroundMigrationJob{
		BBMID:            nb.ID,
		StartID:          0,
		EndID:            0,
		BatchSize:        nb.BatchSize,
		JobName:          nb.JobName,
		PaginationColumn: nb.TargetColumn,
		PaginationTable:  nb.TargetTable,
	}

	setupMocks := func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
		bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
		// use local copy because code may mutate nb
		nbLocal := *nb
		nbLocalRunning := nbLocal
		nbLocalRunning.Status = models.BackgroundMigrationRunning
		gomock.InOrder(
			bbmStoreMock.EXPECT().FindNext(ctx).Return(&nbLocal, nil).Times(1),
			bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, nbLocal.TargetTable, nbLocal.TargetColumn).Return(nil).Times(1),
			bbmStoreMock.EXPECT().UpdateStatus(ctx, &nbLocalRunning).Return(nil).Times(1),
			bbmStoreMock.EXPECT().HasNullIndex(ctx, nbLocal.TargetTable, nbLocal.TargetColumn).Return(true, nil).Times(1),
			bbmStoreMock.EXPECT().HasNullValues(ctx, nbLocal.TargetTable, nbLocal.TargetColumn).Return(true, nil).Times(1),
			bbmStoreMock.EXPECT().FindLastJob(ctx, &nbLocal).Return(nil, nil).Times(1),
			bbmStoreMock.EXPECT().EstimateTotalTupleCount(ctx, &nbLocal).Return(int64(500), nil).Times(1),
			bbmStoreMock.EXPECT().SetTotalTupleCount(ctx, nbLocal.ID, gomock.Any()).Return(nil).Times(1),
			bbmStoreMock.EXPECT().CreateNewJob(ctx, expectedJob).Return(nil).Times(1),
		)
		return bbmStoreMock
	}

	job, err := NewWorker(work).FindJob(ctx, setupMocks(gomock.NewController(t)))
	require.NoError(t, err)
	require.Equal(t, expectedJob, job)
}

func TestFindJob_NullBatchingStrategy_MarkFinishedWhenNoNulls(t *testing.T) {
	ctx := context.TODO()

	work := map[string]Work{
		"NullBackfill": {Name: "NullBackfill", Do: doErrorReturn(nil)},
	}

	nb := &models.BackgroundMigration{
		ID:               11,
		Name:             "NullBackfill",
		Status:           models.BackgroundMigrationActive,
		StartID:          0,
		EndID:            0,
		BatchSize:        50,
		JobName:          "NullBackfill",
		TargetTable:      "public.repositories",
		TargetColumn:     "id",
		BatchingStrategy: models.NullBatchingBBMStrategy,
	}

	finished := *nb
	finished.Status = models.BackgroundMigrationFinished

	setupMocks := func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
		bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
		gomock.InOrder(
			bbmStoreMock.EXPECT().FindNext(ctx).Return(nb, nil).Times(1),
			bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, nb.TargetTable, nb.TargetColumn).Return(nil).Times(1),
			bbmStoreMock.EXPECT().UpdateStatus(ctx, gomock.Any()).Return(nil).Times(1),
			bbmStoreMock.EXPECT().HasNullIndex(ctx, nb.TargetTable, nb.TargetColumn).Return(true, nil).Times(1),
			bbmStoreMock.EXPECT().HasNullValues(ctx, nb.TargetTable, nb.TargetColumn).Return(false, nil).Times(1),
			bbmStoreMock.EXPECT().UpdateStatus(ctx, &finished).Return(nil).Times(1),
		)
		return bbmStoreMock
	}

	job, err := NewWorker(work).FindJob(ctx, setupMocks(gomock.NewController(t)))
	require.NoError(t, err)
	require.Nil(t, job)
}

func TestFindJob_NullBatchingStrategy_ErrorFromHasNullValues(t *testing.T) {
	ctx := context.TODO()

	work := map[string]Work{
		"NullBackfill": {Name: "NullBackfill", Do: doErrorReturn(nil)},
	}

	nb := &models.BackgroundMigration{
		ID:               12,
		Name:             "NullBackfill",
		Status:           models.BackgroundMigrationActive,
		StartID:          0,
		EndID:            0,
		BatchSize:        25,
		JobName:          "NullBackfill",
		TargetTable:      "public.repositories",
		TargetColumn:     "id",
		BatchingStrategy: models.NullBatchingBBMStrategy,
	}

	setupMocks := func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
		bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
		gomock.InOrder(
			bbmStoreMock.EXPECT().FindNext(ctx).Return(nb, nil).Times(1),
			bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, nb.TargetTable, nb.TargetColumn).Return(nil).Times(1),
			bbmStoreMock.EXPECT().UpdateStatus(ctx, gomock.Any()).Return(nil).Times(1),
			bbmStoreMock.EXPECT().HasNullIndex(ctx, nb.TargetTable, nb.TargetColumn).Return(true, nil).Times(1),
			bbmStoreMock.EXPECT().HasNullValues(ctx, nb.TargetTable, nb.TargetColumn).Return(false, errAnError).Times(1),
		)
		return bbmStoreMock
	}

	job, err := NewWorker(work).FindJob(ctx, setupMocks(gomock.NewController(t)))
	require.ErrorIs(t, err, errAnError)
	require.Nil(t, job)
}

func TestFindJob_NullBatchingStrategy_NoNullIndex(t *testing.T) {
	ctx := context.TODO()

	work := map[string]Work{
		"NullBackfill": {Name: "NullBackfill", Do: doErrorReturn(nil)},
	}

	nb := &models.BackgroundMigration{
		ID:               12,
		Name:             "NullBackfill",
		Status:           models.BackgroundMigrationActive,
		StartID:          0,
		EndID:            0,
		BatchSize:        25,
		JobName:          "NullBackfill",
		TargetTable:      "public.repositories",
		TargetColumn:     "id",
		BatchingStrategy: models.NullBatchingBBMStrategy,
	}

	setupMocks := func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
		bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
		gomock.InOrder(
			bbmStoreMock.EXPECT().FindNext(ctx).Return(nb, nil).Times(1),
			bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, nb.TargetTable, nb.TargetColumn).Return(nil).Times(1),
			bbmStoreMock.EXPECT().UpdateStatus(ctx, gomock.Any()).Return(nil).Times(1),
			bbmStoreMock.EXPECT().HasNullIndex(ctx, nb.TargetTable, nb.TargetColumn).Return(false, nil).Times(1),
		)
		return bbmStoreMock
	}

	job, err := NewWorker(work).FindJob(ctx, setupMocks(gomock.NewController(t)))
	require.ErrorIs(t, err, ErrNoNullIndex)
	require.Nil(t, job)
}

func TestFindJob_NullBatchingStrategy_NullIndexCheckError(t *testing.T) {
	ctx := context.TODO()

	work := map[string]Work{
		"NullBackfill": {Name: "NullBackfill", Do: doErrorReturn(nil)},
	}

	nb := &models.BackgroundMigration{
		ID:               12,
		Name:             "NullBackfill",
		Status:           models.BackgroundMigrationActive,
		StartID:          0,
		EndID:            0,
		BatchSize:        25,
		JobName:          "NullBackfill",
		TargetTable:      "public.repositories",
		TargetColumn:     "id",
		BatchingStrategy: models.NullBatchingBBMStrategy,
	}

	setupMocks := func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
		bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
		gomock.InOrder(
			bbmStoreMock.EXPECT().FindNext(ctx).Return(nb, nil).Times(1),
			bbmStoreMock.EXPECT().ValidateMigrationTableAndColumn(ctx, nb.TargetTable, nb.TargetColumn).Return(nil).Times(1),
			bbmStoreMock.EXPECT().UpdateStatus(ctx, gomock.Any()).Return(nil).Times(1),
			bbmStoreMock.EXPECT().HasNullIndex(ctx, nb.TargetTable, nb.TargetColumn).Return(false, errAnError).Times(1),
		)
		return bbmStoreMock
	}

	job, err := NewWorker(work).FindJob(ctx, setupMocks(gomock.NewController(t)))
	require.ErrorIs(t, err, errAnError)
	require.Nil(t, job)
}

// TestExecuteJob_Errors tests all the error paths on the `ExecuteJob` method.
func TestExecuteJob_Errors(t *testing.T) {
	ctx := context.TODO()
	testCases := []struct {
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

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			test := tc
			tt.Parallel()
			bbmStore := test.setupMocks(gomock.NewController(tt))
			err := test.worker.ExecuteJob(ctx, bbmStore, job)
			require.ErrorIs(tt, err, test.expectedError)
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

// TestGrabLock tests all the paths on the `ShouldThrottle` method.
func TestShouldThrottle(t *testing.T) {
	ctx := context.TODO()
	worker := NewWorker(nil)
	setupMocks := func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
		bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)

		gomock.InOrder(
			bbmStoreMock.EXPECT().GetPendingWALCount(ctx).Return(-1, nil).Times(1),
			bbmStoreMock.EXPECT().GetPendingWALCount(ctx).Return(0, nil).Times(1),
			bbmStoreMock.EXPECT().GetPendingWALCount(ctx).Return(walSegmentThreshold, nil).Times(1),
			bbmStoreMock.EXPECT().GetPendingWALCount(ctx).Return(walSegmentThreshold+1, nil).Times(1),
			bbmStoreMock.EXPECT().GetPendingWALCount(ctx).Return(0, errAnError).Times(1),
		)

		return bbmStoreMock
	}

	bbmStore := setupMocks(gomock.NewController(t))

	throttle, err := worker.ShouldThrottle(ctx, bbmStore)
	require.NoError(t, err)
	require.False(t, throttle)

	throttle, err = worker.ShouldThrottle(ctx, bbmStore)
	require.NoError(t, err)
	require.False(t, throttle)

	throttle, err = worker.ShouldThrottle(ctx, bbmStore)
	require.NoError(t, err)
	require.False(t, throttle)

	throttle, err = worker.ShouldThrottle(ctx, bbmStore)
	require.NoError(t, err)
	require.True(t, throttle)

	throttle, err = worker.ShouldThrottle(ctx, bbmStore)
	require.ErrorIs(t, err, errAnError)
	require.False(t, throttle)
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
	testCases := []struct {
		name           string
		opts           []WorkerOption
		expectedWorker func() *Worker
	}{
		{
			name: "no options",
			expectedWorker: func() *Worker {
				w := &Worker{
					Work:                       make(map[string]Work, 0),
					logger:                     log.GetLogger().WithFields(log.Fields{componentKey: workerName, walThresholdKey: walSegmentThreshold}),
					jobInterval:                defaultJobInterval,
					maxJobAttempt:              defaultMaxJobAttempt,
					workerStartupJitterSeconds: defaultWorkerStartupJitterSeconds,
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
					Work:                       make(map[string]Work, 0),
					logger:                     log.GetLogger().WithFields(log.Fields{componentKey: workerName, walThresholdKey: walSegmentThreshold}),
					jobInterval:                1 * time.Second,
					maxJobAttempt:              defaultMaxJobAttempt,
					workerStartupJitterSeconds: defaultWorkerStartupJitterSeconds,
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
					Work:                       make(map[string]Work, 0),
					logger:                     log.GetLogger().WithFields(log.Fields{componentKey: workerName, walThresholdKey: walSegmentThreshold}),
					jobInterval:                defaultJobInterval,
					maxJobAttempt:              9,
					workerStartupJitterSeconds: defaultWorkerStartupJitterSeconds,
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
					Work:                       make(map[string]Work, 0),
					db:                         &datastore.DB{},
					logger:                     log.GetLogger().WithFields(log.Fields{componentKey: workerName, walThresholdKey: walSegmentThreshold}),
					jobInterval:                defaultJobInterval,
					maxJobAttempt:              defaultMaxJobAttempt,
					workerStartupJitterSeconds: defaultWorkerStartupJitterSeconds,
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
					Work:                       make(map[string]Work, 0),
					logger:                     log.GetLogger().WithFields(log.Fields{"random_key": "random_value", componentKey: workerName, walThresholdKey: walSegmentThreshold}),
					jobInterval:                defaultJobInterval,
					maxJobAttempt:              defaultMaxJobAttempt,
					workerStartupJitterSeconds: defaultWorkerStartupJitterSeconds,
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
					Work:                       make(map[string]Work, 0),
					logger:                     log.GetLogger().WithFields(log.Fields{componentKey: workerName, walThresholdKey: walSegmentThreshold}),
					jobInterval:                defaultJobInterval,
					maxJobAttempt:              defaultMaxJobAttempt,
					wh:                         wh,
					workerStartupJitterSeconds: defaultWorkerStartupJitterSeconds,
				}
				return w
			},
		},
		{
			name: "WithWALPressureCheck",
			opts: []WorkerOption{WithWALPressureCheck()},
			expectedWorker: func() *Worker {
				w := &Worker{
					Work:                       make(map[string]Work, 0),
					logger:                     log.GetLogger().WithFields(log.Fields{componentKey: workerName, walThresholdKey: walSegmentThreshold}),
					jobInterval:                defaultJobInterval,
					maxJobAttempt:              defaultMaxJobAttempt,
					isWALThrottlingEnabled:     true,
					workerStartupJitterSeconds: defaultWorkerStartupJitterSeconds,
				}
				w.wh = w
				return w
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			test := tc
			tt.Parallel()
			worker, err := RegisterWork(nil, test.opts...)
			require.NoError(tt, err)
			require.Equal(tt, test.expectedWorker(), worker)
		})
	}
}

// TestWorker_Run tests the run method of worker.
func TestWorker_Run(t *testing.T) {
	testCases := []struct {
		name       string
		setupMocks func(ctrl *gomock.Controller) *Worker

		expectedRunResult runResult
		expectedErr       error
	}{
		{
			name: "transaction creation failure",
			setupMocks: func(ctrl *gomock.Controller) *Worker {
				dbMock := mocks.NewMockHandler(ctrl)
				worker := NewWorker(nil, WithDB(dbMock))

				dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(nil, errAnError).Times(1)
				return worker
			},
			expectedErr:       errAnError,
			expectedRunResult: runResult{},
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
			expectedErr:       errAnError,
			expectedRunResult: runResult{},
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
			expectedErr: errAnError,
			expectedRunResult: runResult{
				heldLock: true,
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
			expectedRunResult: runResult{
				heldLock: true,
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
			expectedErr: errAnError,
			expectedRunResult: runResult{
				heldLock: true,
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
			expectedErr: errAnError,
			expectedRunResult: runResult{
				heldLock: true,
				foundJob: true,
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
			expectedErr: errAnError,
			expectedRunResult: runResult{
				heldLock: true,
				foundJob: true,
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
			expectedErr: nil,
			expectedRunResult: runResult{
				heldLock: true,
				foundJob: true,
			},
		},
		{
			name: "WAL pressure",
			setupMocks: func(ctrl *gomock.Controller) *Worker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewWorker(nil, WithDB(dbMock), WithHandler(handler), WithWALPressureCheck())
				bbmStore := datastore.NewBackgroundMigrationStore(txMock)

				gomock.InOrder(
					dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(txMock, nil).Times(1),
					handler.EXPECT().GrabLock(gomock.Any(), bbmStore).Return(nil).Times(1),
					handler.EXPECT().ShouldThrottle(gomock.Any(), bbmStore).Return(true, nil).Times(1),
					txMock.EXPECT().Rollback().Return(nil).Times(1),
				)

				return worker
			},
			expectedErr: nil,
			expectedRunResult: runResult{
				heldLock:        true,
				foundJob:        false,
				highWALPressure: true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			test := tc
			tt.Parallel()
			worker := test.setupMocks(gomock.NewController(tt))
			// Execute the run method and assert the necessary methods/functions are called
			actualRunResult, actualErr := worker.run(context.TODO())

			require.Equal(tt, test.expectedErr, actualErr)
			require.Equal(tt, test.expectedRunResult, actualRunResult)
		})
	}
}

func TestWorker_shouldResetBackOff(t *testing.T) {
	testCases := []struct {
		name     string
		result   runResult
		err      error
		expected bool // true = reset backoff, false = continue backoff
	}{
		{
			name: "job found - should reset backoff",
			result: runResult{
				foundJob:        true,
				heldLock:        true,
				highWALPressure: false,
			},
			err:      nil,
			expected: true,
		},
		{
			name: "lock not held - should reset backoff",
			result: runResult{
				foundJob:        false,
				heldLock:        false,
				highWALPressure: false,
			},
			err:      nil,
			expected: true,
		},
		{
			name: "no job found, lock held - should continue backoff",
			result: runResult{
				foundJob:        false,
				heldLock:        true,
				highWALPressure: false,
			},
			err:      nil,
			expected: false,
		},
		{
			name: "high WAL pressure - should continue backoff",
			result: runResult{
				foundJob:        true, // Even with job found
				heldLock:        true,
				highWALPressure: true,
			},
			err:      nil,
			expected: false,
		},
		{
			name: "error occurred - should continue backoff",
			result: runResult{
				foundJob:        true, // Even with job found
				heldLock:        true,
				highWALPressure: false,
			},
			err:      errors.New("some error"),
			expected: false,
		},
		{
			name: "job found with high WAL pressure - WAL takes precedence",
			result: runResult{
				foundJob:        true,
				heldLock:        true,
				highWALPressure: true,
			},
			err:      nil,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			jw := &Worker{
				logger: log.GetLogger(),
			}
			actual := jw.shouldResetBackOff(tc.result, tc.err)
			require.Equal(tt, tc.expected, actual)
		})
	}
}
