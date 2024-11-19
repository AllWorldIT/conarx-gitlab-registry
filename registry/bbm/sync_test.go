package bbm

import (
	"context"
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
	sync         = &models.BackgroundMigration{ID: 1, EndID: 2, JobName: workFunctionName}
	syncJob      = &models.BackgroundMigrationJob{ID: 1, JobName: workFunctionName}
	syncWorkFunc = map[string]Work{
		syncJob.JobName: {
			Name: workFunctionName,
			Do:   doErrorReturn(errAnError),
		},
	}
)

// TestSyncWorker_FindJob_Errors tests all the error paths on the `FindJob` method.
func TestSyncWorker_FindJob_Errors(t *testing.T) {
	ctx := context.TODO()

	tt := []struct {
		name       string
		setupMocks func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore
	}{
		{
			name: "error when checking for next failed job",
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, errAnError).Times(1)
				return bbmStoreMock
			},
		},
		{
			name: "error when finding failed job",
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(sync, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, sync.ID, models.BackgroundMigrationFailed).Return(nil, errAnError).Times(1),
				)
				return bbmStoreMock
			},
		},
		{
			name: "error when updating status of failed background migration",
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(sync, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, sync.ID, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, sync).Return(errAnError).Times(1),
				)
				return bbmStoreMock
			},
		},
		{
			name: "error when checking for next running or active job",
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindNext(ctx).Return(nil, errAnError).Times(1),
				)
				return bbmStoreMock
			},
		},
		{
			name: "error when finding running or active job",
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindNext(ctx).Return(sync, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, sync.ID, models.BackgroundMigrationFailed).Return(nil, errAnError).Times(1),
				)
				return bbmStoreMock
			},
		},
		{
			name: "error when finding last job",
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindNext(ctx).Return(sync, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, sync.ID, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, sync).Return(nil, errAnError).Times(1),
				)
				return bbmStoreMock
			},
		},
		{
			name: "error when finding job end from job start",
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindNext(ctx).Return(sync, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, sync.ID, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, sync).Return(syncJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobEndFromJobStart(ctx, sync.TargetTable, sync.TargetColumn, syncJob.EndID+1, sync.EndID, sync.BatchSize).Return(0, errAnError).Times(1),
				)
				return bbmStoreMock
			},
		},
		{
			name: "error when updating status of running/active background migration",
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				lastJob := *syncJob
				lastJob.EndID = bbm.EndID
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindNext(ctx).Return(sync, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, sync.ID, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, sync).Return(&lastJob, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, sync).Return(errAnError).Times(1),
				)
				return bbmStoreMock
			},
		},
		{
			name: "error when creating new job",
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindNext(ctx).Return(sync, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, sync.ID, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, sync).Return(syncJob, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobEndFromJobStart(ctx, sync.TargetTable, sync.TargetColumn, syncJob.EndID+1, sync.EndID, sync.BatchSize).Return(syncJob.EndID+sync.BatchSize, nil).Times(1),
					bbmStoreMock.EXPECT().CreateNewJob(ctx, gomock.Any()).Return(errAnError).Times(1),
				)
				return bbmStoreMock
			},
		},
		{
			name: "error when finding last job",
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindNext(ctx).Return(sync, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, sync.ID, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, sync).Return(nil, errAnError).Times(1),
				)
				return bbmStoreMock
			},
		},
		{
			name: "error when updating status of background migration",
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindNext(ctx).Return(sync, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, sync.ID, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, sync).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, sync).Return(errAnError).Times(1),
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
			job, err := NewSyncWorker(nil).FindJob(ctx, bbmStore)
			require.ErrorIs(t, err, errAnError)
			require.Nil(t, job)
		})
	}
}

// TestSyncWorker_ExecuteJob_Errors tests all the error paths on the `ExecuteJob` method.
func TestSyncWorker_ExecuteJob_Errors(t *testing.T) {
	ctx := context.TODO()

	tt := []struct {
		name        string
		job         *models.BackgroundMigrationJob
		setupMocks  func(ctrl *gomock.Controller) (*SyncWorker, datastore.BackgroundMigrationStore)
		expectedErr error
	}{
		{
			name: "work function not found",
			job: &models.BackgroundMigrationJob{
				JobName: "non_existent_job",
			},
			setupMocks: func(ctrl *gomock.Controller) (*SyncWorker, datastore.BackgroundMigrationStore) {
				worker := NewSyncWorker(nil, WithWorkMap(map[string]Work{}))
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				return worker, bbmStoreMock
			},
			expectedErr: ErrWorkFunctionNotFound,
		},
		{
			name: "max job attempts reached",
			job: &models.BackgroundMigrationJob{
				JobName: "test_job",
			},
			setupMocks: func(ctrl *gomock.Controller) (*SyncWorker, datastore.BackgroundMigrationStore) {
				worker := NewSyncWorker(nil,
					WithWorkMap(map[string]Work{
						"test_job": {
							Name: "test_job",
							Do:   doErrorReturn(errAnError),
						},
					}),
					WithSyncMaxJobAttempt(3),
				)
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				return worker, bbmStoreMock
			},
			expectedErr: ErrMaxJobAttemptsReached,
		},
		{
			name: "error updating job status",
			job: &models.BackgroundMigrationJob{
				JobName: "test_job",
			},
			setupMocks: func(ctrl *gomock.Controller) (*SyncWorker, datastore.BackgroundMigrationStore) {
				worker := NewSyncWorker(nil,
					WithWorkMap(map[string]Work{
						"test_job": {
							Name: "test_job",
							Do:   doErrorReturn(nil),
						},
					}),
				)
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				bbmStoreMock.EXPECT().UpdateJobStatus(ctx, gomock.Any()).Return(errAnError).Times(1)
				return worker, bbmStoreMock
			},
			expectedErr: errAnError,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			worker, bbmStore := test.setupMocks(ctrl)
			err := worker.ExecuteJob(ctx, bbmStore, test.job)
			require.Error(t, err)
			require.Equal(t, test.expectedErr, err)
		})
	}
}

// TestSyncWorker_GrabLock tests all the paths on the `GrabLock` method.
func TestSyncWorker_GrabLock(t *testing.T) {
	ctx := context.TODO()
	worker := NewSyncWorker(nil)

	tt := []struct {
		name        string
		setupMocks  func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore
		expectedErr error
	}{
		{
			name: "error when trying to grab lock",
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				bbmStoreMock.EXPECT().SyncLock(ctx).Return(errAnError).Times(1)
				return bbmStoreMock
			},
			expectedErr: errAnError,
		},
		{
			name: "successfully grab lock",
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				bbmStoreMock.EXPECT().SyncLock(ctx).Return(nil).Times(1)
				return bbmStoreMock
			},
			expectedErr: nil,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			bbmStore := test.setupMocks(gomock.NewController(t))
			err := worker.GrabLock(ctx, bbmStore)
			if test.expectedErr != nil {
				require.ErrorIs(t, err, test.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestSyncWorker_FindJob tests all the happy paths on the `FindJob` method.
func TestSyncWorker_FindJob(t *testing.T) {
	ctx := context.TODO()
	expectedJob := models.BackgroundMigrationJob{
		BBMID:   1,
		EndID:   3,
		JobName: workFunctionName,
	}

	tt := []struct {
		name        string
		setupMocks  func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore
		worker      *SyncWorker
		expectedJob *models.BackgroundMigrationJob
	}{
		{
			name:   "no pending background migration found",
			worker: NewSyncWorker(nil),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindNext(ctx).Return(nil, nil).Times(1),
				)
				return bbmStoreMock
			},
			expectedJob: nil,
		},
		{
			name:   "found a new job to run for an active migration",
			worker: NewSyncWorker(nil, WithWorkMap(syncWorkFunc)),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				jobEndID := 3
				job := &models.BackgroundMigrationJob{
					BBMID:            sync.ID,
					StartID:          sync.StartID,
					EndID:            jobEndID,
					BatchSize:        sync.BatchSize,
					JobName:          sync.JobName,
					PaginationColumn: sync.TargetColumn,
				}

				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindNext(ctx).Return(sync, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, sync.ID, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, sync).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, sync).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindJobEndFromJobStart(ctx, sync.TargetTable, sync.TargetColumn, sync.StartID, sync.EndID, sync.BatchSize).Return(jobEndID, nil).Times(1),
					bbmStoreMock.EXPECT().CreateNewJob(ctx, job).Return(nil),
				)
				return bbmStoreMock
			},
			expectedJob: &expectedJob,
		},
		{
			name:   "no failed jobs, set migration to running",
			worker: NewSyncWorker(nil),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(sync, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, sync.ID, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, sync).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindNext(ctx).Return(nil, nil).Times(1),
				)
				return bbmStoreMock
			},
			expectedJob: nil,
		},
		{
			name:   "no jobs to run, set migration to finished",
			worker: NewSyncWorker(nil),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindNext(ctx).Return(sync, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, sync.ID, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindLastJob(ctx, sync).Return(&models.BackgroundMigrationJob{EndID: sync.EndID}, nil).Times(1),
					bbmStoreMock.EXPECT().UpdateStatus(ctx, sync).Return(nil).Times(1),
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(nil, nil).Times(1),
					bbmStoreMock.EXPECT().FindNext(ctx).Return(nil, nil).Times(1),
				)
				return bbmStoreMock
			},
			expectedJob: nil,
		},
		{
			name:   "found a failed job to run",
			worker: NewSyncWorker(nil, WithWorkMap(syncWorkFunc)),
			setupMocks: func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
				bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
				gomock.InOrder(
					bbmStoreMock.EXPECT().FindNextByStatus(ctx, models.BackgroundMigrationFailed).Return(sync, nil).Times(1),
					bbmStoreMock.EXPECT().FindJobWithStatus(ctx, sync.ID, models.BackgroundMigrationFailed).Return(&expectedJob, nil).Times(1),
				)
				return bbmStoreMock
			},
			expectedJob: &expectedJob,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			test := test
			t.Parallel()
			bbmStore := test.setupMocks(gomock.NewController(t))
			job, err := test.worker.FindJob(ctx, bbmStore)
			require.NoError(t, err)
			require.Equal(t, test.expectedJob, job)
		})
	}
}

// TestSyncWorker_ExecuteJob tests all the happy paths on the `ExecuteJob` method.
func TestSyncWorker_ExecuteJob(t *testing.T) {
	ctx := context.TODO()
	worker := NewSyncWorker(nil, WithWorkMap(map[string]Work{
		syncJob.JobName: {
			Name: syncJob.JobName,
			Do:   doErrorReturn(nil),
		},
	}))

	setupMocks := func(ctrl *gomock.Controller) datastore.BackgroundMigrationStore {
		bbmStoreMock := mocks.NewMockBackgroundMigrationStore(ctrl)
		finishedJob := syncJob
		finishedJob.Status = models.BackgroundMigrationFinished
		bbmStoreMock.EXPECT().UpdateJobStatus(ctx, finishedJob).Return(nil).Times(1)
		return bbmStoreMock
	}

	err := worker.ExecuteJob(ctx, setupMocks(gomock.NewController(t)), syncJob)
	require.NoError(t, err)
}

// TestSyncWorker_Run tests the run method of the sync worker.
func TestSyncWorker_Run(t *testing.T) {
	tests := []struct {
		name       string
		setupMocks func(ctrl *gomock.Controller) *SyncWorker
		expectErr  bool
	}{
		{
			name: "transaction creation failure",
			setupMocks: func(ctrl *gomock.Controller) *SyncWorker {
				dbMock := mocks.NewMockHandler(ctrl)
				worker := NewSyncWorker(dbMock)

				dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(nil, errAnError).Times(1)
				return worker
			},
			expectErr: true,
		},
		{
			name: "failed to obtain lock",
			setupMocks: func(ctrl *gomock.Controller) *SyncWorker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewSyncWorker(dbMock, WithSyncHandler(handler))
				bbmStore := datastore.NewBackgroundMigrationStore(txMock)

				gomock.InOrder(
					dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(txMock, nil).Times(1),
					handler.EXPECT().GrabLock(gomock.Any(), bbmStore).Return(errAnError).Times(1),
					txMock.EXPECT().Rollback().Return(nil).Times(1),
				)

				return worker
			},
			expectErr: true,
		},
		{
			name: "job retrieval failure",
			setupMocks: func(ctrl *gomock.Controller) *SyncWorker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewSyncWorker(dbMock, WithSyncHandler(handler))
				bbmStore := datastore.NewBackgroundMigrationStore(txMock)

				gomock.InOrder(
					dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(txMock, nil).Times(1),
					handler.EXPECT().GrabLock(gomock.Any(), bbmStore).Return(nil).Times(1),
					handler.EXPECT().FindJob(gomock.Any(), bbmStore).Return(nil, errAnError).Times(1),
					txMock.EXPECT().Rollback().Return(nil).Times(1),
				)

				return worker
			},
			expectErr: true,
		},
		{
			name: "no jobs available",
			setupMocks: func(ctrl *gomock.Controller) *SyncWorker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewSyncWorker(dbMock, WithSyncHandler(handler))
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
			expectErr: false,
		},
		{
			name: "no jobs available commit failure",
			setupMocks: func(ctrl *gomock.Controller) *SyncWorker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewSyncWorker(dbMock, WithSyncHandler(handler))
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
			expectErr: true,
		},
		{
			name: "job execution failure",
			setupMocks: func(ctrl *gomock.Controller) *SyncWorker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewSyncWorker(dbMock, WithSyncHandler(handler))
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
			expectErr: true,
		},
		{
			name: "post-execution transaction commit failure",
			setupMocks: func(ctrl *gomock.Controller) *SyncWorker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewSyncWorker(dbMock, WithSyncHandler(handler))
				bbmStore := datastore.NewBackgroundMigrationStore(txMock)

				gomock.InOrder(
					dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(txMock, nil).Times(1),
					handler.EXPECT().GrabLock(gomock.Any(), bbmStore).Return(nil).Times(1),
					handler.EXPECT().FindJob(gomock.Any(), bbmStore).Return(job, nil).Times(1),
					handler.EXPECT().ExecuteJob(gomock.Any(), bbmStore, job).Return(nil).Times(1),
					handler.EXPECT().FindJob(gomock.Any(), bbmStore).Return(nil, nil).Times(1),
					txMock.EXPECT().Commit().Return(errAnError).Times(1),
					txMock.EXPECT().Rollback().Return(nil).Times(1),
				)

				return worker
			},
			expectErr: true,
		},
		{
			name: "successful run with max jobs per batch",
			setupMocks: func(ctrl *gomock.Controller) *SyncWorker {
				dbMock := mocks.NewMockHandler(ctrl)
				txMock := mocks.NewMockTransactor(ctrl)
				handler := bbm_mocks.NewMockHandler(ctrl)
				worker := NewSyncWorker(dbMock, WithSyncHandler(handler), WithSyncMaxJobPerBatch(2))
				bbmStore := datastore.NewBackgroundMigrationStore(txMock)

				gomock.InOrder(
					dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(txMock, nil).Times(1),
					handler.EXPECT().GrabLock(gomock.Any(), bbmStore).Return(nil).Times(1),
					handler.EXPECT().FindJob(gomock.Any(), bbmStore).Return(job, nil).Times(1),
					handler.EXPECT().ExecuteJob(gomock.Any(), bbmStore, job).Return(nil).Times(1),
					handler.EXPECT().FindJob(gomock.Any(), bbmStore).Return(job, nil).Times(1),
					handler.EXPECT().ExecuteJob(gomock.Any(), bbmStore, job).Return(nil).Times(1),
					txMock.EXPECT().Commit().Return(nil).Times(1),
					dbMock.EXPECT().BeginTx(gomock.Any(), nil).Return(txMock, nil).Times(1),
					handler.EXPECT().GrabLock(gomock.Any(), bbmStore).Return(nil).Times(1),
					handler.EXPECT().FindJob(gomock.Any(), bbmStore).Return(nil, nil).Times(1),
					txMock.EXPECT().Commit().Return(nil).Times(1),
					txMock.EXPECT().Rollback().Return(nil).Times(1),
					txMock.EXPECT().Rollback().Return(nil).Times(1),
				)

				return worker
			},
			expectErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test := test
			t.Parallel()
			worker := test.setupMocks(gomock.NewController(t))
			err := worker.run(context.TODO())
			if test.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewSyncWorkerOpts(t *testing.T) {
	wh := bbm_mocks.NewMockHandler(gomock.NewController(t))
	wm := map[string]Work{"test": {Name: "test", Do: func(context.Context, datastore.Handler, string, string, int, int, int) error { return nil }}}
	tests := []struct {
		name           string
		opts           []SyncWorkerOption
		expectedWorker *SyncWorker
	}{
		{
			name: "WithWorkMap",
			opts: []SyncWorkerOption{WithWorkMap(wm)},
			expectedWorker: func() *SyncWorker {
				w := &SyncWorker{
					work:            wm,
					logger:          log.GetLogger().WithFields(log.Fields{componentKey: syncWorkerName}),
					maxJobAttempt:   defaultMaxJobAttempt,
					maxJobPerBatch:  defaultMaxJobPerBatch,
					maxBatchTimeout: defaultMaxBatchTimeout,
					lockWaitTimeout: defaultLockWaitTimeout,
					jobTimeout:      defaultJobTimeout,
				}
				w.wh = w
				return w
			}(),
		},
		{
			name: "WithSyncLogger",
			opts: []SyncWorkerOption{WithSyncLogger(log.GetLogger().WithFields(log.Fields{"test": "value"}))},
			expectedWorker: func() *SyncWorker {
				w := &SyncWorker{
					work:            map[string]Work{},
					logger:          log.GetLogger().WithFields(log.Fields{"test": "value", componentKey: syncWorkerName}),
					maxJobAttempt:   defaultMaxJobAttempt,
					maxJobPerBatch:  defaultMaxJobPerBatch,
					maxBatchTimeout: defaultMaxBatchTimeout,
					lockWaitTimeout: defaultLockWaitTimeout,
					jobTimeout:      defaultJobTimeout,
				}
				w.wh = w
				return w
			}(),
		},
		{
			name: "WithSyncMaxJobAttempt",
			opts: []SyncWorkerOption{WithSyncMaxJobAttempt(5)},
			expectedWorker: func() *SyncWorker {
				w := &SyncWorker{
					work:            map[string]Work{},
					logger:          log.GetLogger().WithFields(log.Fields{componentKey: syncWorkerName}),
					maxJobAttempt:   5,
					maxJobPerBatch:  defaultMaxJobPerBatch,
					maxBatchTimeout: defaultMaxBatchTimeout,
					lockWaitTimeout: defaultLockWaitTimeout,
					jobTimeout:      defaultJobTimeout,
				}
				w.wh = w
				return w
			}(),
		},
		{
			name: "WithSyncMaxJobPerBatch",
			opts: []SyncWorkerOption{WithSyncMaxJobPerBatch(10)},
			expectedWorker: func() *SyncWorker {
				w := &SyncWorker{
					work:            map[string]Work{},
					logger:          log.GetLogger().WithFields(log.Fields{componentKey: syncWorkerName}),
					maxJobAttempt:   defaultMaxJobAttempt,
					maxJobPerBatch:  10,
					maxBatchTimeout: defaultMaxBatchTimeout,
					lockWaitTimeout: defaultLockWaitTimeout,
					jobTimeout:      defaultJobTimeout,
				}
				w.wh = w
				return w
			}(),
		},
		{
			name: "WithSyncMaxBatchTimeout",
			opts: []SyncWorkerOption{WithSyncMaxBatchTimeout(2 * time.Minute)},
			expectedWorker: func() *SyncWorker {
				w := &SyncWorker{
					work:            map[string]Work{},
					logger:          log.GetLogger().WithFields(log.Fields{componentKey: syncWorkerName}),
					maxJobAttempt:   defaultMaxJobAttempt,
					maxJobPerBatch:  defaultMaxJobPerBatch,
					maxBatchTimeout: 2 * time.Minute,
					lockWaitTimeout: defaultLockWaitTimeout,
					jobTimeout:      defaultJobTimeout,
				}
				w.wh = w
				return w
			}(),
		},
		{
			name: "WithSyncHandler",
			opts: []SyncWorkerOption{WithSyncHandler(wh)},
			expectedWorker: func() *SyncWorker {
				w := &SyncWorker{
					work:            map[string]Work{},
					logger:          log.GetLogger().WithFields(log.Fields{componentKey: syncWorkerName}),
					maxJobAttempt:   defaultMaxJobAttempt,
					maxJobPerBatch:  defaultMaxJobPerBatch,
					maxBatchTimeout: defaultMaxBatchTimeout,
					lockWaitTimeout: defaultLockWaitTimeout,
					jobTimeout:      defaultJobTimeout,
					wh:              wh,
				}
				return w
			}(),
		},
		{
			name: "WithSyncJobTimeout",
			opts: []SyncWorkerOption{WithJobTimeout(2 * time.Minute)},
			expectedWorker: func() *SyncWorker {
				w := &SyncWorker{
					work:            map[string]Work{},
					logger:          log.GetLogger().WithFields(log.Fields{componentKey: syncWorkerName}),
					maxJobAttempt:   defaultMaxJobAttempt,
					maxJobPerBatch:  defaultMaxJobPerBatch,
					maxBatchTimeout: defaultMaxBatchTimeout,
					lockWaitTimeout: defaultLockWaitTimeout,
					jobTimeout:      2 * time.Minute,
				}
				w.wh = w
				return w
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker := NewSyncWorker(nil, tt.opts...)
			require.Equal(t, tt.expectedWorker, worker)
		})
	}
}
