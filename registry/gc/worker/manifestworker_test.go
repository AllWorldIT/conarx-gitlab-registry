package worker

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/datastore"
	storemock "github.com/docker/distribution/registry/datastore/mocks"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/internal/testutil"
	"github.com/hashicorp/go-multierror"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	mtsMock *storemock.MockGCManifestTaskStore
	msMock  *storemock.MockManifestStore
)

func mockManifestStores(tb testing.TB, ctrl *gomock.Controller) {
	tb.Helper()

	mtsMock = storemock.NewMockGCManifestTaskStore(ctrl)
	msMock = storemock.NewMockManifestStore(ctrl)

	mtsBkp := manifestTaskStoreConstructor
	msBkp := manifestStoreConstructor

	manifestTaskStoreConstructor = func(db datastore.Queryer) datastore.GCManifestTaskStore { return mtsMock }
	manifestStoreConstructor = func(db datastore.Queryer) datastore.ManifestStore { return msMock }

	tb.Cleanup(func() {
		manifestTaskStoreConstructor = mtsBkp
		manifestStoreConstructor = msBkp
	})
}

func Test_NewManifestWorker(t *testing.T) {
	ctrl := gomock.NewController(t)

	dbMock := storemock.NewMockHandler(ctrl)
	w := NewManifestWorker(dbMock)

	require.NotNil(t, w.logger)
	require.Equal(t, defaultTxTimeout, w.txTimeout)
}

func Test_NewManifestWorker_WithLogger(t *testing.T) {
	ctrl := gomock.NewController(t)

	logger := log.GetLogger()
	dbMock := storemock.NewMockHandler(ctrl)
	w := NewManifestWorker(dbMock, WithManifestLogger(logger))

	got, err := log.ToLogrusEntry(w.logger)
	require.NoError(t, err)
	require.Equal(t, got.WithField(componentKey, w.name), got)
}

func Test_NewManifestWorker_WithTxDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)

	d := 5 * time.Minute
	dbMock := storemock.NewMockHandler(ctrl)
	w := NewManifestWorker(dbMock, WithManifestTxTimeout(d))

	require.Equal(t, d, w.txTimeout)
}

func fakeManifestTask() *models.GCManifestTask {
	return &models.GCManifestTask{
		RepositoryID: 1,
		ManifestID:   2,
		ReviewAfter:  time.Now().Add(-10 * time.Minute),
		ReviewCount:  0,
		Event:        "tag_switch",
	}
}

func TestManifestWorker_processTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	w := NewManifestWorker(dbMock)

	ctx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	mt := fakeManifestTask()
	m := &models.Manifest{RepositoryID: mt.RepositoryID, ID: mt.ManifestID, Digest: digest.FromString("foo")}

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(ctx, nil).Return(txMock, nil).Times(1),
		mtsMock.EXPECT().Next(ctx).Return(mt, nil).Times(1),
		mtsMock.EXPECT().IsDangling(ctx, mt).Return(true, nil).Times(1),
		msMock.EXPECT().Delete(ctx, m.NamespaceID, m.RepositoryID, m.ID).Return(&m.Digest, nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.NoError(t, res.Err)
	require.True(t, res.Found)
	require.True(t, res.Dangling)
	require.Equal(t, mt.Event, res.Event)
}

func TestManifestWorker_processTask_BeginTxError(t *testing.T) {
	ctrl := gomock.NewController(t)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	dbMock.EXPECT().BeginTx(dbCtx, nil).Return(nil, fakeErrorA).Times(1)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fmt.Errorf("creating database transaction: %w", fakeErrorA).Error())
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}

func TestManifestWorker_processTask_NextError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)

	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		mtsMock.EXPECT().Next(dbCtx).Return(nil, fakeErrorA).Times(1),
		txMock.EXPECT().Rollback().Return(nil).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fakeErrorA.Error())
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}

func TestManifestWorker_processTask_None(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)

	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		mtsMock.EXPECT().Next(dbCtx).Return(nil, nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.NoError(t, res.Err)
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}

func TestManifestWorker_processTask_None_CommitError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)

	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		mtsMock.EXPECT().Next(dbCtx).Return(nil, nil).Times(1),
		txMock.EXPECT().Commit().Return(fakeErrorA).Times(1),
		txMock.EXPECT().Rollback().Return(nil).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fmt.Errorf("committing database transaction: %w", fakeErrorA).Error())
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}

func TestManifestWorker_processTask_IsDanglingUnknownError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	processTxMock := storemock.NewMockTransactor(ctrl)
	postponeTxMock := storemock.NewMockTransactor(ctrl)
	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	mt := fakeManifestTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(processTxMock, nil).Times(1),
		mtsMock.EXPECT().Next(dbCtx).Return(mt, nil).Times(1),
		mtsMock.EXPECT().IsDangling(dbCtx, mt).Return(false, fakeErrorA).Times(1),
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(postponeTxMock, nil).Times(1),
		mtsMock.EXPECT().FindAndLock(dbCtx, mt.NamespaceID, mt.RepositoryID, mt.ManifestID).Return(mt, nil).Times(1),
		mtsMock.EXPECT().Postpone(dbCtx, mt, isDuration{5 * time.Minute}).Return(nil).Times(1),
		postponeTxMock.EXPECT().Commit().Return(nil).Times(1),
		postponeTxMock.EXPECT().Rollback().Return(nil).Times(1),
		processTxMock.EXPECT().Rollback().Return(nil).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fakeErrorA.Error())
	require.True(t, res.Found)
	require.False(t, res.Dangling)
	require.Equal(t, mt.Event, res.Event)
}

func TestManifestWorker_processTask_IsDanglingPostponeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	processTxMock := storemock.NewMockTransactor(ctrl)
	postponeTxMock := storemock.NewMockTransactor(ctrl)
	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	mt := fakeManifestTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(processTxMock, nil).Times(1),
		mtsMock.EXPECT().Next(dbCtx).Return(mt, nil).Times(1),
		mtsMock.EXPECT().IsDangling(dbCtx, mt).Return(false, fakeErrorA).Times(1),
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(postponeTxMock, nil).Times(1),
		mtsMock.EXPECT().FindAndLock(dbCtx, mt.NamespaceID, mt.RepositoryID, mt.ManifestID).Return(mt, nil).Times(1),
		mtsMock.EXPECT().Postpone(dbCtx, mt, isDuration{5 * time.Minute}).Return(fakeErrorB).Times(1),
		postponeTxMock.EXPECT().Rollback().Return(nil).Times(1),
		processTxMock.EXPECT().Rollback().Return(nil).Times(1),
	)

	res := w.processTask(context.Background())
	expectedErr := multierror.Error{
		Errors: []error{
			fakeErrorA,
			fakeErrorB,
		},
	}
	require.EqualError(t, res.Err, expectedErr.Error())
	require.True(t, res.Found)
	require.False(t, res.Dangling)
	require.Equal(t, mt.Event, res.Event)
}

func TestManifestWorker_processTask_IsDanglingContextError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	mt := fakeManifestTask()

	errs := []error{
		context.Canceled,
		context.DeadlineExceeded,
	}
	for _, err := range errs {
		gomock.InOrder(
			dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
			mtsMock.EXPECT().Next(dbCtx).Return(mt, nil).Times(1),
			mtsMock.EXPECT().IsDangling(dbCtx, mt).Return(false, err).Times(1),
			txMock.EXPECT().Rollback().Return(nil).Times(1),
		)

		res := w.processTask(context.Background())
		require.EqualError(t, err, res.Err.Error())
		require.True(t, res.Found)
		require.False(t, res.Dangling)
		require.Equal(t, mt.Event, res.Event)

		ctrl.Finish()
	}
}

func TestManifestWorker_processTask_StoreDeleteNotFoundError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	mt := fakeManifestTask()
	m := &models.Manifest{RepositoryID: mt.RepositoryID, ID: mt.ManifestID}

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		mtsMock.EXPECT().Next(dbCtx).Return(mt, nil).Times(1),
		mtsMock.EXPECT().IsDangling(dbCtx, mt).Return(true, nil).Times(1),
		msMock.EXPECT().Delete(dbCtx, m.NamespaceID, m.RepositoryID, m.ID).Return(nil, nil).Times(1),
		mtsMock.EXPECT().Delete(dbCtx, mt).Return(nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.NoError(t, res.Err)
	require.True(t, res.Found)
	require.True(t, res.Dangling)
	require.Equal(t, mt.Event, res.Event)
}

func TestManifestWorker_processTask_StoreDeleteDeadlineExceededError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	mt := fakeManifestTask()
	m := &models.Manifest{RepositoryID: mt.RepositoryID, ID: mt.ManifestID}

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		mtsMock.EXPECT().Next(dbCtx).Return(mt, nil).Times(1),
		mtsMock.EXPECT().IsDangling(dbCtx, mt).Return(true, nil).Times(1),
		msMock.EXPECT().Delete(dbCtx, m.NamespaceID, m.RepositoryID, m.ID).Return(nil, context.DeadlineExceeded).Times(1),
		txMock.EXPECT().Rollback().Return(nil).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, context.DeadlineExceeded.Error())
	require.True(t, res.Found)
	require.True(t, res.Dangling)
	require.Equal(t, mt.Event, res.Event)
}

func TestManifestWorker_processTask_StoreDeleteUnknownError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	processTxMock := storemock.NewMockTransactor(ctrl)
	postponeTxMock := storemock.NewMockTransactor(ctrl)
	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	mt := fakeManifestTask()
	m := &models.Manifest{RepositoryID: mt.RepositoryID, ID: mt.ManifestID}

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(processTxMock, nil).Times(1),
		mtsMock.EXPECT().Next(dbCtx).Return(mt, nil).Times(1),
		mtsMock.EXPECT().IsDangling(dbCtx, mt).Return(true, nil).Times(1),
		msMock.EXPECT().Delete(dbCtx, m.NamespaceID, m.RepositoryID, m.ID).Return(nil, fakeErrorA).Times(1),
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(postponeTxMock, nil).Times(1),
		mtsMock.EXPECT().FindAndLock(dbCtx, mt.NamespaceID, mt.RepositoryID, mt.ManifestID).Return(mt, nil).Times(1),
		mtsMock.EXPECT().Postpone(dbCtx, mt, isDuration{5 * time.Minute}).Return(nil).Times(1),
		postponeTxMock.EXPECT().Commit().Return(nil).Times(1),
		postponeTxMock.EXPECT().Rollback().Return(nil).Times(1),
		processTxMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fakeErrorA.Error())
	require.True(t, res.Found)
	require.True(t, res.Dangling)
	require.Equal(t, mt.Event, res.Event)
}

func TestManifestWorker_processTask_IsDanglingNo(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	mt := fakeManifestTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		mtsMock.EXPECT().Next(dbCtx).Return(mt, nil).Times(1),
		mtsMock.EXPECT().IsDangling(dbCtx, mt).Return(false, nil).Times(1),
		mtsMock.EXPECT().Delete(dbCtx, mt).Return(nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.NoError(t, res.Err)
	require.True(t, res.Found)
	require.False(t, res.Dangling)
	require.Equal(t, mt.Event, res.Event)
}

func TestManifestWorker_processTask_IsDanglingNo_DeleteTaskError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	processTxMock := storemock.NewMockTransactor(ctrl)
	postponeTxMock := storemock.NewMockTransactor(ctrl)
	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	mt := fakeManifestTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(processTxMock, nil).Times(1),
		mtsMock.EXPECT().Next(dbCtx).Return(mt, nil).Times(1),
		mtsMock.EXPECT().IsDangling(dbCtx, mt).Return(false, nil).Times(1),
		mtsMock.EXPECT().Delete(dbCtx, mt).Return(fakeErrorA).Times(1),
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(postponeTxMock, nil).Times(1),
		mtsMock.EXPECT().FindAndLock(dbCtx, mt.NamespaceID, mt.RepositoryID, mt.ManifestID).Return(mt, nil).Times(1),
		mtsMock.EXPECT().Postpone(dbCtx, mt, isDuration{5 * time.Minute}).Return(nil).Times(1),
		postponeTxMock.EXPECT().Commit().Return(nil).Times(1),
		postponeTxMock.EXPECT().Rollback().Return(nil).Times(1),
		processTxMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fakeErrorA.Error())
	require.True(t, res.Found)
	require.False(t, res.Dangling)
	require.Equal(t, mt.Event, res.Event)
}

func TestManifestWorker_processTask_IsDanglingNo_CommitError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	mt := fakeManifestTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		mtsMock.EXPECT().Next(dbCtx).Return(mt, nil).Times(1),
		mtsMock.EXPECT().IsDangling(dbCtx, mt).Return(false, nil).Times(1),
		mtsMock.EXPECT().Delete(dbCtx, mt).Return(nil).Times(1),
		txMock.EXPECT().Commit().Return(fakeErrorA).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrConnDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fmt.Errorf("committing database transaction: %w", fakeErrorA).Error())
	require.True(t, res.Found)
	require.False(t, res.Dangling)
	require.Equal(t, mt.Event, res.Event)
}

func TestManifestWorker_processTask_RollbackOnExitUnknownError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		mtsMock.EXPECT().Next(dbCtx).Return(nil, fakeErrorA).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrConnDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fakeErrorA.Error())
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}

func TestManifestWorker_Run(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		mtsMock.EXPECT().Next(dbCtx).Return(nil, nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.Run(context.Background())
	require.NoError(t, res.Err)
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}

func TestManifestWorker_Run_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockManifestStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	w := NewManifestWorker(dbMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}

	dbMock.EXPECT().BeginTx(dbCtx, nil).Return(nil, fakeErrorA).Times(1)

	res := w.Run(context.Background())
	require.EqualError(t, res.Err, fmt.Errorf("processing task: creating database transaction: %w", fakeErrorA).Error())
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}
