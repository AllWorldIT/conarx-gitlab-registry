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
	"github.com/docker/distribution/registry/storage/driver"
	drivermock "github.com/docker/distribution/registry/storage/driver/mocks"
	"github.com/hashicorp/go-multierror"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	btsMock *storemock.MockGCBlobTaskStore
	bsMock  *storemock.MockBlobStore
)

func mockBlobStores(tb testing.TB, ctrl *gomock.Controller) {
	tb.Helper()

	btsMock = storemock.NewMockGCBlobTaskStore(ctrl)
	bsMock = storemock.NewMockBlobStore(ctrl)

	bkpBts := blobTaskStoreConstructor
	bkpBs := blobStoreConstructor

	blobTaskStoreConstructor = func(db datastore.Queryer) datastore.GCBlobTaskStore { return btsMock }
	blobStoreConstructor = func(db datastore.Queryer) datastore.BlobStore { return bsMock }

	tb.Cleanup(func() {
		blobTaskStoreConstructor = bkpBts
		blobStoreConstructor = bkpBs
	})
}

func blobPath(d digest.Digest) string {
	return fmt.Sprintf("/docker/registry/v2/blobs/%s/%s/%s", d.Algorithm(), d.Hex()[0:2], d.Hex())
}

func Test_NewBlobWorker(t *testing.T) {
	ctrl := gomock.NewController(t)

	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)
	require.NotNil(t, w.logger)
	require.Equal(t, defaultTxTimeout, w.txTimeout)
	require.Equal(t, defaultStorageTimeout, w.storageTimeout)
}

func Test_NewBlobWorker_WithLogger(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()

	logger := log.GetLogger(log.WithContext(ctx))
	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)
	w := NewBlobWorker(dbMock, driverMock, WithBlobLogger(logger))

	require.Equal(t, logger.WithFields(log.Fields{componentKey: w.name}), w.logger)
}

func Test_NewBlobWorker_WithTxDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)

	d := 10 * time.Minute
	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)
	w := NewBlobWorker(dbMock, driverMock, WithBlobTxTimeout(d))

	require.Equal(t, d, w.txTimeout)
}

func Test_NewBlobWorker_WithStorageDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)

	d := 1 * time.Minute
	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)
	w := NewBlobWorker(dbMock, driverMock, WithBlobStorageTimeout(d))

	require.Equal(t, d, w.storageTimeout)
}

func fakeBlobTask() *models.GCBlobTask {
	return &models.GCBlobTask{
		Digest:      "sha256:c6f988f4874bb0add23a778f753c65efe992244e148a1d2ec2a8b664fb66bbd1",
		ReviewAfter: time.Now().Add(-10 * time.Minute),
		ReviewCount: 1,
		Event:       "blob_upload",
	}
}

func TestBlobWorker_processTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)
	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	driverCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultStorageTimeout)}
	bt := fakeBlobTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(true, nil).Times(1),
		driverMock.EXPECT().Delete(driverCtx, blobPath(bt.Digest)).Return(nil).Times(1),
		bsMock.EXPECT().FindByDigest(dbCtx, bt.Digest).Return(&models.Blob{}, nil).Times(1),
		bsMock.EXPECT().Delete(dbCtx, bt.Digest).Return(nil).Times(1),
		btsMock.EXPECT().Delete(dbCtx, bt).Return(nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.NoError(t, res.Err)
	require.True(t, res.Found)
	require.True(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_BeginTxError(t *testing.T) {
	ctrl := gomock.NewController(t)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)
	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	dbMock.EXPECT().BeginTx(dbCtx, nil).Return(nil, fakeErrorA).Times(1)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fmt.Errorf("creating database transaction: %w", fakeErrorA).Error())
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}

func TestBlobWorker_processTask_NextError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(nil, fakeErrorA).Times(1),
		txMock.EXPECT().Rollback().Return(nil).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fakeErrorA.Error())
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}

func TestBlobWorker_processTask_None(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(nil, nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.NoError(t, res.Err)
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}

func TestBlobWorker_processTask_None_CommitError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(nil, nil).Times(1),
		txMock.EXPECT().Commit().Return(fakeErrorA).Times(1),
		txMock.EXPECT().Rollback().Return(nil).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fmt.Errorf("committing database transaction: %w", fakeErrorA).Error())
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}

func TestBlobWorker_processTask_IsDanglingError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	bt := fakeBlobTask()
	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(false, sql.ErrConnDone).Times(1),
		btsMock.EXPECT().Postpone(dbCtx, bt, isDuration{10 * time.Minute}).Return(nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(nil).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, sql.ErrConnDone.Error())
	require.True(t, res.Found)
	require.False(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_IsDanglingErrorAndPostponeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	bt := fakeBlobTask()
	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(false, fakeErrorA).Times(1),
		btsMock.EXPECT().Postpone(dbCtx, bt, isDuration{10 * time.Minute}).Return(fakeErrorB).Times(1),
		txMock.EXPECT().Rollback().Return(nil).Times(1),
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
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_IsDanglingErrorAndPostponeCommitError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	bt := fakeBlobTask()
	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(true, fakeErrorA).Times(1),
		btsMock.EXPECT().Postpone(dbCtx, bt, isDuration{10 * time.Minute}).Return(nil).Times(1),
		txMock.EXPECT().Commit().Return(fakeErrorB).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrConnDone).Times(1),
	)

	res := w.processTask(context.Background())
	expectedErr := multierror.Error{
		Errors: []error{
			fakeErrorA,
			fmt.Errorf("committing database transaction: %s", fakeErrorB),
		},
	}
	require.EqualError(t, res.Err, expectedErr.Error())
	require.True(t, res.Found)
	require.False(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_IsDanglingDeadlineExceededError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	bt := fakeBlobTask()
	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(false, context.DeadlineExceeded).Times(1),
		txMock.EXPECT().Rollback().Return(nil).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, context.DeadlineExceeded.Error())
	require.True(t, res.Found)
	require.False(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_StoreDeleteNotFoundError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	driverCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultStorageTimeout)}
	bt := fakeBlobTask()
	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(true, nil).Times(1),
		driverMock.EXPECT().Delete(driverCtx, blobPath(bt.Digest)).Return(nil).Times(1),
		bsMock.EXPECT().FindByDigest(dbCtx, bt.Digest).Return(&models.Blob{}, nil).Times(1),
		bsMock.EXPECT().Delete(dbCtx, bt.Digest).Return(datastore.ErrNotFound).Times(1),
		btsMock.EXPECT().Delete(dbCtx, bt).Return(nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.NoError(t, res.Err)
	require.True(t, res.Found)
	require.True(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_StoreDeleteDeadlineExceededError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	driverCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultStorageTimeout)}
	bt := fakeBlobTask()
	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(true, nil).Times(1),
		driverMock.EXPECT().Delete(driverCtx, blobPath(bt.Digest)).Return(nil).Times(1),
		bsMock.EXPECT().FindByDigest(dbCtx, bt.Digest).Return(&models.Blob{}, nil).Times(1),
		bsMock.EXPECT().Delete(dbCtx, bt.Digest).Return(context.DeadlineExceeded).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, context.DeadlineExceeded.Error())
	require.True(t, res.Found)
	require.True(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_StoreDeleteUnknownError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	driverCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultStorageTimeout)}
	bt := fakeBlobTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(true, nil).Times(1),
		driverMock.EXPECT().Delete(driverCtx, blobPath(bt.Digest)).Return(nil).Times(1),
		bsMock.EXPECT().FindByDigest(dbCtx, bt.Digest).Return(&models.Blob{}, nil).Times(1),
		bsMock.EXPECT().Delete(dbCtx, bt.Digest).Return(fakeErrorA).Times(1),
		btsMock.EXPECT().Postpone(dbCtx, bt, isDuration{10 * time.Minute}).Return(nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fakeErrorA.Error())
	require.True(t, res.Found)
	require.True(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_StoreDeleteUnknownErrorAndPostponeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	driverCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultStorageTimeout)}
	bt := fakeBlobTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(true, nil).Times(1),
		driverMock.EXPECT().Delete(driverCtx, blobPath(bt.Digest)).Return(nil).Times(1),
		bsMock.EXPECT().FindByDigest(dbCtx, bt.Digest).Return(&models.Blob{}, nil).Times(1),
		bsMock.EXPECT().Delete(dbCtx, bt.Digest).Return(fakeErrorA).Times(1),
		btsMock.EXPECT().Postpone(dbCtx, bt, isDuration{10 * time.Minute}).Return(fakeErrorB).Times(1),
		txMock.EXPECT().Rollback().Return(nil).Times(1),
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
	require.True(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_VacuumNotFoundError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	driverCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultStorageTimeout)}
	bt := fakeBlobTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(true, nil).Times(1),
		driverMock.EXPECT().Delete(driverCtx, blobPath(bt.Digest)).Return(driver.PathNotFoundError{}).Times(1),
		bsMock.EXPECT().Delete(dbCtx, bt.Digest).Return(nil).Times(1),
		btsMock.EXPECT().Delete(dbCtx, bt).Return(nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.NoError(t, res.Err)
	require.True(t, res.Found)
	require.True(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_VacuumUnknownError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	driverCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultStorageTimeout)}
	bt := fakeBlobTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(true, nil).Times(1),
		driverMock.EXPECT().Delete(driverCtx, blobPath(bt.Digest)).Return(fakeErrorA).Times(1),
		btsMock.EXPECT().Postpone(dbCtx, bt, isDuration{10 * time.Minute}).Return(nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fmt.Errorf("deleting blob from storage: %w", fakeErrorA).Error())
	require.True(t, res.Found)
	require.True(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_FindByDigestError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	driverCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultStorageTimeout)}
	bt := fakeBlobTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(true, nil).Times(1),
		driverMock.EXPECT().Delete(driverCtx, blobPath(bt.Digest)).Return(nil).Times(1),
		bsMock.EXPECT().FindByDigest(dbCtx, bt.Digest).Return(nil, fakeErrorA).Times(1),
		bsMock.EXPECT().Delete(dbCtx, bt.Digest).Return(fakeErrorA).Times(1),
		btsMock.EXPECT().Postpone(dbCtx, bt, isDuration{10 * time.Minute}).Return(fakeErrorB).Times(1),
		txMock.EXPECT().Rollback().Return(nil).Times(1),
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
	require.True(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_FindByDigestNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	driverCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultStorageTimeout)}
	bt := fakeBlobTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(true, nil).Times(1),
		driverMock.EXPECT().Delete(driverCtx, blobPath(bt.Digest)).Return(nil).Times(1),
		bsMock.EXPECT().FindByDigest(dbCtx, bt.Digest).Return(nil, nil).Times(1),
		btsMock.EXPECT().Delete(dbCtx, bt).Return(nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.NoError(t, res.Err)
	require.True(t, res.Found)
	require.True(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_VacuumUnknownErrorAndPostponeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	driverCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultStorageTimeout)}
	bt := fakeBlobTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(true, nil).Times(1),
		driverMock.EXPECT().Delete(driverCtx, blobPath(bt.Digest)).Return(fakeErrorA).Times(1),
		btsMock.EXPECT().Postpone(dbCtx, bt, isDuration{10 * time.Minute}).Return(fakeErrorB).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	expectedErr := multierror.Error{
		Errors: []error{
			fmt.Errorf("deleting blob from storage: %w", fakeErrorA),
			fakeErrorB,
		},
	}
	require.EqualError(t, res.Err, expectedErr.Error())
	require.True(t, res.Found)
	require.True(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_IsDanglingNo(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	bt := fakeBlobTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(false, nil).Times(1),
		btsMock.EXPECT().Delete(dbCtx, bt).Return(nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.NoError(t, res.Err)
	require.True(t, res.Found)
	require.False(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_IsDanglingNo_DeleteTaskError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	bt := fakeBlobTask()
	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(false, nil).Times(1),
		btsMock.EXPECT().Delete(dbCtx, bt).Return(fakeErrorA).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fakeErrorA.Error())
	require.True(t, res.Found)
	require.False(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_IsDanglingNo_CommitError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}
	bt := fakeBlobTask()

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(bt, nil).Times(1),
		btsMock.EXPECT().IsDangling(dbCtx, bt).Return(false, nil).Times(1),
		btsMock.EXPECT().Delete(dbCtx, bt).Return(nil).Times(1),
		txMock.EXPECT().Commit().Return(fakeErrorA).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrConnDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fmt.Errorf("committing database transaction: %s", fakeErrorA).Error())
	require.True(t, res.Found)
	require.False(t, res.Dangling)
	require.Equal(t, bt.Event, res.Event)
}

func TestBlobWorker_processTask_RollbackOnExitUnknownError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(nil, fakeErrorA).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrConnDone).Times(1),
	)

	res := w.processTask(context.Background())
	require.EqualError(t, res.Err, fakeErrorA.Error())
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}

func TestBlobWorker_Run(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	txMock := storemock.NewMockTransactor(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}

	gomock.InOrder(
		dbMock.EXPECT().BeginTx(dbCtx, nil).Return(txMock, nil).Times(1),
		btsMock.EXPECT().Next(dbCtx).Return(nil, nil).Times(1),
		txMock.EXPECT().Commit().Return(nil).Times(1),
		txMock.EXPECT().Rollback().Return(sql.ErrTxDone).Times(1),
	)

	res := w.Run(context.Background())
	require.NoError(t, res.Err)
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}

func TestBlobWorker_Run_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBlobStores(t, ctrl)
	clockMock := stubClock(t, time.Now())

	dbMock := storemock.NewMockHandler(ctrl)
	driverMock := drivermock.NewMockStorageDeleter(ctrl)

	w := NewBlobWorker(dbMock, driverMock)

	dbCtx := testutil.IsContextWithDeadline{Deadline: clockMock.Now().Add(defaultTxTimeout)}

	dbMock.EXPECT().BeginTx(dbCtx, nil).Return(nil, fakeErrorA).Times(1)

	res := w.Run(context.Background())
	require.EqualError(t, res.Err, fmt.Errorf("processing task: creating database transaction: %w", fakeErrorA).Error())
	require.False(t, res.Found)
	require.False(t, res.Dangling)
	require.Empty(t, res.Event)
}
