package checks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestFileChecker(t *testing.T) {
	if err := FileChecker("/tmp").Check(); err == nil {
		t.Errorf("/tmp was expected as exists")
	}

	if err := FileChecker("NoSuchFileFromMoon").Check(); err != nil {
		t.Errorf("NoSuchFileFromMoon was expected as not exists, error:%v", err)
	}
}

func TestHTTPChecker(t *testing.T) {
	if err := HTTPChecker("https://www.google.cybertron", 200, 0, nil).Check(); err == nil {
		t.Errorf("Google on Cybertron was expected as not exists")
	}

	if err := HTTPChecker("https://www.google.pt", 200, 0, nil).Check(); err != nil {
		t.Errorf("Google at Portugal was expected as exists, error:%v", err)
	}
}

func TestDBChecker(t *testing.T) {
	checkTimeout := 200 * time.Millisecond

	setupMocks := func(
		t *testing.T,
		primaryQueryMatcherF func(*sqlmock.ExpectedPing),
		replicaQueryMatcherFs []func(*sqlmock.ExpectedPing),
	) (
		*mocks.MockLoadBalancer, sqlmock.Sqlmock, []sqlmock.Sqlmock, func(),
	) {
		ctrl := gomock.NewController(t)

		// Mock LoadBalancer
		loadBalancer := mocks.NewMockLoadBalancer(ctrl)

		// Mock primary
		primaryMockDB, primaryMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		loadBalancer.EXPECT().Primary().Return(&datastore.DB{DB: primaryMockDB})
		primaryQueryMatcherF(primaryMock.ExpectPing())

		// Mock replicas
		replicaMocks := make([]sqlmock.Sqlmock, len(replicaQueryMatcherFs))
		replicaMockDBs := make([]*datastore.DB, len(replicaQueryMatcherFs))
		for i := range replicaQueryMatcherFs {
			replicaMockDB, replicaMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
			require.NoError(t, err)

			replicaMocks[i] = replicaMock
			replicaQueryMatcherFs[i](replicaMock.ExpectPing())
			replicaMockDBs[i] = &datastore.DB{DB: replicaMockDB}
		}
		loadBalancer.EXPECT().Replicas().Return(replicaMockDBs)

		doneF := func() {
			primaryMockDB.Close()
			for _, m := range replicaMockDBs {
				m.Close()
			}
		}
		return loadBalancer, primaryMock, replicaMocks, doneF
	}

	t.Run("both primary and replicas succeed", func(t *testing.T) {
		t.Parallel()

		ctx, cancelF := context.WithCancel(context.Background())
		defer cancelF()

		loadBalancer, primaryMock, replicaMocks, doneF := setupMocks(
			t,
			func(eq *sqlmock.ExpectedPing) {
				eq.WillReturnError(nil)
			},
			[]func(*sqlmock.ExpectedPing){
				func(eq *sqlmock.ExpectedPing) {
					eq.WillReturnError(nil)
				},
				func(eq *sqlmock.ExpectedPing) {
					eq.WillReturnError(nil)
				},
			},
		)
		defer doneF()

		// Run the DBChecker
		checkFunc := DBChecker(ctx, checkTimeout, loadBalancer)
		err := checkFunc()

		// Verify error message
		require.NoError(t, err)

		// Verify DB mock expectations
		require.NoError(t, primaryMock.ExpectationsWereMet())
		for _, m := range replicaMocks {
			require.NoError(t, m.ExpectationsWereMet())
		}
	})

	t.Run("primary fails replicas succeed", func(t *testing.T) {
		t.Parallel()

		ctx, cancelF := context.WithCancel(context.Background())
		defer cancelF()

		loadBalancer, primaryMock, replicaMocks, doneF := setupMocks(
			t,
			func(eq *sqlmock.ExpectedPing) {
				eq.WillReturnError(errors.New("Maryna is Boryna"))
			},
			[]func(*sqlmock.ExpectedPing){
				func(eq *sqlmock.ExpectedPing) {
					eq.WillReturnError(nil)
				},
				func(eq *sqlmock.ExpectedPing) {
					eq.WillReturnError(nil)
				},
			},
		)
		defer doneF()

		// Run the DBChecker
		checkFunc := DBChecker(ctx, checkTimeout, loadBalancer)
		err := checkFunc()

		// Verify error message
		require.ErrorContains(t, err, "Maryna is Boryna")

		// Verify DB mock expectations
		require.NoError(t, primaryMock.ExpectationsWereMet())
		for _, m := range replicaMocks {
			require.NoError(t, m.ExpectationsWereMet())
		}
	})

	t.Run("primary succeeds replica fails", func(t *testing.T) {
		t.Parallel()

		ctx, cancelF := context.WithCancel(context.Background())
		defer cancelF()

		loadBalancer, primaryMock, replicaMocks, doneF := setupMocks(
			t,
			func(eq *sqlmock.ExpectedPing) {
				eq.WillReturnError(nil)
			},
			[]func(*sqlmock.ExpectedPing){
				func(eq *sqlmock.ExpectedPing) {
					eq.WillReturnError(errors.New("Maryna is Boryna"))
				},
				func(eq *sqlmock.ExpectedPing) {
					eq.WillReturnError(nil)
				},
			},
		)
		defer doneF()

		// Run the DBChecker
		checkFunc := DBChecker(ctx, checkTimeout, loadBalancer)
		err := checkFunc()

		// Verify error message
		require.ErrorContains(t, err, "Maryna is Boryna")

		// Verify DB mock expectations
		require.NoError(t, primaryMock.ExpectationsWereMet())
		for _, m := range replicaMocks {
			require.NoError(t, m.ExpectationsWereMet())
		}
	})

	t.Run("primary fails due to timeout, replicas succeed", func(t *testing.T) {
		t.Parallel()

		ctx, cancelF := context.WithCancel(context.Background())
		defer cancelF()

		loadBalancer, primaryMock, replicaMocks, doneF := setupMocks(
			t,
			func(eq *sqlmock.ExpectedPing) {
				eq.WillDelayFor(checkTimeout + 100*time.Millisecond).WillReturnError(nil)
			},
			[]func(*sqlmock.ExpectedPing){
				func(eq *sqlmock.ExpectedPing) {
					eq.WillReturnError(nil)
				},
				func(eq *sqlmock.ExpectedPing) {
					eq.WillReturnError(nil)
				},
			},
		)
		defer doneF()

		// Run the DBChecker
		checkFunc := DBChecker(ctx, checkTimeout, loadBalancer)
		err := checkFunc()

		// Verify error message
		require.ErrorContains(t, err, "canceling query due to user request")

		// Verify DB mock expectations
		require.NoError(t, primaryMock.ExpectationsWereMet())
		for _, m := range replicaMocks {
			require.NoError(t, m.ExpectationsWereMet())
		}
	})

	t.Run("replica fails due to timeout, primary succeeds", func(t *testing.T) {
		t.Parallel()

		ctx, cancelF := context.WithCancel(context.Background())
		defer cancelF()

		loadBalancer, primaryMock, replicaMocks, doneF := setupMocks(
			t,
			func(eq *sqlmock.ExpectedPing) {
				eq.WillReturnError(nil)
			},
			[]func(*sqlmock.ExpectedPing){
				func(eq *sqlmock.ExpectedPing) {
					eq.WillDelayFor(checkTimeout + 100*time.Millisecond).WillReturnError(nil)
				},
				func(eq *sqlmock.ExpectedPing) {
					eq.WillReturnError(nil)
				},
			},
		)
		defer doneF()

		// Run the DBChecker
		checkFunc := DBChecker(ctx, checkTimeout, loadBalancer)
		err := checkFunc()

		// Verify error message
		require.ErrorContains(t, err, "canceling query due to user request")

		// Verify DB mock expectations
		require.NoError(t, primaryMock.ExpectationsWereMet())
		for _, m := range replicaMocks {
			require.NoError(t, m.ExpectationsWereMet())
		}
	})
}
