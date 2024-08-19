package checks

import (
	"errors"
	"fmt"
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
	dbName := "cr_db"

	setupMocks := func(
		t *testing.T,
		primaryQueryMatcherF func(*sqlmock.ExpectedQuery),
		replicaQueryMatcherFs []func(*sqlmock.ExpectedQuery),
	) (
		*mocks.MockLoadBalancer, sqlmock.Sqlmock, []sqlmock.Sqlmock, func(),
	) {
		query := fmt.Sprintf(`SELECT(.+)EXISTS(.+)\((.+)SELECT(.+)1(.+)FROM(.+)pg_stat_database(.+)WHERE(.+)datname(.+)=(.+)'%s'(.+)LIMIT(.+)1(.+)\)`, dbName)

		ctrl := gomock.NewController(t)

		// Mock LoadBalancer
		loadBalancer := mocks.NewMockLoadBalancer(ctrl)

		// Mock primary
		primaryMockDB, primaryMock, err := sqlmock.New()
		require.NoError(t, err)
		loadBalancer.EXPECT().Primary().Return(&datastore.DB{DB: primaryMockDB})
		primaryQueryMatcherF(primaryMock.ExpectQuery(query))

		// Mock replicas
		replicaMocks := make([]sqlmock.Sqlmock, len(replicaQueryMatcherFs))
		replicaMockDBs := make([]*datastore.DB, len(replicaQueryMatcherFs))
		for i := range replicaQueryMatcherFs {
			replicaMockDB, replicaMock, err := sqlmock.New()
			require.NoError(t, err)

			replicaMocks[i] = replicaMock
			replicaQueryMatcherFs[i](replicaMock.ExpectQuery(query))
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

		loadBalancer, primaryMock, replicaMocks, doneF := setupMocks(
			t,
			func(eq *sqlmock.ExpectedQuery) {
				rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
				eq.WillReturnRows(rows)
			},
			[]func(*sqlmock.ExpectedQuery){
				func(eq *sqlmock.ExpectedQuery) {
					rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
					eq.WillReturnRows(rows)
				},
				func(eq *sqlmock.ExpectedQuery) {
					rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
					eq.WillReturnRows(rows)
				},
			},
		)
		defer doneF()

		// Run the DBChecker
		checkFunc := DBChecker(2*time.Second, loadBalancer, dbName)
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

		loadBalancer, primaryMock, replicaMocks, doneF := setupMocks(
			t,
			func(eq *sqlmock.ExpectedQuery) {
				eq.WillReturnError(errors.New("Maryna is Boryna"))
			},
			[]func(*sqlmock.ExpectedQuery){
				func(eq *sqlmock.ExpectedQuery) {
					rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
					eq.WillReturnRows(rows)
				},
				func(eq *sqlmock.ExpectedQuery) {
					rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
					eq.WillReturnRows(rows)
				},
			},
		)
		defer doneF()

		// Run the DBChecker
		checkFunc := DBChecker(2*time.Second, loadBalancer, dbName)
		err := checkFunc()

		// Verify error message
		require.ErrorContains(t, err, "Maryna is Boryna")

		// Verify DB mock expectations
		require.NoError(t, primaryMock.ExpectationsWereMet())
		for _, m := range replicaMocks {
			require.NoError(t, m.ExpectationsWereMet())
		}
	})

	t.Run("primary succeds replica fails", func(t *testing.T) {
		t.Parallel()

		loadBalancer, primaryMock, replicaMocks, doneF := setupMocks(
			t,
			func(eq *sqlmock.ExpectedQuery) {
				rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
				eq.WillReturnRows(rows)
			},
			[]func(*sqlmock.ExpectedQuery){
				func(eq *sqlmock.ExpectedQuery) {
					rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
					eq.WillReturnRows(rows)
				},
				func(eq *sqlmock.ExpectedQuery) {
					eq.WillReturnError(errors.New("Maryna is Boryna"))
				},
			},
		)
		defer doneF()

		// Run the DBChecker
		checkFunc := DBChecker(2*time.Second, loadBalancer, dbName)
		err := checkFunc()

		// Verify error message
		require.ErrorContains(t, err, "Maryna is Boryna")

		// Verify DB mock expectations
		require.NoError(t, primaryMock.ExpectationsWereMet())
		for _, m := range replicaMocks {
			require.NoError(t, m.ExpectationsWereMet())
		}
	})

	t.Run("primary fails due to no access to schema replicas succeed", func(t *testing.T) {
		t.Parallel()

		loadBalancer, primaryMock, replicaMocks, doneF := setupMocks(
			t,
			func(eq *sqlmock.ExpectedQuery) {
				rows := sqlmock.NewRows([]string{"exists"}).AddRow(false)
				eq.WillReturnRows(rows)
			},
			[]func(*sqlmock.ExpectedQuery){
				func(eq *sqlmock.ExpectedQuery) {
					rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
					eq.WillReturnRows(rows)
				},
				func(eq *sqlmock.ExpectedQuery) {
					rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
					eq.WillReturnRows(rows)
				},
			},
		)
		defer doneF()

		// Run the DBChecker
		checkFunc := DBChecker(2*time.Second, loadBalancer, dbName)
		err := checkFunc()

		// Verify error message
		require.ErrorContains(t, err, "cannot access database schema")

		// Verify DB mock expectations
		require.NoError(t, primaryMock.ExpectationsWereMet())
		for _, m := range replicaMocks {
			require.NoError(t, m.ExpectationsWereMet())
		}
	})

	t.Run("replica fails due to no access to schema replicas succeed", func(t *testing.T) {
		t.Parallel()

		loadBalancer, primaryMock, replicaMocks, doneF := setupMocks(
			t,
			func(eq *sqlmock.ExpectedQuery) {
				rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
				eq.WillReturnRows(rows)
			},
			[]func(*sqlmock.ExpectedQuery){
				func(eq *sqlmock.ExpectedQuery) {
					rows := sqlmock.NewRows([]string{"exists"}).AddRow(false)
					eq.WillReturnRows(rows)
				},
				func(eq *sqlmock.ExpectedQuery) {
					rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
					eq.WillReturnRows(rows)
				},
			},
		)
		defer doneF()

		// Run the DBChecker
		checkFunc := DBChecker(2*time.Second, loadBalancer, dbName)
		err := checkFunc()

		// Verify error message
		require.ErrorContains(t, err, "cannot access database schema")

		// Verify DB mock expectations
		require.NoError(t, primaryMock.ExpectationsWereMet())
		for _, m := range replicaMocks {
			require.NoError(t, m.ExpectationsWereMet())
		}
	})
}
