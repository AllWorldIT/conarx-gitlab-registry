package handlers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/docker/distribution/configuration"
	v1 "github.com/docker/distribution/registry/api/gitlab/v1"
	"github.com/docker/distribution/registry/auth"
	amocks "github.com/docker/distribution/registry/auth/mocks"
	"github.com/docker/distribution/registry/datastore"
	dmocks "github.com/docker/distribution/registry/datastore/mocks"
	"github.com/docker/distribution/registry/datastore/models"
	dtestutil "github.com/docker/distribution/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type BackgroundMigrationsHandlerTestSuite struct {
	suite.Suite
	ctrl           *gomock.Controller
	mockLB         *dmocks.MockLoadBalancer
	mockPrimaryDB  *sql.DB
	mockQuerier    sqlmock.Sqlmock
	mockAccessCtrl *amocks.MockAccessController
	app            *App
	server         *httptest.Server
}

func (s *BackgroundMigrationsHandlerTestSuite) SetupTest() {
	var err error

	s.ctrl = gomock.NewController(s.T())
	s.mockLB = dmocks.NewMockLoadBalancer(s.ctrl)
	s.mockPrimaryDB, s.mockQuerier, err = sqlmock.New()
	require.NoError(s.T(), err)

	// Create mock access controller
	s.mockAccessCtrl = amocks.NewMockAccessController(s.ctrl)

	ctx := dtestutil.NewContextWithLogger(s.T())
	config := &configuration.Configuration{
		Database: configuration.Database{
			Enabled: configuration.DatabaseEnabledTrue,
		},
	}

	// Create app with mocked database and access controller
	s.app = &App{
		Config:           config,
		Context:          ctx,
		router:           &metaRouter{gitlab: v1.RouterWithPrefix("")},
		db:               s.mockLB,
		accessController: s.mockAccessCtrl,
	}

	require.NoError(s.T(), s.app.initMetaRouter())

	s.server = httptest.NewServer(s.app)
}

func (s *BackgroundMigrationsHandlerTestSuite) TearDownTest() {
	s.server.Close()
	s.ctrl.Finish()
}

func (s *BackgroundMigrationsHandlerTestSuite) TestGetBackgroundMigrationsStatus_Success() {
	// Prepare test data
	migrations := models.BackgroundMigrations{
		{
			ID:               1,
			Name:             "migration_1",
			Status:           models.BackgroundMigrationActive,
			JobName:          "job_1",
			TargetTable:      "public.repositories",
			TargetColumn:     "id",
			BatchSize:        100,
			StartID:          1,
			EndID:            1000,
			BatchingStrategy: models.SerialKeySetBatchingBBMStrategy,
			TotalTupleCount:  sql.NullInt64{Int64: 500, Valid: true},
			ErrorCode:        models.NullErrCode,
		},
		{
			ID:               2,
			Name:             "migration_2",
			Status:           models.BackgroundMigrationPaused,
			JobName:          "job_2",
			TargetTable:      "public.manifests",
			TargetColumn:     "id",
			BatchSize:        50,
			StartID:          1,
			EndID:            500,
			BatchingStrategy: models.NullBatchingBBMStrategy,
			TotalTupleCount:  sql.NullInt64{Valid: false},
			ErrorCode:        models.InvalidTableBBMErrCode,
		},
	}

	// Setup sqlmock rows from migrations variable
	rows := sqlmock.NewRows([]string{
		"id",
		"name",
		"min_value",
		"max_value",
		"batch_size",
		"status",
		"job_signature_name",
		"table_name",
		"column_name",
		"failure_error_code",
		"batching_strategy",
		"total_tuple_count",
	})

	// Add rows from migrations slice
	for _, m := range migrations {
		errVal, _ := m.ErrorCode.Value()
		bStrategy, _ := m.BatchingStrategy.Value()
		rows.AddRow(
			m.ID,
			m.Name,
			m.StartID,
			m.EndID,
			m.BatchSize,
			int(m.Status),
			m.JobName,
			m.TargetTable,
			m.TargetColumn,
			errVal,
			bStrategy,
			m.TotalTupleCount,
		)
	}

	// Expect authorization check with wildcard access to background-migrations
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), auth.Access{
			Resource: auth.Resource{
				Type: "registry",
				Name: "background-migrations",
			},
			Action: "*",
		}).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Expect the query
	s.mockQuerier.ExpectQuery(`SELECT.*FROM.*batched_background_migrations.*ORDER BY.*id ASC`).
		WillReturnRows(rows)

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(1)

	// Make request
	resp, err := http.Get(fmt.Sprintf("%s%s", s.server.URL, v1.BBM.Path))
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify response
	require.Equal(s.T(), http.StatusOK, resp.StatusCode)
	require.Equal(s.T(), "application/json", resp.Header.Get("Content-Type"))

	var result BackgroundMigrationsListResponse
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(s.T(), err)

	// Verify response content
	require.Len(s.T(), result.Migrations, 2)

	// Check first migration
	assert.Equal(s.T(), 1, result.Migrations[0].ID)
	assert.Equal(s.T(), "migration_1", result.Migrations[0].Name)
	assert.Equal(s.T(), "active", result.Migrations[0].Status)
	assert.Equal(s.T(), "job_1", result.Migrations[0].JobSignatureName)
	assert.Equal(s.T(), "public.repositories", result.Migrations[0].TargetTable)
	assert.Equal(s.T(), "id", result.Migrations[0].TargetColumn)
	assert.Equal(s.T(), 100, result.Migrations[0].BatchSize)
	assert.Equal(s.T(), 1, result.Migrations[0].MinValue)
	assert.Equal(s.T(), 1000, result.Migrations[0].MaxValue)
	assert.Empty(s.T(), result.Migrations[0].BatchingStrategy)
	if assert.NotNil(s.T(), result.Migrations[0].TotalTupleCount) {
		assert.Equal(s.T(), int64(500), *result.Migrations[0].TotalTupleCount)
	}
	assert.Nilf(s.T(), result.Migrations[0].ErrorCode, "error is not nil: %v", result.Migrations[0].ErrorCode)

	// Check second migration
	assert.Equal(s.T(), 2, result.Migrations[1].ID)
	assert.Equal(s.T(), "migration_2", result.Migrations[1].Name)
	assert.Equal(s.T(), "paused", result.Migrations[1].Status)
	assert.Nil(s.T(), result.Migrations[1].TotalTupleCount)
	if assert.NotNil(s.T(), result.Migrations[1].ErrorCode) {
		assert.Equal(s.T(), 1, *result.Migrations[1].ErrorCode)
	}
}

func (s *BackgroundMigrationsHandlerTestSuite) TestGetBackgroundMigrationsStatus_Forbidden() {
	mockChallenge := amocks.NewMockChallenge(s.ctrl)
	mockChallenge.EXPECT().SetHeaders(gomock.Any(), gomock.Any()).Times(1)

	// Expect authorization check to deny access
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), auth.Access{
			Resource: auth.Resource{
				Type: "registry",
				Name: "background-migrations",
			},
			Action: "*",
		}).
		Return(nil, mockChallenge)

	// Make request
	resp, err := http.Get(fmt.Sprintf("%s%s", s.server.URL, v1.BBM.Path))
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify 403 Forbidden response
	require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestGetBackgroundMigrationStatus_Success() {
	// Prepare test data
	migration := &models.BackgroundMigration{
		ID:               1,
		Name:             "migration_1",
		Status:           models.BackgroundMigrationActive,
		JobName:          "job_1",
		TargetTable:      "public.repositories",
		TargetColumn:     "id",
		BatchSize:        100,
		StartID:          1,
		EndID:            1000,
		BatchingStrategy: models.SerialKeySetBatchingBBMStrategy,
		TotalTupleCount:  sql.NullInt64{Int64: 500, Valid: true},
		ErrorCode:        models.NullErrCode,
	}

	// Setup sqlmock rows
	rows := sqlmock.NewRows([]string{
		"id",
		"name",
		"min_value",
		"max_value",
		"batch_size",
		"status",
		"job_signature_name",
		"table_name",
		"column_name",
		"failure_error_code",
		"batching_strategy",
		"total_tuple_count",
	})

	errVal, _ := migration.ErrorCode.Value()
	bStrategy, _ := migration.BatchingStrategy.Value()
	rows.AddRow(
		migration.ID,
		migration.Name,
		migration.StartID,
		migration.EndID,
		migration.BatchSize,
		int(migration.Status),
		migration.JobName,
		migration.TargetTable,
		migration.TargetColumn,
		errVal,
		bStrategy,
		migration.TotalTupleCount,
	)

	// Expect authorization check with wildcard access to background-migrations
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), auth.Access{
			Resource: auth.Resource{
				Type: "registry",
				Name: "background-migrations",
			},
			Action: "*",
		}).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Expect the query
	s.mockQuerier.ExpectQuery(`SELECT.*FROM.*batched_background_migrations.*WHERE.*id = \$1`).
		WithArgs(1).
		WillReturnRows(rows)

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(1)

	// Make request
	resp, err := http.Get(fmt.Sprintf("%s%s1/", s.server.URL, v1.BBM.Path))
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify response
	require.Equal(s.T(), http.StatusOK, resp.StatusCode)
	require.Equal(s.T(), "application/json", resp.Header.Get("Content-Type"))

	var result BackgroundMigrationGetResponse
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(s.T(), err)

	// Verify response content
	assert.Equal(s.T(), 1, result.Migration.ID)
	assert.Equal(s.T(), "migration_1", result.Migration.Name)
	assert.Equal(s.T(), "active", result.Migration.Status)
	assert.Equal(s.T(), "job_1", result.Migration.JobSignatureName)
	assert.Equal(s.T(), "public.repositories", result.Migration.TargetTable)
	assert.Equal(s.T(), "id", result.Migration.TargetColumn)
	assert.Equal(s.T(), 100, result.Migration.BatchSize)
	assert.Equal(s.T(), 1, result.Migration.MinValue)
	assert.Equal(s.T(), 1000, result.Migration.MaxValue)
	if assert.NotNil(s.T(), result.Migration.TotalTupleCount) {
		assert.Equal(s.T(), int64(500), *result.Migration.TotalTupleCount)
	}
	assert.Nil(s.T(), result.Migration.ErrorCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestGetBackgroundMigrationStatus_Forbidden() {
	mockChallenge := amocks.NewMockChallenge(s.ctrl)
	mockChallenge.EXPECT().SetHeaders(gomock.Any(), gomock.Any()).Times(1)

	// Expect authorization check to deny access
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), auth.Access{
			Resource: auth.Resource{
				Type: "registry",
				Name: "background-migrations",
			},
			Action: "*",
		}).
		Return(nil, mockChallenge)

	// Make request
	resp, err := http.Get(fmt.Sprintf("%s%s1/", s.server.URL, v1.BBM.Path))
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify 403 Forbidden response
	require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestGetBackgroundMigrationStatus_InvalidID() {
	// Expect authorization check
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Make request with invalid ID
	resp, err := http.Get(fmt.Sprintf("%s%s99999999999999999999999999999999999999999999999/", s.server.URL, v1.BBM.Path))
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify error response
	require.Equal(s.T(), http.StatusBadRequest, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestGetBackgroundMigrationStatus_NotFound() {
	// Expect authorization check
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Expect the query to return no rows
	s.mockQuerier.ExpectQuery(`SELECT.*FROM.*batched_background_migrations.*WHERE.*id.*`).
		WithArgs(1).
		WillReturnError(sql.ErrNoRows)

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(1)

	// Make request
	resp, err := http.Get(fmt.Sprintf("%s%s1/", s.server.URL, v1.BBM.Path))
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify error response
	require.Equal(s.T(), http.StatusNotFound, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestGetBackgroundMigrationStatus_DatabaseError() {
	// Expect authorization check
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Expect the query to return an error
	s.mockQuerier.ExpectQuery(`SELECT.*FROM.*batched_background_migrations.*WHERE.*id.*`).
		WithArgs(1).
		WillReturnError(fmt.Errorf("database connection error"))

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(1)

	// Make request
	resp, err := http.Get(fmt.Sprintf("%s%s1/", s.server.URL, v1.BBM.Path))
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify error response
	require.Equal(s.T(), http.StatusInternalServerError, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestGetBackgroundMigrationsStatus_DatabaseError() {
	// Expect authorization check
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Expect the query to return an error
	s.mockQuerier.ExpectQuery(`SELECT.*FROM.*batched_background_migrations.*ORDER BY.*id ASC`).
		WillReturnError(fmt.Errorf("database connection error"))

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(1)

	// Make request
	resp, err := http.Get(fmt.Sprintf("%s%s", s.server.URL, v1.BBM.Path))
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify error response
	require.Equal(s.T(), http.StatusInternalServerError, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestGetBackgroundMigrationsStatus_EmptyResults() {
	// Expect authorization check
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Setup empty sqlmock rows
	rows := sqlmock.NewRows([]string{
		"id",
		"name",
		"min_value",
		"max_value",
		"batch_size",
		"status",
		"job_signature_name",
		"table_name",
		"column_name",
		"failure_error_code",
		"batching_strategy",
		"total_tuple_count",
	})

	// Expect the query
	s.mockQuerier.ExpectQuery(`SELECT.*FROM.*batched_background_migrations.*ORDER BY.*id ASC`).
		WillReturnRows(rows)

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(1)

	// Make request
	resp, err := http.Get(fmt.Sprintf("%s%s", s.server.URL, v1.BBM.Path))
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify response
	require.Equal(s.T(), http.StatusOK, resp.StatusCode)
	require.Equal(s.T(), "application/json", resp.Header.Get("Content-Type"))

	var result BackgroundMigrationsListResponse
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(s.T(), err)

	// Verify empty results
	require.Empty(s.T(), result.Migrations)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestPauseBackgroundMigrations_Success() {
	// Expect authorization check with wildcard access to background-migrations
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), auth.Access{
			Resource: auth.Resource{
				Type: "registry",
				Name: "background-migrations",
			},
			Action: "*",
		}).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Expect the update query
	s.mockQuerier.ExpectExec(`UPDATE.*batched_background_migrations.*SET.*status.*WHERE.*status.*`).
		WillReturnResult(sqlmock.NewResult(0, 2))

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(1)

	// Make request
	resp, err := http.Post(fmt.Sprintf("%s%spause/", s.server.URL, v1.BBM.Path), "application/json", nil)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify response
	require.Equal(s.T(), http.StatusOK, resp.StatusCode)
	require.Equal(s.T(), "application/json", resp.Header.Get("Content-Type"))

	var result BackgroundMigrationActionResponse
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(s.T(), err)

	// Verify response content
	assert.True(s.T(), result.Success)
	assert.Equal(s.T(), "All eligible background migrations have been paused", result.Message)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestPauseBackgroundMigrations_Forbidden() {
	mockChallenge := amocks.NewMockChallenge(s.ctrl)
	mockChallenge.EXPECT().SetHeaders(gomock.Any(), gomock.Any()).Times(1)

	// Expect authorization check to deny access
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), auth.Access{
			Resource: auth.Resource{
				Type: "registry",
				Name: "background-migrations",
			},
			Action: "*",
		}).
		Return(nil, mockChallenge)

	// Make request
	resp, err := http.Post(fmt.Sprintf("%s%spause/", s.server.URL, v1.BBM.Path), "application/json", nil)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify 403 Forbidden response
	require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestPauseBackgroundMigrations_DatabaseError() {
	// Expect authorization check
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Expect the update query to fail
	s.mockQuerier.ExpectExec(`UPDATE.*batched_background_migrations.*SET.*status.*WHERE.*status.*`).
		WillReturnError(fmt.Errorf("database error"))

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(1)

	// Make request
	resp, err := http.Post(fmt.Sprintf("%s%spause/", s.server.URL, v1.BBM.Path), "application/json", nil)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify error response
	require.Equal(s.T(), http.StatusInternalServerError, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestResumeBackgroundMigrations_Success() {
	// Expect authorization check with wildcard access to background-migrations
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), auth.Access{
			Resource: auth.Resource{
				Type: "registry",
				Name: "background-migrations",
			},
			Action: "*",
		}).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Expect the update query
	s.mockQuerier.ExpectExec(`UPDATE.*batched_background_migrations.*SET.*status.*WHERE.*status.*`).
		WillReturnResult(sqlmock.NewResult(0, 3))

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(1)

	// Make request
	resp, err := http.Post(fmt.Sprintf("%s%sresume/", s.server.URL, v1.BBM.Path), "application/json", nil)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify response
	require.Equal(s.T(), http.StatusOK, resp.StatusCode)
	require.Equal(s.T(), "application/json", resp.Header.Get("Content-Type"))

	var result BackgroundMigrationActionResponse
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(s.T(), err)

	// Verify response content
	assert.True(s.T(), result.Success)
	assert.Equal(s.T(), "All eligible background migrations have been resumed", result.Message)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestResumeBackgroundMigrations_Forbidden() {
	mockChallenge := amocks.NewMockChallenge(s.ctrl)
	mockChallenge.EXPECT().SetHeaders(gomock.Any(), gomock.Any()).Times(1)

	// Expect authorization check to deny access
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), auth.Access{
			Resource: auth.Resource{
				Type: "registry",
				Name: "background-migrations",
			},
			Action: "*",
		}).
		Return(nil, mockChallenge)

	// Make request
	resp, err := http.Post(fmt.Sprintf("%s%sresume/", s.server.URL, v1.BBM.Path), "application/json", nil)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestResumeBackgroundMigrations_DatabaseError() {
	// Expect authorization check
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Expect the update query to fail
	s.mockQuerier.ExpectExec(`UPDATE.*batched_background_migrations.*SET.*status.*WHERE.*status.*`).
		WillReturnError(fmt.Errorf("database error"))

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(1)

	// Make request
	resp, err := http.Post(fmt.Sprintf("%s%sresume/", s.server.URL, v1.BBM.Path), "application/json", nil)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify error response
	require.Equal(s.T(), http.StatusInternalServerError, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestRestartBackgroundMigration_Success() {
	// Prepare test data - a failed migration
	migration := &models.BackgroundMigration{
		ID:               1,
		Name:             "migration_1",
		Status:           models.BackgroundMigrationFailed,
		JobName:          "job_1",
		TargetTable:      "public.repositories",
		TargetColumn:     "id",
		BatchSize:        100,
		StartID:          1,
		EndID:            1000,
		BatchingStrategy: models.SerialKeySetBatchingBBMStrategy,
		TotalTupleCount:  sql.NullInt64{Int64: 500, Valid: true},
		ErrorCode:        models.InvalidTableBBMErrCode,
	}

	// Setup sqlmock rows for FindById
	findRows := sqlmock.NewRows([]string{
		"id",
		"name",
		"min_value",
		"max_value",
		"batch_size",
		"status",
		"job_signature_name",
		"table_name",
		"column_name",
		"failure_error_code",
		"batching_strategy",
		"total_tuple_count",
	})

	errVal, _ := migration.ErrorCode.Value()
	bStrategy, _ := migration.BatchingStrategy.Value()
	findRows.AddRow(
		migration.ID,
		migration.Name,
		migration.StartID,
		migration.EndID,
		migration.BatchSize,
		int(migration.Status),
		migration.JobName,
		migration.TargetTable,
		migration.TargetColumn,
		errVal,
		bStrategy,
		migration.TotalTupleCount,
	)

	// Setup sqlmock rows for UpdateStatus
	updateRows := sqlmock.NewRows([]string{
		"status",
		"failure_error_code",
	}).AddRow(int(models.BackgroundMigrationActive), nil)

	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), auth.Access{
			Resource: auth.Resource{
				Type: "registry",
				Name: "background-migrations",
			},
			Action: "*",
		}).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Expect the FindById query
	s.mockQuerier.ExpectQuery(`SELECT.*FROM.*batched_background_migrations.*WHERE.*id = \$1`).
		WithArgs(1).
		WillReturnRows(findRows)

	// Expect the UpdateStatus query
	s.mockQuerier.ExpectQuery(`UPDATE.*batched_background_migrations.*SET.*status.*failure_error_code.*WHERE.*id.*`).
		WithArgs(int(models.BackgroundMigrationActive), nil, 1, int(models.BackgroundMigrationRunning), int(models.BackgroundMigrationFinished)).
		WillReturnRows(updateRows)

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(2)

	// Make request
	resp, err := http.Post(fmt.Sprintf("%s/gitlab/v1/restart/1/", s.server.URL), "application/json", nil)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify response
	require.Equal(s.T(), http.StatusOK, resp.StatusCode)
	require.Equal(s.T(), "application/json", resp.Header.Get("Content-Type"))

	var result BackgroundMigrationActionResponse
	err = json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(s.T(), err)

	// Verify response content
	assert.True(s.T(), result.Success)
	assert.Equal(s.T(), "Background migration 1 has been restarted", result.Message)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestRestartBackgroundMigration_Forbidden() {
	mockChallenge := amocks.NewMockChallenge(s.ctrl)
	mockChallenge.EXPECT().SetHeaders(gomock.Any(), gomock.Any()).Times(1)

	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), auth.Access{
			Resource: auth.Resource{
				Type: "registry",
				Name: "background-migrations",
			},
			Action: "*",
		}).
		Return(nil, mockChallenge)

	// Make request
	resp, err := http.Post(fmt.Sprintf("%s/gitlab/v1/restart/1/", s.server.URL), "application/json", nil)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify 403 Forbidden response
	require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestRestartBackgroundMigration_InvalidID() {
	// Expect authorization check
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Make request with invalid ID
	resp, err := http.Post(fmt.Sprintf("%s/gitlab/v1/restart/99999999999999999999999999999999999999999999999/", s.server.URL), "application/json", nil)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify error response
	require.Equal(s.T(), http.StatusBadRequest, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestRestartBackgroundMigration_NotFound() {
	// Expect authorization check
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Expect the query to return no rows
	s.mockQuerier.ExpectQuery(`SELECT.*FROM.*batched_background_migrations.*WHERE.*id.*`).
		WithArgs(999).
		WillReturnError(sql.ErrNoRows)

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(1)

	// Make request
	resp, err := http.Post(fmt.Sprintf("%s/gitlab/v1/restart/999/", s.server.URL), "application/json", nil)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify error response
	require.Equal(s.T(), http.StatusNotFound, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestRestartBackgroundMigration_DatabaseError() {
	// Expect authorization check
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Expect the query to return an error
	s.mockQuerier.ExpectQuery(`SELECT.*FROM.*batched_background_migrations.*WHERE.*id.*`).
		WithArgs(1).
		WillReturnError(fmt.Errorf("database connection error"))

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(1)

	// Make request
	resp, err := http.Post(fmt.Sprintf("%s/gitlab/v1/restart/1/", s.server.URL), "application/json", nil)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify error response
	require.Equal(s.T(), http.StatusInternalServerError, resp.StatusCode)
}

func (s *BackgroundMigrationsHandlerTestSuite) TestRestartBackgroundMigration_UpdateError() {
	// Prepare test data
	migration := &models.BackgroundMigration{
		ID:               1,
		Name:             "migration_1",
		Status:           models.BackgroundMigrationFailed,
		JobName:          "job_1",
		TargetTable:      "public.repositories",
		TargetColumn:     "id",
		BatchSize:        100,
		StartID:          1,
		EndID:            1000,
		BatchingStrategy: models.SerialKeySetBatchingBBMStrategy,
		TotalTupleCount:  sql.NullInt64{Int64: 500, Valid: true},
		ErrorCode:        models.InvalidTableBBMErrCode,
	}

	// Setup sqlmock rows for FindById
	findRows := sqlmock.NewRows([]string{
		"id",
		"name",
		"min_value",
		"max_value",
		"batch_size",
		"status",
		"job_signature_name",
		"table_name",
		"column_name",
		"failure_error_code",
		"batching_strategy",
		"total_tuple_count",
	})

	errVal, _ := migration.ErrorCode.Value()
	bStrategy, _ := migration.BatchingStrategy.Value()
	findRows.AddRow(
		migration.ID,
		migration.Name,
		migration.StartID,
		migration.EndID,
		migration.BatchSize,
		int(migration.Status),
		migration.JobName,
		migration.TargetTable,
		migration.TargetColumn,
		errVal,
		bStrategy,
		migration.TotalTupleCount,
	)

	// Expect authorization check
	s.mockAccessCtrl.EXPECT().
		Authorized(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx any, _ ...auth.Access) (any, error) {
			return ctx, nil
		})

	// Expect the FindById query
	s.mockQuerier.ExpectQuery(`SELECT.*FROM.*batched_background_migrations.*WHERE.*id = \$1`).
		WithArgs(1).
		WillReturnRows(findRows)

	// Expect the UpdateStatus query to fail
	s.mockQuerier.ExpectQuery(`UPDATE.*batched_background_migrations.*SET.*status.*failure_error_code.*WHERE.*id.*`).
		WithArgs(int(models.BackgroundMigrationActive), nil, 1, int(models.BackgroundMigrationRunning), int(models.BackgroundMigrationFinished)).
		WillReturnError(fmt.Errorf("database update error"))

	s.mockLB.EXPECT().Primary().Return(&datastore.DB{DB: s.mockPrimaryDB}).Times(2)

	// Make request
	resp, err := http.Post(fmt.Sprintf("%s/gitlab/v1/restart/1/", s.server.URL), "application/json", nil)
	require.NoError(s.T(), err)
	defer resp.Body.Close()

	// Verify error response
	require.Equal(s.T(), http.StatusInternalServerError, resp.StatusCode)
}

func TestBackgroundMigrationsHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(BackgroundMigrationsHandlerTestSuite))
}
