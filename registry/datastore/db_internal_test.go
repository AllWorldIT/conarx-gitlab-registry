package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestApplyOptions(t *testing.T) {
	defaultLogger := logrus.New()
	defaultLogger.SetOutput(io.Discard)

	l := logrus.NewEntry(logrus.New())
	poolConfig := &PoolConfig{
		MaxIdle:     1,
		MaxOpen:     2,
		MaxLifetime: 1 * time.Minute,
		MaxIdleTime: 10 * time.Minute,
	}

	tests := []struct {
		name           string
		opts           []Option
		wantLogger     *logrus.Entry
		wantPoolConfig *PoolConfig
	}{
		{
			name:           "empty",
			opts:           nil,
			wantLogger:     logrus.NewEntry(defaultLogger),
			wantPoolConfig: &PoolConfig{},
		},
		{
			name:           "with logger",
			opts:           []Option{WithLogger(l)},
			wantLogger:     l,
			wantPoolConfig: &PoolConfig{},
		},
		{
			name:           "with pool config",
			opts:           []Option{WithPoolConfig(poolConfig)},
			wantLogger:     logrus.NewEntry(defaultLogger),
			wantPoolConfig: poolConfig,
		},
		{
			name:           "combined",
			opts:           []Option{WithLogger(l), WithPoolConfig(poolConfig)},
			wantLogger:     l,
			wantPoolConfig: poolConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := applyOptions(tt.opts)
			require.Equal(t, tt.wantLogger.Logger.Out, got.logger.Logger.Out)
			require.Equal(t, tt.wantLogger.Logger.Level, got.logger.Logger.Level)
			require.Equal(t, tt.wantLogger.Logger.Formatter, got.logger.Logger.Formatter)
			require.Equal(t, tt.wantPoolConfig, got.pool)
		})
	}
}

func TestLexicographicallyNextPath(t *testing.T) {
	tests := []struct {
		path             string
		expectedNextPath string
	}{
		{
			path:             "gitlab.com",
			expectedNextPath: "gitlab.con",
		},
		{
			path:             "gitlab.com.",
			expectedNextPath: "gitlab.com/",
		},
		{
			path:             "",
			expectedNextPath: "a",
		},
		{
			path:             "zzzz/zzzz",
			expectedNextPath: "zzzz0zzzz",
		},
		{
			path:             "zzz",
			expectedNextPath: "zzza",
		},
		{
			path:             "zzzZ",
			expectedNextPath: "zzz[",
		},
		{
			path:             "gitlab-com/gl-infra/k8s-workloads",
			expectedNextPath: "gitlab-com/gl-infra/k8s-workloadt",
		},
	}

	for _, test := range tests {
		require.Equal(t, test.expectedNextPath, lexicographicallyNextPath(test.path))
	}
}

func TestLexicographicallyBeforePath(t *testing.T) {
	tests := []struct {
		path               string
		expectedBeforePath string
	}{
		{
			path:               "gitlab.con",
			expectedBeforePath: "gitlab.com",
		},
		{
			path:               "gitlab.com/",
			expectedBeforePath: "gitlab.com.",
		},
		{
			path:               "",
			expectedBeforePath: "z",
		},
		{
			path:               "aaa",
			expectedBeforePath: "aaz",
		},
		{
			path:               "aaaB",
			expectedBeforePath: "aaaA",
		},
		{
			path:               "aaa0aaa",
			expectedBeforePath: "aaa/aaa",
		},
		{
			path:               "zzz",
			expectedBeforePath: "zzy",
		},
		{
			path:               "zzz[",
			expectedBeforePath: "zzzZ",
		},
		{
			path:               "gitlab-com/gl-infra/k8s-workloadt",
			expectedBeforePath: "gitlab-com/gl-infra/k8s-workloads",
		},
	}

	for _, test := range tests {
		require.Equal(t, test.expectedBeforePath, lexicographicallyBeforePath(test.path))
	}
}

func TestIsInRecovery(t *testing.T) {
	ctx := context.Background()
	primaryDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	db := &DB{DB: primaryDB}

	// case 1 database is in recovery mode
	mock.ExpectQuery("SELECT pg_is_in_recovery()").WillReturnRows(
		sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true),
	)

	inRecovery, err := IsInRecovery(ctx, db)
	require.NoError(t, err)
	require.True(t, inRecovery)

	// case 2 database is not in recovery mode
	mock.ExpectQuery("SELECT pg_is_in_recovery()").WillReturnRows(
		sqlmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false),
	)

	inRecovery, err = IsInRecovery(ctx, db)
	require.NoError(t, err)
	require.False(t, inRecovery)

	// case 3 there was a database error (query failure)
	mock.ExpectQuery("SELECT pg_is_in_recovery()").WillReturnError(fmt.Errorf("query failed"))

	inRecovery, err = IsInRecovery(ctx, db)
	require.Error(t, err)
	require.False(t, inRecovery)

	// all expectations were met
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestIsDBSupported(t *testing.T) {
	ctx := context.Background()
	primaryDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	db := &DB{DB: primaryDB}

	// case 1: database version is supported (PostgreSQL 15.0)
	mock.ExpectQuery("SELECT current_setting\\('server_version_num'\\)::integer >= 150000").WillReturnRows(
		sqlmock.NewRows([]string{"?column?"}).AddRow(true),
	)

	isSupported, err := IsDBSupported(ctx, db)
	require.NoError(t, err)
	require.True(t, isSupported)

	// case 2: database version is not supported (below PostgreSQL 15.0)
	mock.ExpectQuery("SELECT current_setting\\('server_version_num'\\)::integer >= 150000").WillReturnRows(
		sqlmock.NewRows([]string{"?column?"}).AddRow(false),
	)

	isSupported, err = IsDBSupported(ctx, db)
	require.NoError(t, err)
	require.False(t, isSupported)

	// case 3: there was a database error (query failure)
	mock.ExpectQuery("SELECT current_setting\\('server_version_num'\\)::integer >= 150000").WillReturnError(fmt.Errorf("query failed"))

	isSupported, err = IsDBSupported(ctx, db)
	require.Error(t, err)
	require.False(t, isSupported)

	// all expectations were met
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestIsArchivingEnabled(t *testing.T) {
	ctx := context.Background()
	primaryDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	db := &DB{DB: primaryDB}

	// case 1: database archive_mode is off
	mock.ExpectQuery("SELECT current_setting\\('archive_mode'\\)").WillReturnRows(
		sqlmock.NewRows([]string{"?column?"}).AddRow("off"),
	)

	isEnabled, err := IsArchivingEnabled(ctx, db.DB)
	require.NoError(t, err)
	require.False(t, isEnabled)

	// case 2: database archive_mode is on
	mock.ExpectQuery("SELECT current_setting\\('archive_mode'\\)").WillReturnRows(
		sqlmock.NewRows([]string{"?column?"}).AddRow("on"),
	)

	isEnabled, err = IsArchivingEnabled(ctx, db.DB)
	require.NoError(t, err)
	require.True(t, isEnabled)

	// case 3: database archive_mode is always
	mock.ExpectQuery("SELECT current_setting\\('archive_mode'\\)").WillReturnRows(
		sqlmock.NewRows([]string{"?column?"}).AddRow("always"),
	)

	isEnabled, err = IsArchivingEnabled(ctx, db.DB)
	require.NoError(t, err)
	require.True(t, isEnabled)

	// case 4: error
	mock.ExpectQuery("SELECT current_setting\\('archive_mode'\\)").WillReturnError(fmt.Errorf("query failed"))

	isEnabled, err = IsArchivingEnabled(ctx, db.DB)
	require.Error(t, err)
	require.False(t, isEnabled)

	// all expectations were met
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestVersionToServerVersionNum(t *testing.T) {
	tests := []struct {
		name        string
		version     string
		expected    int
		expectError bool
	}{
		{
			name:        "valid version 15.0",
			version:     "15.0",
			expected:    150000,
			expectError: false,
		},
		{
			name:        "valid version 14.5",
			version:     "14.5",
			expected:    140005,
			expectError: false,
		},
		{
			name:        "valid version 16.2",
			version:     "16.2",
			expected:    160002,
			expectError: false,
		},
		{
			name:        "invalid format - too many parts",
			version:     "15.0.1",
			expected:    0,
			expectError: true,
		},
		{
			name:        "invalid format - single number",
			version:     "15",
			expected:    0,
			expectError: true,
		},
		{
			name:        "invalid major version",
			version:     "abc.5",
			expected:    0,
			expectError: true,
		},
		{
			name:        "invalid minor version",
			version:     "15.xyz",
			expected:    0,
			expectError: true,
		},
		{
			name:        "empty version",
			version:     "",
			expected:    0,
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tt *testing.T) {
			result, err := versionToServerVersionNum(tc.version)

			if tc.expectError {
				require.Error(tt, err)
				require.Equal(tt, 0, result)
			} else {
				require.NoError(tt, err)
				require.Equal(tt, tc.expected, result)
			}
		})
	}
}

// Helper function to create test DB connections
func createTestDB(t *testing.T, host string) (*sql.DB, *DB) {
	mockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { mockDB.Close() })

	db := &DB{
		DB: mockDB,
		DSN: &DSN{
			Host: host,
			Port: 5432,
		},
	}
	return mockDB, db
}

func TestDB_QueryContext(t *testing.T) {
	// Create mock database
	mockDB, sqlMock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	ctx := context.Background()

	t.Run("successful query", func(t *testing.T) {
		// Create a DB with the mock processor
		mockProcessor := &MockQueryErrorProcessor{}
		db := &DB{DB: mockDB, errorProcessor: mockProcessor}

		// Set up query expectation with success
		query := "SELECT 1"
		rows := sqlmock.NewRows([]string{"id"}).AddRow(1)
		sqlMock.ExpectQuery(query).WillReturnRows(rows)

		// Execute query
		result, err := db.QueryContext(ctx, query)
		require.NoError(t, err)
		defer result.Close()

		// Verify result
		require.True(t, result.Next())
		var val int
		require.NoError(t, result.Scan(&val))
		require.Equal(t, 1, val)

		// Verify processor was not called
		require.Equal(t, 0, mockProcessor.callCount)
		require.NoError(t, sqlMock.ExpectationsWereMet())
	})

	t.Run("query with error", func(t *testing.T) {
		// Create a DB with the mock processor
		mockProcessor := &MockQueryErrorProcessor{}
		db := &DB{DB: mockDB, errorProcessor: mockProcessor}

		// Set up query expectation with error
		query := "SELECT 1"
		expectedError := errors.New("connection failure")
		sqlMock.ExpectQuery(query).WillReturnError(expectedError)

		// Execute query
		_, err := db.QueryContext(ctx, query)

		// Verify error is returned
		require.Error(t, err)
		require.Equal(t, expectedError, err)

		// Verify processor was called with correct arguments
		require.Equal(t, 1, mockProcessor.callCount)
		require.Equal(t, db, mockProcessor.lastDB)
		require.Equal(t, query, mockProcessor.lastQuery)
		require.Equal(t, expectedError, mockProcessor.lastError)
		require.NoError(t, sqlMock.ExpectationsWereMet())
	})

	t.Run("transaction", func(t *testing.T) {
		// Create a DB and mock processor
		mockProcessor := &MockQueryErrorProcessor{}
		db := &DB{DB: mockDB, errorProcessor: mockProcessor}

		// Set up transaction
		sqlMock.ExpectBegin()
		tx, err := db.Begin()
		require.NoError(t, err)

		// Set up query expectation with error
		query := "SELECT 1"
		expectedError := errors.New("transaction query error")
		sqlMock.ExpectQuery(query).WillReturnError(expectedError)

		// Execute query through transaction
		_, err = tx.QueryContext(ctx, query)

		// Verify error is returned
		require.Error(t, err)
		require.Equal(t, expectedError, err)

		// Verify processor was called with correct arguments
		require.Equal(t, 1, mockProcessor.callCount)
		require.Equal(t, db, mockProcessor.lastDB)
		require.Equal(t, query, mockProcessor.lastQuery)
		require.Equal(t, expectedError, mockProcessor.lastError)

		require.NoError(t, sqlMock.ExpectationsWereMet())
	})
}

func TestDB_ExecContext(t *testing.T) {
	// Create mock database
	mockDB, sqlMock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	ctx := context.Background()

	t.Run("successful exec", func(t *testing.T) {
		// Create a DB with the mock processor
		mockProcessor := &MockQueryErrorProcessor{}
		db := &DB{DB: mockDB, errorProcessor: mockProcessor}

		// Set up exec expectation with success
		query := "UPDATE repositories SET name = 'test'"
		sqlMock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(1, 1))

		// Execute exec
		result, err := db.ExecContext(ctx, query)
		require.NoError(t, err)

		// Verify result
		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		// Verify processor was not called
		require.Equal(t, 0, mockProcessor.callCount)
		require.NoError(t, sqlMock.ExpectationsWereMet())
	})

	t.Run("exec with error", func(t *testing.T) {
		// Create a DB with the mock processor
		mockProcessor := &MockQueryErrorProcessor{}
		db := &DB{DB: mockDB, errorProcessor: mockProcessor}

		// Set up exec expectation with error
		query := "UPDATE repositories SET name = 'test'"
		expectedError := errors.New("connection failure")
		sqlMock.ExpectExec(query).WillReturnError(expectedError)

		// Execute exec
		_, err := db.ExecContext(ctx, query)

		// Verify error is returned
		require.Error(t, err)
		require.Equal(t, expectedError, err)

		// Verify processor was called with correct arguments
		require.Equal(t, 1, mockProcessor.callCount)
		require.Equal(t, db, mockProcessor.lastDB)
		require.Equal(t, query, mockProcessor.lastQuery)
		require.Equal(t, expectedError, mockProcessor.lastError)
		require.NoError(t, sqlMock.ExpectationsWereMet())
	})

	t.Run("transaction", func(t *testing.T) {
		// Create a DB and mock processor
		mockProcessor := &MockQueryErrorProcessor{}
		db := &DB{DB: mockDB, errorProcessor: mockProcessor}

		// Set up transaction
		sqlMock.ExpectBegin()
		tx, err := db.Begin()
		require.NoError(t, err)

		// Set up exec expectation with error
		query := "UPDATE repositories SET name = 'test'"
		expectedError := errors.New("transaction exec error")
		sqlMock.ExpectExec(query).WillReturnError(expectedError)

		// Execute exec through transaction
		_, err = tx.ExecContext(ctx, query)

		// Verify error is returned
		require.Error(t, err)
		require.Equal(t, expectedError, err)

		// Verify processor was called with correct arguments
		require.Equal(t, 1, mockProcessor.callCount)
		require.Equal(t, db, mockProcessor.lastDB)
		require.Equal(t, query, mockProcessor.lastQuery)
		require.Equal(t, expectedError, mockProcessor.lastError)

		require.NoError(t, sqlMock.ExpectationsWereMet())
	})
}

func TestDB_QueryRowContext(t *testing.T) {
	// Create mock database
	mockDB, sqlMock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	ctx := context.Background()

	t.Run("successful query", func(t *testing.T) {
		// Create a DB with the mock processor
		mockProcessor := &MockQueryErrorProcessor{}
		db := &DB{DB: mockDB, errorProcessor: mockProcessor}

		// Set up query expectation with successful result
		query := "SELECT 1"
		sqlMock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

		// Execute query
		var id int
		err := db.QueryRowContext(ctx, query).Scan(&id)

		// Verify no error is returned
		require.NoError(t, err)
		require.Equal(t, 1, id)

		// Verify processor was not called
		require.Equal(t, 0, mockProcessor.callCount)
		require.NoError(t, sqlMock.ExpectationsWereMet())
	})

	t.Run("query with error", func(t *testing.T) {
		// Create a DB with the mock processor
		mockProcessor := &MockQueryErrorProcessor{}
		db := &DB{DB: mockDB, errorProcessor: mockProcessor}

		// Set up query expectation with error result
		query := "SELECT 1"
		expectedError := errors.New("no rows in result set")
		sqlMock.ExpectQuery(query).WillReturnError(expectedError)

		// Execute query
		var id int
		err := db.QueryRowContext(ctx, query).Scan(&id)

		// Verify error is returned
		require.Error(t, err)
		require.Equal(t, expectedError, err)

		// Verify processor was called with correct arguments
		require.Equal(t, 1, mockProcessor.callCount)
		require.Equal(t, db, mockProcessor.lastDB)
		require.Equal(t, query, mockProcessor.lastQuery)
		require.Equal(t, expectedError, mockProcessor.lastError)
		require.NoError(t, sqlMock.ExpectationsWereMet())
	})

	t.Run("transaction", func(t *testing.T) {
		// Create a DB and start a transaction
		mockProcessor := &MockQueryErrorProcessor{}
		db := &DB{DB: mockDB, errorProcessor: mockProcessor}

		// Mock transaction
		sqlMock.ExpectBegin()
		tx, err := db.Begin()
		require.NoError(t, err)

		// Set up query expectation with error
		query := "SELECT 1"
		expectedError := errors.New("transaction error")
		sqlMock.ExpectQuery(query).WillReturnError(expectedError)

		// Execute query through transaction
		var id int
		err = tx.QueryRowContext(ctx, query).Scan(&id)

		// Verify error is returned
		require.Error(t, err)
		require.Equal(t, expectedError, err)

		// Verify processor was called with correct arguments
		require.Equal(t, 1, mockProcessor.callCount)
		require.Equal(t, db, mockProcessor.lastDB)
		require.Equal(t, query, mockProcessor.lastQuery)
		require.Equal(t, expectedError, mockProcessor.lastError)

		require.NoError(t, sqlMock.ExpectationsWereMet())
	})
}
