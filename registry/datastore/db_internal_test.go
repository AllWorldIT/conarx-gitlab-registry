package datastore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
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

func TestDBLoadBalancer_Close(t *testing.T) {
	primaryDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)

	replicaDB1, replicaMock1, err := sqlmock.New()
	require.NoError(t, err)

	replicaDB2, replicaMock2, err := sqlmock.New()
	require.NoError(t, err)

	lb := &DBLoadBalancer{
		primary:  &DB{DB: primaryDB},
		replicas: []*DB{{DB: replicaDB1}, {DB: replicaDB2}},
	}

	// Ensure that all handlers are closed
	primaryMock.ExpectClose()
	replicaMock1.ExpectClose()
	replicaMock2.ExpectClose()

	require.NoError(t, lb.Close())
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock1.ExpectationsWereMet())
	require.NoError(t, replicaMock2.ExpectationsWereMet())
}

func TestDBLoadBalancer_Close_Error(t *testing.T) {
	primaryDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)

	replicaDB1, replicaMock1, err := sqlmock.New()
	require.NoError(t, err)

	replicaDB2, replicaMock2, err := sqlmock.New()
	require.NoError(t, err)

	lb := &DBLoadBalancer{
		primary: &DB{
			DB:  primaryDB,
			DSN: &DSN{Host: "primary"},
		},
		replicas: []*DB{
			{
				DB:  replicaDB1,
				DSN: &DSN{Host: "replica1"},
			},
			{
				DB:  replicaDB2,
				DSN: &DSN{Host: "replica2"},
			},
		},
	}

	// Set expectations for close operations
	primaryMock.ExpectClose().WillReturnError(fmt.Errorf("primary close error"))
	replicaMock1.ExpectClose().WillReturnError(fmt.Errorf("replica1 close error"))
	replicaMock2.ExpectClose()

	err = lb.Close()
	require.Error(t, err)

	var ee *multierror.Error
	require.ErrorAs(t, err, &ee)
	require.Len(t, ee.Errors, 2)
	require.Contains(t, ee.Errors[0].Error(), "primary close error")
	require.Contains(t, ee.Errors[1].Error(), "replica1 close error")

	// Ensure all expectations are met
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock1.ExpectationsWereMet())
	require.NoError(t, replicaMock2.ExpectationsWereMet())
}

func TestDBLoadBalancer_Primary(t *testing.T) {
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	lb := &DBLoadBalancer{
		primary: &DB{DB: primaryDB},
	}

	db := lb.Primary()
	require.NotNil(t, db)
	require.Equal(t, primaryDB, db.DB)
}

func TestDBLoadBalancer_Replica(t *testing.T) {
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	replicaDB1, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaDB1.Close()

	replicaDB2, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaDB2.Close()

	lb := &DBLoadBalancer{
		primary:  &DB{DB: primaryDB},
		replicas: []*DB{{DB: replicaDB1}, {DB: replicaDB2}},
	}

	// Test round-robin selection of replicas
	ctx := context.Background()
	db1 := lb.Replica(ctx)
	require.NotNil(t, db1)
	require.Equal(t, replicaDB1, db1.DB)

	db2 := lb.Replica(ctx)
	require.NotNil(t, db2)
	require.Equal(t, replicaDB2, db2.DB)

	db3 := lb.Replica(ctx)
	require.NotNil(t, db3)
	require.Equal(t, replicaDB1, db3.DB)
}

func TestDBLoadBalancer_NoReplicas(t *testing.T) {
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	lb := &DBLoadBalancer{
		primary: &DB{DB: primaryDB},
	}

	db := lb.Replica(context.Background())
	require.NotNil(t, db)
	require.Equal(t, primaryDB, db.DB)
}

func TestDBLoadBalancer_RecordLSN_NoStoreError(t *testing.T) {
	lb := &DBLoadBalancer{}
	err := lb.RecordLSN(context.Background(), &models.Repository{})
	require.EqualError(t, err, "LSN cache is not configured")
}

func TestDBLoadBalancer_UpToDateReplica_NoReplicas(t *testing.T) {
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	lb := &DBLoadBalancer{
		primary:  &DB{DB: primaryDB},
		lsnCache: NewNoOpRepositoryCache(),
	}

	db := lb.UpToDateReplica(context.Background(), &models.Repository{})
	require.NotNil(t, db)
	require.Equal(t, primaryDB, db.DB)
}

func TestDBLoadBalancer_UpToDateReplica_NoStore(t *testing.T) {
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	replicaDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaDB.Close()

	lb := &DBLoadBalancer{
		primary:  &DB{DB: primaryDB},
		replicas: []*DB{{DB: replicaDB}},
	}

	db := lb.UpToDateReplica(context.Background(), &models.Repository{})
	require.NotNil(t, db)
	require.Equal(t, primaryDB, db.DB)
}

func TestDBLoadBalancer_TypeOf(t *testing.T) {
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()
	replicaDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaDB.Close()
	unknownDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer unknownDB.Close()

	lb := &DBLoadBalancer{
		primary:  &DB{DB: primaryDB},
		replicas: []*DB{{DB: replicaDB}},
	}

	require.Equal(t, HostTypePrimary, lb.TypeOf(lb.primary))
	require.Equal(t, HostTypeReplica, lb.TypeOf(lb.replicas[0]))
	require.Equal(t, HostTypeUnknown, lb.TypeOf(&DB{DB: unknownDB}))
}

func TestDBLoadBalancer_ThrottledResolveReplicas(t *testing.T) {
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	// Create a counter for tracking resolver calls and a stub resolver that increments it
	resolverCallCount := 0
	stubResolver := &stubDNSResolver{
		lookupSRVFunc: func(_ context.Context) ([]*net.SRV, error) {
			resolverCallCount++
			return make([]*net.SRV, 0), nil
		},
	}

	// Create a load balancer with a custom interval for testing
	lb := &DBLoadBalancer{
		primary:  &DB{DB: primaryDB, DSN: &DSN{Host: "primary", Port: 5432}},
		resolver: stubResolver,
		throttledPoolResolver: NewThrottledPoolResolver(
			WithMinInterval(50*time.Millisecond),
			WithResolveFunction(func(ctx context.Context) error {
				_, err := stubResolver.LookupSRV(ctx)
				return err
			})),
	}

	ctx := context.Background()
	pgErr := &pgconn.PgError{Code: pgerrcode.ConnectionFailure}

	// First call should succeed
	lb.ProcessQueryError(ctx, lb.Primary(), "SELECT 1", pgErr)
	// Wait for the asynchronous operation to complete (should be "instantaneous" and stable as we're using a dummy mock)
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 1, resolverCallCount)

	// Second immediate call should be throttled
	lb.ProcessQueryError(ctx, lb.Primary(), "SELECT 1", pgErr)
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 1, resolverCallCount)

	// Wait for throttle interval to expire
	time.Sleep(60 * time.Millisecond)

	// Now should succeed again
	lb.ProcessQueryError(ctx, lb.Primary(), "SELECT 1", pgErr)
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 2, resolverCallCount)
}

func TestDBLoadBalancer_IsConnectivityError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "regular error",
			err:      errors.New("regular error"),
			expected: false,
		},
		{
			name:     "connection exception (08)",
			err:      &pgconn.PgError{Code: pgerrcode.ConnectionFailure}, // 08006
			expected: true,
		},
		{
			name:     "operator intervention (57)",
			err:      &pgconn.PgError{Code: pgerrcode.AdminShutdown}, // 57P01
			expected: true,
		},
		{
			name:     "insufficient resources (53)",
			err:      &pgconn.PgError{Code: pgerrcode.TooManyConnections}, // 53300
			expected: true,
		},
		{
			name:     "system error (58)",
			err:      &pgconn.PgError{Code: pgerrcode.SystemError}, // 58000
			expected: true,
		},
		{
			name:     "internal error (XX)",
			err:      &pgconn.PgError{Code: pgerrcode.InternalError}, // XX000
			expected: true,
		},
		{
			name:     "syntax error (42)",
			err:      &pgconn.PgError{Code: pgerrcode.SyntaxError}, // 42601
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isConnectivityError(tt.err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNewLivenessProber(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		p := NewLivenessProber()
		require.NotNil(t, p)
		require.Equal(t, minLivenessProbeInterval, p.minInterval)
		require.Equal(t, livenessProbeTimeout, p.timeout)
		require.Nil(t, p.onUnhealthy)
	})

	t.Run("custom options", func(t *testing.T) {
		var callbackCalled bool
		var callbackDB *DB

		p := NewLivenessProber(
			WithMinProbeInterval(123*time.Millisecond),
			WithProbeTimeout(456*time.Millisecond),
			WithUnhealthyCallback(func(_ context.Context, db *DB) {
				callbackCalled = true
				callbackDB = db
			}),
		)

		require.NotNil(t, p)
		require.Equal(t, 123*time.Millisecond, p.minInterval)
		require.Equal(t, 456*time.Millisecond, p.timeout)
		require.NotNil(t, p.onUnhealthy)

		// Test callback directly
		mockDB := &DB{DSN: &DSN{Host: "test-db"}}
		p.onUnhealthy(context.Background(), mockDB)
		require.True(t, callbackCalled, "Callback should be called")
		require.Equal(t, mockDB, callbackDB, "Callback should receive the input DB")
	})
}

func TestLivenessProber_BeginCheck(t *testing.T) {
	t.Run("same host", func(t *testing.T) {
		prober := NewLivenessProber(
			WithMinProbeInterval(50 * time.Millisecond),
		)
		hostAddr := "test-db:5432"

		require.True(t, prober.BeginCheck(hostAddr), "First probe should be allowed")
		require.False(t, prober.BeginCheck(hostAddr), "Second immediate probe should be throttled")
		time.Sleep(30 * time.Millisecond)
		require.False(t, prober.BeginCheck(hostAddr), "Probe should be throttled when within interval")
		time.Sleep(30 * time.Millisecond)
		require.True(t, prober.BeginCheck(hostAddr), "Probe should be allowed after interval expires")
	})

	t.Run("different hosts", func(t *testing.T) {
		prober := NewLivenessProber(
			WithMinProbeInterval(50 * time.Millisecond),
		)

		require.True(t, prober.BeginCheck("host1:5432"), "First host should be allowed")
		require.True(t, prober.BeginCheck("host2:5432"), "Second host should be allowed")
		require.False(t, prober.BeginCheck("host1:5432"), "First host should be throttled")
		require.False(t, prober.BeginCheck("host2:5432"), "Second host should be throttled")
	})
}

func TestLivenessProber_Probe(t *testing.T) {
	t.Run("successful", func(t *testing.T) {
		dbMock, sqlMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer dbMock.Close()

		db := &DB{
			DB:  dbMock,
			DSN: &DSN{Host: "test-db", Port: 5432},
		}

		// Create a prober with a test callback
		var callbackCalled bool
		prober := NewLivenessProber(
			WithUnhealthyCallback(func(_ context.Context, _ *DB) {
				callbackCalled = true
			}),
		)

		// Mock successful ping
		sqlMock.ExpectPing().WillReturnError(nil)
		prober.Probe(context.Background(), db)

		require.False(t, callbackCalled, "Callback should not be called for successful probe")
		require.NoError(t, sqlMock.ExpectationsWereMet())
	})

	t.Run("failed", func(t *testing.T) {
		dbMock, sqlMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer dbMock.Close()

		db := &DB{
			DB:  dbMock,
			DSN: &DSN{Host: "test-db", Port: 5432},
		}

		// Create a prober with a test callback
		var callbackCalled bool
		var callbackDB *DB
		prober := NewLivenessProber(
			WithUnhealthyCallback(func(_ context.Context, db *DB) {
				callbackCalled = true
				callbackDB = db
			}),
		)

		// Mock failed ping
		sqlMock.ExpectPing().WillReturnError(errors.New("connection failed"))
		prober.Probe(context.Background(), db)

		require.True(t, callbackCalled, "Callback should be called for failed probe")
		require.Equal(t, db, callbackDB, "Callback should receive the unhealthy DB")
		require.NoError(t, sqlMock.ExpectationsWereMet())
	})

	t.Run("timeout", func(t *testing.T) {
		dbMock, sqlMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer dbMock.Close()

		db := &DB{
			DB:  dbMock,
			DSN: &DSN{Host: "test-db", Port: 5432},
		}

		// Create a prober with a test callback
		var callbackCalled bool
		prober := NewLivenessProber(
			WithProbeTimeout(50*time.Millisecond),
			WithUnhealthyCallback(func(_ context.Context, _ *DB) {
				callbackCalled = true
			}),
		)

		// Mock ping that delays longer than the timeout
		sqlMock.ExpectPing().WillDelayFor(60 * time.Millisecond).WillReturnError(nil)
		prober.Probe(context.Background(), db)

		require.True(t, callbackCalled, "Callback should be called when probe times out")
		require.NoError(t, sqlMock.ExpectationsWereMet())
	})
}

func TestLivenessProber_EndCheck(t *testing.T) {
	prober := NewLivenessProber(
		WithMinProbeInterval(50 * time.Millisecond),
	)
	hostAddr := "test-db:5432"

	require.True(t, prober.BeginCheck(hostAddr), "First probe should be allowed")
	require.Contains(t, prober.inProgress, hostAddr, "Host should be in inProgress map after BeginCheck returns true")

	prober.EndCheck(hostAddr)
	require.NotContains(t, prober.inProgress, hostAddr, "Host should be removed from inProgress map after EndCheck")
}

// Implement any other methods required by the error interfaces

// stubDNSResolver implements the DNSResolver interface for testing
type stubDNSResolver struct {
	lookupSRVFunc  func(context.Context) ([]*net.SRV, error)
	lookupHostFunc func(context.Context, string) ([]string, error)
}

func (r *stubDNSResolver) LookupSRV(ctx context.Context) ([]*net.SRV, error) {
	if r.lookupSRVFunc != nil {
		return r.lookupSRVFunc(ctx)
	}
	return nil, errors.New("not implemented")
}

func (r *stubDNSResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	if r.lookupHostFunc != nil {
		return r.lookupHostFunc(ctx, host)
	}
	return nil, errors.New("not implemented")
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

func TestThrottledPoolResolver(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		r := NewThrottledPoolResolver()
		require.NotNil(t, r)
		require.Equal(t, minResolveReplicasInterval, r.minInterval)
		require.Nil(t, r.resolveFn)
	})

	t.Run("custom options", func(t *testing.T) {
		var resolveCalled bool
		r := NewThrottledPoolResolver(
			WithMinInterval(123*time.Millisecond),
			WithResolveFunction(func(_ context.Context) error {
				resolveCalled = true
				return nil
			}),
		)

		require.NotNil(t, r)
		require.Equal(t, 123*time.Millisecond, r.minInterval)
		require.NotNil(t, r.resolveFn)

		// Test resolve function
		err := r.resolveFn(context.Background())
		require.NoError(t, err)
		require.True(t, resolveCalled)
	})
}

func TestThrottledPoolResolver_BeginComplete(t *testing.T) {
	r := NewThrottledPoolResolver(
		WithMinInterval(50 * time.Millisecond),
	)

	// First call should be allowed
	require.True(t, r.Begin(), "First resolution should be allowed")

	// Verify inProgress is set
	require.True(t, r.inProgress, "inProgress should be true after Begin")
	require.Zero(t, r.lastComplete, "lastComplete should be zero time initially")

	// Second immediate call should be throttled due to inProgress
	require.False(t, r.Begin(), "Second call should be throttled while in progress")

	// Complete the resolution
	r.Complete()

	// Verify lastComplete is updated and inProgress is cleared
	require.False(t, r.inProgress, "inProgress should be false after Complete")
	require.NotZero(t, r.lastComplete, "lastComplete should be updated")

	// Immediate third call should be throttled due to timing
	require.False(t, r.Begin(), "Call should be throttled immediately after completion")

	// After waiting, next call should be allowed
	time.Sleep(60 * time.Millisecond)
	require.True(t, r.Begin(), "Call should be allowed after interval")
}

func TestThrottledPoolResolver_Resolve(t *testing.T) {
	t.Run("successful", func(t *testing.T) {
		var resolveCalled bool
		r := NewThrottledPoolResolver(
			WithMinInterval(50*time.Millisecond),
			WithResolveFunction(func(_ context.Context) error {
				resolveCalled = true
				return nil
			}),
		)

		// First resolution should succeed
		result := r.Resolve(context.Background())
		require.True(t, result, "Resolve should return true when resolution was performed")
		require.True(t, resolveCalled, "Resolve function should have been called")

		// Reset for next test
		resolveCalled = false

		// Immediate second resolution should be throttled
		result = r.Resolve(context.Background())
		require.False(t, result, "Immediate second resolution should be throttled")
		require.False(t, resolveCalled, "Resolve function should not have been called")

		// After waiting, third resolution should succeed
		resolveCalled = false
		time.Sleep(60 * time.Millisecond)

		result = r.Resolve(context.Background())
		require.True(t, result, "Resolution after waiting should succeed")
		require.True(t, resolveCalled, "Resolve function should have been called")
	})

	t.Run("error", func(t *testing.T) {
		expectedErr := errors.New("resolution error")
		r := NewThrottledPoolResolver(
			WithResolveFunction(func(_ context.Context) error {
				return expectedErr
			}),
		)

		// Resolution should proceed but return the error
		result := r.Resolve(context.Background())
		require.True(t, result, "Resolve should return true even when resolution function returns an error")

		// Verify inProgress was cleared despite the error
		require.False(t, r.inProgress, "inProgress should be cleared after error")
		require.NotEqual(t, time.Time{}, r.lastComplete, "lastComplete should be updated after error")
	})

	t.Run("concurrent", func(t *testing.T) {
		// Create a resolver with a resolve function that blocks
		resolveStarted := make(chan struct{})
		resolveFinished := make(chan struct{})
		resolveCount := 0

		r := NewThrottledPoolResolver(
			WithResolveFunction(func(_ context.Context) error {
				resolveCount++
				resolveStarted <- struct{}{}
				<-resolveFinished // Block until test signals completion
				return nil
			}),
		)

		// Start a goroutine that will attempt to resolve
		go func() {
			r.Resolve(context.Background())
		}()

		// Wait for resolution to start
		<-resolveStarted

		// Try another resolution while first one is in progress
		result := r.Resolve(context.Background())
		require.False(t, result, "Concurrent resolution should be throttled")

		// Allow first resolution to complete
		resolveFinished <- struct{}{}

		// Give time for goroutine to complete
		time.Sleep(10 * time.Millisecond)

		// Verify only one resolution occurred
		require.Equal(t, 1, resolveCount, "Only one resolution should have occurred")
	})
}

// MockQueryErrorProcessor records all arguments for verification
type MockQueryErrorProcessor struct {
	callCount int
	lastDB    *DB
	lastQuery string
	lastError error
}

func (m *MockQueryErrorProcessor) ProcessQueryError(_ context.Context, db *DB, query string, err error) {
	m.callCount++
	m.lastDB = db
	m.lastQuery = query
	m.lastError = err
}

func TestNewReplicaLagTracker(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		tracker := NewReplicaLagTracker()
		require.NotNil(t, tracker)
		require.NotNil(t, tracker.lagInfo, "lagInfo map should be initialized")
		require.Equal(t, defaultReplicaCheckInterval, tracker.checkInterval, "Should use default check interval")
	})

	t.Run("with custom lag check interval", func(t *testing.T) {
		customInterval := 5 * time.Minute
		tracker := NewReplicaLagTracker(
			WithLagCheckInterval(customInterval),
		)
		require.NotNil(t, tracker)
		require.Equal(t, customInterval, tracker.checkInterval, "Should use custom check interval")

		// Check with zero interval (should use default)
		zeroTracker := NewReplicaLagTracker(
			WithLagCheckInterval(0),
		)
		require.NotNil(t, zeroTracker)
		require.Equal(t, defaultReplicaCheckInterval, zeroTracker.checkInterval, "Should use default for zero interval")

		// Check with negative interval (should use default)
		negativeTracker := NewReplicaLagTracker(
			WithLagCheckInterval(-1 * time.Second),
		)
		require.NotNil(t, negativeTracker)
		require.Equal(t, defaultReplicaCheckInterval, negativeTracker.checkInterval, "Should use default for negative interval")
	})
}

func TestReplicaLagTracker_Get(t *testing.T) {
	tracker := NewReplicaLagTracker()
	ctx := context.Background()

	// Create a mock DB with an address
	mockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	db := &DB{
		DB: mockDB,
		DSN: &DSN{
			Host: "test-replica",
			Port: 5432,
		},
	}

	// Verify initial state - lagInfo map should be empty
	require.Empty(t, tracker.lagInfo, "lagInfo map should start empty")

	// Test Get when replica info doesn't exist
	info := tracker.Get(db.Address())
	require.Nil(t, info, "Should return nil for non-existent replica")

	// Add lag info for the replica
	lagTime := 5 * time.Second
	tracker.set(ctx, db, lagTime)

	// Verify internal state - lagInfo map should have one entry
	require.Len(t, tracker.lagInfo, 1, "lagInfo map should have one entry")
	require.Contains(t, tracker.lagInfo, db.Address(), "lagInfo map should contain the DB address")
	require.Equal(t, lagTime, tracker.lagInfo[db.Address()].TimeLag, "TimeLag should match what was set")

	// Now Get should return lag info
	info = tracker.Get(db.Address())
	require.NotNil(t, info, "Should return lag info after set")
	require.Equal(t, db.Address(), info.Address)
	require.Equal(t, lagTime, info.TimeLag)
	require.WithinDuration(t, time.Now(), info.LastChecked, 1*time.Second)
}

func TestReplicaLagTracker_Set(t *testing.T) {
	tracker := NewReplicaLagTracker()
	ctx := context.Background()

	// Create a mock DB with an address
	mockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	db := &DB{
		DB: mockDB,
		DSN: &DSN{
			Host: "test-replica",
			Port: 5432,
		},
	}

	// Verify initial state
	require.Empty(t, tracker.lagInfo, "lagInfo map should start empty")

	// Set initial lag info
	initialLag := 3 * time.Second
	tracker.set(ctx, db, initialLag)

	// Verify internal state after first Set
	require.Len(t, tracker.lagInfo, 1, "lagInfo map should have one entry")
	require.Contains(t, tracker.lagInfo, db.Address(), "lagInfo map should contain the DB address")
	require.Equal(t, initialLag, tracker.lagInfo[db.Address()].TimeLag, "TimeLag should match what was set")
	initialTime := tracker.lagInfo[db.Address()].LastChecked

	// Verify returned info matches internal state
	info := tracker.Get(db.Address())
	require.NotNil(t, info)
	require.Equal(t, initialLag, info.TimeLag)
	require.Equal(t, initialTime, info.LastChecked)

	// Update lag info
	updatedLag := 5 * time.Second
	time.Sleep(10 * time.Millisecond) // Ensure LastChecked time changes
	tracker.set(ctx, db, updatedLag)

	// Verify internal state after update
	require.Len(t, tracker.lagInfo, 1, "lagInfo map should still have one entry")
	require.Equal(t, updatedLag, tracker.lagInfo[db.Address()].TimeLag, "TimeLag should be updated")
	require.True(t, tracker.lagInfo[db.Address()].LastChecked.After(initialTime), "LastChecked should be updated")

	// Verify returned info reflects the update
	info = tracker.Get(db.Address())
	require.NotNil(t, info)
	require.Equal(t, updatedLag, info.TimeLag)
	require.True(t, info.LastChecked.After(initialTime), "LastChecked should be updated")
}

func TestReplicaLagTracker_CheckTimeLag(t *testing.T) {
	ctx := context.Background()

	// Create a mock DB with an address
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err, "Creating SQL mock should succeed")
	defer mockDB.Close()

	db := &DB{
		DB: mockDB,
		DSN: &DSN{
			Host: "test-replica",
			Port: 5432,
		},
	}

	t.Run("successful query", func(t *testing.T) {
		tracker := NewReplicaLagTracker()

		// Mock the lag query
		expectedLag := 2.5 // seconds
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillReturnRows(sqlmock.NewRows([]string{"lag"}).AddRow(expectedLag))

		// Check lag
		beforeCheck := time.Now()
		lag, err := tracker.CheckTimeLag(ctx, db)

		// Verify no error and lag is correct
		require.NoError(t, err, "CheckTimeLag should not return error on successful query")
		require.Equal(t, time.Duration(expectedLag*float64(time.Second)), lag, "Should return the correct lag duration")

		// Verify lag info was saved in internal state
		require.Len(t, tracker.lagInfo, 1, "lagInfo map should have one entry after successful check")
		require.Contains(t, tracker.lagInfo, db.Address(), "lagInfo map should contain the DB address")
		require.Equal(t, lag, tracker.lagInfo[db.Address()].TimeLag, "Internal TimeLag should match returned lag")
		require.GreaterOrEqual(t, tracker.lagInfo[db.Address()].LastChecked, beforeCheck,
			"LastChecked should be at or after the captured start time")

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("query error", func(t *testing.T) {
		tracker := NewReplicaLagTracker()

		// Mock a query error
		expectedErr := errors.New("query failed")
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillReturnError(expectedErr)

		// Check lag
		lag, err := tracker.CheckTimeLag(ctx, db)

		// Verify error is returned and zero lag
		require.Error(t, err, "CheckTimeLag should return error on query failure")
		require.ErrorContains(t, err, "query failed", "Error should contain the original error message")
		require.Zero(t, lag, "Should return zero lag on query error")

		// Verify no lag info was saved on error
		require.Empty(t, tracker.lagInfo, "lagInfo map should remain empty on error")

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("query timeout", func(t *testing.T) {
		tracker := NewReplicaLagTracker()

		// Mock a query that takes too long (exceeds the 100ms timeout)
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillDelayFor(150 * time.Millisecond).
			WillReturnRows(sqlmock.NewRows([]string{"lag"}).AddRow(1.5))

		// Check lag
		lag, err := tracker.CheckTimeLag(ctx, db)

		// Verify error is returned and zero lag on timeout
		require.Error(t, err, "CheckTimeLag should return error on timeout")
		// The error message could be either "context deadline exceeded" or "canceling query due to user request"
		// depending on how the database driver implements context cancellation
		require.Contains(t, err.Error(), "cancel", "Error should indicate cancellation or timeout")
		require.Zero(t, lag, "Should return zero lag on query timeout")

		// Verify no lag info was saved on timeout
		require.Empty(t, tracker.lagInfo, "lagInfo map should remain empty on timeout")

		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestReplicaLagTracker_Check(t *testing.T) {
	// Create a mock DB with an address
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	db := &DB{
		DB: mockDB,
		DSN: &DSN{
			Host: "test-replica",
			Port: 5432,
		},
	}
	ctx := context.Background()

	t.Run("successful check", func(t *testing.T) {
		tracker := NewReplicaLagTracker()

		// Mock the lag query
		expectedLag := 3.5 // seconds
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillReturnRows(sqlmock.NewRows([]string{"lag"}).AddRow(expectedLag))

		// Call Check
		beforeCheck := time.Now()
		err := tracker.Check(ctx, db)
		require.NoError(t, err, "Check should not return error on successful query")

		// Verify internal state changed
		require.Len(t, tracker.lagInfo, 1, "lagInfo map should have one entry after Check")
		require.Contains(t, tracker.lagInfo, db.Address(), "lagInfo map should contain the DB address")
		require.Equal(t, time.Duration(expectedLag*float64(time.Second)), tracker.lagInfo[db.Address()].TimeLag,
			"Internal TimeLag should match query result")
		require.GreaterOrEqual(t, tracker.lagInfo[db.Address()].LastChecked, beforeCheck,
			"LastChecked should be at or after the captured start time")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("query error", func(t *testing.T) {
		tracker := NewReplicaLagTracker()

		// Mock a query error
		expectedErr := errors.New("query failed")
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillReturnError(expectedErr)

		// Call Check
		err := tracker.Check(ctx, db)
		require.Error(t, err, "Check should return error on query failure")
		require.ErrorContains(t, err, "query failed", "Error should contain the original error message")

		// Verify that lag info was not set on error
		require.Empty(t, tracker.lagInfo, "lagInfo map should remain empty on error")

		require.NoError(t, mock.ExpectationsWereMet())
	})
}
