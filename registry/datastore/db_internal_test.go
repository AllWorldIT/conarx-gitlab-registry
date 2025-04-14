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
		primary:                         &DB{DB: primaryDB, DSN: &DSN{Host: "primary"}},
		resolver:                        stubResolver,
		minResolveReplicasInterval:      50 * time.Millisecond,
		replicaLivenessProbesInProgress: make(map[string]time.Time),
	}

	ctx := context.Background()

	// First call should succeed
	lb.throttledResolveReplicas(ctx)
	require.Equal(t, 1, resolverCallCount)

	// Second immediate call should be throttled
	lb.throttledResolveReplicas(ctx)
	require.Equal(t, 1, resolverCallCount) // Still 1, throttled

	// Wait for throttle interval to expire
	time.Sleep(60 * time.Millisecond)

	// Now should succeed again
	lb.throttledResolveReplicas(ctx)
	require.Equal(t, 2, resolverCallCount)
}

func TestDBLoadBalancer_LivenessProbeReplica(t *testing.T) {
	// Create mock databases
	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replicaMockDB, replicaMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer replicaMockDB.Close()

	// Create load balancer
	lb := &DBLoadBalancer{
		primary: &DB{DB: primaryMockDB, DSN: &DSN{Host: "primary"}},
		replicas: []*DB{
			{DB: replicaMockDB, DSN: &DSN{Host: "replica1"}},
		},
		replicaLivenessProbesInProgress: make(map[string]time.Time),
		minReplicaLivenessProbeInterval: 50 * time.Millisecond,
	}

	ctx := context.Background()
	replica := lb.replicas[0]

	// Test successful liveness probe
	replicaMock.ExpectPing().WillReturnError(nil)
	lb.livenessProbeReplica(ctx, replica)

	// Verify replica is still in the pool
	require.Len(t, lb.replicas, 1)
	require.Equal(t, replicaMockDB, lb.replicas[0].DB)

	// Test failed liveness probe
	replicaMock.ExpectPing().WillReturnError(errors.New("connection failed"))
	replicaMock.ExpectClose()

	lb.livenessProbeReplica(ctx, replica)

	// Verify replica was removed from the pool
	require.Empty(t, lb.replicas)

	// Verify mock expectations
	require.NoError(t, replicaMock.ExpectationsWereMet())
}

func TestDBLoadBalancer_LivenessProbeReplicaThrottling(t *testing.T) {
	lb := &DBLoadBalancer{
		replicaLivenessProbesInProgress: make(map[string]time.Time),
		minReplicaLivenessProbeInterval: 50 * time.Millisecond,
	}

	replicaAddr := "replica1:5432"

	// First probe should be allowed
	allowed := lb.canProbeReplicaLiveness(replicaAddr)
	require.True(t, allowed)

	// Immediate second probe should be throttled
	allowed = lb.canProbeReplicaLiveness(replicaAddr)
	require.False(t, allowed)

	// After waiting, next probe should be allowed
	time.Sleep(60 * time.Millisecond)

	// Complete the first probe
	lb.completeReplicaLivenessProbe(replicaAddr)

	// Now the next probe should be allowed
	allowed = lb.canProbeReplicaLiveness(replicaAddr)
	require.True(t, allowed)
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

func TestDBLoadBalancer_CanResolveReplicas(t *testing.T) {
	lb := &DBLoadBalancer{
		minResolveReplicasInterval: 50 * time.Millisecond,
	}

	// First call, should be allowed
	allowed := lb.canResolveReplicas()
	require.True(t, allowed)

	// The call marked operation as in-progress, complete it to simulate finished operation
	lb.completeReplicaResolution()

	// Second call too soon, should be throttled
	allowed = lb.canResolveReplicas()
	require.False(t, allowed)

	// After waiting, next resolution should be allowed
	time.Sleep(60 * time.Millisecond)
	allowed = lb.canResolveReplicas()
	require.True(t, allowed)

	// While in progress, additional resolutions should be rejected
	allowed = lb.canResolveReplicas()
	require.False(t, allowed)
}

func TestDBLoadBalancer_ProcessQueryError(t *testing.T) {
	// Create mock databases
	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replicaMockDB, replicaMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer replicaMockDB.Close()

	primaryDB := &DB{
		DB:  primaryMockDB,
		DSN: &DSN{Host: "primary", Port: 5432},
	}
	replicaDB := &DB{
		DB:  replicaMockDB,
		DSN: &DSN{Host: "replica1", Port: 5432},
	}

	// Create a load balancer
	lb := &DBLoadBalancer{
		primary:                         primaryDB,
		replicas:                        []*DB{replicaDB},
		replicaLivenessProbesInProgress: make(map[string]time.Time),
	}
	replicaDB.errorProcessor = lb

	// Test replica error processing
	replicaMock.ExpectPing().WillReturnError(errors.New("connection failed"))
	replicaMock.ExpectClose()
	lb.ProcessQueryError(context.Background(), replicaDB, "SELECT 1", &pgconn.PgError{Code: pgerrcode.ConnectionFailure})

	// Give time for the goroutine to execute
	time.Sleep(50 * time.Millisecond)

	// Verify the replica was removed due to failed liveness probe
	require.Empty(t, lb.replicas)

	// Verify all mock expectations were met
	require.NoError(t, replicaMock.ExpectationsWereMet())
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

func TestDB_QueryWrappers(t *testing.T) {
	// Create mock database and error processor that will be shared
	mockDB, sqlMock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	ctx := context.Background()

	t.Run("QueryContext", func(t *testing.T) {
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

	t.Run("ExecContext", func(t *testing.T) {
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
