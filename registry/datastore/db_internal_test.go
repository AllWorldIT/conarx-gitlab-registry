package datastore

import (
	"context"
	"database/sql"
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

// TestDBLoadBalancer_Replica tests the replica selection logic of DBLoadBalancer in various scenarios including no
// replicas, some/all quarantined replicas, and round-robin selection.
func TestDBLoadBalancer_Replica(t *testing.T) {
	ctx := context.Background()

	t.Run("no replicas falls back to primary", func(t *testing.T) {
		// Create primary DB
		primaryMockDB, primaryDB := createTestDB(t, "primary")

		// Create load balancer with no replicas
		lb := &DBLoadBalancer{
			primary:  primaryDB,
			replicas: make([]*DB, 0),
		}

		// Should return primary when no replicas available
		db := lb.Replica(ctx)
		require.Equal(t, primaryMockDB, db.DB)
	})

	t.Run("round-robin selection with multiple replicas", func(t *testing.T) {
		// Create primary and replica DBs
		_, primaryDB := createTestDB(t, "primary")
		replica1MockDB, replica1 := createTestDB(t, "replica1")
		replica2MockDB, replica2 := createTestDB(t, "replica2")
		replica3MockDB, replica3 := createTestDB(t, "replica3")

		// Create load balancer with multiple replicas
		lb := &DBLoadBalancer{
			primary:  primaryDB,
			replicas: []*DB{replica1, replica2, replica3},
		}

		// First call should return first replica
		db1 := lb.Replica(ctx)
		require.Equal(t, replica1MockDB, db1.DB)

		// Second call should return second replica
		db2 := lb.Replica(ctx)
		require.Equal(t, replica2MockDB, db2.DB)

		// Third call should return third replica
		db3 := lb.Replica(ctx)
		require.Equal(t, replica3MockDB, db3.DB)

		// Fourth call should wrap around to first replica
		db4 := lb.Replica(ctx)
		require.Equal(t, replica1MockDB, db4.DB)
	})

	t.Run("skips quarantined replicas", func(t *testing.T) {
		// Create primary and replica DBs
		_, primaryDB := createTestDB(t, "primary")
		replica1MockDB, replica1 := createTestDB(t, "replica1")
		_, replica2 := createTestDB(t, "replica2") // we'll quarantine this one
		replica3MockDB, replica3 := createTestDB(t, "replica3")

		// Create mock lag tracker
		tracker := newMockLagTracker()
		now := time.Now()

		// Quarantine the second replica
		tracker.lagInfo[replica2.Address()] = &ReplicaLagInfo{
			Address:       replica2.Address(),
			LastChecked:   now,
			TimeLag:       MaxReplicaLagTime * 2,
			BytesLag:      int64(MaxReplicaLagBytes * 2),
			Quarantined:   true,
			QuarantinedAt: now,
		}

		// Create load balancer with tracker
		lb := &DBLoadBalancer{
			primary:    primaryDB,
			replicas:   []*DB{replica1, replica2, replica3},
			lagTracker: tracker,
		}

		// First call should return first replica
		db1 := lb.Replica(ctx)
		require.Equal(t, replica1MockDB, db1.DB)

		// Second call should skip quarantined replica2 and return replica3
		db2 := lb.Replica(ctx)
		require.Equal(t, replica3MockDB, db2.DB)

		// Third call should wrap around to replica1
		db3 := lb.Replica(ctx)
		require.Equal(t, replica1MockDB, db3.DB)
	})

	t.Run("falls back to primary when all replicas quarantined", func(t *testing.T) {
		// Create primary and replica DBs
		primaryMockDB, primaryDB := createTestDB(t, "primary")
		_, replica1 := createTestDB(t, "replica1")
		_, replica2 := createTestDB(t, "replica2")

		// Create mock lag tracker and quarantine all replicas
		tracker := newMockLagTracker()
		now := time.Now()

		// Quarantine both replicas
		tracker.lagInfo[replica1.Address()] = &ReplicaLagInfo{
			Address:       replica1.Address(),
			LastChecked:   now,
			TimeLag:       MaxReplicaLagTime * 2,
			BytesLag:      int64(MaxReplicaLagBytes * 2),
			Quarantined:   true,
			QuarantinedAt: now,
		}
		tracker.lagInfo[replica2.Address()] = &ReplicaLagInfo{
			Address:       replica2.Address(),
			LastChecked:   now,
			TimeLag:       MaxReplicaLagTime * 2,
			BytesLag:      int64(MaxReplicaLagBytes * 2),
			Quarantined:   true,
			QuarantinedAt: now,
		}

		// Create load balancer with tracker
		lb := &DBLoadBalancer{
			primary:    primaryDB,
			replicas:   []*DB{replica1, replica2},
			lagTracker: tracker,
		}

		// Should fall back to primary when all replicas are quarantined
		db := lb.Replica(ctx)
		require.Equal(t, primaryMockDB, db.DB)
	})

	t.Run("index updates correctly with mixed quarantined replicas", func(t *testing.T) {
		// Create primary and replica DBs
		_, primaryDB := createTestDB(t, "primary")
		replica1MockDB, replica1 := createTestDB(t, "replica1")
		replica2MockDB, replica2 := createTestDB(t, "replica2") // will be quarantined
		replica3MockDB, replica3 := createTestDB(t, "replica3")

		// Create mock lag tracker
		tracker := newMockLagTracker()
		now := time.Now()

		// Quarantine only the second replica
		tracker.lagInfo[replica2.Address()] = &ReplicaLagInfo{
			Address:       replica2.Address(),
			LastChecked:   now,
			TimeLag:       MaxReplicaLagTime * 2,
			BytesLag:      int64(MaxReplicaLagBytes * 2),
			Quarantined:   true,
			QuarantinedAt: now,
		}

		// Create load balancer with tracker
		lb := &DBLoadBalancer{
			primary:    primaryDB,
			replicas:   []*DB{replica1, replica2, replica3},
			lagTracker: tracker,
		}

		// First call should return replica1
		db1 := lb.Replica(ctx)
		require.Equal(t, replica1MockDB, db1.DB)

		// Second call should skip quarantined replica2 and return replica3
		db2 := lb.Replica(ctx)
		require.Equal(t, replica3MockDB, db2.DB)

		// Third call should return replica1 again (round-robin)
		db3 := lb.Replica(ctx)
		require.Equal(t, replica1MockDB, db3.DB)

		// The index should update based on total replicas (not just available ones)
		// Start: index=0, Calls: R1->index=1, R3->index=2, R1->index=0
		require.Equal(t, 0, lb.replicaIndex, "Index should wrap around to 0")

		// Now reintegrate replica2
		tracker.lagInfo[replica2.Address()].Quarantined = false
		tracker.lagInfo[replica2.Address()].QuarantinedAt = time.Time{}

		// Fourth call should return replica1 (index=0)
		db4 := lb.Replica(ctx)
		require.Equal(t, replica1MockDB, db4.DB)

		// Fifth call should return replica2 which is now available (index=1)
		db5 := lb.Replica(ctx)
		require.Equal(t, replica2MockDB, db5.DB)

		// Sixth call should return replica3 (index=2)
		db6 := lb.Replica(ctx)
		require.Equal(t, replica3MockDB, db6.DB)
	})
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

		// Allowed first resolution to complete
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
	tracker.set(ctx, db, lagTime, 0)

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

// TestReplicaLagTracker_Quarantine tests different lag scenarios to ensure replicas are correctly quarantined and
// reintegrated based on the quarantine logic.
func TestReplicaLagTracker_Quarantine(t *testing.T) {
	ctx := context.Background()

	// Helper function to create a test DB
	createTestDB := func(t *testing.T, host string) *DB {
		mockDB, _, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() { mockDB.Close() })

		return &DB{
			DB: mockDB,
			DSN: &DSN{
				Host: host,
				Port: 5432,
			},
		}
	}

	t.Run("no quarantine with normal lag values", func(t *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(t, "replica-normal")

		// Set lag values below both thresholds
		normalTimeLag := MaxReplicaLagTime / 2
		normalBytesLag := int64(MaxReplicaLagBytes / 2)

		tracker.set(ctx, db, normalTimeLag, normalBytesLag)

		info := tracker.Get(db.Address())
		require.NotNil(t, info)
		require.False(t, info.Quarantined, "Replica shouldn't be quarantined when both lag values are below thresholds")
		require.Zero(t, info.QuarantinedAt, "QuarantinedAt should be zero")
	})

	t.Run("no quarantine with only time lag exceeding threshold", func(t *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(t, "replica-high-time")

		// Set time lag above threshold but bytes lag below
		highTimeLag := MaxReplicaLagTime * 2
		normalBytesLag := int64(MaxReplicaLagBytes / 2)

		tracker.set(ctx, db, highTimeLag, normalBytesLag)

		info := tracker.Get(db.Address())
		require.NotNil(t, info)
		require.False(t, info.Quarantined, "Replica shouldn't be quarantined when only time lag exceeds threshold")
		require.Zero(t, info.QuarantinedAt, "QuarantinedAt should be zero")
	})

	t.Run("no quarantine with only bytes lag exceeding threshold", func(t *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(t, "replica-high-bytes")

		// Set bytes lag above threshold but time lag below
		normalTimeLag := MaxReplicaLagTime / 2
		highBytesLag := int64(MaxReplicaLagBytes * 2)

		tracker.set(ctx, db, normalTimeLag, highBytesLag)

		info := tracker.Get(db.Address())
		require.NotNil(t, info)
		require.False(t, info.Quarantined, "Replica shouldn't be quarantined when only bytes lag exceeds threshold")
		require.Zero(t, info.QuarantinedAt, "QuarantinedAt should be zero")
	})

	t.Run("quarantine when both lag values exceed thresholds", func(t *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(t, "replica-high-both")

		// Set both lags above thresholds
		highTimeLag := MaxReplicaLagTime * 2
		highBytesLag := int64(MaxReplicaLagBytes * 2)

		tracker.set(ctx, db, highTimeLag, highBytesLag)

		info := tracker.Get(db.Address())
		require.NotNil(t, info)
		require.True(t, info.Quarantined, "Replica should be quarantined when both lag values exceed thresholds")
		require.NotZero(t, info.QuarantinedAt, "QuarantinedAt should be set")
	})

	t.Run("reintegration when time lag drops below threshold", func(t *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(t, "replica-reintegrate-time")

		// First quarantine the replica
		highTimeLag := MaxReplicaLagTime * 2
		highBytesLag := int64(MaxReplicaLagBytes * 2)
		tracker.set(ctx, db, highTimeLag, highBytesLag)

		info := tracker.Get(db.Address())
		require.True(t, info.Quarantined, "Replica should be quarantined initially")

		// Now lower time lag below threshold but keep bytes lag high
		normalTimeLag := MaxReplicaLagTime / 2
		tracker.set(ctx, db, normalTimeLag, highBytesLag)

		info = tracker.Get(db.Address())
		require.NotNil(t, info)
		require.False(t, info.Quarantined, "Replica should be reintegrated when time lag drops below threshold")
		require.Zero(t, info.QuarantinedAt, "QuarantinedAt should be reset")
	})

	t.Run("reintegration when bytes lag drops below threshold", func(t *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(t, "replica-reintegrate-bytes")

		// First quarantine the replica
		highTimeLag := MaxReplicaLagTime * 2
		highBytesLag := int64(MaxReplicaLagBytes * 2)
		tracker.set(ctx, db, highTimeLag, highBytesLag)

		info := tracker.Get(db.Address())
		require.True(t, info.Quarantined, "Replica should be quarantined initially")

		// Now lower bytes lag below threshold but keep time lag high
		normalBytesLag := int64(MaxReplicaLagBytes / 2)
		tracker.set(ctx, db, highTimeLag, normalBytesLag)

		info = tracker.Get(db.Address())
		require.NotNil(t, info)
		require.False(t, info.Quarantined, "Replica should be reintegrated when bytes lag drops below threshold")
		require.Zero(t, info.QuarantinedAt, "QuarantinedAt should be reset")
	})

	t.Run("stays quarantined when both lag values remain high", func(t *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(t, "replica-stay-quarantined")

		// First quarantine the replica
		highTimeLag := MaxReplicaLagTime * 2
		highBytesLag := int64(MaxReplicaLagBytes * 2)
		tracker.set(ctx, db, highTimeLag, highBytesLag)

		info := tracker.Get(db.Address())
		require.True(t, info.Quarantined, "Replica should be quarantined initially")
		initialQuarantinedAt := info.QuarantinedAt

		// Update with still high lag values
		newHighTimeLag := MaxReplicaLagTime * 3 / 2          // 1.5x
		newHighBytesLag := int64(MaxReplicaLagBytes * 3 / 2) // 1.5x
		tracker.set(ctx, db, newHighTimeLag, newHighBytesLag)

		info = tracker.Get(db.Address())
		require.NotNil(t, info)
		require.True(t, info.Quarantined, "Replica should stay quarantined when both lag values remain high")
		require.Equal(t, initialQuarantinedAt, info.QuarantinedAt, "QuarantinedAt should not change")
	})
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
	tracker.set(ctx, db, initialLag, 0)

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
	tracker.set(ctx, db, updatedLag, 0)

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
		expectedLag := 3.5 // seconds
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillReturnRows(sqlmock.NewRows([]string{"lag"}).AddRow(expectedLag))

		// Check lag
		lag, err := tracker.CheckTimeLag(ctx, db)

		// Verify no error and lag is correct
		require.NoError(t, err, "CheckTimeLag should not return error on successful query")
		require.Equal(t, time.Duration(expectedLag*float64(time.Second)), lag, "Should return the correct lag duration")
		require.NoError(t, mock.ExpectationsWereMet(), "All SQL expectations should be met")
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
		require.ErrorContains(t, err, expectedErr.Error(), "Error should contain the original error message")
		require.Zero(t, lag, "Should return zero lag on query error")

		require.NoError(t, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})

	t.Run("query timeout", func(t *testing.T) {
		tracker := NewReplicaLagTracker()

		// Mock a query that takes too long (exceeds the replicaLagCheckTimeout timeout)
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillDelayFor(replicaLagCheckTimeout + 50*time.Millisecond).
			WillReturnRows(sqlmock.NewRows([]string{"lag"}).AddRow(1.5))

		// Check lag
		lag, err := tracker.CheckTimeLag(ctx, db)

		// Verify error is returned and zero lag on timeout
		require.Error(t, err, "CheckTimeLag should return error on timeout")
		require.ErrorContains(t, err, "canceling query due to user request", "Error should indicate timeout")
		require.Zero(t, lag, "Should return zero lag on query timeout")

		require.NoError(t, mock.ExpectationsWereMet(), "All SQL expectations should be met")
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
	primaryLSN := "0/1234567"

	t.Run("successful check", func(t *testing.T) {
		tracker := NewReplicaLagTracker()

		// Mock time lag query
		expectedTimeLag := 2.5 // seconds
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillReturnRows(sqlmock.NewRows([]string{"lag"}).AddRow(expectedTimeLag))

		// Mock bytes lag query
		expectedBytesLag := int64(1048576)
		mock.ExpectQuery("SELECT pg_wal_lsn_diff").
			WithArgs(primaryLSN).
			WillReturnRows(sqlmock.NewRows([]string{"diff"}).AddRow(expectedBytesLag))

		// Call Check
		beforeCheck := time.Now()
		err := tracker.Check(ctx, primaryLSN, db)
		require.NoError(t, err, "Check should not return error on successful query")

		// Verify internal state changed
		require.Len(t, tracker.lagInfo, 1, "lagInfo map should have one entry after Check")
		require.Contains(t, tracker.lagInfo, db.Address(), "lagInfo map should contain the DB address")
		require.Equal(t, time.Duration(expectedTimeLag*float64(time.Second)),
			tracker.lagInfo[db.Address()].TimeLag,
			"TimeLag should match query result")
		require.Equal(t, expectedBytesLag,
			tracker.lagInfo[db.Address()].BytesLag,
			"BytesLag should match query result")
		require.GreaterOrEqual(t, tracker.lagInfo[db.Address()].LastChecked, beforeCheck,
			"LastChecked should be at or after the captured start time")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("time lag error", func(t *testing.T) {
		tracker := NewReplicaLagTracker()

		// Mock time lag query error
		timeLagErr := errors.New("time lag query failed")
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillReturnError(timeLagErr)

		// Check lag
		err := tracker.Check(ctx, primaryLSN, db)

		// Verify error is returned
		require.Error(t, err, "Check should return error when time lag query fails")
		require.ErrorContains(t, err, timeLagErr.Error(), "Original error should be included")

		// Verify no lag info was stored
		require.Empty(t, tracker.lagInfo, "lagInfo map should remain empty on error")

		require.NoError(t, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})

	t.Run("bytes lag error", func(t *testing.T) {
		tracker := NewReplicaLagTracker()

		// Mock successful time lag query
		expectedTimeLag := 1.5
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillReturnRows(sqlmock.NewRows([]string{"lag"}).AddRow(expectedTimeLag))

		// Mock bytes lag query error
		bytesLagErr := errors.New("bytes lag query failed")
		mock.ExpectQuery("SELECT pg_wal_lsn_diff").
			WithArgs(primaryLSN).
			WillReturnError(bytesLagErr)

		// Check lag
		err := tracker.Check(ctx, primaryLSN, db)

		// Verify error is returned
		require.Error(t, err, "Check should return error when bytes lag query fails")
		require.ErrorContains(t, err, bytesLagErr.Error(), "Original error should be included")

		// Verify no lag info was stored
		require.Empty(t, tracker.lagInfo, "lagInfo map should remain empty on error")

		require.NoError(t, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})
}

func TestDBLoadBalancer_primaryLSN(t *testing.T) {
	// Create a mock database
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	// Create a load balancer with the mock database as primary
	lb := &DBLoadBalancer{
		primary: &DB{
			DB: mockDB,
		},
	}

	ctx := context.Background()
	expectedQueryPattern := `SELECT pg_current_wal_insert_lsn()`
	expectedLSN := "0/1234567"

	t.Run("success", func(t *testing.T) {
		// Set up mock expectation for the LSN query
		mock.ExpectQuery(expectedQueryPattern).
			WillReturnRows(sqlmock.NewRows([]string{"location"}).AddRow(expectedLSN))

		// Call the method
		lsn, err := lb.primaryLSN(ctx)

		// Verify result
		require.NoError(t, err, "primaryLSN should not return an error")
		require.Equal(t, expectedLSN, lsn, "LSN should match expected value")
		require.NoError(t, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})

	t.Run("error", func(t *testing.T) {
		// Set up mock expectation for a failed LSN query
		expectedErr := errors.New("database error")
		mock.ExpectQuery(expectedQueryPattern).
			WillReturnError(expectedErr)

		// Call the method
		lsn, err := lb.primaryLSN(ctx)

		// Verify error is returned
		require.Error(t, err, "primaryLSN should return an error")
		require.ErrorContains(t, err, expectedErr.Error(), "Error message should be descriptive")
		require.ErrorContains(t, err, "database error", "Original error should be included")
		require.Empty(t, lsn, "LSN should be empty on error")
		require.NoError(t, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})

	t.Run("timeout", func(t *testing.T) {
		// Set up mock expectation with delay exceeding timeout
		mock.ExpectQuery(expectedQueryPattern).
			WillDelayFor(50 * time.Millisecond).
			WillReturnRows(sqlmock.NewRows([]string{"location"}).AddRow(expectedLSN))

		// Call the method
		ctx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
		defer cancel()
		lsn, err := lb.primaryLSN(ctx)

		// Verify timeout error is handled
		require.Error(t, err, "primaryLSN should return an error on timeout")
		require.ErrorContains(t, err, "canceling query due to user request", "Error message should be descriptive")
		require.Empty(t, lsn, "LSN should be empty on error")
		require.NoError(t, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})
}

func TestReplicaLagTracker_CheckBytesLag(t *testing.T) {
	tracker := NewReplicaLagTracker()
	ctx := context.Background()

	// Create a mock DB with an address
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	db := &DB{DB: mockDB}

	// Primary LSN to use for testing
	primaryLSN := "0/1234567"
	expectedQueryPattern := "SELECT pg_wal_lsn_diff"

	t.Run("successful query", func(t *testing.T) {
		expectedLag := int64(1048576)

		// Mock the bytes lag query
		mock.ExpectQuery(expectedQueryPattern).
			WithArgs(primaryLSN).
			WillReturnRows(sqlmock.NewRows([]string{"diff"}).AddRow(expectedLag))

		// Check lag
		bytesLag, err := tracker.CheckBytesLag(ctx, primaryLSN, db)

		// Verify no error and lag is correct
		require.NoError(t, err, "CheckBytesLag should not return error on successful query")
		require.Equal(t, expectedLag, bytesLag, "Should return the correct bytes lag")
		require.NoError(t, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})

	t.Run("query error", func(t *testing.T) {
		// Mock a query error
		expectedErr := errors.New("query failed")
		mock.ExpectQuery(expectedQueryPattern).
			WithArgs(primaryLSN).
			WillReturnError(expectedErr)

		// Check lag
		bytesLag, err := tracker.CheckBytesLag(ctx, primaryLSN, db)

		// Verify error is returned and zero lag
		require.Error(t, err, "CheckBytesLag should return error on query failure")
		require.ErrorContains(t, err, "failed to calculate replica bytes lag", "Error should be descriptive")
		require.ErrorContains(t, err, "query failed", "Original error should be included")
		require.Zero(t, bytesLag, "Should return zero lag on query error")
		require.NoError(t, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})

	t.Run("query timeout", func(t *testing.T) {
		// Mock a query that takes too long (exceeds the replicaLagCheckTimeout timeout)
		mock.ExpectQuery(expectedQueryPattern).
			WithArgs(primaryLSN).
			WillDelayFor(replicaLagCheckTimeout + 50*time.Millisecond).
			WillReturnRows(sqlmock.NewRows([]string{"diff"}).AddRow(1024))

		// Check lag
		bytesLag, err := tracker.CheckBytesLag(ctx, primaryLSN, db)

		// Verify error is returned and zero lag on timeout
		require.Error(t, err, "CheckBytesLag should return error on timeout")
		require.ErrorContains(t, err, "failed to calculate replica bytes lag", "Error should be descriptive")
		require.Contains(t, err.Error(), "cancel", "Error should indicate cancellation")
		require.Zero(t, bytesLag, "Should return zero lag on query timeout")
		require.NoError(t, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})
}

// mockLagTracker is a test implementation of the LagTracker interface
type mockLagTracker struct {
	checkCalls      map[string]int
	checkFn         func(ctx context.Context, primaryLSN string, replica *DB) error
	inputPrimaryLSN string
	lagInfo         map[string]*ReplicaLagInfo
}

func newMockLagTracker() *mockLagTracker {
	return &mockLagTracker{
		checkCalls: make(map[string]int),
		checkFn: func(_ context.Context, _ string, _ *DB) error {
			return nil
		},
		lagInfo: make(map[string]*ReplicaLagInfo),
	}
}

func (m *mockLagTracker) Check(ctx context.Context, primaryLSN string, replica *DB) error {
	m.inputPrimaryLSN = primaryLSN
	m.checkCalls[replica.DSN.Host]++
	return m.checkFn(ctx, primaryLSN, replica)
}

func (m *mockLagTracker) Get(replicaAddr string) *ReplicaLagInfo {
	info, exists := m.lagInfo[replicaAddr]
	if !exists {
		return nil
	}
	return info
}

func TestDBLoadBalancer_StartLagCheck(t *testing.T) {
	// Create test primary DB
	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	primaryDSN := &DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	primaryDB := &DB{
		DB:  primaryMockDB,
		DSN: primaryDSN,
	}

	// Create test replicas
	replica1MockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	replica1 := &DB{
		DB:  replica1MockDB,
		DSN: &DSN{Host: "replica1", Port: 5432},
	}

	replica2 := &DB{
		DB:  replica2MockDB,
		DSN: &DSN{Host: "replica2", Port: 5432},
	}

	// Create our mock lag tracker
	mockTracker := newMockLagTracker()

	// Configure the primary LSN query
	primaryLSN := "0/1000000"
	primaryLSNRows := sqlmock.NewRows([]string{"location"}).AddRow(primaryLSN)
	primaryMock.ExpectQuery("SELECT pg_current_wal_insert_lsn\\(\\)::text AS location").WillReturnRows(primaryLSNRows)

	// Create the load balancer and populate its fields for testing
	lb := &DBLoadBalancer{
		primary:              primaryDB,
		replicas:             []*DB{replica1, replica2},
		replicaCheckInterval: 50 * time.Millisecond,
		lagTracker:           mockTracker,
	}

	// Start lag checking in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- lb.StartLagCheck(ctx)
	}()

	// Wait just enough time for the lag checker to run once
	time.Sleep(60 * time.Millisecond)

	// Cancel the context and make sure it exits properly
	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)

	// Verify our lag tracker was called for each replica
	require.Equal(t, 1, mockTracker.checkCalls["replica1"])
	require.Equal(t, 1, mockTracker.checkCalls["replica2"])
	require.Equal(t, primaryLSN, mockTracker.inputPrimaryLSN)

	// Verify primary mock expectations (LSN query)
	require.NoError(t, primaryMock.ExpectationsWereMet())
}

func TestDBLoadBalancer_StartLagCheck_ZeroInterval(t *testing.T) {
	// Create a load balancer with zero check interval
	lb := &DBLoadBalancer{
		replicaCheckInterval: 0,                                   // Zero interval
		replicas:             []*DB{{DSN: &DSN{Host: "replica"}}}, // Non-empty replicas
	}

	// StartLagCheck should return immediately
	err := lb.StartLagCheck(context.Background())
	require.NoError(t, err)
}

func TestDBLoadBalancer_StartLagCheck_NoReplicas(t *testing.T) {
	// Create a load balancer with no replicas
	lb := &DBLoadBalancer{
		replicaCheckInterval: 50 * time.Millisecond, // Non-zero interval
		replicas:             nil,                   // Empty replicas
	}

	// StartLagCheck should return immediately
	err := lb.StartLagCheck(context.Background())
	require.NoError(t, err)
}

func TestDBLoadBalancer_StartLagCheck_PrimaryLSNError(t *testing.T) {
	// Create mock primary DB
	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	primaryDB := &DB{
		DB:  primaryMockDB,
		DSN: &DSN{Host: "primary"},
	}

	// Create test replicas
	replica := &DB{
		DSN: &DSN{Host: "replica1"},
	}

	// Create mock tracker
	mockTracker := newMockLagTracker()

	// Configure primaryLSN to return an error
	primaryMock.ExpectQuery("SELECT pg_current_wal_insert_lsn\\(\\)::text AS location").
		WillReturnError(errors.New("query failed"))

	// Create the load balancer
	lb := &DBLoadBalancer{
		primary:              primaryDB,
		replicas:             []*DB{replica},
		replicaCheckInterval: 50 * time.Millisecond,
		lagTracker:           mockTracker,
	}

	// Start lag checking in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- lb.StartLagCheck(ctx)
	}()

	// Wait just enough time for the lag checker to attempt to run once
	time.Sleep(60 * time.Millisecond)

	// Cancel the context to stop the lag checking
	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)

	// Verify the tracker wasn't called at all since we had a primaryLSN error
	require.Empty(t, mockTracker.checkCalls, "Lag tracker should not be called when primary LSN query fails")

	// Verify primary mock expectations (LSN query)
	require.NoError(t, primaryMock.ExpectationsWereMet())
}
