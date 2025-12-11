package datastore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

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

// TestDBLoadBalancer_Replica tests the replica selection logic of DBLoadBalancer in various scenarios including no
// replicas, some/all quarantined replicas, and round-robin selection.
func TestDBLoadBalancer_Replica(t *testing.T) {
	ctx := context.Background()

	t.Run("no replicas falls back to primary", func(tt *testing.T) {
		// Create primary DB
		primaryMockDB, primaryDB := createTestDB(tt, "primary")

		// Create load balancer with no replicas
		lb := &DBLoadBalancer{
			primary:             primaryDB,
			replicas:            make([]*DB, 0),
			connectivityTracker: NewReplicaConnectivityTracker(),
		}

		// Should return primary when no replicas available
		db := lb.Replica(ctx)
		require.Equal(tt, primaryMockDB, db.DB)
	})

	t.Run("round-robin selection with multiple replicas", func(tt *testing.T) {
		// Create primary and replica DBs
		_, primaryDB := createTestDB(tt, "primary")
		replica1MockDB, replica1 := createTestDB(tt, "replica1")
		replica2MockDB, replica2 := createTestDB(tt, "replica2")
		replica3MockDB, replica3 := createTestDB(tt, "replica3")

		// Create load balancer with multiple replicas
		lb := &DBLoadBalancer{
			primary:             primaryDB,
			replicas:            []*DB{replica1, replica2, replica3},
			connectivityTracker: NewReplicaConnectivityTracker(),
		}

		// First call should return first replica
		db1 := lb.Replica(ctx)
		require.Equal(tt, replica1MockDB, db1.DB)

		// Second call should return second replica
		db2 := lb.Replica(ctx)
		require.Equal(tt, replica2MockDB, db2.DB)

		// Third call should return third replica
		db3 := lb.Replica(ctx)
		require.Equal(tt, replica3MockDB, db3.DB)

		// Fourth call should wrap around to first replica
		db4 := lb.Replica(ctx)
		require.Equal(tt, replica1MockDB, db4.DB)
	})

	t.Run("skips quarantined replicas", func(tt *testing.T) {
		// Create primary and replica DBs
		_, primaryDB := createTestDB(tt, "primary")
		replica1MockDB, replica1 := createTestDB(tt, "replica1")
		_, replica2 := createTestDB(tt, "replica2") // we'll quarantine this one
		replica3MockDB, replica3 := createTestDB(tt, "replica3")

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
			primary:             primaryDB,
			replicas:            []*DB{replica1, replica2, replica3},
			lagTracker:          tracker,
			connectivityTracker: NewReplicaConnectivityTracker(),
		}

		// First call should return first replica
		db1 := lb.Replica(ctx)
		require.Equal(tt, replica1MockDB, db1.DB)

		// Second call should skip quarantined replica2 and return replica3
		db2 := lb.Replica(ctx)
		require.Equal(tt, replica3MockDB, db2.DB)

		// Third call should wrap around to replica1
		db3 := lb.Replica(ctx)
		require.Equal(tt, replica1MockDB, db3.DB)
	})

	t.Run("falls back to primary when all replicas quarantined", func(tt *testing.T) {
		// Create primary and replica DBs
		primaryMockDB, primaryDB := createTestDB(tt, "primary")
		_, replica1 := createTestDB(tt, "replica1")
		_, replica2 := createTestDB(tt, "replica2")

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
			primary:             primaryDB,
			replicas:            []*DB{replica1, replica2},
			lagTracker:          tracker,
			connectivityTracker: NewReplicaConnectivityTracker(),
		}

		// Should fall back to primary when all replicas are quarantined
		db := lb.Replica(ctx)
		require.Equal(tt, primaryMockDB, db.DB)
	})

	t.Run("index updates correctly with mixed quarantined replicas", func(tt *testing.T) {
		// Create primary and replica DBs
		_, primaryDB := createTestDB(tt, "primary")
		replica1MockDB, replica1 := createTestDB(tt, "replica1")
		replica2MockDB, replica2 := createTestDB(tt, "replica2") // will be quarantined
		replica3MockDB, replica3 := createTestDB(tt, "replica3")

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
			primary:             primaryDB,
			replicas:            []*DB{replica1, replica2, replica3},
			lagTracker:          tracker,
			connectivityTracker: NewReplicaConnectivityTracker(),
		}

		// First call should return replica1
		db1 := lb.Replica(ctx)
		require.Equal(tt, replica1MockDB, db1.DB)

		// Second call should skip quarantined replica2 and return replica3
		db2 := lb.Replica(ctx)
		require.Equal(tt, replica3MockDB, db2.DB)

		// Third call should return replica1 again (round-robin)
		db3 := lb.Replica(ctx)
		require.Equal(tt, replica1MockDB, db3.DB)

		// The index should update based on total replicas (not just available ones)
		// Start: index=0, Calls: R1->index=1, R3->index=2, R1->index=0
		require.Equal(tt, 0, lb.replicaIndex, "Index should wrap around to 0")

		// Now reintegrate replica2
		tracker.lagInfo[replica2.Address()].Quarantined = false
		tracker.lagInfo[replica2.Address()].QuarantinedAt = time.Time{}

		// Fourth call should return replica1 (index=0)
		db4 := lb.Replica(ctx)
		require.Equal(tt, replica1MockDB, db4.DB)

		// Fifth call should return replica2 which is now available (index=1)
		db5 := lb.Replica(ctx)
		require.Equal(tt, replica2MockDB, db5.DB)

		// Sixth call should return replica3 (index=2)
		db6 := lb.Replica(ctx)
		require.Equal(tt, replica3MockDB, db6.DB)
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

func TestDBLoadBalancer_TypeOf_AddressFallback(t *testing.T) {
	// Set up a load balancer with DSN addresses
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()
	replicaDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaDB.Close()

	lb := &DBLoadBalancer{
		primary:  &DB{DB: primaryDB, DSN: &DSN{Host: "primary-host", Port: 5432}},
		replicas: []*DB{{DB: replicaDB, DSN: &DSN{Host: "replica-host", Port: 5432}}},
	}

	// Now create different *sql.DB handles that are not pointer-equal, but share the same DSN addresses
	altPrimaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer altPrimaryDB.Close()
	altReplicaDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer altReplicaDB.Close()

	otherPrimary := &DB{DB: altPrimaryDB, DSN: &DSN{Host: "primary-host", Port: 5432}}
	otherReplica := &DB{DB: altReplicaDB, DSN: &DSN{Host: "replica-host", Port: 5432}}

	// Because pointer identity fails, TypeOf should fall back to address and classify correctly
	require.Equal(t, HostTypePrimary, lb.TypeOf(otherPrimary))
	require.Equal(t, HostTypeReplica, lb.TypeOf(otherReplica))
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
	testCases := []struct {
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
		{
			name:     "statement timeout (57014) is not connectivity",
			err:      &pgconn.PgError{Code: pgerrcode.QueryCanceled}, // 57014
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			result := isConnectivityError(tc.err)
			require.Equal(tt, tc.expected, result)
		})
	}
}

func TestNewLivenessProber(t *testing.T) {
	t.Run("default options", func(tt *testing.T) {
		p := NewLivenessProber()
		require.NotNil(tt, p)
		require.Equal(tt, minLivenessProbeInterval, p.minInterval)
		require.Equal(tt, livenessProbeTimeout, p.timeout)
		require.Nil(tt, p.onUnhealthy)
	})

	t.Run("custom options", func(tt *testing.T) {
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

		require.NotNil(tt, p)
		require.Equal(tt, 123*time.Millisecond, p.minInterval)
		require.Equal(tt, 456*time.Millisecond, p.timeout)
		require.NotNil(tt, p.onUnhealthy)

		// Test callback directly
		mockDB := &DB{DSN: &DSN{Host: "test-db"}}
		p.onUnhealthy(context.Background(), mockDB)
		require.True(tt, callbackCalled, "Callback should be called")
		require.Equal(tt, mockDB, callbackDB, "Callback should receive the input DB")
	})
}

func TestLivenessProber_BeginCheck(t *testing.T) {
	t.Run("same host", func(tt *testing.T) {
		prober := NewLivenessProber(
			WithMinProbeInterval(50 * time.Millisecond),
		)
		hostAddr := "test-db:5432"

		require.True(tt, prober.BeginCheck(hostAddr), "First probe should be allowed")
		require.False(tt, prober.BeginCheck(hostAddr), "Second immediate probe should be throttled")
		time.Sleep(30 * time.Millisecond)
		require.False(tt, prober.BeginCheck(hostAddr), "Probe should be throttled when within interval")
		time.Sleep(30 * time.Millisecond)
		require.True(tt, prober.BeginCheck(hostAddr), "Probe should be allowed after interval expires")
	})

	t.Run("different hosts", func(tt *testing.T) {
		prober := NewLivenessProber(
			WithMinProbeInterval(50 * time.Millisecond),
		)

		require.True(tt, prober.BeginCheck("host1:5432"), "First host should be allowed")
		require.True(tt, prober.BeginCheck("host2:5432"), "Second host should be allowed")
		require.False(tt, prober.BeginCheck("host1:5432"), "First host should be throttled")
		require.False(tt, prober.BeginCheck("host2:5432"), "Second host should be throttled")
	})
}

func TestLivenessProber_Probe(t *testing.T) {
	t.Run("successful", func(tt *testing.T) {
		dbMock, sqlMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(tt, err)
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

		require.False(tt, callbackCalled, "Callback should not be called for successful probe")
		require.NoError(tt, sqlMock.ExpectationsWereMet())
	})

	t.Run("failed", func(tt *testing.T) {
		dbMock, sqlMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(tt, err)
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

		require.True(tt, callbackCalled, "Callback should be called for failed probe")
		require.Equal(tt, db, callbackDB, "Callback should receive the unhealthy DB")
		require.NoError(tt, sqlMock.ExpectationsWereMet())
	})

	t.Run("timeout", func(tt *testing.T) {
		dbMock, sqlMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(tt, err)
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

		require.True(tt, callbackCalled, "Callback should be called when probe times out")
		require.NoError(tt, sqlMock.ExpectationsWereMet())
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

func TestThrottledPoolResolver(t *testing.T) {
	t.Run("default options", func(tt *testing.T) {
		r := NewThrottledPoolResolver()
		require.NotNil(tt, r)
		require.Equal(tt, minResolveReplicasInterval, r.minInterval)
		require.Nil(tt, r.resolveFn)
	})

	t.Run("custom options", func(tt *testing.T) {
		var resolveCalled bool
		r := NewThrottledPoolResolver(
			WithMinInterval(123*time.Millisecond),
			WithResolveFunction(func(_ context.Context) error {
				resolveCalled = true
				return nil
			}),
		)

		require.NotNil(tt, r)
		require.Equal(tt, 123*time.Millisecond, r.minInterval)
		require.NotNil(tt, r.resolveFn)

		// Test resolve function
		err := r.resolveFn(context.Background())
		require.NoError(tt, err)
		require.True(tt, resolveCalled)
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
	t.Run("successful", func(tt *testing.T) {
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
		require.True(tt, result, "Resolve should return true when resolution was performed")
		require.True(tt, resolveCalled, "Resolve function should have been called")

		// Reset for next test
		resolveCalled = false

		// Immediate second resolution should be throttled
		result = r.Resolve(context.Background())
		require.False(tt, result, "Immediate second resolution should be throttled")
		require.False(tt, resolveCalled, "Resolve function should not have been called")

		// After waiting, third resolution should succeed
		resolveCalled = false
		time.Sleep(60 * time.Millisecond)

		result = r.Resolve(context.Background())
		require.True(tt, result, "Resolution after waiting should succeed")
		require.True(tt, resolveCalled, "Resolve function should have been called")
	})

	t.Run("error", func(tt *testing.T) {
		expectedErr := errors.New("resolution error")
		r := NewThrottledPoolResolver(
			WithResolveFunction(func(_ context.Context) error {
				return expectedErr
			}),
		)

		// Resolution should proceed but return the error
		result := r.Resolve(context.Background())
		require.True(tt, result, "Resolve should return true even when resolution function returns an error")

		// Verify inProgress was cleared despite the error
		require.False(tt, r.inProgress, "inProgress should be cleared after error")
		require.NotEqual(tt, time.Time{}, r.lastComplete, "lastComplete should be updated after error")
	})

	t.Run("concurrent", func(tt *testing.T) {
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
		require.False(tt, result, "Concurrent resolution should be throttled")

		// Allowed first resolution to complete
		resolveFinished <- struct{}{}

		// Give time for goroutine to complete
		time.Sleep(10 * time.Millisecond)

		// Verify only one resolution occurred
		require.Equal(tt, 1, resolveCount, "Only one resolution should have occurred")
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
	t.Run("default options", func(tt *testing.T) {
		tracker := NewReplicaLagTracker()
		require.NotNil(tt, tracker)
		require.NotNil(tt, tracker.lagInfo, "lagInfo map should be initialized")
		require.Equal(tt, defaultReplicaCheckInterval, tracker.checkInterval, "Should use default check interval")
	})

	t.Run("with custom lag check interval", func(tt *testing.T) {
		customInterval := 5 * time.Minute
		tracker := NewReplicaLagTracker(
			WithLagCheckInterval(customInterval),
		)
		require.NotNil(tt, tracker)
		require.Equal(tt, customInterval, tracker.checkInterval, "Should use custom check interval")

		// Check with zero interval (should use default)
		zeroTracker := NewReplicaLagTracker(
			WithLagCheckInterval(0),
		)
		require.NotNil(tt, zeroTracker)
		require.Equal(tt, defaultReplicaCheckInterval, zeroTracker.checkInterval, "Should use default for zero interval")

		// Check with negative interval (should use default)
		negativeTracker := NewReplicaLagTracker(
			WithLagCheckInterval(-1 * time.Second),
		)
		require.NotNil(tt, negativeTracker)
		require.Equal(tt, defaultReplicaCheckInterval, negativeTracker.checkInterval, "Should use default for negative interval")
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
	createTestDB := func(tt *testing.T, host string) *DB {
		mockDB, _, err := sqlmock.New()
		require.NoError(tt, err)
		tt.Cleanup(func() { mockDB.Close() })

		return &DB{
			DB: mockDB,
			DSN: &DSN{
				Host: host,
				Port: 5432,
			},
		}
	}

	t.Run("no quarantine with normal lag values", func(tt *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(tt, "replica-normal")

		// Set lag values below both thresholds
		normalTimeLag := MaxReplicaLagTime / 2
		normalBytesLag := int64(MaxReplicaLagBytes / 2)

		tracker.set(ctx, db, normalTimeLag, normalBytesLag)

		info := tracker.Get(db.Address())
		require.NotNil(tt, info)
		require.False(tt, info.Quarantined, "Replica shouldn't be quarantined when both lag values are below thresholds")
		require.Zero(tt, info.QuarantinedAt, "QuarantinedAt should be zero")
	})

	t.Run("no quarantine with only time lag exceeding threshold", func(tt *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(tt, "replica-high-time")

		// Set time lag above threshold but bytes lag below
		highTimeLag := MaxReplicaLagTime * 2
		normalBytesLag := int64(MaxReplicaLagBytes / 2)

		tracker.set(ctx, db, highTimeLag, normalBytesLag)

		info := tracker.Get(db.Address())
		require.NotNil(tt, info)
		require.False(tt, info.Quarantined, "Replica shouldn't be quarantined when only time lag exceeds threshold")
		require.Zero(tt, info.QuarantinedAt, "QuarantinedAt should be zero")
	})

	t.Run("no quarantine with only bytes lag exceeding threshold", func(tt *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(tt, "replica-high-bytes")

		// Set bytes lag above threshold but time lag below
		normalTimeLag := MaxReplicaLagTime / 2
		highBytesLag := int64(MaxReplicaLagBytes * 2)

		tracker.set(ctx, db, normalTimeLag, highBytesLag)

		info := tracker.Get(db.Address())
		require.NotNil(tt, info)
		require.False(tt, info.Quarantined, "Replica shouldn't be quarantined when only bytes lag exceeds threshold")
		require.Zero(tt, info.QuarantinedAt, "QuarantinedAt should be zero")
	})

	t.Run("quarantine when both lag values exceed thresholds", func(tt *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(tt, "replica-high-both")

		// Set both lags above thresholds
		highTimeLag := MaxReplicaLagTime * 2
		highBytesLag := int64(MaxReplicaLagBytes * 2)

		tracker.set(ctx, db, highTimeLag, highBytesLag)

		info := tracker.Get(db.Address())
		require.NotNil(tt, info)
		require.True(tt, info.Quarantined, "Replica should be quarantined when both lag values exceed thresholds")
		require.NotZero(tt, info.QuarantinedAt, "QuarantinedAt should be set")
	})

	t.Run("reintegration when time lag drops below threshold", func(tt *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(tt, "replica-reintegrate-time")

		// First quarantine the replica
		highTimeLag := MaxReplicaLagTime * 2
		highBytesLag := int64(MaxReplicaLagBytes * 2)
		tracker.set(ctx, db, highTimeLag, highBytesLag)

		info := tracker.Get(db.Address())
		require.True(tt, info.Quarantined, "Replica should be quarantined initially")

		// Now lower time lag below threshold but keep bytes lag high
		normalTimeLag := MaxReplicaLagTime / 2
		tracker.set(ctx, db, normalTimeLag, highBytesLag)

		info = tracker.Get(db.Address())
		require.NotNil(tt, info)
		require.False(tt, info.Quarantined, "Replica should be reintegrated when time lag drops below threshold")
		require.Zero(tt, info.QuarantinedAt, "QuarantinedAt should be reset")
	})

	t.Run("reintegration when bytes lag drops below threshold", func(tt *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(tt, "replica-reintegrate-bytes")

		// First quarantine the replica
		highTimeLag := MaxReplicaLagTime * 2
		highBytesLag := int64(MaxReplicaLagBytes * 2)
		tracker.set(ctx, db, highTimeLag, highBytesLag)

		info := tracker.Get(db.Address())
		require.True(tt, info.Quarantined, "Replica should be quarantined initially")

		// Now lower bytes lag below threshold but keep time lag high
		normalBytesLag := int64(MaxReplicaLagBytes / 2)
		tracker.set(ctx, db, highTimeLag, normalBytesLag)

		info = tracker.Get(db.Address())
		require.NotNil(tt, info)
		require.False(tt, info.Quarantined, "Replica should be reintegrated when bytes lag drops below threshold")
		require.Zero(tt, info.QuarantinedAt, "QuarantinedAt should be reset")
	})

	t.Run("stays quarantined when both lag values remain high", func(tt *testing.T) {
		tracker := NewReplicaLagTracker()
		db := createTestDB(tt, "replica-stay-quarantined")

		// First quarantine the replica
		highTimeLag := MaxReplicaLagTime * 2
		highBytesLag := int64(MaxReplicaLagBytes * 2)
		tracker.set(ctx, db, highTimeLag, highBytesLag)

		info := tracker.Get(db.Address())
		require.True(tt, info.Quarantined, "Replica should be quarantined initially")
		initialQuarantinedAt := info.QuarantinedAt

		// Update with still high lag values
		newHighTimeLag := MaxReplicaLagTime * 3 / 2          // 1.5x
		newHighBytesLag := int64(MaxReplicaLagBytes * 3 / 2) // 1.5x
		tracker.set(ctx, db, newHighTimeLag, newHighBytesLag)

		info = tracker.Get(db.Address())
		require.NotNil(tt, info)
		require.True(tt, info.Quarantined, "Replica should stay quarantined when both lag values remain high")
		require.Equal(tt, initialQuarantinedAt, info.QuarantinedAt, "QuarantinedAt should not change")
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

	t.Run("successful query", func(tt *testing.T) {
		tracker := NewReplicaLagTracker()

		// Mock the lag query
		expectedLag := 3.5 // seconds
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillReturnRows(sqlmock.NewRows([]string{"lag"}).AddRow(expectedLag))

		// Check lag
		lag, err := tracker.CheckTimeLag(ctx, db)

		// Verify no error and lag is correct
		require.NoError(tt, err, "CheckTimeLag should not return error on successful query")
		require.Equal(tt, time.Duration(expectedLag*float64(time.Second)), lag, "Should return the correct lag duration")
		require.NoError(tt, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})

	t.Run("query error", func(tt *testing.T) {
		tracker := NewReplicaLagTracker()

		// Mock a query error
		expectedErr := errors.New("query failed")
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillReturnError(expectedErr)

		// Check lag
		lag, err := tracker.CheckTimeLag(ctx, db)

		// Verify error is returned and zero lag
		require.Error(tt, err, "CheckTimeLag should return error on query failure")
		require.ErrorContains(tt, err, expectedErr.Error(), "Error should contain the original error message")
		require.Zero(tt, lag, "Should return zero lag on query error")

		require.NoError(tt, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})

	t.Run("query timeout", func(tt *testing.T) {
		tracker := NewReplicaLagTracker()

		// Mock a query that takes too long (exceeds the replicaLagCheckTimeout timeout)
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillDelayFor(replicaLagCheckTimeout + 50*time.Millisecond).
			WillReturnRows(sqlmock.NewRows([]string{"lag"}).AddRow(1.5))

		// Check lag
		lag, err := tracker.CheckTimeLag(ctx, db)

		// Verify error is returned and zero lag on timeout
		require.Error(tt, err, "CheckTimeLag should return error on timeout")
		require.ErrorContains(tt, err, "canceling query due to user request", "Error should indicate timeout")
		require.Zero(tt, lag, "Should return zero lag on query timeout")

		require.NoError(tt, mock.ExpectationsWereMet(), "All SQL expectations should be met")
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

	t.Run("successful check", func(tt *testing.T) {
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
		require.NoError(tt, err, "Check should not return error on successful query")

		// Verify internal state changed
		require.Len(tt, tracker.lagInfo, 1, "lagInfo map should have one entry after Check")
		require.Contains(tt, tracker.lagInfo, db.Address(), "lagInfo map should contain the DB address")
		require.Equal(tt, time.Duration(expectedTimeLag*float64(time.Second)),
			tracker.lagInfo[db.Address()].TimeLag,
			"TimeLag should match query result")
		require.Equal(tt, expectedBytesLag,
			tracker.lagInfo[db.Address()].BytesLag,
			"BytesLag should match query result")
		require.GreaterOrEqual(tt, tracker.lagInfo[db.Address()].LastChecked, beforeCheck,
			"LastChecked should be at or after the captured start time")
		require.NoError(tt, mock.ExpectationsWereMet())
	})

	t.Run("time lag error", func(tt *testing.T) {
		tracker := NewReplicaLagTracker()

		// Mock time lag query error
		timeLagErr := errors.New("time lag query failed")
		mock.ExpectQuery("SELECT EXTRACT\\(EPOCH FROM").
			WillReturnError(timeLagErr)

		// Check lag
		err := tracker.Check(ctx, primaryLSN, db)

		// Verify error is returned
		require.Error(tt, err, "Check should return error when time lag query fails")
		require.ErrorContains(tt, err, timeLagErr.Error(), "Original error should be included")

		// Verify no lag info was stored
		require.Empty(tt, tracker.lagInfo, "lagInfo map should remain empty on error")

		require.NoError(tt, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})

	t.Run("bytes lag error", func(tt *testing.T) {
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
		require.Error(tt, err, "Check should return error when bytes lag query fails")
		require.ErrorContains(tt, err, bytesLagErr.Error(), "Original error should be included")

		// Verify no lag info was stored
		require.Empty(tt, tracker.lagInfo, "lagInfo map should remain empty on error")

		require.NoError(tt, mock.ExpectationsWereMet(), "All SQL expectations should be met")
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

	t.Run("success", func(tt *testing.T) {
		// Set up mock expectation for the LSN query
		mock.ExpectQuery(expectedQueryPattern).
			WillReturnRows(sqlmock.NewRows([]string{"location"}).AddRow(expectedLSN))

		// Call the method
		lsn, err := lb.primaryLSN(ctx)

		// Verify result
		require.NoError(tt, err, "primaryLSN should not return an error")
		require.Equal(tt, expectedLSN, lsn, "LSN should match expected value")
		require.NoError(tt, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})

	t.Run("error", func(tt *testing.T) {
		// Set up mock expectation for a failed LSN query
		expectedErr := errors.New("database error")
		mock.ExpectQuery(expectedQueryPattern).
			WillReturnError(expectedErr)

		// Call the method
		lsn, err := lb.primaryLSN(ctx)

		// Verify error is returned
		require.Error(tt, err, "primaryLSN should return an error")
		require.ErrorContains(tt, err, expectedErr.Error(), "Error message should be descriptive")
		require.ErrorContains(tt, err, "database error", "Original error should be included")
		require.Empty(tt, lsn, "LSN should be empty on error")
		require.NoError(tt, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})

	t.Run("timeout", func(tt *testing.T) {
		// Set up mock expectation with delay exceeding timeout
		mock.ExpectQuery(expectedQueryPattern).
			WillDelayFor(50 * time.Millisecond).
			WillReturnRows(sqlmock.NewRows([]string{"location"}).AddRow(expectedLSN))

		// Call the method
		ctx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
		defer cancel()
		lsn, err := lb.primaryLSN(ctx)

		// Verify timeout error is handled
		require.Error(tt, err, "primaryLSN should return an error on timeout")
		require.ErrorContains(tt, err, "canceling query due to user request", "Error message should be descriptive")
		require.Empty(tt, lsn, "LSN should be empty on error")
		require.NoError(tt, mock.ExpectationsWereMet(), "All SQL expectations should be met")
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

	t.Run("successful query", func(tt *testing.T) {
		expectedLag := int64(1048576)

		// Mock the bytes lag query
		mock.ExpectQuery(expectedQueryPattern).
			WithArgs(primaryLSN).
			WillReturnRows(sqlmock.NewRows([]string{"diff"}).AddRow(expectedLag))

		// Check lag
		bytesLag, err := tracker.CheckBytesLag(ctx, primaryLSN, db)

		// Verify no error and lag is correct
		require.NoError(tt, err, "CheckBytesLag should not return error on successful query")
		require.Equal(tt, expectedLag, bytesLag, "Should return the correct bytes lag")
		require.NoError(tt, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})

	t.Run("query error", func(tt *testing.T) {
		// Mock a query error
		expectedErr := errors.New("query failed")
		mock.ExpectQuery(expectedQueryPattern).
			WithArgs(primaryLSN).
			WillReturnError(expectedErr)

		// Check lag
		bytesLag, err := tracker.CheckBytesLag(ctx, primaryLSN, db)

		// Verify error is returned and zero lag
		require.Error(tt, err, "CheckBytesLag should return error on query failure")
		require.ErrorContains(tt, err, "failed to calculate replica bytes lag", "Error should be descriptive")
		require.ErrorContains(tt, err, "query failed", "Original error should be included")
		require.Zero(tt, bytesLag, "Should return zero lag on query error")
		require.NoError(tt, mock.ExpectationsWereMet(), "All SQL expectations should be met")
	})

	t.Run("query timeout", func(tt *testing.T) {
		// Mock a query that takes too long (exceeds the replicaLagCheckTimeout timeout)
		mock.ExpectQuery(expectedQueryPattern).
			WithArgs(primaryLSN).
			WillDelayFor(replicaLagCheckTimeout + 50*time.Millisecond).
			WillReturnRows(sqlmock.NewRows([]string{"diff"}).AddRow(1024))

		// Check lag
		bytesLag, err := tracker.CheckBytesLag(ctx, primaryLSN, db)

		// Verify error is returned and zero lag on timeout
		require.Error(tt, err, "CheckBytesLag should return error on timeout")
		require.ErrorContains(tt, err, "failed to calculate replica bytes lag", "Error should be descriptive")
		require.Contains(tt, err.Error(), "cancel", "Error should indicate cancellation")
		require.Zero(tt, bytesLag, "Should return zero lag on query timeout")
		require.NoError(tt, mock.ExpectationsWereMet(), "All SQL expectations should be met")
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

func TestReplicaConnectivityTrackerSuite(t *testing.T) {
	suite.Run(t, new(ReplicaConnectivityTrackerSuite))
}

type ReplicaConnectivityTrackerSuite struct {
	suite.Suite
	tracker *ReplicaConnectivityTracker
	ctx     context.Context
	addr    string
}

func (s *ReplicaConnectivityTrackerSuite) SetupTest() {
	s.tracker = NewReplicaConnectivityTracker()
	s.ctx = context.Background()
	s.addr = "replica1:5432"
}

func (s *ReplicaConnectivityTrackerSuite) TestRecordFailure() {
	// Record first failure
	s.tracker.RecordFailure(s.ctx, s.addr)
	info := s.tracker.Get(s.addr)
	require.NotNil(s.T(), info)
	require.Equal(s.T(), 1, info.ConsecutiveFailures)
	require.False(s.T(), info.Quarantined)

	// Record second failure
	s.tracker.RecordFailure(s.ctx, s.addr)
	info = s.tracker.Get(s.addr)
	require.Equal(s.T(), 2, info.ConsecutiveFailures)
	require.False(s.T(), info.Quarantined)

	// Record third failure - should trigger quarantine
	s.tracker.RecordFailure(s.ctx, s.addr)
	info = s.tracker.Get(s.addr)
	require.Equal(s.T(), 3, info.ConsecutiveFailures)
	require.True(s.T(), info.Quarantined)
	require.NotZero(s.T(), info.QuarantinedAt)
	require.NotZero(s.T(), info.QuarantineReleaseAt)
}

func (s *ReplicaConnectivityTrackerSuite) TestRecordSuccess() {
	// Record some failures
	s.tracker.RecordFailure(s.ctx, s.addr)
	s.tracker.RecordFailure(s.ctx, s.addr)
	info := s.tracker.Get(s.addr)
	require.Equal(s.T(), 2, info.ConsecutiveFailures)

	// Record success - should reset counter
	s.tracker.RecordSuccess(s.ctx, s.addr)
	info = s.tracker.Get(s.addr)
	require.Zero(s.T(), info.ConsecutiveFailures)
}

func (s *ReplicaConnectivityTrackerSuite) TestRecordPoolEventFlapping() {
	// Record events below threshold
	for i := 0; i < flappingThreshold-1; i++ {
		s.tracker.RecordPoolEvent(s.ctx, s.addr)
	}
	info := s.tracker.Get(s.addr)
	require.NotNil(s.T(), info)
	require.False(s.T(), info.Quarantined)
	require.Len(s.T(), info.PoolEvents, flappingThreshold-1)

	// Record one more event to trigger quarantine
	s.tracker.RecordPoolEvent(s.ctx, s.addr)
	info = s.tracker.Get(s.addr)
	require.True(s.T(), info.Quarantined)
	require.Len(s.T(), info.PoolEvents, flappingThreshold)
}

func (s *ReplicaConnectivityTrackerSuite) TestRecordPoolEventSlidingWindow() {
	// Manually set old events outside the window
	now := time.Now()
	oldEvent := now.Add(-flappingWindowDuration - 10*time.Second)
	s.tracker.connectivityInfo[s.addr] = &ReplicaConnectivityInfo{
		Address:    s.addr,
		PoolEvents: []time.Time{oldEvent, oldEvent, oldEvent},
	}

	// Record new event
	s.tracker.RecordPoolEvent(s.ctx, s.addr)

	// Old events should be removed, only new event remains
	info := s.tracker.Get(s.addr)
	require.Len(s.T(), info.PoolEvents, 1)
	require.False(s.T(), info.Quarantined)
}

func (s *ReplicaConnectivityTrackerSuite) TestIsQuarantined() {
	// Not quarantined initially
	require.False(s.T(), s.tracker.IsQuarantined(s.ctx, s.addr))

	// Quarantine the replica
	s.tracker.RecordFailure(s.ctx, s.addr)
	s.tracker.RecordFailure(s.ctx, s.addr)
	s.tracker.RecordFailure(s.ctx, s.addr)
	require.True(s.T(), s.tracker.IsQuarantined(s.ctx, s.addr))

	// Manually set quarantine to expire in the past
	s.tracker.connectivityInfo[s.addr].QuarantineReleaseAt = time.Now().Add(-1 * time.Second)

	// Should auto-reintegrate when checked
	require.False(s.T(), s.tracker.IsQuarantined(s.ctx, s.addr))
	info := s.tracker.Get(s.addr)
	require.False(s.T(), info.Quarantined)
	require.Zero(s.T(), info.ConsecutiveFailures)
	require.Empty(s.T(), info.PoolEvents)
}

func (s *ReplicaConnectivityTrackerSuite) TestGetCopy() {
	// Add some pool events
	s.tracker.RecordPoolEvent(s.ctx, s.addr)
	s.tracker.RecordPoolEvent(s.ctx, s.addr)

	// Get info and modify the copy
	info := s.tracker.Get(s.addr)
	require.Len(s.T(), info.PoolEvents, 2)

	// Modify the copied slice
	info.PoolEvents = append(info.PoolEvents, time.Now())

	// Original should not be modified
	info2 := s.tracker.Get(s.addr)
	require.Len(s.T(), info2.PoolEvents, 2)
}

func TestDBLoadBalancer_Replica_ConnectivityQuarantine(t *testing.T) {
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	replicaDB1, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaDB1.Close()

	replicaDB2, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaDB2.Close()

	replica1 := &DB{DB: replicaDB1, DSN: &DSN{Host: "replica1", Port: 5432}}
	replica2 := &DB{DB: replicaDB2, DSN: &DSN{Host: "replica2", Port: 5432}}

	tracker := NewReplicaConnectivityTracker()
	ctx := context.Background()

	// Quarantine replica1 for connectivity
	tracker.RecordFailure(ctx, replica1.Address())
	tracker.RecordFailure(ctx, replica1.Address())
	tracker.RecordFailure(ctx, replica1.Address())

	lb := &DBLoadBalancer{
		primary:             &DB{DB: primaryDB},
		replicas:            []*DB{replica1, replica2},
		connectivityTracker: tracker,
	}

	// Should skip quarantined replica1 and return replica2
	selected := lb.Replica(ctx)
	require.Equal(t, replica2, selected)
}

func TestDBLoadBalancer_Replica_BothQuarantineTypes(t *testing.T) {
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	replicaDB1, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaDB1.Close()

	replicaDB2, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaDB2.Close()

	replica1 := &DB{DB: replicaDB1, DSN: &DSN{Host: "replica1", Port: 5432}}
	replica2 := &DB{DB: replicaDB2, DSN: &DSN{Host: "replica2", Port: 5432}}

	connectivityTracker := NewReplicaConnectivityTracker()
	lagTracker := &mockLagTracker{
		lagInfo: map[string]*ReplicaLagInfo{
			replica1.Address(): {Quarantined: true}, // Quarantined for lag
		},
	}

	ctx := context.Background()

	// Quarantine replica2 for connectivity
	connectivityTracker.RecordFailure(ctx, replica2.Address())
	connectivityTracker.RecordFailure(ctx, replica2.Address())
	connectivityTracker.RecordFailure(ctx, replica2.Address())

	lb := &DBLoadBalancer{
		primary:             &DB{DB: primaryDB},
		replicas:            []*DB{replica1, replica2},
		lagTracker:          lagTracker,
		connectivityTracker: connectivityTracker,
	}

	// Both replicas quarantined, should fall back to primary
	selected := lb.Replica(ctx)
	require.Equal(t, lb.primary, selected)
}

func TestDBLoadBalancer_ResolveReplicas_SkipsQuarantinedReplicas(t *testing.T) {
	ctx := context.Background()

	// Create primary DB
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	// Create replica DBs for mocking
	replica1DB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1DB.Close()

	replica2DB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2DB.Close()

	// Set up flapping tracker with replica1 quarantined
	connectivityTracker := NewReplicaConnectivityTracker()
	connectivityTracker.RecordFailure(ctx, "replica1:5432")
	connectivityTracker.RecordFailure(ctx, "replica1:5432")
	connectivityTracker.RecordFailure(ctx, "replica1:5432") // Quarantine after 3 failures

	// Verify replica1 is quarantined
	require.True(t, connectivityTracker.IsQuarantined(ctx, "replica1:5432"))
	require.False(t, connectivityTracker.IsQuarantined(ctx, "replica2:5432"))

	// Create stub connector that returns our mock DBs
	connector := &stubConnector{
		openFunc: func(_ context.Context, dsn *DSN, _ ...Option) (*DB, error) {
			addr := dsn.Address()
			if addr == "replica1:5432" {
				return &DB{DB: replica1DB, DSN: dsn}, nil
			}
			if addr == "replica2:5432" {
				return &DB{DB: replica2DB, DSN: dsn}, nil
			}
			return nil, fmt.Errorf("unknown host: %s", addr)
		},
	}

	lb := &DBLoadBalancer{
		primary:             &DB{DB: primaryDB, DSN: &DSN{Host: "primary", Port: 5432, DBName: "testdb"}},
		primaryDSN:          &DSN{Host: "primary", Port: 5432, DBName: "testdb"},
		connector:           connector,
		fixedHosts:          []string{"replica1", "replica2"},
		connectivityTracker: connectivityTracker,
	}

	// Resolve replicas
	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err)

	// Verify only replica2 was added (replica1 should be skipped because it's quarantined)
	require.Len(t, lb.replicas, 1)
	require.Equal(t, "replica2:5432", lb.replicas[0].Address())
}

func TestDBLoadBalancer_ResolveReplicas_RecordsPoolEvents(t *testing.T) {
	ctx := context.Background()

	// Create primary DB
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	// Create replica DBs
	replica1DB, replica1Mock, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1DB.Close()

	replica2DB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2DB.Close()

	connectivityTracker := NewReplicaConnectivityTracker()

	// Create stub connector
	connector := &stubConnector{
		openFunc: func(_ context.Context, dsn *DSN, _ ...Option) (*DB, error) {
			addr := dsn.Address()
			if addr == "replica1:5432" {
				return &DB{DB: replica1DB, DSN: dsn}, nil
			}
			if addr == "replica2:5432" {
				return &DB{DB: replica2DB, DSN: dsn}, nil
			}
			return nil, fmt.Errorf("unknown host: %s", addr)
		},
	}

	lb := &DBLoadBalancer{
		primary:             &DB{DB: primaryDB, DSN: &DSN{Host: "primary", Port: 5432, DBName: "testdb"}},
		primaryDSN:          &DSN{Host: "primary", Port: 5432, DBName: "testdb"},
		connector:           connector,
		fixedHosts:          []string{"replica1"},
		connectivityTracker: connectivityTracker,
	}

	// First resolution - add replica1
	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err)
	require.Len(t, lb.replicas, 1)

	// Verify pool event was recorded for replica1 addition
	info1 := connectivityTracker.Get("replica1:5432")
	require.NotNil(t, info1)
	require.Len(t, info1.PoolEvents, 1)

	// Now add replica2 to the fixed hosts
	lb.fixedHosts = []string{"replica1", "replica2"}

	// Second resolution - add replica2
	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err)
	require.Len(t, lb.replicas, 2)

	// Verify pool event was recorded for replica2 addition
	info2 := connectivityTracker.Get("replica2:5432")
	require.NotNil(t, info2)
	require.Len(t, info2.PoolEvents, 1)

	// Replica1 should still have only 1 event (no new event on second resolution)
	info1Updated := connectivityTracker.Get("replica1:5432")
	require.Len(t, info1Updated.PoolEvents, 1)

	// Now remove replica1 from fixed hosts
	lb.fixedHosts = []string{"replica2"}

	// Expect Close() call when replica1 is removed from the pool
	replica1Mock.ExpectClose()

	// Third resolution - remove replica1
	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err)
	require.Len(t, lb.replicas, 1)
	require.Equal(t, "replica2:5432", lb.replicas[0].Address())

	// Verify removal event was recorded for replica1
	info1Final := connectivityTracker.Get("replica1:5432")
	require.NotNil(t, info1Final)
	require.Len(t, info1Final.PoolEvents, 2) // Add + Remove events
}

// stubConnector is a test implementation of the Connector interface
type stubConnector struct {
	openFunc func(ctx context.Context, dsn *DSN, opts ...Option) (*DB, error)
}

func (c *stubConnector) Open(ctx context.Context, dsn *DSN, opts ...Option) (*DB, error) {
	if c.openFunc != nil {
		return c.openFunc(ctx, dsn, opts...)
	}
	return nil, fmt.Errorf("stubConnector.openFunc not set")
}

func TestDBLoadBalancer_ResolveReplicas_ConsecutiveFailures_NewReplica(t *testing.T) {
	ctx := context.Background()

	// Create primary DB
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	connectivityTracker := NewReplicaConnectivityTracker()

	// Create a connector that always fails
	connector := &stubConnector{
		openFunc: func(_ context.Context, _ *DSN, _ ...Option) (*DB, error) {
			return nil, fmt.Errorf("connection failed")
		},
	}

	lb := &DBLoadBalancer{
		primary:             &DB{DB: primaryDB, DSN: &DSN{Host: "primary", Port: 5432, DBName: "testdb"}},
		primaryDSN:          &DSN{Host: "primary", Port: 5432, DBName: "testdb"},
		connector:           connector,
		fixedHosts:          []string{"replica1"},
		connectivityTracker: connectivityTracker,
	}

	// Attempt to resolve replicas 3 times
	for i := 0; i < 3; i++ {
		err := lb.ResolveReplicas(ctx)
		require.Error(t, err) // Should fail each time
	}

	// Verify replica was quarantined after 3 consecutive failures
	require.True(t, connectivityTracker.IsQuarantined(ctx, "replica1:5432"))

	info := connectivityTracker.Get("replica1:5432")
	require.NotNil(t, info)
	require.Equal(t, 3, info.ConsecutiveFailures)
	require.True(t, info.Quarantined)

	// Verify replica is skipped on next resolution
	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err) // Should succeed because replica is skipped
	require.Empty(t, lb.replicas)
}

func TestDBLoadBalancer_ResolveReplicas_ConsecutiveFailures_Reconnection(t *testing.T) {
	ctx := context.Background()

	// Create primary DB
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	// Create replica DB that will fail ping
	replicaDB, replicaMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer replicaDB.Close()

	connectivityTracker := NewReplicaConnectivityTracker()

	callCount := 0
	connector := &stubConnector{
		openFunc: func(_ context.Context, dsn *DSN, _ ...Option) (*DB, error) {
			callCount++
			if callCount == 1 {
				// First call succeeds
				return &DB{DB: replicaDB, DSN: dsn}, nil
			}
			// Subsequent calls fail (reconnection attempts)
			return nil, fmt.Errorf("reconnection failed")
		},
	}

	lb := &DBLoadBalancer{
		primary:             &DB{DB: primaryDB, DSN: &DSN{Host: "primary", Port: 5432, DBName: "testdb"}},
		primaryDSN:          &DSN{Host: "primary", Port: 5432, DBName: "testdb"},
		connector:           connector,
		fixedHosts:          []string{"replica1"},
		connectivityTracker: connectivityTracker,
	}

	// First resolution - succeeds
	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err)
	require.Len(t, lb.replicas, 1)

	// Verify no failures recorded yet
	info := connectivityTracker.Get("replica1:5432")
	require.NotNil(t, info)
	require.Equal(t, 0, info.ConsecutiveFailures)

	// Now make the replica fail ping 3 times
	for i := 0; i < 3; i++ {
		replicaMock.ExpectPing().WillReturnError(fmt.Errorf("ping failed"))
		err = lb.ResolveReplicas(ctx)
		require.Error(t, err) // Should fail each time
	}

	// Verify replica was quarantined after 3 consecutive reconnection failures
	require.True(t, connectivityTracker.IsQuarantined(ctx, "replica1:5432"))

	info = connectivityTracker.Get("replica1:5432")
	require.NotNil(t, info)
	require.Equal(t, 3, info.ConsecutiveFailures)
	require.True(t, info.Quarantined)
}

func TestDBLoadBalancer_ResolveReplicas_ConsecutiveFailures_SuccessResetsCounter(t *testing.T) {
	ctx := context.Background()

	// Create primary DB
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	// Create replica DB
	replicaDB, replicaMock, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaDB.Close()

	connectivityTracker := NewReplicaConnectivityTracker()

	callCount := 0
	connector := &stubConnector{
		openFunc: func(_ context.Context, dsn *DSN, _ ...Option) (*DB, error) {
			callCount++
			if callCount <= 2 {
				// First 2 calls fail
				return nil, fmt.Errorf("connection failed")
			}
			// Third call succeeds
			return &DB{DB: replicaDB, DSN: dsn}, nil
		},
	}

	lb := &DBLoadBalancer{
		primary:             &DB{DB: primaryDB, DSN: &DSN{Host: "primary", Port: 5432, DBName: "testdb"}},
		primaryDSN:          &DSN{Host: "primary", Port: 5432, DBName: "testdb"},
		connector:           connector,
		fixedHosts:          []string{"replica1"},
		connectivityTracker: connectivityTracker,
	}

	// Fail twice
	for i := 0; i < 2; i++ {
		err := lb.ResolveReplicas(ctx)
		require.Error(t, err)
	}

	// Verify 2 consecutive failures recorded
	info := connectivityTracker.Get("replica1:5432")
	require.NotNil(t, info)
	require.Equal(t, 2, info.ConsecutiveFailures)
	require.False(t, info.Quarantined) // Not quarantined yet

	// Third attempt succeeds
	replicaMock.ExpectClose() // Expect close for cleanup
	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err)
	require.Len(t, lb.replicas, 1)

	// Verify failure counter was reset
	info = connectivityTracker.Get("replica1:5432")
	require.NotNil(t, info)
	require.Zero(t, info.ConsecutiveFailures)
	require.False(t, info.Quarantined)
}

func TestDBLoadBalancer_ResolveReplicas_ConsecutiveFailures_HealthyResetsCounter(t *testing.T) {
	ctx := context.Background()

	// Create primary DB
	primaryDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryDB.Close()

	// Create replica DB
	replicaDB, replicaMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	defer replicaDB.Close()

	connectivityTracker := NewReplicaConnectivityTracker()

	// Manually set consecutive failures
	connectivityTracker.RecordFailure(ctx, "replica1:5432")
	connectivityTracker.RecordFailure(ctx, "replica1:5432")

	// Verify 2 failures recorded
	info := connectivityTracker.Get("replica1:5432")
	require.NotNil(t, info)
	require.Equal(t, 2, info.ConsecutiveFailures)

	connector := &stubConnector{
		openFunc: func(_ context.Context, dsn *DSN, _ ...Option) (*DB, error) {
			return &DB{DB: replicaDB, DSN: dsn}, nil
		},
	}

	lb := &DBLoadBalancer{
		primary:             &DB{DB: primaryDB, DSN: &DSN{Host: "primary", Port: 5432, DBName: "testdb"}},
		primaryDSN:          &DSN{Host: "primary", Port: 5432, DBName: "testdb"},
		connector:           connector,
		fixedHosts:          []string{"replica1"},
		connectivityTracker: connectivityTracker,
	}

	// First resolution - succeeds
	replicaMock.ExpectClose() // Expect close for cleanup
	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err)
	require.Len(t, lb.replicas, 1)

	// Verify failure counter was reset by successful connection
	info = connectivityTracker.Get("replica1:5432")
	require.NotNil(t, info)
	require.Equal(t, 0, info.ConsecutiveFailures)

	// Second resolution - replica is healthy, should also record success
	replicaMock.ExpectPing() // Healthy ping
	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err)
	require.Len(t, lb.replicas, 1)

	// Verify counter is still 0
	info = connectivityTracker.Get("replica1:5432")
	require.NotNil(t, info)
	require.Equal(t, 0, info.ConsecutiveFailures)
}
