package datastore_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/mocks"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/hashicorp/go-multierror"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDSN_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arg  datastore.DSN
		out  string
	}{
		{name: "empty", arg: datastore.DSN{}, out: ""},
		{
			name: "full",
			arg: datastore.DSN{
				Host:           "127.0.0.1",
				Port:           5432,
				User:           "registry",
				Password:       "secret",
				DBName:         "registry_production",
				SSLMode:        "require",
				SSLCert:        "/path/to/client.crt",
				SSLKey:         "/path/to/client.key",
				SSLRootCert:    "/path/to/root.crt",
				ConnectTimeout: 5 * time.Second,
			},
			out: "host=127.0.0.1 port=5432 user=registry password=secret dbname=registry_production sslmode=require sslcert=/path/to/client.crt sslkey=/path/to/client.key sslrootcert=/path/to/root.crt connect_timeout=5",
		},
		{
			name: "with zero port",
			arg: datastore.DSN{
				Port: 0,
			},
			out: "",
		},
		{
			name: "with spaces",
			arg: datastore.DSN{
				Password: "jw8s 0F4",
			},
			out: `password=jw8s\ 0F4`,
		},
		{
			name: "with quotes",
			arg: datastore.DSN{
				Password: "jw8s'0F4",
			},
			out: `password=jw8s\'0F4`,
		},
		{
			name: "with other special characters",
			arg: datastore.DSN{
				Password: "jw8s%^@0F4",
			},
			out: "password=jw8s%^@0F4",
		},
		{
			name: "with zero connection timeout",
			arg: datastore.DSN{
				ConnectTimeout: 0,
			},
			out: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.out, tt.arg.String())
		})
	}
}

func TestDSN_Address(t *testing.T) {
	tests := []struct {
		name string
		arg  datastore.DSN
		out  string
	}{
		{name: "empty", arg: datastore.DSN{}, out: ":0"},
		{name: "no port", arg: datastore.DSN{Host: "127.0.0.1"}, out: "127.0.0.1:0"},
		{name: "no host", arg: datastore.DSN{Port: 5432}, out: ":5432"},
		{name: "full", arg: datastore.DSN{Host: "127.0.0.1", Port: 5432}, out: "127.0.0.1:5432"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.out, tt.arg.Address())
		})
	}
}

func TestNewDBLoadBalancer_WithFixedHosts(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, replicaMock1, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, replicaMock2, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	replica1DSN := &datastore.DSN{
		Host:     "replica1",
		Port:     5432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	replica2DSN := &datastore.DSN{
		Host:     "replica2",
		Port:     5432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	// Mock the expected connections
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(&datastore.DB{DB: replica1MockDB, DSN: replica1DSN}, nil)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(&datastore.DB{DB: replica2MockDB, DSN: replica2DSN}, nil)

	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithFixedHosts([]string{"replica1", "replica2"}),
		datastore.WithConnector(mockConnector),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Verify primary
	require.Equal(t, primaryMockDB, lb.Primary().DB)

	// Verify replicas
	require.NotEmpty(t, lb.Replicas())
	require.Equal(t, []*datastore.DB{
		{DB: replica1MockDB, DSN: replica1DSN},
		{DB: replica2MockDB, DSN: replica2DSN},
	}, lb.Replicas())

	// Verify mock expectations (no operations triggered)
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock1.ExpectationsWereMet())
	require.NoError(t, replicaMock2.ExpectationsWereMet())
}

func TestNewDBLoadBalancer_WithFixedHosts_ConnectionError(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockConnector := mocks.NewMockConnector(ctrl)

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Mock Open function to return errors based on host matching
	mockConnector.EXPECT().Open(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, dsn *datastore.DSN, opts ...datastore.Option) (*datastore.DB, error) {
			if dsn.Host == "fail_primary" {
				return nil, errors.New("primary connection failed")
			} else if match, _ := regexp.MatchString(`fail_replica\d*`, dsn.Host); match {
				return nil, errors.New("replica connection failed")
			}
			return &datastore.DB{DSN: dsn}, nil
		}).AnyTimes()

	tests := []struct {
		name         string
		primaryDSN   *datastore.DSN
		replicaHosts []string
		expectedErr  string
	}{
		{
			name:         "primary connection fails",
			primaryDSN:   &datastore.DSN{Host: "fail_primary"},
			replicaHosts: []string{"replica1"},
			expectedErr:  "failed to open primary database connection: primary connection failed",
		},
		{
			name:       "one replica connection fails",
			primaryDSN: primaryDSN,
			replicaHosts: []string{
				"replica1",
				"fail_replica2",
			},
		},
		{
			name:       "multiple replica connections fail",
			primaryDSN: primaryDSN,
			replicaHosts: []string{
				"fail_replica1",
				"fail_replica2",
			},
		},
		{
			name:       "primary and replica connections fail",
			primaryDSN: &datastore.DSN{Host: "fail_primary", Port: 1234},
			replicaHosts: []string{
				"fail_replica2",
			},
			expectedErr: "failed to open primary database connection: primary connection failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lb, err := datastore.NewDBLoadBalancer(
				context.Background(),
				tt.primaryDSN,
				datastore.WithConnector(mockConnector),
				datastore.WithFixedHosts(tt.replicaHosts),
			)
			if tt.expectedErr != "" {
				require.Nil(t, lb)
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NotNil(t, lb)
				require.NoError(t, err)
			}
		})
	}
}

func TestNewDBLoadBalancer_WithServiceDiscovery(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	// Mock the expected DNS lookups
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
		}, nil)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv2.example.com").
		Return([]string{"192.168.1.2"}, nil)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, replicaMock1, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, replicaMock2, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	replica2DSN := &datastore.DSN{
		Host:     "192.168.1.2",
		Port:     6433,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	// Expect connections to primary and replicas
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(&datastore.DB{DB: replica1MockDB}, nil)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(&datastore.DB{DB: replica2MockDB}, nil)

	// Create the load balancer with the service discovery option
	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	ctx := context.Background()

	// Verify primary
	require.Equal(t, primaryMockDB, lb.Primary().DB)

	// Verify replicas round-robin rotation
	require.Equal(t, replica1MockDB, lb.Replica(ctx).DB)
	require.Equal(t, replica2MockDB, lb.Replica(ctx).DB)
	require.Equal(t, replica1MockDB, lb.Replica(ctx).DB)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock1.ExpectationsWereMet())
	require.NoError(t, replicaMock2.ExpectationsWereMet())
}

func TestDBLoadBalancer_ResolveReplicas_SRVLookupError(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	// Mock the expected DNS lookups with an error
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return(nil, fmt.Errorf("DNS SRV lookup error")).
		Times(2)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Expect connection to primary
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil).AnyTimes()

	// Create the load balancer with the service discovery option
	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	err = lb.ResolveReplicas(context.Background())
	require.ErrorContains(t, err, "error resolving DNS SRV record: DNS SRV lookup error")

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
}

func TestDBLoadBalancer_ResolveReplicas_HostLookupError(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	// Mock the expected DNS lookups
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
		}, nil).
		Times(2)

	// Mock the expected host lookup with an error
	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return(nil, fmt.Errorf("DNS host lookup error")).
		Times(2)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Expect connection to primary
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil).AnyTimes()

	// Create the load balancer with the service discovery option
	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	err = lb.ResolveReplicas(context.Background())
	require.ErrorContains(t, err, `error resolving host "srv1.example.com" address: DNS host lookup error`)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
}

func TestNewDBLoadBalancer_WithServiceDiscovery_ConnectionError(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
		}, nil).AnyTimes()

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).AnyTimes()

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv2.example.com").
		Return([]string{"192.168.1.2"}, nil).AnyTimes()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	failPrimaryDSN := &datastore.DSN{
		Host:     "fail_primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	replica2DSN := &datastore.DSN{
		Host:     "192.168.1.2",
		Port:     6433,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	tests := []struct {
		name           string
		primaryDSN     *datastore.DSN
		mockExpectFunc func()
		expectedErr    string
	}{
		{
			name:       "primary connection fails",
			primaryDSN: failPrimaryDSN,
			mockExpectFunc: func() {
				mockConnector.EXPECT().Open(gomock.Any(), failPrimaryDSN, gomock.Any()).Return(nil, fmt.Errorf("primary connection failed"))
			},
			expectedErr: "failed to open primary database connection: primary connection failed",
		},
		{
			name:       "one replica connection fails",
			primaryDSN: primaryDSN,
			mockExpectFunc: func() {
				primaryMockDB, _, err := sqlmock.New()
				require.NoError(t, err)
				defer primaryMockDB.Close()
				mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil)
				mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(nil, fmt.Errorf("failed to open replica 1"))
				replica2MockDB, _, err := sqlmock.New()
				require.NoError(t, err)
				defer replica2MockDB.Close()
				mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(&datastore.DB{DB: replica2MockDB}, nil)
			},
		},
		{
			name:       "multiple replica connections fail",
			primaryDSN: primaryDSN,
			mockExpectFunc: func() {
				primaryMockDB, _, err := sqlmock.New()
				require.NoError(t, err)
				defer primaryMockDB.Close()
				mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil)
				mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(nil, fmt.Errorf("failed to open replica 1"))
				mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(nil, fmt.Errorf("failed to open replica 2"))
			},
		},
		{
			name:       "primary and replica connections fail",
			primaryDSN: failPrimaryDSN,
			mockExpectFunc: func() {
				mockConnector.EXPECT().Open(gomock.Any(), failPrimaryDSN, gomock.Any()).Return(nil, fmt.Errorf("primary connection failed"))
			},
			expectedErr: "failed to open primary database connection: primary connection failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockExpectFunc()

			lb, err := datastore.NewDBLoadBalancer(
				context.Background(),
				tt.primaryDSN,
				datastore.WithConnector(mockConnector),
				datastore.WithServiceDiscovery(mockResolver),
			)
			if tt.expectedErr != "" {
				require.Nil(t, lb)
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NotNil(t, lb)
				require.NoError(t, err)
			}
		})
	}
}

func TestNewDBLoadBalancer_WithoutOptions(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Mock the expected connections
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil)

	lb, err := datastore.NewDBLoadBalancer(context.Background(), primaryDSN, datastore.WithConnector(mockConnector))
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Verify primary (ok) and replicas (none)
	require.Equal(t, primaryMockDB, lb.Primary().DB)
	require.Empty(t, lb.Replicas())
	require.NoError(t, primaryMock.ExpectationsWereMet())
}

func TestNewDBLoadBalancer_WithBothHostsAndDiscoveryOptions(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	// Set expected DNS lookups (service discovery should take precedence over fixed hosts)
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
		}, nil)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv2.example.com").
		Return([]string{"192.168.1.2"}, nil)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, replicaMock1, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, replicaMock2, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	replica2DSN := &datastore.DSN{
		Host:     "192.168.1.2",
		Port:     6433,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	// Expect connections to primary and replicas
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(&datastore.DB{DB: replica1MockDB}, nil)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(&datastore.DB{DB: replica2MockDB}, nil)

	// Create the load balancer with both service discovery and fixed hosts options
	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
		// Use different hosts than those on the DSNs used for the mock expectations to guarantee that this test will
		// fail if precedence of service discovery is not observed
		datastore.WithFixedHosts([]string{"foo.example.com", "bar.example.com"}),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	ctx := context.Background()

	// Verify primary
	require.Equal(t, primaryMockDB, lb.Primary().DB)

	// Verify replicas round-robin rotation
	require.Equal(t, replica1MockDB, lb.Replica(ctx).DB)
	require.Equal(t, replica2MockDB, lb.Replica(ctx).DB)
	require.Equal(t, replica1MockDB, lb.Replica(ctx).DB)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock1.ExpectationsWereMet())
	require.NoError(t, replicaMock2.ExpectationsWereMet())
}

func TestNewDBLoadBalancer_MetricsCollection_Primary(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	tests := []struct {
		name                  string
		enableMetrics         bool
		openPrimarySucceeds   bool
		wantMetricsRegistered bool
	}{
		{
			name:                  "metrics collection disabled",
			enableMetrics:         false,
			openPrimarySucceeds:   true,
			wantMetricsRegistered: false,
		},
		{
			name:                  "metrics collection enabled, primary connection succeeds",
			enableMetrics:         true,
			openPrimarySucceeds:   true,
			wantMetricsRegistered: true,
		},
		{
			name:                  "metrics collection enabled, primary connection fails",
			enableMetrics:         true,
			openPrimarySucceeds:   false,
			wantMetricsRegistered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
				DoAndReturn(func(_ context.Context, _ *datastore.DSN, _ ...datastore.Option) (*datastore.DB, error) {
					if tt.openPrimarySucceeds {
						return &datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil
					}
					return nil, errors.New("failed to open connection")
				}).Times(1)

			reg := prometheus.NewRegistry()

			options := []datastore.Option{
				datastore.WithConnector(mockConnector),
				datastore.WithPrometheusRegisterer(reg),
			}
			if tt.enableMetrics {
				options = append(options, datastore.WithMetricsCollection())
			}

			lb, err := datastore.NewDBLoadBalancer(ctx, primaryDSN, options...)
			if tt.openPrimarySucceeds {
				require.NoError(t, err)
				require.NotNil(t, lb)
			} else {
				require.Error(t, err)
				require.Nil(t, lb)
			}

			// verify registered metrics
			metricCount, err := testutil.GatherAndCount(reg)
			require.NoError(t, err)

			if tt.wantMetricsRegistered {
				require.NotZero(t, metricCount)
				// verify that custom labels were added to all metrics
				metrics, err := reg.Gather()
				require.NoError(t, err)

				var hostTypeLabelFound, hostAddrLabelFound bool
				for _, m := range metrics {
					for _, metric := range m.GetMetric() {
						for _, label := range metric.GetLabel() {
							switch label.GetName() {
							case "host_type":
								hostTypeLabelFound = true
								require.Equal(t, datastore.HostTypePrimary, label.GetValue())
							case "host_addr":
								hostAddrLabelFound = true
								require.Equal(t, lb.Primary().Address(), label.GetValue())
							}
						}
					}
				}
				require.True(t, hostTypeLabelFound)
				require.True(t, hostAddrLabelFound)
			} else {
				require.Zero(t, metricCount)
			}
		})
	}
}

func TestNewDBLoadBalancer_MetricsCollection_Replicas(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	replica1DSN := &datastore.DSN{
		Host:     "replica1",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	replica2DSN := &datastore.DSN{
		Host:     "replica2",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	replicaMockDB1, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaMockDB1.Close()

	replicaMockDB2, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaMockDB2.Close()

	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	tests := []struct {
		name                         string
		enableMetrics                bool
		openReplica1Succeeds         bool
		openReplica2Succeeds         bool
		wantReplicaMetricsRegistered bool
	}{
		{
			name:                         "metrics collection disabled",
			enableMetrics:                false,
			openReplica1Succeeds:         true,
			openReplica2Succeeds:         true,
			wantReplicaMetricsRegistered: false,
		},
		{
			name:                         "metrics collection enabled, all replicas succeed",
			enableMetrics:                true,
			openReplica1Succeeds:         true,
			openReplica2Succeeds:         true,
			wantReplicaMetricsRegistered: true,
		},
		{
			name:                         "metrics collection enabled, one replica fails",
			enableMetrics:                true,
			openReplica1Succeeds:         true,
			openReplica2Succeeds:         false,
			wantReplicaMetricsRegistered: true,
		},
		{
			name:                         "metrics collection enabled, all replicas fail",
			enableMetrics:                true,
			openReplica1Succeeds:         false,
			openReplica2Succeeds:         false,
			wantReplicaMetricsRegistered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()

			// Mock connections according to expectations
			mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
				Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)
			mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).
				DoAndReturn(func(_ context.Context, _ *datastore.DSN, _ ...datastore.Option) (*datastore.DB, error) {
					if tt.openReplica1Succeeds {
						return &datastore.DB{DB: replicaMockDB1, DSN: replica1DSN}, nil
					}
					return nil, errors.New("failed to open replica 1 connection")
				}).Times(1)
			mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).
				DoAndReturn(func(_ context.Context, _ *datastore.DSN, _ ...datastore.Option) (*datastore.DB, error) {
					if tt.openReplica2Succeeds {
						return &datastore.DB{DB: replicaMockDB2, DSN: replica2DSN}, nil
					}
					return nil, errors.New("failed to open replica 2 connection")
				}).Times(1)

			// Initialize load balancer
			options := []datastore.Option{
				datastore.WithConnector(mockConnector),
				datastore.WithPrometheusRegisterer(reg),
				datastore.WithFixedHosts([]string{"replica1", "replica2"}),
			}
			if tt.enableMetrics {
				options = append(options, datastore.WithMetricsCollection())
			}

			_, err := datastore.NewDBLoadBalancer(ctx, primaryDSN, options...)
			require.NoError(t, err)

			// Verify registered metrics
			metricCount, err := testutil.GatherAndCount(reg)
			require.NoError(t, err)

			metrics, err := reg.Gather()
			require.NoError(t, err)

			if tt.enableMetrics {
				require.NotZero(t, metricCount)
			} else {
				require.Zero(t, metricCount)
			}

			// Scan all registered metrics and keep track of which labels are found
			var (
				primaryLabelsFound,
				replica1LabelsFound,
				replica2LabelsFound bool
			)

			for _, m := range metrics {
				for _, metric := range m.GetMetric() {
					var hostType, hostAddr string
					for _, label := range metric.GetLabel() {
						switch label.GetName() {
						case "host_type":
							hostType = label.GetValue()
						case "host_addr":
							hostAddr = label.GetValue()
						}
					}
					// Check for primary labels
					if hostType == datastore.HostTypePrimary {
						primaryLabelsFound = true
						require.Equal(t, primaryDSN.Address(), hostAddr)
					}
					// Check for replica labels
					if hostType == datastore.HostTypeReplica {
						if hostAddr == replica1DSN.Address() {
							replica1LabelsFound = true
						}
						if hostAddr == replica2DSN.Address() {
							replica2LabelsFound = true
						}
					}
				}
			}

			require.Equal(t, tt.enableMetrics, primaryLabelsFound)
			if tt.wantReplicaMetricsRegistered {
				require.Equal(t, tt.openReplica1Succeeds, replica1LabelsFound)
				require.Equal(t, tt.openReplica2Succeeds, replica2LabelsFound)
			} else {
				require.False(t, replica1LabelsFound)
				require.False(t, replica2LabelsFound)
			}
		})
	}
}

func TestDBLoadBalancer_ResolveReplicas(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, replica1Mock, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, replica2Mock, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	replica3MockDB, replica3Mock, err := sqlmock.New()
	require.NoError(t, err)
	defer replica3MockDB.Close()

	// Initial DNS resolver response, with replica 1 and 2
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
		}, nil).Times(1)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).Times(1)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv2.example.com").
		Return([]string{"192.168.1.2"}, nil).Times(1)

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}
	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica2DSN := &datastore.DSN{
		Host:     "192.168.1.2",
		Port:     6433,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica3DSN := &datastore.DSN{
		Host:     "192.168.1.3",
		Port:     6434,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	// Expect initial connection attempts to primary, replica 1 and 2
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica1MockDB, DSN: replica1DSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica2MockDB, DSN: replica2DSN}, nil).Times(1)

	// Create the load balancer with the required options
	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Verify initial replicas
	replicas := lb.Replicas()
	require.Len(t, replicas, 2)
	require.Equal(t, replica1MockDB, replicas[0].DB)
	require.Equal(t, replica2MockDB, replicas[1].DB)

	// DNS resolver response on refresh, with replica 1 and 3 (2 is gone)
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv3.example.com", Port: 6434},
		}, nil).Times(1)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).Times(1)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv3.example.com").
		Return([]string{"192.168.1.3"}, nil).Times(1)

	// Expect new connection attempts to replica 3 (not 1, which was already open, neither 2, which is gone)
	mockConnector.EXPECT().Open(gomock.Any(), replica3DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica3MockDB, DSN: replica3DSN}, nil).Times(1)
	replica2Mock.ExpectClose()

	err = lb.ResolveReplicas(context.Background())
	require.NoError(t, err)

	// Verify updated replicas
	replicas = lb.Replicas()
	require.Len(t, replicas, 2)
	require.Equal(t, replica1MockDB, replicas[0].DB)
	require.Equal(t, replica3MockDB, replicas[1].DB)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replica1Mock.ExpectationsWereMet())
	require.NoError(t, replica2Mock.ExpectationsWereMet())
	require.NoError(t, replica3Mock.ExpectationsWereMet())
}

func TestDBLoadBalancer_ResolveReplicas_PartialFail(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	// Mock the expected DNS lookups
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
		}, nil).
		Times(2)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).
		Times(2)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv2.example.com").
		Return([]string{"192.168.1.2"}, nil).
		Times(2)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}
	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica2DSN := &datastore.DSN{
		Host:     "192.168.1.2",
		Port:     6433,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	// Simulate successful connections to primary and all replicas
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(&datastore.DB{DB: replica1MockDB}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(&datastore.DB{DB: replica2MockDB}, nil).Times(1)

	// Create the load balancer with the service discovery option
	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Simulate successful connection for replica 1 and failure for replica 2
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(&datastore.DB{DB: replica1MockDB}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(nil, errors.New("failed to open replica 2")).Times(1)

	err = lb.ResolveReplicas(context.Background())
	require.Error(t, err)

	var errs *multierror.Error
	require.ErrorAs(t, err, &errs)
	require.Len(t, errs.Errors, 1)
	require.EqualError(t, errs.Errors[0], `failed to open replica "192.168.1.2:6433" database connection: failed to open replica 2`)

	// Ensure that there is only one replica in the pool, and that's replica 1
	replicas := lb.Replicas()
	require.Len(t, replicas, 1)
	require.Equal(t, replica1MockDB, replicas[0].DB)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
}

func TestDBLoadBalancer_ResolveReplicas_AllFail(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	// Mock the expected DNS lookups
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
		}, nil).
		Times(2)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).
		Times(2)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv2.example.com").
		Return([]string{"192.168.1.2"}, nil).
		Times(2)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, replica1Mock, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, replica2Mock, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}
	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica2DSN := &datastore.DSN{
		Host:     "192.168.1.2",
		Port:     6433,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	// Simulate successful connections to primary and all replicas
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(&datastore.DB{DB: replica1MockDB}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(&datastore.DB{DB: replica2MockDB}, nil).Times(1)

	// Create the load balancer with the service discovery option
	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Simulate failed connections to all replicas
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(nil, errors.New("failed to open replica 1")).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(nil, errors.New("failed to open replica 2")).Times(1)
	replica1Mock.ExpectClose()
	replica2Mock.ExpectClose()

	err = lb.ResolveReplicas(context.Background())
	require.Error(t, err)

	var errs *multierror.Error
	require.ErrorAs(t, err, &errs)
	require.Len(t, errs.Errors, 2)
	require.EqualError(t, errs.Errors[0], `failed to open replica "192.168.1.1:6432" database connection: failed to open replica 1`)
	require.EqualError(t, errs.Errors[1], `failed to open replica "192.168.1.2:6433" database connection: failed to open replica 2`)

	// Ensure that there are no replicas in the pool
	require.Empty(t, lb.Replicas())

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replica1Mock.ExpectationsWereMet())
	require.NoError(t, replica2Mock.ExpectationsWereMet())
}

func TestDBLoadBalancer_ResolveReplicas_CloseRemoved(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	// Mock the initial expected DNS lookups
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
		}, nil).
		Times(1)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).
		Times(2)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv2.example.com").
		Return([]string{"192.168.1.2"}, nil).
		Times(1)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, replica1Mock, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, replica2Mock, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}
	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica2DSN := &datastore.DSN{
		Host:     "192.168.1.2",
		Port:     6433,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	// Simulate successful connections to primary and all replicas
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica1MockDB, DSN: replica1DSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica2MockDB, DSN: replica2DSN}, nil).Times(1)

	// Create the load balancer with the service discovery option
	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Mock the next DNS lookup, where replica 2 is gone
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
		}, nil).
		Times(1)

	// Ensure replica 2 connection handle is removed from the pool AND closed
	replica2Mock.ExpectClose()
	err = lb.ResolveReplicas(context.Background())
	require.NoError(t, err)

	replicas := lb.Replicas()
	require.Len(t, replicas, 1)
	require.Equal(t, replica1MockDB, replicas[0].DB)

	// Repeat with replica 1 gone but this time simulate a close error to make sure it's handled properly
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{}, nil).
		Times(1)

	fakeErr := errors.New("foo")
	replica1Mock.ExpectClose().WillReturnError(fakeErr)
	err = lb.ResolveReplicas(context.Background())
	require.ErrorIs(t, err, fakeErr)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replica1Mock.ExpectationsWereMet())
	require.NoError(t, replica2Mock.ExpectationsWereMet())
}

func TestDBLoadBalancer_ResolveReplicas_MetricsCollection_PoolUnchanged(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
		}, nil).Times(2)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).Times(2)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv2.example.com").
		Return([]string{"192.168.1.2"}, nil).Times(2)

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}
	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica2DSN := &datastore.DSN{
		Host:     "192.168.1.2",
		Port:     6433,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	// Expect initial connection attempts to primary, replica 1 and 2
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica1MockDB, DSN: replica1DSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica2MockDB, DSN: replica2DSN}, nil).Times(1)

	// Initialize load balancer (metrics collection during the initial call to ResolveReplicas within NewDBLoadBalancer
	// is already tested in TestNewDBLoadBalancer_MetricsCollection_*).
	reg := prometheus.NewRegistry()
	options := []datastore.Option{
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
		datastore.WithPrometheusRegisterer(reg),
		datastore.WithMetricsCollection(),
	}

	ctx := context.Background()
	lb, err := datastore.NewDBLoadBalancer(ctx, primaryDSN, options...)
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Simulate scenario where nothing has changed, so we should still see metrics labeled for primary, replica 1 and 2
	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err)

	// Verify registered metrics
	metricCount, err := testutil.GatherAndCount(reg)
	require.NoError(t, err)

	metrics, err := reg.Gather()
	require.NoError(t, err)
	require.NotZero(t, metricCount)

	// Track which labels are found during the scan
	var (
		primaryLabelsFound,
		replica1LabelsFound,
		replica2LabelsFound bool
	)

	for _, m := range metrics {
		for _, metric := range m.GetMetric() {
			var hostType, hostAddr string
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "host_type":
					hostType = label.GetValue()
				case "host_addr":
					hostAddr = label.GetValue()
				}
			}
			// Check for primary metrics
			if hostType == datastore.HostTypePrimary {
				primaryLabelsFound = true
				require.Equal(t, primaryDSN.Address(), hostAddr)
			}
			// Check for replica metrics
			if hostType == datastore.HostTypeReplica {
				if hostAddr == replica1DSN.Address() {
					replica1LabelsFound = true
				}
				if hostAddr == replica2DSN.Address() {
					replica2LabelsFound = true
				}
			}
		}
	}

	// Verify presence of labeled metrics
	require.True(t, primaryLabelsFound)
	require.True(t, replica1LabelsFound)
	require.True(t, replica2LabelsFound)
}

func TestDBLoadBalancer_ResolveReplicas_MetricsCollection_ReplicaRemoved(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, replica2Mock, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	// Initial DNS resolver response, with replica 1 and 2
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
		}, nil).Times(1)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).AnyTimes()

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv2.example.com").
		Return([]string{"192.168.1.2"}, nil).AnyTimes()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}
	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica2DSN := &datastore.DSN{
		Host:     "192.168.1.2",
		Port:     6433,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	// Expect initial connection attempts to primary, replica 1 and 2
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica1MockDB, DSN: replica1DSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica2MockDB, DSN: replica2DSN}, nil).Times(1)

	// Initialize load balancer (metrics collection during the initial call to ResolveReplicas within NewDBLoadBalancer
	// is already tested in TestNewDBLoadBalancer_MetricsCollection_*).
	reg := prometheus.NewRegistry()
	options := []datastore.Option{
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
		datastore.WithPrometheusRegisterer(reg),
		datastore.WithMetricsCollection(),
	}

	ctx := context.Background()
	lb, err := datastore.NewDBLoadBalancer(ctx, primaryDSN, options...)
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Simulate the scenario where a replica (2) goes missing and the corresponding collector should be unregistered
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
		}, nil).Times(1)
	replica2Mock.ExpectClose()

	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err)

	// Verify registered metrics
	metricCount, err := testutil.GatherAndCount(reg)
	require.NoError(t, err)

	metrics, err := reg.Gather()
	require.NoError(t, err)
	require.NotZero(t, metricCount)

	// Track which labels are found during the scan
	var (
		primaryLabelsFound,
		replica1LabelsFound,
		replica2LabelsFound bool
	)

	for _, m := range metrics {
		for _, metric := range m.GetMetric() {
			var hostType, hostAddr string
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "host_type":
					hostType = label.GetValue()
				case "host_addr":
					hostAddr = label.GetValue()
				}
			}
			// Check for primary metrics
			if hostType == datastore.HostTypePrimary {
				primaryLabelsFound = true
				require.Equal(t, primaryDSN.Address(), hostAddr)
			}
			// Check for replica metrics
			if hostType == datastore.HostTypeReplica {
				if hostAddr == replica1DSN.Address() {
					replica1LabelsFound = true
				}
				if hostAddr == replica2DSN.Address() {
					replica2LabelsFound = true
				}
			}
		}
	}

	// Verify presence of labeled metrics
	require.True(t, primaryLabelsFound)
	require.True(t, replica1LabelsFound)
	require.False(t, replica2LabelsFound)
}

func TestDBLoadBalancer_ResolveReplicas_MetricsCollection_ReplicaAdded(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	replica3MockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica3MockDB.Close()

	// Initial DNS resolver response, with replica 1 and 2
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
		}, nil).Times(1)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).AnyTimes()

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv2.example.com").
		Return([]string{"192.168.1.2"}, nil).AnyTimes()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}
	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica2DSN := &datastore.DSN{
		Host:     "192.168.1.2",
		Port:     6433,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica3DSN := &datastore.DSN{
		Host:     "192.168.1.3",
		Port:     6434,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	// Expect initial connection attempts to primary, replica 1 and 2
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica1MockDB, DSN: replica1DSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica2MockDB, DSN: replica2DSN}, nil).Times(1)

	// Initialize load balancer (metrics collection during the initial call to ResolveReplicas within NewDBLoadBalancer
	// is already tested in TestNewDBLoadBalancer_MetricsCollection_*).
	reg := prometheus.NewRegistry()
	options := []datastore.Option{
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
		datastore.WithPrometheusRegisterer(reg),
		datastore.WithMetricsCollection(),
	}

	ctx := context.Background()
	lb, err := datastore.NewDBLoadBalancer(ctx, primaryDSN, options...)
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Simulate the scenario where a replica (3) is introduced and the corresponding collector should be registered
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
			{Target: "srv3.example.com", Port: 6434},
		}, nil).Times(1)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv3.example.com").
		Return([]string{"192.168.1.3"}, nil).Times(1)

	mockConnector.EXPECT().Open(gomock.Any(), replica3DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica3MockDB, DSN: replica3DSN}, nil).Times(1)

	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err)

	// Verify registered metrics
	metricCount, err := testutil.GatherAndCount(reg)
	require.NoError(t, err)

	metrics, err := reg.Gather()
	require.NoError(t, err)
	require.NotZero(t, metricCount)

	// Track which labels are found during the scan
	var (
		primaryLabelsFound,
		replica1LabelsFound,
		replica2LabelsFound,
		replica3LabelsFound bool
	)

	for _, m := range metrics {
		for _, metric := range m.GetMetric() {
			var hostType, hostAddr string
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "host_type":
					hostType = label.GetValue()
				case "host_addr":
					hostAddr = label.GetValue()
				}
			}
			// Check for primary metrics
			if hostType == datastore.HostTypePrimary {
				primaryLabelsFound = true
				require.Equal(t, primaryDSN.Address(), hostAddr)
			}
			// Check for replica metrics
			if hostType == datastore.HostTypeReplica {
				if hostAddr == replica1DSN.Address() {
					replica1LabelsFound = true
				}
				if hostAddr == replica2DSN.Address() {
					replica2LabelsFound = true
				}
				if hostAddr == replica3DSN.Address() {
					replica3LabelsFound = true
				}
			}
		}
	}

	// Verify presence of labeled metrics
	require.True(t, primaryLabelsFound)
	require.True(t, replica1LabelsFound)
	require.True(t, replica2LabelsFound)
	require.True(t, replica3LabelsFound)
}

func TestDBLoadBalancer_ResolveReplicas_MetricsCollection_ReplicaReAdded(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, replica2Mock, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	// Initial DNS resolver response, with replica 1 and 2
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
		}, nil).Times(1)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).AnyTimes()

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv2.example.com").
		Return([]string{"192.168.1.2"}, nil).AnyTimes()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}
	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica2DSN := &datastore.DSN{
		Host:     "192.168.1.2",
		Port:     6433,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	// Expect initial connection attempts to primary, replica 1 and 2
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica1MockDB, DSN: replica1DSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica2MockDB, DSN: replica2DSN}, nil).Times(2)

	// Initialize load balancer (metrics collection during the initial call to ResolveReplicas within NewDBLoadBalancer
	// is already tested in TestNewDBLoadBalancer_MetricsCollection_*).
	reg := prometheus.NewRegistry()
	options := []datastore.Option{
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
		datastore.WithPrometheusRegisterer(reg),
		datastore.WithMetricsCollection(),
	}

	ctx := context.Background()
	lb, err := datastore.NewDBLoadBalancer(ctx, primaryDSN, options...)
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Simulate the scenario where a replica (2) goes missing temporarily but then gets re-added on the next refresh
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
		}, nil).Times(1)
	replica2Mock.ExpectClose()

	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err)

	// re-add replica 2
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
		}, nil).Times(1)

	err = lb.ResolveReplicas(ctx)
	require.NoError(t, err)

	// Verify registered metrics
	metricCount, err := testutil.GatherAndCount(reg)
	require.NoError(t, err)

	metrics, err := reg.Gather()
	require.NoError(t, err)
	require.NotZero(t, metricCount)

	// Track which labels are found during the scan
	var (
		primaryLabelsFound,
		replica1LabelsFound,
		replica2LabelsFound bool
	)

	for _, m := range metrics {
		for _, metric := range m.GetMetric() {
			var hostType, hostAddr string
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "host_type":
					hostType = label.GetValue()
				case "host_addr":
					hostAddr = label.GetValue()
				}
			}
			// Check for primary metrics
			if hostType == datastore.HostTypePrimary {
				primaryLabelsFound = true
				require.Equal(t, primaryDSN.Address(), hostAddr)
			}
			// Check for replica metrics
			if hostType == datastore.HostTypeReplica {
				if hostAddr == replica1DSN.Address() {
					replica1LabelsFound = true
				}
				if hostAddr == replica2DSN.Address() {
					replica2LabelsFound = true
				}
			}
		}
	}

	// Verify presence of labeled metrics
	require.True(t, primaryLabelsFound)
	require.True(t, replica1LabelsFound)
	require.True(t, replica2LabelsFound)
}

func TestDBLoadBalancer_ResolveReplicas_MetricsCollection(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}
	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica2DSN := &datastore.DSN{
		Host:     "192.168.1.2",
		Port:     6433,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica3DSN := &datastore.DSN{
		Host:     "192.168.1.3",
		Port:     6434,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, replica2Mock, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	replica3MockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer replica3MockDB.Close()

	tests := []struct {
		name                     string
		setupMocks               func()
		numberOfResolveCalls     int
		expectedLabelAddrTypeMap map[string]string
		cleanup                  func(t *testing.T)
	}{
		{
			name: "pool unchanged",
			setupMocks: func() {
				// Mock initial (NewDBLoadBalancer) DNS resolver response and the one that follows (ResolveReplicas)
				// to return the exact same replicas
				mockResolver.EXPECT().
					LookupSRV(gomock.Any()).
					Return([]*net.SRV{
						{Target: "srv1.example.com", Port: 6432},
						{Target: "srv2.example.com", Port: 6433},
					}, nil).Times(2)

				mockResolver.EXPECT().
					LookupHost(gomock.Any(), "srv1.example.com").
					Return([]string{"192.168.1.1"}, nil).Times(2)
				mockResolver.EXPECT().
					LookupHost(gomock.Any(), "srv2.example.com").
					Return([]string{"192.168.1.2"}, nil).Times(2)

				// Mock connection attempts
				mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
					Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)
				mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).
					Return(&datastore.DB{DB: replica1MockDB, DSN: replica1DSN}, nil).Times(1)
				mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).
					Return(&datastore.DB{DB: replica2MockDB, DSN: replica2DSN}, nil).Times(1)
			},
			numberOfResolveCalls: 1,
			expectedLabelAddrTypeMap: map[string]string{
				primaryDSN.Address():  datastore.HostTypePrimary,
				replica1DSN.Address(): datastore.HostTypeReplica,
				replica2DSN.Address(): datastore.HostTypeReplica,
			},
		},
		{
			name: "replica removed",
			setupMocks: func() {
				// Mock initial (NewDBLoadBalancer) DNS resolver response with replica 1 and 2
				mockResolver.EXPECT().
					LookupSRV(gomock.Any()).
					Return([]*net.SRV{
						{Target: "srv1.example.com", Port: 6432},
						{Target: "srv2.example.com", Port: 6433},
					}, nil).Times(1)

				mockResolver.EXPECT().
					LookupHost(gomock.Any(), "srv1.example.com").
					Return([]string{"192.168.1.1"}, nil).Times(2)
				mockResolver.EXPECT().
					LookupHost(gomock.Any(), "srv2.example.com").
					Return([]string{"192.168.1.2"}, nil).Times(1)

				// Mock connection attempts
				mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
					Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)
				mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).
					Return(&datastore.DB{DB: replica1MockDB, DSN: replica1DSN}, nil).Times(1)
				mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).
					Return(&datastore.DB{DB: replica2MockDB, DSN: replica2DSN}, nil).Times(1)

				// Mock second (ResolveReplicas) DNS resolver response, with only replica 1
				mockResolver.EXPECT().
					LookupSRV(gomock.Any()).
					Return([]*net.SRV{
						{Target: "srv1.example.com", Port: 6432},
					}, nil).Times(1)
				replica2Mock.ExpectClose()
			},
			numberOfResolveCalls: 1,
			expectedLabelAddrTypeMap: map[string]string{
				primaryDSN.Address():  datastore.HostTypePrimary,
				replica1DSN.Address(): datastore.HostTypeReplica,
			},
			cleanup: func(t *testing.T) {
				// recreate replica 2 connection as it was closed during this test
				replica2MockDB, replica2Mock, err = sqlmock.New()
				require.NoError(t, err)
			},
		},
		{
			name: "replica added",
			setupMocks: func() {
				// Mock initial (NewDBLoadBalancer) DNS resolver response with replica 1 and 2
				mockResolver.EXPECT().
					LookupSRV(gomock.Any()).
					Return([]*net.SRV{
						{Target: "srv1.example.com", Port: 6432},
						{Target: "srv2.example.com", Port: 6433},
					}, nil).Times(1)

				mockResolver.EXPECT().
					LookupHost(gomock.Any(), "srv1.example.com").
					Return([]string{"192.168.1.1"}, nil).Times(2)

				mockResolver.EXPECT().
					LookupHost(gomock.Any(), "srv2.example.com").
					Return([]string{"192.168.1.2"}, nil).Times(2)

				// Setup connection attempts
				mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
					Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)
				mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).
					Return(&datastore.DB{DB: replica1MockDB, DSN: replica1DSN}, nil).Times(1)
				mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).
					Return(&datastore.DB{DB: replica2MockDB, DSN: replica2DSN}, nil).Times(1)

				// Mock second (ResolveReplicas) DNS resolver response, with replica 3 added
				mockResolver.EXPECT().
					LookupSRV(gomock.Any()).
					Return([]*net.SRV{
						{Target: "srv1.example.com", Port: 6432},
						{Target: "srv2.example.com", Port: 6433},
						{Target: "srv3.example.com", Port: 6434},
					}, nil).Times(1)

				mockResolver.EXPECT().
					LookupHost(gomock.Any(), "srv3.example.com").
					Return([]string{"192.168.1.3"}, nil).Times(1)

				mockConnector.EXPECT().Open(gomock.Any(), replica3DSN, gomock.Any()).
					Return(&datastore.DB{DB: replica3MockDB, DSN: replica3DSN}, nil).Times(1)
			},
			numberOfResolveCalls: 1,
			expectedLabelAddrTypeMap: map[string]string{
				primaryDSN.Address():  datastore.HostTypePrimary,
				replica1DSN.Address(): datastore.HostTypeReplica,
				replica2DSN.Address(): datastore.HostTypeReplica,
				replica3DSN.Address(): datastore.HostTypeReplica,
			},
		},
		{
			name: "replica re-added",
			setupMocks: func() {
				// Mock initial (NewDBLoadBalancer) DNS resolver response with replica 1 and 2
				mockResolver.EXPECT().
					LookupSRV(gomock.Any()).
					Return([]*net.SRV{
						{Target: "srv1.example.com", Port: 6432},
						{Target: "srv2.example.com", Port: 6433},
					}, nil).Times(1)

				mockResolver.EXPECT().
					LookupHost(gomock.Any(), "srv1.example.com").
					Return([]string{"192.168.1.1"}, nil).Times(3)

				mockResolver.EXPECT().
					LookupHost(gomock.Any(), "srv2.example.com").
					Return([]string{"192.168.1.2"}, nil).Times(2)

				// Setup connection attempts
				mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
					Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)
				mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).
					Return(&datastore.DB{DB: replica1MockDB, DSN: replica1DSN}, nil).Times(1)
				mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).
					Return(&datastore.DB{DB: replica2MockDB, DSN: replica2DSN}, nil).Times(2)

				// Mock second (ResolveReplicas) DNS resolver response, with replica 2 missing
				mockResolver.EXPECT().
					LookupSRV(gomock.Any()).
					Return([]*net.SRV{
						{Target: "srv1.example.com", Port: 6432},
					}, nil).Times(1)
				replica2Mock.ExpectClose()

				// Mock third (ResolveReplicas) DNS resolver response, with replica 2 re-added
				mockResolver.EXPECT().
					LookupSRV(gomock.Any()).
					Return([]*net.SRV{
						{Target: "srv1.example.com", Port: 6432},
						{Target: "srv2.example.com", Port: 6433},
					}, nil).Times(1)
			},
			numberOfResolveCalls: 2,
			expectedLabelAddrTypeMap: map[string]string{
				primaryDSN.Address():  datastore.HostTypePrimary,
				replica1DSN.Address(): datastore.HostTypeReplica,
				replica2DSN.Address(): datastore.HostTypeReplica,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up mocks
			tt.setupMocks()

			// Initialize load balancer with the test Prometheus registry
			reg := prometheus.NewRegistry()
			options := []datastore.Option{
				datastore.WithConnector(mockConnector),
				datastore.WithServiceDiscovery(mockResolver),
				datastore.WithPrometheusRegisterer(reg),
				datastore.WithMetricsCollection(),
			}

			ctx := context.Background()
			lb, err := datastore.NewDBLoadBalancer(ctx, primaryDSN, options...)
			require.NoError(t, err)
			require.NotNil(t, lb)

			// Resolve replicas
			for i := 0; i < tt.numberOfResolveCalls; i++ {
				err = lb.ResolveReplicas(ctx)
				require.NoError(t, err)
			}

			// Verify registered metrics
			metricCount, err := testutil.GatherAndCount(reg)
			require.NoError(t, err)
			require.NotZero(t, metricCount)

			metrics, err := reg.Gather()
			require.NoError(t, err)

			// Search for relevant matching labels
			labelsFound := map[string]string{}
			for _, m := range metrics {
				for _, metric := range m.GetMetric() {
					var hostType, hostAddr string
					for _, label := range metric.GetLabel() {
						switch label.GetName() {
						case "host_type":
							hostType = label.GetValue()
						case "host_addr":
							hostAddr = label.GetValue()
						}
					}
					// Record found addresses and their host types
					labelsFound[hostAddr] = hostType
				}
			}

			// Verify the presence and correctness of expected host addresses and type labels
			for expectedAddr, expectedType := range tt.expectedLabelAddrTypeMap {
				foundType, found := labelsFound[expectedAddr]
				require.True(t, found)
				require.Equal(t, expectedType, foundType)
			}

			// Ensure there are no extra host type/addr labels beyond the expected ones
			require.Equal(t, len(tt.expectedLabelAddrTypeMap), len(labelsFound))

			// Cleanup before next test
			if tt.cleanup != nil {
				tt.cleanup(t)
			}
		})
	}
}

func TestDBLoadBalancer_StartReplicaChecking(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, replicaMock1, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	replica2MockDB, replicaMock2, err := sqlmock.New()
	require.NoError(t, err)
	defer replica2MockDB.Close()

	replica3MockDB, replicaMock3, err := sqlmock.New()
	require.NoError(t, err)
	defer replica3MockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Initial DNS resolver response, with replica 1 and 2
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv2.example.com", Port: 6433},
		}, nil).Times(1)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).Times(1)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv2.example.com").
		Return([]string{"192.168.1.2"}, nil).Times(1)

	// DNS resolver response after refresh, with replica 1 and 3 (2 is gone)
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
			{Target: "srv3.example.com", Port: 6434},
		}, nil).AnyTimes()

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).AnyTimes()

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv3.example.com").
		Return([]string{"192.168.1.3"}, nil).AnyTimes()

	// Mock connection attempts
	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica2DSN := &datastore.DSN{
		Host:     "192.168.1.2",
		Port:     6433,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	replica3DSN := &datastore.DSN{
		Host:     "192.168.1.3",
		Port:     6434,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(&datastore.DB{DB: replica1MockDB, DSN: replica1DSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(&datastore.DB{DB: replica2MockDB, DSN: replica2DSN}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica3DSN, gomock.Any()).Return(&datastore.DB{DB: replica3MockDB, DSN: replica3DSN}, nil).Times(1)

	// Create the load balancer with the required options
	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
		datastore.WithReplicaCheckInterval(50*time.Millisecond),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	ctx := context.Background()

	// Verify initial replicas
	require.Equal(t, replica1MockDB, lb.Replica(ctx).DB)
	require.Equal(t, replica2MockDB, lb.Replica(ctx).DB)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- lb.StartReplicaChecking(ctx)
	}()

	// Wait just enough time for the replicas to be refreshed once
	time.Sleep(60 * time.Millisecond)

	// Cancel the context to stop the refreshing
	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)

	// Verify new replicas
	require.Equal(t, replica1MockDB, lb.Replica(ctx).DB)
	require.Equal(t, replica3MockDB, lb.Replica(ctx).DB)

	// Wait just enough time to confirm that context cancellation stopped the refresh process. We've set expectations
	// for the amount of times that connections can be established, so any reattempt to do so would lead to a failure.
	time.Sleep(60 * time.Millisecond)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock1.ExpectationsWereMet())
	require.NoError(t, replicaMock2.ExpectationsWereMet())
	require.NoError(t, replicaMock3.ExpectationsWereMet())
}

func TestDBLoadBalancer_StartReplicaChecking_ZeroInterval(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replica1MockDB, replicaMock1, err := sqlmock.New()
	require.NoError(t, err)
	defer replica1MockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Initial DNS resolver response
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
		}, nil).
		Times(1)

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).
		Times(1)

	// Mock connection attempts
	replica1DSN := &datastore.DSN{
		Host:     "192.168.1.1",
		Port:     6432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}
	mockConnector.EXPECT().
		Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB}, nil).
		Times(1)
	mockConnector.EXPECT().
		Open(gomock.Any(), replica1DSN, gomock.Any()).
		Return(&datastore.DB{DB: replica1MockDB}, nil).
		Times(1)

	// Create the load balancer with zero interval
	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
		datastore.WithReplicaCheckInterval(0),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- lb.StartReplicaChecking(ctx)
	}()

	// Wait just enough time to confirm that no connections were attempted in the background. We've set expectations
	// for the amount of times that connections can be established, so any attempt here to do so would lead to a failure.
	time.Sleep(50 * time.Millisecond)

	// Canceling the context should not lead to an error as the execution should have been skipped
	cancel()
	require.NoError(t, <-errCh)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock1.ExpectationsWereMet())
}

func TestDBLoadBalancer_StartReplicaChecking_NoFixedHostsOrServiceDiscovery(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Mock the expected connections
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)

	// Create load balancer without fixed hosts or service discovery
	ctx := context.Background()
	lb, err := datastore.NewDBLoadBalancer(
		ctx,
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithReplicaCheckInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- lb.StartReplicaChecking(ctx)
	}()

	// Wait just enough time to confirm that no connections were attempted in the background. We've set expectations
	// for the amount of times that connections can be established, so any attempt here to do so would lead to a failure.
	time.Sleep(50 * time.Millisecond)

	// Canceling the context should not lead to an error as the execution should have been skipped
	cancel()
	require.NoError(t, <-errCh)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
}

func TestDBLoadBalancer_RecordLSN(t *testing.T) {
	// Setup primary and replica DB mocks
	ctrl := gomock.NewController(t)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()
	replicaMockDB, replicaMock, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaMockDB.Close()

	// Define expected DB connections. The connections' open logic is heavily tested elsewhere, here we only care about
	// the bare minimum setup for testing the record LSN logic, thus the use of gomock.Any()
	ctx := context.Background()
	mockConnector := mocks.NewMockConnector(ctrl)
	gomock.InOrder(
		mockConnector.EXPECT().
			Open(gomock.Any(), gomock.Any(), gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil).Times(1),
		mockConnector.EXPECT().
			Open(gomock.Any(), gomock.Any(), gomock.Any()).Return(&datastore.DB{DB: replicaMockDB}, nil).Times(1),
	)

	// Setup load balancer
	lsnCacheMock := mocks.NewMockRepositoryCache(ctrl)
	lb, err := datastore.NewDBLoadBalancer(
		ctx,
		&datastore.DSN{},
		datastore.WithFixedHosts([]string{"replica"}),
		datastore.WithConnector(mockConnector),
		datastore.WithLSNCache(lsnCacheMock),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)
	require.Equal(t, primaryMockDB, lb.Primary().DB)

	// Test LSN recording, ensuring that the LSN is queried on primary and then stored in cache
	lsn := "0/16B3748"
	repo := &models.Repository{Path: "gitlab-org/gitlab"}

	primaryMock.ExpectQuery("SELECT pg_current_wal_insert_lsn()").
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_insert_lsn"}).AddRow(lsn))
	lsnCacheMock.EXPECT().SetLSN(ctx, repo, lsn).Return(nil).Times(1)

	err = lb.RecordLSN(ctx, repo)
	require.NoError(t, err)

	// Verify DB mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock.ExpectationsWereMet())
}

func TestDBLoadBalancer_RecordLSN_QueryError(t *testing.T) {
	// Setup primary and replica DB mocks
	ctrl := gomock.NewController(t)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()
	replicaMockDB, replicaMock, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaMockDB.Close()

	// Define expected DB connections. The connections' open logic is heavily tested elsewhere, here we only care about
	// the bare minimum setup for testing the record LSN logic, thus the use of gomock.Any()
	ctx := context.Background()
	mockConnector := mocks.NewMockConnector(ctrl)
	gomock.InOrder(
		mockConnector.EXPECT().
			Open(gomock.Any(), gomock.Any(), gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil).Times(1),
		mockConnector.EXPECT().
			Open(gomock.Any(), gomock.Any(), gomock.Any()).Return(&datastore.DB{DB: replicaMockDB}, nil).Times(1),
	)

	// Setup load balancer
	lsnCacheMock := mocks.NewMockRepositoryCache(ctrl)
	lb, err := datastore.NewDBLoadBalancer(
		ctx,
		&datastore.DSN{},
		datastore.WithFixedHosts([]string{"replica"}),
		datastore.WithConnector(mockConnector),
		datastore.WithLSNCache(lsnCacheMock),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)
	require.Equal(t, primaryMockDB, lb.Primary().DB)

	// Test LSN recording, ensuring that the LSN is queried on primary and results in an error, with no subsequent calls
	// against the LSN store.
	repo := &models.Repository{Path: "gitlab-org/gitlab"}
	primaryMock.ExpectQuery("SELECT pg_current_wal_insert_lsn()").
		WillReturnError(errors.New("some error"))

	err = lb.RecordLSN(ctx, repo)
	require.EqualError(t, err, "failed to query current WAL insert LSN: some error")

	// Verify DB mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock.ExpectationsWereMet())
}

func TestDBLoadBalancer_RecordLSN_StoreSetError(t *testing.T) {
	// Setup primary and replica DB mocks
	ctrl := gomock.NewController(t)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()
	replicaMockDB, replicaMock, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaMockDB.Close()

	// Define expected DB connections. The connections' open logic is heavily tested elsewhere, here we only care about
	// the bare minimum setup for testing the record LSN logic, thus the use of gomock.Any()
	ctx := context.Background()
	mockConnector := mocks.NewMockConnector(ctrl)
	gomock.InOrder(
		mockConnector.EXPECT().
			Open(gomock.Any(), gomock.Any(), gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil).Times(1),
		mockConnector.EXPECT().
			Open(gomock.Any(), gomock.Any(), gomock.Any()).Return(&datastore.DB{DB: replicaMockDB}, nil).Times(1),
	)

	// Setup load balancer
	lsnCacheMock := mocks.NewMockRepositoryCache(ctrl)
	lb, err := datastore.NewDBLoadBalancer(
		ctx,
		&datastore.DSN{},
		datastore.WithFixedHosts([]string{"replica"}),
		datastore.WithConnector(mockConnector),
		datastore.WithLSNCache(lsnCacheMock),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)
	require.Equal(t, primaryMockDB, lb.Primary().DB)

	// Test LSN recording, ensuring that the LSN is queried on primary and then stored in cache, which yields an error
	lsn := "0/16B3748"
	repo := &models.Repository{Path: "gitlab-org/gitlab"}

	primaryMock.ExpectQuery("SELECT pg_current_wal_insert_lsn()").
		WillReturnRows(sqlmock.NewRows([]string{"pg_current_wal_insert_lsn"}).AddRow(lsn))
	lsnCacheMock.EXPECT().SetLSN(ctx, repo, lsn).Return(errors.New("some error")).Times(1)

	err = lb.RecordLSN(ctx, repo)
	require.EqualError(t, err, "failed to cache WAL insert LSN: some error")

	// Verify DB mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock.ExpectationsWereMet())
}

// expectSingleRowQuery asserts that a query on the mock database returns a single row with the specified value for the
// given column, or returns an error if specified. It takes the mock database, the query string, the response row column
// name, the expected response row column value, an error (if any), and the query arguments as input.
func expectSingleRowQuery(db sqlmock.Sqlmock, query, column string, value driver.Value, err error, args ...driver.Value) {
	if err != nil {
		db.ExpectQuery(query).
			WithArgs(args...).
			WillReturnError(err)
	} else {
		db.ExpectQuery(query).
			WithArgs(args...).
			WillReturnRows(sqlmock.NewRows([]string{column}).AddRow(value))
	}
}

func TestDBLoadBalancer_UpToDateReplica(t *testing.T) {
	ctrl := gomock.NewController(t)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replicaMockDB, replicaMock, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaMockDB.Close()

	ctx := context.Background()
	mockConnector := mocks.NewMockConnector(ctrl)
	lsnCacheMock := mocks.NewMockRepositoryCache(ctrl)

	// Define expected DB connections. The connections' open logic is heavily tested elsewhere, here we only care about
	// the bare minimum setup for testing the LSN logic, thus the use of gomock.Any()
	gomock.InOrder(
		mockConnector.EXPECT().
			Open(gomock.Any(), gomock.Any(), gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil).Times(1),
		mockConnector.EXPECT().
			Open(gomock.Any(), gomock.Any(), gomock.Any()).Return(&datastore.DB{DB: replicaMockDB}, nil).Times(1),
	)

	// Setup load balancer
	lb, err := datastore.NewDBLoadBalancer(
		ctx,
		&datastore.DSN{},
		datastore.WithFixedHosts([]string{"replica"}),
		datastore.WithConnector(mockConnector),
		datastore.WithLSNCache(lsnCacheMock),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)
	require.Equal(t, primaryMockDB, lb.Primary().DB)
	require.Len(t, lb.Replicas(), 1)
	require.Equal(t, replicaMockDB, lb.Replica(ctx).DB)

	repo := &models.Repository{Path: "test/repo"}
	primaryLSN := "0/16B3748"
	query := "SELECT pg_last_wal_replay_lsn (.+) SELECT pg_wal_lsn_diff"
	column := "pg_wal_lsn_diff"

	tests := []struct {
		name         string
		getLSNReturn string
		getLSNError  error
		queryResult  driver.Value
		queryError   error
		expectedDB   *sql.DB
	}{
		{
			name:         "LSN record exists and replica candidate is up-to-date",
			getLSNReturn: primaryLSN,
			queryResult:  true,
			expectedDB:   replicaMockDB,
		},
		{
			name:         "LSN record exists and replica candidate is not up-to-date",
			getLSNReturn: primaryLSN,
			queryResult:  false,
			expectedDB:   primaryMockDB,
		},
		{
			name:         "LSN record does not exist",
			getLSNReturn: "",
			expectedDB:   replicaMockDB,
		},
		{
			name:         "Query fails",
			getLSNReturn: primaryLSN,
			queryError:   errors.New("database error"),
			expectedDB:   primaryMockDB,
		},
		{
			name:        "LSN cache retrieval fails",
			getLSNError: errors.New("cache error"),
			expectedDB:  primaryMockDB,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lsnCacheMock.EXPECT().GetLSN(gomock.Any(), repo).Return(tt.getLSNReturn, tt.getLSNError).Times(1)
			if tt.getLSNError == nil && tt.getLSNReturn != "" {
				expectSingleRowQuery(replicaMock, query, column, tt.queryResult, tt.queryError, primaryLSN)
			}
			db := lb.UpToDateReplica(ctx, repo)
			require.Equal(t, tt.expectedDB, db.DB)
		})
	}

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock.ExpectationsWereMet())
}

func TestDBLoadBalancer_UpToDateReplica_Inactive(t *testing.T) {
	ctrl := gomock.NewController(t)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	ctx := context.Background()
	mockConnector := mocks.NewMockConnector(ctrl)
	lsnCacheMock := mocks.NewMockRepositoryCache(ctrl)

	// Define expected DB connections (only primary)
	mockConnector.EXPECT().
		Open(gomock.Any(), gomock.Any(), gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil).Times(1)

	// Setup load balancer without WithFixedHosts nor WithServiceDiscovery options
	lb, err := datastore.NewDBLoadBalancer(
		ctx,
		&datastore.DSN{},
		datastore.WithConnector(mockConnector),
		datastore.WithLSNCache(lsnCacheMock),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)
	require.Equal(t, primaryMockDB, lb.Primary().DB)
	require.Empty(t, lb.Replicas())

	// Test that we successfully get the primary handle as result
	repo := &models.Repository{Path: "test/repo"}
	db := lb.UpToDateReplica(ctx, repo)
	require.Equal(t, lb.Primary(), db)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
}

func TestDBLoadBalancer_UpToDateReplica_FallbackToPrimaryOnTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)

	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	replicaMockDB, replicaMock, err := sqlmock.New()
	require.NoError(t, err)
	defer replicaMockDB.Close()

	ctx := context.Background()
	mockConnector := mocks.NewMockConnector(ctrl)
	lsnCacheMock := mocks.NewMockRepositoryCache(ctrl)

	// Define expected DB connections
	gomock.InOrder(
		mockConnector.EXPECT().
			Open(gomock.Any(), gomock.Any(), gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil).Times(1),
		mockConnector.EXPECT().
			Open(gomock.Any(), gomock.Any(), gomock.Any()).Return(&datastore.DB{DB: replicaMockDB}, nil).Times(1),
	)

	// Setup load balancer
	lb, err := datastore.NewDBLoadBalancer(
		ctx,
		&datastore.DSN{},
		datastore.WithFixedHosts([]string{"replica"}),
		datastore.WithConnector(mockConnector),
		datastore.WithLSNCache(lsnCacheMock),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)
	require.Equal(t, primaryMockDB, lb.Primary().DB)
	require.Len(t, lb.Replicas(), 1)
	require.Equal(t, replicaMockDB, lb.Replica(ctx).DB)

	repo := &models.Repository{Path: "test/repo"}
	primaryLSN := "0/16B3748"
	query := "SELECT pg_last_wal_replay_lsn (.+) SELECT pg_wal_lsn_diff"
	column := "pg_wal_lsn_diff"

	// test LSN cache lookup taking too long (over datastore.upToDateReplicaTimeout, i.e. 100ms)
	lsnCacheMock.EXPECT().GetLSN(gomock.Any(), repo).DoAndReturn(func(_ context.Context, _ *models.Repository) (string, error) {
		time.Sleep(110 * time.Millisecond)
		return primaryLSN, nil
	}).Times(1)
	db := lb.UpToDateReplica(ctx, repo)
	require.Equal(t, primaryMockDB, db.DB)

	// test LSN cache lookup being fast, but subsequent LSN DB query taking too long
	lsnCacheMock.EXPECT().GetLSN(gomock.Any(), repo).Return(primaryLSN, nil).Times(1)
	replicaMock.ExpectQuery(query).
		WithArgs(primaryLSN).
		WillDelayFor(110 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{column}).AddRow(driver.Value(false)))
	db = lb.UpToDateReplica(ctx, repo)
	require.Equal(t, primaryMockDB, db.DB)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock.ExpectationsWereMet())
}

func TestDB_Address(t *testing.T) {
	tests := []struct {
		name string
		arg  datastore.DB
		out  string
	}{
		{name: "nil DSN", arg: datastore.DB{}, out: ""},
		{name: "empty DSN", arg: datastore.DB{DSN: &datastore.DSN{}}, out: ":0"},
		{name: "DSN with no port", arg: datastore.DB{DSN: &datastore.DSN{Host: "127.0.0.1"}}, out: "127.0.0.1:0"},
		{name: "DSN with no host", arg: datastore.DB{DSN: &datastore.DSN{Port: 5432}}, out: ":5432"},
		{name: "full DSN", arg: datastore.DB{DSN: &datastore.DSN{Host: "127.0.0.1", Port: 5432}}, out: "127.0.0.1:5432"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.out, tt.arg.Address())
		})
	}
}

func TestQueryBuilder_Build(t *testing.T) {
	testCases := []struct {
		name           string
		query          string
		args           []any
		expectedSQL    string
		expectedParams []any
		expectError    bool
	}{
		{
			name:           "empty query",
			query:          "",
			args:           []any{},
			expectedSQL:    "",
			expectedParams: []any{},
		},
		{
			name:           "single placeholder",
			query:          "SELECT * FROM users WHERE id = ?",
			args:           []any{1},
			expectedSQL:    "SELECT * FROM users WHERE id = $1",
			expectedParams: []any{1},
		},
		{
			name:           "leading and trailing space is removed",
			query:          " SELECT * FROM users WHERE id = ? ",
			args:           []any{1},
			expectedSQL:    "SELECT * FROM users WHERE id = $1",
			expectedParams: []any{1},
		},
		{
			name:           "multiple placeholders",
			query:          "SELECT * FROM users WHERE id = ? AND name = ?",
			args:           []any{1, "John Doe"},
			expectedSQL:    "SELECT * FROM users WHERE id = $1 AND name = $2",
			expectedParams: []any{1, "John Doe"},
		},
		{
			name:           "placeholders with spaces",
			query:          "SELECT * FROM users WHERE id = ? AND name = ?",
			args:           []any{1, "John Doe"},
			expectedSQL:    "SELECT * FROM users WHERE id = $1 AND name = $2",
			expectedParams: []any{1, "John Doe"},
		},
		{
			name:           "query with newline",
			query:          "SELECT * FROM users WHERE id = ?\n",
			args:           []any{1},
			expectedSQL:    "SELECT * FROM users WHERE id = $1",
			expectedParams: []any{1},
		},
		{
			name:           "query without arguments",
			query:          "SELECT * FROM users WHERE id = 10",
			args:           []any{},
			expectedSQL:    "SELECT * FROM users WHERE id = 10",
			expectedParams: []any{},
		},
		{
			name:           "query with multiple newlines",
			query:          "SELECT * FROM users\nWHERE id = ?\n",
			args:           []any{1},
			expectedSQL:    "SELECT * FROM users\nWHERE id = $1",
			expectedParams: []any{1},
		},
		{
			name:        "errors on mismatched placeholder count",
			query:       "SELECT * FROM users WHERE id = ? AND name = ?",
			args:        []any{1},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			qb := datastore.NewQueryBuilder()

			err := qb.Build(tc.query, tc.args...)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.Equal(t, tc.expectedSQL, qb.SQL())
				require.Equal(t, tc.expectedParams, qb.Params())
			}
		})
	}
}

func TestQueryBuilder_MultipleBuildCalls(t *testing.T) {
	t.Parallel()
	qb := datastore.NewQueryBuilder()
	qb.Build("SELECT * FROM users WHERE id = ?", 1)
	qb.Build("AND name = ?", "John Doe")
	require.Equal(t, "SELECT * FROM users WHERE id = $1 AND name = $2", qb.SQL())
	require.Equal(t, []any{1, "John Doe"}, qb.Params())
}

func TestQueryBuilder_WrapIntoSubqueryOf(t *testing.T) {
	testCases := []struct {
		name           string
		query          string
		args           []any
		wrapQuery      string
		expectedSQL    string
		expectedParams []any
		expectError    bool
	}{
		{
			name:           "basic subquery",
			query:          "SELECT * FROM users WHERE id = ?",
			args:           []any{1},
			wrapQuery:      "SELECT * FROM orders WHERE user_id IN (%s)",
			expectedSQL:    "SELECT * FROM orders WHERE user_id IN (SELECT * FROM users WHERE id = $1)",
			expectedParams: []any{1},
		},
		{
			name:           "subquery with multiple placeholders",
			query:          "SELECT * FROM users WHERE id = ? AND name = ?",
			args:           []any{1, "John Doe"},
			wrapQuery:      "SELECT * FROM orders WHERE user_id IN (%s)",
			expectedSQL:    "SELECT * FROM orders WHERE user_id IN (SELECT * FROM users WHERE id = $1 AND name = $2)",
			expectedParams: []any{1, "John Doe"},
		},
		{
			name:        "subquery without placeholder",
			query:       "SELECT * FROM users WHERE id = ? AND name = ?",
			args:        []any{1, "John Doe"},
			wrapQuery:   "SELECT * FROM orders WHERE user_id IN ?",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			qb := datastore.NewQueryBuilder()

			err := qb.Build(tc.query, tc.args...)
			require.NoError(t, err)

			err = qb.WrapIntoSubqueryOf(tc.wrapQuery)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.Equal(t, tc.expectedSQL, qb.SQL())
				require.Equal(t, tc.expectedParams, qb.Params())
			}
		})
	}
}

func TestNewDBLoadBalancer_ReplicaResolveTimeout_SRVLookupTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Setup primary DB connection to succeed
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)

	// Mock the SRV lookup during replica resolution to validate context deadline of ~100ms and simulate a
	// context.DeadlineExceeded error returned by the resolver.
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		DoAndReturn(func(ctx context.Context) ([]string, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.WithinDuration(t, time.Now().Add(datastore.InitReplicaResolveTimeout), deadline, datastore.InitReplicaResolveTimeout/10)
			return nil, context.DeadlineExceeded
		}).
		Times(1)

	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
	)

	// Ensure no error on load balancer creation and that the replica list is empty and primary is set
	require.NoError(t, err)
	require.Equal(t, lb.Primary().DB, primaryMockDB)
	require.Empty(t, lb.Replicas())
}

func TestNewDBLoadBalancer_ReplicaResolveTimeout_HostLookupTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Setup primary DB connection to succeed
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)

	// Mock the SRV lookup to succeed
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
		}, nil).
		Times(1)

	// Mock the Host lookup to validate context deadline is ~100ms and simulate context.DeadlineExceeded
	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		DoAndReturn(func(ctx context.Context, host string) ([]string, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.WithinDuration(t, time.Now().Add(datastore.InitReplicaResolveTimeout), deadline, datastore.InitReplicaResolveTimeout/10)
			return nil, context.DeadlineExceeded
		}).
		Times(1)

	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
	)

	// Ensure no error on load balancer creation and that the replica list is empty and primary is set
	require.NoError(t, err)
	require.Equal(t, lb.Primary().DB, primaryMockDB)
	require.Empty(t, lb.Replicas())
}

func TestNewDBLoadBalancer_ReplicaResolveTimeout_ConnectionOpenTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Setup primary DB connection to succeed
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)

	// Mock the SRV lookup to succeed and return one replica target
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
		}, nil).
		Times(1)

	// Mock the Host lookup to succeed, returning a valid IP for the SRV target
	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.10"}, nil).
		Times(1)

	// Mock the connection open attempt for the replica to validate the context deadline and return DeadlineExceeded
	replicaDSN := &datastore.DSN{
		Host:     "192.168.1.10",
		Port:     6432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	mockConnector.EXPECT().Open(gomock.Any(), replicaDSN, gomock.Any()).
		DoAndReturn(func(ctx context.Context, dsn *datastore.DSN, opts ...any) (*datastore.DB, error) {
			// Validate that the context has a deadline and it's ~100ms from now
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.WithinDuration(t, time.Now().Add(datastore.InitReplicaResolveTimeout), deadline, datastore.InitReplicaResolveTimeout/10)
			return nil, context.DeadlineExceeded
		}).
		Times(1)

	// Initialize the load balancer
	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
	)

	// Ensure no error on load balancer creation and that the replica list is empty and primary is set
	require.NoError(t, err)
	require.Equal(t, lb.Primary().DB, primaryMockDB)
	require.Empty(t, lb.Replicas())
}

func TestStartReplicaChecking_ReplicaResolveTimeout_SRVLookupTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Mock successful primary connection
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)

	// Mock failed DNS lookup to interrupt the replica resolution that occurs during NewDBLoadBalancer
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return(nil, errors.New("some error")).
		Times(1)

	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
		datastore.WithReplicaCheckInterval(50*time.Millisecond),
	)
	require.NoError(t, err)

	// Mock the SRV lookup to validate context deadline is ~200ms and simulate context.DeadlineExceeded
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		DoAndReturn(func(ctx context.Context) ([]string, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.WithinDuration(t, time.Now().Add(datastore.ReplicaResolveTimeout), deadline, datastore.ReplicaResolveTimeout/10)
			return nil, context.DeadlineExceeded
		}).
		Times(1)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- lb.StartReplicaChecking(ctx)
	}()

	// Wait just enough time for the replicas to be refreshed once
	time.Sleep(60 * time.Millisecond)

	// Cancel the context to stop the refreshing
	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)

	// Ensure replica list is empty
	require.Empty(t, lb.Replicas())
}

func TestStartReplicaChecking_ReplicaResolveTimeout_HostLookupTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Mock successful primary connection
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)

	// Mock failed DNS lookup to interrupt the replica resolution that occurs during NewDBLoadBalancer
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return(nil, errors.New("some error")).
		Times(1)

	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
		datastore.WithReplicaCheckInterval(50*time.Millisecond),
	)
	require.NoError(t, err)

	// Initial SRV lookup setup to succeed
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
		}, nil).
		Times(1)

	// Mock the Host lookup to validate context deadline is ~200ms and simulate context.DeadlineExceeded
	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		DoAndReturn(func(ctx context.Context, host string) ([]string, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.WithinDuration(t, time.Now().Add(datastore.ReplicaResolveTimeout), deadline, datastore.ReplicaResolveTimeout/10)
			return nil, context.DeadlineExceeded
		}).
		Times(1)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- lb.StartReplicaChecking(ctx)
	}()

	// Wait just enough time for the replicas to be refreshed once
	time.Sleep(60 * time.Millisecond)

	// Cancel the context to stop the refreshing
	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)

	// Ensure replica list is empty
	require.Empty(t, lb.Replicas())
}

func TestStartReplicaChecking_ReplicaResolveTimeout_OpenConnectionTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	primaryMockDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer primaryMockDB.Close()

	primaryDSN := &datastore.DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Mock successful primary connection
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).
		Return(&datastore.DB{DB: primaryMockDB, DSN: primaryDSN}, nil).Times(1)

	// Mock failed DNS lookup to interrupt the replica resolution that occurs during NewDBLoadBalancer
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return(nil, errors.New("some error")).
		Times(1)

	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
		datastore.WithReplicaCheckInterval(50*time.Millisecond),
	)
	require.NoError(t, err)

	// Mock the SRV lookup to succeed and return one replica target
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
		}, nil).
		Times(1)

	// Mock the Host lookup to succeed, returning a valid IP for the SRV target
	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.10"}, nil).
		Times(1)

	// Mock the replica connection open to validate context deadline is ~200ms and simulate context.DeadlineExceeded
	replicaDSN := &datastore.DSN{
		Host:     "192.168.1.10",
		Port:     6432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	mockConnector.EXPECT().Open(gomock.Any(), replicaDSN, gomock.Any()).
		DoAndReturn(func(ctx context.Context, dsn *datastore.DSN, opts ...any) (*datastore.DB, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.WithinDuration(t, time.Now().Add(datastore.ReplicaResolveTimeout), deadline, datastore.ReplicaResolveTimeout/10)
			return nil, context.DeadlineExceeded
		}).
		Times(1)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- lb.StartReplicaChecking(ctx)
	}()

	// Wait just enough time for the replicas to be refreshed once
	time.Sleep(60 * time.Millisecond)

	// Cancel the context to stop the refreshing
	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)

	// Ensure replica list is empty
	require.Empty(t, lb.Replicas())
}
