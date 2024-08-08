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
	defer ctrl.Finish()

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
	defer ctrl.Finish()

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
		expectedErrs []string
	}{
		{
			name:         "primary connection fails",
			primaryDSN:   &datastore.DSN{Host: "fail_primary"},
			replicaHosts: []string{"replica1"},
			expectedErrs: []string{"failed to open primary database connection: primary connection failed"},
		},
		{
			name:       "one replica connection fails",
			primaryDSN: primaryDSN,
			replicaHosts: []string{
				"replica1",
				"fail_replica2",
			},
			expectedErrs: []string{`failed to open replica "fail_replica2:5432" database connection: replica connection failed`},
		},
		{
			name:       "multiple replica connections fail",
			primaryDSN: primaryDSN,
			replicaHosts: []string{
				"fail_replica1",
				"fail_replica2",
			},
			expectedErrs: []string{
				`failed to open replica "fail_replica1:5432" database connection: replica connection failed`,
				`failed to open replica "fail_replica2:5432" database connection: replica connection failed`,
			},
		},
		{
			name:       "primary and replica connections fail",
			primaryDSN: &datastore.DSN{Host: "fail_primary", Port: 1234},
			replicaHosts: []string{
				"fail_replica2",
			},
			expectedErrs: []string{
				`failed to open primary database connection: primary connection failed`,
				`failed to open replica "fail_replica2:1234" database connection: replica connection failed`,
			},
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
			require.Nil(t, lb)

			var errs *multierror.Error
			require.ErrorAs(t, err, &errs)
			require.NotNil(t, errs)
			require.Len(t, errs.Errors, len(tt.expectedErrs))

			for _, expectedErr := range tt.expectedErrs {
				require.Contains(t, errs.Error(), expectedErr)
			}
		})
	}
}

func TestNewDBLoadBalancer_WithServiceDiscovery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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

func TestNewDBLoadBalancer_WithServiceDiscovery_SRVLookupError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	// Mock the expected DNS lookups with an error
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return(nil, fmt.Errorf("DNS SRV lookup error"))

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
	_, err = datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "error resolving DNS SRV record: DNS SRV lookup error")

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
}

func TestNewDBLoadBalancer_WithServiceDiscovery_HostLookupError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResolver := mocks.NewMockDNSResolver(ctrl)
	mockConnector := mocks.NewMockConnector(ctrl)

	// Mock the expected DNS lookups
	mockResolver.EXPECT().
		LookupSRV(gomock.Any()).
		Return([]*net.SRV{
			{Target: "srv1.example.com", Port: 6432},
		}, nil)

	// Mock the expected host lookup with an error
	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return(nil, fmt.Errorf("DNS host lookup error"))

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
	_, err = datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithConnector(mockConnector),
		datastore.WithServiceDiscovery(mockResolver),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, `error resolving host "srv1.example.com" address: DNS host lookup error`)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
}

func TestNewDBLoadBalancer_WithServiceDiscovery_ConnectionError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
		expectedErrors []string
	}{
		{
			name:       "primary connection fails",
			primaryDSN: failPrimaryDSN,
			mockExpectFunc: func() {
				mockConnector.EXPECT().Open(gomock.Any(), failPrimaryDSN, gomock.Any()).Return(nil, fmt.Errorf("primary connection failed"))
				replica1MockDB, _, err := sqlmock.New()
				require.NoError(t, err)
				defer replica1MockDB.Close()
				replica2MockDB, _, err := sqlmock.New()
				require.NoError(t, err)
				defer replica2MockDB.Close()
				mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(&datastore.DB{DB: replica1MockDB}, nil)
				mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(&datastore.DB{DB: replica2MockDB}, nil)
			},
			expectedErrors: []string{"failed to open primary database connection: primary connection failed"},
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
			expectedErrors: []string{`failed to open replica "192.168.1.1:6432" database connection: failed to open replica 1`},
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
			expectedErrors: []string{
				`failed to open replica "192.168.1.1:6432" database connection: failed to open replica 1`,
				`failed to open replica "192.168.1.2:6433" database connection: failed to open replica 2`,
			},
		},
		{
			name:       "primary and replica connections fail",
			primaryDSN: failPrimaryDSN,
			mockExpectFunc: func() {
				mockConnector.EXPECT().Open(gomock.Any(), failPrimaryDSN, gomock.Any()).Return(nil, fmt.Errorf("primary connection failed"))
				mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(nil, fmt.Errorf("failed to open replica 1"))
				mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(nil, fmt.Errorf("failed to open replica 2"))
			},
			expectedErrors: []string{
				`failed to open primary database connection: primary connection failed`,
				`failed to open replica "192.168.1.1:6432" database connection: failed to open replica 1`,
				`failed to open replica "192.168.1.2:6433" database connection: failed to open replica 2`,
			},
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
			require.Error(t, err)
			require.Nil(t, lb)

			var errs *multierror.Error
			require.ErrorAs(t, err, &errs)
			require.NotNil(t, errs)
			require.Len(t, errs.Errors, len(tt.expectedErrors))

			for _, expectedErr := range tt.expectedErrors {
				require.Contains(t, errs.Error(), expectedErr)
			}
		})
	}
}

func TestNewDBLoadBalancer_WithoutOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
	defer ctrl.Finish()

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

func TestDBLoadBalancer_ResolveReplicas(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(&datastore.DB{DB: replica1MockDB}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(&datastore.DB{DB: replica2MockDB}, nil).Times(1)

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
		}, nil).AnyTimes()

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv1.example.com").
		Return([]string{"192.168.1.1"}, nil).AnyTimes()

	mockResolver.EXPECT().
		LookupHost(gomock.Any(), "srv3.example.com").
		Return([]string{"192.168.1.3"}, nil).AnyTimes()

	// Expect new connection attempts to replica 1 and 3
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(&datastore.DB{DB: replica1MockDB}, nil).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica3DSN, gomock.Any()).Return(&datastore.DB{DB: replica3MockDB}, nil).Times(1)

	errs := lb.ResolveReplicas(context.Background())
	require.NoError(t, errs.ErrorOrNil())

	// Verify updated replicas
	replicas = lb.Replicas()
	require.Len(t, replicas, 2)
	require.Equal(t, replica1MockDB, replicas[0].DB)
	require.Equal(t, replica3MockDB, replicas[1].DB)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock1.ExpectationsWereMet())
	require.NoError(t, replicaMock2.ExpectationsWereMet())
	require.NoError(t, replicaMock3.ExpectationsWereMet())
}

func TestDBLoadBalancer_ResolveReplicas_PartialFail(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
	defer ctrl.Finish()

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

	// Simulate failed connections to all replicas
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(nil, errors.New("failed to open replica 1")).Times(1)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(nil, errors.New("failed to open replica 2")).Times(1)

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
}

func TestDBLoadBalancer_StartReplicaChecking(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(&datastore.DB{DB: replica1MockDB, DSN: replica1DSN}, nil).Times(2)
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
	go lb.StartReplicaChecking(ctx)

	// Wait just enough time for the replicas to be refreshed once
	time.Sleep(60 * time.Millisecond)

	// Cancel the context to stop the refreshing
	cancel()

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
	defer ctrl.Finish()

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

	// this should return immediately with no error
	err = lb.StartReplicaChecking(context.Background())
	require.NoError(t, err)

	// Wait just enough time to confirm that no connections were attempted in the background. We've set expectations
	// for the amount of times that connections can be established, so any attempt here to do so would lead to a failure.
	time.Sleep(50 * time.Millisecond)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock1.ExpectationsWereMet())
}

func TestDBLoadBalancer_StartReplicaChecking_NoServiceDiscovery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
	replica1DSN := &datastore.DSN{
		Host:     "replica1",
		Port:     5432,
		User:     primaryDSN.User,
		Password: primaryDSN.Password,
		DBName:   primaryDSN.DBName,
		SSLMode:  primaryDSN.SSLMode,
	}

	// Mock the expected connections
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(&datastore.DB{DB: replica1MockDB}, nil)

	// Create load balancer without service discovery
	lb, err := datastore.NewDBLoadBalancer(
		context.Background(),
		primaryDSN,
		datastore.WithFixedHosts([]string{"replica1"}),
		datastore.WithConnector(mockConnector),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	// this should return immediately with no error
	err = lb.StartReplicaChecking(context.Background())
	require.NoError(t, err)

	// Wait just enough time to confirm that no connections were attempted in the background. We've set expectations
	// for the amount of times that connections can be established, so any attempt here to do so would lead to a failure.
	time.Sleep(50 * time.Millisecond)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock1.ExpectationsWereMet())
}

func TestDBLoadBalancer_RecordLSN(t *testing.T) {
	// Setup primary and replica DB mocks
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
	defer ctrl.Finish()

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
	defer ctrl.Finish()

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
func expectSingleRowQuery(db sqlmock.Sqlmock, query string, column string, value driver.Value, err error, args ...driver.Value) {
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
	defer ctrl.Finish()

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
			lsnCacheMock.EXPECT().GetLSN(ctx, repo).Return(tt.getLSNReturn, tt.getLSNError).Times(1)
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
	t.Run("empty query", func(t *testing.T) {
		qb := datastore.NewQueryBuilder()
		qb.Build("")
		assertSQL(t, qb, "", []any{})
	})

	t.Run("single placeholder", func(t *testing.T) {
		qb := datastore.NewQueryBuilder()
		qb.Build("SELECT * FROM users WHERE id = ?", 1)
		assertSQL(t, qb, "SELECT * FROM users WHERE id = $1", []any{1})
	})

	t.Run("multiple placeholders", func(t *testing.T) {
		qb := datastore.NewQueryBuilder()
		qb.Build("SELECT * FROM users WHERE id = ? AND name = ?", 1, "John Doe")
		assertSQL(t, qb, "SELECT * FROM users WHERE id = $1 AND name = $2", []any{1, "John Doe"})
	})

	t.Run("placeholders with spaces", func(t *testing.T) {
		qb := datastore.NewQueryBuilder()
		qb.Build("SELECT * FROM users WHERE id = ? AND name = ?", 1, "John Doe")
		assertSQL(t, qb, "SELECT * FROM users WHERE id = $1 AND name = $2", []any{1, "John Doe"})
	})

	t.Run("query with newline", func(t *testing.T) {
		qb := datastore.NewQueryBuilder()
		qb.Build("SELECT * FROM users WHERE id = ?\n", 1)
		assertSQL(t, qb, "SELECT * FROM users WHERE id = $1", []any{1})
	})

	t.Run("query with multiple newlines", func(t *testing.T) {
		qb := datastore.NewQueryBuilder()
		qb.Build("SELECT * FROM users\nWHERE id = ?\n", 1)
		assertSQL(t, qb, "SELECT * FROM users\nWHERE id = $1", []any{1})
	})

	t.Run("panic on mismatch placeholder count", func(t *testing.T) {
		qb := datastore.NewQueryBuilder()
		require.Panics(t, func() {
			qb.Build("SELECT * FROM users WHERE id = ? AND name = ?", 1)
		}, "Expected panic, but none occurred")
	})

	t.Run("multiple build calls", func(t *testing.T) {
		qb := datastore.NewQueryBuilder()
		qb.Build("SELECT * FROM users WHERE id = ?", 1)
		qb.Build("AND name = ?", "John Doe")
		assertSQL(t, qb, "SELECT * FROM users WHERE id = $1 AND name = $2", []any{1, "John Doe"})
	})
}

func TestQueryBuilder_WrapIntoSubqueryOf(t *testing.T) {
	t.Run("basic subquery", func(t *testing.T) {
		qb := datastore.NewQueryBuilder()
		qb.Build("SELECT * FROM users WHERE id = ?", 1)
		qb.WrapIntoSubqueryOf("SELECT * FROM orders WHERE user_id IN (%s)")
		assertSQL(t, qb, "SELECT * FROM orders WHERE user_id IN (SELECT * FROM users WHERE id = $1)", []any{1})
	})

	t.Run("subquery with multiple placeholders", func(t *testing.T) {
		qb := datastore.NewQueryBuilder()
		qb.Build("SELECT * FROM users WHERE id = ? AND name = ?", 1, "John Doe")
		qb.WrapIntoSubqueryOf("SELECT * FROM orders WHERE user_id IN (%s)")
		assertSQL(t, qb, "SELECT * FROM orders WHERE user_id IN (SELECT * FROM users WHERE id = $1 AND name = $2)", []any{1, "John Doe"})
	})
}

func assertSQL(t *testing.T, qb *datastore.QueryBuilder, expectedSQL string, expectedParams []any) {
	require.Equal(t, expectedSQL, qb.SQL(), "Expected SQL: %q, got: %q", expectedSQL, qb.SQL())
	require.Equal(t, expectedParams, qb.Params(), "Expected params: %v, got: %v", expectedParams, qb.Params())
}
