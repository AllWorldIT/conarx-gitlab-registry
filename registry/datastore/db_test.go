package datastore_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/mocks"
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
	mockConnector.EXPECT().Open(gomock.Any(), primaryDSN, gomock.Any()).Return(&datastore.DB{DB: primaryMockDB}, nil)
	mockConnector.EXPECT().Open(gomock.Any(), replica1DSN, gomock.Any()).Return(&datastore.DB{DB: replica1MockDB}, nil)
	mockConnector.EXPECT().Open(gomock.Any(), replica2DSN, gomock.Any()).Return(&datastore.DB{DB: replica2MockDB}, nil)

	lb, err := datastore.NewDBLoadBalancer(context.Background(), primaryDSN, mockConnector, datastore.WithFixedHosts([]string{"replica1", "replica2"}))
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Verify primary
	require.Equal(t, primaryMockDB, lb.Primary().DB)

	// Verify replicas
	require.NotEmpty(t, lb.Replicas())
	require.Equal(t, []*datastore.DB{
		{DB: replica1MockDB},
		{DB: replica2MockDB},
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
			return &datastore.DB{}, nil
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
			expectedErrs: []string{`failed to open replica "fail_replica2" database connection: replica connection failed`},
		},
		{
			name:       "multiple replica connections fail",
			primaryDSN: primaryDSN,
			replicaHosts: []string{
				"fail_replica1",
				"fail_replica2",
			},
			expectedErrs: []string{
				`failed to open replica "fail_replica1" database connection: replica connection failed`,
				`failed to open replica "fail_replica2" database connection: replica connection failed`,
			},
		},
		{
			name:       "primary and replica connections fail",
			primaryDSN: &datastore.DSN{Host: "fail_primary"},
			replicaHosts: []string{
				"fail_replica2",
			},
			expectedErrs: []string{
				`failed to open primary database connection: primary connection failed`,
				`failed to open replica "fail_replica2" database connection: replica connection failed`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lb, err := datastore.NewDBLoadBalancer(context.Background(), tt.primaryDSN, mockConnector, datastore.WithFixedHosts(tt.replicaHosts))
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
	lb, err := datastore.NewDBLoadBalancer(context.Background(), primaryDSN, mockConnector, datastore.WithServiceDiscovery(mockResolver))
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Verify primary
	require.Equal(t, primaryMockDB, lb.Primary().DB)

	// Verify replicas round-robin rotation
	require.Equal(t, replica1MockDB, lb.Replica().DB)
	require.Equal(t, replica2MockDB, lb.Replica().DB)
	require.Equal(t, replica1MockDB, lb.Replica().DB)

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
	_, err = datastore.NewDBLoadBalancer(context.Background(), primaryDSN, mockConnector, datastore.WithServiceDiscovery(mockResolver))
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
	_, err = datastore.NewDBLoadBalancer(context.Background(), primaryDSN, mockConnector, datastore.WithServiceDiscovery(mockResolver))
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
			expectedErrors: []string{`failed to open replica "192.168.1.1" database connection: failed to open replica 1`},
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
				`failed to open replica "192.168.1.1" database connection: failed to open replica 1`,
				`failed to open replica "192.168.1.2" database connection: failed to open replica 2`,
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
				`failed to open replica "192.168.1.1" database connection: failed to open replica 1`,
				`failed to open replica "192.168.1.2" database connection: failed to open replica 2`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockExpectFunc()

			lb, err := datastore.NewDBLoadBalancer(context.Background(), tt.primaryDSN, mockConnector, datastore.WithServiceDiscovery(mockResolver))
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

	lb, err := datastore.NewDBLoadBalancer(context.Background(), primaryDSN, mockConnector)
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
	lb, err := datastore.NewDBLoadBalancer(context.Background(), primaryDSN, mockConnector,
		datastore.WithServiceDiscovery(mockResolver),
		// Use different hosts than those on the DSNs used for the mock expectations to guarantee that this test will
		// fail if precedence of service discovery is not observed
		datastore.WithFixedHosts([]string{"foo.example.com", "bar.example.com"}),
	)
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Verify primary
	require.Equal(t, primaryMockDB, lb.Primary().DB)

	// Verify replicas round-robin rotation
	require.Equal(t, replica1MockDB, lb.Replica().DB)
	require.Equal(t, replica2MockDB, lb.Replica().DB)
	require.Equal(t, replica1MockDB, lb.Replica().DB)

	// Verify mock expectations
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock1.ExpectationsWereMet())
	require.NoError(t, replicaMock2.ExpectationsWereMet())
}
