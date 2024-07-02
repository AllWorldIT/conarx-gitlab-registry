package datastore

import (
	"errors"
	"fmt"
	"io"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hashicorp/go-multierror"
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
	var tests = []struct {
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
	var tests = []struct {
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

func TestNewDBLoadBalancer(t *testing.T) {
	primaryMockDB, primaryMock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { primaryMockDB.Close() })

	replica1MockDB, replicaMock1, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { replica1MockDB.Close() })

	replica2MockDB, replicaMock2, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { replica2MockDB.Close() })

	primaryDSN := &DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	primary := &DB{DB: primaryMockDB}
	replica1 := &DB{DB: replica1MockDB}
	replica2 := &DB{DB: replica2MockDB}

	// Mock Open function to return our sqlmock instances
	originalOpen := openFunc
	t.Cleanup(func() { openFunc = originalOpen })
	openFunc = func(dsn *DSN, opts ...Option) (*DB, error) {
		switch dsn.Host {
		case "primary":
			return primary, nil
		case "replica1":
			return replica1, nil
		case "replica2":
			return replica2, nil
		default:
			return nil, nil
		}
	}

	lb, err := NewDBLoadBalancer(primaryDSN, WithLoadBalancingHosts([]string{"replica1", "replica2"}))
	require.NoError(t, err)
	require.NotNil(t, lb)

	// Verify primary
	require.Equal(t, primaryMockDB, lb.primary.DB)

	// Verify replicas
	require.NotEmpty(t, lb.replicas)
	require.Equal(t, []*DB{replica1, replica2}, lb.replicas)

	// Verify mock expectations (no operations triggered)
	require.NoError(t, primaryMock.ExpectationsWereMet())
	require.NoError(t, replicaMock1.ExpectationsWereMet())
	require.NoError(t, replicaMock2.ExpectationsWereMet())
}

func TestNewDBLoadBalancer_Error(t *testing.T) {
	primaryDSN := &DSN{
		Host:     "primary",
		Port:     5432,
		User:     "user",
		Password: "password",
		DBName:   "dbname",
		SSLMode:  "disable",
	}

	// Mock Open function to return errors based on host matching
	originalOpen := openFunc
	t.Cleanup(func() { openFunc = originalOpen })
	openFunc = func(dsn *DSN, opts ...Option) (*DB, error) {
		if dsn.Host == "fail_primary" {
			return nil, errors.New("primary connection failed")
		} else if match, _ := regexp.MatchString(`fail_replica\d*`, dsn.Host); match {
			return nil, errors.New("replica connection failed")
		}
		return &DB{}, nil
	}

	tests := []struct {
		name         string
		primaryDSN   *DSN
		replicaHosts []string
		expectedErrs []string
	}{
		{
			name:         "primary connection fails",
			primaryDSN:   &DSN{Host: "fail_primary"},
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
			primaryDSN: &DSN{Host: "fail_primary"},
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
			lb, err := NewDBLoadBalancer(tt.primaryDSN, WithLoadBalancingHosts(tt.replicaHosts))
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
			dsn: &DSN{Host: "primary"},
		},
		replicas: []*DB{
			{
				DB:  replicaDB1,
				dsn: &DSN{Host: "replica1"},
			},
			{
				DB:  replicaDB2,
				dsn: &DSN{Host: "replica2"},
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
	db1 := lb.Replica()
	require.NotNil(t, db1)
	require.Equal(t, replicaDB1, db1.DB)

	db2 := lb.Replica()
	require.NotNil(t, db2)
	require.Equal(t, replicaDB2, db2.DB)

	db3 := lb.Replica()
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

	db := lb.Replica()
	require.NotNil(t, db)
	require.Equal(t, primaryDB, db.DB)
}
