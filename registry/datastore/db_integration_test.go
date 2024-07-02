//go:build integration

package datastore_test

import (
	"testing"
	"time"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		dsnFactory func() (*datastore.DSN, error)
		opts       []datastore.Option
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:       "success",
			dsnFactory: testutil.NewDSNFromEnv,
			opts: []datastore.Option{
				datastore.WithLogger(logrus.NewEntry(logrus.New())),
				datastore.WithPoolConfig(&datastore.PoolConfig{
					MaxIdle:     1,
					MaxOpen:     1,
					MaxLifetime: 1 * time.Minute,
					MaxIdleTime: 10 * time.Minute,
				}),
			},
			wantErr: false,
		},
		{
			name: "error",
			dsnFactory: func() (*datastore.DSN, error) {
				dsn, err := testutil.NewDSNFromEnv()
				if err != nil {
					return nil, err
				}
				dsn.DBName = "nonexistent"
				return dsn, nil
			},
			wantErr:    true,
			wantErrMsg: `FATAL: database "nonexistent" does not exist`,
		},
		{
			name: "wrong_credentials",
			dsnFactory: func() (*datastore.DSN, error) {
				dsn, err := testutil.NewDSNFromEnv()
				if err != nil {
					return nil, err
				}
				dsn.Password = "bad_password"
				return dsn, nil
			},
			wantErr:    true,
			wantErrMsg: "FATAL: password authentication failed for user",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn, err := tt.dsnFactory()
			require.NoError(t, err)

			db, err := datastore.Open(dsn)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErrMsg)
			} else {
				defer db.Close()
				require.NoError(t, err)
				require.IsType(t, new(datastore.DB), db)
			}
		})
	}
}
