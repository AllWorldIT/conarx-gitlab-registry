//go:build integration

package datastore_test

import (
	"context"
	"testing"
	"time"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	t.Parallel()

	testCases := []struct {
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
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			dsn, err := tc.dsnFactory()
			require.NoError(tt, err)

			db, err := datastore.NewConnector().Open(context.Background(), dsn)
			if tc.wantErr {
				require.Error(tt, err)
				require.Contains(tt, err.Error(), tc.wantErrMsg)
			} else {
				defer db.Close()
				require.NoError(tt, err)
				require.IsType(tt, new(datastore.DB), db)
			}
		})
	}
}
