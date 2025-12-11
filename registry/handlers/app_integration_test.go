//go:build integration && handlers_test

package handlers_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/internal/feature"
	"github.com/docker/distribution/registry/handlers"
	"github.com/docker/distribution/registry/storage"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

func TestNewApp_Lockfiles(t *testing.T) {
	testCases := map[string]struct {
		path               string
		dbEnabled          configuration.DatabaseEnabled
		ffEnforceLockfiles bool
		expectedErr        error
	}{
		"filesystem-in-use with db disabled and ff enabled": {
			path:               "../datastore/testdata/fixtures/importer/happy-path",
			dbEnabled:          configuration.DatabaseEnabledFalse,
			ffEnforceLockfiles: true,
			expectedErr:        nil,
		},
		"filesystem-in-use with db enabled and ff enabled": {
			path:               "../datastore/testdata/fixtures/importer/happy-path",
			dbEnabled:          configuration.DatabaseEnabledTrue,
			ffEnforceLockfiles: true,
			expectedErr:        handlers.ErrFilesystemInUse,
		},
		"filesystem-in-use with db enabled and ff disabled": {
			path:               "../datastore/testdata/fixtures/importer/happy-path",
			dbEnabled:          configuration.DatabaseEnabledTrue,
			ffEnforceLockfiles: false,
			expectedErr:        nil,
		},
		"filesystem-in-use with db prefer and ff enabled": {
			path:               "../datastore/testdata/fixtures/importer/happy-path",
			dbEnabled:          configuration.DatabaseEnabledPrefer,
			ffEnforceLockfiles: true,
			expectedErr:        nil,
		},
		"filesystem-in-use with db prefer and ff disabled": {
			path:               "../datastore/testdata/fixtures/importer/happy-path",
			dbEnabled:          configuration.DatabaseEnabledPrefer,
			ffEnforceLockfiles: false,
			expectedErr:        nil,
		},
		"database-in-use with db disabled and ff enabled": {
			path:               "../datastore/testdata/fixtures/importer/lockfile-db-in-use",
			dbEnabled:          configuration.DatabaseEnabledFalse,
			ffEnforceLockfiles: true,
			expectedErr:        handlers.ErrDatabaseInUse,
		},
		"database-in-use with db disabled and ff disabled": {
			path:               "../datastore/testdata/fixtures/importer/lockfile-db-in-use",
			dbEnabled:          configuration.DatabaseEnabledFalse,
			ffEnforceLockfiles: false,
			expectedErr:        nil,
		},
		"database-in-use with db enabled and ff enabled": {
			path:               "../datastore/testdata/fixtures/importer/lockfile-db-in-use",
			dbEnabled:          configuration.DatabaseEnabledTrue,
			ffEnforceLockfiles: true,
			expectedErr:        nil,
		},
		"database-in-use with db prefer and ff disabled": {
			path:               "../datastore/testdata/fixtures/importer/lockfile-db-in-use",
			dbEnabled:          configuration.DatabaseEnabledPrefer,
			ffEnforceLockfiles: false,
			expectedErr:        nil,
		},
		"database-in-use with db prefer and ff enabled": {
			path:               "../datastore/testdata/fixtures/importer/lockfile-db-in-use",
			dbEnabled:          configuration.DatabaseEnabledPrefer,
			ffEnforceLockfiles: true,
			expectedErr:        nil,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(tt *testing.T) {
			if os.Getenv("REGISTRY_DATABASE_ENABLED") != "true" {
				tt.Skip("Skipping test as database is disabled")
			}

			tt.Setenv(feature.EnforceLockfiles.EnvVariable, strconv.FormatBool(tc.ffEnforceLockfiles))

			opts := []configOpt{withFSDriver(tc.path)}

			config := newConfig(opts...)
			config.Database.Enabled = tc.dbEnabled

			app, err := handlers.NewApp(context.Background(), &config)
			if tc.expectedErr != nil {
				require.ErrorIs(tt, err, tc.expectedErr)
				return
			}

			require.NoError(tt, err)
			require.NotNil(tt, app)

			tt.Cleanup(func() {
				restoreLockfiles(tt, &config)
			})
		})
	}
}

func restoreLockfiles(t *testing.T, config *configuration.Configuration) {
	t.Helper()

	driverParams := config.Storage.Parameters()
	driver, err := factory.Create(config.Storage.Type(), driverParams)
	require.NoError(t, err)

	fsLocker := storage.FilesystemInUseLocker{Driver: driver}
	dbLocker := storage.DatabaseInUseLocker{Driver: driver}
	ctx := context.Background()

	switch {
	case strings.Contains(t.Name(), "database-in-use"):
		err = fsLocker.Unlock(ctx)
		require.NoError(t, err)
		err = dbLocker.Lock(ctx)
		require.NoError(t, err)
	case strings.Contains(t.Name(), "filesystem-in-use"):
		err = dbLocker.Unlock(ctx)
		require.NoError(t, err)
		err = fsLocker.Lock(ctx)
		require.NoError(t, err)
	}
}
