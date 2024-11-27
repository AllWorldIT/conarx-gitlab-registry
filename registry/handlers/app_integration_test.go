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
	tcs := map[string]struct {
		path               string
		dbEnabled          bool
		ffEnforceLockfiles bool
		expectedErr        error
	}{
		"filesystem-in-use with db disabled and ff enabled": {
			path:               "../datastore/testdata/fixtures/importer/happy-path",
			dbEnabled:          false,
			ffEnforceLockfiles: true,
			expectedErr:        nil,
		},
		"filesystem-in-use with db enabled and ff enabled": {
			path:               "../datastore/testdata/fixtures/importer/happy-path",
			dbEnabled:          true,
			ffEnforceLockfiles: true,
			expectedErr:        handlers.ErrFilesystemInUse,
		},
		"filesystem-in-use with db enabled and ff disabled": {
			path:               "../datastore/testdata/fixtures/importer/happy-path",
			dbEnabled:          true,
			ffEnforceLockfiles: false,
			expectedErr:        nil,
		},
		"database-in-use with db disabled and ff enabled": {
			path:               "../datastore/testdata/fixtures/importer/lockfile-db-in-use",
			dbEnabled:          false,
			ffEnforceLockfiles: true,
			expectedErr:        handlers.ErrDatabaseInUse,
		},
		"database-in-use with db disabled and ff disabled": {
			path:               "../datastore/testdata/fixtures/importer/lockfile-db-in-use",
			dbEnabled:          false,
			ffEnforceLockfiles: false,
			expectedErr:        nil,
		},
		"database-in-use with db enabled and ff enabled": {
			path:               "../datastore/testdata/fixtures/importer/lockfile-db-in-use",
			dbEnabled:          true,
			ffEnforceLockfiles: true,
			expectedErr:        nil,
		},
	}

	for tn, tc := range tcs {
		t.Run(tn, func(t *testing.T) {

			if os.Getenv("REGISTRY_DATABASE_ENABLED") != "true" {
				t.Skip("Skipping test as database is disabled")
			}

			t.Setenv(feature.EnforceLockfiles.EnvVariable, strconv.FormatBool(tc.ffEnforceLockfiles))

			opts := []configOpt{withFSDriver(tc.path)}
			if !tc.dbEnabled {
				opts = append(opts, withDBDisabled)
			}

			config := newConfig(opts...)
			app, err := handlers.NewApp(context.Background(), &config)
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, app)

			t.Cleanup(func() {
				restoreLockfiles(t, &config)
			})
		})
	}
}

func restoreLockfiles(t *testing.T, config *configuration.Configuration) {
	t.Helper()


	driver, err := factory.Create(config.Storage.Type(), config.Storage.Parameters())
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
