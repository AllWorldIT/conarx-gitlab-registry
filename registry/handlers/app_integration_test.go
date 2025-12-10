//go:build integration && handlers_test

package handlers_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/internal/feature"
	"github.com/docker/distribution/registry/datastore/migrations"
	"github.com/docker/distribution/registry/datastore/migrations/postmigrations"
	"github.com/docker/distribution/registry/datastore/migrations/premigrations"
	datastoretestutil "github.com/docker/distribution/registry/datastore/testutil"
	"github.com/docker/distribution/registry/handlers"
	"github.com/docker/distribution/registry/storage"
	"github.com/docker/distribution/registry/storage/driver/factory"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestHandleFilesystemLockFile_BothLocksPresent tests handleFilesystemLockFile
// behavior when both filesystem and database lockfiles are present.
func TestHandleFilesystemLockFile_BothLocksPresent(t *testing.T) {
	testCases := []struct {
		name               string
		ffEnforceLockfiles bool
		expectedErr        error
		expectDBLock       bool
		expectFSLock       bool
	}{
		{
			name:               "both locks with ff enabled",
			ffEnforceLockfiles: true,
			expectedErr:        handlers.ErrInvalidLockfiles,
			expectDBLock:       true,
			expectFSLock:       true,
		},
		{
			name:               "both locks with ff disabled",
			ffEnforceLockfiles: false,
			expectedErr:        nil,   // No enforcement when FF disabled
			expectDBLock:       false, // Data repair when FF disabled
			expectFSLock:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			tt.Setenv(feature.EnforceLockfiles.EnvVariable, strconv.FormatBool(tc.ffEnforceLockfiles))

			// Copy happy-path fixture and set both locks
			tmpDir := tt.TempDir()
			copyFixture(tt, tmpDir)

			config := newConfig(withFSDriver(tmpDir))
			config.Database.Enabled = configuration.DatabaseEnabledFalse

			// Set both locks
			ctx := context.Background()
			l := newTestLockers(t, &config)
			require.NoError(tt, l.FS.Lock(ctx))
			require.NoError(tt, l.DB.Lock(ctx))

			app, appErr := handlers.NewApp(context.Background(), &config)

			locked, err := l.FSIsLocked(ctx)
			require.NoError(tt, err)
			assert.Equal(tt, tc.expectFSLock, locked)

			locked, err = l.DBIsLocked(ctx)
			require.NoError(tt, err)
			assert.Equal(tt, tc.expectDBLock, locked)

			if tc.expectedErr != nil {
				assert.ErrorIs(tt, appErr, tc.expectedErr)
				return
			}

			require.NoError(tt, appErr)
			assert.NotNil(tt, app)
		})
	}
}

// TestHandleFilesystemLockFile_FSLockedOnly tests handleFilesystemLockFile
// behavior when only the filesystem lockfile is present.
func TestHandleFilesystemLockFile_FSLockedOnly(t *testing.T) {
	testCases := []struct {
		name               string
		ffEnforceLockfiles bool
	}{
		{
			name:               "fs locked only with ff enabled",
			ffEnforceLockfiles: true,
		},
		{
			name:               "fs locked only with ff disabled",
			ffEnforceLockfiles: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			tt.Setenv(feature.EnforceLockfiles.EnvVariable, strconv.FormatBool(tc.ffEnforceLockfiles))

			// Copy happy-path fixture and set only FS lock
			tmpDir := tt.TempDir()
			copyFixture(tt, tmpDir)

			config := newConfig(withFSDriver(tmpDir))
			config.Database.Enabled = configuration.DatabaseEnabledFalse

			// Set only FS lock
			ctx := context.Background()
			l := newTestLockers(t, &config)
			require.NoError(tt, l.FS.Lock(ctx))
			require.NoError(tt, l.DB.Unlock(ctx))

			app, err := handlers.NewApp(context.Background(), &config)
			require.NoError(tt, err)
			assert.NotNil(tt, app)
		})
	}
}

// TestHandleFilesystemLockFile_DBLockedOnly tests handleFilesystemLockFile
// behavior when only the database lockfile is present.
func TestHandleFilesystemLockFile_DBLockedOnly(t *testing.T) {
	testCases := []struct {
		name               string
		ffEnforceLockfiles bool
		expectedErr        error
		expectDBLock       bool
		expectFSLock       bool
	}{
		{
			name:               "db locked only with ff enabled",
			ffEnforceLockfiles: true,
			expectedErr:        handlers.ErrDatabaseInUse,
			expectDBLock:       true,
			expectFSLock:       false,
		},
		{
			name:               "db locked only with ff disabled",
			ffEnforceLockfiles: false,
			expectedErr:        nil,   // No enforcement when FF disabled
			expectDBLock:       false, // Data repair when FF disabled
			expectFSLock:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			tt.Setenv(feature.EnforceLockfiles.EnvVariable, strconv.FormatBool(tc.ffEnforceLockfiles))

			// Copy happy-path fixture and set only DB lock
			tmpDir := tt.TempDir()
			copyFixture(tt, tmpDir)

			config := newConfig(withFSDriver(tmpDir))
			config.Database.Enabled = configuration.DatabaseEnabledFalse

			// Set only DB lock
			ctx := context.Background()
			l := newTestLockers(t, &config)
			require.NoError(tt, l.FS.Unlock(ctx))
			require.NoError(tt, l.DB.Lock(ctx))

			app, appErr := handlers.NewApp(context.Background(), &config)

			locked, err := l.FSIsLocked(ctx)
			require.NoError(tt, err)
			assert.Equal(tt, tc.expectFSLock, locked)

			locked, err = l.DBIsLocked(ctx)
			require.NoError(tt, err)
			assert.Equal(tt, tc.expectDBLock, locked)

			if tc.expectedErr != nil {
				assert.ErrorIs(tt, appErr, tc.expectedErr)
				return
			}

			require.NoError(tt, appErr)
			assert.NotNil(tt, app)
		})
	}
}

// TestHandleFilesystemLockFile_NoLocksWithLegacyRepos tests handleFilesystemLockFile
// behavior when no lockfiles exist but legacy repositories are present. Should create FS lock.
func TestHandleFilesystemLockFile_NoLocksWithLegacyRepos(t *testing.T) {
	testCases := []struct {
		name               string
		ffEnforceLockfiles bool
	}{
		{
			name:               "no locks with legacy repos and ff enabled",
			ffEnforceLockfiles: true,
		},
		{
			name:               "no locks with legacy repos and ff disabled",
			ffEnforceLockfiles: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			tt.Setenv(feature.EnforceLockfiles.EnvVariable, strconv.FormatBool(tc.ffEnforceLockfiles))

			// Copy happy-path fixture and remove all locks
			tmpDir := tt.TempDir()
			copyFixture(tt, tmpDir)

			config := newConfig(withFSDriver(tmpDir))
			config.Database.Enabled = configuration.DatabaseEnabledFalse

			// Remove all locks
			ctx := context.Background()
			l := newTestLockers(t, &config)
			require.NoError(tt, l.FS.Unlock(ctx))
			require.NoError(tt, l.DB.Unlock(ctx))

			app, err := handlers.NewApp(context.Background(), &config)
			require.NoError(tt, err)
			assert.NotNil(tt, app)

			// Verify FS lock was created
			locked, err := app.Lockers.FSIsLocked(ctx)
			require.NoError(tt, err)
			assert.True(tt, locked, "FS lock should be created when legacy repos exist")
		})
	}
}

// TestHandleFilesystemLockFile_NoLocksNoRepos tests handleFilesystemLockFile
// behavior when no lockfiles exist and no repositories are present. Should not create any locks.
func TestHandleFilesystemLockFile_NoLocksNoRepos(t *testing.T) {
	testCases := []struct {
		name               string
		ffEnforceLockfiles bool
	}{
		{
			name:               "clean fs with ff enabled",
			ffEnforceLockfiles: true,
		},
		{
			name:               "clean fs with ff disabled",
			ffEnforceLockfiles: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			tt.Setenv(feature.EnforceLockfiles.EnvVariable, strconv.FormatBool(tc.ffEnforceLockfiles))

			tmpDir := tt.TempDir()

			config := newConfig(withFSDriver(tmpDir))
			config.Database.Enabled = configuration.DatabaseEnabledFalse

			app, err := handlers.NewApp(context.Background(), &config)
			require.NoError(tt, err)
			assert.NotNil(tt, app)

			// Verify no FS lock was created
			locked, err := app.Lockers.FSIsLocked(context.Background())
			require.NoError(tt, err)
			assert.False(tt, locked, "FS lock should not be created when no repos exist")
		})
	}
}

// TestInitializeMetadataDatabase_BothLocksPresent tests initializeMetadataDatabase
// behavior when both lockfiles are present.
func TestInitializeMetadataDatabase_BothLocksPresent(t *testing.T) {
	if os.Getenv("REGISTRY_DATABASE_ENABLED") != "true" {
		t.Skip("Skipping test as database is disabled")
	}

	testCases := []struct {
		name               string
		ffEnforceLockfiles bool
		dbMode             configuration.DatabaseEnabled
		expectedErr        error
		expectDBLock       bool
		expectFSLock       bool
	}{
		{
			name:               "both locks with db enabled and ff enabled",
			ffEnforceLockfiles: true,
			dbMode:             configuration.DatabaseEnabledTrue,
			expectedErr:        handlers.ErrInvalidLockfiles,
			expectDBLock:       true,
			expectFSLock:       true,
		},
		{
			name:               "both locks with db enabled and ff disabled",
			ffEnforceLockfiles: false,
			dbMode:             configuration.DatabaseEnabledTrue,
			expectedErr:        nil, // No enforcement, continues initialization
			expectDBLock:       true,
			expectFSLock:       false, // Data repair when FF disabled
		},
		{
			name:               "both locks with db prefer and ff enabled",
			ffEnforceLockfiles: true,
			dbMode:             configuration.DatabaseEnabledPrefer,
			expectedErr:        handlers.ErrInvalidLockfiles,
			expectDBLock:       true,
			expectFSLock:       true,
		},
		{
			name:               "both locks with db prefer and ff disabled",
			ffEnforceLockfiles: false,
			dbMode:             configuration.DatabaseEnabledPrefer,
			expectedErr:        nil,
			expectDBLock:       true,
			expectFSLock:       false, // Data repair when FF disabled
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			tt.Setenv(feature.EnforceLockfiles.EnvVariable, strconv.FormatBool(tc.ffEnforceLockfiles))

			// Copy happy-path fixture
			tmpDir := tt.TempDir()
			copyFixture(tt, tmpDir)

			config := newConfig(withFSDriver(tmpDir))

			// Set both locks
			ctx := context.Background()
			l := newTestLockers(t, &config)
			require.NoError(tt, l.DB.Lock(ctx))
			require.NoError(tt, l.FS.Lock(ctx))

			// Reconfigure with database enabled
			config.Database.Enabled = tc.dbMode

			truncateTables := runMigrations(t, &config)
			tt.Cleanup(truncateTables)

			app, appErr := handlers.NewApp(context.Background(), &config)

			locked, err := l.FSIsLocked(ctx)
			require.NoError(tt, err)
			assert.Equal(tt, tc.expectFSLock, locked)

			locked, err = l.DBIsLocked(ctx)
			require.NoError(tt, err)
			assert.Equal(tt, tc.expectDBLock, locked)

			if tc.expectedErr != nil {
				assert.ErrorIs(tt, appErr, tc.expectedErr)
				return
			}

			require.NoError(tt, appErr)
			assert.NotNil(tt, app)
		})
	}
}

// TestInitializeMetadataDatabase_FSLockedPreferMode tests initializeMetadataDatabase
// behavior in prefer mode when filesystem is locked.
func TestInitializeMetadataDatabase_FSLockedPreferMode(t *testing.T) {
	if os.Getenv("REGISTRY_DATABASE_ENABLED") != "true" {
		t.Skip("Skipping test as database is disabled")
	}

	testCases := []struct {
		name               string
		ffEnforceLockfiles bool
		expectFallback     bool
	}{
		{
			name:               "fs locked prefer mode with ff enabled",
			ffEnforceLockfiles: true,
			expectFallback:     true, // Falls back to filesystem
		},
		{
			name:               "fs locked prefer mode with ff disabled",
			ffEnforceLockfiles: false,
			expectFallback:     false, // Continues with DB
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			tt.Setenv(feature.EnforceLockfiles.EnvVariable, strconv.FormatBool(tc.ffEnforceLockfiles))

			// Copy happy-path fixture
			tmpDir := tt.TempDir()
			copyFixture(tt, tmpDir)

			config := newConfig(withFSDriver(tmpDir))

			// Set only FS lock
			ctx := context.Background()
			l := newTestLockers(t, &config)
			require.NoError(tt, l.FS.Lock(ctx))
			require.NoError(tt, l.DB.Unlock(ctx))

			// Reconfigure with database in prefer mode
			config.Database.Enabled = configuration.DatabaseEnabledPrefer

			truncateTables := runMigrations(t, &config)
			tt.Cleanup(truncateTables)

			app, err := handlers.NewApp(context.Background(), &config)
			require.NoError(tt, err)
			assert.NotNil(tt, app)

			// Verify fallback state
			assert.Equal(tt, tc.expectFallback, config.Database.PreferFallback, "Fallback state mismatch")
		})
	}
}

// TestInitializeMetadataDatabase_FSLockedEnabledMode tests initializeMetadataDatabase
// behavior in enabled mode when filesystem is locked.
func TestInitializeMetadataDatabase_FSLockedEnabledMode(t *testing.T) {
	if os.Getenv("REGISTRY_DATABASE_ENABLED") != "true" {
		t.Skip("Skipping test as database is disabled")
	}

	testCases := []struct {
		name               string
		ffEnforceLockfiles bool
		expectedErr        error
		expectDBLock       bool
		expectFSLock       bool
	}{
		{
			name:               "fs locked enabled mode with ff enabled",
			ffEnforceLockfiles: true,
			expectedErr:        handlers.ErrFilesystemInUse,
			expectDBLock:       false,
			expectFSLock:       true,
		},
		{
			name:               "fs locked enabled mode with ff disabled",
			ffEnforceLockfiles: false,
			expectedErr:        nil, // No enforcement, continues
			expectDBLock:       true,
			expectFSLock:       false, // Data repair when FF disabled
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			tt.Setenv(feature.EnforceLockfiles.EnvVariable, strconv.FormatBool(tc.ffEnforceLockfiles))

			// Copy happy-path fixture and set only FS lock
			tmpDir := tt.TempDir()
			copyFixture(tt, tmpDir)

			config := newConfig(withFSDriver(tmpDir))

			// Set only FS lock
			ctx := context.Background()
			l := newTestLockers(t, &config)
			require.NoError(tt, l.FS.Lock(ctx))
			require.NoError(tt, l.DB.Unlock(ctx))

			// Reconfigure with database in enabled mode
			config.Database.Enabled = configuration.DatabaseEnabledTrue

			truncateTables := runMigrations(t, &config)
			tt.Cleanup(truncateTables)

			app, appErr := handlers.NewApp(context.Background(), &config)

			locked, err := l.FSIsLocked(ctx)
			require.NoError(tt, err)
			assert.Equal(tt, tc.expectFSLock, locked)

			locked, err = l.DBIsLocked(ctx)
			require.NoError(tt, err)
			assert.Equal(tt, tc.expectDBLock, locked)

			if tc.expectedErr != nil {
				assert.ErrorIs(tt, appErr, tc.expectedErr)
				return
			}

			require.NoError(tt, appErr)
			assert.NotNil(tt, app)
		})
	}
}

// TestInitializeMetadataDatabase_NoLocksCreatesDBLock tests that DB lock is
// created after successful database initialization regardless of FF state.
func TestInitializeMetadataDatabase_NoLocksCreatesDBLock(t *testing.T) {
	if os.Getenv("REGISTRY_DATABASE_ENABLED") != "true" {
		t.Skip("Skipping test as database is disabled")
	}

	testCases := []struct {
		name               string
		ffEnforceLockfiles bool
		dbMode             configuration.DatabaseEnabled
	}{
		{
			name:               "no locks db enabled with ff enabled",
			ffEnforceLockfiles: true,
			dbMode:             configuration.DatabaseEnabledTrue,
		},
		{
			name:               "no locks db enabled with ff disabled",
			ffEnforceLockfiles: false,
			dbMode:             configuration.DatabaseEnabledTrue,
		},
		{
			name:               "no locks db prefer with ff enabled",
			ffEnforceLockfiles: true,
			dbMode:             configuration.DatabaseEnabledPrefer,
		},
		{
			name:               "no locks db prefer with ff disabled",
			ffEnforceLockfiles: false,
			dbMode:             configuration.DatabaseEnabledPrefer,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			tt.Setenv(feature.EnforceLockfiles.EnvVariable, strconv.FormatBool(tc.ffEnforceLockfiles))

			// Use a fresh temp directory with no locks
			tmpDir := tt.TempDir()

			config := newConfig(withFSDriver(tmpDir))

			// Ensure no locks exist
			ctx := context.Background()
			l := newTestLockers(t, &config)
			require.NoError(tt, l.FS.Unlock(ctx))
			require.NoError(tt, l.DB.Unlock(ctx))

			// Reconfigure database
			config.Database.Enabled = tc.dbMode

			truncateTables := runMigrations(t, &config)
			tt.Cleanup(truncateTables)

			app, err := handlers.NewApp(context.Background(), &config)
			require.NoError(tt, err)
			assert.NotNil(tt, app)

			// Verify DB lock was created
			locked, err := app.Lockers.DBIsLocked(ctx)
			require.NoError(tt, err)
			assert.True(tt, locked, "DB lock should be created after successful initialization")
		})
	}
}

// TestInitializeMetadataDatabase_DBLockedOnly tests initializeMetadataDatabase
// behavior when only the database lockfile is present.
func TestInitializeMetadataDatabase_DBLockedOnly(t *testing.T) {
	if os.Getenv("REGISTRY_DATABASE_ENABLED") != "true" {
		t.Skip("Skipping test as database is disabled")
	}

	testCases := []struct {
		name               string
		ffEnforceLockfiles bool
		dbMode             configuration.DatabaseEnabled
	}{
		{
			name:               "db locked only with db enabled and ff enabled",
			ffEnforceLockfiles: true,
			dbMode:             configuration.DatabaseEnabledTrue,
		},
		{
			name:               "db locked only with db enabled and ff disabled",
			ffEnforceLockfiles: false,
			dbMode:             configuration.DatabaseEnabledTrue,
		},
		{
			name:               "db locked only with db prefer and ff enabled",
			ffEnforceLockfiles: true,
			dbMode:             configuration.DatabaseEnabledPrefer,
		},
		{
			name:               "db locked only with db prefer and ff disabled",
			ffEnforceLockfiles: false,
			dbMode:             configuration.DatabaseEnabledPrefer,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			tt.Setenv(feature.EnforceLockfiles.EnvVariable, strconv.FormatBool(tc.ffEnforceLockfiles))

			// Copy happy-path fixture and set only DB lock
			tmpDir := tt.TempDir()
			copyFixture(tt, tmpDir)

			config := newConfig(withFSDriver(tmpDir))

			// Set only DB lock
			ctx := context.Background()
			l := newTestLockers(t, &config)
			require.NoError(tt, l.FS.Unlock(ctx))
			require.NoError(tt, l.DB.Lock(ctx))

			// Reconfigure database
			config.Database.Enabled = tc.dbMode

			truncateTables := runMigrations(t, &config)
			tt.Cleanup(truncateTables)

			app, err := handlers.NewApp(context.Background(), &config)
			require.NoError(tt, err)
			assert.NotNil(tt, app)

			// DB lock should still be present
			locked, err := app.Lockers.DBIsLocked(ctx)
			require.NoError(tt, err)
			assert.True(tt, locked, "DB lock should remain after successful initialization")
		})
	}
}

// Helper function to copy fixture directory
func copyFixture(t *testing.T, dst string) {
	t.Helper()

	src := "../datastore/testdata/fixtures/importer/happy-path"

	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer srcFile.Close()

		dstFile, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode())
		if err != nil {
			return err
		}
		defer dstFile.Close()

		_, err = io.Copy(dstFile, srcFile)
		return err
	})
	require.NoError(t, err)
}

// newTestLockers creates a storage.Lockers instance for manipulating lockfile
// state before app initialization.
func newTestLockers(t *testing.T, config *configuration.Configuration) *storage.Lockers {
	t.Helper()

	driverParams := config.Storage.Parameters()
	driver, err := factory.Create(config.Storage.Type(), driverParams)
	require.NoError(t, err)

	return &storage.Lockers{
		FS: &storage.FilesystemInUseLocker{Driver: driver},
		DB: &storage.DatabaseInUseLocker{Driver: driver},
	}
}

func runMigrations(t *testing.T, config *configuration.Configuration) func() {
	t.Helper()

	db, err := datastoretestutil.NewDBFromConfig(config)
	require.NoError(t, err)

	var m []migrations.PureMigrator
	m = append(m, premigrations.NewMigrator(db.Primary()), postmigrations.NewMigrator(db.Primary()))
	for _, mig := range m {
		_, err := mig.Up()
		require.NoError(t, err)
	}

	return func() { datastoretestutil.TruncateAllTables(db.Primary()) }
}
