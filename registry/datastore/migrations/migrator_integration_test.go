//go:build integration

package migrations_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/docker/distribution/registry/bbm"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/migrations"
	testmigrations "github.com/docker/distribution/registry/datastore/migrations/testdata/fixtures"
	"github.com/docker/distribution/registry/datastore/testutil"
	migrate "github.com/rubenv/sql-migrate"

	"github.com/stretchr/testify/require"
)

const migrationTableName = "test_migrations"

func init() {
	migrate.SetTable(migrationTableName)
}

func TestMigrator_Version(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.All()))
	_, err = m.Up()
	require.NoError(t, err)

	latest, err := m.LatestVersion()
	require.NoError(t, err)

	current, err := m.Version()
	require.NoError(t, err)
	require.Equal(t, latest, current)
}

func TestMigrator_Version_NoMigrations(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	// Create migrator with an empty migration source.
	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(make([]*migrations.Migration, 0)))

	v, err := m.Version()
	require.NoError(t, err)
	require.Empty(t, v)
}

func TestMigrator_LatestVersion(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.All()))

	v, err := m.LatestVersion()
	require.NoError(t, err)
	require.Equal(t, v, testmigrations.All()[len(testmigrations.All())-1].Id)
}

func TestMigrator_LatestVersion_NoMigrations(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	// Create migrator with an empty migration source.
	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(make([]*migrations.Migration, 0)))
	v, err := m.LatestVersion()
	require.NoError(t, err)
	require.Empty(t, v)
}

func TestMigrator_Up(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.All()))

	all := testmigrations.All()

	mr, err := m.Up()
	require.NoError(t, err)
	require.Equal(t, len(all), mr.AppliedCount)

	currentVersion, err := m.Version()
	require.NoError(t, err)

	v, err := m.LatestVersion()
	require.NoError(t, err)
	require.Equal(t, v, currentVersion)
}

func TestMigrator_UpN(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.All()))

	// apply all except the last two
	all := testmigrations.All()
	n := len(all) - 1 - 2
	nth := all[n-1]

	mr, err := m.UpN(n)
	require.NoError(t, err)
	require.Equal(t, n, mr.AppliedCount)

	v, err := m.Version()
	require.NoError(t, err)
	require.Equal(t, nth.Id, v)

	// resume and apply the remaining
	mr, err = m.UpN(0)
	require.NoError(t, err)
	require.Equal(t, len(all)-n, mr.AppliedCount)

	v, err = m.Version()
	require.NoError(t, err)
	require.Equal(t, all[len(all)-1].Id, v)

	// make sure it's idempotent
	mr, err = m.UpN(100)
	require.NoError(t, err)
	require.Zero(t, mr.AppliedCount)

	v2, err := m.Version()
	require.NoError(t, err)
	require.Equal(t, v, v2)
}

func TestMigrator_UpNPlan(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.All()))

	all := testmigrations.All()

	var allPlan []string
	for _, migration := range all {
		allPlan = append(allPlan, migration.Id)
	}

	// plan all except the last two
	n := len(allPlan) - 1 - 2
	allExceptLastTwoPlan := allPlan[:n]

	plan, err := m.UpNPlan(n)
	require.NoError(t, err)
	require.Equal(t, allExceptLastTwoPlan, plan)

	// apply two migrations and re-plan all (the first two shouldn't be part of the plan anymore)
	_, err = m.UpN(2)
	require.NoError(t, err)

	plan, err = m.UpNPlan(0)
	require.NoError(t, err)

	allExceptFirstTwoPlan := allPlan[2:]
	require.Equal(t, allExceptFirstTwoPlan, plan)

	// make sure it's idempotent
	plan, err = m.UpNPlan(100)
	require.NoError(t, err)
	require.Equal(t, allExceptFirstTwoPlan, plan)
}

func TestMigrator_Down(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.All()))
	_, err = m.Up()
	require.NoError(t, err)

	all := testmigrations.All()

	count, err := m.Down()
	require.NoError(t, err)
	require.Equal(t, len(all), count)

	currentVersion, err := m.Version()
	require.NoError(t, err)
	require.Empty(t, currentVersion)
}

func TestMigrator_DownN(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.All()))
	_, err = m.Up()
	require.NoError(t, err)

	// rollback all except the first two
	all := testmigrations.All()
	n := len(all) - 2
	second := all[1]

	count, err := m.DownN(n)
	require.NoError(t, err)
	require.Equal(t, n, count)

	v, err := m.Version()
	require.NoError(t, err)
	require.Equal(t, second.Id, v)

	// resume and rollback the remaining two
	count, err = m.DownN(0)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	v, err = m.Version()
	require.NoError(t, err)
	require.Empty(t, v)

	// make sure it's idempotent
	count, err = m.DownN(100)
	require.NoError(t, err)
	require.Zero(t, count)

	v, err = m.Version()
	require.NoError(t, err)
	require.Empty(t, v)
}

func TestMigrator_DownNPlan(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.All()))
	_, err = m.Up()
	require.NoError(t, err)

	all := testmigrations.All()

	var allPlan []string

	for _, migration := range all {
		allPlan = append(allPlan, migration.Id)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(allPlan))) // down migrations are applied in reverse order

	// plan all except the last two
	n := len(allPlan) - 1 - 2
	allExceptLastTwoPlan := allPlan[:n]

	plan, err := m.DownNPlan(n)
	require.NoError(t, err)
	require.Equal(t, allExceptLastTwoPlan, plan)

	// apply two migrations and re-plan all (the first two shouldn't be part of the plan anymore)
	_, err = m.DownN(2)
	require.NoError(t, err)

	plan, err = m.DownNPlan(0)
	require.NoError(t, err)

	allExceptFirstTwoPlan := allPlan[2:]
	require.Equal(t, allExceptFirstTwoPlan, plan)

	// make sure it's idempotent
	plan, err = m.DownNPlan(100)
	require.NoError(t, err)
	require.Equal(t, allExceptFirstTwoPlan, plan)
}

func TestMigrator_Status_Empty(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.All()))

	all := testmigrations.All()

	statuses, err := m.Status()
	require.NoError(t, err)
	require.Len(t, statuses, len(all))

	var expectedIDs, actualIDs []string
	for _, m := range all {
		expectedIDs = append(expectedIDs, m.Id)
	}
	for id := range statuses {
		actualIDs = append(actualIDs, id)
	}
	require.ElementsMatch(t, expectedIDs, actualIDs)

	for _, s := range statuses {
		require.False(t, s.Unknown)
		require.Nil(t, s.AppliedAt)
	}
}

func TestMigrator_Status_Full(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.All()))
	_, err = m.Up()
	require.NoError(t, err)

	all := testmigrations.All()

	statuses, err := m.Status()
	require.NoError(t, err)
	require.Len(t, statuses, len(all))

	var expectedIDs, actualIDs []string
	for _, m := range all {
		expectedIDs = append(expectedIDs, m.Id)
	}
	for id := range statuses {
		actualIDs = append(actualIDs, id)
	}
	require.ElementsMatch(t, expectedIDs, actualIDs)

	for _, s := range statuses {
		require.False(t, s.Unknown)
		require.NotNil(t, s.AppliedAt)
	}
}

func TestMigrator_Status_Unknown(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.All()))
	_, err = m.Up()
	require.NoError(t, err)

	all := testmigrations.All()

	// temporarily insert fake migration record
	fakeID := "20060102150405_foo"
	fakeAppliedAt := time.Now()
	_, err = db.DB.Exec("INSERT INTO "+migrationTableName+" (id, applied_at) VALUES ($1, $2)", fakeID, fakeAppliedAt)
	require.NoError(t, err)
	defer db.DB.Exec("DELETE FROM "+migrationTableName+" WHERE id = $1", fakeID)

	statuses, err := m.Status()
	require.NoError(t, err)
	require.Len(t, statuses, len(all)+1)

	fakeStatus := statuses[fakeID]
	require.NotNil(t, fakeStatus)
	require.True(t, fakeStatus.Unknown)
	require.Equal(t, fakeAppliedAt.Round(time.Millisecond).UTC(), fakeStatus.AppliedAt.Round(time.Millisecond).UTC())
}

func TestMigrator_HasPending_No(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.All()))
	_, err = m.Up()
	require.NoError(t, err)

	pending, err := m.HasPending()
	require.NoError(t, err)
	require.False(t, pending)
}

func TestMigrator_HasPending_Yes(t *testing.T) {
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db)

	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.All()))
	_, err = m.Up()
	require.NoError(t, err)

	_, err = m.DownN(1)
	require.NoError(t, err)

	pending, err := m.HasPending()
	require.NoError(t, err)
	require.True(t, pending)
}

// clenaupOpts provides functional options for cleaning up the database.
type clenaupOpts func(*datastore.DB)

// withCleanupBBM wipes the entire batched_background_migrations and batched_background_migration_jobs tables.
func withCleanupBBM(t *testing.T) clenaupOpts {
	return func(db *datastore.DB) {
		_, err := db.DB.Exec("DROP TABLE IF EXISTS batched_background_migration_jobs CASCADE")
		require.NoError(t, err)
		_, err = db.DB.Exec("DROP TABLE IF EXISTS batched_background_migrations CASCADE")
		require.NoError(t, err)
	}
}

func cleanupDB(t *testing.T, db *datastore.DB, opts ...clenaupOpts) {
	_, err := db.DB.Exec("DELETE FROM " + migrationTableName)
	require.NoError(t, err)

	for _, opt := range opts {
		opt(db)
	}

	require.NoError(t, db.Close())
}

// TestMigrator_Up_WithEnforcedBBM tests the behavior of the migrator when an enforced Batched Background Migration (BBM) is present in a non-fresh registry install.
func TestMigrator_Up_WithEnforcedBBM(t *testing.T) {
	// Initialize a new database connection from environment variables
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db, withCleanupBBM(t))

	// Create a new migrator instance with enforced batched background migrations
	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.AllWithEnforcedBBMMigrations()))
	all := testmigrations.AllWithEnforcedBBMMigrations()

	// Apply the first 5 migrations to simulate a non-fresh registry install
	// This allows us to test the case where the registry has existing migrations
	initialMigrationCount := 5
	mr, err := m.UpN(initialMigrationCount)
	require.NoError(t, err)
	require.Equal(t, initialMigrationCount, mr.AppliedCount) // Expect 5 migrations to be applied
	require.Equal(t, 0, mr.AppliedBBMCount)                  // Expect no batched background migrations to be applied

	// Attempt to apply all remaining migrations
	mr, err = m.Up()
	// Expect an error due to enforced batched background migrations not being complete
	require.Error(t, err)
	require.ErrorIs(t, err, migrations.ErrBBMNotComplete)
	// Expect all migrations except the first 5 and the last one - requiring an enforced BBM - to be applied
	require.Equal(t, len(all)-initialMigrationCount-1, mr.AppliedCount)

	// Check the current version after migration
	currentVersion, err := m.Version()
	require.NoError(t, err)

	// Check the latest version available
	v, err := m.LatestVersion()
	require.NoError(t, err)
	// Expect the current version to be the second last migration's ID
	require.NotEqual(t, v, currentVersion)
	require.Equal(t, all[len(all)-2].Id, currentVersion)
}

// TestMigrator_Up_NewInstall_WithEnforcedBBM tests the behavior of the migrator when an enforced
// Batched Background Migration (BBM) is present in a new installation scenario.
func TestMigrator_Up_NewInstall_WithEnforcedBBM(t *testing.T) {
	// Set up a test database and ensure cleanup after the test
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db, withCleanupBBM(t))

	// Create a new migrator with enforced BBM migrations and a custom BBM worker
	m := migrations.NewMigrator(db,
		migrations.WithTable(migrationTableName),
		migrations.Source(
			testmigrations.AllWithEnforcedBBMMigrations(),
		),
		migrations.WithBBMWorker(
			bbm.NewSyncWorker(
				db,
				bbm.WithWorkMap(
					map[string]bbm.Work{
						"signatureName": {
							Name: "signatureName",
							Do:   func(_ context.Context, _ datastore.Handler, _, _ string, _, _, _ int) error { return nil },
						},
					},
				),
			),
		),
	)
	all := testmigrations.AllWithEnforcedBBMMigrations()

	// Attempt to run all migrations in a new installation scenario
	mr, err := m.Up()
	// Expect no error as this is a new installation and all migrations should be applied
	require.NoError(t, err)
	// Expect all migrations to be applied
	require.Equal(t, len(all), mr.AppliedCount)
	// Expect the only enforced BBM to be applied
	require.Equal(t, 1, mr.AppliedBBMCount)

	// Check the current version after migration
	currentVersion, err := m.Version()
	require.NoError(t, err)

	// Verify that the current version is the latest version
	v, err := m.LatestVersion()
	require.NoError(t, err)
	require.Equal(t, v, currentVersion)
}

// TestMigrator_Up_WithUnEnforcedBBM tests the behavior of the migrator when an unenforced
// Batched Background Migration (BBM) is present.
func TestMigrator_Up_WithUnEnforcedBBM(t *testing.T) {
	// Set up a test database and ensure cleanup after the test
	db, err := testutil.NewDBFromEnv()
	require.NoError(t, err)
	defer cleanupDB(t, db, withCleanupBBM(t))

	// Create a new migrator with unenforced BBM migrations
	m := migrations.NewMigrator(db, migrations.WithTable(migrationTableName), migrations.Source(testmigrations.AllWithUnEnforcedBBMMigrations()))

	all := testmigrations.AllWithUnEnforcedBBMMigrations()

	// Run all migrations
	mr, err := m.Up()
	// Expect no error as the BBM is unenforced
	require.NoError(t, err)
	// Expect all migrations to be applied
	require.Equal(t, len(all), mr.AppliedCount)
	// Expect no bbm to be applied
	require.Equal(t, 0, mr.AppliedBBMCount)

	// Check the current version after migration
	currentVersion, err := m.Version()
	require.NoError(t, err)

	// Verify that the current version is the latest version
	v, err := m.LatestVersion()
	require.NoError(t, err)
	require.Equal(t, v, currentVersion)
}
