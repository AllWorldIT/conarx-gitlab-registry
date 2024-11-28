//go:generate mockgen -package mocks -destination mocks/migration.go . Migrator

package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/docker/distribution/registry/bbm"
	"github.com/docker/distribution/registry/datastore"
	migrate "github.com/rubenv/sql-migrate"
)

const (
	migrationTableName = "schema_migrations"
	dialect            = "postgres"
)

var ErrBBMNotComplete = errors.New("background migration must be complete before proceeding")

func init() {
	migrate.SetTable(migrationTableName)
}

// MigrationResult holds the outcome of a migration operation.
// It includes the count of applied Batched Background Migrations (BBMs)
// and the total number of applied schema migrations.
type MigrationResult struct {
	AppliedBBMCount int
	AppliedCount    int
}

type Migrator interface {
	Up() (MigrationResult, error)
	Down() (int, error)
}

type MigratorImpl struct {
	db         *sql.DB
	migrations []*Migration

	skipPostDeployment bool
	bbmWorker          *bbm.SyncWorker
}

// NewMigrator creates new Migrator.
// NOTE(prozlach): we can not use interface here because in some cases the code
// is accessing private fields of the pacakge/object. This would need a deeper
// cleanup to make the API clean.
func NewMigrator(dsdb *datastore.DB, opts ...MigratorOption) *MigratorImpl {
	var db *sql.DB
	if dsdb != nil {
		db = dsdb.DB
	}
	m := &MigratorImpl{
		db:         db,
		migrations: allMigrations,
		bbmWorker:  bbm.NewSyncWorker(dsdb),
	}

	for _, o := range opts {
		o(m)
	}

	return m
}

// MigratorOption enables the creation of functional options for the
// configuration of the migrator.
type MigratorOption func(m *MigratorImpl)

// Source allows the migrator to use an alternative source of migrations, used
// for testing.
func Source(a []*Migration) MigratorOption {
	return func(m *MigratorImpl) {
		m.migrations = a
	}
}

// WithBBMWorker allows the migrator to use an alternative BBM worker, used
// for testing.
func WithBBMWorker(w *bbm.SyncWorker) MigratorOption {
	return func(m *MigratorImpl) {
		m.bbmWorker = w
	}
}

// SkipPostDeployment configures the migration to not apply postdeployment migrations.
func SkipPostDeployment() MigratorOption {
	return func(m *MigratorImpl) {
		m.skipPostDeployment = true
	}
}

// Reconfigure is used to change the configuration of an existing Migrator
// using given config option.
func (m *MigratorImpl) Reconfigure(f MigratorOption) {
	f(m)
}

// Version returns the current applied migration version (if any).
func (m *MigratorImpl) Version() (string, error) {
	records, err := migrate.GetMigrationRecords(m.db, dialect)
	if err != nil {
		return "", err
	}
	if len(records) == 0 {
		return "", nil
	}

	return records[len(records)-1].Id, nil
}

// LatestVersion identifies the version of the most recent migration in the repository (if any).
func (m *MigratorImpl) LatestVersion() (string, error) {
	all, err := m.eligibleMigrations()
	if err != nil {
		return "", err
	}
	if len(all) == 0 {
		return "", nil
	}

	return all[len(all)-1].Id, nil
}

func (m *MigratorImpl) migrate(direction migrate.MigrationDirection, limit int) (int, error) {
	src, err := m.eligibleMigrationSource()
	if err != nil {
		return 0, err
	}

	return migrate.ExecMax(m.db, dialect, src, direction, limit)
}

// Up applies all pending up migrations. Returns the number of applied migrations and background migrations.
func (m *MigratorImpl) Up() (MigrationResult, error) {
	return m.migrateUpWithBBMCheck(0)
}

// UpN applies up to n pending up migrations. All pending migrations will be applied if n is 0.  Returns the number of
// applied migrations and background migrations.
func (m *MigratorImpl) UpN(n int) (MigrationResult, error) {
	return m.migrateUpWithBBMCheck(n)
}

// UpNPlan plans up to n pending up migrations and returns the ordered list of migration IDs. All pending migrations
// will be planned if n is 0.
func (m *MigratorImpl) UpNPlan(n int) ([]string, error) {
	return m.plan(migrate.Up, n)
}

// Down applies all pending down migrations.  Returns the number of applied migrations.
func (m *MigratorImpl) Down() (int, error) {
	return m.migrate(migrate.Down, 0)
}

// DownN applies up to n pending down migrations. All migrations will be applied if n is 0.  Returns the number of
// applied migrations.
func (m *MigratorImpl) DownN(n int) (int, error) {
	return m.migrate(migrate.Down, n)
}

// DownNPlan plans up to n pending down migrations and returns the ordered list of migration IDs. All pending migrations
// will be planned if n is 0.
func (m *MigratorImpl) DownNPlan(n int) ([]string, error) {
	return m.plan(migrate.Down, n)
}

// MigrationStatus represents the status of a migration. Unknown will be set to true if a migration was applied but is
// not known by the current build.
type MigrationStatus struct {
	Unknown        bool
	PostDeployment bool
	AppliedAt      *time.Time
}

// Status returns the status of all migrations, indexed by migration ID.
func (m *MigratorImpl) Status() (map[string]*MigrationStatus, error) {
	applied, err := migrate.GetMigrationRecords(m.db, dialect)
	if err != nil {
		return nil, err
	}
	known, err := m.allMigrations()
	if err != nil {
		return nil, err
	}

	statuses := make(map[string]*MigrationStatus, len(applied))
	for _, k := range known {
		statuses[k.Id] = &MigrationStatus{}

		if mig := m.FindMigrationByID(k.Id); mig != nil && mig.PostDeployment {
			statuses[k.Id].PostDeployment = true
		}
	}

	for _, m := range applied {
		if _, ok := statuses[m.Id]; !ok {
			statuses[m.Id] = &MigrationStatus{Unknown: true}
		}

		statuses[m.Id].AppliedAt = &m.AppliedAt
	}

	return statuses, nil
}

// HasPending determines whether all known migrations are applied or not.
func (m *MigratorImpl) HasPending() (bool, error) {
	records, err := migrate.GetMigrationRecords(m.db, dialect)
	if err != nil {
		return false, err
	}

	eligible, err := m.eligibleMigrations()
	if err != nil {
		return false, err
	}

	for _, k := range eligible {
		if !migrationApplied(records, k.Id) {
			return true, nil
		}
	}

	return false, nil
}

func (m *MigratorImpl) plan(direction migrate.MigrationDirection, limit int) ([]string, error) {
	src, err := m.eligibleMigrationSource()
	if err != nil {
		return nil, err
	}

	planned, _, err := migrate.PlanMigration(m.db, dialect, src, direction, limit)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(planned))
	for _, m := range planned {
		result = append(result, m.Id)
	}

	return result, nil
}

func (m *MigratorImpl) allMigrations() ([]*migrate.Migration, error) {
	return m.allMigrationSource().FindMigrations()
}

func (m *MigratorImpl) allMigrationSource() *migrate.MemoryMigrationSource {
	src := &migrate.MemoryMigrationSource{}

	for _, migration := range m.migrations {
		src.Migrations = append(src.Migrations, migration.Migration)
	}

	return src
}

func (m *MigratorImpl) eligibleMigrations() ([]*migrate.Migration, error) {
	src, err := m.eligibleMigrationSource()
	if err != nil {
		return nil, err
	}

	return src.FindMigrations()
}

func (m *MigratorImpl) eligibleMigrationSource() (*migrate.MemoryMigrationSource, error) {
	src := &migrate.MemoryMigrationSource{}

	records, err := migrate.GetMigrationRecords(m.db, dialect)
	if err != nil {
		return src, err
	}

	for _, migration := range m.migrations {
		if m.skipPostDeployment && migration.PostDeployment &&
			// Do not skip already applied postdeployment migrations. The migration
			// library expects to see applied migrations when it plans a migration,
			// and we should ensure that down migrations affect all applied migrations.
			!migrationApplied(records, migration.Id) {
			continue
		}

		src.Migrations = append(src.Migrations, migration.Migration)
	}

	return src, nil
}

func migrationApplied(records []*migrate.MigrationRecord, id string) bool {
	for _, r := range records {
		if r.Id == id {
			return true
		}
	}

	return false
}

func (m *MigratorImpl) FindMigrationByID(id string) *Migration {
	for _, mig := range m.migrations {
		if mig.Id == id {
			return mig
		}
	}
	return nil
}

// migrateUpWithBBMCheck applies up to 'max' database migrations (0 for unlimited).
// It verifies Batched Background Migration (BBM) dependencies before each migration.
// Returns the number of applied schema migrations, background migrations or an error if any step fails,
// including when required BBMs are incomplete.
func (m *MigratorImpl) migrateUpWithBBMCheck(maximum int) (MigrationResult, error) {
	var mr MigrationResult
	// Initialize a new store to manage background migrations
	bbmStore := datastore.NewBackgroundMigrationStore(m.db)

	// Retrieve the source of eligible migrations
	src, err := m.eligibleMigrationSource()
	if err != nil {
		return mr, fmt.Errorf("getting eligible migration source: %w", err)
	}

	// Fetch the migration records that have already been applied
	migrationRecords, err := migrate.GetMigrationRecords(m.db, dialect)
	if err != nil {
		return mr, fmt.Errorf("retrieving migration records: %w", err)
	}

	// no migrations have been applied means this is an new install
	newInstall := len(migrationRecords) == 0

	// Create a map to store applied migrations for quick lookup
	migrationRecordsMap := make(map[string]struct{}, len(migrationRecords))
	for _, record := range migrationRecords {
		migrationRecordsMap[record.Id] = struct{}{}
	}

	// Retrieve and sort all available migrations by ID
	sortedMigrations, err := src.FindMigrations()
	if err != nil {
		return mr, fmt.Errorf("finding migrations: %w", err)
	}

	// Map to hold all local migrations for reference during the process
	localMigrationsMap := make(map[string]*Migration, len(m.migrations))
	for _, migration := range m.migrations {
		localMigrationsMap[migration.Id] = migration
	}

	ctx := context.Background()

	// Iterate through each migration, applying them if necessary
	for _, migration := range sortedMigrations {
		// Stop if we reach the specified 'max' number of migrations
		if maximum != 0 && mr.AppliedCount == maximum {
			break
		}

		// Skip migrations that have already been applied
		if _, applied := migrationRecordsMap[migration.Id]; applied {
			continue
		}

		// Ensure all Batched Background Migrations (BBMs) are completed before applying migration
		if err := m.ensureBBMsComplete(ctx, bbmStore, localMigrationsMap[migration.Id]); err != nil {
			// Apply incomplete BBMs during new installations
			if !errors.Is(err, ErrBBMNotComplete) || !newInstall {
				// Return the error if it's not a new installation and there are incomplete BBMs
				return mr, err
			}

			// Run the BBM worker
			if err := m.bbmWorker.Run(ctx); err != nil {
				return mr, err
			}

			// Increment the count of finished BBMs
			mr.AppliedBBMCount += m.bbmWorker.FinishedMigrationCount()
		}

		// Apply the migration
		if err := m.applyMigration(src, migration); err != nil {
			return mr, err
		}

		// Increment the count of applied migrations
		mr.AppliedCount++
	}

	// Return the number of applied migrations
	return mr, nil
}

// ensureBBMsComplete checks if all required Batched Background Migrations (BBMs) are complete for a schema migration.
func (*MigratorImpl) ensureBBMsComplete(ctx context.Context, bbmStore datastore.BackgroundMigrationStore, migration *Migration) error {
	// If no BBMs are required, return early
	if len(migration.RequiredBBMs) == 0 {
		return nil
	}

	// Check if all required BBMs are completed
	complete, err := bbmStore.AreFinished(ctx, migration.RequiredBBMs)
	if err != nil {
		return fmt.Errorf("checking BBM completion: %w", err)
	}
	// If BBMs are not complete, return an error
	if !complete {
		return fmt.Errorf("schema migration: %s failed on ensuring batched background migration: %v: %w", migration.Id, migration.RequiredBBMs, ErrBBMNotComplete)
	}
	return nil
}

// applyMigration executes a single migration in the 'Up' direction.
func (m *MigratorImpl) applyMigration(src *migrate.MemoryMigrationSource, migration *migrate.Migration) error {
	// Execute the migration and move the database schema "Up"
	_, err := migrate.ExecVersion(m.db, dialect, src, migrate.Up, migration.VersionInt())
	if err != nil {
		return fmt.Errorf("applying migration %s: %w", migration.Id, err)
	}
	return nil
}
