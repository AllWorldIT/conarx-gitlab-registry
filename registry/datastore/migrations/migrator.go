//go:generate mockgen -package mocks -destination mocks/migration.go . Migrator

package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/docker/distribution/registry/bbm"
	"github.com/docker/distribution/registry/datastore"
	migrate "github.com/rubenv/sql-migrate"
)

const (
	PreDeployMigrationTableName  = "schema_migrations"
	PostDeployMigrationTableName = "post_deploy_schema_migrations"
	dialect                      = "postgres"
)

var ErrBBMNotComplete = errors.New("background migration must be complete before proceeding")

// MigrationDependencyResolver determines whether a migration can be applied by verifying its pre/post deploy dependencies.
// It returns the number of dependencies successfully applied (if any) or an error if the process fails.
type MigrationDependencyResolver func(ctx context.Context, migration *Migration) (int, error)

// MigrationResult holds the outcome of a migration operation.
// It includes the count of applied Batched Background Migrations (BBMs)
// and the total number of applied schema migrations.
type MigrationResult struct {
	AppliedBBMCount        int
	AppliedCount           int
	AppliedDependencyCount int
}

type Migrator interface {
	Up() (MigrationResult, error)
	Down() (int, error)
}

type DepMigratorImp struct {
	MigratorImpl
}

var (
	once        sync.Once
	StatusCache map[string]*MigrationStatus
)

func (s *DepMigratorImp) StatusCache() map[string]*MigrationStatus {
	once.Do(func() {
		StatusCache, _ = s.Status()
	})

	// In tests we may force reset the cache by setting it to nil.
	// This allows us to re-trigger getting the latest status in case
	// the old status map is stale or was corrupted by another test.
	if StatusCache == nil {
		StatusCache, _ = s.Status()
	}
	return StatusCache
}

type MigratorImpl struct {
	db         *sql.DB
	migrations []*Migration
	bbmWorker  *bbm.SyncWorker
	ms         migrate.MigrationSet
}

type PureMigrator interface {
	Name() string
	Down() (int, error)
	DownN(n int) (int, error)
	DownNPlan(n int) ([]string, error)
	FindMigrationByID(id string) *Migration
	HasPending() (bool, error)
	LatestVersion() (string, error)
	Reconfigure(f MigratorOption)
	Status() (map[string]*MigrationStatus, error)
	Up(extraCheck ...MigrationDependencyResolver) (MigrationResult, error)
	UpN(n int, extraCheck ...MigrationDependencyResolver) (MigrationResult, error)
	UpNPlan(n int) ([]string, error)
	Version() (string, error)
}

// NewMigrator creates new Migrator.
// NOTE(prozlach): we can not use interface here because in some cases the code
// is accessing private fields of the package/object. This would need a deeper
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

// WithTable allows the migrator to use an alternative table to track migrations.
func WithTable(tableName string) MigratorOption {
	return func(m *MigratorImpl) {
		m.ms = migrate.MigrationSet{TableName: tableName}
	}
}

// WithMigrations allows the migrator to use an alternative set of migrations.
func WithMigrations(all []*Migration) MigratorOption {
	return func(m *MigratorImpl) {
		m.migrations = all
	}
}

// Name of migrator.
func (*MigratorImpl) Name() string {
	return ""
}

// Reconfigure is used to change the configuration of an existing Migrator
// using given config option.
func (m *MigratorImpl) Reconfigure(f MigratorOption) {
	f(m)
}

// Version returns the current applied migration version (if any).
func (m *MigratorImpl) Version() (string, error) {
	records, err := m.ms.GetMigrationRecords(m.db, dialect)
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
	src, err := m.EligibleMigrationSource()
	if err != nil {
		return 0, err
	}

	return m.ms.ExecMax(m.db, dialect, src, direction, limit)
}

// Up applies all pending up migrations. Returns the number of applied migrations and background migrations.
func (m *MigratorImpl) Up(extraCheck ...MigrationDependencyResolver) (MigrationResult, error) {
	return m.migrateUpWithCheck(0, extraCheck...)
}

// UpN applies up to n pending up migrations. All pending migrations will be applied if n is 0.  Returns the number of
// applied migrations and background migrations.
func (m *MigratorImpl) UpN(n int, extraCheck ...MigrationDependencyResolver) (MigrationResult, error) {
	return m.migrateUpWithCheck(n, extraCheck...)
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
	Unknown   bool
	AppliedAt *time.Time
}

// Status returns the status of all migrations, indexed by migration ID.
func (m *MigratorImpl) Status() (map[string]*MigrationStatus, error) {
	applied, err := m.ms.GetMigrationRecords(m.db, dialect)
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
	records, err := m.ms.GetMigrationRecords(m.db, dialect)
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
	src, err := m.EligibleMigrationSource()
	if err != nil {
		return nil, err
	}

	planned, _, err := m.ms.PlanMigration(m.db, dialect, src, direction, limit)
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
	src, err := m.EligibleMigrationSource()
	if err != nil {
		return nil, err
	}

	return src.FindMigrations()
}

func (m *MigratorImpl) EligibleMigrationSource() (*migrate.MemoryMigrationSource, error) {
	src := &migrate.MemoryMigrationSource{}

	for _, migration := range m.migrations {
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

// migrateUpWithCheck applies up to 'max' database migrations (0 for unlimited).
// It verifies Batched Background Migration (BBM) dependencies before each migration.
// Additionally, it allows for custom checks to be performed on each migration using MigrationDependencyResolver.
// These checks can verify if a migration depends on a pre/post migration and either run it or fail the migration.
// Returns the number of applied (pre/post) schema migrations, background migrations, or an error if any step fails.
func (m *MigratorImpl) migrateUpWithCheck(maximum int, extraCheck ...MigrationDependencyResolver) (MigrationResult, error) {
	var mr MigrationResult
	// Initialize a new store to manage background migrations
	bbmStore := datastore.NewBackgroundMigrationStore(m.db)

	// Retrieve the source of eligible migrations
	src, err := m.EligibleMigrationSource()
	if err != nil {
		return mr, fmt.Errorf("getting eligible migration source: %w", err)
	}

	// Fetch the migration records that have already been applied
	migrationRecords, err := m.ms.GetMigrationRecords(m.db, dialect)
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

		// runs custom checks per migration
		for _, check := range extraCheck {
			var appliedDependentMigration int
			if appliedDependentMigration, err = check(ctx, localMigrationsMap[migration.Id]); err != nil {
				return mr, err
			}

			// Increment the count of applied migrations
			mr.AppliedDependencyCount += appliedDependentMigration
		}

		// Apply the migration
		var appliedCount int
		if appliedCount, err = m.ApplyMigration(src, migration); err != nil {
			return mr, err
		}

		// Increment the count of applied migrations
		mr.AppliedCount += appliedCount
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

// ApplyMigration applies all migration in the 'Up' direction up to `migration`.
func (m *MigratorImpl) ApplyMigration(src *migrate.MemoryMigrationSource, migration *migrate.Migration) (int, error) {
	// Execute the migration and move the database schema "Up"
	n, err := m.ms.ExecVersion(m.db, dialect, src, migrate.Up, migration.VersionInt())
	if err != nil {
		return 0, fmt.Errorf("applying migration %s: %w", migration.Id, err)
	}
	return n, nil
}
