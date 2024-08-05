//go:generate mockgen -package mocks -destination mocks/migration.go . Migrator

package migrations

import (
	"database/sql"
	"time"

	migrate "github.com/rubenv/sql-migrate"
)

const (
	migrationTableName = "schema_migrations"
	dialect            = "postgres"
)

func init() {
	migrate.SetTable(migrationTableName)
}

type Migrator interface {
	Up() (int, error)
	Down() (int, error)
}

type migrator struct {
	db         *sql.DB
	migrations []*Migration

	skipPostDeployment bool
}

func NewMigrator(db *sql.DB, opts ...MigratorOption) *migrator {
	m := &migrator{
		db:         db,
		migrations: allMigrations,
	}

	for _, o := range opts {
		o(m)
	}

	return m
}

// MigratorOption enables the creation of functional options for the
// configuration of the migrator.
type MigratorOption func(m *migrator)

// Source allows the migrator to use an alternative source of migrations, used
// for testing.
func Source(a []*Migration) func(m *migrator) {
	return func(m *migrator) {
		m.migrations = a
	}
}

// SkipPostDeployment configures the migration to not apply postdeployment migrations.
func SkipPostDeployment(m *migrator) {
	m.skipPostDeployment = true
}

// Version returns the current applied migration version (if any).
func (m *migrator) Version() (string, error) {
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
func (m *migrator) LatestVersion() (string, error) {
	all, err := m.eligibleMigrations()
	if err != nil {
		return "", err
	}
	if len(all) == 0 {
		return "", nil
	}

	return all[len(all)-1].Id, nil
}

func (m *migrator) migrate(direction migrate.MigrationDirection, limit int) (int, error) {
	src, err := m.eligibleMigrationSource()
	if err != nil {
		return 0, err
	}

	return migrate.ExecMax(m.db, dialect, src, direction, limit)
}

// Up applies all pending up migrations. Returns the number of applied migrations.
func (m *migrator) Up() (int, error) {
	return m.migrate(migrate.Up, 0)
}

// UpN applies up to n pending up migrations. All pending migrations will be applied if n is 0.  Returns the number of
// applied migrations.
func (m *migrator) UpN(n int) (int, error) {
	return m.migrate(migrate.Up, n)
}

// UpNPlan plans up to n pending up migrations and returns the ordered list of migration IDs. All pending migrations
// will be planned if n is 0.
func (m *migrator) UpNPlan(n int) ([]string, error) {
	return m.plan(migrate.Up, n)
}

// Down applies all pending down migrations.  Returns the number of applied migrations.
func (m *migrator) Down() (int, error) {
	return m.migrate(migrate.Down, 0)
}

// DownN applies up to n pending down migrations. All migrations will be applied if n is 0.  Returns the number of
// applied migrations.
func (m *migrator) DownN(n int) (int, error) {
	return m.migrate(migrate.Down, n)
}

// DownNPlan plans up to n pending down migrations and returns the ordered list of migration IDs. All pending migrations
// will be planned if n is 0.
func (m *migrator) DownNPlan(n int) ([]string, error) {
	return m.plan(migrate.Down, n)
}

// migrationStatus represents the status of a migration. Unknown will be set to true if a migration was applied but is
// not known by the current build.
type migrationStatus struct {
	Unknown        bool
	PostDeployment bool
	AppliedAt      *time.Time
}

// Status returns the status of all migrations, indexed by migration ID.
func (m *migrator) Status() (map[string]*migrationStatus, error) {
	applied, err := migrate.GetMigrationRecords(m.db, dialect)
	if err != nil {
		return nil, err
	}
	known, err := m.allMigrations()
	if err != nil {
		return nil, err
	}

	statuses := make(map[string]*migrationStatus, len(applied))
	for _, k := range known {
		statuses[k.Id] = &migrationStatus{}

		if mig := m.FindMigrationByID(k.Id); mig != nil && mig.PostDeployment {
			statuses[k.Id].PostDeployment = true
		}
	}

	for _, m := range applied {
		if _, ok := statuses[m.Id]; !ok {
			statuses[m.Id] = &migrationStatus{Unknown: true}
		}

		statuses[m.Id].AppliedAt = &m.AppliedAt
	}

	return statuses, nil
}

// HasPending determines whether all known migrations are applied or not.
func (m *migrator) HasPending() (bool, error) {
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

func (m *migrator) plan(direction migrate.MigrationDirection, limit int) ([]string, error) {
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

func (m *migrator) allMigrations() ([]*migrate.Migration, error) {
	return m.allMigrationSource().FindMigrations()
}

func (m *migrator) allMigrationSource() *migrate.MemoryMigrationSource {
	src := &migrate.MemoryMigrationSource{}

	for _, migration := range m.migrations {
		src.Migrations = append(src.Migrations, migration.Migration)
	}

	return src
}

func (m *migrator) eligibleMigrations() ([]*migrate.Migration, error) {
	src, err := m.eligibleMigrationSource()
	if err != nil {
		return nil, err
	}

	return src.FindMigrations()
}

func (m *migrator) eligibleMigrationSource() (*migrate.MemoryMigrationSource, error) {
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

func (m *migrator) FindMigrationByID(id string) *Migration {
	for _, mig := range m.migrations {
		if mig.Id == id {
			return mig
		}
	}
	return nil
}
