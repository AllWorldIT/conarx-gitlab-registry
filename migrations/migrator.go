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

type migrator struct {
	db  *sql.DB
	src *migrate.MemoryMigrationSource
}

func NewMigrator(db *sql.DB) *migrator {
	return &migrator{
		db:  db,
		src: &migrate.MemoryMigrationSource{Migrations: allMigrations},
	}
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
	all, err := m.src.FindMigrations()
	if err != nil {
		return "", err
	}
	if len(all) == 0 {
		return "", nil
	}

	return all[len(all)-1].Id, nil
}

func (m *migrator) migrate(direction migrate.MigrationDirection, limit int) error {
	_, err := migrate.ExecMax(m.db, dialect, m.src, direction, limit)
	return err
}

// Up applies all pending up migrations.
func (m *migrator) Up() error {
	return m.migrate(migrate.Up, 0)
}

// UpN applies up to n pending up migrations. All pending migrations will be applied if n is 0.
func (m *migrator) UpN(n int) error {
	return m.migrate(migrate.Up, n)
}

// UpNPlan plans up to n pending up migrations and returns the ordered list of migration IDs. All pending migrations
// will be planned if n is 0.
func (m *migrator) UpNPlan(n int) ([]string, error) {
	return m.plan(migrate.Up, n)
}

// Down applies all pending down migrations.
func (m *migrator) Down() error {
	return m.migrate(migrate.Down, 0)
}

// migrationStatus represents the status of a migration. Unknown will be set to true if a migration was applied but is
// not known by the current build.
type migrationStatus struct {
	Unknown   bool
	AppliedAt *time.Time
}

// Status returns the status of all migrations, indexed by migration ID.
func (m *migrator) Status() (map[string]*migrationStatus, error) {
	applied, err := migrate.GetMigrationRecords(m.db, dialect)
	known, err := m.src.FindMigrations()
	if err != nil {
		return nil, err
	}

	statuses := make(map[string]*migrationStatus, len(applied))
	for _, m := range known {
		statuses[m.Id] = &migrationStatus{}
	}
	for _, m := range applied {
		if _, ok := statuses[m.Id]; !ok {
			statuses[m.Id] = &migrationStatus{Unknown: true}
		}
		statuses[m.Id].AppliedAt = &m.AppliedAt
	}

	return statuses, nil
}

// DownN applies up to n pending down migrations. All migrations will be applied if n is 0.
func (m *migrator) DownN(n int) error {
	return m.migrate(migrate.Down, n)
}

// DownNPlan plans up to n pending down migrations and returns the ordered list of migration IDs. All pending migrations
// will be planned if n is 0.
func (m *migrator) DownNPlan(n int) ([]string, error) {
	return m.plan(migrate.Down, n)
}

func (m *migrator) plan(direction migrate.MigrationDirection, limit int) ([]string, error) {
	planned, _, err := migrate.PlanMigration(m.db, dialect, m.src, direction, limit)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(planned))
	for _, m := range planned {
		result = append(result, m.Id)
	}

	return result, nil
}
