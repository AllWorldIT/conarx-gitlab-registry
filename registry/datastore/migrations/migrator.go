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

// CanSkipPostDeploy checks whether post-deployment migrations (PDM) can be safely skipped.
// It ensures that all pending PDM are positioned at the end of the migration list.
// If a PDM appears between non-PDM, skipping is unsafe.
// TODO (suleimiahmed): this is a roundabout way of ensuring that users do not harm themselves when skipping PDM
// due to the sequential way that PDM and non-PDM are applied. We should revisit this and only rely on
// strictly defined functional dependencies when skipping post-deploy migrations. https://gitlab.com/gitlab-org/container-registry/-/issues/1521.
func (m *MigratorImpl) CanSkipPostDeploy(migrationlimit int) (bool, int, error) {
	// retrieve applied migration records from the database.
	records, err := migrate.GetMigrationRecords(m.db, dialect)
	if err != nil {
		return false, 0, fmt.Errorf("skip-post-deployment migration check failed: %w", err)
	}

	// sort pending migrations and classify post-deployment migrations.
	sortedMigrations, pendingPDSet := classifyPendingMigrations(migrationlimit, records, m.migrations)

	// validate that all post-deployment migrations appear in the correct order.
	if failure, valid := validatePostDeployMigrationOrder(sortedMigrations, pendingPDSet); !valid {
		return false, failure.SafeToMigrateLimit, fmt.Errorf("cannot safely skip post-deployment migration: %s", failure.MigrationID)
	}

	return true, 0, nil
}

// PostDeployOrderFailure represents a failure case where a post-deployment migration is incorrectly ordered.
type PostDeployOrderFailure struct {
	MigrationID        string // id of the improperly ordered migration
	SafeToMigrateLimit int    // number of non-PDM that can be safely applied before failure
}

// validatePostDeployOrder ensures that post-deployment migrations are correctly positioned.
// It returns an error if a non-PDM appears after an pending PDM.
func validatePostDeployMigrationOrder(sortedMigrations []*migrate.Migration, pendingPDSet map[string]struct{}) (*PostDeployOrderFailure, bool) {
	var (
		lastPDID         string
		safeMigrateLimit int
		pdSeen           bool
	)

	for _, migration := range sortedMigrations {
		if _, isPostDeploy := pendingPDSet[migration.Id]; isPostDeploy {
			// mark the first occurrence of a post-deployment migration.
			lastPDID = migration.Id
			pdSeen = true
		} else if pdSeen {
			// a non-PDM appearing after a PDM is a violation.
			if safeMigrateLimit == 0 {
				safeMigrateLimit = -1 // no safe non-PDM can be applied.
			}
			return &PostDeployOrderFailure{
				MigrationID:        lastPDID,
				SafeToMigrateLimit: safeMigrateLimit,
			}, false
		}
		if !pdSeen {
			// Only count non-PDM that come before encountering a PDM.
			safeMigrateLimit++
		}
	}

	return nil, true
}

// classifyPendingMigrations sorts pending migrations and identifies post-deployment migrations.
func classifyPendingMigrations(migrationlimit int, appliedRecords []*migrate.MigrationRecord, allMigrations []*Migration) ([]*migrate.Migration, map[string]struct{}) {
	src := &migrate.MemoryMigrationSource{}
	pendingPDSet := make(map[string]struct{})
	pendingNonPDCount := 0

	// identify pending migrations
	for _, migration := range allMigrations {
		if migrationApplied(appliedRecords, migration.Id) {
			continue // skip already applied migrations
		}
		if migration.PostDeployment {
			pendingPDSet[migration.Id] = struct{}{} // track pending post-deployment migrations
		} else {
			pendingNonPDCount++
		}

		src.Migrations = append(src.Migrations, migration.Migration)

		// stop early if we've already reached the migration limit for non-post-deployment migrations
		if migrationlimit != 0 && pendingNonPDCount >= migrationlimit {
			// migrationlimit == 0 means no limit
			break
		}
	}

	// sort the migrations by ID
	sortedMigrations, _ := src.FindMigrations()

	return sortedMigrations, pendingPDSet
}
