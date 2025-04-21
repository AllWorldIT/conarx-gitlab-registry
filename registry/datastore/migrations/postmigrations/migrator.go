package postmigrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/docker/distribution/registry/bbm"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/migrations"
	"github.com/hashicorp/go-multierror"
)

// PostDeployTypeName is the type name for post-deployment migrations.
const PostDeployTypeName = "post-deployment"

// MigratorImpl is the implementation of the Migrator interface for post-deployment migrations.
type MigratorImpl struct {
	migrations.MigratorImpl // Embed the base MigratorImpl for common functionality.
	db                      *sql.DB
	migrations              []*migrations.Migration
	preMigrator             *migrations.DepMigratorImp
	bbmWorker               *bbm.SyncWorker
}

// NewMigrator creates a new instance of MigratorImpl.
func NewMigrator(dsdb *datastore.DB, opts ...MigratorOption) *MigratorImpl {
	var db *sql.DB
	if dsdb != nil {
		db = dsdb.DB
	}

	m := &MigratorImpl{
		MigratorImpl: *migrations.NewMigrator(dsdb, migrations.WithTable(migrations.PostDeployMigrationTableName), migrations.WithMigrations(migrations.AllPostMigrations())),
		db:           db,
		migrations:   migrations.AllPostMigrations(),
		bbmWorker:    bbm.NewSyncWorker(dsdb),
		preMigrator: &migrations.DepMigratorImp{
			MigratorImpl: *migrations.NewMigrator(dsdb, migrations.WithTable(migrations.PreDeployMigrationTableName)),
		},
	}

	// Apply any additional options to the migrator.
	for _, o := range opts {
		o(m)
	}

	return m
}

// MigratorOption enables the creation of functional options for the
// configuration of the migrator.
type MigratorOption func(m *MigratorImpl)

// Name returns the name of the migrator.
func (*MigratorImpl) Name() string {
	return PostDeployTypeName
}

// Up applies all pending up migrations. Returns the number of applied migrations and background migrations.
func (m *MigratorImpl) Up(_ ...migrations.MigrationDependencyResolver) (migrations.MigrationResult, error) {
	return m.MigratorImpl.Up(preDeployCheckFunc(m))
}

// UpN applies up to n pending up migrations. All pending migrations will be applied if n is 0.  Returns the number of
// applied migrations and background migrations.
func (m *MigratorImpl) UpN(n int, _ ...migrations.MigrationDependencyResolver) (migrations.MigrationResult, error) {
	return m.MigratorImpl.UpN(n, preDeployCheckFunc(m))
}

// preDeployCheckFunc is a function that checks if all required pre-deployment migrations are completed before applying a migration.
func preDeployCheckFunc(m *MigratorImpl) migrations.MigrationDependencyResolver {
	return func(_ context.Context, migration *migrations.Migration) (int, error) {
		// If no migrations are required, return early
		if len(migration.RequiredPreDeploy) == 0 {
			return 0, nil
		}

		// Check if all required migrations are completed
		statuses := m.preMigrator.StatusCache()
		var depErrs *multierror.Error
		for _, id := range migration.RequiredPreDeploy {
			val, found := statuses[id]
			if !found {
				return 0, fmt.Errorf("pre-deploy migration %s, does not exist in migration source", id)
			}
			if val.AppliedAt != nil {
				continue
			}
			depErrs = multierror.Append(depErrs, fmt.Errorf("post-deploy migration %s cannot be applied until the following pre-deploy migrations is applied: %v", migration.Id, id))
		}
		return 0, depErrs.ErrorOrNil()
	}
}
