package premigrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/docker/distribution/registry/bbm"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/migrations"
	"github.com/hashicorp/go-multierror"
)

// PreDeployTypeName is the name of the pre-deployment migrator.
const PreDeployTypeName = "pre-deployment"

// MigratorImpl is the implementation of the pre-deployment migrator.
type MigratorImpl struct {
	migrations.MigratorImpl // Embed the base MigratorImpl for common functionality.
	db                      *sql.DB
	migrations              []*migrations.Migration
	postMigrator            *migrations.DepMigratorImp
	bbmWorker               *bbm.SyncWorker
	skipPostDeployment      bool
}

// Name returns the name of the pre-deployment migrator.
func (*MigratorImpl) Name() string {
	return PreDeployTypeName
}

// NewMigrator creates a new pre-deployment migrator.
func NewMigrator(dsdb *datastore.DB, opts ...MigratorOption) *MigratorImpl {
	var db *sql.DB
	if dsdb != nil {
		db = dsdb.DB
	}
	m := &MigratorImpl{
		MigratorImpl: *migrations.NewMigrator(dsdb, migrations.WithTable(migrations.PreDeployMigrationTableName), migrations.WithMigrations(migrations.AllPreMigrations())),
		db:           db,
		migrations:   migrations.AllPreMigrations(),
		bbmWorker:    bbm.NewSyncWorker(dsdb),
		postMigrator: &migrations.DepMigratorImp{
			MigratorImpl: *migrations.NewMigrator(dsdb, migrations.WithTable(migrations.PostDeployMigrationTableName), migrations.WithMigrations(migrations.AllPostMigrations())),
		},
	}

	for _, o := range opts {
		o(m)
	}

	return m
}

// MigratorOption is a functional option for the configuration of the pre-deployment migrator.
type MigratorOption func(m *MigratorImpl)

// SkipPostDeployment configures the pre-deployment migrator to not apply post-deployment migrations.
func SkipPostDeployment() MigratorOption {
	return func(m *MigratorImpl) {
		m.skipPostDeployment = true
	}
}

// Up applies all pending up migrations. Returns the number of applied migrations and background migrations.
func (m *MigratorImpl) Up(_ ...migrations.MigrationDependencyResolver) (migrations.MigrationResult, error) {
	return m.MigratorImpl.Up(postDeployCheckFunc(m))
}

// UpN applies up to n pending up migrations. All pending migrations will be applied if n is 0.  Returns the number of
// applied migrations and background migrations.
func (m *MigratorImpl) UpN(n int, _ ...migrations.MigrationDependencyResolver) (migrations.MigrationResult, error) {
	return m.MigratorImpl.UpN(n, postDeployCheckFunc(m))
}

// postDeployCheckFunc checks if all required post-deployment migrations for a given migration are completed.
func postDeployCheckFunc(m *MigratorImpl) migrations.MigrationDependencyResolver {
	spotApplyRequiredMigration := !m.skipPostDeployment
	return func(_ context.Context, migration *migrations.Migration) (int, error) {
		// If no migrations are required, return early
		if len(migration.RequiredPostDeploy) == 0 {
			return 0, nil
		}

		// Check if all required migrations are completed
		statuses := m.postMigrator.StatusCache()
		var depErrs *multierror.Error
		var appliedPostDeployCount int
		for _, id := range migration.RequiredPostDeploy {
			val, found := statuses[id]
			if !found {
				return appliedPostDeployCount, fmt.Errorf("post-deploy migration %s does not exist in migration source", id)
			}

			if val.AppliedAt != nil {
				continue
			}

			if !spotApplyRequiredMigration {
				depErrs = multierror.Append(depErrs, fmt.Errorf("required post-deploy schema migration %s not yet applied and can not be skipped", id))
				continue
			}

			// only try to apply a post migration if there are no dependency errors
			if depErrs.ErrorOrNil() == nil {
				// run post deployment migrations up till the point point of the required migration id (inclusive)
				mig := m.postMigrator.FindMigrationByID(id)
				src, err := m.postMigrator.EligibleMigrationSource()
				if err != nil {
					return appliedPostDeployCount, fmt.Errorf("getting eligible migration source: %w", err)
				}
				n, err := m.postMigrator.ApplyMigration(src, mig.Migration)
				if err != nil {
					return appliedPostDeployCount, fmt.Errorf("applying post-deploy migration up to migration %s: %w", migration.Id, err)
				}
				appliedPostDeployCount += n
			}
		}
		return appliedPostDeployCount, depErrs.ErrorOrNil()
	}
}
