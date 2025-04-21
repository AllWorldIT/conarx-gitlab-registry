//go:build integration

package migrationfixtures

import (
	"fmt"
	"time"

	"github.com/docker/distribution/registry/datastore/migrations"
	_ "github.com/docker/distribution/registry/datastore/migrations/premigrations"
	migrate "github.com/rubenv/sql-migrate"
)

var (
	currentTime = time.Now()

	// IDs of base schema migrations for batched background migrations
	bbmBaseSchemaMigrationIDs = []string{
		"20240604074823_create_batched_background_migrations_table",
		"20240604074846_create_batched_background_migration_jobs_table",
		"20240711175726_add_background_migration_failure_error_code_column",
		"20241031081325_add_background_migration_timing_columns",
	}

	// Enforced background migration
	enforcedBBMMigrations = &migrations.Migration{
		Migration: &migrate.Migration{
			Id: fmt.Sprintf("%s_insert_enforced_bbm_record", currentTime.Format("20060102150405")),
			Up: []string{
				`INSERT INTO batched_background_migrations ("name", "min_value", "max_value", "batch_size", "status", "job_signature_name", "table_name", "column_name")
				VALUES ('enforcedBBM', 0, 5,  5, 2, 'signatureName', 'public.repository_manifests_test', 'id')`,
			},
			Down: []string{
				`DELETE FROM batched_background_migrations WHERE "name" = 'enforcedBBM'`,
			},
		},
		RequiredBBMs: []string{"unenforcedBBM"},
	}

	// Unenforced running background migration
	unEnforcedRunningBBMMigrations = &migrations.Migration{
		Migration: &migrate.Migration{
			Id: fmt.Sprintf("%s_insert_unenforced_bbm_record", currentTime.Add(-1*time.Second).Format("20060102150405")),
			Up: []string{
				`INSERT INTO batched_background_migrations ("name", "min_value", "max_value", "batch_size", "status", "job_signature_name", "table_name", "column_name")
				VALUES ('unenforcedBBM', 0, 5,  5, 4, 'signatureName', 'public.repository_manifests_test', 'id')`,
			},
			Down: []string{
				`DELETE FROM batched_background_migrations WHERE "name" = 'unenforcedBBM'`,
			},
		},
	}
)

// allWithBBMSchema returns all migrations including the BBM schema migrations
func allWithBBMSchema() []*migrations.Migration {
	stdMigrator := migrations.NewMigrator(nil, migrations.Source(migrations.AllPreMigrations()))
	var allWithBBMSchema []*migrations.Migration
	for _, v := range bbmBaseSchemaMigrationIDs {
		if mig := stdMigrator.FindMigrationByID(v); mig != nil {
			allWithBBMSchema = append(allWithBBMSchema, mig)
		}
	}
	return append(allMigrations, allWithBBMSchema...)
}

// AllWithEnforcedBBMMigrations returns all migrations including enforced BBM migrations
func AllWithEnforcedBBMMigrations() []*migrations.Migration {
	return append(AllWithUnEnforcedBBMMigrations(), enforcedBBMMigrations)
}

// AllWithUnEnforcedBBMMigrations returns all migrations including unenforced BBM migrations
func AllWithUnEnforcedBBMMigrations() []*migrations.Migration {
	return append(allWithBBMSchema(), unEnforcedRunningBBMMigrations)
}
