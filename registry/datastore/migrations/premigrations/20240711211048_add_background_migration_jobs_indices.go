package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20240711211048_add_background_migration_jobs_indices",
			Up: []string{
				"CREATE INDEX IF NOT EXISTS index_batched_background_migration_jobs_on_bbm_id ON batched_background_migration_jobs USING btree (batched_background_migration_id)",
				"CREATE INDEX IF NOT EXISTS index_batched_background_migration_jobs_on_bbm_id_and_max_value ON batched_background_migration_jobs USING btree (batched_background_migration_id, max_value)",
				"CREATE INDEX IF NOT EXISTS index_batched_background_migration_jobs_on_bbm_id_and_id_desc ON batched_background_migration_jobs USING btree (batched_background_migration_id, id DESC)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS index_batched_background_migration_jobs_on_bbm_id CASCADE",
				"DROP INDEX IF EXISTS index_batched_background_migration_jobs_on_bbm_id_and_max_value CASCADE",
				"DROP INDEX IF EXISTS index_batched_background_migration_jobs_on_bbm_id_and_id_desc CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
