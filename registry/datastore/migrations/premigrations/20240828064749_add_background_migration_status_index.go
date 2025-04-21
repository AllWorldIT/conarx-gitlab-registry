package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20240828064749_add_background_migration_status_index",
			Up: []string{
				"CREATE INDEX IF NOT EXISTS index_batched_background_migrations_on_status ON batched_background_migrations USING btree (status)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS index_batched_background_migrations_on_status CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
