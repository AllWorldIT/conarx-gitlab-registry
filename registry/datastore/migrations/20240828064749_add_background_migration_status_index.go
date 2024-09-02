package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20240828064749_add_background_migration_status_index",
			Up: []string{
				"CREATE INDEX IF NOT EXISTS index_batched_background_migrations_on_status ON batched_background_migrations USING btree (status)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS index_batched_background_migrations_on_status CASCADE",
			},
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}
