package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20240711175726_add_background_migration_failure_error_code_column",
			Up: []string{
				"ALTER TABLE batched_background_migrations ADD COLUMN IF NOT EXISTS failure_error_code smallint",
			},
			Down: []string{
				"ALTER TABLE batched_background_migrations DROP COLUMN IF EXISTS failure_error_code",
			},
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}
