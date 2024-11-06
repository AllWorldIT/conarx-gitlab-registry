package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20241031081325_add_background_migration_timing_columns",
			Up: []string{
				"ALTER TABLE batched_background_migrations ADD COLUMN IF NOT EXISTS started_at timestamp WITH time zone",
				"ALTER TABLE batched_background_migrations ADD COLUMN IF NOT EXISTS finished_at timestamp WITH time zone",
			},
			Down: []string{
				"ALTER TABLE batched_background_migrations DROP COLUMN IF EXISTS started_at",
				"ALTER TABLE batched_background_migrations DROP COLUMN IF EXISTS finished_at",
			},
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}
