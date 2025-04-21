package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220107172750_add_repositories_migration_status_column",
			Up: []string{
				"ALTER TABLE repositories ADD COLUMN IF NOT EXISTS migration_status text NOT NULL DEFAULT 'native'",
				`DO $$
				BEGIN
					IF NOT EXISTS (
						SELECT
							1
						FROM
							information_schema.constraint_column_usage
						WHERE
							table_name = 'repositories'
							AND column_name = 'migration_status'
							AND constraint_name = 'check_repositories_migration_status_length') THEN
						ALTER TABLE public.repositories
							ADD CONSTRAINT check_repositories_migration_status_length CHECK ((char_length(migration_status) <= 255)) NOT VALID;
					END IF;
				END;
				$$`,
				"ALTER TABLE repositories VALIDATE CONSTRAINT check_repositories_migration_status_length",
			},
			Down: []string{
				"ALTER TABLE repositories DROP CONSTRAINT IF EXISTS check_repositories_migration_status_length",
				"ALTER TABLE repositories DROP COLUMN IF EXISTS migration_status",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
