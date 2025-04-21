package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220224124146_add_repositories_migration_error_column",
			Up: []string{
				"ALTER TABLE repositories ADD COLUMN IF NOT EXISTS migration_error text",
				`DO $$
				BEGIN
					IF NOT EXISTS (
						SELECT
							1
						FROM
							information_schema.constraint_column_usage
						WHERE
							table_name = 'repositories'
							AND column_name = 'migration_error'
							AND constraint_name = 'check_repositories_migration_error_length') THEN
						ALTER TABLE public.repositories
							ADD CONSTRAINT check_repositories_migration_error_length CHECK ((char_length(migration_error) <= 255)) NOT VALID;
					END IF;
				END;
				$$`,
				"ALTER TABLE repositories VALIDATE CONSTRAINT check_repositories_migration_error_length",
			},
			Down: []string{
				"ALTER TABLE repositories DROP CONSTRAINT IF EXISTS check_repositories_migration_error_length",
				"ALTER TABLE repositories DROP COLUMN IF EXISTS migration_error",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
