package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20230822130421_drop_repositories_migration_columns",
			Up: []string{
				"ALTER TABLE repositories DROP COLUMN IF EXISTS migration_status",
				"ALTER TABLE repositories DROP COLUMN IF EXISTS migration_error",
			},
			Down: []string{
				"ALTER TABLE repositories ADD COLUMN IF NOT EXISTS migration_status text",
				`DO $$
				BEGIN
					IF NOT EXISTS (
						SELECT
							1
						FROM
							pg_constraint
						WHERE
							conrelid = 'repositories'::regclass
							AND conname = 'check_repositories_migration_status_length') THEN
						ALTER TABLE repositories
							ADD CONSTRAINT check_repositories_migration_status_length CHECK ((char_length(migration_status) <= 255)) NOT VALID;
					END IF;
				END;
				$$`,
				"ALTER TABLE repositories VALIDATE CONSTRAINT check_repositories_migration_status_length",
				"ALTER TABLE repositories ADD COLUMN IF NOT EXISTS migration_error text",
				`DO $$
				BEGIN
					IF NOT EXISTS (
						SELECT
							1
						FROM
							pg_constraint
						WHERE
							conrelid = 'repositories'::regclass
							AND conname = 'check_repositories_migration_error_length') THEN
						ALTER TABLE repositories
							ADD CONSTRAINT check_repositories_migration_error_length CHECK ((char_length(migration_error) <= 255)) NOT VALID;
					END IF;
				END;
				$$`,
				"ALTER TABLE repositories VALIDATE CONSTRAINT check_repositories_migration_error_length",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPreMigration(m)
}
