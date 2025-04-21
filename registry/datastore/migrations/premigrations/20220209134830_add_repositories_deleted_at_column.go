package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220209134830_add_repositories_deleted_at_column",
			Up: []string{
				"ALTER TABLE repositories ADD COLUMN IF NOT EXISTS deleted_at timestamp WITH time zone",
			},
			Down: []string{
				"ALTER TABLE repositories DROP COLUMN IF EXISTS deleted_at",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
