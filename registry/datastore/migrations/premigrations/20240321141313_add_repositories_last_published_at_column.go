package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20240321141313_add_repositories_last_published_at_column",
			Up: []string{
				"ALTER TABLE repositories ADD COLUMN IF NOT EXISTS last_published_at timestamp WITH time zone",
			},
			Down: []string{
				"ALTER TABLE repositories DROP COLUMN IF EXISTS last_published_at",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
