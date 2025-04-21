package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210728165231_add_manifests_non_conformant_column",
			Up: []string{
				"ALTER TABLE manifests ADD COLUMN IF NOT EXISTS non_conformant BOOLEAN DEFAULT FALSE",
			},
			Down: []string{
				"ALTER TABLE manifests DROP COLUMN IF EXISTS non_conformant",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
