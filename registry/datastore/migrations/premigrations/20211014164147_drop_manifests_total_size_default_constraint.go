package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20211014164147_drop_manifests_total_size_default_constraint",
			Up: []string{
				"ALTER TABLE manifests ALTER COLUMN total_size DROP DEFAULT",
			},
			Down: []string{
				"ALTER TABLE manifests ALTER COLUMN total_size SET DEFAULT 0",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
