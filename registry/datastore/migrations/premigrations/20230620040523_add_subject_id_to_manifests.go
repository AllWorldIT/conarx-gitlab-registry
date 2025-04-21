package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20230620040523_add_subject_id_to_manifests",
			Up: []string{
				`ALTER TABLE manifests ADD COLUMN IF NOT EXISTS subject_id BIGINT`,
			},
			Down: []string{
				`ALTER TABLE manifests DROP COLUMN IF EXISTS subject_id`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
