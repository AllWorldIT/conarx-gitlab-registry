package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20231013035836_add_artifact_media_type_id_to_manifests",
			Up: []string{
				`ALTER TABLE manifests ADD COLUMN IF NOT EXISTS artifact_media_type_id BIGINT`,
			},
			Down: []string{
				`ALTER TABLE manifests DROP COLUMN IF EXISTS artifact_media_type_id`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
