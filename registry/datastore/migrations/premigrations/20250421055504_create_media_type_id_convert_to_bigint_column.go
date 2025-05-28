package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250421055504_create_media_type_id_convert_to_bigint_column",
			Up: []string{
				// Add the new column with a default value of 0 and a NOT NULL constraint (same as `media_type_id`)
				`ALTER TABLE manifests
    				ADD COLUMN media_type_id_convert_to_bigint bigint NOT NULL DEFAULT 0`,
			},
			Down: []string{
				`ALTER TABLE manifests
    				DROP COLUMN IF EXISTS media_type_id_convert_to_bigint`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
