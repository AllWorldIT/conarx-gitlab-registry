package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20251215055504_add_media_type_id_convert_to_bigint_column",
			Up: []string{
				`ALTER TABLE blobs
    				ADD COLUMN media_type_id_convert_to_bigint bigint NOT NULL DEFAULT 0`,
				`ALTER TABLE layers
    				ADD COLUMN media_type_id_convert_to_bigint bigint NOT NULL DEFAULT 0`,
				// configuration_media_type_id_convert_to_bigint must be nullable since the source column configuration_media_type_id is nullable
				`ALTER TABLE manifests
    				ADD COLUMN configuration_media_type_id_convert_to_bigint bigint`,
			},
			Down: []string{
				`ALTER TABLE blobs
    				DROP COLUMN IF EXISTS media_type_id_convert_to_bigint`,
				`ALTER TABLE layers
    				DROP COLUMN IF EXISTS media_type_id_convert_to_bigint`,
				`ALTER TABLE manifests
    				DROP COLUMN IF EXISTS configuration_media_type_id_convert_to_bigint`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
