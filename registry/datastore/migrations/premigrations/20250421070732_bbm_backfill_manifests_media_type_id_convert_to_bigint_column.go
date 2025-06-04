package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250421070732_bbm_backfill_manifests_media_type_id_convert_to_bigint_column",

			Up: []string{
				`INSERT INTO batched_background_migrations ("name", "min_value", "max_value", "batch_size", "status", "job_signature_name", "table_name", "column_name")
					VALUES ('copy_manifests_media_type_id_column_to_media_type_id_convert_to_bigint_column', 1, -- Default BIGINT Undershoot minimum for IDENTITY
						9223372036854775807, -- BIGINT maximum (overshoot)
						10000, 1, -- Active status
						'copyManifestMediaTypeIDToNewBigIntColumn', 'public.manifests', 'id')`,
			},
			Down: []string{
				`DELETE FROM batched_background_migrations WHERE "name" = 'copy_manifests_media_type_id_column_to_media_type_id_convert_to_bigint_column'`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
