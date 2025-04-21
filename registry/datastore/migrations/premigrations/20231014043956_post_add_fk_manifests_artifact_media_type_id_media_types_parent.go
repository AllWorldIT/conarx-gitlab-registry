package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20231014043956_post_add_fk_manifests_artifact_media_type_id_media_types_parent",
			Up: []string{
				`DO $$
				BEGIN
					IF NOT EXISTS (
						SELECT
						1
					FROM
						pg_catalog.pg_constraint
					WHERE
						conrelid = 'public.manifests'::regclass
						AND conname = 'fk_manifests_artifact_media_type_id_media_types'
					) THEN
						ALTER TABLE manifests ADD CONSTRAINT fk_manifests_artifact_media_type_id_media_types
							FOREIGN KEY (artifact_media_type_id)
							REFERENCES media_types(id);
					END IF;
				END;
				$$`,
			},
			Down: []string{
				`ALTER TABLE manifests DROP CONSTRAINT IF EXISTS fk_manifests_artifact_media_type_id_media_types`,
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPreMigration(m)
}
