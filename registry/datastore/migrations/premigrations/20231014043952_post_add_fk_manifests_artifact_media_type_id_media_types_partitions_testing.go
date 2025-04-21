//go:build integration

package premigrations

import (
	"fmt"

	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	var ups, downs []string
	for i := 0; i <= 3; i++ {
		ups = append(ups, fmt.Sprintf(
			`DO $$
			BEGIN
				IF NOT EXISTS (
					SELECT
						1
					FROM
						pg_catalog.pg_constraint
					WHERE
						conrelid = 'partitions.manifests_p_%d'::regclass
						AND conname = 'fk_manifests_artifact_media_type_id_media_types'
				) THEN
					ALTER TABLE partitions.manifests_p_%d ADD CONSTRAINT fk_manifests_artifact_media_type_id_media_types
						FOREIGN KEY (artifact_media_type_id)
						REFERENCES media_types(id);
				END IF;
			END;
			$$`, i, i))

		downs = append(downs, fmt.Sprintf(
			"ALTER TABLE partitions.manifests_p_%d DROP CONSTRAINT IF EXISTS fk_manifests_artifact_media_type_id_media_types",
			i))
	}

	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id:                   "20231014043952_post_add_fk_manifests_artifact_media_type_id_media_types_partitions_testing",
			Up:                   ups,
			Down:                 downs,
			DisableTransactionUp: true,
		},
	}

	migrations.AppendPreMigration(m)
}
