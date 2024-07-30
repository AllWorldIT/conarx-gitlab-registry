//go:build !integration

package migrations

import (
	"fmt"

	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	ups := []string{
		"SET statement_timeout TO 0",
	}

	for i := 0; i <= 12; i++ {
		ups = append(ups, fmt.Sprintf(
			`DO $$
			BEGIN
				IF EXISTS (
					SELECT
						1
					FROM
						pg_catalog.pg_constraint
					WHERE
						conrelid = 'partitions.manifests_p_%d'::regclass
						AND conname = 'fk_manifests_artifact_media_type_id_media_types'
					) THEN
					ALTER TABLE partitions.manifests_p_%d VALIDATE CONSTRAINT fk_manifests_artifact_media_type_id_media_types;
				END IF;
			END;
			$$`, i, i))
	}

	ups = append(ups, "RESET statement_timeout")

	m := &Migration{
		Migration: &migrate.Migration{
			Id:                   "20231014043852_post_validate_fk_manifests_artifact_media_type_id_media_types_batch_1",
			Up:                   ups,
			DisableTransactionUp: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}
