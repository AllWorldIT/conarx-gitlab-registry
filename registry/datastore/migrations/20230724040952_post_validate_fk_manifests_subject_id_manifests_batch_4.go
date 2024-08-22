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

	for i := 39; i <= 51; i++ {
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
						AND conname = 'fk_manifests_subject_id_manifests'
				) THEN
					ALTER TABLE partitions.manifests_p_%d VALIDATE CONSTRAINT fk_manifests_subject_id_manifests;
				END IF;
			END;
			$$`, i, i))
	}

	ups = append(ups, "RESET statement_timeout")

	m := &Migration{
		Migration: &migrate.Migration{
			Id:                   "20230724040952_post_validate_fk_manifests_subject_id_manifests_batch_4",
			Up:                   ups,
			Down:                 []string{},
			DisableTransactionUp: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}
