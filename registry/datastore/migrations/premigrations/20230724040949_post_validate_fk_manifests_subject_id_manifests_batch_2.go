//go:build !integration

package premigrations

import (
	"fmt"

	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	ups := []string{
		"SET statement_timeout TO 0",
	}

	for i := 13; i <= 25; i++ {
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

	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id:                   "20230724040949_post_validate_fk_manifests_subject_id_manifests_batch_2",
			Up:                   ups,
			Down:                 []string{},
			DisableTransactionUp: true,
		},
	}

	migrations.AppendPreMigration(m)
}
