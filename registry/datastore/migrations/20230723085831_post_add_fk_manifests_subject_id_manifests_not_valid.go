//go:build !integration

package migrations

import (
	"fmt"

	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	var ups, downs []string
	for i := 0; i <= 63; i++ {
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
						AND conname = 'fk_manifests_subject_id_manifests'
				) THEN
					ALTER TABLE partitions.manifests_p_%d ADD CONSTRAINT fk_manifests_subject_id_manifests
						FOREIGN KEY (top_level_namespace_id, repository_id, subject_id)
						REFERENCES manifests(top_level_namespace_id, repository_id, id)
						ON DELETE CASCADE NOT VALID;
				END IF;
			END;
			$$`, i, i))

		downs = append(downs, fmt.Sprintf(
			"ALTER TABLE partitions.manifests_p_%d DROP CONSTRAINT IF EXISTS fk_manifests_subject_id_manifests",
			i))
	}

	m := &Migration{
		Migration: &migrate.Migration{
			Id:                     "20230723085831_post_add_fk_manifests_subject_id_manifests_not_valid",
			Up:                     ups,
			Down:                   downs,
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}
