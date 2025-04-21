package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20230724040955_post_add_fk_manifests_subject_id_manifests_parent",
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
						AND conname = 'fk_manifests_subject_id_manifests'
					) THEN
						ALTER TABLE manifests ADD CONSTRAINT fk_manifests_subject_id_manifests
							FOREIGN KEY (top_level_namespace_id, repository_id, subject_id)
							REFERENCES manifests(top_level_namespace_id, repository_id, id)
							ON DELETE CASCADE;
					END IF;
				END;
				$$`,
			},
			Down: []string{
				`ALTER TABLE manifests DROP CONSTRAINT IF EXISTS fk_manifests_subject_id_manifests`,
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPreMigration(m)
}
