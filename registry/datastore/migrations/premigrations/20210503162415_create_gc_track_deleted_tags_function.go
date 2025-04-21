package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503162415_create_gc_track_deleted_tags_function",
			Up: []string{
				`CREATE OR REPLACE FUNCTION gc_track_deleted_tags ()
					RETURNS TRIGGER
					AS $$
				BEGIN
					IF EXISTS (
						SELECT
							1
						FROM
							manifests
						WHERE
							top_level_namespace_id = OLD.top_level_namespace_id
							AND repository_id = OLD.repository_id
							AND id = OLD.manifest_id) THEN
					INSERT INTO gc_manifest_review_queue (top_level_namespace_id, repository_id, manifest_id, review_after)
						VALUES (OLD.top_level_namespace_id, OLD.repository_id, OLD.manifest_id, gc_review_after ('tag_delete'))
					ON CONFLICT (top_level_namespace_id, repository_id, manifest_id)
						DO UPDATE SET
							review_after = gc_review_after ('tag_delete');
				END IF;
					RETURN NULL;
				END;
				$$
				LANGUAGE plpgsql`,
			},
			Down: []string{
				"DROP FUNCTION IF EXISTS gc_track_deleted_tags CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
