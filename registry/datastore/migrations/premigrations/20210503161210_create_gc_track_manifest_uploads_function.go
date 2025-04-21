package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503161210_create_gc_track_manifest_uploads_function",
			Up: []string{
				`CREATE OR REPLACE FUNCTION gc_track_manifest_uploads ()
					RETURNS TRIGGER
					AS $$
				BEGIN
					INSERT INTO gc_manifest_review_queue (top_level_namespace_id, repository_id, manifest_id, review_after)
						VALUES (NEW.top_level_namespace_id, NEW.repository_id, NEW.id, gc_review_after('manifest_upload'));
					RETURN NULL;
				END;
				$$
				LANGUAGE plpgsql`,
			},
			Down: []string{
				"DROP FUNCTION IF EXISTS gc_track_manifest_uploads CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
